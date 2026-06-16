"""
page2_sniper.py — Engine 2: Page 2 Keyword Opportunity Sniper
EthicalSEO Outbound Signal Engine

INPUT:  qualified_companies.csv  (output from qualify.py)
OUTPUT: sniper_results.csv

Signal logic:
  1. Pull page 2 keywords (positions 11-20, CPC > 0) via SEMrush organic-keywords
  2. Pull top 10 keywords (positions 1-10) for the same domain — used for overlap check
  3. Drop any page-2 keyword that semantically overlaps with a top-10 keyword
     (e.g. "accounting software" vs "best software for accounting" — same intent,
     pitching it would make no sense since they already rank page 1 for that theme)
  4. Pick the best remaining keyword per domain using Claude Haiku (highest commercial intent)
  5. Find the ranking URL from SEMrush
  6. Scrape the ranking page — scan title, H1, H2s, H3s, and body text for keyword presence
  7. Flag only domains where keyword is ABSENT from the entire page (real on-page opportunity)
  8. Generate personalised cold email hook using Claude Sonnet

Key logic changes vs previous versions:
  - v1: checked if keyword missing from H1 → suggested adding to H1
  - v2: scans entire page (title, H1, H2s, H3s, body). Only flags if keyword is
    completely absent from the page. Pitch is to add it to H2/H3 — not H1,
    since H1 may already be used for a page-1 keyword.
  - v3: adds top-10 overlap filter — drops page-2 keywords that share core intent
    with an existing top-10 ranking to avoid nonsensical outreach pitches.

Performance:
  - 5 parallel workers for ~5x speed improvement
  - Rate limiter keeps SEMrush calls under 10 req/sec (conservative)
  - 429 retry with exponential backoff on all API calls
  - Thread-safe checkpointing every 100 companies
  - Resumes from interruption automatically

SEMrush field reference (organic/organic endpoint):
  Ph  — keyword string
  Po  — ranking position (int)
  Ur  — ranking URL
  Cp  — CPC in dollars (float, e.g. 4.20)
  Nq  — monthly search volume (int)
  Kd  — keyword difficulty (int, 0-100)
"""

import csv
import json
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import anthropic
import requests
from bs4 import BeautifulSoup

# ─── API KEYS ────────────────────────────────────────────────────────────────
SEMRUSH_API_KEY   = "YOUR_SEMRUSH_API_KEY_HERE"
ANTHROPIC_API_KEY = "YOUR_ANTHROPIC_API_KEY_HERE"
# ─── MODELS ──────────────────────────────────────────────────────────────────
HAIKU_MODEL  = "claude-haiku-4-5-20251001"   # keyword picker (fast + cheap)
SONNET_MODEL = "claude-sonnet-4-6"           # hook writer (quality)

# ─── PATHS ───────────────────────────────────────────────────────────────────
INPUT_FILE      = "qualified_companies.csv"
OUTPUT_FILE     = "sniper_results.csv"
CHECKPOINT_FILE = "sniper_checkpoint.json"

# ─── CONCURRENCY & RATE LIMITING ─────────────────────────────────────────────
MAX_WORKERS      = 5     # parallel workers
SEMRUSH_RPS      = 8     # stay under 10 req/sec — conservative buffer
MIN_SEMRUSH_GAP  = 1.0 / SEMRUSH_RPS  # minimum seconds between SEMrush calls

# ─── RETRY SETTINGS ──────────────────────────────────────────────────────────
MAX_RETRIES     = 4    # max attempts on 429 / 5xx
RETRY_BASE_WAIT = 2.0  # seconds — doubles each retry (2, 4, 8, 16)

# ─── SEMRUSH SETTINGS ────────────────────────────────────────────────────────
SEMRUSH_BASE_URL = "https://api.semrush.com"
PAGE2_LIMIT      = 10   # top 10 page-2 keywords by volume per domain
MIN_POSITION     = 11
MAX_POSITION     = 20
CHECKPOINT_EVERY = 100  # save checkpoint every N companies

# ─── KEYWORD QUALITY FILTERS ─────────────────────────────────────────────────
JUNK_SUBSTRINGS = [
    "login", "log in", "sign in", "sign up", "signup", "register",
    "account", "password", "forgot", "reset password",
    "tracking", "track my", "track order", "where is my",
    "customer service", "contact", "support",
    "coupon", "promo code", "discount code",
    "near me", "phone number", "hours",
    "download", "install", "app store", "play store",
]

MIN_CPC_DOLLARS   = 2.00  # drop keywords worth less than $2 CPC
MIN_KEYWORD_WORDS = 2     # drop single-word keywords

INFORMATIONAL_PREFIXES = [
    "what is", "what are", "what does", "what do",
    "how to", "how do", "how does",
    "why is", "why are", "why does",
    "when is", "when are", "when does",
    "who is", "who are",
    "where is", "where are",
    "definition of", "meaning of", "examples of",
    "types of", "list of", "history of",
]

# ─── OUTPUT COLUMNS ──────────────────────────────────────────────────────────
OUTPUT_COLUMNS = [
    "company_name", "domain", "keyword", "position", "cpc",
    "volume", "ranking_url", "keyword_on_page", "found_in",
    "final_hook", "processed_at", "error",
]


# ══════════════════════════════════════════════════════════════════════════════
# THREAD-SAFE RATE LIMITER FOR SEMRUSH
# ══════════════════════════════════════════════════════════════════════════════

class RateLimiter:
    """Ensures minimum gap between calls across all threads."""
    def __init__(self, min_gap_seconds: float):
        self._lock      = threading.Lock()
        self._last_call = 0.0
        self._gap       = min_gap_seconds

    def acquire(self):
        with self._lock:
            now  = time.monotonic()
            wait = self._gap - (now - self._last_call)
            if wait > 0:
                time.sleep(wait)
            self._last_call = time.monotonic()

semrush_limiter = RateLimiter(MIN_SEMRUSH_GAP)


# ══════════════════════════════════════════════════════════════════════════════
# THREAD-SAFE CHECKPOINT + OUTPUT WRITER
# ══════════════════════════════════════════════════════════════════════════════

_checkpoint_lock   = threading.Lock()
_output_lock       = threading.Lock()
_counter_lock      = threading.Lock()
_processed_domains: set = set()
_counters = {"success": 0, "skipped": 0, "errors": 0, "done": 0}


def load_checkpoint() -> set:
    if not os.path.exists(CHECKPOINT_FILE):
        return set()
    try:
        with open(CHECKPOINT_FILE) as f:
            data = json.load(f)
        return set(data.get("processed_domains", []))
    except Exception:
        return set()


def save_checkpoint():
    with _checkpoint_lock:
        with open(CHECKPOINT_FILE, "w") as f:
            json.dump({"processed_domains": list(_processed_domains)}, f)


def mark_processed(domain: str, counter_key: str, total: int):
    with _checkpoint_lock:
        _processed_domains.add(domain)
    with _counter_lock:
        _counters[counter_key] += 1
        _counters["done"] += 1
        done = _counters["done"]
    if done % CHECKPOINT_EVERY == 0:
        save_checkpoint()
        print(f"\n  [CHECKPOINT] {done}/{total} companies processed\n")


def append_result(row: dict):
    with _output_lock:
        file_exists = os.path.exists(OUTPUT_FILE)
        with open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS, extrasaction="ignore")
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)


# ══════════════════════════════════════════════════════════════════════════════
# RETRY HELPER
# ══════════════════════════════════════════════════════════════════════════════

def with_retry(fn, *args, label="call", **kwargs):
    """Exponential backoff on 429 / 5xx for both requests and Anthropic calls."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return fn(*args, **kwargs)
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else 0
            if status in (429, 500, 502, 503, 504) and attempt < MAX_RETRIES:
                wait = RETRY_BASE_WAIT * (2 ** (attempt - 1))
                print(f"    [Retry {attempt}/{MAX_RETRIES}] {label} — HTTP {status}, waiting {wait:.0f}s")
                time.sleep(wait)
            else:
                raise
        except anthropic.RateLimitError:
            if attempt < MAX_RETRIES:
                wait = RETRY_BASE_WAIT * (2 ** (attempt - 1))
                print(f"    [Retry {attempt}/{MAX_RETRIES}] {label} — Anthropic rate limit, waiting {wait:.0f}s")
                time.sleep(wait)
            else:
                raise
        except Exception:
            raise
    raise RuntimeError(f"Exhausted {MAX_RETRIES} retries for {label}")


# ══════════════════════════════════════════════════════════════════════════════
# SEMRUSH — PAGE 2 KEYWORDS
# ══════════════════════════════════════════════════════════════════════════════

def _clean(val: str) -> str:
    """Strip surrounding quotes and whitespace from a SEMrush CSV value."""
    return val.strip().strip('"')


def _semrush_get(domain: str) -> tuple[list[dict], list[str]]:
    """
    Raw SEMrush API call using the domain organic keywords endpoint.
    Fetches a larger batch and splits client-side into:
      - page2_keywords: positions 11-20, CPC > 0
      - top10_keywords: positions 1-10, keyword strings only (for overlap check)
    SEMrush returns semicolon-delimited text with quoted values.
    Actual headers returned: Keyword;Position;Url;CPC;Search Volume;Keyword Difficulty
    """
    semrush_limiter.acquire()

    params = {
        "type":           "domain_organic",
        "key":            SEMRUSH_API_KEY,
        "domain":         domain,
        "database":       "us",
        "display_limit":  20,             # fetch more, filter client-side
        "display_offset": 0,
        "display_sort":   "nq_desc",      # sort by volume descending
        "export_columns": "Ph,Po,Ur,Cp,Nq,Kd",
        "export_escape":  1,
    }

    resp = requests.get(
        SEMRUSH_BASE_URL,
        params=params,
        timeout=30,
    )
    resp.raise_for_status()

    # SEMrush returns semicolon-delimited text with quoted values
    # Headers: Keyword;Position;Url;CPC;Search Volume;Keyword Difficulty
    lines = resp.text.strip().splitlines()
    if len(lines) < 2:
        return [], []

    headers = [h.strip().strip('"') for h in lines[0].split(";")]
    page2_results = []
    top10_keywords = []

    for line in lines[1:]:
        values = line.split(";")
        if len(values) != len(headers):
            continue
        row = {headers[i]: _clean(values[i]) for i in range(len(headers))}
        try:
            position = int(float(row.get("Position", 0) or 0))
            cpc      = float(row.get("CPC", 0) or 0)
            keyword  = row.get("Keyword", "").strip()

            # Collect top-10 keywords for overlap check
            if 1 <= position <= 10 and keyword:
                top10_keywords.append(keyword.lower())
                continue

            # Client-side filter: positions 11-20, CPC > 0
            if not (MIN_POSITION <= position <= MAX_POSITION):
                continue
            if cpc <= 0:
                continue

            page2_results.append({
                "keyword":            keyword,
                "best_position":      position,
                "best_position_url":  row.get("Url", "").strip(),
                "cpc_display":        round(cpc, 2),
                "volume":             int(float(row.get("Search Volume", 0) or 0)),
                "keyword_difficulty": int(float(row.get("Keyword Difficulty", 0) or 0)),
            })

            if len(page2_results) >= PAGE2_LIMIT:
                break

        except (ValueError, TypeError):
            continue

    return page2_results, top10_keywords


def fetch_page2_keywords(domain: str) -> tuple[list[dict], list[str]]:
    try:
        return with_retry(_semrush_get, domain, label=f"semrush:{domain}")
    except Exception as e:
        print(f"    [SEMrush error] {domain}: {e}")
        return [], []


# ══════════════════════════════════════════════════════════════════════════════
# KEYWORD QUALITY FILTER
# ══════════════════════════════════════════════════════════════════════════════

def filter_keywords(keywords: list[dict], domain: str) -> list[dict]:
    domain_root     = domain.split(".")[0].lower()
    filtered, dropped = [], []

    for kw in keywords:
        keyword     = kw["keyword"].lower().strip()
        cpc_dollars = kw["cpc_display"]
        word_count  = len(keyword.split())

        if word_count < MIN_KEYWORD_WORDS:
            dropped.append((kw["keyword"], "single_word")); continue

        if cpc_dollars < MIN_CPC_DOLLARS:
            dropped.append((kw["keyword"], f"low_cpc_${cpc_dollars}")); continue

        junk_hit = next((j for j in JUNK_SUBSTRINGS if j in keyword), None)
        if junk_hit:
            dropped.append((kw["keyword"], f"junk:{junk_hit}")); continue

        info_hit = next((p for p in INFORMATIONAL_PREFIXES if p in keyword), None)
        if info_hit:
            dropped.append((kw["keyword"], f"informational:{info_hit}")); continue

        words = keyword.split()
        if words[0] == domain_root and word_count <= 2:
            dropped.append((kw["keyword"], "own_brand_nav")); continue

        # Drop platform analysis pages (e.g. semrush.com/website/otherdomain.com/overview)
        ranking_url = kw.get("best_position_url", "")
        url_path    = ranking_url.replace("https://", "").replace("http://", "")
        path_only   = "/".join(url_path.split("/")[1:])
        domain_in_path = any(
            re.search(r'\.[a-z]{2,6}$', seg, re.I)
            for seg in path_only.split("/")
            if len(seg) > 3
        )
        if domain_in_path:
            dropped.append((kw["keyword"], "domain_in_url_path")); continue

        filtered.append(kw)

    if dropped:
        print(f"    ↳ Filtered out {len(dropped)} junk keywords: "
              + ", ".join(f'"{k}"({r})' for k, r in dropped[:4])
              + (" ..." if len(dropped) > 4 else ""))
    return filtered


# ══════════════════════════════════════════════════════════════════════════════
# TOP-10 OVERLAP FILTER
# ══════════════════════════════════════════════════════════════════════════════

# Stop-words excluded from overlap comparison — these are too generic to signal
# intent on their own (e.g. "best", "software", "for" appear in almost everything)
_OVERLAP_STOP_WORDS = {
    "best", "top", "good", "great", "free", "cheap", "affordable", "easy",
    "software", "tool", "tools", "app", "platform", "service", "solution",
    "for", "to", "the", "a", "an", "and", "or", "of", "in", "with",
    "how", "what", "why", "is", "are", "vs", "review", "reviews",
    "online", "small", "business", "company", "using",
}

OVERLAP_THRESHOLD = 0.6  # drop if ≥60% of meaningful words match a top-10 keyword


def _meaningful_words(keyword: str) -> set[str]:
    """Return lowercase non-stop words from a keyword string."""
    return {w for w in keyword.lower().split() if w not in _OVERLAP_STOP_WORDS and len(w) > 1}


def filter_top10_overlaps(keywords: list[dict], top10_keywords: list[str]) -> list[dict]:
    """
    Drop any page-2 keyword whose core intent overlaps with a top-10 keyword.

    Logic: if ≥60% of the meaningful words in the page-2 keyword appear in
    any single top-10 keyword (or vice versa), they share the same search intent
    and pitching the page-2 keyword would make no sense.

    Example:
      top-10:  "best software for accounting"  → meaningful: {accounting}
      page-2:  "accounting software"           → meaningful: {accounting}
      overlap: 1/1 = 100% → DROP
    """
    if not top10_keywords:
        return keywords  # no top-10 data, skip filter

    filtered, dropped = [], []

    for kw in keywords:
        p2_words = _meaningful_words(kw["keyword"])
        if not p2_words:
            filtered.append(kw)
            continue

        overlap_hit = None
        for t10 in top10_keywords:
            t10_words = _meaningful_words(t10)
            if not t10_words:
                continue
            # Check overlap in both directions — take the higher ratio
            shared      = p2_words & t10_words
            ratio       = max(len(shared) / len(p2_words), len(shared) / len(t10_words))
            if ratio >= OVERLAP_THRESHOLD:
                overlap_hit = t10
                break

        if overlap_hit:
            dropped.append((kw["keyword"], f"top10_overlap:'{overlap_hit}'"))
        else:
            filtered.append(kw)

    if dropped:
        print(f"    ↳ Dropped {len(dropped)} top-10 overlaps: "
              + ", ".join(f'"{k}"({r})' for k, r in dropped[:4])
              + (" ..." if len(dropped) > 4 else ""))
    return filtered


# ══════════════════════════════════════════════════════════════════════════════
# CLAUDE HAIKU — KEYWORD PICKER
# ══════════════════════════════════════════════════════════════════════════════

def pick_best_keyword(client: anthropic.Anthropic, domain: str, keywords: list[dict]) -> dict | None:
    if not keywords:
        return None
    if len(keywords) == 1:
        return keywords[0]

    kw_list = "\n".join(
        f"{i+1}. \"{kw['keyword']}\" — position {kw['best_position']}, "
        f"volume {kw['volume']}, CPC ${kw['cpc_display']}"
        for i, kw in enumerate(keywords)
    )

    prompt = f"""You are an SEO commercial-intent analyst. Your job is to pick ONE keyword that EthicalSEO can use in a cold outreach email to {domain}.

Domain: {domain}
Page 2 keywords (positions 11-20):

{kw_list}

STEP 1 - HARD REJECTION (eliminate any keyword that fails ANY of these):
- Is the keyword a third-party tool, platform, or brand that {domain} merely integrates with or mentions in a blog post? REJECT. (e.g. streak.com ranking for "gmail" - gmail is not what Streak sells)
- Is the keyword a feature or action within a third-party platform, even if {domain} integrates with it? REJECT. (e.g. tactiq.io ranking for "microsoft teams meeting" - the meeting itself is a Teams feature, not what Tactiq sells)
- Is the keyword a generic consumer term, social media platform, or productivity tool unrelated to {domain}'s core product? REJECT. (e.g. "google sheets", "gmail inbox", "chrome extensions")
- Is the keyword a competitor brand name? REJECT.
- Is the keyword about a topic {domain} wrote one blog post about but doesn't actually sell? REJECT. Be strict here — a blog post about "spamassassin score" does not mean the domain sells spam scoring tools. A blog post about "wireframe examples" does not mean the domain sells wireframing software. If the keyword describes something a visitor would go elsewhere to buy, REJECT.
- Does the keyword describe something a completely different type of business would sell? REJECT.
- Is the keyword a compliance, regulatory, or HR administration topic that the domain covers in content but is not their core product? REJECT. (e.g. an employee benefits platform ranking for "employee's state insurance" — that's a compliance topic, not what they sell)
- Is the keyword a cloud infrastructure or managed service product name from AWS, GCP, or Azure, even if {domain} integrates with it? REJECT. (e.g. "elastic container registry", "cloud run", "s3 bucket" — these are AWS/GCP products, not what the domain sells)
- Is the keyword ranking via a forum thread, community post, or Q&A page rather than the domain's core product pages? REJECT. (e.g. remnote.io ranking for "custom css" via a forum thread — that's community content, not what they sell)

STEP 2 - POSITIVE SELECTION (from what remains, pick the best):
- Must sound like a BUYING keyword - someone ready to spend money (e.g. "best CRM software", "crm for small business") NOT informational
- Highest commercial value = CPC x volume
- Directly describes {domain}'s core product or a feature they actually sell

If NOTHING passes Step 1, respond with index 0:
{{"index": 0, "reason": "no relevant keywords found"}}

Otherwise respond with ONLY a JSON object, no other text:
{{"index": <1-based index of chosen keyword>, "reason": "<one sentence>"}}"""

    def _call():
        return client.messages.create(
            model=HAIKU_MODEL,
            max_tokens=200,
            messages=[{"role": "user", "content": prompt}],
        )

    try:
        msg          = with_retry(_call, label=f"haiku:{domain}")
        raw          = msg.content[0].text.strip()
        raw          = raw.replace("```json", "").replace("```", "").strip()
        match        = re.search(r'\{[^}]+\}', raw, re.DOTALL)
        if not match:
            raise ValueError(f"No JSON in Haiku response: {raw[:100]}")
        result       = json.loads(match.group())
        chosen_index = int(result["index"]) - 1
        if chosen_index == -1:
            print(f"    ↳ Haiku: no relevant keywords for {domain} — skipping")
            return None
        if 0 <= chosen_index < len(keywords):
            return keywords[chosen_index]
        return keywords[0]
    except Exception as e:
        print(f"    [Haiku error] {domain}: {e}")
        return keywords[0]


# ══════════════════════════════════════════════════════════════════════════════
# PAGE SCRAPER — FULL PAGE KEYWORD SCAN
# ══════════════════════════════════════════════════════════════════════════════
#
# Logic change from previous version:
#   OLD: checked if keyword missing from H1 → suggested adding to H1
#   NEW: scans title, H1, all H2s, all H3s, and body text.
#        Only flags the domain if keyword is completely absent from the page.
#        Reason: H1 may already be used for a page-1 keyword — we shouldn't
#        suggest overwriting it. Instead, pitch adding the keyword to H2/H3.

SCRAPE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}


def scrape_page(url: str, keyword: str) -> dict:
    """
    Scrape the ranking page and check whether the keyword appears anywhere.

    Returns:
        keyword_on_page (bool): True if keyword found anywhere on page
        found_in (str):         Where it was found: "title", "h1", "h2", "h3",
                                "body", "multiple" — or "" if not found
        error (str):            Any scrape/parse error
    """
    result = {
        "keyword_on_page": False,
        "found_in":        "",
        "error":           "",
    }
    if not url:
        result["error"] = "no_url"
        return result

    try:
        resp = requests.get(url, headers=SCRAPE_HEADERS, timeout=15, allow_redirects=True)
        resp.raise_for_status()
    except requests.exceptions.Timeout:
        result["error"] = "timeout"; return result
    except requests.exceptions.TooManyRedirects:
        result["error"] = "too_many_redirects"; return result
    except Exception as e:
        result["error"] = f"scrape_error: {str(e)[:80]}"; return result

    try:
        soup    = BeautifulSoup(resp.text, "html.parser")
        kw      = keyword.lower().strip()
        found   = []

        # Check title
        title_tag = soup.find("title")
        if title_tag and kw in title_tag.get_text(strip=True).lower():
            found.append("title")

        # Check H1
        h1_tag = soup.find("h1")
        if h1_tag and kw in h1_tag.get_text(strip=True).lower():
            found.append("h1")

        # Check all H2s
        for h2 in soup.find_all("h2"):
            if kw in h2.get_text(strip=True).lower():
                found.append("h2")
                break  # one match is enough

        # Check all H3s
        for h3 in soup.find_all("h3"):
            if kw in h3.get_text(strip=True).lower():
                found.append("h3")
                break

        # Check body text (paragraphs, divs, spans — broad sweep)
        if not found:
            body_text = soup.get_text(separator=" ", strip=True).lower()
            if kw in body_text:
                found.append("body")

        if found:
            result["keyword_on_page"] = True
            result["found_in"] = "multiple" if len(found) > 1 else found[0]
        else:
            result["keyword_on_page"] = False
            result["found_in"] = ""

    except Exception as e:
        result["error"] = f"parse_error: {str(e)[:80]}"

    return result


# ══════════════════════════════════════════════════════════════════════════════
# CLAUDE SONNET — HOOK WRITER
# ══════════════════════════════════════════════════════════════════════════════

def generate_hook(
    client: anthropic.Anthropic,
    company_name: str, domain: str, keyword: str,
    position: int, cpc_display: float, volume: int,
    keyword_on_page: bool, found_in: str,
) -> str:
    if keyword_on_page:
        # Keyword exists on page but still ranking page 2 — different pitch
        location_note = f"(found in: {found_in})" if found_in else ""
        onpage_obs = (
            f"The keyword does appear on their page {location_note}, but they're still stuck on page 2 — "
            f"so the issue is likely authority or backlinks, not on-page content."
        )
    else:
        # Keyword completely absent — this is the real opportunity
        onpage_obs = (
            f"I checked the page — the keyword \"{keyword}\" doesn't appear anywhere on it. "
            f"Not in the title, not in any heading, not in the body copy. "
            f"Adding it to an H2 or H3 (without touching the H1) could push them to page 1."
        )

    prompt = f"""You write cold email openers for an SEO agency called EthicalSEO.

Company: {company_name} ({domain})
Keyword: "{keyword}"
Current position: #{position} (page 2)
Monthly search volume: {volume:,}
CPC: ${cpc_display:.2f}
On-page finding: {onpage_obs}

Write a single cold email opening paragraph (3-4 sentences max).

Rules:
- Lead with the specific keyword and position — make it hyper-personal
- Mention the CPC to signal the commercial value they're missing
- Reference what was actually found on the page (or not found)
- If the keyword is absent from the page: pitch adding it to H2 or H3 as a quick win
- If the keyword is on the page but still page 2: pitch a backlink/authority angle instead
- End with a clear, low-pressure invitation
- NO fluff, NO "I hope this email finds you well", NO generic SEO speak
- Sound like a consultant who did their homework, not a salesperson
- First sentence must immediately reference the specific keyword and position

Output ONLY the hook paragraph, no subject line, no sign-off."""

    def _call():
        return client.messages.create(
            model=SONNET_MODEL,
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}],
        )

    try:
        msg = with_retry(_call, label=f"sonnet:{domain}")
        return msg.content[0].text.strip()
    except Exception as e:
        return f"[hook generation error: {e}]"


# ══════════════════════════════════════════════════════════════════════════════
# PER-COMPANY WORKER
# ══════════════════════════════════════════════════════════════════════════════

def process_company(company: dict, anthropic_client: anthropic.Anthropic, idx: int, total: int) -> None:
    domain       = company["domain"]
    company_name = company["company_name"]

    print(f"\n[{idx}/{total}] {company_name} ({domain})")

    result_row = {
        "company_name":   company_name,
        "domain":         domain,
        "keyword":        "",
        "position":       "",
        "cpc":            "",
        "volume":         "",
        "ranking_url":    "",
        "keyword_on_page": "",
        "found_in":       "",
        "final_hook":     "",
        "processed_at":   datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "error":          "",
    }

    try:
        # Step 1: Fetch page-2 keywords + top-10 keywords from SEMrush (single call)
        keywords, top10_keywords = fetch_page2_keywords(domain)

        if not keywords:
            print(f"    ✗ No page-2 keywords with CPC — skipping")
            result_row["error"] = "no_page2_keywords"
            append_result(result_row)
            mark_processed(domain, "skipped", total)
            return

        print(f"    ✓ Found {len(keywords)} page-2 keywords  |  {len(top10_keywords)} top-10 keywords for overlap check")

        # Step 1b: Filter junk
        keywords = filter_keywords(keywords, domain)

        if not keywords:
            print(f"    ✗ All keywords filtered as junk — skipping")
            result_row["error"] = "all_keywords_filtered"
            append_result(result_row)
            mark_processed(domain, "skipped", total)
            return

        # Step 1c: Drop page-2 keywords that overlap with top-10 rankings
        keywords = filter_top10_overlaps(keywords, top10_keywords)

        if not keywords:
            print(f"    ✗ All keywords overlap with top-10 rankings — skipping")
            result_row["error"] = "all_keywords_top10_overlap"
            append_result(result_row)
            mark_processed(domain, "skipped", total)
            return

        # Step 2: Pick best keyword with Haiku
        print(f"    → Picking best keyword with Claude Haiku...")
        best_kw = pick_best_keyword(anthropic_client, domain, keywords)

        if not best_kw:
            result_row["error"] = "no_relevant_keywords"
            append_result(result_row)
            mark_processed(domain, "skipped", total)
            return

        keyword     = best_kw["keyword"]
        position    = best_kw["best_position"]
        cpc_display = best_kw["cpc_display"]
        volume      = best_kw["volume"]
        ranking_url = best_kw["best_position_url"]

        print(f"    ✓ Best keyword: \"{keyword}\" (pos #{position}, ${cpc_display} CPC, {volume:,} vol)")

        result_row.update({
            "keyword":     keyword,
            "position":    position,
            "cpc":         f"{cpc_display:.2f}",
            "volume":      volume,
            "ranking_url": ranking_url,
        })

        # Step 3: Verify ranking URL belongs to the target domain
        url_domain = ranking_url.replace("https://", "").replace("http://", "").split("/")[0].lower()
        url_domain = url_domain.lstrip("www.")
        target_domain = domain.lower().lstrip("www.")
        if not (url_domain == target_domain or url_domain.endswith("." + target_domain)):
            print(f"    ✗ Ranking URL domain mismatch ({url_domain} ≠ {target_domain}) — skipping")
            result_row["error"] = "ranking_url_domain_mismatch"
            append_result(result_row)
            mark_processed(domain, "skipped", total)
            return

        # Step 3b: Scrape ranking page — full keyword scan
        print(f"    → Scanning page for keyword presence: {ranking_url[:70]}...")
        page_data       = scrape_page(ranking_url, keyword)
        keyword_on_page = page_data["keyword_on_page"]
        found_in        = page_data["found_in"]

        if page_data["error"]:
            print(f"    ⚠ Scrape issue: {page_data['error']} — continuing")

        result_row.update({
            "keyword_on_page": keyword_on_page,
            "found_in":        found_in,
        })

        if keyword_on_page:
            print(f"    ✓ Keyword found on page (in: {found_in}) — authority issue, not on-page")
        else:
            print(f"    ✓ Keyword ABSENT from page — on-page opportunity confirmed")

        # Step 4: Generate hook with Sonnet
        print(f"    → Writing hook with Claude Sonnet...")
        hook = generate_hook(
            client=anthropic_client,
            company_name=company_name,
            domain=domain,
            keyword=keyword,
            position=position,
            cpc_display=cpc_display,
            volume=volume,
            keyword_on_page=keyword_on_page,
            found_in=found_in,
        )
        result_row["final_hook"] = hook
        print(f"    ✓ Hook written ({len(hook)} chars)")

        append_result(result_row)
        mark_processed(domain, "success", total)

    except Exception as e:
        print(f"    [UNEXPECTED ERROR] {domain}: {e}")
        result_row["error"] = f"unexpected: {str(e)[:120]}"
        append_result(result_row)
        mark_processed(domain, "errors", total)


# ══════════════════════════════════════════════════════════════════════════════
# INPUT LOADER
# ══════════════════════════════════════════════════════════════════════════════

def load_qualified_companies(filepath: str) -> list[dict]:
    companies = []
    try:
        with open(filepath, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                domain = row.get("domain", "").strip()
                domain = domain.replace("https://", "").replace("http://", "").rstrip("/")
                if domain:
                    companies.append({
                        "company_name": row.get("company_name", row.get("Company", domain)),
                        "domain":       domain,
                    })
    except FileNotFoundError:
        print(f"[ERROR] Input file not found: {filepath}")
    return companies


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    global OUTPUT_FILE, CHECKPOINT_FILE, _processed_domains

    print("=" * 60)
    print("  EthicalSEO — Engine 2: Page 2 Sniper (parallel)")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Workers: {MAX_WORKERS} | SEMrush rate limit: {SEMRUSH_RPS} req/sec")
    print("=" * 60)

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",  default=INPUT_FILE)
    parser.add_argument("--output", default=OUTPUT_FILE)
    args_p = parser.parse_args()

    OUTPUT_FILE     = args_p.output
    CHECKPOINT_FILE = os.path.splitext(args_p.output)[0] + "_checkpoint.json"

    companies = load_qualified_companies(args_p.input)
    if not companies:
        print(f"[ABORT] No companies loaded from {args_p.input}")
        return
    print(f"\n✓ Loaded {len(companies)} companies from {args_p.input}")

    _processed_domains = load_checkpoint()
    remaining = [c for c in companies if c["domain"] not in _processed_domains]
    print(f"✓ Already processed: {len(_processed_domains)} | Remaining: {len(remaining)}")

    if not remaining:
        print("\n[DONE] All companies already processed. Check sniper_results.csv")
        return

    anthropic_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    total            = len(remaining)
    est_mins         = total * MIN_SEMRUSH_GAP / MAX_WORKERS / 60
    print(f"\n  Estimated time: ~{est_mins:.0f} minutes\n")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_company, company, anthropic_client, idx, total): company
            for idx, company in enumerate(remaining, 1)
        }
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                company = futures[future]
                print(f"  [THREAD ERROR] {company['domain']}: {e}")

    save_checkpoint()

    print("\n" + "=" * 60)
    print("  PIPELINE COMPLETE")
    print(f"  Success:  {_counters['success']}")
    print(f"  Skipped:  {_counters['skipped']}")
    print(f"  Errors:   {_counters['errors']}")
    print(f"  Output:   {OUTPUT_FILE}")
    print(f"  Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)


if __name__ == "__main__":
    main()
