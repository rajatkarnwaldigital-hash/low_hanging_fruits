import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import re
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# ── CONFIG ──────────────────────────────────────────────────────────────────
SEMRUSH_API_KEY   = "YOUR_SEMRUSH_API_KEY_HERE"
ANTHROPIC_API_KEY = "YOUR_ANTHROPIC_API_KEY_HERE"
MIN_TRAFFIC       = 500
MIN_AS            = 10
REQUEST_TIMEOUT   = 10  # increased for slow servers
DELAY_BETWEEN     = 0.3 # reduced from 1
THREADS           = 10  # parallel workers
CHECKPOINT_EVERY  = 100 # save progress every N rows

# ── SECTOR ALLOWLIST ─────────────────────────────────────────────────────────
ALLOWED_SECTORS = {
    "saas", "software", "crm", "marketing automation", "email marketing",
    "email outreach", "sales automation", "sales engagement", "sales intelligence",
    "data enrichment", "analytics", "seo", "developer tools", "no-code",
    "low-code", "automation", "integration", "cms", "customer support",
    "customer success", "onboarding", "project management", "productivity",
    "billing", "payments", "lms", "training", "enablement", "experimentation",
    "landing pages", "website builder", "blogging", "ecommerce", "log management",
    "monitoring", "error tracking", "cdp", "conversational marketing",
    "email infrastructure", "communications", "it management", "itsm",
    "spend management", "expense management", "time tracking", "forms",
    "scheduling", "notifications", "database", "cybersecurity", "fintech",
    "accounting", "bookkeeping", "financial services", "insurance",
    "consulting", "professional services", "recruitment", "hr",
    "real estate", "property management", "mortgage", "legal tech",
    "digital marketing", "seo agency", "content marketing", "pr agency",
    "ecommerce support", "proposals", "esignature", "approval workflow",
}

BLOCKED_SECTORS = {
    "healthcare it", "medtech", "medical", "life sciences", "dental",
    "govtech", "government", "nonprofit tech", "non-profit",
    "education", "university", "restaurant", "food", "retail",
    "construction", "manufacturing", "logistics", "transportation"
}

BLOG_PATHS = ["/blog", "/resources", "/insights", "/articles", "/news", "/learn", "/content", "/updates"]

# ── HELPERS ──────────────────────────────────────────────────────────────────
def normalize_domain(domain):
    domain = domain.strip().lower()
    if not domain.startswith("http"):
        domain = "https://" + domain
    return domain.rstrip("/")

def check_site_alive(url):
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}

    # build list of URLs to try: www/non-www variants + HTTP fallback
    variants = []
    from urllib.parse import urlparse
    parsed = urlparse(url)
    hostname = parsed.netloc

    if hostname.startswith("www."):
        non_www = hostname[4:]
        variants = [
            f"https://{hostname}",
            f"https://{non_www}",
            f"http://{hostname}",
            f"http://{non_www}",
        ]
    else:
        variants = [
            f"https://{hostname}",
            f"https://www.{hostname}",
            f"http://{hostname}",
            f"http://www.{hostname}",
        ]

    got_403 = False

    for variant in variants:
        # HEAD first — faster, less likely to get throttled
        try:
            r = requests.head(variant, timeout=REQUEST_TIMEOUT, allow_redirects=True, headers=headers)
            if r.status_code < 400:
                r = requests.get(variant, timeout=REQUEST_TIMEOUT, allow_redirects=True, headers=headers)
                return "alive", r
            elif r.status_code == 403:
                got_403 = True
        except Exception:
            pass

        # HEAD failed — try GET directly with one retry
        for attempt in range(2):
            try:
                r = requests.get(variant, timeout=REQUEST_TIMEOUT, allow_redirects=True, headers=headers)
                if r.status_code < 400:
                    return "alive", r
                elif r.status_code == 403:
                    got_403 = True
                break
            except Exception:
                if attempt == 0:
                    time.sleep(2)
                continue

    if got_403:
        return "bot_blocked", None
    return "dead", None

def detect_language(response):
    try:
        soup = BeautifulSoup(response.text, "html.parser")
        html_tag = soup.find("html")
        if html_tag and html_tag.get("lang"):
            return html_tag.get("lang", "").lower().startswith("en")
        meta = soup.find("meta", attrs={"http-equiv": re.compile("content-language", re.I)})
        if meta:
            return "en" in meta.get("content", "").lower()
        return True
    except Exception:
        return True

def detect_blog(base_url):
    for path in BLOG_PATHS:
        try:
            r = requests.get(base_url + path, timeout=REQUEST_TIMEOUT,
                             allow_redirects=True,
                             headers={"User-Agent": "Mozilla/5.0"})
            if r.status_code == 200 and len(r.text) > 500:
                return True, base_url + path
        except Exception:
            continue
    return False, None

def check_sector(industry):
    ind = industry.lower().strip()
    if ind in BLOCKED_SECTORS:
        return False
    return True

def is_physical_service(company, domain, industry):
    if not ANTHROPIC_API_KEY:
        return False
    prompt = f"""Does the business "{company}" ({domain}) in the "{industry}" industry require customers to physically visit a location to use their core service?
Examples of physical = YES: dental clinic, law firm, restaurant, gym, hair salon
Examples of digital = NO: accounting software, online insurance, SEO agency, SaaS tool, recruitment platform
Answer with only YES or NO."""
    try:
        response = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json"
            },
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 5,
                "messages": [{"role": "user", "content": prompt}]
            },
            timeout=REQUEST_TIMEOUT
        )
        answer = response.json()["content"][0]["text"].strip().upper()
        return answer.startswith("YES")
    except Exception:
        return False

def get_traffic(domain):
    try:
        url = (
            f"https://api.semrush.com/?type=domain_rank"
            f"&key={SEMRUSH_API_KEY}"
            f"&export_columns=Dn,Or,Ot"
            f"&domain={domain}"
            f"&database=us"
        )
        r = requests.get(url, timeout=REQUEST_TIMEOUT)
        lines = r.text.strip().split("\n")
        if len(lines) < 2:
            return None
        values = lines[1].split(";")
        return int(values[2]) if len(values) > 2 and values[2].isdigit() else None
    except Exception:
        return None

def get_authority_score(domain):
    try:
        url = (
            f"https://api.semrush.com/analytics/v1/"
            f"?key={SEMRUSH_API_KEY}"
            f"&type=backlinks_overview"
            f"&target={domain}"
            f"&target_type=root_domain"
            f"&export_columns=ascore"
        )
        r = requests.get(url, timeout=REQUEST_TIMEOUT)
        lines = r.text.strip().split("\n")
        if len(lines) < 2:
            return None
        values = lines[1].split(";")
        return int(values[0]) if len(values) > 0 and values[0].isdigit() else None
    except Exception:
        return None

# ── NORMALIZE APOLLO COLUMNS ─────────────────────────────────────────────────
def normalize_apollo_columns(df):
    col_map = {
        "Company Name": "company_name",
        "Industry"    : "industry",
        "Website"     : "domain",
        "Company City": "city",
        "# Employees" : "employees",
        "Country"     : "country",
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
    if "domain" in df.columns:
        df["domain"] = df["domain"].astype(str).apply(
            lambda x: re.sub(r"https?://(www\.)?", "", str(x)).rstrip("/").lower()
            if pd.notna(x) and str(x) not in ("nan", "") else ""
        )
    # drop rows with no domain
    df = df[df["domain"] != ""].reset_index(drop=True)
    # deduplicate by domain
    df = df.drop_duplicates(subset="domain").reset_index(drop=True)
    return df

# ── PROCESS SINGLE COMPANY ───────────────────────────────────────────────────
def process_row(args):
    i, total, row = args
    company  = str(row.get("company_name", "")).strip()
    domain   = str(row.get("domain", "")).strip()
    industry = str(row.get("industry", "")).strip()

    result = {
        "company_name"    : company,
        "domain"          : domain,
        "industry"        : industry,
        "city"            : row.get("city", ""),
        "country"         : row.get("country", ""),
        "employees"       : row.get("employees", ""),
        "site_alive"      : None,
        "is_english"      : None,
        "sector_ok"       : None,
        "is_digital"      : None,
        "has_blog"        : None,
        "blog_url"        : None,
        "authority_score" : None,
        "monthly_traffic" : None,
        "semrush_pending" : False,
        "qualified"       : False,
        "fail_reason"     : ""
    }

    if not domain:
        result["fail_reason"] = "no_domain"
        return i, result, f"[{i}/{total}] {company} ... ❌ no domain"

    base_url = normalize_domain(domain)

    # FILTER 1: Sector
    if not check_sector(industry):
        result["sector_ok"] = False
        result["fail_reason"] = "wrong_sector"
        return i, result, f"[{i}/{total}] {company} ({domain}) ... ❌ wrong sector"
    result["sector_ok"] = True

    # FILTER 2: Digital check
    physical = is_physical_service(company, domain, industry)
    result["is_digital"] = not physical
    if physical:
        result["fail_reason"] = "physical_service"
        return i, result, f"[{i}/{total}] {company} ({domain}) ... ❌ physical service"

    # FILTER 3: Site alive
    site_status, response = check_site_alive(base_url)
    result["site_alive"] = site_status == "alive"
    if site_status == "bot_blocked":
        result["fail_reason"] = "bot_blocked"
        return i, result, f"[{i}/{total}] {company} ({domain}) ... 🤖 bot blocked"
    if site_status == "dead":
        result["fail_reason"] = "dead_site"
        return i, result, f"[{i}/{total}] {company} ({domain}) ... ❌ dead site"

    # FILTER 4: Language
    is_english = detect_language(response)
    result["is_english"] = is_english
    if not is_english:
        result["fail_reason"] = "non_english"
        return i, result, f"[{i}/{total}] {company} ({domain}) ... ❌ non-English"

    # FILTER 5: Blog
    has_blog, blog_url = detect_blog(base_url)
    result["has_blog"] = has_blog
    result["blog_url"] = blog_url
    if not has_blog:
        result["fail_reason"] = "no_blog"
        return i, result, f"[{i}/{total}] {company} ({domain}) ... ❌ no blog"

    # FILTER 6: SEMrush traffic
    if not SEMRUSH_API_KEY:
        result["semrush_pending"] = True
        result["qualified"] = True
        return i, result, f"[{i}/{total}] {company} ({domain}) ... ⏳ SEMrush pending"

    time.sleep(DELAY_BETWEEN)
    traffic = get_traffic(domain)
    result["monthly_traffic"] = traffic

    if traffic is not None and traffic < MIN_TRAFFIC:
        result["fail_reason"] = "low_traffic"
        return i, result, f"[{i}/{total}] {company} ({domain}) ... ❌ low traffic ({traffic})"

    # FILTER 7: Authority Score
    time.sleep(DELAY_BETWEEN)
    ascore = get_authority_score(domain)
    result["authority_score"] = ascore

    if ascore is not None and ascore < MIN_AS:
        result["fail_reason"] = "low_authority_score"
        return i, result, f"[{i}/{total}] {company} ({domain}) ... ❌ low AS ({ascore})"

    result["qualified"] = True
    return i, result, f"[{i}/{total}] {company} ({domain}) ... ✅ qualified (AS:{ascore} Traffic:{traffic})"

# ── MAIN ENGINE ──────────────────────────────────────────────────────────────
def qualify(input_csv, output_csv):
    df = pd.read_csv(input_csv)
    df = normalize_apollo_columns(df)
    total = len(df)
    print(f"\n📥 Loaded {total} companies (after dedup + empty domain removal)\n")

    # check for existing checkpoint
    checkpoint_file = output_csv + ".checkpoint"
    done_domains = set()
    results = []
    if os.path.exists(checkpoint_file):
        existing = pd.read_csv(checkpoint_file)
        done_domains = set(existing["domain"].tolist())
        results = existing.to_dict("records")
        print(f"♻️  Resuming from checkpoint — {len(done_domains)} already processed\n")

    # filter out already processed
    df = df[~df["domain"].isin(done_domains)].reset_index(drop=True)
    remaining = len(df)
    print(f"🔄 Processing {remaining} remaining companies with {THREADS} threads\n")

    lock = Lock()
    processed = 0

    args_list = [(i + len(done_domains) + 1, total, row) for i, row in df.iterrows()]

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        futures = {executor.submit(process_row, args): args for args in args_list}
        for future in as_completed(futures):
            try:
                idx, result, log = future.result()
                with lock:
                    results.append(result)
                    processed += 1
                    print(log)
                    # checkpoint every N rows
                    if processed % CHECKPOINT_EVERY == 0:
                        pd.DataFrame(results).to_csv(checkpoint_file, index=False)
                        print(f"\n💾 Checkpoint saved ({processed}/{remaining})\n")
            except Exception as e:
                print(f"Error: {e}")

    out_df = pd.DataFrame(results)
    out_df.to_csv(output_csv, index=False)

    # remove checkpoint on success
    if os.path.exists(checkpoint_file):
        os.remove(checkpoint_file)

    total_out = len(out_df)
    qualified = int(out_df["qualified"].sum())
    failed    = total_out - qualified

    print(f"\n{'─'*50}")
    print(f"✅ Qualified      : {qualified}")
    print(f"❌ Filtered out   : {failed}")
    print(f"📊 Total          : {total_out}")
    print(f"\nFail breakdown:")
    fails = out_df[out_df["fail_reason"] != ""]["fail_reason"].value_counts()
    for reason, count in fails.items():
        print(f"  {reason}: {count}")
    print(f"\n💾 Output saved to: {output_csv}")

if __name__ == "__main__":
    qualify(
        input_csv  = "apollo_export.csv",
        output_csv = "qualified_companies.csv"
    )
