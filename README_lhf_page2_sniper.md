# lhf-page2-sniper

Identifies SaaS companies ranking on page 2 of Google for high-value commercial keywords and generates personalised outreach hooks. Part of the EthicalSEO outbound engine.

---

## What it does

Takes a raw Apollo export of SaaS companies and runs them through a 2-stage pipeline:

1. **Qualify** — filters the raw list down to companies worth contacting (live site, English, has a blog, 500+ monthly visits, Authority Score ≥10)
2. **Snipe** — for each qualified company, finds keywords they rank positions 11–20 for with CPC ≥$2, checks whether the keyword is actually missing from their ranking page, picks the best opportunity via Claude Haiku, and writes a personalised cold email hook via Claude Sonnet

The signal logic is: a company ranking page 2 for a commercial keyword is already doing SEO and almost converting that traffic — a small nudge (adding the keyword to an H2/H3, or building a few backlinks) could push them to page 1. That gap is what the outreach is built around.

The end result is a CSV with one row per company, containing the target keyword, position, CPC, ranking URL, whether the keyword was found on the page, and a ready-to-use outreach hook.

---

## Scripts

### 1. `qualify.py`

Filters a raw Apollo export down to qualified prospects.

**Input:** `apollo_export.csv`

The script expects a CSV exported directly from Apollo. The following columns must be present with these exact header names:

| Column | Required | Notes |
|--------|----------|-------|
| `Company Name` | Yes | Used in hook generation and output |
| `Website` | Yes | Primary filter field — rows with no website are dropped |
| `Industry` | Yes | Used for sector blocklist and physical service check |
| `Company City` | No | Passed through to output |
| `# Employees` | No | Passed through to output |
| `Country` | No | Passed through to output |

**How to export from Apollo:** Companies tab → select your filtered list → Export → CSV → choose "All fields" or at minimum the columns above.

The script automatically cleans the `Website` column — strips `https://`, `www.`, and trailing slashes, and deduplicates by domain. You do not need to pre-clean the URLs.

**Example input row:**
```
Company Name,Website,Industry,Company City,# Employees,Country
Acme Analytics,https://www.acmeanalytics.com,SaaS,London,51-200,United Kingdom
```

**Output:** `qualified_companies.csv`

Output columns:
| Column | Description |
|--------|-------------|
| `company_name` | Company name |
| `domain` | Cleaned domain (no https, no www, no trailing slash) |
| `industry` | Industry label |
| `city`, `country`, `employees` | Passed through from input |
| `site_alive` | `True` if site responded |
| `is_english` | `True` if `<html lang>` is English |
| `sector_ok` | `True` if not in blocked sector list |
| `is_digital` | `True` if Claude Haiku determined no physical visit required |
| `has_blog` | `True` if a blog path returned 200 with content |
| `blog_url` | The exact blog path that resolved |
| `authority_score` | SEMrush Authority Score |
| `monthly_traffic` | SEMrush organic traffic (US database) |
| `semrush_pending` | `True` if SEMrush key was missing at run time |
| `qualified` | `True` if all filters passed |
| `fail_reason` | Why the company was filtered out (if applicable) |

**Filters applied (in order):**
1. Sector blocklist check (healthcare, government, education, food, construction, etc.)
2. Physical service check — Claude Haiku YES/NO prompt
3. Site alive — tries 4 URL variants (https/http × www/non-www), HEAD then GET fallback
4. Language detection — reads `<html lang>` attribute
5. Blog detection — checks 8 paths: `/blog`, `/resources`, `/insights`, `/articles`, `/news`, `/learn`, `/content`, `/updates`
6. Organic traffic ≥ 500/month (SEMrush `domain_rank`, US database)
7. Authority Score ≥ 10 (SEMrush `backlinks_overview`)

**SEMrush cost:** ~20 units per company that reaches filters 6–7

**Performance:** Runs 10 parallel workers via `ThreadPoolExecutor`. Checkpoints every 100 rows to `qualified_companies.csv.checkpoint` — safe to interrupt and resume.

---

### 2. `page2_sniper.py`

Finds the best page 2 keyword opportunity per domain and writes a personalised hook.

**Input:** `qualified_companies.csv` — reads `company_name` and `domain` columns; only processes rows where `qualified == True` is not required by the script itself, so feed it the filtered output from qualify.py

> **Note:** `page2_sniper.py` reads `company_name` and `domain` from the input file. It accepts either the full qualify output or a trimmed CSV with just those two columns.

**Output:** `sniper_results.csv`

Output columns:
| Column | Description |
|--------|-------------|
| `company_name` | Company name |
| `domain` | Domain |
| `keyword` | The target page 2 keyword selected for this company |
| `position` | Current ranking position (11–20) |
| `cpc` | Cost-per-click in USD (e.g. `4.20`) |
| `volume` | Monthly search volume |
| `ranking_url` | The specific page on their site ranking for this keyword |
| `keyword_on_page` | `True` if the keyword was found anywhere on the ranking page |
| `found_in` | Where the keyword was found: `title`, `h1`, `h2`, `h3`, `body`, `multiple`, or blank if absent |
| `final_hook` | Personalised 3–4 sentence cold email opening |
| `processed_at` | Timestamp |
| `error` | Populated if the company was skipped — see skip reasons below |

**Signal logic (per company, in order):**

1. **Fetch keywords** — pulls top 20 organic keywords from SEMrush (`domain_organic`, US database, sorted by volume). Splits client-side into page 2 candidates (positions 11–20, CPC > 0) and top-10 keywords (positions 1–10, used for overlap check only)

2. **Junk filter** — drops keywords containing login/signup/support/tracking/coupon/download terms, single-word keywords, CPC < $2, informational prefixes (what is / how to / why does etc.), and brand-navigation queries

3. **Top-10 overlap filter** — drops any page 2 keyword that shares ≥60% of its meaningful words with a keyword the company already ranks top 10 for. Prevents pitching a keyword they've already captured under a slightly different phrasing

4. **Keyword picker (Claude Haiku)** — from the remaining candidates, picks the one with the highest commercial intent that directly describes the company's core product. Hard-rejects keywords for third-party tools the company merely integrates with, competitor brand names, compliance topics, blog-only content, and AWS/GCP/Azure product names

5. **Page scraper** — fetches the actual ranking URL and checks whether the keyword appears in: `<title>`, `<h1>`, any `<h2>`, any `<h3>`, or body text

6. **Hook writer (Claude Sonnet)** — writes a 3–4 sentence hook. Two variants depending on the scrape result:
   - **Keyword absent from page** → pitches adding it to an H2 or H3 as a quick on-page win
   - **Keyword present but still page 2** → pivots to a backlink/authority angle instead

**Skip reasons (written to the `error` column):**
| Error | Meaning |
|-------|---------|
| `no_page2_keywords` | No keywords found in positions 11–20 with CPC > 0 |
| `all_keywords_filtered` | All keywords were junk-filtered |
| `all_keywords_top10_overlap` | All keywords overlapped with existing top-10 rankings |
| `no_relevant_keywords` | Haiku rejected all candidates as irrelevant to the company's product |
| `ranking_url_domain_mismatch` | The ranking URL returned by SEMrush belonged to a different domain |

**SEMrush cost:** 200 units per company (single `domain_organic` call, display_limit=20, 10 units/line)

**Performance:** 5 parallel workers. Thread-safe rate limiter keeps SEMrush calls under 8 req/sec. Exponential backoff (2, 4, 8, 16s) on 429/5xx errors from both SEMrush and Anthropic. Checkpoints every 100 companies to `sniper_results_checkpoint.json`.

---

## API Keys Required

| Key | Variable name | Used in |
|-----|--------------|---------|
| SEMrush API key | `SEMRUSH_API_KEY` | `qualify.py`, `page2_sniper.py` |
| Anthropic API key | `ANTHROPIC_API_KEY` | `qualify.py`, `page2_sniper.py` |

Set both at the top of each script in the `CONFIG` / `API KEYS` block.

> **Note:** The SEMrush key is IP-whitelisted. Run locally or from a consistent IP. Do not run from cloud VMs with dynamic IPs.

---

## Installation

```bash
pip install pandas requests beautifulsoup4 anthropic
```

Python 3.9+ required.

---

## Usage

### Step 1 — Prepare your input and run qualify.py

Export your company list from Apollo as a CSV and save it as `apollo_export.csv` in the same folder as the scripts. Required columns: `Company Name`, `Website`, `Industry`.

```bash
python qualify.py
```

Produces `qualified_companies.csv`. On a 5,000-company list expect ~30–45 minutes with 10 threads.

### Step 2 — Run page2_sniper.py

```bash
python page2_sniper.py
```

Default input is `qualified_companies.csv`, default output is `sniper_results.csv`. Both can be overridden via flags:

```bash
python page2_sniper.py --input qualified_companies.csv --output sniper_results.csv
```

On 2,000 qualified companies expect ~2–3 hours (5 workers, rate-limited to 8 SEMrush req/sec) and ~400k SEMrush units.

---

## Checkpointing and Resume

Both scripts are safe to interrupt mid-run.

| Script | Checkpoint file | How to resume |
|--------|----------------|---------------|
| `qualify.py` | `qualified_companies.csv.checkpoint` | Re-run — already-processed domains skipped automatically |
| `page2_sniper.py` | `sniper_results_checkpoint.json` | Re-run — already-processed domains skipped automatically |

Checkpoint files are deleted automatically on successful completion of `qualify.py`. The sniper checkpoint persists so you can inspect it.

---

## SEMrush Unit Consumption (per full run)

| Stage | Units per domain | On 5,000 companies |
|-------|-----------------|-------------------|
| `qualify.py` | ~20 units (traffic + AS) | ~50,000 units |
| `page2_sniper.py` | 200 units (top 20 keywords) | ~400,000 units (on ~2,000 qualified) |
| **Total** | | **~450,000 units** |

---

## Known Limitations

- `page2_sniper.py` fetches the top 20 keywords sorted by volume and filters client-side. If a domain has many high-volume informational keywords, the page 2 commercial candidates may not appear in the top 20 — in which case the company is skipped with `no_page2_keywords`.
- The page scraper checks for exact keyword match (lowercase). Partial matches and stemming variants are not detected — a keyword like "project management tools" will not match a page that says "project management tool" (singular).
- Bot-protected pages (Cloudflare, JS-rendered) return a scrape error but the hook is still generated using the keyword and SEMrush data alone.
- The Haiku keyword picker can be conservative — it hard-rejects keywords for integrations and blog-only content, which is intentional but may reduce yield on companies with broad content strategies.
- Hook generation uses Claude Sonnet (`claude-sonnet-4-6`). Hooks reference the specific keyword, position, CPC, and on-page finding — review a sample before bulk sending.

---

## File Structure

```
lhf-page2-sniper/
├── qualify.py                    # Stage 1: filter Apollo export
├── page2_sniper.py               # Stage 2: find keyword gaps + write hooks
├── apollo_export.csv             # Your input file (gitignored)
├── qualified_companies.csv       # Output of Stage 1 (gitignored)
└── sniper_results.csv            # Output of Stage 2 — load into Plusvibe
```

---

## Plusvibe Sequence (Reference)

Load `sniper_results.csv` into Plusvibe. Map the `final_hook` column to `{{hook}}` in the email copy.

| Step | Day | Purpose |
|------|-----|---------|
| Email 1 | Day 1 | Hook-led opener — `{{hook}}` as the first paragraph |
| Email 2 | Day 4 | Follow-up — restate the keyword opportunity, add social proof |
| Email 3 | Day 9 | Short nudge — one line + CTA |
| Email 4 | Day 14 | Break-up email |

Proof points to use in copy: Wallester (800% organic growth in 12 months), Vespia (10x traffic, acquired by Veriff), Remofirst (136 backlinks, 478% overall growth, $170K/year traffic value).
