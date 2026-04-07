# Bourbon Scout

Automated inventory tracker for allocated and rare bourbon. Monitors 7 major retailers near your zip code and sends real-time Discord alerts with SKU/item numbers, store details, and stock status changes when bottles are spotted or go out of stock.

## Tracked Bottles (44)

Blanton's (Original, Gold, SFTB, Special Reserve), Weller (Special Reserve, Antique 107, 12 Year, Full Proof, Single Barrel, C.Y.P.B.), E.H. Taylor (Small Batch, Single Barrel, Barrel Proof, Straight Rye, Seasoned Wood, Four Grain, Amaranth, Cured Oak, 18 Year Marriage), Stagg Jr, BTAC (George T. Stagg, Eagle Rare 17, William Larue Weller, Thomas H. Handy), Pappy Van Winkle (10/12/15/20/23), Van Winkle Family Reserve Rye 13, Elmer T. Lee, Rock Hill Farms, King of Kentucky, Old Forester (Birthday Bourbon, President's Choice, 150th Anniversary, King Ranch), Michter's 10 Year, Penelope (Founder's Reserve, Estate Collection), Jack Daniel's (10 Year, 12 Year, 14 Year), Buffalo Trace (canary health check).

Some bottles are retailer-restricted (e.g., Michter's 10 Year and Penelope only match at Costco, Total Wine, and Walmart).

## Supported Retailers

| Retailer | Method | Data Source |
|----------|--------|-------------|
| Costco | Fetch-first (got-scraping), clean browser fallback | MUI `data-testid` attributes |
| Total Wine | Clean browser (headed on Mac) | `window.INITIAL_STATE` JSON |
| Walmart | Fetch-first (got-scraping), clean browser fallback | `__NEXT_DATA__` JSON |
| Kroger | REST API | Structured JSON |
| Safeway | REST API | Structured JSON |
| Walgreens | Clean browser (headed on Mac) | Server-rendered HTML (CSS selectors) |
| Sam's Club | Fetch-first (got-scraping), clean browser fallback | `__NEXT_DATA__` JSON (per-product) |

## How It Works

1. **Store Discovery** — On startup, auto-discovers nearby stores for each retailer based on your zip code and search radius. Results are cached for 7 days. Falls back to static store data if browser-based locators fail (e.g., on CI).
2. **Inventory Scanning** — Scans all stores concurrently (limit 8) using 15 broad search queries that cover all 44 bottles. Each retailer gets a dedicated residential IP via per-retailer sticky proxy sessions. If a proxy IP gets blocked mid-scan, it automatically rotates to a fresh IP after 2 consecutive failures. Retailers are staggered 10-30s apart. All 5 browser scrapers use "clean" Chrome — plain `chromium.launch()` without stealth plugin or rebrowser-patches (these CDP modifications are actually fingerprinted by anti-bot systems). On Mac, browsers run headed and minimized via CDP `Browser.setWindowBounds`. 4 of 5 scrapers try `got-scraping` fetch-first (Chrome TLS fingerprint impersonation via JA3/JA4 spoofing) before falling back to browser; Total Wine goes straight to browser (PerimeterX requires JS sensor execution). Persistent browser profiles per retailer (HTTP cache, service workers, IndexedDB persist on disk). Browser scrapers follow a human-like flow: homepage → category page → search queries. Includes a PerimeterX "Press & Hold" challenge solver with retry, micro-movements during hold, and button render wait. Known bottle URLs from previous finds are checked directly before searching. Includes a canary bottle (Buffalo Trace) as a scraper health check. Retailers that fail 3 consecutive scans are automatically backed off for 30 minutes.
3. **State Tracking** — Tracks stock changes between scans: new finds, still in stock, and gone out of stock. Persists `firstSeen` timestamps and scan counts per bottle per store.
4. **Discord Alerts** — Sends color-coded embeds based on stock changes: green `@here` for new finds, orange for OOS losses, blue re-alerts for bottles still in stock, and a purple summary after every scan. Summary includes per-scraper health metrics with canary indicators and a 24h trend (per-retailer success rate, canary hit rate, bottles found). Includes SKU/item numbers, store numbers, fulfillment info, and Google Maps links.
5. **Scan Metrics** — Appends one JSON line per scan to `metrics.jsonl` with per-retailer health stats, canary hits, bottles found, and duration. Enables historical trend analysis: retailer reliability over time, drop patterns, proxy ROI.
6. **Reddit Intel** — Monitors r/ArizonaWhiskey and r/arizonabourbon for community-reported drops. Posts matching bottle names, store names, or drop keywords (kok, btac, pappy, allocated, drop, etc.) trigger @here gold Discord embeds with post details and links. No proxy or auth needed — uses Reddit's public JSON API.
7. **Watch List** — Manual human intelligence system for rumored drops. Add entries to `WATCH_LIST` in scraper.js with retailer, store IDs, and source info. Triggers a one-time @here gold Discord notification listing all tracked bottles at that retailer. For tips from private Facebook groups, phone calls, or store visits.

## Setup

### Prerequisites

- Node.js 18+
- A Discord webhook URL

### Install

```sh
git clone <repo-url> && cd bourbon-scout
npm install
npx playwright install chromium
```

### Configure

Create a `.env` file:

```env
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...
ZIP_CODE=85283
SEARCH_RADIUS_MILES=15
MAX_STORES_PER_RETAILER=5
POLL_INTERVAL=*/15 * * * *
REALERT_EVERY_N_SCANS=4
KROGER_CLIENT_ID=
KROGER_CLIENT_SECRET=
SAFEWAY_API_KEY=
```

| Variable | Required | Description |
|----------|----------|-------------|
| `DISCORD_WEBHOOK_URL` | Yes | Discord channel webhook for alerts |
| `ZIP_CODE` | Yes | Your zip code for store discovery |
| `SEARCH_RADIUS_MILES` | No | Search radius in miles (default: 15) |
| `MAX_STORES_PER_RETAILER` | No | Max stores per retailer chain (default: 5) |
| `POLL_INTERVAL` | No | Base poll interval as cron expression (default: `*/15 * * * *`). Parsed to minutes; actual intervals vary ±3 min with jitter. |
| `REALERT_EVERY_N_SCANS` | No | Re-alert interval for bottles still in stock (default: 4, meaning every ~1hr at 15-min polls) |
| `KROGER_CLIENT_ID` | No | Kroger API client ID (get from [developer.kroger.com](https://developer.kroger.com)) |
| `KROGER_CLIENT_SECRET` | No | Kroger API client secret |
| `SAFEWAY_API_KEY` | No | Safeway product search API key |
| `PROXY_URL` | No | Residential proxy URL (e.g. `http://user:pass@gw.dataimpulse.com:823`). Each retailer gets a dedicated IP via per-retailer sticky sessions. DataImpulse ($1/GB) recommended. |
| `BACKUP_PROXY_URL` | No | Backup proxy URL (e.g. IPRoyal). Used by retailers listed in `BACKUP_PROXY_RETAILERS` when primary proxy IPs are burned. |
| `BACKUP_PROXY_RETAILERS` | No | Comma-separated retailer keys to route through backup proxy (e.g. `costco,totalwine`). |

Kroger and Safeway scrapers are skipped if their API keys aren't provided. All other retailers work without credentials. Without `PROXY_URL`, all scrapers fall back to browser-only mode. With `PROXY_URL` set, each retailer gets its own sticky session IP and `got-scraping` fetch-first paths use Chrome TLS fingerprint impersonation for faster, lighter scraping. All browser fallbacks use clean Chrome (no stealth plugin — it's counterproductive) with `headless: false` on Mac. Queries use priority-based rotation — high-value bottles (BTAC, Pappy, Taylor, Michter's) are checked every scan while lower-priority queries alternate, with human-like pacing to reduce bot detection signals. Retailers that fail 3+ consecutive scans are automatically backed off for 30 minutes.

### Run

```sh
npm start        # Discover stores + start polling
npm run dev      # Same with --watch for auto-restart
```

### GitHub Actions (CI)

The included workflow runs every 15 minutes via GitHub Actions cron. It uses `RUN_ONCE=true` for single-run mode and caches Playwright browsers and state files between runs.

Set these repository secrets:
- `DISCORD_WEBHOOK_URL` (required)
- `KROGER_CLIENT_ID`, `KROGER_CLIENT_SECRET` (optional)
- `SAFEWAY_API_KEY` (optional)

Manual trigger: `gh workflow run "Bourbon Scout"`

## Discord Alerts

Four types of color-coded embeds with per-retailer SKU/item numbers and rich store info:

**🟢 New Find** (green, `@everyone` ping)
- Triggered when a bottle is spotted for the first time at a store
- Shows product name with link, price, SKU/item number, bottle size, and fulfillment
- Lists any bottles still in stock from previous scans

**🟠 Stock Lost** (orange, quiet)
- Triggered when a previously in-stock bottle is no longer found
- Shows which bottles went OOS, their last known price, and how long they were in stock

**🔵 Still Available** (blue, quiet)
- Re-alert for bottles that remain in stock across multiple scans
- Sent every N scans (configurable via `REALERT_EVERY_N_SCANS`, default 4)

**🟣 Scan Summary** (purple, quiet)
- Posted after every scan with counts: new finds, still in stock, went OOS, nothing found
- Shows total stores, retailers, and scan duration
- All 7 retailers always shown: ✅ (≥75% success), ⚠️ (25-74%), ❌ (<25%), ⏭️ (skipped/no data), with 🐤 canary indicator
- When no allocations are found, lists all scanned stores grouped by retailer

Each embed includes:
- Store name, number, city/state, and distance
- Google Maps link to store address
- Retailer-specific SKU labels (Item # for Costco/Total Wine/Walmart/Sam's Club, SKU for Kroger, UPC for Safeway)

## Generated Files

| File | Purpose | Safe to Delete? |
|------|---------|-----------------|
| `stores.json` | Cached store discovery results | Yes — triggers re-discovery on next startup |
| `state.json` | Per-store stock state with timestamps and scan counts | Yes — treats all bottles as new finds on next poll |
| `browser-state.json` | Playwright browser cookies/storage (reduces bot detection) | Yes — cookies will re-accumulate on next poll |
| `browser-profiles/` | Per-retailer persistent Chrome profiles (HTTP cache, service workers, IndexedDB) | Yes — profiles rebuild from scratch on next poll |
| `debug/` | Screenshots and HTML from store locator pages | Yes — regenerate with `node debug-locators.js` |

## Debugging Store Locators

If a retailer stops finding stores, the selectors likely need updating:

```sh
node debug-locators.js
```

This captures screenshots, full HTML, and selector lists from each retailer's store locator page into `debug/`. Compare against the selectors in `lib/discover-stores.js` and update as needed.

## Tests

791 tests across 5 files using [Vitest](https://vitest.dev/) (95.8% line coverage, 83.9% branch):

```sh
npm test                # Run all tests
npm test -- --coverage  # With coverage report
```

| File | Tests | Focus |
|------|-------|-------|
| `test/scraper.test.js` | 669 | Bottle matching, per-retailer filtering, EXCLUDE_TERMS, miniature filter, price ceiling ($500), all 7 scrapers, Discord embeds (re-alert/OOS/health emoji/canary/trend), poll orchestration, error isolation, health tracking, scan metrics/trends, retry mechanisms, bot detection (isFetchBlocked 8 patterns + 10K limit), state management, browser mutex, proxy rotation, challenge solver, schedule-aware polling, known URL tracking, priority-based query rotation, Chrome header order/GREASE brand, env validation, Discord 5xx/network retry, searchTerm coverage (Old Rip/Colonel/SFB), parseSize liter/litre, normalizeText unicode, truncateDescription OOS, dedupFound N/A, shuffle/withTimeout/runWithConcurrency edges, peak hour formatting, watch list (key generation/embed/processing), Reddit intel (keywords/subreddits/scraping/dedup), shuffleKeepCanaryFirst, proxy availability/failover, Safeway browser wrapper, handleShutdown, poll integration (store scanning/canary retry), Walmart/Sam's Club fetch paths |
| `test/proxy.test.js` | 35 | Proxy routing, SOCKS5/HTTP auto-detection, fetch-first paths, Costco blocked retry, rotateRetailerProxy (port change/isolation/dynamic URL) |
| `test/discover-stores.test.js` | 69 | Store locator logic per retailer, store name sanitization |
| `test/geo.test.js` | 9 | Zip-to-coords, haversine distance |
| `test/fallback-stores.test.js` | 9 | Static fallback store data validation, EXTRA_STORES structure and dedup |
