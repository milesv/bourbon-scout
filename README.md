# Bourbon Scout

Automated inventory tracker for allocated and rare bourbon. Monitors 7 major retailers near your zip code and sends real-time Discord alerts with SKU/item numbers, store details, and stock status changes when bottles are spotted or go out of stock.

## Tracked Bottles (39)

Blanton's (Original, Gold, SFTB, Special Reserve), Weller (Special Reserve, Antique 107, 12 Year, Full Proof, Single Barrel, C.Y.P.B.), E.H. Taylor (Small Batch, Single Barrel, Barrel Proof, Straight Rye, Seasoned Wood, Four Grain, Amaranth, Cured Oak, 18 Year Marriage), Stagg Jr, BTAC (George T. Stagg, Eagle Rare 17, William Larue Weller, Thomas H. Handy, Sazerac Rye 18), Pappy Van Winkle (10/12/15/20/23), Van Winkle Family Reserve Rye 13, Elmer T. Lee, Rock Hill Farms, King of Kentucky, Old Forester (Birthday Bourbon, President's Choice, 150th Anniversary, King Ranch), Buffalo Trace (canary health check).

## Supported Retailers

| Retailer | Method | Data Source |
|----------|--------|-------------|
| Costco | Dedicated browser (per-retailer IP) | MUI `data-testid` attributes |
| Total Wine | Dedicated browser (per-retailer IP) | `window.INITIAL_STATE` JSON |
| Walmart | Fetch-first, browser fallback | `__NEXT_DATA__` JSON |
| Kroger | REST API | Structured JSON |
| Safeway | REST API | Structured JSON |
| Walgreens | Dedicated browser (per-retailer IP) | Server-rendered HTML (CSS selectors) |
| Sam's Club | Fetch-first, browser fallback | `__NEXT_DATA__` JSON (per-product) |

## How It Works

1. **Store Discovery** — On startup, auto-discovers nearby stores for each retailer based on your zip code and search radius. Results are cached for 7 days. Falls back to static store data if browser-based locators fail (e.g., on CI).
2. **Inventory Scanning** — Scans all stores concurrently (limit 8) using 14 broad search queries that cover all 39 bottles. Each retailer gets a dedicated residential IP via per-retailer sticky proxy sessions. Costco, Total Wine, and Walgreens use dedicated browser instances (Akamai/PerimeterX block plain fetch). Walmart and Sam's Club try fetch-first (`__NEXT_DATA__` extraction) with browser fallback. All browser scrapers pre-warm the homepage with `networkidle` + randomized dwell time to let anti-bot sensors execute. Sam's Club uses per-product-URL scraping since its search excludes spirits. Includes a canary bottle (Buffalo Trace) as a scraper health check.
3. **State Tracking** — Tracks stock changes between scans: new finds, still in stock, and gone out of stock. Persists `firstSeen` timestamps and scan counts per bottle per store.
4. **Discord Alerts** — Sends color-coded embeds based on stock changes: green `@everyone` for new finds, orange for OOS losses, blue re-alerts for bottles still in stock, and a purple summary after every scan. Summary includes per-scraper health metrics with canary indicators. Includes SKU/item numbers, store numbers, fulfillment info, and Google Maps links.

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
| `POLL_INTERVAL` | No | Cron expression for poll frequency (default: every 15 min) |
| `REALERT_EVERY_N_SCANS` | No | Re-alert interval for bottles still in stock (default: 4, meaning every ~1hr at 15-min polls) |
| `KROGER_CLIENT_ID` | No | Kroger API client ID (get from [developer.kroger.com](https://developer.kroger.com)) |
| `KROGER_CLIENT_SECRET` | No | Kroger API client secret |
| `SAFEWAY_API_KEY` | No | Safeway product search API key |
| `PROXY_URL` | No | Residential proxy URL (e.g. `http://user:pass@gw.dataimpulse.com:823`). Each retailer gets a dedicated IP via per-retailer sticky sessions. DataImpulse ($1/GB) recommended. |
| `BACKUP_PROXY_URL` | No | Backup proxy URL (e.g. IPRoyal). Used by retailers listed in `BACKUP_PROXY_RETAILERS` when primary proxy IPs are burned. |
| `BACKUP_PROXY_RETAILERS` | No | Comma-separated retailer keys to route through backup proxy (e.g. `costco,totalwine`). |

Kroger and Safeway scrapers are skipped if their API keys aren't provided. All other retailers work without credentials. Without `PROXY_URL`, Costco, Total Wine, and Sam's Club use browser-only scraping; Walmart's fetch path skips on CI but works locally. With `PROXY_URL` set, each retailer gets its own sticky session IP. Queries are rotated across scans (half per scan) with human-like pacing to reduce bot detection signals.

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
| `debug/` | Screenshots and HTML from store locator pages | Yes — regenerate with `node debug-locators.js` |

## Debugging Store Locators

If a retailer stops finding stores, the selectors likely need updating:

```sh
node debug-locators.js
```

This captures screenshots, full HTML, and selector lists from each retailer's store locator page into `debug/`. Compare against the selectors in `lib/discover-stores.js` and update as needed.
