# Bourbon Scout

Automated inventory tracker for allocated and rare bourbon. Monitors 6 major retailers near your zip code and sends real-time Discord reports as each store is scanned.

## Tracked Bottles (25)

Blanton's (Gold, Straight from the Barrel, Special Reserve, Red), Weller (Special Reserve, Antique 107, 12 Year, Full Proof, Single Barrel, C.Y.P.B.), E.H. Taylor Small Batch, Stagg Jr, BTAC (George T. Stagg, Eagle Rare 17, William Larue Weller, Thomas H. Handy, Sazerac Rye 18), Pappy Van Winkle (10/12/15/20/23), Elmer T. Lee, Rock Hill Farms, King of Kentucky.

## Supported Retailers

Costco, Total Wine, Walmart, Kroger (including Fry's, Ralph's, etc.), Safeway, BevMo.

## How It Works

1. **Store Discovery** — On startup, auto-discovers nearby stores for each retailer based on your zip code and search radius. Results are cached for 7 days.
2. **Inventory Polling** — Every 15 minutes (configurable), scrapes each discovered store for the target bottles.
3. **Real-Time Discord Reports** — Each store's results are sent to Discord immediately after scanning, showing in-stock and out-of-stock bottles with store name, address, and distance.

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

Copy `.env` and fill in your values:

```env
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...
ZIP_CODE=85283
SEARCH_RADIUS_MILES=15
MAX_STORES_PER_RETAILER=5
POLL_INTERVAL=*/15 * * * *
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
| `KROGER_CLIENT_ID` | No | Kroger API client ID (get from [developer.kroger.com](https://developer.kroger.com)) |
| `KROGER_CLIENT_SECRET` | No | Kroger API client secret |
| `SAFEWAY_API_KEY` | No | Safeway product search API key |

Kroger and Safeway scrapers are skipped if their API keys aren't provided. All other retailers work without credentials.

### Run

```sh
npm start
```

On first run you'll see store discovery then polling output:

```
Bourbon Scout 🥃 starting up...
[discover] Discovering stores near 85283 within 15 miles...
[discover] Zip 85283 → 33.3665, -111.9312
[discover] Locating costco stores...
[discover] Found 5 costco store(s)
[discover] Locating totalwine stores...
[discover] Found 5 totalwine store(s)
[discover] Locating walmart stores...
[discover] Found 5 walmart store(s)
...
[discover] Discovery complete — 20 stores cached to stores.json
Tracking 25 bottles across 20 stores
Poll schedule: */15 * * * *

[poll] Starting scan at 2026-03-07T02:44:52.608Z
[poll] Checking Costco — Costco Chandler...
[costco:736] In stock: none
[poll] Checking Costco — Costco Gilbert...
```

Each store result is sent to Discord immediately as it completes — you'll see embeds stream in during the poll. Subsequent runs reuse the cached `stores.json` until the cache expires (7 days) or your zip/radius changes.

## Discord Reports

Each poll sends one Discord embed per store showing:
- Store name and distance from your zip
- Store address
- In-stock bottles (highlighted individually)
- Out-of-stock bottles (compact comma-separated list)

Embeds are color-coded: green if anything is in stock, gray if nothing found.

## Generated Files

| File | Purpose | Safe to Delete? |
|------|---------|-----------------|
| `stores.json` | Cached store discovery results | Yes — triggers re-discovery on next startup |
| `state.json` | Last known stock state per store | Yes — sends full report for all stores on next poll |
| `debug/` | Screenshots and HTML from store locator pages | Yes — regenerate with `node debug-locators.js` |

## Development

```sh
npm run dev    # Runs with --watch for auto-restart on file changes
```

### Debugging Store Locators

If a retailer stops finding stores, the CSS selectors likely need updating:

```sh
node debug-locators.js
```

This captures screenshots, full HTML, and CSS selector lists from each retailer's store locator page into `debug/`. Compare against the selectors in `lib/discover-stores.js` and update as needed.
