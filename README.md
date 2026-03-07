# Bourbon Scout

Automated inventory tracker for allocated and rare bourbon. Monitors 5 major retailers near your zip code and sends real-time Discord alerts when bottles are spotted.

## Tracked Bottles (25)

Blanton's (Gold, Straight from the Barrel, Special Reserve, Red), Weller (Special Reserve, Antique 107, 12 Year, Full Proof, Single Barrel, C.Y.P.B.), E.H. Taylor Small Batch, Stagg Jr, BTAC (George T. Stagg, Eagle Rare 17, William Larue Weller, Thomas H. Handy, Sazerac Rye 18), Pappy Van Winkle (10/12/15/20/23), Elmer T. Lee, Rock Hill Farms, King of Kentucky.

## Supported Retailers

| Retailer | Method | Data Source |
|----------|--------|-------------|
| Costco | Browser (Playwright) | MUI `data-testid` attributes |
| Total Wine | Browser (Playwright) | `window.INITIAL_STATE` JSON |
| Walmart | Fetch-first, browser fallback | `__NEXT_DATA__` JSON |
| Kroger | REST API | Structured JSON |
| Safeway | REST API | Structured JSON |

## How It Works

1. **Store Discovery** — On startup, auto-discovers nearby stores for each retailer based on your zip code and search radius. Results are cached for 7 days. Falls back to static store data if browser-based locators fail (e.g., on CI).
2. **Inventory Scanning** — Scans all stores concurrently (limit 4) using 11 broad search queries that cover all 25 bottles. Prefers structured JSON extraction over CSS selectors for reliability.
3. **Discord Alerts** — When bottles are found, sends an `@everyone` urgent alert with product URLs, prices, and fulfillment info. A quiet summary posts after every scan. Store addresses link to Google Maps.

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

**When bottles are found:**
- `@everyone` ping with urgent embed
- Clickable product URLs and prices
- Fulfillment info (e.g., "Pickup today")
- Store name, distance, and Google Maps link

**After every scan:**
- Quiet summary embed with store count, retailer count, and scan duration

## Generated Files

| File | Purpose | Safe to Delete? |
|------|---------|-----------------|
| `stores.json` | Cached store discovery results | Yes — triggers re-discovery on next startup |
| `state.json` | Last known stock state per store | Yes — sends full report for all stores on next poll |
| `debug/` | Screenshots and HTML from store locator pages | Yes — regenerate with `node debug-locators.js` |

## Debugging Store Locators

If a retailer stops finding stores, the selectors likely need updating:

```sh
node debug-locators.js
```

This captures screenshots, full HTML, and selector lists from each retailer's store locator page into `debug/`. Compare against the selectors in `lib/discover-stores.js` and update as needed.
