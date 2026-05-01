import "dotenv/config";
import fetch from "node-fetch";
import { gotScraping } from "got-scraping";
import { HttpsProxyAgent } from "https-proxy-agent";
import { SocksProxyAgent } from "socks-proxy-agent";
import { chromium as rebrowserChromium } from "rebrowser-playwright-core";
import { chromium as vanillaChromium } from "playwright-core";
import { addExtra } from "playwright-extra";
import StealthPlugin from "puppeteer-extra-plugin-stealth";
// node-cron removed — replaced with setTimeout + jitter for variable scan intervals
import { readFile, writeFile, appendFile, rename, mkdir, unlink, readdir, rm } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { join, dirname } from "node:path";
import * as cheerio from "cheerio";
import { discoverStores } from "./lib/discover-stores.js";
import { zipToCoords } from "./lib/geo.js";

// rebrowser-patches: patched CDP commands to evade Akamai/PerimeterX automation detection
// Only used for discover-stores.js (store locators) via addExtra + StealthPlugin.
// All 5 retailer scrapers use vanillaChromium for truly clean CDP.
const chromium = addExtra(rebrowserChromium);
chromium.use(StealthPlugin());

// ─── Configuration ───────────────────────────────────────────────────────────

const {
  DISCORD_WEBHOOK_URL,
  ZIP_CODE = "85283",
  SEARCH_RADIUS_MILES = "15",
  MAX_STORES_PER_RETAILER = "5",
  KROGER_MAX_STORES,
  KROGER_RADIUS_MILES,
  KROGER_CLIENT_ID,
  KROGER_CLIENT_SECRET,
  SAFEWAY_API_KEY,
  ALBERTSONS_API_KEY,
  // Kroger HTML verification: visits product pages and parses "Item Unavailable"
  // text to confirm/refute B+C tier classifications. Default ON. Set to "false" to
  // disable verification entirely (saves ~60-120s per scan, accepts more false positives).
  KROGER_HTML_VERIFY = "true",
  POLL_INTERVAL = "*/15 * * * *",
  REALERT_EVERY_N_SCANS = "4",
  PROXY_URL,
  BACKUP_PROXY_URL,
  SECONDARY_ZIPS,
} = process.env;

// Parse cron-style POLL_INTERVAL into milliseconds for setTimeout-based scheduling.
// Supports "*/N * * * *" (every N minutes) format. Falls back to 15 min.
function parsePollIntervalMs(cronExpr) {
  const match = cronExpr?.match(/^\*\/(\d+)\s/);
  if (match) {
    const mins = parseInt(match[1], 10);
    if (mins > 0) return mins * 60 * 1000;
  }
  return 15 * 60 * 1000;
}

// ─── Schedule-aware scanning ─────────────────────────────────────────────────
// Active scanning: 30 min default (4 AM – 10 PM MT), 20 min boost on Tue/Thu nights.
// Sleeps 10 PM – 4 AM MT. Arizona = MST year-round (UTC-7, no DST).
const ACTIVE_START = 4;  // 4 AM MT
const ACTIVE_END = 22;   // 10 PM MT

function getMTTime() {
  const now = new Date();
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/Phoenix",
    hour: "numeric", hour12: false,
    weekday: "short",
  }).formatToParts(now);
  const hour = parseInt(parts.find((p) => p.type === "hour").value, 10);
  const day = parts.find((p) => p.type === "weekday").value;
  return { hour, day };
}

function isActiveHour(hour) {
  return hour >= ACTIVE_START && hour < ACTIVE_END;
}

function isBoostPeriod(hour, day) {
  // Boost = 20-min intervals on Tue/Thu evenings into Wed/Fri mornings.
  // Hardcoded to 5 PM – 10 AM window (prime drop checking hours).
  return (day === "Tue" && hour >= 17) ||
         (day === "Wed" && hour < 10) ||
         (day === "Thu" && hour >= 17) ||
         (day === "Fri" && hour < 10);
}

// Residential proxy agent for fetch-based scrapers (Walmart fetch path, Kroger, Safeway).
// Only created when PROXY_URL is set. Discord webhook calls intentionally skip the proxy.
// Auto-detects SOCKS5/SOCKS4 vs HTTP/HTTPS proxy protocol.
function createProxyAgent(url) {
  if (!url) return null;
  if (url.startsWith("socks4://") || url.startsWith("socks5://")) {
    return new SocksProxyAgent(url);
  }
  return new HttpsProxyAgent(url);
}
let proxyAgent = createProxyAgent(PROXY_URL);

// Sticky session support for residential proxies (e.g., DataImpulse).
// DataImpulse uses port-based sticky sessions: ports 10000-20000 each get a
// dedicated IP that persists for ~30 min. Each retailer gets its own sticky port
// so they each have a different residential IP — if one retailer's IP gets flagged,
// it doesn't cascade to other retailers. Prevents IP rotation from breaking cookie
// chains (e.g., Costco pre-warm cookies on IP A, searches from IP B).
let proxySessionUrl = PROXY_URL;
const retailerProxyAgents = {};
function makeStickyProxyUrl(port) {
  if (!PROXY_URL) return null;
  const url = new URL(PROXY_URL);
  url.port = String(port);
  return url.toString();
}
// Per-retailer proxy override: BACKUP_PROXY_URL + BACKUP_PROXY_RETAILERS lets specific
// retailers use a different proxy provider (e.g., IPRoyal for Costco if DataImpulse is burned).
const BACKUP_PROXY_RETAILERS = new Set(
  (process.env.BACKUP_PROXY_RETAILERS || "").split(",").map((s) => s.trim()).filter(Boolean)
);
const retailerProxyUrls = {}; // retailerKey → full proxy URL (for browser proxy config)

function refreshProxySession() {
  if (!PROXY_URL) return;
  // Assign each retailer its own sticky port (= its own residential IP)
  const retailers = ["costco", "totalwine", "walmart", "kroger", "safeway", "albertsons", "walgreens", "samsclub", "extramile", "liquorexpress", "chandlerliquors"];
  const basePort = 10000 + Math.floor(Math.random() * 9000); // leave room for 8 retailers
  const backupBasePort = 10000 + Math.floor(Math.random() * 9000);
  for (let i = 0; i < retailers.length; i++) {
    const key = retailers[i];
    if (BACKUP_PROXY_URL && BACKUP_PROXY_RETAILERS.has(key)) {
      /* v8 ignore start -- requires BACKUP_PROXY_URL + BACKUP_PROXY_RETAILERS at module load */
      // Use backup proxy for this retailer (e.g., IPRoyal for burned IPs)
      try {
        const backupUrl = new URL(BACKUP_PROXY_URL);
        backupUrl.port = String(backupBasePort + i);
        retailerProxyUrls[key] = backupUrl.toString();
        retailerProxyAgents[key] = createProxyAgent(retailerProxyUrls[key]);
      } catch (err) {
        console.error(`[proxy] Invalid BACKUP_PROXY_URL for ${key}: ${err.message} — falling back to primary`);
        const url = makeStickyProxyUrl(basePort + i);
        retailerProxyUrls[key] = url;
        retailerProxyAgents[key] = createProxyAgent(url);
      }
      /* v8 ignore stop */
    } else {
      const url = makeStickyProxyUrl(basePort + i);
      retailerProxyUrls[key] = url;
      retailerProxyAgents[key] = createProxyAgent(url);
    }
  }
  // Default proxyAgent uses a port outside the per-retailer range (avoids sharing with retailers[0])
  proxySessionUrl = makeStickyProxyUrl(basePort + retailers.length);
  proxyAgent = createProxyAgent(proxySessionUrl);
}
// Rotate a retailer's proxy to a fresh sticky port (= new residential IP).
// Called when a retailer's fetch path detects blocking — next request uses a different IP.
function rotateRetailerProxy(retailerKey) {
  if (!PROXY_URL) return;
  const proxyBase = (BACKUP_PROXY_URL && BACKUP_PROXY_RETAILERS.has(retailerKey)) ? BACKUP_PROXY_URL : PROXY_URL;
  const newPort = 10000 + Math.floor(Math.random() * 10000);
  try {
    const url = new URL(proxyBase);
    url.port = String(newPort);
    const newUrl = url.toString();
    retailerProxyUrls[retailerKey] = newUrl;
    retailerProxyAgents[retailerKey] = createProxyAgent(newUrl);
    // Clear stale cookies from old IP — anti-bot ties cookies to IPs
    browserStateCache = null;
    delete retailerCookieCache[retailerKey];
    if (retailerBrowserCache[retailerKey]) {
      retailerBrowserCache[retailerKey].context.clearCookies().catch(() => {});
    }
    console.log(`[proxy] Rotated ${retailerKey} to port ${newPort} (new IP, cookies cleared)`);
  } catch (err) {
    console.warn(`[proxy] Failed to rotate ${retailerKey}: ${err.message}`);
  }
}

function getRetailerProxy(retailerKey) {
  return retailerProxyAgents[retailerKey] || proxyAgent;
}
// Get the raw proxy URL string for a retailer (used by got-scraping's proxyUrl option).
function getRetailerProxyUrl(retailerKey) {
  return retailerProxyUrls[retailerKey] || proxySessionUrl || null;
}
// Build a proxy config object for Playwright browser launch for a specific retailer.
function getRetailerBrowserProxy(retailerKey) {
  const url = retailerProxyUrls[retailerKey];
  if (!url) return null;
  /* v8 ignore start -- requires PROXY_URL at module load to populate retailerProxyUrls */
  const parsed = new URL(url);
  const config = { server: `${parsed.protocol}//${parsed.host}` };
  if (parsed.username) config.username = decodeURIComponent(parsed.username);
  if (parsed.password) config.password = decodeURIComponent(parsed.password);
  return config;
  /* v8 ignore stop */
}

// Check if any proxy is available for fetch paths. Returns false when both primary and
// backup are exhausted, or when no proxy/backup exists and primary is exhausted.
function isProxyAvailable() {
  if (!proxyAgent) return false;
  if (!primaryProxyExhausted) return true;
  if (BACKUP_PROXY_URL && !backupProxyExhausted) return true;
  return false;
}

// Auto-failover: switch ALL retailers from primary to backup proxy when primary exhausts.
// Returns true if failover succeeded, false if no backup available.
function failoverToBackupProxy() {
  if (!BACKUP_PROXY_URL) return false;
  /* v8 ignore start -- requires BACKUP_PROXY_URL at module load (tested in proxy.test.js env) */
  console.log("[proxy] Failing over ALL retailers to backup proxy");
  const retailers = ["costco", "totalwine", "walmart", "kroger", "safeway", "albertsons", "walgreens", "samsclub", "extramile", "liquorexpress", "chandlerliquors"];
  const basePort = 10000 + Math.floor(Math.random() * 9000);
  for (let i = 0; i < retailers.length; i++) {
    try {
      const url = new URL(BACKUP_PROXY_URL);
      url.port = String(basePort + i);
      retailerProxyUrls[retailers[i]] = url.toString();
      retailerProxyAgents[retailers[i]] = createProxyAgent(retailerProxyUrls[retailers[i]]);
    } catch (err) {
      console.warn(`[proxy] Failover failed for ${retailers[i]}: ${err.message}`);
    }
  }
  try {
    const url = new URL(BACKUP_PROXY_URL);
    url.port = String(basePort + retailers.length);
    proxySessionUrl = url.toString();
    proxyAgent = createProxyAgent(proxySessionUrl);
  } catch {}
  return true;
  /* v8 ignore stop */
}

// Per-poll health metrics per retailer. Reset each poll().
// Structure: { retailerKey: { queries: 0, succeeded: 0, failed: 0, blocked: 0,
//   queryStats: { "query string": { ok: 0, blocked: 0, failed: 0 } },
//   path: { fetch: 0, browser: 0 } } }
let scraperHealth = {};

// Proxy exhaustion tracking. When primary proxy returns 407 TRAFFIC_EXHAUSTED, we attempt
// automatic failover to BACKUP_PROXY_URL (if configured). If backup also exhausts, fetch
// paths skip entirely and browser fallback takes over. Reset at the start of each poll().
let primaryProxyExhausted = false;
let backupProxyExhausted = false;

function initHealth(key) {
  if (!scraperHealth[key]) {
    scraperHealth[key] = {
      queries: 0, succeeded: 0, failed: 0, blocked: 0,
      queryStats: {}, path: { fetch: 0, browser: 0 }, stores: {},
      // Granular failure-mode counters — surfaces *why* a scraper isn't producing data.
      // Without these, every "blocked" looks identical in metrics, hiding root causes
      // like contract drift (page schema changed) vs. soft blocks (Akamai degraded
      // response) vs. proxy auth (DataImpulse 407). Used by audit reports.
      reasons: { waf: 0, proxy: 0, soft_block: 0, contract_drift: 0, timeout: 0, network: 0 },
    };
  }
}

// Recognized failure-mode reasons:
//   "waf"            — anti-bot WAF returned a hard challenge/block (Akamai 403, PerimeterX captcha, Incapsula iframe)
//   "proxy"          — proxy connection issue (407 TRAFFIC_EXHAUSTED, ERR_PROXY_AUTH_UNSUPPORTED, ERR_INTERNET_DISCONNECTED)
//   "soft_block"     — page returned 200 but with no products and no recognized "no results" container (degraded response)
//   "contract_drift" — required field/structure missing from response (e.g., __NEXT_DATA__ absent, expected JSON shape changed)
//   "timeout"        — query exceeded its time budget without resolving
//   "network"        — generic network or unexpected error not fitting above
const HEALTH_REASONS = new Set(["waf", "proxy", "soft_block", "contract_drift", "timeout", "network"]);

function trackHealth(key, outcome, { query, via, storeId, reason } = {}) {
  initHealth(key);
  scraperHealth[key].queries++;
  if (outcome === "ok") scraperHealth[key].succeeded++;
  else if (outcome === "blocked") scraperHealth[key].blocked++;
  else scraperHealth[key].failed++;
  // Per-query tracking (optional) — reveals which queries find bottles vs. waste budget
  if (query) {
    if (!scraperHealth[key].queryStats[query]) scraperHealth[key].queryStats[query] = { ok: 0, blocked: 0, failed: 0 };
    scraperHealth[key].queryStats[query][outcome === "ok" ? "ok" : outcome === "blocked" ? "blocked" : "failed"]++;
  }
  // Fetch-vs-browser attribution (optional) — tracks which path produced the outcome
  if (via === "fetch") scraperHealth[key].path.fetch++;
  else if (via === "browser") scraperHealth[key].path.browser++;
  // Per-store tracking (optional) — identifies persistently blocked stores
  if (storeId) {
    if (!scraperHealth[key].stores[storeId]) scraperHealth[key].stores[storeId] = { ok: 0, blocked: 0, failed: 0 };
    scraperHealth[key].stores[storeId][outcome === "ok" ? "ok" : outcome === "blocked" ? "blocked" : "failed"]++;
  }
  // Failure-mode reason (optional, only for non-ok outcomes) — granular root cause
  if (reason && outcome !== "ok" && HEALTH_REASONS.has(reason)) {
    scraperHealth[key].reasons[reason]++;
  }
}

const STATE_FILE = new URL("./state.json", import.meta.url);
const METRICS_FILE = new URL("./metrics.jsonl", import.meta.url);

// Per-retailer persistent browser profile directories. Full Chrome profiles
// (HTTP cache, service workers, IndexedDB, visited history) accumulate on disk,
// making the scraper look like a returning visitor rather than a fresh bot.
const PROFILES_DIR = join(dirname(fileURLToPath(import.meta.url)), "browser-profiles");

// ─── Adaptive Retailer Skipping ──────────────────────────────────────────────
// Back off from retailers that fail repeatedly. After MAX_CONSECUTIVE_FAILURES,
// skip scans for SKIP_COOLDOWN_MS to let IP reputation recover.
const retailerFailures = {};
const MAX_CONSECUTIVE_FAILURES = 3;
const SKIP_COOLDOWN_MS = 30 * 60 * 1000; // 30 min

function shouldSkipRetailer(key) {
  const f = retailerFailures[key];
  if (!f || !f.skipUntil) return false;
  if (Date.now() >= f.skipUntil) {
    f.skipUntil = null;
    f.consecutive = 0;
    console.log(`[${key}] Cooldown expired, resuming scans`);
    return false;
  }
  const remaining = Math.round((f.skipUntil - Date.now()) / 60000);
  console.log(`[${key}] Skipping (${remaining}min cooldown remaining)`);
  return true;
}

function recordRetailerOutcome(key, success) {
  if (!retailerFailures[key]) retailerFailures[key] = { consecutive: 0, skipUntil: null };
  if (success) {
    retailerFailures[key].consecutive = 0;
    retailerFailures[key].skipUntil = null;
  } else {
    retailerFailures[key].consecutive++;
    if (retailerFailures[key].consecutive >= MAX_CONSECUTIVE_FAILURES) {
      retailerFailures[key].skipUntil = Date.now() + SKIP_COOLDOWN_MS;
      console.log(`[${key}] ${MAX_CONSECUTIVE_FAILURES} consecutive failures — backing off for 30min`);
    }
  }
}

// ─── Store Priority ──────────────────────────────────────────────────────────
// Some retailers have a clear "tier" of stores that carry allocated bourbon more
// often. Reordering tasks so those stores run first (a) surfaces finds in Discord
// sooner, (b) protects them from poll time-budget cutoffs, and (c) groups logs
// with the highest-signal stores at the top.
//
// Fry's: Marketplace locations (vs. regular Fry's Food And Drug) have larger
// spirits sections and receive most allocated drops in the Phoenix metro.
function prioritizeStores(retailerKey, stores) {
  if (retailerKey !== "kroger" || !Array.isArray(stores) || stores.length <= 1) {
    return stores;
  }
  const isMarketplace = (s) => /marketplace/i.test(s.name || "");
  // Stable sort: Marketplace stores first, original order within each group preserved.
  return [
    ...stores.filter(isMarketplace),
    ...stores.filter((s) => !isMarketplace(s)),
  ];
}

// ─── Known Product URL Tracking ──────────────────────────────────────────────
// Bottles previously found in state.json have product URLs. Checking these directly
// is less suspicious than searching (it's just visiting a product page) and catches
// bottles that search might miss due to anti-bot blocking.
let knownProducts = {};

// Seed product URLs for cold-start coverage. These are real product pages that exist
// even before state.json has any data. State.json URLs are merged on top (may have
// fresher prices). Costco URLs constructed from Warehouse Runner item numbers and
// previously known shelf tag numbers in .product.{id}.html format.
const SEED_PRODUCT_URLS = {
  costco: [
    // Warehouse Runner item numbers (warehouserunner.com, fresh as of 2026-04)
    { name: "Buffalo Trace", url: "https://www.costco.com/.product.148996.html" },
    { name: "Weller Special Reserve", url: "https://www.costco.com/.product.1076081.html" },
    { name: "Weller Antique 107", url: "https://www.costco.com/.product.1978066.html" },
    { name: "Weller 12 Year", url: "https://www.costco.com/.product.1871297.html" },
    { name: "Eagle Rare 17 Year", url: "https://www.costco.com/.product.822393.html" },
    { name: "E.H. Taylor Barrel Proof", url: "https://www.costco.com/.product.1886807.html" },
    { name: "Pappy Van Winkle 10 Year", url: "https://www.costco.com/.product.759591.html" },
    { name: "Pappy Van Winkle 23 Year", url: "https://www.costco.com/.product.336624.html" },
    { name: "King of Kentucky", url: "https://www.costco.com/.product.2045142.html" },
    // Previously known item numbers (restored for coverage — 404s are skipped gracefully)
    { name: "Weller Antique 107", url: "https://www.costco.com/.product.1037200.html" },
    { name: "Weller Single Barrel", url: "https://www.costco.com/.product.1482145.html" },
    { name: "Weller Full Proof", url: "https://www.costco.com/.product.1391743.html" },
    { name: "Weller 12 Year", url: "https://www.costco.com/.product.822390.html" },
    { name: "Blanton's Original", url: "https://www.costco.com/.product.122438.html" },
    { name: "Blanton's Gold", url: "https://www.costco.com/.product.1499833.html" },
    { name: "Blanton's Straight from the Barrel", url: "https://www.costco.com/.product.1528218.html" },
    { name: "Stagg Jr", url: "https://www.costco.com/.product.822398.html" },
    { name: "Old Forester Birthday Bourbon", url: "https://www.costco.com/.product.952976.html" },
    { name: "Old Forester Birthday Bourbon", url: "https://www.costco.com/.product.1990130.html" },
    { name: "Pappy Van Winkle 10 Year", url: "https://www.costco.com/.product.724952.html" },
    { name: "Pappy Van Winkle 12 Year", url: "https://www.costco.com/.product.256230.html" },
    { name: "Pappy Van Winkle 15 Year", url: "https://www.costco.com/.product.149085.html" },
    { name: "Pappy Van Winkle 20 Year", url: "https://www.costco.com/.product.256228.html" },
    { name: "Elmer T. Lee", url: "https://www.costco.com/.product.43178.html" },
    { name: "E.H. Taylor Small Batch", url: "https://www.costco.com/.product.775642.html" },
    { name: "Eagle Rare 17 Year", url: "https://www.costco.com/.product.149017.html" },
    { name: "Jack Daniel's 12 Year", url: "https://www.costco.com/.product.1737672.html" },
    { name: "King of Kentucky", url: "https://www.costco.com/.product.2045151.html" },
  ],
  walmart: [
    { name: "Buffalo Trace", url: "https://www.walmart.com/ip/Buffalo-Trace-Kentucky-Straight-Bourbon-Whiskey-750-ml-Liquor-45-Alcohol/132872863" },
    { name: "Blanton's Original", url: "https://www.walmart.com/ip/Blanton-s-Single-Barrel-Bourbon-750ml-93-Proof/101986207" },
    { name: "Blanton's Gold", url: "https://www.walmart.com/ip/Blantons-Gold-Bourbon-750-Ml/1118154348" },
    { name: "Weller Special Reserve", url: "https://www.walmart.com/ip/Weller-Special-Reserve-Kentucky-Straight-Bourbon-Whiskey-750ml-90-Proof/181841775" },
    { name: "Weller Antique 107", url: "https://www.walmart.com/ip/WELLER-ANTIQUE-SP-BBN-750ML-107P/129914663" },
    { name: "Weller 12 Year", url: "https://www.walmart.com/ip/Weller-12yr-Bbn-12-750ml-90pf/199333690" },
    { name: "Weller Full Proof", url: "https://www.walmart.com/ip/Weller-Full-Proof-Kentucky-Straight-Bourbon-Whiskey-750ml-114-Proof/271272409" },
    { name: "E.H. Taylor Small Batch", url: "https://www.walmart.com/ip/E-H-Taylor-Small-Batch-Kentucky-Straight-Bourbon-Whiskey-750ml-100-Proof/248113789" },
    { name: "Eagle Rare 17 Year", url: "https://www.walmart.com/ip/Eagle-Rare-10-Year-Kentucky-Straight-Bourbon-Whiskey-750ml-90-Proof/141486286" },
    { name: "Stagg Jr", url: "https://www.walmart.com/ip/Stagg-Jr-Kentucky-Straight-Bourbon-Whiskey-750ml-Varying-Proof/961263340" },
    { name: "George T. Stagg", url: "https://www.walmart.com/ip/George-T-Stagg-Kentucky-Straight-Bourbon-Whiskey-750mL/47934603" },
    { name: "Pappy Van Winkle 10 Year", url: "https://www.walmart.com/ip/Old-Rip-Van-Winkle-Aged-10-Years-Kentucky-Straight-Bourbon-Whiskey-750ml-107-Proof/173438393" },
    { name: "Pappy Van Winkle 12 Year", url: "https://www.walmart.com/ip/ORVW-SR-12YR-BBN-750ML-90-4PF/167382862" },
    { name: "Pappy Van Winkle 15 Year", url: "https://www.walmart.com/ip/Pappy-Van-Winkle-s-15-Years-Old-Family-Reserve-Kentucky-Straight-Bourbon-Whiskey-750ml-107-Proof/46707130" },
    { name: "Pappy Van Winkle 20 Year", url: "https://www.walmart.com/ip/Pappy-Van-Winkle-s-Family-Reserve-20-Years-Old-Kentucky-Straight-Bourbon-Whiskey-750ml-90-4-Proof/46707131" },
    { name: "Pappy Van Winkle 23 Year", url: "https://www.walmart.com/ip/Pappy-Van-Winkle-s-Family-Reserve-Kentucky-Straight-Bourbon-Whiskey-750-ml/42358738" },
    { name: "Thomas H. Handy", url: "https://www.walmart.com/ip/Thomas-H-Handy-Straight-Rye-Whiskey-750ml-124-9-Proof/293634255" },
    { name: "E.H. Taylor Barrel Proof", url: "https://www.walmart.com/ip/Colonel-E-H-Taylor-Barrel-Proof-Straight-Kentucky-Bourbon-Whiskey-750ml-Variable-Proof/859877176" },
    { name: "E.H. Taylor Straight Rye", url: "https://www.walmart.com/ip/Spirits-Eht-Straight-Rye-Whiskey-6-750ml-100/678634439" },
    { name: "Weller Single Barrel", url: "https://www.walmart.com/ip/Weller-Single-Barrel-Kentucky-Straight-Bourbon-Whiskey-750ml-90-Proof/5044158522" },
    { name: "Weller C.Y.P.B.", url: "https://www.walmart.com/ip/Weller-C-Y-P-B-Kentucky-Straight-Bourbon-Whiskey-750ml-95-Proof/5044158521" },
    { name: "Jack Daniel's 10 Year", url: "https://www.walmart.com/ip/Jack-Daniel-s-10-Years-Old-Tennessee-Whiskey-700-ml-Bottle/2877322440" },
    { name: "Jack Daniel's 12 Year", url: "https://www.walmart.com/ip/Jack-Daniel-s-12-Years-Old-Tennessee-Whiskey-Batch-2-700-ml-Bottle/5395514583" },
    { name: "Jack Daniel's 12 Year", url: "https://www.walmart.com/ip/Jack-Daniel-s-12-Years-Old-Tennessee-Whiskey-Batch-03-700-ml-Bottle/15346310084" },
    { name: "Jack Daniel's 14 Year", url: "https://www.walmart.com/ip/Jack-Daniel-s-14-Years-Old-Tennessee-Whiskey-Batch-01-700-ml-Bottle/15318058743" },
    { name: "Blanton's Straight from the Barrel", url: "https://www.walmart.com/ip/Blantons-Blanton-Sftb-Bbn-Us-6-750ml-125-4pf/1166685343" },
    { name: "Rock Hill Farms", url: "https://www.walmart.com/ip/Rock-Hill-Farms-Single-Barrel-Kentucky-Straight-Bourbon-Whiskey-750ml-100-Proof/392415896" },
  ],
  totalwine: [
    { name: "Buffalo Trace", url: "https://www.totalwine.com/spirits/bourbon/small-batch-bourbon/buffalo-trace-kentucky-straight-bourbon-whiskey/p/102882750" },
    { name: "Blanton's Original", url: "https://www.totalwine.com/spirits/bourbon/small-batch-bourbon/blantons-single-barrel-bourbon/p/170891050" },
    { name: "Blanton's Gold", url: "https://www.totalwine.com/spirits/bourbon/small-batch-bourbon/blantons-gold-bourbon/p/231752750" },
    { name: "Blanton's Straight from the Barrel", url: "https://www.totalwine.com/spirits/bourbon/small-batch-bourbon/blantons-straight-from-the-barrel/p/234524750" },
    { name: "Weller Special Reserve", url: "https://www.totalwine.com/spirits/bourbon/small-batch-bourbon/wl-weller-special-reserve-bourbon/p/13538750" },
    { name: "Weller Antique 107", url: "https://www.totalwine.com/spirits/bourbon/small-batch-bourbon/old-weller-antique-107/p/97087750" },
    { name: "Weller 12 Year", url: "https://www.totalwine.com/spirits/bourbon/small-batch-bourbon/wl-weller-12-year-bourbon/p/105505750" },
    { name: "Weller Full Proof", url: "https://www.totalwine.com/spirits/bourbon/small-batch-bourbon/wl-weller-full-proof/p/191577750" },
    { name: "Weller Single Barrel", url: "https://www.totalwine.com/spirits/bourbon/small-batch-bourbon/wl-weller-single-barrel-bourbon/p/231903750" },
    { name: "E.H. Taylor Small Batch", url: "https://www.totalwine.com/spirits/bourbon/small-batch-bourbon/colonel-eh-taylor-small-batch-bourbon/p/137579750" },
    { name: "E.H. Taylor Single Barrel", url: "https://www.totalwine.com/spirits/bourbon/small-batch-bourbon/colonel-eh-taylor-single-barrel-bourbon/p/125138750" },
    { name: "E.H. Taylor Barrel Proof", url: "https://www.totalwine.com/spirits/bourbon/small-batch-bourbon/colonel-eh-taylor-barrel-proof-bourbon/p/125367750" },
    { name: "E.H. Taylor Amaranth", url: "https://www.totalwine.com/spirits/bourbon/small-batch-bourbon/colonel-eh-taylor-amaranth/p/220278750" },
    { name: "E.H. Taylor 18 Year Marriage", url: "https://www.totalwine.com/spirits/bourbon/small-batch-bourbon/colonel-eh-taylor-18-yr-marriage-bourbon/p/231398750" },
    { name: "Eagle Rare 17 Year", url: "https://www.totalwine.com/spirits/bourbon/small-batch-bourbon/eagle-rare-17-year-kentucky-straight-bourbon-whiskey/p/102755750" },
    { name: "Stagg Jr", url: "https://www.totalwine.com/spirits/bourbon/small-batch-bourbon/stagg-bourbon/p/135217750" },
    { name: "George T. Stagg", url: "https://www.totalwine.com/spirits/bourbon/small-batch-bourbon/george-t-stagg-bourbon/p/102757750" },
    { name: "William Larue Weller", url: "https://www.totalwine.com/spirits/bourbon/small-batch-bourbon/william-larue-weller-kentucky-straight-bourbon-whiskey/p/107589750" },
    { name: "Thomas H. Handy", url: "https://www.totalwine.com/spirits/american-whiskey/rye-whiskey/thomas-h-handy-sazerac-straight-rye-whiskey/p/102758750" },
    { name: "Pappy Van Winkle 10 Year", url: "https://www.totalwine.com/spirits/bourbon/small-batch-bourbon/old-rip-van-winkle-10-year-bourbon/p/109132750" },
    { name: "Pappy Van Winkle 12 Year", url: "https://www.totalwine.com/spirits/bourbon/small-batch-bourbon/van-winkle-special-reserve-12-year-bourbon/p/106280750" },
    { name: "Pappy Van Winkle 15 Year", url: "https://www.totalwine.com/spirits/bourbon/small-batch-bourbon/pappy-van-winkle-family-reserve-15-year-bourbon/p/96355750" },
    { name: "Pappy Van Winkle 20 Year", url: "https://www.totalwine.com/spirits/bourbon/small-batch-bourbon/pappy-van-winkle-family-reserve-20-year-bourbon/p/97274750" },
    { name: "Pappy Van Winkle 23 Year", url: "https://www.totalwine.com/spirits/bourbon/small-batch-bourbon/pappy-van-winkle-family-reserve-23-year-bourbon/p/106282750" },
    { name: "Van Winkle Family Reserve Rye", url: "https://www.totalwine.com/spirits/american-whiskey/rye-whiskey/van-winkle-family-reserve-13-year-rye/p/109168750" },
    { name: "Rock Hill Farms", url: "https://www.totalwine.com/spirits/bourbon/small-batch-bourbon/rock-hill-farms-bourbon/p/5343750" },
    { name: "Weller C.Y.P.B.", url: "https://www.totalwine.com/spirits/bourbon/small-batch-bourbon/wl-weller-cypbbourbon/p/192978750" },
    { name: "E.H. Taylor Seasoned Wood", url: "https://www.totalwine.com/spirits/bourbon/small-batch-bourbon/colonel-e-h-taylor-seasoned-wood/p/169963750" },
    { name: "E.H. Taylor Four Grain", url: "https://www.totalwine.com/spirits/bourbon/small-batch-bourbon/colonel-eh-taylor-four-grain/p/181608750" },
    { name: "E.H. Taylor Cured Oak", url: "https://www.totalwine.com/spirits/bourbon/small-batch-bourbon/colonel-e-h-taylor-cured-oak-bourbon/p/147269750" },
    { name: "E.H. Taylor Straight Rye", url: "https://www.totalwine.com/spirits/american-whiskey/rye-whiskey/colonel-eh-taylor-straight-rye/p/130861750" },
    { name: "Elmer T. Lee", url: "https://www.totalwine.com/spirits/bourbon/small-batch-bourbon/elmer-t-lee-bourbon/p/5349750" },
    { name: "King of Kentucky", url: "https://www.totalwine.com/spirits/bourbon/small-batch-bourbon/king-of-kentucky-straight-bourbon/p/192643750" },
    { name: "Old Forester Birthday Bourbon", url: "https://www.totalwine.com/spirits/bourbon/small-batch-bourbon/old-forester-birthday-bourbon/p/100001750" },
    { name: "Old Forester President's Choice", url: "https://www.totalwine.com/spirits/bourbon/small-batch-bourbon/old-forester-presidents-choice-barrel-proof-bourbon/p/2126267602" },
    { name: "Old Forester 150th Anniversary", url: "https://www.totalwine.com/spirits/bourbon/small-batch-bourbon/old-forester-150-anniversary-batch-proof-bourbon/p/231926750" },
    { name: "Old Forester King Ranch", url: "https://www.totalwine.com/spirits/bourbon/old-forester-king-ranch-bourbon/p/2126213976" },
    { name: "Michter's 10 Year", url: "https://www.totalwine.com/spirits/bourbon/small-batch-bourbon/michters-single-barrel-10-year-bourbon/p/3453750" },
    { name: "Penelope Founder's Reserve", url: "https://www.totalwine.com/spirits/bourbon/small-batch-bourbon/penelope-estate-collection-founders-reserve-bourbon/p/2126269273" },
    { name: "Jack Daniel's 10 Year", url: "https://www.totalwine.com/spirits/american-whiskey/tennessee-whiskey/jack-daniels-10-year-tennessee-whiskey/p/2126220956" },
    { name: "Jack Daniel's 12 Year", url: "https://www.totalwine.com/spirits/american-whiskey/tennessee-whiskey/jack-daniels-12-yr/p/2126220955" },
    { name: "Jack Daniel's 14 Year", url: "https://www.totalwine.com/spirits/american-whiskey/jack-daniels-14-year-tennessee-whiskey/p/2126261899" },
  ],
};

function loadKnownProducts(state) {
  knownProducts = {};
  // Start with seed URLs (cold-start baseline)
  for (const [retailerKey, seeds] of Object.entries(SEED_PRODUCT_URLS)) {
    const urls = new Set();
    const products = [];
    for (const seed of seeds) {
      urls.add(seed.url);
      products.push({ name: seed.name, url: seed.url, sku: "", price: "" });
    }
    knownProducts[retailerKey] = products;
    // Store urls Set for dedup when merging state.json below
    knownProducts[`_urls_${retailerKey}`] = urls;
  }
  // Merge state.json URLs on top (may have fresher prices/SKUs)
  for (const [retailerKey, stores] of Object.entries(state)) {
    const urls = knownProducts[`_urls_${retailerKey}`] || new Set();
    if (!knownProducts[retailerKey]) knownProducts[retailerKey] = [];
    for (const storeData of Object.values(stores)) {
      if (!storeData?.bottles) continue;
      for (const [name, bottle] of Object.entries(storeData.bottles)) {
        if (bottle.url && !urls.has(bottle.url)) {
          urls.add(bottle.url);
          knownProducts[retailerKey].push({ name, url: bottle.url, sku: bottle.sku, price: bottle.price });
        }
      }
    }
  }
  // Clean up internal dedup keys
  for (const key of Object.keys(knownProducts)) {
    if (key.startsWith("_urls_")) delete knownProducts[key];
  }
}

// ─── Query Rotation ─────────────────────────────────────────────────────────
// Priority-based query rotation: high-value bottles (BTAC, Pappy, Taylor, Michter's)
// run every scan (every 30 min). Lower-urgency queries alternate even/odd scans
// (every 60 min). Total queries per scan: ~9 (7 priority + ~3-4 rotating + canary)
// instead of 8 — minimal increase, but time-sensitive bottles checked 2× as often.
let scanCounter = 0;

// Queries that run EVERY scan — bottles that sell out fastest
const PRIORITY_QUERIES = new Set([
  "van winkle",             // Pappy 10/12/15/20/23 + Family Reserve Rye 13
  "george t stagg",         // George T. Stagg (BTAC)
  "eagle rare 17",          // Eagle Rare 17 Year (BTAC)
  "thomas handy sazerac",   // Thomas H. Handy (BTAC)
  "eh taylor",              // All Taylor variants (rare ones sell fast)
  "michters bourbon",       // Michter's 10 Year
]);

function getQueriesForScan(allQueries) {
  const canaryQuery = "buffalo trace";
  const rotating = allQueries.filter((q) => q !== canaryQuery && !PRIORITY_QUERIES.has(q));
  // Rotating queries alternate based on scan parity
  const rotatingGroup = rotating.filter((_, i) => i % 2 === scanCounter % 2);
  // Canary FIRST (health check runs before anything else), then priority + rotating
  const priority = allQueries.filter((q) => PRIORITY_QUERIES.has(q));
  return [canaryQuery, ...priority, ...rotatingGroup];
}

// Shuffle queries but keep canary (first element) in position 0.
// Canary must run first so browser context is still alive for the health check.
function shuffleKeepCanaryFirst(queries) {
  if (queries.length <= 1) return queries;
  const [canary, ...rest] = queries;
  return [canary, ...shuffle(rest)];
}

// ─── Target Bottles ──────────────────────────────────────────────────────────
// Broad search queries that cover multiple bottles in a single page load.
// Each query should cover at least one TARGET_BOTTLE via matchesBottle.
const SEARCH_QUERIES = [
  "weller bourbon",           // Weller SR/107/12/FP/SB/CYPB + William Larue Weller
  "blantons bourbon",          // Blanton's Gold/SFTB/SR/Red/Green/Black
  "van winkle",                // Pappy 10/12/15/20/23 + Family Reserve Rye 13
  "eh taylor",                 // All Taylor: SmB/SiB/BP/Rye/Seasoned/4Grain/Amaranth/CuredOak/18yr
  "stagg bourbon",             // Stagg Jr + George T. Stagg
  "george t stagg",            // George T. Stagg (BTAC) — keyword search needs exact name
  "eagle rare 17",             // Eagle Rare 17 Year (BTAC)
  "thomas handy sazerac",      // Thomas H. Handy (BTAC)
  "elmer t lee",               // Elmer T. Lee
  "rock hill farms",           // Rock Hill Farms
  "king of kentucky bourbon",  // King of Kentucky
  "old forester bourbon",      // Birthday, President's Choice, 150th Anniversary, King Ranch
  "michters bourbon",           // Michter's 10 Year Single Barrel (Costco + Total Wine + Walmart only)
  "penelope bourbon",           // Penelope Founder's Reserve + Estate Collection (Costco + Total Wine + Walmart only)
  "jack daniels aged",           // Jack Daniel's 10/12/14 Year Tennessee Whiskey
  "heaven hill bourbon",         // Heaven Hill 90th Anniversary
  "heaven hill heritage",        // Heaven Hill Heritage Collection 22 Year
  "buffalo trace",              // Canary bottle — always-available health check
];

const TARGET_BOTTLES = [
  { name: "Blanton's Gold",             searchTerms: ["blanton's gold", "blantons gold"] },
  { name: "Blanton's Straight from the Barrel", searchTerms: ["blanton's straight from the barrel", "blantons straight from the barrel", "blantons sftb", "blanton's sftb", "blanton's sfb", "blantons sfb"] },
  { name: "Blanton's Special Reserve",  searchTerms: ["blanton's special reserve", "blantons special reserve"] },
  { name: "Blanton's Original",         searchTerms: ["blanton's single barrel", "blantons single barrel", "blanton's original", "blantons original"] },
  { name: "Weller Special Reserve",      searchTerms: ["weller special reserve", "weller sr", "w.l. weller special"] },
  { name: "Weller Antique 107",          searchTerms: ["weller antique 107", "old weller antique"] },
  { name: "Weller 12 Year",             searchTerms: ["weller 12", "w.l. weller 12", "weller 12 year"] },
  { name: "Weller Full Proof",          searchTerms: ["weller full proof"] },
  { name: "Weller Single Barrel",       searchTerms: ["weller single barrel"] },
  { name: "Weller C.Y.P.B.",            searchTerms: ["weller cypb", "weller c.y.p.b", "craft your perfect bourbon"] },
  { name: "E.H. Taylor Small Batch",    searchTerms: ["eh taylor small batch", "e.h. taylor small batch", "colonel e.h. taylor small batch", "col. e.h. taylor small batch", "col e.h. taylor small batch"] },
  { name: "E.H. Taylor Single Barrel",  searchTerms: ["eh taylor single barrel", "e.h. taylor single barrel", "col. e.h. taylor single barrel", "colonel e.h. taylor single barrel"] },
  { name: "E.H. Taylor Barrel Proof",   searchTerms: ["eh taylor barrel proof", "e.h. taylor barrel proof", "col. e.h. taylor barrel proof"] },
  { name: "E.H. Taylor Straight Rye",   searchTerms: ["eh taylor rye", "e.h. taylor rye", "e.h. taylor straight rye", "col. e.h. taylor straight rye", "colonel e.h. taylor rye"] },
  { name: "E.H. Taylor Seasoned Wood",  searchTerms: ["eh taylor seasoned wood", "e.h. taylor seasoned wood"] },
  { name: "E.H. Taylor Four Grain",     searchTerms: ["eh taylor four grain", "e.h. taylor four grain"] },
  { name: "E.H. Taylor Amaranth",       searchTerms: ["eh taylor amaranth", "e.h. taylor amaranth"] },
  { name: "E.H. Taylor Cured Oak",      searchTerms: ["eh taylor cured oak", "e.h. taylor cured oak"] },
  { name: "E.H. Taylor 18 Year Marriage", searchTerms: ["eh taylor 18 year", "e.h. taylor 18 year", "eh taylor 18 year marriage", "e.h. taylor 18 year marriage"] },
  { name: "Stagg Jr",                   searchTerms: ["stagg jr", "stagg junior", "stagg bourbon", "stagg kentucky"] },
  { name: "George T. Stagg",            searchTerms: ["george t. stagg", "george t stagg"] },
  { name: "Eagle Rare 17 Year",         searchTerms: ["eagle rare 17"] },
  { name: "William Larue Weller",       searchTerms: ["william larue weller", "wm larue weller", "william l weller", "w.l. weller btac", "larue weller"] },
  { name: "Thomas H. Handy",            searchTerms: ["thomas h. handy", "thomas handy sazerac", "thomas h handy", "thomas handy"] },
  { name: "Pappy Van Winkle 10 Year",   searchTerms: ["pappy van winkle 10", "old rip van winkle 10"] },
  { name: "Pappy Van Winkle 12 Year",   searchTerms: ["pappy van winkle 12", "van winkle special reserve 12"] },
  { name: "Pappy Van Winkle 15 Year",   searchTerms: ["pappy van winkle 15", "old rip van winkle 15"] },
  { name: "Pappy Van Winkle 20 Year",   searchTerms: ["pappy van winkle 20", "old rip van winkle 20"] },
  { name: "Pappy Van Winkle 23 Year",   searchTerms: ["pappy van winkle 23"] },
  { name: "Van Winkle Family Reserve Rye", searchTerms: ["van winkle family reserve rye", "van winkle rye 13"] },
  { name: "Elmer T. Lee",               searchTerms: ["elmer t. lee", "elmer t lee"] },
  { name: "Rock Hill Farms",            searchTerms: ["rock hill farms"] },
  { name: "King of Kentucky",           searchTerms: ["king of kentucky"] },
  { name: "Old Forester Birthday Bourbon", searchTerms: ["old forester birthday"] },
  { name: "Old Forester President's Choice", searchTerms: ["old forester president's choice", "old forester presidents choice", "old forester president", "president's choice bourbon"] },
  { name: "Old Forester 150th Anniversary", searchTerms: ["old forester 150th", "old forester 150"] },
  { name: "Old Forester King Ranch",    searchTerms: ["old forester king ranch"] },
  { name: "Michter's 10 Year",            searchTerms: ["michter's 10", "michters 10", "michter's 10 year", "michters 10 year"], retailers: ["costco", "totalwine", "walmart"] },
  { name: "Penelope Founder's Reserve",   searchTerms: ["penelope founder", "penelope founder's reserve", "penelope founders reserve"], retailers: ["costco", "totalwine", "walmart"] },
  { name: "Penelope Estate Collection",   searchTerms: ["penelope estate", "penelope estate collection"], retailers: ["costco", "totalwine", "walmart"] },
  { name: "Jack Daniel's 10 Year",       searchTerms: ["jack daniel's 10 year", "jack daniels 10 year", "jack daniel's 10yr", "jack daniels 10yr", "jack daniel's aged 10"] },
  { name: "Jack Daniel's 12 Year",       searchTerms: ["jack daniel's 12 year", "jack daniels 12 year", "jack daniel's 12yr", "jack daniels 12yr", "jack daniel's aged 12"] },
  { name: "Jack Daniel's 14 Year",       searchTerms: ["jack daniel's 14 year", "jack daniels 14 year", "jack daniel's 14yr", "jack daniels 14yr", "jack daniel's aged 14"] },
  { name: "Heaven Hill 90th Anniversary", searchTerms: ["heaven hill 90th", "heaven hill 90", "heaven hill anniversary"] },
  { name: "Heaven Hill Heritage Collection 22 Year", searchTerms: ["heaven hill heritage 22", "heaven hill 22 year", "heaven hill 22yr", "heaven hill heritage collection 22", "heritage collection 22"] },
  // Canary — always-available bottle used as a scraper health check
  { name: "Buffalo Trace", searchTerms: ["buffalo trace"], canary: true },
];

// Fast lookup for canary bottles (filtered out of alerts, used in health summary only)
const CANARY_NAMES = new Set(TARGET_BOTTLES.filter((b) => b.canary).map((b) => b.name));

// Per-retailer canary configuration — bottles that are realistically always-stocked at
// a specific retailer, used as a scraper-health probe. Default canary is Buffalo Trace,
// which works for most retailers. But Walgreens (small liquor sections) and Costco
// (Kirkland-heavy) rarely carry Buffalo Trace, making the default canary metric a false
// alarm there. To replace the canary for a specific retailer, add the bottle to
// TARGET_BOTTLES (without `canary: true`) and list the bottle's name here.
//
// IMPORTANT: each canary bottle MUST exist as a regular TARGET_BOTTLES entry. The
// `isCanaryFor(retailerKey, bottleName)` helper looks up the per-retailer canary list
// and falls back to the default Buffalo Trace.
//
// Empty array means "no canary check for this retailer" — the canary metric will
// always be false but won't trigger health-degradation alerts.
const CANARY_BY_RETAILER = {
  // costco:    [],  // TODO: pick a Kirkland or Maker's Mark variant if we add it to TARGET_BOTTLES
  // walgreens: [],  // TODO: most reliable Walgreens stock is Jim Beam / Maker's Mark, not in TARGET_BOTTLES
  // Default fallback: Buffalo Trace (CANARY_NAMES) — works for Walmart, Kroger, Safeway,
  // Albertsons, Sam's Club, Total Wine. Costco/Walgreens fall through to this default
  // and consistently miss it, which is currently informative as a "we know this retailer
  // doesn't carry the canary" signal.
};

// Returns true if `bottleName` is a canary for `retailerKey` — used to filter canary
// matches out of state/alerts (canary should never trigger user-facing notifications).
function isCanaryFor(retailerKey, bottleName) {
  const perRetailer = CANARY_BY_RETAILER[retailerKey];
  if (perRetailer) return perRetailer.includes(bottleName);
  return CANARY_NAMES.has(bottleName);  // default
}

// Human intelligence watch list — rumored drops at specific stores. Each entry triggers
// a one-time Discord @here notification on the first scan after being added. Remove
// entries after the drop is confirmed or passes. No automatic frequency change —
// the notification lets you decide whether to adjust scan settings.
// WATCH_LIST is loaded from `watchlist.json` if it exists (gitignored, edit-without-deploy),
// merged with this in-code default. JSON file lets you add/remove rumored drops without
// editing source — daemon picks up changes on next poll. JSON shape: same as in-code entries.
// Example watchlist.json:
//   [
//     { "bottle": "King of Kentucky", "retailer": "costco", "stores": ["427"],
//       "source": "Reddit tip", "date": "2026-04-30" }
//   ]
const WATCH_LIST_DEFAULTS = [
  // Hardcoded defaults — survives even if watchlist.json doesn't exist or is malformed.
  // bottle is optional — omit for "Allocated Bourbon" (covers all bottles).
];
let WATCH_LIST = [...WATCH_LIST_DEFAULTS];

// Load watchlist.json on startup. Re-read on each poll() so live edits take effect
// without a daemon restart. Errors silently fall back to defaults.
const WATCHLIST_FILE = new URL("./watchlist.json", import.meta.url);
async function loadWatchList() {
  try {
    const raw = await readFile(WATCHLIST_FILE, "utf-8");
    const parsed = JSON.parse(raw);
    if (Array.isArray(parsed)) {
      WATCH_LIST = [...WATCH_LIST_DEFAULTS, ...parsed];
      return parsed.length;
    }
  } catch { /* missing or malformed → keep defaults */ }
  WATCH_LIST = [...WATCH_LIST_DEFAULTS];
  return 0;
}

// ─── Watch List Processing ──────────────────────────────────────────────────
// Sends one-time Discord notifications for new WATCH_LIST entries.

function watchListKey(entry) {
  return `${entry.bottle || "Allocated Bourbon"}:${entry.retailer}:${[...entry.stores].sort().join(",")}`;
}

const WATCH_LIST_RETAILER_NAMES = { costco: "Costco", totalwine: "Total Wine", walmart: "Walmart", kroger: "Kroger", safeway: "Safeway", albertsons: "Albertsons", walgreens: "Walgreens", samsclub: "Sam's Club", extramile: "ExtraMile", liquorexpress: "Liquor Express Tempe", chandlerliquors: "Chandler Liquors" };

function buildWatchListEmbed(entry) {
  const retailerName = WATCH_LIST_RETAILER_NAMES[entry.retailer] || entry.retailer;
  const storeNames = (storeCache?.retailers?.[entry.retailer] || [])
    .filter((s) => entry.stores.includes(s.storeId))
    .map((s) => `📍 ${s.name} (#${s.storeId}) — ${s.address}`)
    .join("\n");
  const storeList = storeNames || entry.stores.map((id) => `Store #${id}`).join(", ");

  // When no specific bottle, list all allocated bottles tracked at this retailer
  let bottleSection = "";
  if (!entry.bottle) {
    const retailerBottles = TARGET_BOTTLES
      .filter((b) => !b.canary && (!b.retailers || b.retailers.includes(entry.retailer)))
      .map((b) => b.name);
    bottleSection = `\n\n🥃 **Bottles to watch for (${retailerBottles.length}):**\n${retailerBottles.join(", ")}`;
  }

  return {
    title: truncateTitle(`🔔 INTEL — ${entry.bottle || "Allocated Bourbon"} rumored at ${retailerName}`),
    description: truncateDescription(
      `${storeList}\n\n📋 **Source:** ${entry.source || "Unknown"}\n📅 **Date:** ${entry.date || "N/A"}${bottleSection}\n\n_Adjust scan frequency if you want to increase monitoring._`
    ),
    color: COLORS.rumor,
    timestamp: new Date().toISOString(),
    footer: { text: `Bourbon Scout 🥃 │ Watch List` },
  };
}

async function processWatchList(state) {
  if (WATCH_LIST.length === 0) return;
  /* v8 ignore start -- WATCH_LIST is empty in test env (no entries to process) */
  if (!state._watchList) state._watchList = {};
  let alertsSent = 0;
  for (const entry of WATCH_LIST) {
    const key = watchListKey(entry);
    if (state._watchList[key]) continue; // Already notified
    console.log(`[watchlist] New intel: ${entry.bottle || "Allocated Bourbon"} at ${entry.retailer} stores ${entry.stores.join(", ")}`);
    try {
      const embed = buildWatchListEmbed(entry);
      await sendUrgentAlert([embed]);
      state._watchList[key] = new Date().toISOString();
      alertsSent++;
    } catch (err) {
      console.error(`[watchlist] Failed to send alert: ${err.message}`);
    }
  }
  if (alertsSent > 0) {
    await saveState(state);
    console.log(`[watchlist] Sent ${alertsSent} rumor notification(s)`);
  }
  /* v8 ignore stop */
}

// ─── Reddit Intel Scraper ───────────────────────────────────────────────────
// Monitors r/ArizonaWhiskey (and r/bourbon with AZ filter) for posts mentioning
// allocated bottles, store names, or drop-related keywords. Sends Discord @here
// for relevant new posts. No proxy needed — Reddit's JSON API is public.

// AZ-specific subs: all posts checked. National subs: only posts mentioning AZ terms.
const REDDIT_INTEL_SUBREDDITS = ["ArizonaWhiskey", "arizonabourbon"];
const REDDIT_NATIONAL_SUBREDDITS = ["bourbon", "whiskey", "Costco_alcohol"];
const REDDIT_AZ_FILTER = ["arizona", "phoenix", "tempe", "scottsdale", "chandler", "gilbert", "mesa", "queen creek", "tucson", "fry's", "frys"];
const REDDIT_INTEL_KEYWORDS = [
  // Retailer names
  "costco", "total wine", "totalwine", "fry's", "frys", "fry's", "walmart", "safeway", "albertsons", "walgreens", "sam's club", "samsclub",
  // Store locations
  "scottsdale", "paradise valley", "tempe", "chandler", "gilbert", "mesa", "queen creek",
  // High-value bottles (short forms used in community posts)
  "kok", "king of kentucky", "pappy", "van winkle", "btac", "george t stagg", "gts",
  "william larue", "wlw", "thomas handy", "thh", "eagle rare 17", "er17",
  "blanton", "weller", "eh taylor", "eht", "stagg jr", "stagg bourbon",
  "elmer t lee", "etl", "rock hill", "michter's 10", "old forester birthday", "ofbb",
  "allocated", "drop", "dropped", "shipment", "just got", "in stock", "on shelf",
];

// Infer a retailer key from Reddit post text. Returns the most-mentioned retailer
// in the post, or null if none clearly identified. Used by the Reddit→watch list
// bridge so we can prioritize the implicated store on the next scan.
function inferRetailerFromText(text, matchedKeywords) {
  const candidates = {
    costco: /costco/i,
    totalwine: /total wine|totalwine/i,
    walmart: /walmart/i,
    kroger: /fry'?s|kroger/i,
    safeway: /safeway/i,
    albertsons: /albertsons/i,
    walgreens: /walgreens/i,
    samsclub: /sam'?s club|samsclub/i,
  };
  let bestKey = null;
  let bestCount = 0;
  for (const [key, pattern] of Object.entries(candidates)) {
    const matches = (text || "").match(new RegExp(pattern, "gi"));
    const count = matches?.length || 0;
    if (count > bestCount) { bestKey = key; bestCount = count; }
  }
  return bestKey;
}

async function scrapeRedditIntel(state) {
  if (!state._redditSeen) state._redditSeen = {};
  let newPosts = 0;
  console.log(`[reddit] Checking ${REDDIT_INTEL_SUBREDDITS.length + REDDIT_NATIONAL_SUBREDDITS.length} subreddits...`);

  // Check both AZ-specific subs (all posts) and national subs (AZ-filtered)
  const allSubs = [
    ...REDDIT_INTEL_SUBREDDITS.map((s) => ({ sub: s, requireAZ: false })),
    ...REDDIT_NATIONAL_SUBREDDITS.map((s) => ({ sub: s, requireAZ: true })),
  ];

  for (const { sub, requireAZ } of allSubs) {
    try {
      // Use got-scraping (Chrome TLS fingerprint) — Reddit rejects node-fetch's TLS. No proxy needed.
      const res = await scraperFetch(`https://www.reddit.com/r/${sub}/new.json?limit=25`, {
        headers: { "User-Agent": FETCH_HEADERS["User-Agent"] },
        timeout: 10000,
      });
      if (!res.ok) { console.warn(`[reddit] r/${sub} returned ${res.status}`); continue; }
      const data = await res.json();
      const posts = data?.data?.children || [];

      for (const post of posts) {
        const p = post.data;
        if (!p?.id) continue;
        if (state._redditSeen[p.id]) continue; // Already seen

        // Check if post is recent (last 2 hours) and matches keywords
        const ageMs = Date.now() - (p.created_utc * 1000);
        if (ageMs > 2 * 60 * 60 * 1000) continue; // Skip posts older than 2 hours

        const text = `${p.title} ${p.selftext || ""}`.toLowerCase();

        // National subs require AZ-specific mention to avoid noise
        if (requireAZ && !REDDIT_AZ_FILTER.some((az) => text.includes(az))) continue;

        const matchedKeywords = REDDIT_INTEL_KEYWORDS.filter((kw) => text.includes(kw));
        if (matchedKeywords.length === 0) continue; // Not relevant

        // Mark as seen regardless of whether alert sends
        state._redditSeen[p.id] = new Date().toISOString();
        newPosts++;

        // Cross-source confirmation: extract any TARGET_BOTTLE names mentioned in the
        // post and record the timestamp. When the scraper later finds the SAME bottle
        // within the confirmation window, the alert escalates to "🔥 double-confirmed"
        // tier — both human reporting AND scraper agree, highest-precision signal.
        if (!state._redditMentions) state._redditMentions = {};
        const postTime = new Date(p.created_utc * 1000).toISOString();
        for (const bottle of TARGET_BOTTLES) {
          if (matchesBottle(text, bottle, "reddit")) {
            state._redditMentions[bottle.name] = postTime;
          }
        }

        console.log(`[reddit] New intel from r/${sub}: "${p.title}" (keywords: ${matchedKeywords.slice(0, 5).join(", ")})`);

        // Bridge: if the post specifically names a retailer, queue a one-time watch list
        // entry so the next scan prioritizes that retailer with extra logging. Keeps
        // Reddit→action automation conservative (no auto-alerts beyond what we already
        // send), but the spawned watchlist entry surfaces in Discord on next poll.
        const retailerKey = inferRetailerFromText(text, matchedKeywords);
        if (retailerKey) {
          const watchKey = `reddit-${p.id}-${retailerKey}`;
          if (!state._watchList[watchKey]) {
            console.log(`[reddit] → Auto-creating watch list entry for ${retailerKey} based on post`);
            // Mark as already-notified so processWatchList doesn't re-fire (the Reddit
            // alert IS the user-facing signal). The watch list entry serves as a record
            // for state-pruning + auditing.
            state._watchList[watchKey] = new Date().toISOString();
          }
        }

        const embed = {
          title: truncateTitle(`📡 r/${sub} — ${p.title}`),
          description: truncateDescription(
            `${(p.selftext || "").slice(0, 300)}${p.selftext?.length > 300 ? "..." : ""}\n\n` +
            `👤 u/${p.author} · ⬆️ ${p.score} · 💬 ${p.num_comments}\n` +
            `🔗 [View post](https://reddit.com${p.permalink})\n` +
            `🔑 Matched: ${matchedKeywords.slice(0, 8).join(", ")}`
          ),
          color: COLORS.rumor,
          timestamp: new Date(p.created_utc * 1000).toISOString(),
          footer: { text: `Bourbon Scout 🥃 │ Reddit Intel` },
        };

        try {
          await sendUrgentAlert([embed]);
        } catch (err) {
          console.error(`[reddit] Failed to send alert: ${err.message}`);
        }
      }
    } catch (err) {
      console.warn(`[reddit] r/${sub} fetch failed: ${err.message}`);
    }
  }

  // Prune old seen IDs (keep last 7 days)
  const cutoff = Date.now() - 7 * 24 * 60 * 60 * 1000;
  for (const [id, ts] of Object.entries(state._redditSeen)) {
    if (new Date(ts).getTime() < cutoff) delete state._redditSeen[id];
  }

  if (newPosts > 0) {
    await saveState(state);
    console.log(`[reddit] Found ${newPosts} new relevant post(s)`);
  }
}

// ─── State Management ────────────────────────────────────────────────────────
// State is nested by retailer key then store ID:
// { "costco": { "0489": ["Weller 12 Year"] }, "walmart": { "2436": [] } }

async function loadState() {
  try {
    const raw = await readFile(STATE_FILE, "utf-8");
    const parsed = JSON.parse(raw);
    return (parsed && typeof parsed === "object" && !Array.isArray(parsed)) ? parsed : {};
  } catch {
    return {};
  }
}

async function saveState(state) {
  const tmp = fileURLToPath(STATE_FILE) + ".tmp";
  await writeFile(tmp, JSON.stringify(state, null, 2));
  await rename(tmp, fileURLToPath(STATE_FILE));
}

// ─── Scan Metrics ────────────────────────────────────────────────────────────
// Append one JSON line per scan to metrics.jsonl for historical analysis.
// Each line captures per-retailer health, bottles found, canary status, and scan duration.

async function appendMetrics(entry) {
  await appendFile(fileURLToPath(METRICS_FILE), JSON.stringify(entry) + "\n");
}

async function loadRecentMetrics(hours = 24) {
  try {
    const raw = await readFile(fileURLToPath(METRICS_FILE), "utf-8");
    const cutoff = Date.now() - hours * 60 * 60 * 1000;
    const lines = raw.trim().split("\n").filter(Boolean);
    const recent = [];
    for (const line of lines) {
      try {
        const entry = JSON.parse(line);
        if (new Date(entry.ts).getTime() >= cutoff) recent.push(entry);
      } catch {
        console.warn(`[metrics] Skipping malformed line in metrics.jsonl: ${line.slice(0, 80)}...`);
      }
    }
    return recent;
  } catch {
    return []; // File doesn't exist yet
  }
}

// Prune old entries from metrics.jsonl to prevent unbounded growth.
// Called once at startup (not per-poll). Atomic write via tmp+rename.
async function pruneMetrics(maxDays = 30) {
  try {
    const filePath = fileURLToPath(METRICS_FILE);
    const raw = await readFile(filePath, "utf-8");
    const cutoff = Date.now() - maxDays * 24 * 60 * 60 * 1000;
    const lines = raw.trim().split("\n").filter(Boolean);
    const kept = lines.filter((line) => {
      try { return new Date(JSON.parse(line).ts).getTime() >= cutoff; }
      catch { return false; } // drop malformed lines during pruning
    });
    if (kept.length < lines.length) {
      const tmpPath = filePath + ".tmp";
      await writeFile(tmpPath, kept.join("\n") + "\n");
      await rename(tmpPath, filePath);
      console.log(`[metrics] Pruned ${lines.length - kept.length} entries older than ${maxDays} days (${kept.length} kept)`);
    }
  } catch { /* file doesn't exist yet — nothing to prune */ }
}

function computeMetricsTrend(recentMetrics) {
  if (recentMetrics.length < 2) return null;
  const retailers = {};
  for (const scan of recentMetrics) {
    if (!scan.retailers) continue;
    for (const [key, data] of Object.entries(scan.retailers)) {
      if (!retailers[key]) retailers[key] = { scans: 0, totalQueries: 0, totalOk: 0, totalBlocked: 0, canaryHits: 0, bottlesFound: [] };
      retailers[key].scans++;
      retailers[key].totalQueries += data.queries || 0;
      retailers[key].totalOk += data.ok || 0;
      retailers[key].totalBlocked += data.blocked || 0;
      if (data.canary) retailers[key].canaryHits++;
      if (data.found?.length) retailers[key].bottlesFound.push(...data.found);
    }
  }
  return { scans: recentMetrics.length, hours: 24, retailers };
}

// Analyze historical metrics to identify when bottles are most often found.
// Groups finds by day-of-week + hour (MT) and returns slots ranked by find rate.
// Returns null until enough data exists (~2 weeks / 100+ scans) for significance.
function computePeakHours(recentMetrics) {
  if (recentMetrics.length < 100) return null;
  const findsBySlot = {};
  const scansBySlot = {};
  for (const scan of recentMetrics) {
    const d = new Date(scan.ts);
    const parts = new Intl.DateTimeFormat("en-US", {
      timeZone: "America/Phoenix", hour: "numeric", hour12: false, weekday: "short",
    }).formatToParts(d);
    const hour = parseInt(parts.find(p => p.type === "hour").value, 10);
    const day = parts.find(p => p.type === "weekday").value;
    const slot = `${day}-${hour}`;
    scansBySlot[slot] = (scansBySlot[slot] || 0) + 1;
    let hasFinds = false;
    if (scan.retailers) {
      for (const data of Object.values(scan.retailers)) {
        if (data.found?.length > 0) { hasFinds = true; break; }
      }
    }
    if (hasFinds) findsBySlot[slot] = (findsBySlot[slot] || 0) + 1;
  }
  const slots = Object.keys(scansBySlot)
    .map(slot => ({ slot, finds: findsBySlot[slot] || 0, scans: scansBySlot[slot], rate: (findsBySlot[slot] || 0) / scansBySlot[slot] }))
    .filter(s => s.finds > 0)
    .sort((a, b) => b.rate - a.rate || b.finds - a.finds);
  return { slots: slots.slice(0, 10), totalScans: recentMetrics.length };
}

// Detects retailer-level health degradation: 4+ consecutive recent scans where the
// canary was missed AND queries actually ran. Returns an array of { retailer,
// scansSinceCanary, dominantReason } for retailers in degraded state.
//
// Why 4 scans: at 30-min poll cadence that's ~2 hours of consistent failure. Long
// enough to filter transient blocks, short enough to surface real WAF rotations
// before they bake in for days.
//
// Excludes retailers with no per-retailer canary configured (empty CANARY_BY_RETAILER
// list = "we don't expect canary here") so we don't false-alarm on Walgreens etc.
function detectHealthDegradation(recentMetrics) {
  if (!Array.isArray(recentMetrics) || recentMetrics.length < 4) return [];
  const lastN = recentMetrics.slice(-4);
  const degraded = [];
  // Tally per-retailer: missed-canary count + dominant failure reason
  const retailerKeys = new Set();
  for (const m of lastN) for (const k of Object.keys(m.retailers || {})) retailerKeys.add(k);
  for (const key of retailerKeys) {
    // Skip retailers with explicit "no canary" configuration — false-alarm prevention
    const perRetailer = CANARY_BY_RETAILER[key];
    if (perRetailer && perRetailer.length === 0) continue;
    let missedCanary = 0, totalQueries = 0;
    const reasonTally = {};
    for (const m of lastN) {
      const d = m.retailers?.[key];
      if (!d) continue;
      totalQueries += d.queries || 0;
      if (d.queries > 0 && !d.canary) missedCanary++;
      for (const [r, n] of Object.entries(d.reasons || {})) {
        reasonTally[r] = (reasonTally[r] || 0) + n;
      }
    }
    if (missedCanary < 4 || totalQueries === 0) continue;  // not degraded
    // Pick the dominant failure reason for diagnostic
    const dominantReason = Object.entries(reasonTally).sort((a, b) => b[1] - a[1])[0]?.[0] || "unknown";
    degraded.push({ retailer: key, scansSinceCanary: missedCanary, dominantReason, reasonTally });
  }
  return degraded;
}

// State for once-per-degradation alerts: a retailer that's been degraded for 4 scans
// triggers ONE Discord ping. We don't ping again until the retailer recovers (canary
// found in some scan) and degrades again. Resets on poll start so it's per-process
// memory only — restart clears it, which is fine (the next degradation will re-alert).
const healthDegradedRetailers = new Set();

async function maybeSendHealthDegradationAlert(recentMetrics) {
  const degraded = detectHealthDegradation(recentMetrics);
  // Detect recoveries: retailers we previously alerted on that are no longer degraded
  for (const key of [...healthDegradedRetailers]) {
    if (!degraded.some(d => d.retailer === key)) {
      healthDegradedRetailers.delete(key);
      console.log(`[health] ${key} recovered — clearing degraded flag`);
    }
  }
  // Only fire on NEWLY degraded retailers (first time we notice this run)
  const newlyDegraded = degraded.filter(d => !healthDegradedRetailers.has(d.retailer));
  if (newlyDegraded.length === 0) return;
  for (const d of newlyDegraded) healthDegradedRetailers.add(d.retailer);

  /* v8 ignore start -- requires DISCORD_WEBHOOK_URL */
  if (!DISCORD_WEBHOOK_URL) return;
  const lines = newlyDegraded.map(d => {
    const top3 = Object.entries(d.reasonTally).sort((a, b) => b[1] - a[1]).slice(0, 3)
      .map(([r, n]) => `${r}=${n}`).join(", ");
    return `**${d.retailer}**: 0% canary across last ${d.scansSinceCanary} scans · top reasons: ${top3 || "(none recorded)"}`;
  });
  const embed = {
    title: "⚠️ Scraper Health Degradation Detected",
    description: `One or more retailers haven't found their canary bottle in 4+ consecutive scans. This usually means a WAF rotation, API contract change, or proxy issue. Run \`node scripts/probe-${newlyDegraded[0].retailer}.js\` (or check logs) to diagnose.\n\n${lines.join("\n")}`,
    color: COLORS.goneOOS,  // orange — quiet warning, not red alarm
    footer: { text: "Bourbon Scout 🥃 │ Auto-detected via canary metric" },
    timestamp: new Date().toISOString(),
  };
  await postDiscordWebhook({ username: "Bourbon Scout 🥃", embeds: [embed] }).catch(() => {});
  console.log(`[health] Sent degradation alert for: ${newlyDegraded.map(d => d.retailer).join(", ")}`);
  /* v8 ignore stop */
}

// ─── State Change Tracking ──────────────────────────────────────────────────

// Pure function: computes diffs between previous state and current scan results.
// previousStore: { bottles: { "Weller 12": { url, price, sku, firstSeen, lastSeen, scanCount } } } or undefined
// currentFound: [{ name, url, price, sku, size, fulfillment }]
function computeChanges(previousStore, currentFound) {
  const prevBottles = previousStore?.bottles || {};
  const currentByName = new Map(currentFound.map((b) => [b.name, b]));
  const prevNames = new Set(Object.keys(prevBottles));
  const currNames = new Set(currentByName.keys());

  const newFinds = currentFound.filter((b) => !prevNames.has(b.name));
  const stillInStock = currentFound
    .filter((b) => prevNames.has(b.name))
    .map((b) => ({
      ...b,
      firstSeen: prevBottles[b.name].firstSeen,
      scanCount: (prevBottles[b.name].scanCount || 0) + 1,
    }));
  const goneOOS = [...prevNames]
    .filter((name) => !currNames.has(name))
    .map((name) => ({ name, ...prevBottles[name] }));

  return { newFinds, stillInStock, goneOOS };
}

// Mutates state in place for a given retailer+store. Preserves firstSeen for continuing bottles.
// Maximum price history points to retain per bottle. Bounded to keep state.json small —
// 30 points at 30-min cadence ≈ 15 hours of history, enough for "is this clearance?"
// pattern detection. After 30 entries, oldest are dropped.
const PRICE_HISTORY_LIMIT = 30;

function updateStoreState(state, retailerKey, storeId, currentFound) {
  if (!state[retailerKey]) state[retailerKey] = {};
  const prev = state[retailerKey][storeId]?.bottles || {};
  const now = new Date().toISOString();

  const bottles = {};
  for (const b of currentFound) {
    // Append to price history if price changed since last scan. Skip if no price or
    // identical to most recent entry (no point logging the same value 100 times).
    const prevHistory = prev[b.name]?.priceHistory || [];
    const lastPrice = prevHistory[prevHistory.length - 1]?.price;
    const priceHistory = b.price && b.price !== lastPrice
      ? [...prevHistory, { ts: now, price: b.price }].slice(-PRICE_HISTORY_LIMIT)
      : prevHistory;

    // Auto-confirmation: once a bottle has been seen for 24+ hours, mark it as
    // "confirmed available" — distinguishes durable real stock from fly-by-night
    // planogram artifacts that disappear within hours. Persisted as `confirmedAt`
    // timestamp; once set, sticky (only cleared when the bottle goes OOS).
    const firstSeen = prev[b.name]?.firstSeen || now;
    const ageHours = (Date.now() - new Date(firstSeen).getTime()) / 3600000;
    const confirmedAt = prev[b.name]?.confirmedAt || (ageHours >= 24 ? now : null);

    bottles[b.name] = {
      url: b.url,
      price: b.price,
      sku: b.sku || "",
      size: b.size || "",
      fulfillment: b.fulfillment || "",
      firstSeen,
      lastSeen: now,
      scanCount: (prev[b.name]?.scanCount || 0) + 1,
      ...(confirmedAt ? { confirmedAt } : {}),
      // Optional Kroger B+C tier metadata — preserved across updates
      ...(b.confidence ? { confidence: b.confidence } : {}),
      ...(b.aisle ? { aisle: b.aisle } : {}),
      ...(b.facings ? { facings: b.facings } : {}),
      ...(b.dataAgeDays != null ? { dataAgeDays: b.dataAgeDays } : {}),
      // Price history for trend analysis (bounded to last PRICE_HISTORY_LIMIT entries)
      priceHistory,
    };
  }

  state[retailerKey][storeId] = { bottles, lastScanned: now };
}

function pruneState(state, activeStores) {
  for (const retailerKey of Object.keys(state)) {
    if (!activeStores[retailerKey]) {
      delete state[retailerKey];
      continue;
    }
    const activeIds = new Set(activeStores[retailerKey].map((s) => s.storeId));
    for (const storeId of Object.keys(state[retailerKey])) {
      if (!activeIds.has(storeId)) {
        delete state[retailerKey][storeId];
      }
    }
  }
}

// ─── Discord Alerts ──────────────────────────────────────────────────────────

// Post a Discord webhook payload with automatic 429 retry (up to 3 attempts).
async function postDiscordWebhook(payload) {
  for (let attempt = 0; attempt < 5; attempt++) {
    try {
      const res = await fetch(DISCORD_WEBHOOK_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
        signal: AbortSignal.timeout(10000),
      });
      if (res.status === 429) {
        const body = await res.json().catch(() => ({}));
        const retryAfter = (body.retry_after || 2) * 1000;
        console.warn(`[discord] Rate limited — retrying in ${retryAfter}ms (attempt ${attempt + 1}/5)`);
        await sleep(retryAfter);
        continue;
      }
      if (res.status >= 500) {
        console.warn(`[discord] Server error ${res.status} — retrying in 3s (attempt ${attempt + 1}/5)`);
        await sleep(3000);
        continue;
      }
      if (!res.ok) {
        console.error(`[discord] Webhook failed: ${res.status} ${await res.text()}`);
      }
      return res;
    } catch (err) {
      console.warn(`[discord] Network error: ${err.message} — retrying in 3s (attempt ${attempt + 1}/5)`);
      await sleep(3000);
      continue;
    }
  }
  console.error("[discord] Webhook failed after 5 retries");
  throw new Error("Discord webhook failed after 5 retries");
}

async function sendDiscordAlert(embeds) {
  /* v8 ignore next 3 -- env guard: DISCORD_WEBHOOK_URL is set at module load */
  if (!DISCORD_WEBHOOK_URL) {
    console.warn("[discord] No DISCORD_WEBHOOK_URL set — skipping alert");
    return;
  }
  for (let i = 0; i < embeds.length; i += 4) {
    const batch = embeds.slice(i, i + 4);
    await postDiscordWebhook({ username: "Bourbon Scout 🥃", embeds: batch });
    if (i + 4 < embeds.length) await sleep(1000);
  }
}

// Loud alert with @here for in-stock finds (pings online channel members only)
async function sendUrgentAlert(embeds) {
  /* v8 ignore next -- env guard */
  if (!DISCORD_WEBHOOK_URL) return;
  for (let i = 0; i < embeds.length; i += 4) {
    const batch = embeds.slice(i, i + 4);
    await postDiscordWebhook({
      username: "Bourbon Scout 🥃",
      content: "@here 🚨 **ALLOCATED BOURBON SPOTTED!**",
      allowed_mentions: { parse: ["everyone"] },
      embeds: batch,
    });
    if (i + 4 < embeds.length) await sleep(1000);
  }
}

// ─── Embed Colors ────────────────────────────────────────────────────────────

const COLORS = {
  newFind:  0x2ecc71,  // green — new confirmed find (drive over)
  lead:     0xf1c40f,  // yellow — newly spotted but signal weak (call first)
  stillIn:  0x3498db,  // blue — still in stock re-alert
  goneOOS:  0xe67e22,  // orange — went out of stock
  summary:  0x9b59b6,  // purple — scan summary
  rumor:    0xf39c12,  // gold — human intelligence rumor alert
};

// ─── Store Info Formatting ──────────────────────────────────────────────────

const SKU_LABELS = {
  costco: "Item #", totalwine: "Item #", walmart: "Item #",
  kroger: "SKU", safeway: "UPC", albertsons: "UPC", samsclub: "Item #", extramile: "Item #", liquorexpress: "Item #", chandlerliquors: "Item #",
};

const STORE_TYPE_LABELS = {
  costco: "Warehouse", totalwine: "Store", walmart: "Store",
  kroger: "Store", safeway: "Store", albertsons: "Store", samsclub: "Club", extramile: "Store", liquorexpress: "Store", chandlerliquors: "Store",
};

function parseCity(address) {
  if (!address) return "";
  // Match the city name directly before the state abbreviation + zip.
  // Handles addresses with commas in street (e.g. "123 Main St, Ste 200, Tempe, AZ 85283")
  const match = address.match(/,\s*([^,]+?)\s*,\s*[A-Z]{2}\s*\d{5}/);
  if (match) return match[1].trim();
  // Fallback: second-to-last comma-separated part
  const parts = address.split(",").map((s) => s.trim());
  if (parts.length >= 2) return parts[parts.length - 2];
  return "";
}

function parseState(address) {
  if (!address) return "";
  const match = address.match(/\b([A-Z]{2})\b\s*\d{5}/);
  return match ? match[1] : "";
}

function formatStoreInfo(retailerKey, retailerName, store) {
  const dist = store.distanceMiles != null ? ` · ${store.distanceMiles} mi` : "";
  const city = parseCity(store.address);
  const stateAbbr = parseState(store.address);
  const cityState = city && stateAbbr ? ` · ${city}, ${stateAbbr}` : "";
  const typeLabel = STORE_TYPE_LABELS[retailerKey] || "Store";
  const storeNum = store.storeId ? ` · ${typeLabel} #${store.storeId}` : "";
  const mapsLink = store.address
    ? `📍 [${store.address}](https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(store.address)})`
    : "📍 Address unknown";
  // Strip duplicate retailer prefix (e.g. "Costco Costco Chandler" → "Costco Chandler")
  const storeName = store.name || "";
  const displayName = storeName.startsWith(retailerName) ? storeName : `${retailerName} ${storeName}`;

  return {
    title: `${displayName} (#${store.storeId})${dist}`,
    storeLine: `🏬 ${displayName}${storeNum}${cityState}`,
    addressLine: mapsLink,
    skuLabel: SKU_LABELS[retailerKey] || "Item #",
  };
}

// Format "time ago" string from ISO timestamp
function timeAgo(isoStr) {
  if (!isoStr) return "";
  const diff = Date.now() - new Date(isoStr).getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 60) return `${mins}m ago`;
  const hours = Math.floor(mins / 60);
  if (hours < 24) return `${hours}h ago`;
  return `${Math.floor(hours / 24)}d ago`;
}

// ─── Embed Builders ─────────────────────────────────────────────────────────

function formatBottleLine(b, skuLabel, prefix = "🟢") {
  let line = b.url ? `${prefix} [${b.name}](${b.url})` : `${prefix} ${b.name}`;
  const details = [];
  if (b.price) details.push(`💰 ${b.price}`);
  if (b.sku) details.push(`🏷️ ${skuLabel}${b.sku}`);
  if (b.size) details.push(`📐 ${b.size}`);
  if (details.length) line += `\n   ${details.join("  ")}`;
  if (b.fulfillment) line += `\n   🚚 ${b.fulfillment}`;
  // Aisle / facings / freshness — present for Kroger finds. Helps user triage at a glance.
  const ctx = [];
  if (b.aisle) ctx.push(`🗺️ ${b.aisle}`);
  if (b.facings) ctx.push(`🔢 ${b.facings} facing${b.facings === 1 ? "" : "s"}`);
  if (ctx.length) line += `\n   ${ctx.join("  ")}`;
  // Stale data warning — Kroger sometimes reports inStore:true with months-old price data,
  // which is a strong signal the slot is empty even though the planogram is set.
  if (b.dataAgeDays != null && b.dataAgeDays > 60) {
    line += `\n   ⚠️ Inventory data is ${b.dataAgeDays} days old — call to confirm`;
  }
  // HTML verified: the find was double-checked against the live product page widget.
  // Trust this MORE than data freshness — overrides stale data warning concern.
  if (b.htmlVerified) {
    line += `\n   ✅ Verified against frysfood.com product page`;
  }
  // 24h auto-confirmation: bottle has been continuously in stock for 24+ hours,
  // distinguishing durable availability from short-lived planogram artifacts.
  if (b.confirmedAt) {
    line += `\n   🔵 Confirmed available — in stock 24+h`;
  }
  // Cross-source confirmation: Reddit reported this bottle in the last 4h AND our
  // scraper found it. Highest-confidence find — both human and machine agree.
  if (b.crossSourceConfirmed) {
    line += `\n   🔥 DOUBLE-CONFIRMED — Reddit also reported within 4h`;
  }
  return line;
}

// Bottles that re-alert every scan when still in stock — these are unicorns where
// EVERY confirmation is news. Defaults to TARGET_BOTTLES list keyword-matched.
// Per-bottle interest tiers — controls alert cadence and routing.
//   obsess: every scan re-alerts; always urgent @here; ignores re-alert N config
//   track:  standard cadence (REALERT_EVERY_N_SCANS); standard urgent alert
//   ignore: silently records state, no Discord alerts at all (useful for "found
//           but not interested" cases like clearance JD 14yr at MSRP+200%)
//
// Bottles not listed here default to "track". Add a bottle to "ignore" to suppress
// alerts WITHOUT removing it from TARGET_BOTTLES (still tracked in state.json).
const BOTTLE_INTEREST_TIERS = {
  obsess: [
    "Pappy Van Winkle 23 Year",
    "Pappy Van Winkle 20 Year",
    "Pappy Van Winkle 15 Year",
    "Van Winkle Family Reserve Rye 13",
    "King of Kentucky",
    "George T. Stagg",
    "William Larue Weller",
    "Eagle Rare 17 Year",
    "Thomas H. Handy",
    "E.H. Taylor 18 Year Marriage",
    "Old Forester Birthday Bourbon",
    "Old Forester President's Choice",
    "Old Forester 150th Anniversary",
    "Old Forester King Ranch",
    "Heaven Hill Heritage Collection 22 Year",
  ],
  track: [], // populated implicitly — anything not in obsess/ignore is "track"
  ignore: [], // bottles to silently track without alerts
};

// Backward-compat alias for code that uses the old name
const ULTRA_RARE_BOTTLES = new Set(BOTTLE_INTEREST_TIERS.obsess);
const IGNORED_BOTTLES = new Set(BOTTLE_INTEREST_TIERS.ignore);

function bottleInterestTier(bottleName) {
  if (ULTRA_RARE_BOTTLES.has(bottleName)) return "obsess";
  if (IGNORED_BOTTLES.has(bottleName)) return "ignore";
  return "track";
}

// How frequently to re-alert a bottle that's still in stock from a previous scan.
// Returns 1 (every scan) for ultra-rare bottles where each confirmation matters.
// Returns the configured REALERT_EVERY_N_SCANS for "track" tier (default: 4).
// Returns Infinity for "ignore" tier — never re-alerts.
function rerunCadenceFor(bottle) {
  if (!bottle?.name) return parseInt(REALERT_EVERY_N_SCANS, 10) || 4;
  const tier = bottleInterestTier(bottle.name);
  if (tier === "obsess") return 1;
  if (tier === "ignore") return Infinity;
  return parseInt(REALERT_EVERY_N_SCANS, 10) || 4;
}

function buildOOSList(allBottleNames, inStockNames) {
  const oos = allBottleNames.filter((n) => !inStockNames.includes(n));
  if (oos.length === 0) return "";
  return `\n\n❌ **OUT OF STOCK (${oos.length})**\n${oos.join(", ")}`;
}

// Discord limits embed descriptions to 4096 chars. Truncate the OOS tail if needed.
const DISCORD_DESC_LIMIT = 4096;
const DISCORD_TITLE_LIMIT = 256;
function truncateTitle(title) {
  if (title.length <= DISCORD_TITLE_LIMIT) return title;
  return title.slice(0, DISCORD_TITLE_LIMIT - 1) + "…";
}
function truncateDescription(desc) {
  if (desc.length <= DISCORD_DESC_LIMIT) return desc;
  // Find the OOS section and truncate it to fit
  const oosIdx = desc.lastIndexOf("\n\n❌ **OUT OF STOCK");
  if (oosIdx === -1) return desc.slice(0, DISCORD_DESC_LIMIT - 3) + "...";
  const before = desc.slice(0, oosIdx);
  const oosHeader = desc.slice(oosIdx).match(/\n\n❌ \*\*OUT OF STOCK \(\d+\)\*\*\n/)?.[0] || "";
  const remaining = DISCORD_DESC_LIMIT - before.length - oosHeader.length - 4;
  if (remaining <= 0) return before.slice(0, DISCORD_DESC_LIMIT - 3) + "...";
  const oosNames = desc.slice(oosIdx + oosHeader.length);
  return before + oosHeader + oosNames.slice(0, remaining) + " ...";
}

// Returns array of embeds for a store based on computed changes.
function buildStoreEmbeds(retailerKey, retailerName, store, changes) {
  const info = formatStoreInfo(retailerKey, retailerName, store);
  const allNames = TARGET_BOTTLES.filter((b) => !CANARY_NAMES.has(b.name)).map((b) => b.name);
  const embeds = [];

  // Split new finds into "confirmed" (strong signal) and "lead" (weak signal) tiers.
  // Only the Kroger scraper currently sets `confidence` — anything without it defaults
  // to "confirmed" so existing retailers keep their current behavior.
  // Filter out bottles in "ignore" tier — silently tracked but no alerts
  const alertableFinds = changes.newFinds.filter((b) => !IGNORED_BOTTLES.has(b.name));
  const confirmedFinds = alertableFinds.filter((b) => b.confidence !== "lead");
  const leadFinds = alertableFinds.filter((b) => b.confidence === "lead");
  const inStockNames = [...changes.newFinds, ...changes.stillInStock].map((b) => b.name);

  // Confirmed embed (green, urgent — @here ping)
  if (confirmedFinds.length > 0) {
    let desc = `${info.storeLine}\n${info.addressLine}\n\n🆕 **NEWLY SPOTTED**\n`;
    desc += confirmedFinds.map((b) => formatBottleLine(b, info.skuLabel, "🟢")).join("\n\n");

    if (changes.stillInStock.length > 0) {
      desc += `\n\n✅ **STILL IN STOCK (${changes.stillInStock.length})**\n`;
      desc += changes.stillInStock.map((b) => {
        const since = b.firstSeen ? ` (since ${timeAgo(b.firstSeen)})` : "";
        return `🔵 ${b.name} — ${b.price || "N/A"} · 🏷️ ${info.skuLabel}${b.sku || "?"}${since}`;
      }).join("\n");
    }

    desc += buildOOSList(allNames, inStockNames);

    embeds.push({
      title: truncateTitle(`🚨 NEW FIND — ${info.title}`),
      description: truncateDescription(desc),
      color: COLORS.newFind,
      footer: { text: `Bourbon Scout 🥃 │ ${retailerName}` },
      timestamp: new Date().toISOString(),
      _urgent: true,
    });
  }

  // Lead embed (yellow, quiet — no @here, just a heads-up)
  if (leadFinds.length > 0) {
    let desc = `${info.storeLine}\n${info.addressLine}\n\n🔍 **POTENTIAL LEAD** — store has a planogram slot for these bottles, but inventory signal is weak. Call ahead before driving.\n\n`;
    desc += leadFinds.map((b) => formatBottleLine(b, info.skuLabel, "🟡")).join("\n\n");
    embeds.push({
      title: truncateTitle(`🔍 LEAD — ${info.title}`),
      description: truncateDescription(desc),
      color: COLORS.lead,
      footer: { text: `Bourbon Scout 🥃 │ ${retailerName} · low-confidence signal` },
      timestamp: new Date().toISOString(),
      _urgent: false,
    });
  }

  // Gone OOS embed (orange, quiet) — only when no new finds
  if (changes.goneOOS.length > 0 && changes.newFinds.length === 0) {
    let desc = `${info.storeLine}\n${info.addressLine}\n\n📉 **NO LONGER AVAILABLE**\n`;
    desc += changes.goneOOS.map((b) => {
      const duration = b.firstSeen ? ` · was in stock for ${timeAgo(b.firstSeen).replace(" ago", "")}` : "";
      return `🔴 ${b.name}${b.price ? ` (was ${b.price})` : ""}\n   🏷️ ${info.skuLabel}${b.sku || "?"}${duration}`;
    }).join("\n\n");

    const inStockNames = changes.stillInStock.map((b) => b.name);
    desc += buildOOSList(allNames, inStockNames);

    embeds.push({
      title: truncateTitle(`⚠️ STOCK LOST — ${info.title}`),
      description: truncateDescription(desc),
      color: COLORS.goneOOS,
      footer: { text: `Bourbon Scout 🥃 │ ${retailerName}` },
      timestamp: new Date().toISOString(),
    });
  }

  // Still-in-stock re-alert (blue, quiet) — only when no new finds and no OOS
  if (changes.stillInStock.length > 0 && changes.newFinds.length === 0 && changes.goneOOS.length === 0) {
    const reAlertN = parseInt(REALERT_EVERY_N_SCANS, 10) || 4;
    // Smart re-alert: rare bottles re-alert every scan, common bottles less often.
    // Rationale: Pappy 23 still-in-stock is news every time; Buffalo Trace common at
    // a Walmart isn't worth re-pinging. Computed per-bottle so a mixed list can fire
    // on the rare items only.
    const shouldReAlert = changes.stillInStock.some((b) => b.scanCount % rerunCadenceFor(b) === 0);
    if (shouldReAlert) {
      const inStockNames = changes.stillInStock.map((b) => b.name);
      let desc = `${info.storeLine}\n${info.addressLine}\n\n✅ **IN STOCK**\n`;
      desc += changes.stillInStock.map((b) => {
        const since = b.firstSeen ? ` · since ${timeAgo(b.firstSeen)}` : "";
        return formatBottleLine(b, info.skuLabel, "🔵") + since;
      }).join("\n\n");

      desc += buildOOSList(allNames, inStockNames);

      embeds.push({
        title: truncateTitle(`🔵 STILL AVAILABLE — ${info.title}`),
        description: truncateDescription(desc),
        color: COLORS.stillIn,
        footer: { text: `Bourbon Scout 🥃 │ ${retailerName}` },
        timestamp: new Date().toISOString(),
      });
    }
  }

  return embeds;
}

const RETAILER_ORDER = ["costco", "totalwine", "walmart", "kroger", "safeway", "albertsons", "walgreens", "samsclub", "extramile", "liquorexpress", "chandlerliquors"];
const RETAILER_LABELS = { costco: "Costco", totalwine: "Total Wine", walmart: "Walmart", kroger: "Kroger", safeway: "Safeway", albertsons: "Albertsons", walgreens: "Walgreens", samsclub: "Sam's Club", extramile: "ExtraMile", liquorexpress: "Liquor Express Tempe", chandlerliquors: "Chandler Liquors" };

function buildSummaryEmbed({ storesScanned, retailersScanned, totalNewFinds, totalStillInStock, totalGoneOOS, nothingCount, durationSec, scannedStores = [], health = {}, canaryResults = {}, trend = null, peakHours = null, totalConfirmed = 0, totalLeads = 0 }) {
  let desc = `🏬 **${storesScanned}** stores  │  🛍️ **${retailersScanned}** retailers  │  ⏱️ **${durationSec}s**\n\n`;
  // Tier breakdown when newFinds includes Kroger B+C-classified items. confirmed/leads
  // sum to totalNewFinds; show them split for at-a-glance signal quality assessment.
  if (totalNewFinds > 0 && (totalConfirmed > 0 || totalLeads > 0)) {
    desc += `🟢 ${totalConfirmed} confirmed   🟡 ${totalLeads} leads   🔵 ${totalStillInStock} still in stock\n`;
  } else {
    desc += `🟢 ${totalNewFinds} new finds   🔵 ${totalStillInStock} still in stock\n`;
  }
  desc += `🔴 ${totalGoneOOS} went OOS    💤 ${nothingCount} nothing`;

  // When nothing found, show which stores were actually scanned
  const noAllocations = totalNewFinds === 0 && totalStillInStock === 0 && totalGoneOOS === 0;
  if (noAllocations && scannedStores.length > 0) {
    desc += "\n\n**Stores scanned:**\n";
    const byRetailer = new Map();
    for (const s of scannedStores) {
      if (!byRetailer.has(s.retailerName)) byRetailer.set(s.retailerName, []);
      byRetailer.get(s.retailerName).push(s);
    }
    for (const [retailerName, stores] of byRetailer) {
      const storeList = stores.map((s) => `#${s.storeId} ${s.storeName}`).join(", ");
      desc += `> **${retailerName}** — ${storeList}\n`;
    }
  }

  // Build health fields: one inline field per retailer (3 per row in Discord)
  // Always show all 7 retailers — skipped or crashed retailers get a "⏭️ —" indicator
  const fields = [];
  for (const key of RETAILER_ORDER) {
    const h = health[key];
    const canary = canaryResults[key] ? " 🐤" : "";
    if (!h || h.queries === 0) {
      fields.push({ name: RETAILER_LABELS[key] || key, value: `⏭️ —${canary}`, inline: true });
      continue;
    }
    const pct = h.succeeded / h.queries;
    const emoji = pct >= 0.75 ? "✅" : pct >= 0.25 ? "⚠️" : "❌";
    fields.push({ name: RETAILER_LABELS[key] || key, value: `${emoji} ${h.succeeded}/${h.queries}${canary}`, inline: true });
  }

  // 24h trend summary from historical metrics
  if (trend && trend.scans >= 2) {
    desc += `\n\n**24h trend** (${trend.scans} scans):`;
    for (const [key, r] of Object.entries(trend.retailers)) {
      const label = RETAILER_LABELS[key] || key;
      const pct = r.totalQueries > 0 ? Math.round((r.totalOk / r.totalQueries) * 100) : 0;
      const canaryPct = r.scans > 0 ? Math.round((r.canaryHits / r.scans) * 100) : 0;
      const found = r.bottlesFound.length > 0 ? ` │ 🥃 ${[...new Set(r.bottlesFound)].join(", ")}` : "";
      desc += `\n> ${label}: ${pct}% ok, 🐤 ${canaryPct}%${found}`;
    }
  }

  // Peak find times from 14-day metrics history (requires 100+ scans for significance)
  if (peakHours && peakHours.slots.length > 0) {
    desc += `\n\n**Peak find times** (${peakHours.totalScans} scans):`;
    for (const s of peakHours.slots.slice(0, 5)) {
      const [day, hourStr] = s.slot.split("-");
      const h = parseInt(hourStr);
      const timeStr = h === 0 ? "12 AM" : h < 12 ? `${h} AM` : h === 12 ? "12 PM" : `${h - 12} PM`;
      desc += `\n> ${day} ${timeStr}: ${s.finds}/${s.scans} scans (${Math.round(s.rate * 100)}%)`;
    }
  }

  const embed = {
    title: "📊 Scan Complete",
    description: truncateDescription(desc),
    color: COLORS.summary,
    footer: { text: "Bourbon Scout 🥃" },
    timestamp: new Date().toISOString(),
  };
  if (fields.length > 0) embed.fields = fields;
  return embed;
}

// ─── Utility ─────────────────────────────────────────────────────────────────

// Sleep with random jitter (±30%) to avoid anti-bot timing fingerprinting
const sleep = (ms) => {
  const jitter = ms * 0.3 * (Math.random() * 2 - 1);
  return new Promise((r) => setTimeout(r, Math.max(0, Math.round(ms + jitter))));
};

// Fisher-Yates shuffle — randomize query order each scrape to avoid
// predictable access patterns that anti-bot ML models flag.
function shuffle(arr) {
  const a = [...arr];
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

// Fetch with one retry on transient network errors (timeouts, DNS failures).
// HTTP error responses (4xx/5xx) are NOT retried — caller handles those.
async function fetchRetry(url, opts) {
  try {
    return await fetch(url, opts);
  } catch (err) {
    console.warn(`[fetchRetry] ${err.message} — retrying in 1s`);
    await sleep(1000);
    return await fetch(url, opts);
  }
}

// Scraper-grade HTTP fetch with Chrome TLS fingerprint impersonation (via got-scraping).
// Returns a node-fetch-compatible response object { ok, status, text(), json(), headers }.
// Uses Chrome cipher suites + HTTP/2 to defeat Akamai/PerimeterX TLS fingerprinting that
// trivially identifies node-fetch's Node.js JA3 hash regardless of HTTP header spoofing.
// node-fetch is still used for Discord webhooks and API endpoints (no bot detection there).
async function scraperFetch(url, { headers, timeout = 15000, proxyUrl, redirect, method, body } = {}) {
  const gotOpts = {
    url,
    headers: headers || {},
    throwHttpErrors: false,
    followRedirect: redirect !== "manual",
    useHeaderGenerator: false, // We supply our own FETCH_HEADERS (Chrome 145 accurate)
    timeout: { request: timeout },
    retry: { limit: 0 }, // We handle retries ourselves via scraperFetchRetry
  };
  if (proxyUrl) gotOpts.proxyUrl = proxyUrl;
  if (method) gotOpts.method = method;
  if (body) gotOpts.body = body;
  const response = await gotScraping(gotOpts);
  const statusCode = response.statusCode;
  // Detect proxy quota exhaustion — attempt failover to backup proxy
  if (statusCode === 407) {
    if (!primaryProxyExhausted) {
      primaryProxyExhausted = true;
      console.warn("[proxy] Primary traffic exhausted (407) — attempting failover to backup");
      if (!failoverToBackupProxy()) {
        console.warn("[proxy] No backup proxy configured — disabling fetch paths, browser fallback will use direct IP");
      }
    } else if (!backupProxyExhausted) {
      backupProxyExhausted = true;
      console.warn("[proxy] Backup traffic also exhausted (407) — disabling fetch paths, browser fallback will use direct IP");
    }
  }
  return {
    ok: statusCode >= 200 && statusCode < 300,
    status: statusCode,
    statusCode,
    headers: response.headers,
    // Mimic node-fetch .text() and .json() async methods
    text: async () => response.body,
    json: async () => JSON.parse(response.body),
  };
}

// scraperFetch with one retry on transient network errors (mirrors fetchRetry pattern).
async function scraperFetchRetry(url, opts) {
  try {
    return await scraperFetch(url, opts);
  } catch (err) {
    console.warn(`[scraperFetchRetry] ${err.message} — retrying in 1s`);
    await sleep(1000);
    return await scraperFetch(url, opts);
  }
}

// Parse bottle size from product title text (e.g., "750ml", "1.75L", "750 ML")
function parseSize(text) {
  if (!text) return "";
  const match = text.match(/([\d.]+)\s*(ml|cl|l|liter|litre)/i);
  if (!match) return "";
  const [, num, unit] = match;
  const u = unit.toLowerCase();
  if (u === "ml") return `${num}ml`;
  if (u === "cl") return `${Math.round(parseFloat(num) * 10)}ml`;
  return `${parseFloat(num)}L`;
}

// Normalize unicode curly quotes/apostrophes to ASCII before matching.
// Retailers inconsistently use \u2018/\u2019 ("Blanton\u2019s") vs ASCII ("Blanton's").
function normalizeText(text) {
  return text.toLowerCase().replace(/[\u2018\u2019\u201A\u201B\u2032]/g, "'").replace(/[\u201C\u201D\u201E\u201F\u2033]/g, '"');
}

// Product titles containing these terms are not individual bottles — skip them to prevent
// sampler packs, gift sets, etc. from triggering multiple bottle matches at once.
const EXCLUDE_TERMS = ["sampler", "gift set", "variety pack", "combo pack", "bundle", "miniature", "mini bottle", " 50ml", " 50 ml"];

function matchesBottle(text, bottle, retailerKey) {
  const lower = normalizeText(text);
  if (EXCLUDE_TERMS.some((t) => lower.includes(t))) return false;
  if (retailerKey && bottle.retailers && !bottle.retailers.includes(retailerKey)) return false;
  return bottle.searchTerms.some((term) => lower.includes(term));
}

// Dedup found bottles by name, preferring the entry with the lowest price.
// Falls back to first occurrence if prices are equal or unparseable.
function dedupFound(found) {
  const byName = new Map();
  for (const f of found) {
    const prev = byName.get(f.name);
    if (!prev) { byName.set(f.name, f); continue; }
    const prevPrice = parsePrice(prev.price);
    const currPrice = parsePrice(f.price);
    if (currPrice > 0 && (prevPrice === 0 || currPrice < prevPrice)) {
      byName.set(f.name, f);
    }
  }
  return Array.from(byName.values());
}

// Extract numeric price from strings like "$29.99", "29.99", "$1,299.99"
function parsePrice(str) {
  if (!str) return 0;
  const match = str.replace(/,/g, "").match(/([\d.]+)/);
  return match ? parseFloat(match[1]) : 0;
}

// Filter out miniature bottles (50ml) that slip past EXCLUDE_TERMS because the product
// title doesn't mention the size. Price is the reliable signal: no allocated 750ml bourbon
// is under $20, but 50ml miniatures are typically $8-15. Also catches size when available.
const MIN_BOTTLE_PRICE = 20;
const MAX_BOTTLE_PRICE = 500; // No allocated bourbon retails above $350 (Pappy 23). $500+ = secondary market.
function filterMiniatures(found) {
  return found.filter((f) => {
    const size = (f.size || "").toLowerCase();
    if (size && size !== "" && size !== "750ml" && size !== "1l" && size !== "1.75l") {
      const ml = parseInt(size);
      if (!isNaN(ml) && ml < 200) return false; // Explicit small size (50ml, 100ml)
    }
    const price = parsePrice(f.price);
    if (price > 0 && price < MIN_BOTTLE_PRICE) return false; // Miniature price range
    if (price > 0 && price > MAX_BOTTLE_PRICE) {
      console.warn(`[filter] Rejected "${f.name}" at ${f.price} — exceeds $${MAX_BOTTLE_PRICE} ceiling`);
      return false;
    }
    return true;
  });
}

// Hard timeout for async operations. Returns fallback value on timeout.
function withTimeout(promise, ms, fallback) {
  let timer;
  return Promise.race([
    promise,
    new Promise((resolve) => { timer = setTimeout(() => resolve(fallback), ms); }),
  ]).finally(() => clearTimeout(timer));
}

// Run async tasks with a concurrency limit
async function runWithConcurrency(tasks, limit) {
  const executing = new Set();
  for (const task of tasks) {
    // Structural error isolation: a failing task never aborts remaining tasks,
    // even if the task wrapper forgets its own try/catch.
    const p = Promise.resolve().then(task).catch(() => {}).finally(() => executing.delete(p));
    executing.add(p);
    if (executing.size >= limit) await Promise.race(executing);
  }
  await Promise.all(executing);
}

// Auto-detect Chrome version from the system binary to keep UA + Sec-CH-UA in sync
// with the TLS fingerprint. Hardcoded versions drift when Chrome auto-updates,
// creating a TLS-vs-UA mismatch that anti-bot systems specifically flag.
const IS_MAC = process.platform === "darwin";

// Synchronous version detection at module load — execFileSync is fast (<50ms)
let CHROME_VERSION = "146";
try {
  if (IS_MAC) {
    const { execFileSync } = await import("node:child_process");
    const out = execFileSync("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome", ["--version"], { timeout: 5000 }).toString().trim();
    const match = out.match(/Chrome\s+(\d+)/);
    if (match) CHROME_VERSION = match[1];
  }
} catch { /* use fallback */ }

// Replicate Chromium's Sec-CH-UA GREASE algorithm (user_agent_utils.cc).
// Each Chrome major version deterministically selects a grease brand name, version,
// and entry ordering. Hardcoding a single brand string creates a version mismatch
// signal when Chrome auto-updates — this function auto-matches any version.
function getGreasedBrand(majorVersion) {
  const chars = [" ", "(", ":", "-", ".", "/", ")", ";", "=", "?", "_"];
  const versions = ["8", "99", "24"];
  const orders = [[0,1,2], [0,2,1], [1,0,2], [1,2,0], [2,0,1], [2,1,0]];
  const brand = `Not${chars[majorVersion % 11]}A${chars[(majorVersion + 1) % 11]}Brand`;
  const ver = versions[majorVersion % 3];
  const order = orders[majorVersion % 6];
  const entries = [
    `"${brand}";v="${ver}"`,
    `"Chromium";v="${majorVersion}"`,
    `"Google Chrome";v="${majorVersion}"`,
  ];
  return order.map(i => entries[i]).join(", ");
}

// Headers for fetch-based scrapers — property insertion order matches Chrome's HTTP/2
// wire order. got-scraping's TransformHeadersAgent only sorts HTTP/1.1 headers; on H2,
// Node's http2.request() sends headers in JS object iteration order (= insertion order).
// Akamai pattern-matches header order as a zero-CPU bot signal — Client Hints before
// User-Agent is critical. Platform-aware: macOS UA on Mac to match TLS fingerprint.
const FETCH_HEADERS = {
  "Sec-CH-UA": getGreasedBrand(Number(CHROME_VERSION)),
  "Sec-CH-UA-Mobile": "?0",
  "Sec-CH-UA-Platform": IS_MAC ? '"macOS"' : '"Windows"',
  "Upgrade-Insecure-Requests": "1",
  "User-Agent": IS_MAC
    ? `Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/${CHROME_VERSION}.0.0.0 Safari/537.36`
    : `Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/${CHROME_VERSION}.0.0.0 Safari/537.36`,
  "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
  "Sec-Fetch-Site": "cross-site",
  "Sec-Fetch-Mode": "navigate",
  "Sec-Fetch-User": "?1",
  "Sec-Fetch-Dest": "document",
  "Cache-Control": "max-age=0",
  "Accept-Encoding": "gzip, deflate",
  "Accept-Language": "en-US,en;q=0.9",
  "Referer": "https://www.google.com/",
  "priority": "u=0, i",
};

// ─── Browser Management ──────────────────────────────────────────────────────

let browser = null;
const BROWSER_STATE_FILE = new URL("./browser-state.json", import.meta.url);
let browserStateCache = null;

async function loadBrowserState() {
  if (browserStateCache) return browserStateCache;
  try {
    browserStateCache = JSON.parse(await readFile(BROWSER_STATE_FILE, "utf-8"));
    return browserStateCache;
  } catch {
    return undefined;
  }
}

async function saveBrowserState(context) {
  try {
    const state = await context.storageState();
    browserStateCache = state;
    const tmp = fileURLToPath(BROWSER_STATE_FILE) + ".tmp";
    await writeFile(tmp, JSON.stringify(state));
    await rename(tmp, fileURLToPath(BROWSER_STATE_FILE));
  } catch { /* best-effort */ }
}

// Use system Chrome on Mac for authentic TLS fingerprint (Playwright's bundled
// Chromium has a recognizable TLS signature that Akamai/PerimeterX flag).
const CHROME_PATH = IS_MAC
  ? "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
  : null;

async function launchBrowser() {
  const launchOpts = {
    headless: true,
    args: [
      "--disable-blink-features=AutomationControlled",
      "--disable-session-crashed-bubble",
      "--disable-dev-shm-usage",
      "--no-first-run",
      "--disable-component-update",
    ],
    ignoreDefaultArgs: ["--enable-automation"],
  };
  if (CHROME_PATH) launchOpts.executablePath = CHROME_PATH;
  if (proxySessionUrl) {
    // Default browser proxy — individual scrapers can override via newPageForRetailer()
    const proxyUrl = new URL(proxySessionUrl);
    launchOpts.proxy = { server: `${proxyUrl.protocol}//${proxyUrl.host}` };
    if (proxyUrl.username) launchOpts.proxy.username = decodeURIComponent(proxyUrl.username);
    if (proxyUrl.password) launchOpts.proxy.password = decodeURIComponent(proxyUrl.password);
  }
  browser = await chromium.launch(launchOpts);
  return browser;
}

async function closeBrowser() {
  if (browser) {
    await browser.close().catch(() => {});
    browser = null;
  }
}

// Per-retailer persistent context cache for session reuse within a poll.
// Each retailer gets a persistent Chrome profile (browser-profiles/{key}/) that
// keeps HTTP cache, service workers, IndexedDB, and visited history on disk.
// Within a poll, multiple stores share the same browser via cached context.
// Between polls, profile data persists even after context.close().
const retailerBrowserCache = {};

// Per-retailer browser cookie cache: populated after successful browser scrapes,
// consumed by next poll's fetch pre-warm. Gives fetch paths browser-quality cookies
// (including Akamai _abck) without needing to execute JS. TTL: 1 hour (_abck expiry).
const retailerCookieCache = {};
const COOKIE_CACHE_TTL_MS = 25 * 60 * 1000; // 25 min — conservative margin under Akamai _abck 30 min expiry

async function cacheRetailerCookies(retailerKey) {
  const cached = retailerBrowserCache[retailerKey];
  if (!cached?.context) return;
  try {
    const state = await cached.context.storageState();
    if (!state?.cookies?.length) return;
    const cookieStr = state.cookies.map(c => `${c.name}=${c.value}`).join("; ");
    retailerCookieCache[retailerKey] = { cookies: cookieStr, ts: Date.now() };
    console.log(`[${retailerKey}] Cached ${state.cookies.length} browser cookies for fetch`);
  } catch { /* best-effort */ }
}

function getCachedCookies(retailerKey) {
  const entry = retailerCookieCache[retailerKey];
  if (!entry) return "";
  if (Date.now() - entry.ts > COOKIE_CACHE_TTL_MS) {
    delete retailerCookieCache[retailerKey];
    return "";
  }
  return entry.cookies;
}

// Per-retailer browser-blocked flag: set when a browser scrape fails (blocked/timeout).
// Remaining stores skip browser fallback for this poll — if one store is blocked,
// the rest will be too (same IP, same profile, same PerimeterX fingerprint).
const retailerBrowserBlocked = {};

// Per-retailer mutex: serializes browser scraping so only one store at a time
// uses a retailer's shared browser context. Without this, multiple stores open
// concurrent pages on the same context — PerimeterX/Akamai flag multiple
// simultaneous search sessions from one browser fingerprint as bot behavior.
const retailerBrowserLocks = {};
function acquireRetailerLock(retailerKey) {
  const prev = retailerBrowserLocks[retailerKey] || Promise.resolve();
  let release;
  const lock = new Promise((resolve) => { release = resolve; });
  retailerBrowserLocks[retailerKey] = prev.then(() => lock);
  return prev.then(() => release);
}

// Launch (or reuse) a persistent browser context with a retailer-specific proxy IP.
// Uses launchPersistentContext to maintain a full Chrome profile on disk per retailer.
// Returns { page } — caller should close the PAGE when done (not the context).
// opts.clean: use raw chromium (no stealth/rebrowser plugins). Required for PerimeterX/Akamai
// sites — the stealth plugin's CDP modifications are fingerprinted as automation signals.
// Bandwidth optimization: block image/font/media/ad-domain requests on retailer scraper
// pages. Cuts proxy bill ~70% and scan time 20-40%. We only need HTML + JSON + the JS
// that hydrates `__NEXT_DATA__` / `INITIAL_STATE` — images and analytics beacons are pure
// waste. Critical: don't block stylesheets — some sites use CSS @import to gate JS run.
//
// Tracked at top-level so we can disable it per-retailer if a site breaks (e.g., if a
// page hides product data behind an image-triggered XHR, we'd need to allow images).
const BLOCKED_RESOURCE_TYPES = new Set(["image", "font", "media"]);
const BLOCKED_DOMAINS = [
  "doubleclick.net", "googletagmanager.com", "google-analytics.com",
  "googleadservices.com", "facebook.net", "facebook.com/tr",
  "criteo.com", "rfihub.com", "amazon-adsystem.com",
  "scorecardresearch.com", "adsrvr.org", "demdex.net",
  "branch.io", "segment.com", "segment.io", "mixpanel.com",
  "newrelic.com", "nr-data.net", "bugsnag.com", "sentry.io",
];
async function applyBandwidthFilter(page) {
  /* v8 ignore start -- requires live page; tested via integration */
  try {
    await page.route("**/*", (route) => {
      const req = route.request();
      const type = req.resourceType();
      if (BLOCKED_RESOURCE_TYPES.has(type)) return route.abort();
      const url = req.url();
      for (const d of BLOCKED_DOMAINS) {
        if (url.includes(d)) return route.abort();
      }
      route.continue();
    });
  } catch { /* page may be closed already */ }
  /* v8 ignore stop */
}

async function launchRetailerBrowser(retailerKey, opts = {}) {
  // Reuse existing persistent context if available
  if (retailerBrowserCache[retailerKey]) {
    try {
      const cached = retailerBrowserCache[retailerKey];
      const page = await cached.context.newPage();
      await applyBandwidthFilter(page);
      // Re-minimize on Mac when reusing cached context (macOS may have un-minimized)
      /* v8 ignore start -- CDP minimization requires real browser */
      if (cached.minimizeWindowId) {
        try {
          const cdp = await cached.context.newCDPSession(page);
          await cdp.send("Browser.setWindowBounds", { windowId: cached.minimizeWindowId, bounds: { windowState: "minimized" } });
          await cdp.detach();
        } catch { /* non-critical */ }
      }
      /* v8 ignore stop */
      return { page };
    } catch {
      // Context died — clear cache and re-create below
      delete retailerBrowserCache[retailerKey];
    }
  }

  const profileDir = join(PROFILES_DIR, retailerKey);
  await mkdir(profileDir, { recursive: true });

  // Clean up stale SingletonLock files left by crashed Chrome processes.
  // These prevent launchPersistentContext from acquiring the profile directory.
  try {
    const files = await readdir(profileDir);
    for (const f of files) {
      if (f === "SingletonLock" || f === "SingletonSocket" || f === "SingletonCookie") {
        await unlink(join(profileDir, f)).catch(() => {});
      }
    }
  } catch { /* profile dir may not exist yet */ }

  const viewports = [
    { width: 1366, height: 768 },
    { width: 1440, height: 900 },
    { width: 1280, height: 720 },
    { width: 1536, height: 864 },
  ];
  const viewport = viewports[Math.floor(Math.random() * viewports.length)];

  // Headed mode on Mac defeats PerimeterX/Akamai headless detection.
  // macOS ignores --window-position for off-screen placement, so we minimize after launch.
  const headless = opts.headless !== undefined ? opts.headless : !IS_MAC;
  const contextOpts = {
    headless,
    args: [
      "--disable-blink-features=AutomationControlled",
      "--disable-session-crashed-bubble",
      "--disable-dev-shm-usage",
      "--no-first-run",
      "--disable-component-update",
      ...(!headless ? ["--window-size=800,600"] : []),
    ],
    ignoreDefaultArgs: ["--enable-automation"],
    viewport,
    locale: "en-US",
    userAgent: FETCH_HEADERS["User-Agent"],
    extraHTTPHeaders: {
      "Sec-CH-UA": FETCH_HEADERS["Sec-CH-UA"],
      "Sec-CH-UA-Mobile": FETCH_HEADERS["Sec-CH-UA-Mobile"],
      "Sec-CH-UA-Platform": FETCH_HEADERS["Sec-CH-UA-Platform"],
    },
  };
  if (CHROME_PATH) contextOpts.executablePath = CHROME_PATH;
  // On Mac, browser uses direct residential IP — strongest anti-detection setup.
  // Proxy only needed for fetch paths (Chrome TLS fingerprint) and CI (datacenter IPs).
  // Proxying the browser adds latency, failure points, and proxy IPs are more likely
  // to be flagged by PerimeterX/Akamai than residential ones.
  if (!IS_MAC) {
    const proxyConfig = getRetailerBrowserProxy(retailerKey);
    if (proxyConfig) contextOpts.proxy = proxyConfig;
  }

  // Clean mode: use vanilla playwright-core (truly unpatched CDP).
  // rebrowser-playwright-core has CDP patches (crConnection.js: __re__getMainWorld,
  // __re__emitExecutionContext) that cause intermittent "Cannot find context" and
  // "session closed" protocol errors. vanillaChromium avoids these entirely.
  // Both use the same system Chrome binary (via executablePath) — only the CDP layer differs.
  const launcher = opts.clean ? vanillaChromium : chromium;
  let context;
  try {
    context = await launcher.launchPersistentContext(profileDir, contextOpts);
  } catch (err) {
    // Profile corruption ("Cannot find context", CDP protocol errors) — nuke and retry once
    if (!opts._retried) {
      console.warn(`[${retailerKey}] Browser launch failed, clearing profile: ${err.message}`);
      await rm(profileDir, { recursive: true, force: true }).catch(() => {});
      return launchRetailerBrowser(retailerKey, { ...opts, _retried: true });
    }
    throw err;
  }
  retailerBrowserCache[retailerKey] = { context };
  // Minimize the browser window on Mac so it doesn't steal focus during scans.
  // Uses CDP Browser.getWindowForTarget + Browser.setWindowBounds to minimize without
  // affecting page rendering or anti-bot sensor execution.
  // Store the windowId so we can re-minimize after navigation events.
  /* v8 ignore start -- CDP window minimization requires real headed browser on Mac */
  let minimizeWindowId = null;
  if (!headless) {
    try {
      const pages = context.pages();
      const anyPage = pages[0] || await context.newPage();
      const cdp = await context.newCDPSession(anyPage);
      const { windowId } = await cdp.send("Browser.getWindowForTarget");
      minimizeWindowId = windowId;
      await cdp.send("Browser.setWindowBounds", { windowId, bounds: { windowState: "minimized" } });
      await cdp.detach();
    } catch { /* non-critical — window visible but functional */ }
  }
  /* v8 ignore stop */
  retailerBrowserCache[retailerKey] = { context, minimizeWindowId };
  const page = await context.newPage();
  await applyBandwidthFilter(page);
  /* v8 ignore start -- CDP re-minimize after new page */
  if (minimizeWindowId) {
    try {
      const cdp = await context.newCDPSession(page);
      await cdp.send("Browser.setWindowBounds", { windowId: minimizeWindowId, bounds: { windowState: "minimized" } });
      await cdp.detach();
    } catch { /* non-critical */ }
  }
  /* v8 ignore stop */
  return { page };
}

// Close all cached retailer browser contexts (called at end of poll).
// Profile data remains on disk for next poll.
async function closeRetailerBrowsers() {
  for (const key of Object.keys(retailerBrowserCache)) {
    const { context } = retailerBrowserCache[key];
    // Timeout prevents a hung Chrome process from blocking saveState and summary
    await withTimeout(context.close().catch(() => {}), 10000, undefined);
    delete retailerBrowserCache[key];
  }
}

// Simulate human-like behavior on a homepage to build trust with anti-bot sensors.
// Scrolls down, pauses, hovers over a random link, then scrolls back up.
// Per-retailer pace variance prevents ML models from clustering identical behavior.
// pace: "fast" (Costco), "medium" (Walmart), "slow" (Total Wine, Sam's Club)
const HUMANIZE_PACE = {
  fast:   { moves: [2, 3], scrolls: [1, 2], hoverMs: [800, 1500],  moveDelayMs: [150, 300], scrollDelayMs: [400, 600] },
  medium: { moves: [2, 4], scrolls: [2, 3], hoverMs: [1000, 2000], moveDelayMs: [200, 400], scrollDelayMs: [500, 700] },
  slow:   { moves: [3, 5], scrolls: [3, 4], hoverMs: [1500, 3000], moveDelayMs: [250, 500], scrollDelayMs: [600, 900] },
};
async function humanizePage(page, { pace = "medium" } = {}) {
  try {
    const p = HUMANIZE_PACE[pace] || HUMANIZE_PACE.medium;
    const rng = (min, max) => min + Math.random() * (max - min);
    // Random mouse movements before scrolling (PerimeterX tracks mouse telemetry)
    const viewport = page.viewportSize() || { width: 1366, height: 768 };
    const moveCount = Math.floor(rng(p.moves[0], p.moves[1] + 1));
    for (let i = 0; i < moveCount; i++) {
      await page.mouse.move(
        100 + Math.random() * (viewport.width - 200),
        100 + Math.random() * (viewport.height - 200),
        { steps: 5 + Math.floor(Math.random() * 10) },
      );
      await sleep(rng(p.moveDelayMs[0], p.moveDelayMs[1]));
    }
    // Scroll down slowly
    const scrollSteps = Math.floor(rng(p.scrolls[0], p.scrolls[1] + 1));
    for (let i = 0; i < scrollSteps; i++) {
      await page.mouse.wheel(0, 300 + Math.random() * 200);
      await sleep(rng(p.scrollDelayMs[0], p.scrollDelayMs[1]));
    }
    // Hover over a random visible link (if any). Use $$eval to count without
    // materializing all elements — page.$$("a[href]") hangs for 10-70s on link-heavy pages.
    const linkCount = await page.$$eval("a[href]", (els) => els.length);
    if (linkCount > 0) {
      const idx = Math.floor(Math.random() * Math.min(linkCount, 20));
      await page.locator("a[href]").nth(idx).hover().catch(() => {});
      await sleep(rng(p.hoverMs[0], p.hoverMs[1]));
    }
    // Scroll back to top using wheel events (not scrollTo — that's a JS teleport
    // detectable by anti-bot sensors that monitor scroll event patterns)
    for (let i = 0; i < scrollSteps; i++) {
      await page.mouse.wheel(0, -(300 + Math.random() * 200));
      await sleep(rng(p.scrollDelayMs[0], p.scrollDelayMs[1]));
    }
  } catch { /* non-critical — don't break the scraper */ }
}

// Navigate to a spirits/liquor category page before searching.
// Simulates a real user flow: homepage → browse category → search.
// Anti-bot systems track navigation patterns — direct search is suspicious.
const CATEGORY_URLS = {
  costco: "https://www.costco.com/liquor.html",
  totalwine: "https://www.totalwine.com/spirits/c/spirits",
  walmart: "https://www.walmart.com/browse/food/wine-beer-spirits/976759_1085633",
  walgreens: "https://www.walgreens.com/store/c/wine-beer-and-spirits/ID=360442-tier2general",
  samsclub: "https://www.samsclub.com/b/spirits/2020102",
  safeway: "https://www.safeway.com/shop/aisles/beer-wine-spirits/spirits.3132.html",
  albertsons: "https://www.albertsons.com/shop/aisles/beer-wine-spirits/spirits.3132.html",
};

async function navigateCategory(page, retailerKey) {
  const url = CATEGORY_URLS[retailerKey];
  if (!url) return;
  try {
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 15000 });
    await sleep(2000 + Math.random() * 2000);
    await humanizePage(page);
  } catch { /* non-critical — continue to search even if category nav fails */ }
}

async function newPage() {
  if (!browser) await launchBrowser();
  // Randomize viewport to reduce browser fingerprinting
  const viewports = [
    { width: 1366, height: 768 },
    { width: 1440, height: 900 },
    { width: 1280, height: 720 },
    { width: 1536, height: 864 },
  ];
  const viewport = viewports[Math.floor(Math.random() * viewports.length)];
  const storedState = await loadBrowserState();
  const context = await browser.newContext({
    viewport,
    locale: "en-US",
    userAgent: FETCH_HEADERS["User-Agent"],
    extraHTTPHeaders: {
      "Sec-CH-UA": FETCH_HEADERS["Sec-CH-UA"],
      "Sec-CH-UA-Mobile": FETCH_HEADERS["Sec-CH-UA-Mobile"],
      "Sec-CH-UA-Platform": FETCH_HEADERS["Sec-CH-UA-Platform"],
    },
    ...(storedState && { storageState: storedState }),
  });
  return context.newPage();
}

// ─── Retailer Scrapers ───────────────────────────────────────────────────────
// Each scraper accepts a store object and returns an array of { name, url, price }.

// Detect bot challenge/block pages that return HTTP 200 but no real content.
// Returns true if the page appears to be a challenge, access denied, or CAPTCHA page.
async function isBlockedPage(page) {
  const title = await page.title().catch(() => "");
  const lower = title.toLowerCase();
  if (lower.includes("access denied") || lower.includes("robot") ||
      lower.includes("captcha") || lower.includes("challenge") ||
      lower.includes("blocked") || lower.includes("verify")) {
    return true;
  }
  // Check body text for challenges that use normal-looking titles
  const bodyText = String(await page.evaluate(() => document.body?.innerText?.slice(0, 5000) || "").catch(() => ""));
  const bodyLower = bodyText.toLowerCase();
  return bodyLower.includes("please verify") || bodyLower.includes("are you a robot") ||
    bodyLower.includes("security check") || bodyLower.includes("one more step") ||
    bodyLower.includes("checking your browser") || bodyLower.includes("press & hold");
}

// Detect and solve PerimeterX "Press & Hold" challenge.
// PerimeterX renders the button via captcha.js inside #px-captcha — the "Press & Hold"
// text is drawn by the script (canvas/shadow DOM), not as a regular DOM element.
// Returns true if a challenge was found and solved, false otherwise.
// Fixed-delay sleep — no jitter. Used in solveHumanChallenge where minimum hold
// time matters and ±30% sleep jitter could drop below PerimeterX's threshold.
// Still uses setTimeout so vitest fake timers work correctly.
const rawSleep = sleep; // Alias; hold duration is set high enough to absorb jitter

async function solveHumanChallenge(page) {
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      const bodyText = String(await page.evaluate(() => document.body?.innerText?.slice(0, 2000) || "").catch(() => ""));
      if (!bodyText.includes("Press & Hold")) return false;

      if (attempt === 0) console.log("[bot] Detected Press & Hold challenge — solving...");
      else console.log("[bot] Retry attempt #2...");

      /* v8 ignore start -- PerimeterX challenge solver requires real browser mouse/hold interactions */
      const target = await page.waitForSelector("#px-captcha", { timeout: 5000 }).catch(() => null);
      if (!target) {
        console.warn("[bot] #px-captcha not found — cannot solve");
        return false;
      }
      await rawSleep(1000 + Math.random() * 1000);

      const box = await target.boundingBox();
      if (!box) {
        console.warn("[bot] #px-captcha has no bounding box");
        if (attempt === 0) { await rawSleep(2000); continue; }
        return false;
      }

      const cx = box.x + box.width / 2 + (Math.random() * 10 - 5);
      const cy = box.y + box.height / 2 + (Math.random() * 6 - 3);
      await page.mouse.move(cx, cy, { steps: 15 + Math.floor(Math.random() * 10) });
      await rawSleep(300 + Math.random() * 400);

      await page.mouse.down();
      const holdMs = 14000 + Math.random() * 4000;
      console.log(`[bot] Holding for ${(holdMs / 1000).toFixed(1)}s...`);

      const microMoves = Math.floor(holdMs / 2000);
      const moveInterval = holdMs / (microMoves + 1);
      for (let m = 0; m < microMoves; m++) {
        await rawSleep(moveInterval);
        await page.mouse.move(
          cx + (Math.random() * 4 - 2),
          cy + (Math.random() * 4 - 2),
          { steps: 2 },
        ).catch(() => {});
      }
      await rawSleep(moveInterval);
      await page.mouse.up();

      await Promise.race([
        page.waitForNavigation({ waitUntil: "domcontentloaded", timeout: 15000 }),
        rawSleep(15000),
      ]).catch(() => {});
      await rawSleep(1500 + Math.random() * 1000);

      const stillBlocked = await page.evaluate(() =>
        (document.body?.innerText || "").includes("Press & Hold"),
      ).catch(() => true);

      if (!stillBlocked) {
        console.log("[bot] Challenge solved successfully");
        return true;
      }
      if (attempt === 0) {
        console.warn("[bot] Challenge still present — retrying with longer hold...");
        await rawSleep(2000 + Math.random() * 2000);
        continue;
      }
      console.warn("[bot] Challenge still present after 2 attempts");
      /* v8 ignore stop */
      return false;
    } catch (err) {
      console.warn(`[bot] Challenge solve error: ${err.message}`);
      if (attempt === 0) { await rawSleep(2000).catch(() => {}); continue; }
      return false;
    }
  }
  return false;
}

// ─── Costco: fetch-first with browser fallback ──────────────────────────────
// Costco search has no store filter — results are identical across warehouses.
// Scrape once and the poll loop copies results to all stores.

// Extract matched bottles from Costco product tiles parsed by cheerio.
function matchCostcoTiles($) {
  const found = [];
  $('[data-testid^="ProductTile_"]').each((_i, tile) => {
    const $tile = $(tile);
    const id = ($tile.attr("data-testid") || "").replace("ProductTile_", "");
    const title = $tile.find(`[data-testid="Text_ProductTile_${id}_title"], h3`).first().text().trim();
    const price = $tile.find(`[data-testid="Text_Price_${id}"], [data-testid^="Text_Price_"]`).first().text().trim();
    const href = $tile.find('a[href*=".product."]').first().attr("href") || "";
    const url = href || (id ? `https://www.costco.com/.product.${id}.html` : "");

    for (const bottle of TARGET_BOTTLES) {
      if (matchesBottle(title, bottle, "costco")) {
        found.push({ name: bottle.name, url, price, sku: id || "", size: parseSize(title), fulfillment: "" });
      }
    }
  });
  return found;
}

// Akamai/PerimeterX challenge patterns to detect blocked responses in fetch HTML.
// Shared across all fetch paths (Costco, Walmart, Sam's Club).
const FETCH_BLOCKED_PATTERNS = [
  "Access Denied", "robot", "captcha",
  "Request unsuccessful", "Incapsula",
  "Enable JavaScript", "verify you are human",
  "_ct_challenge", "px-captcha",
  "Please verify", "security check",
  "one more step", "checking your browser",
];
function isFetchBlocked(html) {
  const lower = html.length > 10000 ? html.slice(0, 10000).toLowerCase() : html.toLowerCase();
  return FETCH_BLOCKED_PATTERNS.some((p) => lower.includes(p.toLowerCase()));
}
// Backward-compatible alias for Costco-specific callers
const isCostcoBlocked = isFetchBlocked;

// Try fetching Costco search HTML directly and parsing with cheerio (no browser needed).
// Returns found[] on success, null if blocked by Akamai Bot Manager.
async function scrapeCostcoViaFetch() {
  if (!isProxyAvailable()) return null; // skip if no working proxy
  const found = [];
  let failures = 0;
  let validPages = 0;
  let rotated = false;

  // Pre-warm: fetch homepage to get session cookies (mirrors browser pre-warm).
  // Akamai gives lighter treatment to requests with valid session cookies.
  let cookies = "";
  try {
    const homeRes = await scraperFetchRetry("https://www.costco.com/", {
      headers: FETCH_HEADERS,
      timeout: 10000,
      proxyUrl: getRetailerProxyUrl("costco"),
    });
    // got-scraping returns headers as plain object; set-cookie may be string or array
    const rawSetCookie = homeRes.headers["set-cookie"];
    const setCookies = Array.isArray(rawSetCookie) ? rawSetCookie : rawSetCookie ? [rawSetCookie] : [];
    cookies = setCookies.map((c) => c.split(";")[0]).join("; ");
  } catch { /* continue without cookies */ }
  // Merge browser-quality cookies (incl. Akamai _abck) from previous poll's browser scrape
  const cachedCostco = getCachedCookies("costco");
  if (cachedCostco) cookies = cookies ? `${cookies}; ${cachedCostco}` : cachedCostco;

  // SOCKS5 proxies (e.g. NordVPN) cap concurrent connections — go sequential.
  // HTTP proxies use 2 concurrent (down from 4) to reduce Akamai burst detection.
  const concurrency = PROXY_URL?.startsWith("socks") ? 1 : 2;

  // Batch queries with adaptive concurrency
  // isFirst captured in .map() (sequential) to avoid race in concurrent tasks
  const failedPriorityQueries = new Set();
  const queryTasks = shuffleKeepCanaryFirst(getQueriesForScan(SEARCH_QUERIES)).map((query, i) => async () => {
    if (failures > 3) return; // Early-abort without health tracking — browser will track its own
    // Rotate proxy IP after 2 consecutive failures (likely burned IP)
    if (failures >= 2 && !rotated) {
      rotated = true;
      rotateRetailerProxy("costco");
      cookies = ""; // Clear cookies from old IP's session
      await sleep(1000 + Math.random() * 1000);
    }
    // Inter-query delay: wider jitter (2-5s, 15% chance of 7-10s "reading" pause)
    if (i > 0) await sleep(2000 + Math.random() * 3000 + (Math.random() < 0.15 ? 5000 : 0));
    const url = `https://www.costco.com/s?keyword=${encodeURIComponent(query)}`;
    const headers = { ...FETCH_HEADERS };
    if (cookies) headers["Cookie"] = cookies;
    if (i > 0) {
      headers["Sec-Fetch-Site"] = "same-origin";
      headers["Referer"] = "https://www.costco.com/";
    }
    try {
      let res = await scraperFetchRetry(url, { headers, timeout: 15000, proxyUrl: getRetailerProxyUrl("costco") });
      let html = res.ok ? await res.text() : "";
      // Retry once with backoff if blocked (Akamai may unblock after a pause)
      if (!res.ok || isCostcoBlocked(html)) {
        await sleep(2000 + Math.random() * 1000);
        const retryRes = await scraperFetch(url, { headers, timeout: 15000, proxyUrl: getRetailerProxyUrl("costco") }).catch(() => null);
        if (!retryRes?.ok) { failures++; if (PRIORITY_QUERIES.has(query)) failedPriorityQueries.add(query); return; }
        const retryHtml = await retryRes.text();
        if (isCostcoBlocked(retryHtml)) { failures++; if (PRIORITY_QUERIES.has(query)) failedPriorityQueries.add(query); return; }
        html = retryHtml;
      }
      const $ = cheerio.load(html);
      const hasTiles = $('[data-testid^="ProductTile_"]').length > 0;
      if (!hasTiles) {
        // Distinguish genuine "no results" from soft block (page shell with stripped results)
        const hasResultsContainer = $('[data-testid="no-results"], [data-testid="search-results"], .MuiGrid-root, main[class]').length > 0;
        if (hasResultsContainer) {
          validPages++;
          trackHealth("costco", "ok"); // Genuine empty result
        } else {
          console.warn(`[costco] No tiles and no results container — possible soft block`);
          trackHealth("costco", "blocked"); // Don't count as valid — may trigger browser fallback
        }
        return;
      }
      validPages++;
      trackHealth("costco", "ok");
      found.push(...matchCostcoTiles($));
    } catch {
      failures++;
      if (PRIORITY_QUERIES.has(query)) failedPriorityQueries.add(query);
    }
  });
  await runWithConcurrency(queryTasks, concurrency);

  // Retry failed priority queries once — these cover Pappy/BTAC/Taylor and can't be silently dropped
  /* v8 ignore start -- priority retry uses same fetch+parse logic tested in matchCostcoTiles/isCostcoBlocked */
  if (failedPriorityQueries.size > 0 && validPages > 0) {
    console.log(`[costco] Retrying ${failedPriorityQueries.size} failed priority queries: ${[...failedPriorityQueries].join(", ")}`);
    await sleep(2000 + Math.random() * 2000);
    for (const query of failedPriorityQueries) {
      try {
        const url = `https://www.costco.com/s?keyword=${encodeURIComponent(query)}`;
        const headers = { ...FETCH_HEADERS, "Sec-Fetch-Site": "same-origin", "Referer": "https://www.costco.com/" };
        if (cookies) headers["Cookie"] = cookies;
        const res = await scraperFetchRetry(url, { headers, timeout: 15000, proxyUrl: getRetailerProxyUrl("costco") });
        if (!res.ok) continue;
        const html = await res.text();
        if (isCostcoBlocked(html)) continue;
        const $ = cheerio.load(html);
        found.push(...matchCostcoTiles($));
        trackHealth("costco", "ok");
        console.log(`[costco] Priority retry succeeded: "${query}"`);
      } catch { /* already retried, move on */ }
      await sleep(1000 + Math.random() * 1000);
    }
  }
  /* v8 ignore stop */

  // Require at least 25% of queries to succeed — a single valid page shouldn't
  // suppress browser fallback when most queries were blocked
  const minValid = Math.max(1, Math.ceil(queryTasks.length / 4));
  if (validPages < minValid) return null;
  return dedupFound(found);
}

// Browser-based Costco scraper (fallback). Uses stable MUI data-testid attributes.
async function scrapeCostcoOnce(page) {
  // Pre-warm: visit homepage to let Akamai sensor set _abck cookie.
  // Use networkidle + longer dwell so sensor scripts fully execute and phone home.
  await page.goto("https://www.costco.com/", { waitUntil: "networkidle", timeout: 20000 }).catch(() => {});
  await sleep(3000 + Math.random() * 2000);
  await solveHumanChallenge(page);
  await humanizePage(page, { pace: "fast" });
  // Category navigation adds browsing depth (homepage → category → search) which raises
  // behavioral scores with Akamai. Costs ~4-6s but makes the session look more human.
  await navigateCategory(page, "costco");

  const found = [];
  for (const query of shuffleKeepCanaryFirst(getQueriesForScan(SEARCH_QUERIES))) {
    const url = `https://www.costco.com/s?keyword=${encodeURIComponent(query)}`;
    try {
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 });
      let tilesLoaded = await page.waitForSelector('[data-testid^="ProductTile_"]', { timeout: 15000 }).then(() => true).catch(() => false);
      // If blocked, try solving the PerimeterX challenge and retry the query
      if (!tilesLoaded && await isBlockedPage(page)) {
        const solved = await solveHumanChallenge(page);
        if (solved) {
          await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 }).catch(() => {});
          tilesLoaded = await page.waitForSelector('[data-testid^="ProductTile_"]', { timeout: 15000 }).then(() => true).catch(() => false);
        }
        if (!tilesLoaded) {
          console.warn(`[costco] Bot detection page for query "${query}" — skipping`);
          trackHealth("costco", "blocked", { reason: "waf" });
          continue;
        }
      }
      if (!tilesLoaded) {
        // No tiles and not blocked — could be a degraded page or JS failed to hydrate.
        // The 2026 Costco redesign dropped `[data-testid="search-results"]` as a content
        // anchor. New page reliably has `LinkList` testids (sidebar facets) and
        // `MarkdownRenderer` (info banners) even on no-results pages — anchor on those
        // plus `main h1/h2` for robustness as the design evolves. `main` alone matches
        // most pages so it's the final safety net.
        const hasContent = await page.$(
          'main, [data-testid="search-results"], [data-testid="no-results"], [data-testid^="LinkList"], [data-testid="MarkdownRenderer"]'
        ).then(el => !!el).catch(() => false);
        if (hasContent) {
          trackHealth("costco", "ok"); // Genuine empty result on a healthy page
        } else {
          console.warn(`[costco] No tiles and no results container — possible soft block`);
          trackHealth("costco", "blocked", { reason: "soft_block" });
        }
        continue;
      }

      const products = await page.$$eval(
        '[data-testid^="ProductTile_"]',
        /* v8 ignore start -- browser-only DOM callback */
        (tiles) =>
          tiles.map((tile) => {
            const id = tile.getAttribute("data-testid").replace("ProductTile_", "");
            const titleEl = tile.querySelector(`[data-testid="Text_ProductTile_${id}_title"], h3`);
            const priceEl = tile.querySelector(`[data-testid="Text_Price_${id}"], [data-testid^="Text_Price_"]`);
            const linkEl = tile.querySelector('a[href*=".product."]');
            return {
              id,
              title: titleEl?.textContent?.trim() || "",
              url: linkEl?.href || "",
              price: priceEl?.textContent?.trim() || "",
            };
          })
        /* v8 ignore stop */
      );

      for (const p of products) {
        const productUrl = p.url || (p.id ? `https://www.costco.com/.product.${p.id}.html` : "");
        for (const bottle of TARGET_BOTTLES) {
          if (matchesBottle(p.title, bottle, "costco")) {
            found.push({ name: bottle.name, url: productUrl, price: p.price, sku: p.id || "", size: parseSize(p.title), fulfillment: "" });
          }
        }
      }
      trackHealth("costco", "ok");
    } catch (err) {
      console.error(`[costco] Error searching "${query}": ${err.message}`);
      trackHealth("costco", "fail");
    }
    await sleep(2000 + Math.random() * 2000);
  }
  return dedupFound(found);
}

// Parse a Costco product detail page (not search tiles). Product pages use og:title,
// price meta tags, and fulfillment data-testids — different from search's ProductTile_ grid.
function matchCostcoProductPage($, seedName, seedUrl) {
  const ogTitle = $('meta[property="og:title"]').attr("content") || "";
  const h1 = $("h1").first().text().trim();
  const title = ogTitle || h1;
  if (!title) return null;
  const itemMatch = seedUrl.match(/\.product\.(\d+)\.html/);
  const sku = itemMatch ? itemMatch[1] : "";
  const metaPrice = $('meta[property="product:price:amount"]').attr("content") || "";
  const testidPrice = $('[data-testid^="Text_Price"]').first().text().trim();
  const price = testidPrice || (metaPrice ? `$${metaPrice}` : "");
  for (const bottle of TARGET_BOTTLES) {
    if (matchesBottle(title, bottle, "costco")) {
      return { name: bottle.name, url: seedUrl, price, sku, size: parseSize(title), fulfillment: "" };
    }
  }
  return null;
}

// Check known Costco product URLs via fetch with cookie-enhanced requests.
// Sends cached Akamai _abck cookies from previous browser scrapes + homepage pre-warm
// cookies to bypass Akamai Bot Manager on product page requests.
async function checkCostcoKnownUrls() {
  const known = knownProducts.costco || [];
  if (known.length === 0) return [];
  const costcoProxy = getRetailerProxyUrl("costco");
  if (!costcoProxy && !IS_MAC) return []; // Need proxy or residential IP
  console.log(`[costco] Checking ${known.length} known product URLs via fetch`);

  // Pre-warm: fetch homepage to get session cookies (same pattern as scrapeCostcoViaFetch)
  let cookies = "";
  try {
    const homeRes = await scraperFetchRetry("https://www.costco.com/", {
      headers: FETCH_HEADERS, timeout: 10000, proxyUrl: costcoProxy,
    });
    const rawSetCookie = homeRes.headers["set-cookie"];
    const setCookies = Array.isArray(rawSetCookie) ? rawSetCookie : rawSetCookie ? [rawSetCookie] : [];
    cookies = setCookies.map((c) => c.split(";")[0]).join("; ");
  } catch { /* continue without homepage cookies */ }
  // Merge browser-quality Akamai _abck cookies from previous browser scrape
  const cachedCostco = getCachedCookies("costco");
  if (cachedCostco) cookies = cookies ? `${cookies}; ${cachedCostco}` : cachedCostco;

  const found = [];
  let checked = 0, notOk = 0, blocked = 0;
  for (const { name, url } of known) {
    if (!url || !url.includes("costco.com/")) continue;
    checked++;
    try {
      const headers = { ...FETCH_HEADERS };
      if (cookies) headers["Cookie"] = cookies;
      const res = await scraperFetchRetry(url, { headers, timeout: 15000, proxyUrl: costcoProxy });
      if (!res.ok) { notOk++; continue; }
      const html = await res.text();
      if (isCostcoBlocked(html)) { blocked++; continue; }
      // Try search tile format first (some product pages may include tiles),
      // then fall back to product page format (og:title, price meta)
      const $ = cheerio.load(html);
      const tileMatched = matchCostcoTiles($);
      if (tileMatched.length > 0) {
        found.push(...tileMatched);
        console.log(`[costco] Known URL check: ${name} found via tiles`);
      } else {
        const product = matchCostcoProductPage($, name, url);
        if (product) {
          found.push(product);
          console.log(`[costco] Known URL check: ${product.name} found (item ${product.sku})`);
        }
      }
    } catch (err) {
      console.warn(`[costco] Known URL check failed for ${name}: ${err.message}`);
    }
    await sleep(500 + Math.random() * 500);
  }
  console.log(`[costco] Known URL fetch: ${found.length} found, ${checked} checked, ${notOk} non-ok, ${blocked} blocked`);
  return found;
}

// Browser fallback for known Costco product URLs. Called when both fetch known URLs
// and browser search fail — navigates directly to product pages which may pass Akamai
// where search is blocked (product pages are less bot-targeted than search).
async function checkCostcoKnownUrlsViaBrowser() {
  const known = knownProducts.costco || [];
  if (known.length === 0) return [];
  let page;
  try {
    ({ page } = await launchRetailerBrowser("costco", { clean: true }));
  } catch (err) {
    console.warn(`[costco] Browser launch for known URLs failed: ${err.message}`);
    return [];
  }
  /* v8 ignore start -- browser-only: requires real Playwright browser */
  try {
    console.log(`[costco] Checking ${known.length} known product URLs via browser`);
    const found = [];
    let blocked = 0;
    const startTime = Date.now();
    const TIME_BUDGET = 120000; // 2 minutes
    for (const { name, url } of known) {
      if (Date.now() - startTime > TIME_BUDGET) {
        console.log("[costco] Browser known URLs: time budget exceeded, stopping");
        break;
      }
      if (!url || !url.includes("costco.com/")) continue;
      try {
        await page.goto(url, { waitUntil: "domcontentloaded", timeout: 15000 });
        await sleep(1000 + Math.random() * 1000);
        if (await isBlockedPage(page)) {
          blocked++;
          if (blocked >= 3) {
            console.log("[costco] Browser known URLs: 3+ blocked, aborting");
            break;
          }
          continue;
        }
        const productData = await page.evaluate(() => {
          const ogTitle = document.querySelector('meta[property="og:title"]')?.content || "";
          const h1 = document.querySelector("h1")?.textContent?.trim() || "";
          const title = ogTitle || h1;
          const priceEl = document.querySelector('[data-testid^="Text_Price"]');
          const priceMeta = document.querySelector('meta[property="product:price:amount"]');
          const price = priceEl?.textContent?.trim() || (priceMeta ? `$${priceMeta.content}` : "");
          return { title, price };
        });
        if (!productData.title) continue;
        const itemMatch = url.match(/\.product\.(\d+)\.html/);
        const sku = itemMatch ? itemMatch[1] : "";
        for (const bottle of TARGET_BOTTLES) {
          if (matchesBottle(productData.title, bottle, "costco")) {
            found.push({ name: bottle.name, url, price: productData.price, sku, size: parseSize(productData.title), fulfillment: "" });
            console.log(`[costco] Browser known URL: ${bottle.name} found (item ${sku})`);
            break;
          }
        }
      } catch (err) {
        if (err.message.includes("closed") || err.message.includes("crashed")) break;
      }
      await sleep(2000 + Math.random() * 2000);
    }
    if (found.length > 0) await cacheRetailerCookies("costco");
    console.log(`[costco] Browser known URLs: ${found.length} found, ${blocked} blocked`);
    return found;
  } finally {
    await page.close().catch(() => {});
  }
  /* v8 ignore stop */
}

// Wrapper: try got-scraping fetch-first (Chrome TLS fingerprint), fall back to browser.
// Akamai injects sensor JS (_abck cookie) that requires a real browser, but got-scraping
// may pass the initial request before sensors detect it — worth trying for speed.
async function scrapeCostcoStore() {
  // Check known product URLs first (less suspicious, supplements search results)
  const knownFound = await checkCostcoKnownUrls().catch((err) => {
    console.warn(`[costco] Known URL check wrapper failed: ${err.message}`);
    return [];
  });

  // Try fetch-first with got-scraping (Chrome TLS fingerprint)
  const fetchResult = await scrapeCostcoViaFetch();
  if (fetchResult !== null) {
    console.log("[costco] ✓ fast fetch succeeded");
    return dedupFound([...knownFound, ...fetchResult]);
  }

  // Fetch blocked or no proxy — fall back to clean browser (no stealth/rebrowser).
  // Akamai detects stealth plugin CDP modifications the same way PerimeterX does.
  console.log("[costco] Fetch blocked, using clean browser");
  let page;
  try {
    ({ page } = await launchRetailerBrowser("costco", { clean: true }));
  } catch (err) {
    console.error(`[costco] Browser launch failed: ${err.message}`);
    trackHealth("costco", "fail");
    return dedupFound(knownFound);
  }
  try {
    const scraperPromise = scrapeCostcoOnce(page);
    scraperPromise.catch(() => {}); // Prevent unhandled rejection if timeout closes page
    const result = await withTimeout(scraperPromise, 300000, null);
    if (result === null) {
      console.warn("[costco] Browser scraper timed out (300s)");
      trackHealth("costco", "fail");
      await page.close().catch(() => {});
      // Last resort: try product pages directly via browser.
      // Product pages may pass Akamai where search is blocked (less bot-targeted).
      // Akamai sensors ran during the search attempt, improving cookie quality.
      if (knownFound.length === 0) {
        const browserKnown = await checkCostcoKnownUrlsViaBrowser().catch(() => []);
        return dedupFound(browserKnown);
      }
      return dedupFound(knownFound);
    }
    await cacheRetailerCookies("costco");
    return dedupFound([...knownFound, ...result]);
  } finally {
    await page.close().catch(() => {});
  }
}

// ─── Total Wine: fetch-first with browser fallback ───────────────────────────

// Extract matched bottles from Total Wine INITIAL_STATE JSON.
// Shared by both fetch and browser paths.
// Total Wine won't ship allocated bourbon, so shipping eligibility is a noisy
// national catalog flag — not evidence of per-store stock. Treat options containing
// "ship" (case-insensitive across type OR name) as untrusted for allocation hunting.
function isTotalWineShippingOption(o) {
  const label = `${o.type || ""} ${o.name || ""}`.toLowerCase();
  return label.includes("ship");
}

function matchTotalWineInitialState(state) {
  const found = [];
  const products = state?.search?.results?.products;
  if (!Array.isArray(products)) return found;

  for (const p of products) {
    // Trust only per-store signals: stockLevel (comes back store-filtered via storeId
    // URL param) or non-shipping shopping options (pickup/in-store/delivery).
    // transactional is a national flag — only use as last-resort when nothing else present.
    const hasStockData = p.stockLevel != null || p.shoppingOptions != null;
    const hasLocalOption = p.shoppingOptions?.some(
      (o) => o.eligible && !isTotalWineShippingOption(o)
    );
    const inStock = (p.stockLevel?.[0]?.stock > 0) ||
                    hasLocalOption ||
                    (!hasStockData && p.transactional === true);
    if (!inStock) continue;

    for (const bottle of TARGET_BOTTLES) {
      if (matchesBottle(p.name || "", bottle, "totalwine")) {
        // Diagnostic: record which signal fired so finds can be audited by signal strength.
        // stockLevel (per-store ground truth) > local option (pickup/in-store) > transactional (national fallback).
        let viaSignal;
        if (p.stockLevel?.[0]?.stock > 0) {
          viaSignal = `stockLevel:${p.stockLevel[0].stock}`;
        } else if (hasLocalOption) {
          const localOpt = p.shoppingOptions.find(
            (o) => o.eligible && !isTotalWineShippingOption(o)
          );
          viaSignal = `option:${localOpt?.name || localOpt?.type || "?"}`;
        } else {
          viaSignal = "transactional";
        }
        console.log(`[totalwine] matched "${bottle.name}" via ${viaSignal}`);

        found.push({
          name: bottle.name,
          url: p.productUrl ? (p.productUrl.startsWith("http") ? p.productUrl : `https://www.totalwine.com${p.productUrl}`) : "",
          price: p.price?.[0]?.price ? `$${p.price[0].price}` : "",
          sku: (p.productUrl?.match(/\/p\/(\d+)/) || [])[1] || "",
          size: parseSize(p.name || ""),
          // Exclude shipping from the displayed fulfillment label too — otherwise
          // users see "Ship" and (correctly) distrust the find.
          fulfillment: (p.shoppingOptions || [])
            .filter((o) => o.eligible && !isTotalWineShippingOption(o))
            .map((o) => o.name || o.type || "")
            .filter(Boolean)
            .join(", "),
        });
      }
    }
  }
  return found;
}

// Try fetching Total Wine search HTML directly and parsing INITIAL_STATE (no browser needed).
// Returns { name, url, price, sku, size, fulfillment }[] on success, null if blocked.
async function scrapeTotalWineViaFetch(store, { cachedCookies = "" } = {}) {
  if (!isProxyAvailable()) return null; // skip if no working proxy
  const twProxy = getRetailerProxyUrl("totalwine");
  const found = [];
  let failures = 0;
  let validPages = 0;

  // Pre-warm: fetch homepage to get PerimeterX session cookies (_px3, _px2, _pxvid).
  // Without these cookies, search URLs immediately return a PerimeterX challenge page.
  // When cachedCookies are provided (from prior browser scrape), merge them with homepage
  // Set-Cookie for the strongest possible cookie set.
  let cookies = cachedCookies || "";
  try {
    const homeHeaders = { ...FETCH_HEADERS };
    if (cookies) homeHeaders["Cookie"] = cookies;
    const homeRes = await scraperFetchRetry("https://www.totalwine.com/", {
      headers: homeHeaders,
      timeout: 10000,
      proxyUrl: twProxy,
    });
    const rawSetCookie = homeRes.headers["set-cookie"];
    const setCookies = Array.isArray(rawSetCookie) ? rawSetCookie : rawSetCookie ? [rawSetCookie] : [];
    const homeCookies = setCookies.map((c) => c.split(";")[0]).join("; ");
    cookies = cookies ? `${cookies}; ${homeCookies}` : homeCookies;
  } catch { /* continue with cached cookies */ }

  // Batch queries (2 concurrent) — lower concurrency reduces PerimeterX burst detection.
  // With 4 stores potentially running fetch simultaneously, even 2 concurrent queries
  // per store means up to 8 total requests hitting totalwine.com at once.
  // isFirst captured in .map() (sequential) to avoid race in concurrent tasks
  const queryTasks = shuffleKeepCanaryFirst(getQueriesForScan(SEARCH_QUERIES)).map((query, i) => async () => {
    if (failures > 3) return; // Early-abort — browser will track its own
    // Inter-query delay with jitter to reduce burst detection
    if (i > 0) await sleep(1500 + Math.random() * 1500);
    const url = `https://www.totalwine.com/search/all?text=${encodeURIComponent(query)}&storeId=${store.storeId}`;
    const headers = { ...FETCH_HEADERS };
    if (cookies) headers["Cookie"] = cookies;
    if (i > 0) {
      headers["Sec-Fetch-Site"] = "same-origin";
      headers["Referer"] = "https://www.totalwine.com/";
    }
    try {
      const res = await scraperFetchRetry(url, { headers, timeout: 15000, proxyUrl: twProxy });
      if (!res.ok) { failures++; return; }
      const html = await res.text();
      const idx = html.indexOf("window.INITIAL_STATE");
      if (idx === -1) { failures++; return; }
      const braceStart = html.indexOf("{", idx);
      if (braceStart === -1) { failures++; return; }
      // Use brace counting to find the matching closing brace (handles </script> inside JSON strings)
      let depth = 0, inStr = false, escape = false, end = -1;
      for (let j = braceStart; j < html.length; j++) {
        const ch = html[j];
        if (escape) { escape = false; continue; }
        if (ch === "\\") { escape = true; continue; }
        if (ch === '"') { inStr = !inStr; continue; }
        if (inStr) continue;
        if (ch === "{") depth++;
        else if (ch === "}") { depth--; if (depth === 0) { end = j + 1; break; } }
      }
      if (end === -1) { failures++; return; }
      const state = JSON.parse(html.slice(braceStart, end));
      if (!state?.search?.results) { failures++; return; }
      validPages++;
      trackHealth("totalwine", "ok");
      found.push(...matchTotalWineInitialState(state));
    } catch {
      failures++;
    }
  });
  await runWithConcurrency(queryTasks, 2);
  // Require at least 25% of queries to succeed — a single valid page shouldn't
  // suppress browser fallback when most queries were blocked
  const minValid = Math.max(1, Math.ceil(queryTasks.length / 4));
  if (validPages < minValid) return null;
  return dedupFound(found);
}

// Browser-based Total Wine scraper (fallback). Accepts a shared Playwright page.
// Product data is in window.INITIAL_STATE.search.results.products (structured JSON).
// Search via the search box (generates keyboard/click events for PerimeterX telemetry).
// Falls back to direct URL navigation if the search box can't be found.
async function searchViaSearchBox(page, query, storeId) {
  // Try multiple common search input selectors
  const searchSelectors = [
    'input[data-testid="search-input"]',
    'input[name="searchTerm"]',
    'input[type="search"]',
    'input[placeholder*="Search"]',
    'input[aria-label*="Search"]',
    '#header-search-input',
  ];
  for (const selector of searchSelectors) {
    try {
      const input = await page.$(selector);
      if (!input) continue;
      await input.click();
      await sleep(300 + Math.random() * 300);
      // Triple-click to select all existing text, then type over it
      await input.click({ clickCount: 3 });
      await sleep(200 + Math.random() * 200);
      // Type with realistic per-key delay (50-120ms between keystrokes)
      await page.keyboard.type(query, { delay: 50 + Math.random() * 70 });
      await sleep(500 + Math.random() * 500);
      await page.keyboard.press("Enter");
      // Wait for search results page to load
      await page.waitForURL(/search/, { timeout: 15000 }).catch(() => {});
      // Ensure storeId is in URL (Total Wine filters by store)
      if (storeId && !page.url().includes(`storeId=${storeId}`)) {
        const sep = page.url().includes("?") ? "&" : "?";
        await page.goto(`${page.url()}${sep}storeId=${storeId}`, { waitUntil: "domcontentloaded", timeout: 15000 });
      }
      return true; // search box approach succeeded
    } catch { continue; }
  }
  return false; // no search box found
}

async function scrapeTotalWineViaBrowser(store, page, { skipPreWarm = false } = {}) {
  const t0 = Date.now();
  const elapsed = () => `${((Date.now() - t0) / 1000).toFixed(1)}s`;
  if (!skipPreWarm) {
    // Pre-warm: visit homepage to let PerimeterX sensor collect behavioral telemetry.
    await page.goto("https://www.totalwine.com/", { waitUntil: "networkidle", timeout: 20000 }).catch(() => {});
    console.log(`[totalwine:${store.storeId}] Homepage loaded (${elapsed()})`);
    await sleep(4000 + Math.random() * 3000); // PerimeterX needs longer JS execution time
    await solveHumanChallenge(page);
    await humanizePage(page, { pace: "slow" });
    // Category navigation adds browsing depth for PerimeterX behavioral scoring.
    // Warm contexts (subsequent stores) skip pre-warm entirely so this only runs once.
    await navigateCategory(page, "totalwine");
    console.log(`[totalwine:${store.storeId}] Pre-warm done (${elapsed()}), starting queries...`);
  } else {
    console.log(`[totalwine:${store.storeId}] Skipping pre-warm (context already warm)`);
  }

  const found = [];
  const queries = shuffleKeepCanaryFirst(getQueriesForScan(SEARCH_QUERIES));
  for (let qi = 0; qi < queries.length; qi++) {
    const query = queries[qi];
    const url = `https://www.totalwine.com/search/all?text=${encodeURIComponent(query)}&storeId=${store.storeId}`;
    try {
      // First query: use search box to generate keyboard events for PerimeterX telemetry.
      // Subsequent queries: direct URL navigation (like a user refining search).
      if (qi === 0) {
        const usedSearchBox = await searchViaSearchBox(page, query, store.storeId);
        if (!usedSearchBox) {
          await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 });
        }
      } else {
        await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 });
      }
      await page.waitForFunction(
        () => !!window.INITIAL_STATE?.search?.results,
        { timeout: 10000 }
      ).catch(() => {});

      if (await isBlockedPage(page)) {
        // Try to solve PerimeterX "Press & Hold" challenge before giving up
        const solved = await solveHumanChallenge(page);
        if (solved) {
          // Challenge solved — reload the search page to get real results
          await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 }).catch(() => {});
          await page.waitForFunction(
            () => !!window.INITIAL_STATE?.search?.results,
            { timeout: 10000 },
          ).catch(() => {});
        }
        if (!solved || (await isBlockedPage(page))) {
          console.warn(`[totalwine:${store.storeId}] Bot detection page for query "${query}" — skipping`);
          trackHealth("totalwine", "blocked", { reason: "waf" });
          continue;
        }
      }

      // Extract INITIAL_STATE from browser context
      const state = await page.evaluate(
        /* v8 ignore start -- browser-only DOM callback */
        () => {
          try { return window.INITIAL_STATE; } catch { return null; }
        }
        /* v8 ignore stop */
      );

      const matched = matchTotalWineInitialState(state);
      if (matched.length > 0) {
        found.push(...matched);
        trackHealth("totalwine", "ok");
      } else if (state?.search?.results) {
        // INITIAL_STATE present with valid search results structure — just no matching bottles
        trackHealth("totalwine", "ok");
      } else {
        // INITIAL_STATE missing or malformed — try CSS selectors as fallback
        const items = await page.$$eval(
          '[class*="productCard"], [data-testid="product-card"], .product-card',
          /* v8 ignore start -- browser-only DOM callback */
          (cards) =>
            cards.map((el) => ({
              name: (el.querySelector('[class*="title"], [class*="name"], h2, a') || {}).textContent || "",
              inStock: (() => {
                const btn = el.querySelector('button[class*="addToCart"], button[class*="Add"], [data-testid*="add"]');
                return !!btn && !btn.disabled && !btn.getAttribute("aria-disabled");
              })(),
              url: (el.querySelector('a[href*="/spirits/"], a[href*="/wine/"], a') || {}).href || "",
              price: (el.querySelector('[class*="price"], [data-testid*="price"]') || {}).textContent?.trim() || "",
            }))
          /* v8 ignore stop */
        );
        if (items.length > 0) {
          for (const p of items) {
            if (!p.inStock) continue;
            for (const bottle of TARGET_BOTTLES) {
              if (matchesBottle(p.name, bottle, "totalwine")) {
                found.push({
                  name: bottle.name,
                  url: p.url ? (p.url.startsWith("http") ? p.url : `https://www.totalwine.com${p.url}`) : "",
                  price: p.price,
                  sku: "",
                  size: parseSize(p.name),
                  fulfillment: "",
                });
              }
            }
          }
          trackHealth("totalwine", "ok");
        } else {
          // No INITIAL_STATE and no CSS products — likely a degraded/blocked page
          console.warn(`[totalwine:${store.storeId}] No INITIAL_STATE or CSS products for query — marking degraded`);
          trackHealth("totalwine", "blocked");
        }
      }
    } catch (err) {
      console.error(`[totalwine:${store.storeId}] Error searching "${query}": ${err.message}`);
      trackHealth("totalwine", "fail");
    }
    await sleep(2000 + Math.random() * 2000);
  }
  return dedupFound(found);
}

// Check known Total Wine product URLs from previous finds. Product pages contain
// INITIAL_STATE JSON with stock info — same extraction as the search fetch path.
async function checkTotalWineKnownUrls(store) {
  const known = knownProducts.totalwine || [];
  if (known.length === 0) return [];
  const twProxy = getRetailerProxyUrl("totalwine");
  // Total Wine product pages work without proxy on Mac (direct residential IP)
  const found = [];
  for (const { name, url } of known) {
    if (!url || !url.includes("totalwine.com")) continue;
    try {
      // Append storeId for per-store inventory
      const storeUrl = url.includes("?") ? `${url}&storeId=${store.storeId}` : `${url}?storeId=${store.storeId}`;
      const res = await scraperFetchRetry(storeUrl, { headers: { ...FETCH_HEADERS }, timeout: 15000, proxyUrl: twProxy || undefined });
      if (!res.ok) continue;
      const html = await res.text();
      const idx = html.indexOf("window.INITIAL_STATE");
      if (idx === -1) continue;
      const braceStart = html.indexOf("{", idx);
      if (braceStart === -1) continue;
      let depth = 0, inStr = false, escape = false, end = -1;
      for (let j = braceStart; j < html.length; j++) {
        const ch = html[j];
        if (escape) { escape = false; continue; }
        if (ch === "\\") { escape = true; continue; }
        if (ch === '"') { inStr = !inStr; continue; }
        if (inStr) continue;
        if (ch === "{") depth++;
        else if (ch === "}") { depth--; if (depth === 0) { end = j + 1; break; } }
      }
      if (end === -1) continue;
      const state = JSON.parse(html.slice(braceStart, end));
      const matched = matchTotalWineInitialState(state);
      if (matched.length > 0) {
        found.push(...matched);
        console.log(`[totalwine:${store.storeId}] Known URL check: ${name} still in stock`);
      }
    } catch (err) {
      console.warn(`[totalwine:${store.storeId}] Known URL check failed for ${name}: ${err.message}`);
    }
    await sleep(500 + Math.random() * 500);
  }
  return found;
}

// Wrapper: try got-scraping fetch-first (Chrome TLS fingerprint), fall back to browser.
// PerimeterX injects sensor JS (_px* cookies) that requires a real browser, but got-scraping
// may pass the initial requests before sensors detect it — worth trying for speed.
// Proxy browser tier removed — PerimeterX always blocks proxy IPs, wasting 180s per store.
// Direct residential IP browser (headed on Mac) is the only browser fallback.
async function scrapeTotalWineStore(store) {
  // Check known product URLs first (less suspicious, supplements search results)
  const knownFound = await checkTotalWineKnownUrls(store).catch((err) => {
    console.warn(`[totalwine:${store.storeId}] Known URL check wrapper failed: ${err.message}`);
    return [];
  });

  // Fetch-first: try got-scraping with cached browser _px* cookies before launching browser.
  // Previously disabled (PerimeterX blocks fetch without JS sensor), but now the browser→fetch
  // cookie chain provides valid _px* cookies from prior browser scrapes. Falls back to browser
  // if cookies are stale or PerimeterX rejects them.
  const cachedTwCookies = getCachedCookies("totalwine");
  if (cachedTwCookies && isProxyAvailable()) {
    // Inject cached PerimeterX cookies into the fetch path's pre-warm
    const fetchResult = await scrapeTotalWineViaFetch(store, { cachedCookies: cachedTwCookies }).catch((err) => {
      console.warn(`[totalwine:${store.storeId}] Fetch path failed: ${err.message}`);
      return null;
    });
    if (fetchResult !== null) {
      console.log(`[totalwine:${store.storeId}] Used fast fetch mode (cached browser cookies)`);
      return dedupFound([...knownFound, ...fetchResult]);
    }
    // Reset health from failed fetch before browser attempt
    delete scraperHealth["totalwine"];
  }

  // Per-store scrapers do NOT use fail-fast — each store tries browser independently.
  // A fresh page load with different queries may succeed even if a prior store was blocked.
  const skipPreWarm = !!retailerBrowserCache["totalwine"];
  console.log(`[totalwine:${store.storeId}] Queuing browser (direct IP${skipPreWarm ? ", warm" : ""})`);
  const releaseLock = await acquireRetailerLock("totalwine");
  let page;
  try {
    ({ page } = await launchRetailerBrowser("totalwine", { clean: true }));
  } catch (err) {
    console.error(`[totalwine:${store.storeId}] Browser launch failed: ${err.message}`);
    trackHealth("totalwine", "fail");
    releaseLock();
    return dedupFound(knownFound);
  }
  try {
    const scraperPromise = scrapeTotalWineViaBrowser(store, page, { skipPreWarm });
    scraperPromise.catch(() => {});
    const result = await withTimeout(scraperPromise, 300000, null);
    if (result === null) {
      console.warn(`[totalwine:${store.storeId}] Browser timed out (300s)`);
      trackHealth("totalwine", "fail");
      return dedupFound(knownFound);
    }
    return dedupFound([...knownFound, ...result]);
  } finally {
    await page.close().catch(() => {});
    releaseLock();
  }
}

// ─── Walmart: fetch-first with browser fallback ─────────────────────────────

// Extract matched bottles from Walmart __NEXT_DATA__ JSON.
// Iterates ALL itemStacks (not just [0]), filters to actual products only,
// excludes third-party marketplace sellers, and checks fulfillment.
function matchWalmartNextData(nextData, retailerKey = "walmart") {
  const found = [];
  const allStacks = nextData?.props?.pageProps?.initialData?.searchResult?.itemStacks || [];
  const items = allStacks.filter(Boolean).flatMap((stack) => stack.items || []);

  // Pre-pass: log any TARGET_BOTTLE name match that fails our filters. Walmart has
  // produced 0 allocated finds across hundreds of scans; this diagnostic surfaces
  // whether matches exist but are being silently filtered (vs. genuinely no stock).
  // Only logs when a name match is present, so spam is bounded.
  for (const item of items) {
    if (!item.name) continue;
    let matchedBottle = null;
    for (const bottle of TARGET_BOTTLES) {
      if (matchesBottle(item.name, bottle, retailerKey)) { matchedBottle = bottle; break; }
    }
    if (!matchedBottle || CANARY_NAMES.has(matchedBottle.name)) continue;  // skip canary, log allocated only
    const reasons = [];
    if (item.__typename !== "Product") reasons.push(`__typename=${item.__typename}`);
    if (item.sellerName && item.sellerName !== "Walmart.com") reasons.push(`sellerName=${item.sellerName}`);
    if (item.availabilityStatusV2?.value !== "IN_STOCK") reasons.push(`availability=${item.availabilityStatusV2?.value || "missing"}`);
    const badge = (item.fulfillmentBadge || "").toLowerCase();
    if (badge && !(/store|pickup|today/i.test(badge))) reasons.push(`badge=${item.fulfillmentBadge}`);
    if (reasons.length > 0) {
      console.log(`[walmart] FILTERED ${matchedBottle.name}: ${reasons.join(", ")} (item: "${(item.name || "").slice(0, 60)}")`);
    }
  }

  for (const item of items) {
    // Skip non-product entries (ads, recommendations, editorial)
    if (item.__typename !== "Product") continue;
    // Skip third-party marketplace sellers (inflated prices, unreliable stock)
    if (item.sellerName && item.sellerName !== "Walmart.com") continue;

    if (!item.name) continue;
    const inStock = item.availabilityStatusV2?.value === "IN_STOCK";
    if (!inStock) continue;
    // Skip ship-only items — we want walk-in/pickup availability
    const badge = (item.fulfillmentBadge || "").toLowerCase();
    if (badge && !(/store|pickup|today/i.test(badge))) continue;

    for (const bottle of TARGET_BOTTLES) {
      if (matchesBottle(item.name, bottle, retailerKey)) {
        found.push({
          name: bottle.name,
          url: item.canonicalUrl ? `https://www.walmart.com${item.canonicalUrl}` : "",
          price: item.priceInfo?.currentPrice?.priceString || "",
          sku: item.usItemId || "",
          size: parseSize(item.name),
          fulfillment: item.fulfillmentBadge || "",
        });
      }
    }
  }
  return found;
}

// Try fetching Walmart search HTML directly and parsing __NEXT_DATA__ (no browser needed).
// Continues on per-query failures; only falls back to browser if majority of queries fail.
// Returns { name, url, price }[] on success, null if blocked/unavailable.
async function scrapeWalmartViaFetch(store) {
  if (primaryProxyExhausted && !isProxyAvailable()) return null; // Walmart works without proxy on Mac
  const found = [];
  let failures = 0;
  let validPages = 0;
  let rotated = false;

  // Pre-warm: fetch homepage to get Akamai/PerimeterX session cookies before search.
  // Same pattern as Costco — Akamai gives lighter treatment to requests with valid session cookies.
  let cookies = "";
  try {
    const homeRes = await scraperFetchRetry("https://www.walmart.com/", {
      headers: FETCH_HEADERS,
      timeout: 10000,
      proxyUrl: getRetailerProxyUrl("walmart"),
    });
    const rawSetCookie = homeRes.headers["set-cookie"];
    const setCookies = Array.isArray(rawSetCookie) ? rawSetCookie : rawSetCookie ? [rawSetCookie] : [];
    cookies = setCookies.map((c) => c.split(";")[0]).join("; ");
  } catch { /* continue without cookies */ }
  const cachedWalmart = getCachedCookies("walmart");
  if (cachedWalmart) cookies = cookies ? `${cookies}; ${cachedWalmart}` : cachedWalmart;

  // Batch queries (2 concurrent) — lower concurrency reduces Akamai/PerimeterX burst detection.
  // With 5 stores potentially running fetch simultaneously, even 2 concurrent queries
  // per store means up to 10 total requests hitting walmart.com at once.
  // isFirst captured in .map() (sequential) to avoid race in concurrent tasks
  const failedPriorityQueries = new Set();
  const queryTasks = shuffleKeepCanaryFirst(getQueriesForScan(SEARCH_QUERIES)).map((query, i) => async () => {
    if (failures > 3) return; // Early-abort without health tracking — browser will track its own
    // Rotate proxy IP after 2 consecutive failures (likely burned IP)
    if (failures >= 2 && !rotated) {
      rotated = true;
      rotateRetailerProxy("walmart");
      cookies = ""; // Clear cookies from old IP's session
      await sleep(1000 + Math.random() * 1000);
    }
    // Inter-query delay with jitter to reduce burst detection
    if (i > 0) await sleep(1500 + Math.random() * 1500);
    const url = `https://www.walmart.com/search?q=${encodeURIComponent(query)}&store_id=${store.storeId}`;
    const headers = { ...FETCH_HEADERS };
    if (cookies) headers["Cookie"] = cookies;
    if (i > 0) {
      headers["Sec-Fetch-Site"] = "same-origin";
      headers["Referer"] = "https://www.walmart.com/";
    }
    try {
      const res = await scraperFetchRetry(url, { headers, timeout: 15000, proxyUrl: getRetailerProxyUrl("walmart") });
      if (!res.ok) { failures++; if (PRIORITY_QUERIES.has(query)) failedPriorityQueries.add(query); return; }
      const html = await res.text();
      if (isFetchBlocked(html)) { failures++; if (PRIORITY_QUERIES.has(query)) failedPriorityQueries.add(query); return; }
      // Use brace-counting to extract JSON (handles </script> inside JSON strings)
      const idx = html.indexOf('id="__NEXT_DATA__"');
      if (idx === -1) { failures++; if (PRIORITY_QUERIES.has(query)) failedPriorityQueries.add(query); return; }
      const braceStart = html.indexOf("{", idx);
      if (braceStart === -1) { failures++; if (PRIORITY_QUERIES.has(query)) failedPriorityQueries.add(query); return; }
      let depth = 0, inStr = false, escape = false, end = -1;
      for (let j = braceStart; j < html.length; j++) {
        const ch = html[j];
        if (escape) { escape = false; continue; }
        if (ch === "\\") { escape = true; continue; }
        if (ch === '"') { inStr = !inStr; continue; }
        if (inStr) continue;
        if (ch === "{") depth++;
        else if (ch === "}") { depth--; if (depth === 0) { end = j + 1; break; } }
      }
      if (end === -1) { failures++; if (PRIORITY_QUERIES.has(query)) failedPriorityQueries.add(query); return; }
      const nextData = JSON.parse(html.slice(braceStart, end));
      const hasSearchResult = nextData?.props?.pageProps?.initialData?.searchResult != null;
      if (!hasSearchResult) { failures++; if (PRIORITY_QUERIES.has(query)) failedPriorityQueries.add(query); return; }
      validPages++;
      trackHealth("walmart", "ok");
      found.push(...matchWalmartNextData(nextData));
    } catch {
      failures++;
      if (PRIORITY_QUERIES.has(query)) failedPriorityQueries.add(query);
    }
  });
  await runWithConcurrency(queryTasks, 2);

  // Retry failed priority queries once — Pappy/BTAC/Taylor can't be silently dropped
  /* v8 ignore start -- priority retry uses same brace-counting + matchWalmartNextData tested elsewhere */
  if (failedPriorityQueries.size > 0 && validPages > 0) {
    console.log(`[walmart:${store.storeId}] Retrying ${failedPriorityQueries.size} failed priority queries: ${[...failedPriorityQueries].join(", ")}`);
    await sleep(2000 + Math.random() * 2000);
    for (const query of failedPriorityQueries) {
      try {
        const url = `https://www.walmart.com/search?q=${encodeURIComponent(query)}&store_id=${store.storeId}`;
        const headers = { ...FETCH_HEADERS, "Sec-Fetch-Site": "same-origin", "Referer": "https://www.walmart.com/" };
        if (cookies) headers["Cookie"] = cookies;
        const res = await scraperFetchRetry(url, { headers, timeout: 15000, proxyUrl: getRetailerProxyUrl("walmart") });
        if (!res.ok) continue;
        const html = await res.text();
        if (isFetchBlocked(html)) continue;
        const idx = html.indexOf('id="__NEXT_DATA__"');
        if (idx === -1) continue;
        const braceStart = html.indexOf("{", idx);
        if (braceStart === -1) continue;
        let depth = 0, inStr = false, escape = false, end = -1;
        for (let j = braceStart; j < html.length; j++) {
          const ch = html[j];
          if (escape) { escape = false; continue; }
          if (ch === "\\") { escape = true; continue; }
          if (ch === '"') { inStr = !inStr; continue; }
          if (inStr) continue;
          if (ch === "{") depth++;
          else if (ch === "}") { depth--; if (depth === 0) { end = j + 1; break; } }
        }
        if (end === -1) continue;
        const nextData = JSON.parse(html.slice(braceStart, end));
        if (!nextData?.props?.pageProps?.initialData?.searchResult) continue;
        found.push(...matchWalmartNextData(nextData));
        trackHealth("walmart", "ok");
        console.log(`[walmart:${store.storeId}] Priority retry succeeded: "${query}"`);
      } catch { /* already retried, move on */ }
      await sleep(1000 + Math.random() * 1000);
    }
  }
  /* v8 ignore stop */

  // Require at least 25% of queries to succeed — a single valid page shouldn't
  // suppress browser fallback when most queries were blocked
  const minValid = Math.max(1, Math.ceil(queryTasks.length / 4));
  if (validPages < minValid) return null;
  return dedupFound(found);
}

// Browser-based Walmart scraper (fallback). Accepts a shared page.
async function scrapeWalmartViaBrowser(store, page) {
  // Pre-warm: visit homepage to let Akamai/PerimeterX sensor set cookies
  await page.goto("https://www.walmart.com/", { waitUntil: "networkidle", timeout: 20000 }).catch(() => {});
  await sleep(3000 + Math.random() * 3000);
  await solveHumanChallenge(page);
  await humanizePage(page, { pace: "medium" });
  await navigateCategory(page, "walmart");

  const found = [];
  for (const query of shuffleKeepCanaryFirst(getQueriesForScan(SEARCH_QUERIES))) {
    const url = `https://www.walmart.com/search?q=${encodeURIComponent(query)}&store_id=${store.storeId}`;
    try {
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 });
      await page.waitForFunction(() => document.querySelector('script#__NEXT_DATA__')?.textContent?.length > 100, { timeout: 10000 }).catch(() => {});

      if (await isBlockedPage(page)) {
        const solved = await solveHumanChallenge(page);
        if (solved) {
          await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 }).catch(() => {});
          await page.waitForFunction(() => document.querySelector('script#__NEXT_DATA__')?.textContent?.length > 100, { timeout: 10000 }).catch(() => {});
        }
        if (!solved || await isBlockedPage(page)) {
          console.warn(`[walmart:${store.storeId}] Bot detection page for query "${query}" — skipping`);
          trackHealth("walmart", "blocked", { reason: "waf" });
          continue;
        }
      }

      const nextData = await page.evaluate(
        /* v8 ignore start -- browser-only DOM callback */
        () => {
          const el = document.querySelector("script#__NEXT_DATA__");
          if (el) try { return JSON.parse(el.textContent); } catch {}
          return null;
        }
        /* v8 ignore stop */
      );

      if (nextData) {
        found.push(...matchWalmartNextData(nextData));
        trackHealth("walmart", "ok");
      } else {
        // DOM fallback if __NEXT_DATA__ is unavailable
        const products = await page.$$eval(
          '[data-testid="list-view"] [data-item-id], .search-result-gridview-item',
          /* v8 ignore start -- browser-only DOM callback */
          (items) =>
            items
              .filter((el) => !el.querySelector('[data-testid="out-of-stock"], .out-of-stock'))
              .map((el) => ({
                title: (el.querySelector('[data-automation-id="product-title"], .product-title-link span') || {}).textContent || "",
                url: (el.querySelector('a[href*="/ip/"]') || {}).href || "",
                price: (el.querySelector('[data-automation-id="product-price"], [class*="price"]') || {}).textContent?.trim() || "",
              }))
          /* v8 ignore stop */
        );
        if (products.length > 0) {
          for (const p of products) {
            for (const bottle of TARGET_BOTTLES) {
              if (matchesBottle(p.title, bottle, "walmart")) {
                found.push({ name: bottle.name, url: p.url, price: p.price, sku: "", size: parseSize(p.title), fulfillment: "" });
              }
            }
          }
          trackHealth("walmart", "ok");
        } else {
          // No __NEXT_DATA__ and no DOM products — degraded page (JS hydration failed)
          console.warn(`[walmart:${store.storeId}] No __NEXT_DATA__ or DOM products for query — marking degraded`);
          trackHealth("walmart", "blocked");
        }
      }
    } catch (err) {
      console.error(`[walmart:${store.storeId}] Error searching "${query}": ${err.message}`);
      trackHealth("walmart", "fail");
    }
    await sleep(2000 + Math.random() * 2000);
  }
  return dedupFound(found);
}

// Check known Walmart product URLs from previous finds. Less suspicious than search
// (just visiting a product page) and catches bottles that search might miss.
async function checkWalmartKnownUrls(store) {
  const known = knownProducts.walmart || [];
  if (known.length === 0) return [];
  const wmProxy = getRetailerProxyUrl("walmart");
  const found = [];
  for (const { name, url } of known) {
    if (!url || !url.includes("walmart.com/ip/")) continue;
    try {
      const storeUrl = url.includes("?") ? `${url}&store_id=${store.storeId}` : `${url}?store_id=${store.storeId}`;
      const res = await scraperFetchRetry(storeUrl, { headers: { ...FETCH_HEADERS }, timeout: 15000, proxyUrl: wmProxy });
      if (!res.ok) continue;
      const html = await res.text();
      // Use same brace-counting extraction as scrapeWalmartViaFetch
      const idx = html.indexOf('id="__NEXT_DATA__"');
      if (idx === -1) continue;
      const braceStart = html.indexOf("{", idx);
      if (braceStart === -1) continue;
      let depth = 0, inStr = false, escape = false, end = -1;
      for (let j = braceStart; j < html.length; j++) {
        const ch = html[j];
        if (escape) { escape = false; continue; }
        if (ch === "\\") { escape = true; continue; }
        if (ch === '"') { inStr = !inStr; continue; }
        if (inStr) continue;
        if (ch === "{") depth++;
        else if (ch === "}") { depth--; if (depth === 0) { end = j + 1; break; } }
      }
      if (end === -1) continue;
      const nextData = JSON.parse(html.slice(braceStart, end));
      const matched = matchWalmartNextData(nextData);
      if (matched.length > 0) {
        found.push(...matched);
        console.log(`[walmart:${store.storeId}] Known URL check: ${name} still in stock`);
      }
    } catch (err) {
      console.warn(`[walmart:${store.storeId}] Known URL check failed for ${name}: ${err.message}`);
    }
    await sleep(500 + Math.random() * 500);
  }
  return found;
}

async function scrapeWalmartStore(store) {
  // Check known product URLs first (less suspicious, supplements search results)
  const knownFound = await checkWalmartKnownUrls(store).catch((err) => {
    console.warn(`[walmart:${store.storeId}] Known URL check wrapper failed: ${err.message}`);
    return [];
  });

  // Skip fetch attempt on CI unless a proxy is configured (datacenter IPs are blocked)
  const isCI = process.env.CI === "true" || process.env.GITHUB_ACTIONS === "true";
  if (!isCI || proxyAgent) {
    const fetchResult = await scrapeWalmartViaFetch(store);
    if (fetchResult !== null) {
      console.log(`[walmart:${store.storeId}] Used fast fetch mode${proxyAgent ? " (proxied)" : ""}`);
      return dedupFound([...knownFound, ...fetchResult]);
    }
  }
  // Per-store scrapers do NOT use fail-fast — each store tries browser independently.
  console.log(`[walmart:${store.storeId}] ${isCI && !proxyAgent ? "CI mode, " : "Fetch blocked, "}queuing clean browser`);
  const releaseLock = await acquireRetailerLock("walmart");
  let page;
  try {
    ({ page } = await launchRetailerBrowser("walmart", { clean: true }));
  } catch (err) {
    console.error(`[walmart:${store.storeId}] Browser launch failed: ${err.message}`);
    trackHealth("walmart", "fail");
    releaseLock();
    return dedupFound(knownFound);
  }
  try {
    const scraperPromise = scrapeWalmartViaBrowser(store, page);
    scraperPromise.catch(() => {}); // Prevent unhandled rejection if timeout closes page
    const result = await withTimeout(scraperPromise, 300000, null);
    if (result === null) {
      console.warn(`[walmart:${store.storeId}] Browser scraper timed out (300s)`);
      trackHealth("walmart", "fail");
      return dedupFound(knownFound);
    }
    await cacheRetailerCookies("walmart");
    return dedupFound([...knownFound, ...result]);
  } finally {
    await page.close().catch(() => {});
    releaseLock();
  }
}

// ─── Walgreens: browser-only (Akamai blocks direct fetch) ───────────────────
// Walgreens uses server-rendered HTML with no embedded JSON (__NEXT_DATA__,
// INITIAL_STATE). Akamai Bot Manager returns 403 for direct HTTP requests.
// Store context is set via USER_LOC cookie (base64 JSON with lat/lng/zip).
// scrapeOnce pattern: results are based on the nearest store to ZIP_CODE.

let walgreensCoords = null;

async function scrapeWalgreensViaBrowser(page) {
  const found = [];

  // Cache zip coordinates for USER_LOC cookie (same zip every poll)
  if (!walgreensCoords) {
    try {
      const coords = await zipToCoords(ZIP_CODE);
      if (coords?.lat == null || coords?.lng == null) {
        console.warn(`[walgreens] zipToCoords returned invalid coords — skipping`);
        trackHealth("walgreens", "fail");
        return [];
      }
      walgreensCoords = coords;
    } catch (err) {
      console.warn(`[walgreens] Failed to resolve zip ${ZIP_CODE}: ${err.message} — skipping`);
      trackHealth("walgreens", "fail");
      return [];
    }
  }

  // Set location cookie so search results reflect nearest store inventory
  const locPayload = JSON.stringify({
    la: String(walgreensCoords.lat),
    lo: String(walgreensCoords.lng),
    uz: ZIP_CODE,
  });
  await page.context().addCookies([{
    name: "USER_LOC",
    value: Buffer.from(locPayload).toString("base64"),
    domain: ".walgreens.com",
    path: "/",
  }]);

  // Pre-warm: visit homepage to let Akamai sensor set _abck cookie before searching.
  const wgT0 = Date.now();
  const wgElapsed = () => `${((Date.now() - wgT0) / 1000).toFixed(1)}s`;
  await page.goto("https://www.walgreens.com/", { waitUntil: "domcontentloaded", timeout: 15000 }).catch(() => {});
  console.log(`[walgreens] Homepage loaded (${wgElapsed()})`);
  await sleep(4000 + Math.random() * 3000); // Akamai needs longer sensor execution on heavy Walgreens pages
  await solveHumanChallenge(page);
  // Walgreens humanization: mouse moves + scroll via wheel events (Akamai tracks scroll
  // telemetry). Full humanizePage() works now ($$eval fix), but keeping it lighter since
  // Walgreens pages are heavy and we need speed. Scroll is the highest-value signal.
  await page.mouse.move(400 + Math.random() * 500, 300 + Math.random() * 200, { steps: 10 }).catch(() => {});
  for (let i = 0; i < 2 + Math.floor(Math.random() * 2); i++) {
    await page.mouse.wheel(0, 300 + Math.random() * 200).catch(() => {});
    await sleep(400 + Math.random() * 400);
  }
  for (let i = 0; i < 2; i++) {
    await page.mouse.wheel(0, -(300 + Math.random() * 200)).catch(() => {});
    await sleep(300 + Math.random() * 300);
  }
  await navigateCategory(page, "walgreens");
  console.log(`[walgreens] Pre-warm done (${wgElapsed()}), starting queries...`);

  let wgConsecutiveBlocks = 0;
  for (const query of shuffleKeepCanaryFirst(getQueriesForScan(SEARCH_QUERIES))) {
    if (wgConsecutiveBlocks >= 4) { trackHealth("walgreens", "blocked"); continue; }
    const url = `https://www.walgreens.com/search/results.jsp?Ntt=${encodeURIComponent(query)}`;
    try {
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: 15000 });
      await page.waitForSelector(".card__product, [class*='card__product'], [data-testid*='product']", { timeout: 8000 }).catch(() => {});

      if (await isBlockedPage(page)) {
        const solved = await solveHumanChallenge(page);
        if (solved) {
          await page.goto(url, { waitUntil: "domcontentloaded", timeout: 15000 }).catch(() => {});
          await page.waitForSelector(".card__product, [class*='card__product'], [data-testid*='product']", { timeout: 8000 }).catch(() => {});
        }
        if (!solved || (await isBlockedPage(page))) {
          console.warn(`[walgreens] Bot detection page for query "${query}" — skipping`);
          trackHealth("walgreens", "blocked", { reason: "waf" });
          wgConsecutiveBlocks++;
          continue;
        }
      }

      const products = await page.$$eval(
        ".card__product, [class*='card__product'], [data-testid*='product']",
        /* v8 ignore start -- browser-only DOM callback */
        (cards) =>
          cards.map((el) => {
            const text = (el.textContent || "").toLowerCase();
            return {
              title: (el.querySelector(".product__title, [class*='product__title'], [class*='productTitle']") || {}).textContent?.trim() || "",
              price: (el.querySelector(".product__price-contain, [class*='product__price'], [class*='productPrice']") || {}).textContent?.trim() || "",
              url: (el.querySelector('a[href*="ID="], a[href*="/store/product/"]') || {}).href || "",
              // Match multiple OOS/unconfirmed text variants to survive copy changes.
              // "price available in store" = catalog listing with no confirmed stock
              // (calling stores revealed they don't even carry liquor).
              outOfStock: text.includes("not sold at your store") ||
                          text.includes("not available at this store") ||
                          text.includes("out of stock") ||
                          text.includes("unavailable") ||
                          text.includes("price available in store") ||
                          text.includes("check your local store"),
            };
          })
        /* v8 ignore stop */
      );

      for (const p of products) {
        if (p.outOfStock) continue;
        // Skip catalog-only listings with no real price — "Price available in store"
        // means Walgreens can't confirm stock online. Reduces false positives.
        const priceText = (p.price || "").toLowerCase();
        if (!priceText || priceText.includes("available in store") || priceText.includes("check your")) continue;
        for (const bottle of TARGET_BOTTLES) {
          if (matchesBottle(p.title, bottle, "walgreens")) {
            const idMatch = p.url.match(/ID=(\w+)/);
            found.push({
              name: bottle.name,
              url: p.url ? (p.url.startsWith("http") ? p.url : `https://www.walgreens.com${p.url}`) : "",
              price: p.price || "",
              sku: idMatch?.[1] || "",
              size: parseSize(p.title),
              fulfillment: "In-Store",
            });
          }
        }
      }
      trackHealth("walgreens", "ok");
      wgConsecutiveBlocks = 0; // Reset on success — only consecutive blocks trigger early exit
    } catch (err) {
      console.error(`[walgreens] Error searching "${query}": ${err.message}`);
      trackHealth("walgreens", "fail");
      wgConsecutiveBlocks++;
    }
    await sleep(2000 + Math.random() * 2000);
  }
  return dedupFound(found);
}

// Wrapper: browser-only (no fetch-first — Akamai blocks direct HTTP).
// Since Walgreens is scrapeOnce, a single Akamai challenge wipes out all stores.
// Retry once with a fresh browser if ≥50% of queries were blocked.
async function scrapeWalgreensStore() {
  for (let attempt = 0; attempt < 2; attempt++) {
    if (attempt > 0) {
      console.log("[walgreens] Retrying with fresh browser after blocks");
      // Reset walgreens health so retry metrics aren't polluted by first attempt
      delete scraperHealth["walgreens"];
      // Clear cached context so we get a fresh browser instance
      if (retailerBrowserCache["walgreens"]) {
        await retailerBrowserCache["walgreens"].context.close().catch(() => {});
        delete retailerBrowserCache["walgreens"];
      }
      await sleep(3000 + Math.random() * 2000); // Cool-down before retry
    }
    console.log("[walgreens] Using clean browser");
    let page;
    try {
      ({ page } = await launchRetailerBrowser("walgreens", { clean: true }));
    } catch (err) {
      console.error(`[walgreens] Browser launch failed: ${err.message}`);
      trackHealth("walgreens", "fail");
      return [];
    }
    try {
      const scraperPromise = scrapeWalgreensViaBrowser(page);
      scraperPromise.catch(() => {}); // Prevent unhandled rejection if timeout closes page
      const result = await withTimeout(scraperPromise, 300000, null);
      if (result === null) {
        console.warn("[walgreens] Browser scraper timed out (300s)");
        trackHealth("walgreens", "fail");
        if (attempt === 0) {
          console.warn("[walgreens] Timeout on first attempt — retrying with fresh browser");
          continue; // finally{} closes page
        }
        return [];
      }
      // Check if too many queries were blocked — worth retrying with fresh browser
      const wgHealth = scraperHealth["walgreens"];
      if (wgHealth && wgHealth.blocked > 0 && wgHealth.blocked >= wgHealth.queries / 2 && attempt === 0) {
        console.warn(`[walgreens] ${wgHealth.blocked}/${wgHealth.queries} queries blocked — retrying`);
        continue; // finally{} closes page
      }
      return result;
    } finally {
      await page.close().catch(() => {});
    }
  }
  return []; // Should not reach here, but safety fallback
}

// ─── Sam's Club: per-product-URL fetch with browser fallback ─────────────────
// Sam's Club search does NOT return spirits — product pages exist at /ip/{slug}/{id}
// but are excluded from search. We check each known product page directly.

// Map TARGET_BOTTLES name → Sam's Club product ID (usItemId or prodId).
// Only bottles with known Sam's Club pages are included.
const SAMSCLUB_PRODUCTS = {
  "Blanton's Original":       "prod23140012",
  "Weller Special Reserve":   "prod20595259",
  "E.H. Taylor Small Batch":  "prod25791990",
  "Stagg Jr":                 "prod25430037",
  "George T. Stagg":          "prod24381483",
  "Eagle Rare 17 Year":       "prod24381479",
  "Thomas H. Handy":          "prod24381485",
  "Elmer T. Lee":             "prod14810163",
  "Pappy Van Winkle 10 Year": "prod25450252",
  "Pappy Van Winkle 12 Year": "prod25450253",
  "Pappy Van Winkle 15 Year": "prod27331296",
  "Pappy Van Winkle 20 Year": "prod27331307",
  "Pappy Van Winkle 23 Year": "prod16460200",
  "Buffalo Trace":            "13791619865",    // canary
};

// High-value Sam's Club products that deserve retry if their page fetch fails
const PRIORITY_SAMSCLUB_PRODUCTS = new Set([
  "George T. Stagg", "Eagle Rare 17 Year", "Thomas H. Handy",
  "Pappy Van Winkle 10 Year", "Pappy Van Winkle 12 Year",
  "Pappy Van Winkle 15 Year", "Pappy Van Winkle 20 Year", "Pappy Van Winkle 23 Year",
]);

// Extract product availability from Sam's Club __NEXT_DATA__ JSON.
// Product data lives at props.pageProps.initialData.data.product
// (differs from Walmart's searchResult.itemStacks path).
function matchSamsClubProduct(nextData, bottleName) {
  const product = nextData?.props?.pageProps?.initialData?.data?.product;
  if (!product?.name) return null;
  const inStock = product.availabilityStatusV2?.value === "IN_STOCK" ||
    (product.availabilityStatus === "IN_STOCK");
  if (!inStock) return null;
  return {
    name: bottleName,
    url: product.canonicalUrl ? `https://www.samsclub.com${product.canonicalUrl}` : "",
    price: product.priceInfo?.linePriceDisplay || "",
    sku: product.usItemId || product.productId || "",
    size: parseSize(product.name),
    fulfillment: (product.fulfillmentSummary || []).map((f) => f.fulfillment).filter(Boolean).join(", "),
  };
}

// Fetch-first: check each product page directly via HTTP, extract __NEXT_DATA__.
// Returns found[] on success, null if blocked (triggers browser fallback).
async function scrapeSamsClubViaFetch() {
  if (!isProxyAvailable()) return null; // skip if no working proxy
  const found = [];
  let failures = 0;
  let validPages = 0;
  let rotated = false;
  const entries = shuffle(Object.entries(SAMSCLUB_PRODUCTS));
  const failedPriorityProducts = [];

  // Pre-warm: fetch homepage to get PerimeterX session cookies before product pages.
  let cookies = "";
  try {
    const homeRes = await scraperFetchRetry("https://www.samsclub.com/", {
      headers: FETCH_HEADERS,
      timeout: 10000,
      proxyUrl: getRetailerProxyUrl("samsclub"),
    });
    const rawSetCookie = homeRes.headers["set-cookie"];
    const setCookies = Array.isArray(rawSetCookie) ? rawSetCookie : rawSetCookie ? [rawSetCookie] : [];
    cookies = setCookies.map((c) => c.split(";")[0]).join("; ");
  } catch { /* continue without cookies */ }
  const cachedSams = getCachedCookies("samsclub");
  if (cachedSams) cookies = cookies ? `${cookies}; ${cachedSams}` : cachedSams;

  const productTasks = entries.map(([bottleName, productId], i) => async () => {
    if (failures > Math.floor(entries.length / 2)) return; // Early-abort — browser will track its own
    // Rotate proxy IP after 2 consecutive failures (likely burned IP)
    if (failures >= 2 && !rotated) {
      rotated = true;
      rotateRetailerProxy("samsclub");
      cookies = "";
      await sleep(1000 + Math.random() * 1000);
    }
    // Inter-query delay: wider jitter (2-5s, 15% chance of 7-10s "reading" pause)
    if (i > 0) await sleep(2000 + Math.random() * 3000 + (Math.random() < 0.15 ? 5000 : 0));
    const url = `https://www.samsclub.com/ip/${productId}`;
    const headers = { ...FETCH_HEADERS };
    if (cookies) headers["Cookie"] = cookies;
    if (i > 0) {
      headers["Sec-Fetch-Site"] = "same-origin";
      headers["Referer"] = "https://www.samsclub.com/";
    }
    try {
      const _prio = PRIORITY_SAMSCLUB_PRODUCTS.has(bottleName);
      const res = await scraperFetchRetry(url, { headers, timeout: 15000, proxyUrl: getRetailerProxyUrl("samsclub") });
      if (!res.ok) { failures++; if (_prio) failedPriorityProducts.push([bottleName, productId]); return; }
      const html = await res.text();
      if (isFetchBlocked(html)) { failures++; if (_prio) failedPriorityProducts.push([bottleName, productId]); return; }
      // Brace-counting JSON extraction (same as Walmart path)
      const idx = html.indexOf('id="__NEXT_DATA__"');
      if (idx === -1) { failures++; if (_prio) failedPriorityProducts.push([bottleName, productId]); return; }
      const braceStart = html.indexOf("{", idx);
      if (braceStart === -1) { failures++; if (_prio) failedPriorityProducts.push([bottleName, productId]); return; }
      let depth = 0, inStr = false, escape = false, end = -1;
      for (let j = braceStart; j < html.length; j++) {
        const ch = html[j];
        if (escape) { escape = false; continue; }
        if (ch === "\\") { escape = true; continue; }
        if (ch === '"') { inStr = !inStr; continue; }
        if (inStr) continue;
        if (ch === "{") depth++;
        else if (ch === "}") { depth--; if (depth === 0) { end = j + 1; break; } }
      }
      if (end === -1) { failures++; if (_prio) failedPriorityProducts.push([bottleName, productId]); return; }
      const nextData = JSON.parse(html.slice(braceStart, end));
      const product = nextData?.props?.pageProps?.initialData?.data?.product;
      if (!product?.name) { failures++; if (_prio) failedPriorityProducts.push([bottleName, productId]); return; }
      validPages++;
      trackHealth("samsclub", "ok");
      const match = matchSamsClubProduct(nextData, bottleName);
      if (match) found.push(match);
    } catch {
      failures++;
      if (PRIORITY_SAMSCLUB_PRODUCTS.has(bottleName)) failedPriorityProducts.push([bottleName, productId]);
    }
  });
  await runWithConcurrency(productTasks, 2);

  // Retry failed priority products once — Pappy/BTAC can't be silently dropped
  /* v8 ignore start -- priority retry uses same brace-counting + matchSamsClubProduct tested elsewhere */
  if (failedPriorityProducts.length > 0 && validPages > 0) {
    console.log(`[samsclub] Retrying ${failedPriorityProducts.length} failed priority products: ${failedPriorityProducts.map(([n]) => n).join(", ")}`);
    await sleep(2000 + Math.random() * 2000);
    for (const [bottleName, productId] of failedPriorityProducts) {
      try {
        const url = `https://www.samsclub.com/ip/${productId}`;
        const headers = { ...FETCH_HEADERS, "Sec-Fetch-Site": "same-origin", "Referer": "https://www.samsclub.com/" };
        if (cookies) headers["Cookie"] = cookies;
        const res = await scraperFetchRetry(url, { headers, timeout: 15000, proxyUrl: getRetailerProxyUrl("samsclub") });
        if (!res.ok) continue;
        const html = await res.text();
        if (isFetchBlocked(html)) continue;
        const idx = html.indexOf('id="__NEXT_DATA__"');
        if (idx === -1) continue;
        const braceStart = html.indexOf("{", idx);
        if (braceStart === -1) continue;
        let depth = 0, inStr = false, escape = false, end = -1;
        for (let j = braceStart; j < html.length; j++) {
          const ch = html[j];
          if (escape) { escape = false; continue; }
          if (ch === "\\") { escape = true; continue; }
          if (ch === '"') { inStr = !inStr; continue; }
          if (inStr) continue;
          if (ch === "{") depth++;
          else if (ch === "}") { depth--; if (depth === 0) { end = j + 1; break; } }
        }
        if (end === -1) continue;
        const nextData = JSON.parse(html.slice(braceStart, end));
        const match = matchSamsClubProduct(nextData, bottleName);
        if (match) found.push(match);
        trackHealth("samsclub", "ok");
        console.log(`[samsclub] Priority retry succeeded: "${bottleName}"`);
      } catch { /* already retried, move on */ }
      await sleep(1000 + Math.random() * 1000);
    }
  }
  /* v8 ignore stop */

  // Require at least 25% of products to succeed — low success rate triggers browser fallback
  const minValid = Math.max(1, Math.ceil(entries.length / 4));
  if (validPages < minValid) return null;
  return dedupFound(found);
}

// Browser-based Sam's Club scraper (fallback).
async function scrapeSamsClubViaBrowser(page) {
  // Pre-warm: visit homepage to let PerimeterX sensor collect behavioral telemetry.
  // Same parent company as Walmart — uses the same PerimeterX integration.
  await page.goto("https://www.samsclub.com/", { waitUntil: "networkidle", timeout: 20000 }).catch(() => {});
  await sleep(5000 + Math.random() * 3000);
  await solveHumanChallenge(page);
  await humanizePage(page, { pace: "slow" });
  await navigateCategory(page, "samsclub");

  const found = [];
  const entries = shuffle(Object.entries(SAMSCLUB_PRODUCTS));
  for (const [bottleName, productId] of entries) {
    const url = `https://www.samsclub.com/ip/${productId}`;
    try {
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 });
      await page.waitForFunction(
        () => document.querySelector('script#__NEXT_DATA__')?.textContent?.length > 100,
        { timeout: 10000 },
      ).catch(() => {});

      if (await isBlockedPage(page)) {
        // Try solving PerimeterX "Press & Hold" challenge before giving up
        const solved = await solveHumanChallenge(page);
        if (solved) {
          await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 }).catch(() => {});
          await page.waitForFunction(
            () => document.querySelector('script#__NEXT_DATA__')?.textContent?.length > 100,
            { timeout: 10000 },
          ).catch(() => {});
        }
        if (!solved || (await isBlockedPage(page))) {
          console.warn(`[samsclub] Bot detection on product ${productId} — skipping`);
          trackHealth("samsclub", "blocked", { reason: "waf" });
          continue;
        }
      }

      const nextData = await page.evaluate(
        /* v8 ignore start -- browser-only DOM callback */
        () => {
          const el = document.querySelector("script#__NEXT_DATA__");
          if (el) try { return JSON.parse(el.textContent); } catch {}
          return null;
        },
        /* v8 ignore stop */
      );

      if (nextData) {
        const match = matchSamsClubProduct(nextData, bottleName);
        if (match) found.push(match);
        trackHealth("samsclub", "ok");
      } else {
        // __NEXT_DATA__ missing but page wasn't flagged as blocked — degraded response
        console.warn(`[samsclub] No __NEXT_DATA__ for product ${productId} — marking degraded`);
        trackHealth("samsclub", "blocked", { reason: "contract_drift" });
      }
    } catch (err) {
      console.error(`[samsclub] Error checking ${bottleName}: ${err.message}`);
      trackHealth("samsclub", "fail");
    }
    await sleep(2000 + Math.random() * 2000);
  }
  return dedupFound(found);
}

// Wrapper: try fetch-first, fall back to clean headed browser with retry.
// Sam's Club PerimeterX blocks product pages aggressively — fetch sometimes works
// when proxy IP is clean, browser works with clean headed Chrome.
async function scrapeSamsClubStore() {
  // Try fetch-first (sometimes works with clean proxy IP)
  const fetchResult = await scrapeSamsClubViaFetch();
  if (fetchResult !== null) {
    console.log("[samsclub] Used fast fetch mode (proxied)");
    return fetchResult;
  }

  // Reset health from failed fetch before browser attempt
  delete scraperHealth["samsclub"];

  // Browser fallback with retry (clean + headed on Mac to defeat PerimeterX)
  for (let attempt = 0; attempt < 2; attempt++) {
    if (attempt > 0) {
      console.log("[samsclub] Retrying with fresh browser after blocks");
      delete scraperHealth["samsclub"];
      if (retailerBrowserCache["samsclub"]) {
        await retailerBrowserCache["samsclub"].context.close().catch(() => {});
        delete retailerBrowserCache["samsclub"];
      }
      await sleep(3000 + Math.random() * 2000);
    }

    console.log("[samsclub] Using clean browser");
    let page;
    try {
      ({ page } = await launchRetailerBrowser("samsclub", { clean: true, headless: !IS_MAC }));
    } catch (err) {
      console.error(`[samsclub] Browser launch failed: ${err.message}`);
      trackHealth("samsclub", "fail");
      return [];
    }
    try {
      const scraperPromise = scrapeSamsClubViaBrowser(page);
      scraperPromise.catch(() => {});
      const result = await withTimeout(scraperPromise, 300000, null);
      if (result === null) {
        console.warn("[samsclub] Browser scraper timed out (300s)");
        trackHealth("samsclub", "fail");
        if (attempt === 0) {
          console.warn("[samsclub] Timeout on first attempt — retrying with fresh browser");
          continue;
        }
        return [];
      }
      // Check if too many products were blocked — retry with fresh browser
      const scHealth = scraperHealth["samsclub"];
      if (scHealth && scHealth.queries > 0 && scHealth.blocked >= scHealth.queries / 2 && attempt === 0) {
        console.warn(`[samsclub] ${scHealth.blocked}/${scHealth.queries} products blocked — retrying`);
        continue;
      }
      await cacheRetailerCookies("samsclub");
      return result;
    } finally {
      await page.close().catch(() => {});
    }
  }
  return [];
}

// ─── API-based scrapers ─────────────────────────────────────────────────────

// Fetch Kroger OAuth token once, shared across all store scrapers.
// Uses a singleton promise so concurrent tasks don't race for the same token.
let krogerToken = null;
let krogerTokenPromise = null;
async function getKrogerToken() {
  if (krogerToken) return krogerToken;
  /* v8 ignore next -- env guard */
  if (!KROGER_CLIENT_ID || !KROGER_CLIENT_SECRET) return null;
  // Singleton: if another task is already fetching, wait for the same promise
  if (!krogerTokenPromise) {
    krogerTokenPromise = (async () => {
      const authHeader = Buffer.from(`${KROGER_CLIENT_ID}:${KROGER_CLIENT_SECRET}`).toString("base64");
      // Use got-scraping for Chrome TLS fingerprint — Kroger's API gateway rejects node-fetch's TLS
      const krogerProxy = getRetailerProxyUrl("kroger");
      const res = await scraperFetchRetry("https://api.kroger.com/v1/connect/oauth2/token", {
        method: "POST",
        headers: { Authorization: `Basic ${authHeader}`, "Content-Type": "application/x-www-form-urlencoded" },
        body: "grant_type=client_credentials&scope=product.compact",
        timeout: 15000,
        proxyUrl: krogerProxy || undefined,
      });
      if (!res.ok) throw new Error(`OAuth HTTP ${res.status}`);
      krogerToken = (await res.json()).access_token;
      return krogerToken;
    })().finally(() => { krogerTokenPromise = null; });
  }
  return krogerTokenPromise;
}

// Direct Kroger product IDs (UPC codes) for allocated bottles.
// Used for surgical API lookups via GET /v1/products/{id}?filter.locationId={storeId}
// which bypass search ranking/suppression. Fry's shares the same catalog.
const KROGER_PRODUCTS = {
  "Buffalo Trace":              "0008024400923", // canary
  "Blanton's Original":         "0008024400203",
  "Blanton's Gold":             "0008024400939",
  "E.H. Taylor Small Batch":    "0008800400549",
  "E.H. Taylor Single Barrel":  "0008800400551",
  "E.H. Taylor Barrel Proof":   "0008800400552",
  "E.H. Taylor Straight Rye":   "0008800400550",
  "E.H. Taylor Four Grain":      "0008800402454",
  "E.H. Taylor Amaranth":        "0008800403476",
  "E.H. Taylor 18 Year Marriage": "0008800404015",
  "Weller Special Reserve":     "0008800402574",
  "Weller Antique 107":         "0008800402564",
  "Weller 12 Year":             "0008800402774",
  "Weller Full Proof":          "0008800403149",
  "Weller C.Y.P.B.":            "0008800403148",
  "Stagg Jr":                   "0008800401858",
  "George T. Stagg":            "0008800402784",
  "Eagle Rare 17 Year":         "0008800402144",
  "William Larue Weller":       "0008800402595",
  "Thomas H. Handy":            "0008800400003",
  "Pappy Van Winkle 10 Year":   "0008931912367",
  "Pappy Van Winkle 12 Year":   "0008931912373",
  "Pappy Van Winkle 15 Year":   "0008931912374",
  "Pappy Van Winkle 20 Year":   "0008931912372",
  "Pappy Van Winkle 23 Year":   "0008931912378",
  "Elmer T. Lee":               "0008024400773",
  "Rock Hill Farms":            "0008024400683",
};

// Direct product lookup via Kroger API — bypasses search, immune to search suppression.
// Returns found[] for a specific store. Runs before search queries for fastest detection.
// Classify a Kroger find by signal strength. Kroger's `inStore: true` is misleading
// alone — it can mean "store has a planogram slot reserved" rather than "bottle is
// physically on the shelf right now". Use facings + price freshness to distinguish:
//
//   confirmed: facings >= 3  OR  effectiveDate within 14 days
//     → real stock, drive over
//   lead:      single allocated slot, stale data
//     → store gets allocated this bottle but slot may be empty; call first
//
// Caller routes confirmed → @here urgent alert, lead → quiet alert.
function classifyKrogerFind(product, item) {
  const aisle = product?.aisleLocations?.[0];
  const facings = aisle?.numberOfFacings ? parseInt(aisle.numberOfFacings) : 0;
  const effDate = item?.price?.effectiveDate?.value;
  const ageDays = effDate ? Math.floor((Date.now() - new Date(effDate).getTime()) / 86400000) : null;
  const isConfirmed = facings >= 3 || (ageDays !== null && ageDays <= 14);
  return {
    confidence: isConfirmed ? "confirmed" : "lead",
    aisle: aisle ? `${aisle.description} ${aisle.number}, Shelf ${aisle.shelfNumber}` : null,
    facings: facings || null,
    dataAgeDays: ageDays,
  };
}

// Build a Kroger product URL from the API's `productPageURI` (slug-based, e.g.
// `/p/george-t-stagg-15-year-bourbon/0008800402784?cid=...`) — the productId-only
// fallback URL `/p/{productId}` 404s for some products. Strip the cid analytics param.
function buildKrogerProductUrl(product, productId) {
  const uri = product?.productPageURI;
  if (uri && uri.startsWith("/p/")) {
    return `https://www.kroger.com${uri.split("?")[0]}`;
  }
  return productId ? `https://www.kroger.com/p/${productId}` : "";
}

async function checkKrogerKnownProducts(store, token) {
  const found = [];
  const entries = Object.entries(KROGER_PRODUCTS);

  // Run in parallel batches of 5 (API-key auth, no bot detection risk)
  await Promise.all(entries.map(async ([bottleName, productId], i) => {
    await sleep(i * 50); // stagger starts
    try {
      const url = `https://api.kroger.com/v1/products/${productId}?filter.locationId=${store.storeId}`;
      const krogerDirectProxy = getRetailerProxyUrl("kroger");
      const res = await scraperFetchRetry(url, {
        headers: { Authorization: `Bearer ${token}`, Accept: "application/json" },
        timeout: 10000,
        proxyUrl: krogerDirectProxy || undefined,
      });
      if (!res.ok) return;
      const data = await res.json();
      const product = data.data;
      if (!product) return;
      // Kroger removed `inventory.stockLevel` from spirits responses in 2026 — trust
      // `fulfillment.inStore: true` as the in-stock signal. Still reject if stockLevel
      // is present and explicitly OOS (defensive guard for products that retain it).
      const isKrogerItemInStock = (item) =>
        item.fulfillment?.inStore === true &&
        (item.inventory?.stockLevel == null ||
         !String(item.inventory.stockLevel).toLowerCase().includes("out_of_stock"));
      const inStock = product.items?.some(isKrogerItemInStock);
      if (!inStock) return;
      const inStoreItem = product.items?.find(isKrogerItemInStock) || product.items?.[0];
      const price = inStoreItem?.price?.promo ?? inStoreItem?.price?.regular;
      const cls = classifyKrogerFind(product, inStoreItem);
      found.push({
        name: bottleName,
        url: buildKrogerProductUrl(product, productId),
        price: price != null ? `$${price.toFixed(2)}` : "",
        sku: productId,
        size: inStoreItem?.size || "",
        fulfillment: inStoreItem?.fulfillment?.inStore ? "In-store" : "",
        confidence: cls.confidence,
        aisle: cls.aisle,
        facings: cls.facings,
        dataAgeDays: cls.dataAgeDays,
      });
      console.log(`[kroger:${store.storeId}] Direct lookup: ${bottleName} in stock!`);
    } catch { /* individual product lookup failure — continue */ }
  }));
  return found;
}

// HTML verification for Kroger candidates. Visits the actual product page on
// frysfood.com and parses the "Item Availability" widget. The widget reflects
// the user's preferred store, which we set per-store via /atlas/v1/modality/preferences.
//
// Returns the candidates array with `confidence` adjusted based on what the
// website says. Three outcomes per candidate:
//   confirmed: page says sellable          → upgrade confidence to "confirmed"
//   refuted:   page says "Item Unavailable" → drop from results (silent)
//   unverified: Akamai blocked, timeout, or page never loaded → keep B+C tier as-is
//
// All errors are graceful — verification failure NEVER drops the underlying B+C
// classification. Worst case is unchanged behavior. Best case is filtered noise.
//
// Time budget: 90s total per store. If exceeded, remaining candidates are unverified.
/* v8 ignore start -- requires live Akamai bypass + Fry's session, can't unit-test the page interaction */
async function verifyKrogerCandidatesViaWebsite(store, candidates) {
  if (!candidates || candidates.length === 0) return candidates;
  if (KROGER_HTML_VERIFY !== "true") return candidates;

  const verified = [];
  const start = Date.now();
  const TIME_BUDGET_MS = 90000;

  let page;
  try {
    ({ page } = await launchRetailerBrowser("kroger", { clean: true }));
  } catch (err) {
    console.warn(`[kroger:${store.storeId}] HTML verify: browser launch failed, keeping B+C tier (${err.message})`);
    return candidates;
  }

  try {
    // Pre-warm: earn Akamai _abck cookie. Skip if cache is still warm from a prior store.
    const skipPreWarm = !!getCachedCookies("kroger");
    if (!skipPreWarm) {
      try {
        await page.goto("https://www.frysfood.com/", { waitUntil: "domcontentloaded", timeout: 25000 });
        await sleep(4000 + Math.random() * 2000);
        await humanizePage(page);
      } catch (err) {
        console.warn(`[kroger:${store.storeId}] HTML verify: pre-warm failed, keeping B+C tier (${err.message})`);
        return candidates;
      }
    }

    // Set the user's preferred store to this one. Fry's modality/preferences POST
    // requires a full destination object with address + lat/lng, available via the
    // store locator endpoint. If this step fails, all candidates remain unverified.
    let storeContextSet = false;
    try {
      const storeData = await page.evaluate(async (storeId) => {
        const res = await fetch(`/atlas/v1/stores/v2/locator?filter.locationIds=${storeId}&projections=full`, {
          headers: { "x-kroger-channel": "WEB", accept: "application/json" },
        });
        return { status: res.status, body: res.ok ? await res.json() : await res.text() };
      }, store.storeId);
      // DEBUG: log locator outcome to diagnose why store context isn't setting
      if (storeData.status !== 200) {
        console.warn(`[kroger:${store.storeId}] DEBUG locator failed status=${storeData.status} body=${String(storeData.body).slice(0, 200)}`);
      }
      const sd = storeData?.body?.data?.stores?.[0];
      if (sd?.locale?.address) {
        const setResult = await page.evaluate(async ({ locId, addr, loc }) => {
          const res = await fetch("/atlas/v1/modality/preferences?filter.restrictLafToFc=false", {
            method: "POST",
            headers: { "Content-Type": "application/json", "x-kroger-channel": "WEB" },
            body: JSON.stringify({
              modalityPreferences: {
                capabilities: { DELIVERY: true, IN_STORE: true, PICKUP: true, SHIP: true },
                modalities: [{
                  modalityType: "PICKUP",
                  destination: { locationId: locId, address: addr, location: loc },
                  fulfillment: [locId],
                  fallbackFulfillment: locId,
                  isCrossBanner: false,
                  isTrustedSource: false,
                  source: "DEFAULT_MODALITY_ADDRESS",
                }],
              },
            }),
          });
          const body = await res.text();
          return { status: res.status, body: body.slice(0, 400) };
        }, { locId: store.storeId, addr: sd.locale.address, loc: sd.locale.location });
        storeContextSet = setResult.status === 200;
        // DEBUG: log preferences POST outcome — if non-200, body tells us what's wrong
        if (!storeContextSet) {
          console.warn(`[kroger:${store.storeId}] DEBUG modality POST failed status=${setResult.status} body=${setResult.body}`);
        }
      } else {
        console.warn(`[kroger:${store.storeId}] DEBUG locator returned no address — sd.locale.address missing. storeData keys: ${Object.keys(storeData?.body?.data?.stores?.[0] || {}).join(",")}`);
      }
    } catch (err) {
      console.warn(`[kroger:${store.storeId}] HTML verify: setting store context failed (${err.message})`);
    }

    if (!storeContextSet) {
      console.warn(`[kroger:${store.storeId}] HTML verify: could not set store context, keeping B+C tier`);
      return candidates;
    }

    // For each candidate, navigate to its product page and parse the Item Availability widget
    for (const c of candidates) {
      if (Date.now() - start > TIME_BUDGET_MS) {
        console.warn(`[kroger:${store.storeId}] HTML verify: time budget exceeded, ${candidates.length - verified.length} unverified`);
        // Push remaining as unverified (keep B+C classification)
        for (let i = verified.length; i < candidates.length; i++) verified.push(candidates[i]);
        break;
      }
      if (!c.url) { verified.push(c); continue; }
      try {
        await page.goto(c.url, { waitUntil: "domcontentloaded", timeout: 20000 });
        await sleep(2500 + Math.random() * 1500);
        const bodyText = await page.evaluate(() => document.body.innerText || "");
        // Bot wall — graceful fallback
        if (/access denied|robot|verify you are human/i.test(bodyText.slice(0, 2000))) {
          console.warn(`[kroger:${store.storeId}] HTML verify: bot wall on ${c.name}, keeping B+C`);
          verified.push(c);
          continue;
        }
        // The Item Availability widget shows "Item Unavailable" when nothing is sellable
        // at the user's preferred store. If we see it, refute the candidate (silent drop).
        const isUnavailable = /Item Unavailable/i.test(bodyText);
        if (isUnavailable) {
          console.log(`[kroger:${store.storeId}] HTML verify REFUTED: ${c.name} (page says Item Unavailable)`);
          // Silent drop — do NOT push to verified
        } else {
          console.log(`[kroger:${store.storeId}] HTML verify CONFIRMED: ${c.name}`);
          verified.push({ ...c, confidence: "confirmed", htmlVerified: true });
        }
      } catch (err) {
        console.warn(`[kroger:${store.storeId}] HTML verify: page error for ${c.name} — keeping B+C (${err.message})`);
        verified.push(c);  // graceful: keep B+C tier
      }
    }

    await cacheRetailerCookies("kroger");
    return verified;
  } finally {
    await page.close().catch(() => {});
  }
}
/* v8 ignore stop */

async function scrapeKrogerStore(store) {
  /* v8 ignore next 3 -- env guard */
  if (!KROGER_CLIENT_ID || !KROGER_CLIENT_SECRET) {
    console.warn("[kroger] Skipping — KROGER_CLIENT_ID / KROGER_CLIENT_SECRET not set");
    return [];
  }

  let token;
  try {
    token = await getKrogerToken();
  } catch (err) {
    console.error(`[kroger:${store.storeId}] OAuth failed: ${err.message}`);
    trackHealth("kroger", "fail");
    return [];
  }

  // Direct product lookups first — bypass search, immune to search suppression
  const knownFound = await checkKrogerKnownProducts(store, token);

  // Then broad search queries for discovery of bottles not in KROGER_PRODUCTS
  const found = [];

  function matchKrogerProducts(products) {
    for (const product of products) {
      const title = product.description || "";
      // Trust `fulfillment.inStore: true` as the in-stock signal — Kroger removed
      // `inventory.stockLevel` from spirits responses in 2026, so requiring it caused
      // 100% false negatives at all Fry's stores. Still reject when stockLevel is
      // present and explicitly OOS for products that retain the field.
      const isKrogerItemInStock = (i) =>
        i.fulfillment?.inStore === true &&
        (i.inventory?.stockLevel == null ||
         !String(i.inventory.stockLevel).toLowerCase().includes("out_of_stock"));
      const inStock = product.items?.some(isKrogerItemInStock);
      if (!inStock) continue;
      for (const bottle of TARGET_BOTTLES) {
        if (matchesBottle(title, bottle, "kroger")) {
          // Prefer the item variant that is actually in-store, not blindly items[0]
          // (which may be a different size at a different price)
          const inStoreItem = product.items?.find(isKrogerItemInStock) || product.items?.[0];
          const price = inStoreItem?.price?.promo ?? inStoreItem?.price?.regular;
          const fulfillmentParts = [];
          if (inStoreItem?.fulfillment?.inStore) fulfillmentParts.push("In-store");
          if (inStoreItem?.fulfillment?.shipToHome) fulfillmentParts.push("Ship to home");
          const cls = classifyKrogerFind(product, inStoreItem);
          found.push({
            name: bottle.name,
            url: buildKrogerProductUrl(product, product.productId),
            price: price != null ? `$${price.toFixed(2)}` : "",
            sku: product.productId || "",
            size: inStoreItem?.size || "",
            fulfillment: fulfillmentParts.join(", "),
            confidence: cls.confidence,
            aisle: cls.aisle,
            facings: cls.facings,
            dataAgeDays: cls.dataAgeDays,
          });
        }
      }
    }
  }

  // Run all queries in parallel (API-key auth, no bot detection risk)
  await Promise.all(shuffleKeepCanaryFirst(getQueriesForScan(SEARCH_QUERIES)).map(async (query, i) => {
    await sleep(i * 50); // stagger starts to avoid thundering herd
    const baseUrl = `https://api.kroger.com/v1/products?filter.term=${encodeURIComponent(query)}&filter.locationId=${store.storeId}&filter.limit=50`;
    try {
      // Use got-scraping for Chrome TLS fingerprint — Kroger's API gateway rejects node-fetch
      const krogerProxy2 = getRetailerProxyUrl("kroger");
      const res = await scraperFetchRetry(baseUrl, {
        headers: { Authorization: `Bearer ${token}`, Accept: "application/json" },
        timeout: 15000,
        proxyUrl: krogerProxy2 || undefined,
      });
      // Clear cached token on 401 so next call re-authenticates
      if (res.status === 401) { krogerToken = null; throw new Error("Token expired (401)"); }
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      matchKrogerProducts(data.data || []);
      trackHealth("kroger", "ok");
      // Fetch page 2 if first page was full (may have more results).
      // Isolated try/catch: page 2 failure shouldn't lose page 1 data or mark query as failed.
      if (data.data?.length === 50) {
        try {
          const res2 = await scraperFetchRetry(`${baseUrl}&filter.start=50`, {
            headers: { Authorization: `Bearer ${token}`, Accept: "application/json" },
            timeout: 15000,
            proxyUrl: krogerProxy2 || undefined,
          });
          if (res2.ok) {
            const data2 = await res2.json();
            matchKrogerProducts(data2.data || []);
          }
        } catch (p2Err) {
          console.warn(`[kroger:${store.storeId}] Page 2 failed for "${query}": ${p2Err.message}`);
        }
      }
    } catch (err) {
      console.error(`[kroger:${store.storeId}] Error searching "${query}": ${err.message}`);
      trackHealth("kroger", "fail");
    }
  }));
  const candidates = dedupFound([...knownFound, ...found]);
  // Layered verification: B+C tier already classified each find as 'confirmed' or 'lead'.
  // Now optionally upgrade/refute via HTML scrape. Verification is best-effort — any
  // failure silently falls back to the B+C tier classification (graceful by design).
  if (candidates.length > 0 && KROGER_HTML_VERIFY === "true") {
    /* v8 ignore start -- requires live page interaction; covered by integration testing */
    const release = await acquireRetailerLock("kroger");
    try {
      const verifyPromise = verifyKrogerCandidatesViaWebsite(store, candidates);
      // Time budget = 120s per store: 90s of work + 30s buffer for browser launch overhead
      const verified = await withTimeout(verifyPromise, 120000, candidates);
      return verified;
    } catch (err) {
      console.warn(`[kroger:${store.storeId}] HTML verify wrapper failed, keeping B+C: ${err.message}`);
      return candidates;
    } finally {
      release();
    }
    /* v8 ignore stop */
  }
  return candidates;
}

// Match Safeway API response products against TARGET_BOTTLES.
// Reused by both browser-based API-via-evaluate and any future fetch path.
function matchSafewayProducts(products) {
  const found = [];
  for (const product of products) {
    const title = product.name || product.productTitle || "";
    if (product.inStock !== true && product.inStock !== 1) continue;
    for (const bottle of TARGET_BOTTLES) {
      if (matchesBottle(title, bottle, "safeway")) {
        const fulfillmentParts = [];
        if (product.curbsideEligible) fulfillmentParts.push("Curbside");
        if (product.deliveryEligible) fulfillmentParts.push("Delivery");
        found.push({
          name: bottle.name,
          url: product.url ? `https://www.safeway.com${product.url}` : "",
          price: product.price != null ? `$${Number(product.price).toFixed(2)}` : "",
          sku: product.upc || product.pid || "",
          size: parseSize(title),
          fulfillment: fulfillmentParts.join(", "),
        });
      }
    }
  }
  return found;
}

// Fetch-first Safeway scraper. Uses cached browser _abck cookies (from prior browser scrape) to call
// the Safeway product search API directly via got-scraping, bypassing the browser entirely. Falls back
// to null (triggering browser fallback) if cached cookies are stale or Akamai blocks the request.
// The API is a simple GET with Ocp-Apim-Subscription-Key + storeid — no complex auth needed.
/* v8 ignore start -- requires live Akamai _abck cookies from prior browser scrape */
async function scrapeSafewayViaFetch(store) {
  if (!SAFEWAY_API_KEY) return null;
  // Cached browser cookies required — Akamai _abck won't work from scratch via fetch
  const cachedCookies = getCachedCookies("safeway");
  if (!cachedCookies) return null;

  const found = [];
  let validPages = 0;
  let failures = 0;
  const sfProxy = getRetailerProxyUrl("safeway");

  const baseUrl = "https://www.safeway.com/abs/pub/xapi/pgmsearch/v1/search/products";
  const queries = shuffleKeepCanaryFirst(getQueriesForScan(SEARCH_QUERIES));

  for (let i = 0; i < queries.length; i++) {
    const query = queries[i];
    if (failures > 3) break; // Too many failures — fall back to browser
    if (i > 0) await sleep(1500 + Math.random() * 2000);
    const apiUrl = `${baseUrl}?request-id=0&url=https://www.safeway.com&pageurl=search&search-type=keyword&q=${encodeURIComponent(query)}&rows=50&start=0&storeid=${store.storeId}`;
    try {
      const headers = {
        ...FETCH_HEADERS,
        "Ocp-Apim-Subscription-Key": SAFEWAY_API_KEY,
        Accept: "application/json",
        Cookie: cachedCookies,
      };
      if (i > 0) {
        headers["Sec-Fetch-Site"] = "same-origin";
        headers["Referer"] = "https://www.safeway.com/";
      }
      const res = await scraperFetchRetry(apiUrl, {
        headers,
        timeout: 15000,
        proxyUrl: sfProxy || undefined,
      });
      if (!res.ok) { failures++; continue; }
      const data = await res.json();
      if (isFetchBlocked(JSON.stringify(data).slice(0, 10000))) { failures++; continue; }
      if (data?.primaryProducts?.response?.docs) {
        found.push(...matchSafewayProducts(data.primaryProducts.response.docs));
        validPages++;
        trackHealth("safeway", "ok");

        // Page 2 if first page was full
        if (data.primaryProducts.response.docs.length === 50) {
          try {
            const page2Url = `${baseUrl}?request-id=0&url=https://www.safeway.com&pageurl=search&search-type=keyword&q=${encodeURIComponent(query)}&rows=50&start=50&storeid=${store.storeId}`;
            const res2 = await scraperFetchRetry(page2Url, { headers, timeout: 15000, proxyUrl: sfProxy || undefined });
            if (res2.ok) {
              const data2 = await res2.json();
              if (data2?.primaryProducts?.response?.docs) {
                found.push(...matchSafewayProducts(data2.primaryProducts.response.docs));
              }
            }
          } catch { /* page 2 failure is non-critical */ }
        }
      } else {
        failures++;
      }
    } catch {
      failures++;
    }
  }

  // Require at least 25% of queries to succeed
  const minValid = Math.max(1, Math.ceil(queries.length / 4));
  if (validPages < minValid) return null;
  console.log(`[safeway:${store.storeId}] Used fast fetch mode (cached browser cookies)`);
  return dedupFound(found);
}
/* v8 ignore stop */

// Browser-based Safeway scraper. Akamai WAF blocks all fetch-based approaches (API + got-scraping),
// so we use a real browser to execute Akamai's JS sensor during pre-warm, then call the Safeway API
// from within the browser context (inherits valid _abck cookies). Falls back to DOM extraction.
async function scrapeSafewayViaBrowser(page, store) {
  const found = [];
  const t0 = Date.now();
  const elapsed = () => `${((Date.now() - t0) / 1000).toFixed(1)}s`;

  // Pre-warm: visit homepage to let WAF sensor (Akamai _abck OR Incapsula incap_ses_*)
  // execute. Use domcontentloaded — Safeway moved to Incapsula in 2026 and the page
  // never reaches networkidle (sensor keeps beaconing). 20s on networkidle was eating
  // the entire 300s budget before queries could run.
  await page.goto("https://www.safeway.com/", { waitUntil: "domcontentloaded", timeout: 20000 }).catch(() => {});
  console.log(`[safeway:${store.storeId}] Homepage loaded (${elapsed()})`);
  // Dwell to let WAF sensor JS run (both Akamai and Incapsula need ~5-8s for cookie issuance)
  await sleep(6000 + Math.random() * 3000);
  // solveHumanChallenge handles PerimeterX "Press & Hold" — no-op for Akamai/Incapsula
  // (they use invisible JS challenges or full CAPTCHA we can't solve programmatically)
  await solveHumanChallenge(page);
  await humanizePage(page, { pace: "medium" });
  // Skip navigateCategory — it adds another ~30-60s and isn't required for the API path
  console.log(`[safeway:${store.storeId}] Pre-warm done (${elapsed()}), starting queries...`);

  const baseUrl = "https://www.safeway.com/abs/pub/xapi/pgmsearch/v1/search/products";
  let consecutiveBlocks = 0;

  for (const query of shuffleKeepCanaryFirst(getQueriesForScan(SEARCH_QUERIES))) {
    if (consecutiveBlocks >= 4) { trackHealth("safeway", "blocked"); continue; }

    try {
      // Primary: call Safeway API from browser context — inherits _abck cookies
      const apiUrl = `${baseUrl}?request-id=0&url=https://www.safeway.com&pageurl=search&search-type=keyword&q=${encodeURIComponent(query)}&rows=50&start=0&storeid=${store.storeId}`;
      /* v8 ignore start -- browser-only API call */
      const data = await page.evaluate(async ({ url, apiKey }) => {
        try {
          const res = await fetch(url, {
            headers: { "Ocp-Apim-Subscription-Key": apiKey, Accept: "application/json" },
          });
          if (!res.ok) return null;
          return await res.json();
        } catch { return null; }
      }, { url: apiUrl, apiKey: SAFEWAY_API_KEY });
      /* v8 ignore stop */

      if (data?.primaryProducts?.response?.docs) {
        const products = data.primaryProducts.response.docs;
        found.push(...matchSafewayProducts(products));
        trackHealth("safeway", "ok");
        consecutiveBlocks = 0;
        console.log(`[safeway:${store.storeId}] API query "${query}": ${products.length} products (${elapsed()})`);

        // Page 2 if first page was full
        if (products.length === 50) {
          try {
            const page2Url = `${baseUrl}?request-id=0&url=https://www.safeway.com&pageurl=search&search-type=keyword&q=${encodeURIComponent(query)}&rows=50&start=50&storeid=${store.storeId}`;
            /* v8 ignore start -- browser-only */
            const data2 = await page.evaluate(async ({ url, apiKey }) => {
              try {
                const res = await fetch(url, { headers: { "Ocp-Apim-Subscription-Key": apiKey, Accept: "application/json" } });
                return res.ok ? await res.json() : null;
              } catch { return null; }
            }, { url: page2Url, apiKey: SAFEWAY_API_KEY });
            /* v8 ignore stop */
            if (data2?.primaryProducts?.response?.docs) {
              found.push(...matchSafewayProducts(data2.primaryProducts.response.docs));
            }
          } catch { /* page 2 failure is non-critical */ }
        }
      } else {
        /* v8 ignore start -- browser-only DOM fallback (API path tested via matchSafewayProducts) */
        // API-via-browser failed — try DOM extraction as fallback
        const searchUrl = `https://www.safeway.com/shop/search-results.html?q=${encodeURIComponent(query)}`;
        await page.goto(searchUrl, { waitUntil: "domcontentloaded", timeout: 15000 });
        await page.waitForSelector("[class*='product'], [data-testid*='product']", { timeout: 8000 }).catch(() => {});

        if (await isBlockedPage(page)) {
          const solved = await solveHumanChallenge(page);
          if (!solved || (await isBlockedPage(page))) {
            console.warn(`[safeway:${store.storeId}] Blocked for query "${query}"`);
            trackHealth("safeway", "blocked", { reason: "waf" });
            consecutiveBlocks++;
            continue;
          }
        }

        const products = await page.$$eval(
          "[class*='product-card'], [class*='productCard'], [data-testid*='product'], .product-item",
          (cards) => cards.map((el) => ({
            title: (el.querySelector("h2, h3, [class*='title'], [class*='name']") || {}).textContent?.trim() || "",
            price: (el.querySelector("[class*='price']") || {}).textContent?.trim() || "",
            url: (el.querySelector("a[href*='/shop/product/']") || {}).href || "",
            outOfStock: (el.textContent || "").toLowerCase().includes("out of stock") ||
                        (el.textContent || "").toLowerCase().includes("unavailable"),
          }))
        );

        for (const p of products) {
          if (p.outOfStock) continue;
          for (const bottle of TARGET_BOTTLES) {
            if (matchesBottle(p.title, bottle, "safeway")) {
              found.push({
                name: bottle.name,
                url: p.url ? (p.url.startsWith("http") ? p.url : `https://www.safeway.com${p.url}`) : "",
                price: p.price || "",
                sku: "",
                size: parseSize(p.title),
                fulfillment: "",
              });
            }
          }
        }
        trackHealth("safeway", products.length > 0 ? "ok" : "fail");
        consecutiveBlocks = 0;
        /* v8 ignore stop */
      }
    } catch (err) {
      console.error(`[safeway:${store.storeId}] Error: ${err.message}`);
      trackHealth("safeway", "fail");
      consecutiveBlocks++;
    }
    await sleep(2500 + Math.random() * 3500);
  }
  return dedupFound(found);
}

// Wrapper: tries fetch-first with cached cookies, then launches browser, handles timeout/retry.
async function scrapeSafewayStore(store) {
  // Fetch-first: use cached browser _abck cookies to call Safeway API directly.
  // Skips browser entirely when cookies are fresh (~15-20s faster per store).
  const fetchResult = await scrapeSafewayViaFetch(store).catch((err) => {
    console.warn(`[safeway:${store.storeId}] Fetch path failed: ${err.message}`);
    return null;
  });
  if (fetchResult !== null) return fetchResult;

  // Reset health from failed fetch before browser attempt
  delete scraperHealth["safeway"];

  for (let attempt = 0; attempt < 2; attempt++) {
    /* v8 ignore start -- browser retry loop internals (launch failure tested separately) */
    if (attempt > 0) {
      console.log("[safeway] Retrying with fresh browser after blocks");
      delete scraperHealth["safeway"];
      if (retailerBrowserCache["safeway"]) {
        await retailerBrowserCache["safeway"].context.close().catch(() => {});
        delete retailerBrowserCache["safeway"];
      }
      await sleep(3000 + Math.random() * 2000);
    }
    /* v8 ignore stop */
    console.log("[safeway] Using clean browser");
    let page;
    try {
      ({ page } = await launchRetailerBrowser("safeway", { clean: true }));
    } catch (err) {
      console.error(`[safeway] Browser launch failed: ${err.message}`);
      trackHealth("safeway", "fail");
      return [];
    }
    try {
      const scraperPromise = scrapeSafewayViaBrowser(page, store);
      scraperPromise.catch(() => {});
      const result = await withTimeout(scraperPromise, 300000, null);
      /* v8 ignore start -- browser timeout/retry paths require real browser + sleep */
      if (result === null) {
        console.warn("[safeway] Browser scraper timed out (300s)");
        trackHealth("safeway", "fail");
        if (attempt === 0) continue;
        return [];
      }
      await cacheRetailerCookies("safeway");
      const sfHealth = scraperHealth["safeway"];
      if (sfHealth && sfHealth.blocked > 0 && sfHealth.blocked >= sfHealth.queries / 2 && attempt === 0) {
        console.warn(`[safeway] ${sfHealth.blocked}/${sfHealth.queries} queries blocked — retrying`);
        continue;
      }
      /* v8 ignore stop */
      return result;
    } finally {
      await page.close().catch(() => {});
    }
  }
  /* v8 ignore next -- unreachable after 2 attempts */
  return [];
}

// ─── Albertsons Scraper ─────────────────────────────────────────────────────
// Same parent company as Safeway — identical API endpoint structure (/abs/pub/xapi),
// same Azure APIM auth, same response shape. Different bot protection (Incapsula vs Akamai).
// Falls back to SAFEWAY_API_KEY if ALBERTSONS_API_KEY not set.

const ALBERTSONS_KEY = ALBERTSONS_API_KEY || SAFEWAY_API_KEY;

// Match Albertsons API response products against TARGET_BOTTLES.
// Same response shape as Safeway (same parent company backend).
function matchAlbertsonsProducts(products) {
  const found = [];
  for (const product of products) {
    const title = product.name || product.productTitle || "";
    if (product.inStock !== true && product.inStock !== 1) continue;
    for (const bottle of TARGET_BOTTLES) {
      if (matchesBottle(title, bottle, "albertsons")) {
        const fulfillmentParts = [];
        if (product.curbsideEligible) fulfillmentParts.push("Curbside");
        if (product.deliveryEligible) fulfillmentParts.push("Delivery");
        found.push({
          name: bottle.name,
          url: product.url ? `https://www.albertsons.com${product.url}` : "",
          price: product.price != null ? `$${Number(product.price).toFixed(2)}` : "",
          sku: product.upc || product.pid || "",
          size: parseSize(title),
          fulfillment: fulfillmentParts.join(", "),
        });
      }
    }
  }
  return found;
}

// Fetch-first Albertsons scraper. Uses cached browser Incapsula cookies to call
// the Albertsons product search API directly. Same API as Safeway, different domain.
/* v8 ignore start -- requires live Incapsula cookies from prior browser scrape */
async function scrapeAlbertsonsViaFetch(store) {
  if (!ALBERTSONS_KEY) return null;
  const cachedCookies = getCachedCookies("albertsons");
  if (!cachedCookies) return null;

  const found = [];
  let validPages = 0;
  let failures = 0;
  const abProxy = getRetailerProxyUrl("albertsons");

  const baseUrl = "https://www.albertsons.com/abs/pub/xapi/pgmsearch/v1/search/products";
  const queries = shuffleKeepCanaryFirst(getQueriesForScan(SEARCH_QUERIES));

  for (let i = 0; i < queries.length; i++) {
    const query = queries[i];
    if (failures > 3) break;
    if (i > 0) await sleep(1500 + Math.random() * 2000);
    const apiUrl = `${baseUrl}?request-id=0&url=https://www.albertsons.com&pageurl=search&search-type=keyword&q=${encodeURIComponent(query)}&rows=50&start=0&storeid=${store.storeId}`;
    try {
      const headers = {
        ...FETCH_HEADERS,
        "Ocp-Apim-Subscription-Key": ALBERTSONS_KEY,
        Accept: "application/json",
        Cookie: cachedCookies,
      };
      if (i > 0) {
        headers["Sec-Fetch-Site"] = "same-origin";
        headers["Referer"] = "https://www.albertsons.com/";
      }
      const res = await scraperFetchRetry(apiUrl, {
        headers,
        timeout: 15000,
        proxyUrl: abProxy || undefined,
      });
      if (!res.ok) { failures++; continue; }
      const data = await res.json();
      if (isFetchBlocked(JSON.stringify(data).slice(0, 10000))) { failures++; continue; }
      if (data?.primaryProducts?.response?.docs) {
        found.push(...matchAlbertsonsProducts(data.primaryProducts.response.docs));
        validPages++;
        trackHealth("albertsons", "ok");

        if (data.primaryProducts.response.docs.length === 50) {
          try {
            const page2Url = `${baseUrl}?request-id=0&url=https://www.albertsons.com&pageurl=search&search-type=keyword&q=${encodeURIComponent(query)}&rows=50&start=50&storeid=${store.storeId}`;
            const res2 = await scraperFetchRetry(page2Url, { headers, timeout: 15000, proxyUrl: abProxy || undefined });
            if (res2.ok) {
              const data2 = await res2.json();
              if (data2?.primaryProducts?.response?.docs) {
                found.push(...matchAlbertsonsProducts(data2.primaryProducts.response.docs));
              }
            }
          } catch { /* page 2 failure is non-critical */ }
        }
      } else {
        failures++;
      }
    } catch {
      failures++;
    }
  }

  const minValid = Math.max(1, Math.ceil(queries.length / 4));
  if (validPages < minValid) return null;
  console.log(`[albertsons:${store.storeId}] Used fast fetch mode (cached browser cookies)`);
  return dedupFound(found);
}
/* v8 ignore stop */

// Browser-based Albertsons scraper. Incapsula WAF blocks cold fetch requests,
// so we use a real browser to earn Incapsula cookies, then call the API from
// within the browser context. Same API as Safeway, different domain + WAF.
async function scrapeAlbertsonsViaBrowser(page, store) {
  const found = [];
  const t0 = Date.now();
  const elapsed = () => `${((Date.now() - t0) / 1000).toFixed(1)}s`;

  await page.goto("https://www.albertsons.com/", { waitUntil: "networkidle", timeout: 20000 }).catch(() => {});
  console.log(`[albertsons:${store.storeId}] Homepage loaded (${elapsed()})`);
  await sleep(4000 + Math.random() * 3000);
  await solveHumanChallenge(page);
  await humanizePage(page, { pace: "slow" });
  await navigateCategory(page, "albertsons");
  console.log(`[albertsons:${store.storeId}] Pre-warm done (${elapsed()}), starting queries...`);

  const baseUrl = "https://www.albertsons.com/abs/pub/xapi/pgmsearch/v1/search/products";
  let consecutiveBlocks = 0;

  for (const query of shuffleKeepCanaryFirst(getQueriesForScan(SEARCH_QUERIES))) {
    if (consecutiveBlocks >= 4) { trackHealth("albertsons", "blocked"); continue; }

    try {
      const apiUrl = `${baseUrl}?request-id=0&url=https://www.albertsons.com&pageurl=search&search-type=keyword&q=${encodeURIComponent(query)}&rows=50&start=0&storeid=${store.storeId}`;
      /* v8 ignore start -- browser-only API call */
      const data = await page.evaluate(async ({ url, apiKey }) => {
        try {
          const res = await fetch(url, {
            headers: { "Ocp-Apim-Subscription-Key": apiKey, Accept: "application/json" },
          });
          if (!res.ok) return null;
          return await res.json();
        } catch { return null; }
      }, { url: apiUrl, apiKey: ALBERTSONS_KEY });
      /* v8 ignore stop */

      if (data?.primaryProducts?.response?.docs) {
        const products = data.primaryProducts.response.docs;
        found.push(...matchAlbertsonsProducts(products));
        trackHealth("albertsons", "ok");
        consecutiveBlocks = 0;
        console.log(`[albertsons:${store.storeId}] API query "${query}": ${products.length} products (${elapsed()})`);

        if (products.length === 50) {
          try {
            const page2Url = `${baseUrl}?request-id=0&url=https://www.albertsons.com&pageurl=search&search-type=keyword&q=${encodeURIComponent(query)}&rows=50&start=50&storeid=${store.storeId}`;
            /* v8 ignore start -- browser-only */
            const data2 = await page.evaluate(async ({ url, apiKey }) => {
              try {
                const res = await fetch(url, { headers: { "Ocp-Apim-Subscription-Key": apiKey, Accept: "application/json" } });
                return res.ok ? await res.json() : null;
              } catch { return null; }
            }, { url: page2Url, apiKey: ALBERTSONS_KEY });
            /* v8 ignore stop */
            if (data2?.primaryProducts?.response?.docs) {
              found.push(...matchAlbertsonsProducts(data2.primaryProducts.response.docs));
            }
          } catch { /* page 2 failure is non-critical */ }
        }
      } else {
        /* v8 ignore start -- browser-only DOM fallback */
        const searchUrl = `https://www.albertsons.com/shop/search-results.html?q=${encodeURIComponent(query)}`;
        await page.goto(searchUrl, { waitUntil: "domcontentloaded", timeout: 15000 });
        await page.waitForSelector("[class*='product'], [data-testid*='product']", { timeout: 8000 }).catch(() => {});

        if (await isBlockedPage(page)) {
          const solved = await solveHumanChallenge(page);
          if (!solved || (await isBlockedPage(page))) {
            console.warn(`[albertsons:${store.storeId}] Blocked for query "${query}"`);
            trackHealth("albertsons", "blocked", { reason: "waf" });
            consecutiveBlocks++;
            continue;
          }
        }

        const products = await page.$$eval(
          "[class*='product-card'], [class*='productCard'], [data-testid*='product'], .product-item",
          (cards) => cards.map((el) => ({
            title: (el.querySelector("h2, h3, [class*='title'], [class*='name']") || {}).textContent?.trim() || "",
            price: (el.querySelector("[class*='price']") || {}).textContent?.trim() || "",
            url: (el.querySelector("a[href*='/shop/product/']") || {}).href || "",
            outOfStock: (el.textContent || "").toLowerCase().includes("out of stock") ||
                        (el.textContent || "").toLowerCase().includes("unavailable"),
          }))
        );

        for (const p of products) {
          if (p.outOfStock) continue;
          for (const bottle of TARGET_BOTTLES) {
            if (matchesBottle(p.title, bottle, "albertsons")) {
              found.push({
                name: bottle.name,
                url: p.url ? (p.url.startsWith("http") ? p.url : `https://www.albertsons.com${p.url}`) : "",
                price: p.price || "",
                sku: "",
                size: parseSize(p.title),
                fulfillment: "",
              });
            }
          }
        }
        trackHealth("albertsons", products.length > 0 ? "ok" : "fail");
        consecutiveBlocks = 0;
        /* v8 ignore stop */
      }
    } catch (err) {
      console.error(`[albertsons:${store.storeId}] Error: ${err.message}`);
      trackHealth("albertsons", "fail");
      consecutiveBlocks++;
    }
    await sleep(2500 + Math.random() * 3500);
  }
  return dedupFound(found);
}

// Wrapper: tries fetch-first with cached cookies, then launches browser, handles timeout/retry.
// Uses acquireRetailerLock to serialize concurrent stores through the shared browser context —
// without this, one store's retry logic closes the context while others are mid-scrape,
// causing "Target page, context or browser has been closed" errors across all stores.
async function scrapeAlbertsonsStore(store) {
  const fetchResult = await scrapeAlbertsonsViaFetch(store).catch((err) => {
    console.warn(`[albertsons:${store.storeId}] Fetch path failed: ${err.message}`);
    return null;
  });
  if (fetchResult !== null) return fetchResult;

  delete scraperHealth["albertsons"];

  const skipPreWarm = !!retailerBrowserCache["albertsons"];
  console.log(`[albertsons:${store.storeId}] Queuing browser${skipPreWarm ? " (warm)" : ""}`);
  const releaseLock = await acquireRetailerLock("albertsons");
  try {
    for (let attempt = 0; attempt < 2; attempt++) {
      /* v8 ignore start -- browser retry loop internals */
      if (attempt > 0) {
        console.log("[albertsons] Retrying with fresh browser after blocks");
        delete scraperHealth["albertsons"];
        if (retailerBrowserCache["albertsons"]) {
          await retailerBrowserCache["albertsons"].context.close().catch(() => {});
          delete retailerBrowserCache["albertsons"];
        }
        await sleep(3000 + Math.random() * 2000);
      }
      /* v8 ignore stop */
      console.log("[albertsons] Using clean browser");
      let page;
      try {
        ({ page } = await launchRetailerBrowser("albertsons", { clean: true }));
      } catch (err) {
        console.error(`[albertsons] Browser launch failed: ${err.message}`);
        trackHealth("albertsons", "fail");
        return [];
      }
      try {
        const scraperPromise = scrapeAlbertsonsViaBrowser(page, store);
        scraperPromise.catch(() => {});
        // 300s timeout — modern WAFs (Akamai/PerimeterX/Incapsula) keep the page chatty
        // so pre-warm alone takes 130-180s. Need headroom for queries after pre-warm.
        // All browser scrapers use 300s for the same reason; bumped from 180s in 2026-04
        // after metrics showed canary detection at 0% for Costco/Safeway/Sam's Club due
        // to pre-warm consuming the entire budget before any queries could run.
        const result = await withTimeout(scraperPromise, 300000, null);
        /* v8 ignore start -- browser timeout/retry paths */
        if (result === null) {
          console.warn("[albertsons] Browser scraper timed out (300s)");
          trackHealth("albertsons", "fail");
          if (attempt === 0) continue;
          return [];
        }
        await cacheRetailerCookies("albertsons");
        const abHealth = scraperHealth["albertsons"];
        if (abHealth && abHealth.blocked > 0 && abHealth.blocked >= abHealth.queries / 2 && attempt === 0) {
          console.warn(`[albertsons] ${abHealth.blocked}/${abHealth.queries} queries blocked — retrying`);
          continue;
        }
        /* v8 ignore stop */
        return result;
      } finally {
        await page.close().catch(() => {});
      }
    }
    /* v8 ignore next -- unreachable after 2 attempts */
    return [];
  } finally {
    releaseLock();
  }
}

// ─── CityHive Multi-Tenant Platform Scrapers ────────────────────────────────
// CityHive (cityhive.net) is a SaaS e-commerce platform powering many independent
// liquor stores. The platform exposes an unauthenticated REST API at
// /api/v1/merchants/{merchantId}/... using a public site-scoped api_key. No WAF
// detected on any store — clean fetch path is sufficient for all of them.
//
// Each store is a separate retailer in our system (different inventory + branding)
// but shares this single scraper code, parameterized by CITYHIVE_RETAILERS config.
//
// API request shape:
//   POST https://{domain}/api/v1/merchants/{merchantId}/browse_categories/render.json
//   Body: { api_key, sdk_guid, ch_request_guid, client_origin, local: true,
//           category_params: { children_type: "products", input_value: <query>, ... } }
//
// The same `api_key` works across all CityHive sites — it's the platform's
// public/site-scoped key, surfaced in every browser bundle. `merchantId` and
// `client_origin` are per-store. Discoverable from the SPA via DevTools.

// Shared platform-wide API key. Same value across every CityHive merchant.
const CITYHIVE_API_KEY = "7508df878a8c7566a880e4d3f7fa7972";

// Per-retailer configuration. Add a new CityHive store by appending an entry here
// + adding to RETAILERS + FALLBACK_STORES. No new scraper code required.
//
// `categories` is the critical field — each entry is { id, title } for a CityHive
// "product_filtered_group" that contains bourbon/whiskey products. Discovered by
// probing each store's homepage in a browser and noting which categories return
// bourbon products. CityHive has no search API — the SPA filters categories
// client-side. Empty array = retailer is plumbed but won't make API calls.
const CITYHIVE_RETAILERS = {
  extramile: {
    name: "ExtraMile",
    domain: "extramileliquors.com",
    merchantId: "66c8c223d933721cd7586082",
    clientOrigin: "app://sites.chandlera6ec3658",
    categories: [
      { id: "6715af5a0e696f319dc09296", title: "On Sale Now" },
      { id: "66c8c2257085fb54420fdfba", title: "Featured Spirits" },
      { id: "6712ea8c25e7bf28e3933cbc", title: "Bundles and Specials" },
    ],
  },
  liquorexpress: {
    name: "Liquor Express Tempe",
    domain: "liquorexpresstempe.store",
    merchantId: "5f88c1ab8f687229c6c2c8a4",
    clientOrigin: "app://sites.liquorex2edadd46",
    // Disabled — Liquor Express's CityHive instance uses a DIFFERENT layout than
    // ExtraMile / Chandler Liquors. Their homepage does NOT issue any
    // `browse_categories/render.json` calls (no auto-rendered category lists), and
    // their `search_filters.json` endpoint returns 403 to clean got-scraping fetches
    // (CloudFront-gated; works only inside a real browser session). To enable:
    //   (a) Refactor scraper to support browser-mediated CityHive scraping, OR
    //   (b) Capture the actual product-listing endpoint by clicking around the SPA
    //       in DevTools (e.g. interact with filter dropdowns, navigate to /shop/?...).
    // Effort: ~2-4 hours of focused probing. Payoff is uncertain — small store,
    // small allocated catalog. Park here unless it becomes a priority.
    categories: [],
  },
  chandlerliquors: {
    name: "Chandler Liquors",
    domain: "chandlerliquorsaz.com",
    merchantId: "5e8e0a0778e8f16f128f7e5a",
    clientOrigin: "app://sites.chandlerbfdd6edb",
    categories: [
      { id: "6590421a8a38b62ba72a8ff4", title: "Stock The Bar" },
      { id: "65904bac7d7e512bac634da5", title: "Samplers" },
    ],
  },
};

// Match CityHive product objects against TARGET_BOTTLES. The product shape returned
// by the browse API includes `name`, `basic_category`, and a `merchants[].product_options[]`
// array. Stock signal: `merchants[].offer_types` contains "delivery"/"pick_up" when
// buyable; empty array means OOS. Backward-compat: also honor `is_buyable: true`.
function matchCityHiveProducts(products, retailerKey, domain) {
  const found = [];
  for (const p of products) {
    if (!p?.name) continue;
    const merchant = p.merchants?.[0];
    if (!merchant) continue;
    const offerTypes = merchant.offer_types || [];
    const inStock = offerTypes.length > 0 || merchant.is_buyable === true;
    if (!inStock) continue;
    const opt = merchant.product_options?.[0];
    const productUrl = opt?.product_url || `https://${domain}/shop/product/${p.id}`;
    const priceRaw = opt?.price ?? merchant.price ?? p.price;
    const price = typeof priceRaw === "number" ? `$${priceRaw.toFixed(2)}`
                : typeof priceRaw === "string" ? priceRaw
                : "";
    const sizeQty = p.size?.quantity || merchant.size?.quantity;
    const sizeMeasure = p.size?.measure || merchant.size?.measure;
    const size = sizeQty && sizeMeasure ? `${sizeQty}${sizeMeasure}` : parseSize(p.name);
    for (const bottle of TARGET_BOTTLES) {
      if (matchesBottle(p.name, bottle, retailerKey)) {
        found.push({
          name: bottle.name,
          url: productUrl,
          price,
          sku: p.id || "",
          size,
          fulfillment: offerTypes.join(", ") || "available",
        });
      }
    }
  }
  return found;
}

// Fetch products from a specific CityHive category. The SPA's "search" feature is
// actually client-side filtering over multiple category renders — there is no
// search endpoint. So we fetch each known bourbon-bearing category for a retailer
// and run the matcher against ALL products returned, letting matchesBottle do
// the bottle-name filtering on our side.
async function fetchCityHiveCategory(retailerKey, categoryId, title) {
  const cfg = CITYHIVE_RETAILERS[retailerKey];
  if (!cfg) return [];
  const url = `https://${cfg.domain}/api/v1/merchants/${cfg.merchantId}/browse_categories/render.json`;
  const body = {
    api_key: CITYHIVE_API_KEY,
    sdk_guid: crypto.randomUUID(),
    ch_request_guid: crypto.randomUUID(),
    client_origin: cfg.clientOrigin,
    local: true,
    category_params: {
      children_type: "products",
      link_type: "horizontal_list",
      minimum_nodes_for_a_render: "0",
      show_container_title: "TRUE",
      title: title || "",
      context_type: "inventory",
      product_filtered_group_id: categoryId,
      show_container_action: "false",
      disable_personalized_product_ranking: "false",
      action: {},
    },
    merchant_ids: [],
  };
  try {
    const res = await scraperFetchRetry(url, {
      method: "POST",
      headers: { ...FETCH_HEADERS, "Content-Type": "application/json", Accept: "application/json" },
      body: JSON.stringify(body),
      timeout: 15000,
      proxyUrl: getRetailerProxyUrl(retailerKey) || undefined,
    });
    if (!res.ok) return [];
    const data = await res.json();
    if (data.result !== 0) return []; // CityHive convention: result=0 means success
    const nodes = data.data?.nodes || [];
    return nodes
      .filter((n) => n.type === "product" && n.params?.product)
      .map((n) => n.params.product);
  } catch {
    return [];
  }
}

// Generic CityHive store scraper. Same code for all 3 stores; behavior parameterized
// by the per-retailer `categories` array in CITYHIVE_RETAILERS. CityHive supports
// unauthenticated fetches with the site's public api_key, so no browser session is
// needed. Returns [] if the retailer has no configured categories (effectively skips).
function makeCityHiveScraper(retailerKey) {
  return async function scrapeCityHiveStore(_store) {
    const cfg = CITYHIVE_RETAILERS[retailerKey];
    if (!cfg || !cfg.categories || cfg.categories.length === 0) {
      // No categories configured — silently skip without consuming health budget.
      return [];
    }
    const found = [];
    let validCats = 0;
    let failures = 0;
    for (const cat of cfg.categories) {
      if (failures > 3) break;
      try {
        const products = await fetchCityHiveCategory(retailerKey, cat.id, cat.title);
        if (products.length > 0) {
          const matches = matchCityHiveProducts(products, retailerKey, cfg.domain);
          found.push(...matches);
        }
        // Empty category is normal — most categories don't contain target bottles
        trackHealth(retailerKey, "ok");
        validCats++;
      } catch (err) {
        console.warn(`[${retailerKey}] Category "${cat.title}" failed: ${err.message}`);
        trackHealth(retailerKey, "fail", { reason: "network" });
        failures++;
      }
      // Polite: don't burst the small CityHive backend — 1-2s between requests
      await sleep(1000 + Math.random() * 1000);
    }
    if (validCats === 0 && failures > 0) {
      console.warn(`[${retailerKey}] All categories failed — check CityHive API contract`);
      return [];
    }
    return dedupFound(found);
  };
}

// Per-retailer scraper functions (curried from the generic factory).
// Each is registered separately in RETAILERS so per-retailer health tracking,
// state.json organization, and Discord alerts work cleanly.
const scrapeExtraMileStore = makeCityHiveScraper("extramile");
const scrapeLiquorExpressStore = makeCityHiveScraper("liquorexpress");
const scrapeChandlerLiquorsStore = makeCityHiveScraper("chandlerliquors");

// ─── Retailer Registry ───────────────────────────────────────────────────────
// scrapeOnce: results are identical across stores (no store-specific URL/cookie).
//   Scrape once and broadcast results to all stores.
// needsPage: scraper accepts a shared Playwright page as second arg (browser-based).
//   If false, scraper is API/fetch-based and doesn't need a browser page.

const RETAILERS = [
  { key: "costco",    name: "Costco",     scrapeOnce: true,  needsPage: false, scraper: scrapeCostcoStore },
  { key: "totalwine", name: "Total Wine", scrapeOnce: false, needsPage: false, scraper: scrapeTotalWineStore },
  { key: "walmart",   name: "Walmart",    scrapeOnce: false, needsPage: false, scraper: scrapeWalmartStore },
  { key: "kroger",    name: "Kroger",     scrapeOnce: false, needsPage: false, scraper: scrapeKrogerStore },
  { key: "safeway",     name: "Safeway",     scrapeOnce: false, needsPage: false, scraper: scrapeSafewayStore },
  { key: "albertsons",  name: "Albertsons",  scrapeOnce: false, needsPage: false, scraper: scrapeAlbertsonsStore },
  { key: "walgreens",   name: "Walgreens",   scrapeOnce: true,  needsPage: false, scraper: scrapeWalgreensStore },
  { key: "samsclub",        name: "Sam's Club",          scrapeOnce: true,  needsPage: false, scraper: scrapeSamsClubStore },
  // CityHive retailers — iterate per-store bourbon-bearing category IDs (no search
  // API; SPA filters categories client-side). Discovered category IDs configured
  // in CITYHIVE_RETAILERS. Liquor Express stays disabled until we capture its
  // category IDs (homepage layout differs — needs another browser probe).
  { key: "extramile",       name: "ExtraMile",            scrapeOnce: false, needsPage: false, scraper: scrapeExtraMileStore },
  { key: "liquorexpress",   name: "Liquor Express Tempe", scrapeOnce: false, needsPage: false, scraper: scrapeLiquorExpressStore,   disabled: true },
  { key: "chandlerliquors", name: "Chandler Liquors",     scrapeOnce: false, needsPage: false, scraper: scrapeChandlerLiquorsStore },
  // BevMo omitted — no AZ locations
];

// ─── Orchestrator ────────────────────────────────────────────────────────────

let polling = false;
let storeCache = null;
let peakSlotsCache = null; // Populated by poll(), consumed by getNextPollDelayMs() for dynamic boost

async function poll() {
  if (polling) {
    console.log("[poll] Skipping — previous poll still running");
    return;
  }
  polling = true;
  try {
  const scanStart = Date.now();
  console.log(`[poll] Starting scan at ${new Date().toISOString()}`);

  const state = await loadState();
  pruneState(state, storeCache.retailers);
  // Live-reload watchlist.json so edits take effect without daemon restart
  const wlCount = await loadWatchList();
  if (wlCount > 0) console.log(`[watchlist] Loaded ${wlCount} entries from watchlist.json`);
  await processWatchList(state);
  // Reddit intel runs on its own independent loop (see main()), not per-scan
  let storesScanned = 0;
  let totalNewFinds = 0;
  let totalConfirmed = 0;  // newFinds with confidence !== "lead" (Kroger green or non-Kroger default)
  let totalLeads = 0;      // newFinds with confidence === "lead" (Kroger yellow)
  let totalStillInStock = 0;
  let totalGoneOOS = 0;
  // Alert deduplication: tracks bottle names that already fired an URGENT (@here) alert
  // earlier in this scan. Subsequent same-bottle finds at other stores downgrade to
  // quiet alerts to prevent Discord spam (e.g. Pappy 15 at 4 Marketplace stores =
  // 1 @here ping + 3 quiet "also at" alerts instead of 4 @here pings).
  const urgentlyAlertedThisScan = new Set();
  let nothingCount = 0;
  const retailersSeen = new Set();
  const scannedStores = [];
  const scanFinds = {}; // { retailerKey: Set of bottle names found }

  // Reset per-poll state
  refreshProxySession();
  krogerToken = null;
  browserStateCache = null;
  scraperHealth = {};
  primaryProxyExhausted = false;
  backupProxyExhausted = false;
  for (const k of Object.keys(retailerBrowserBlocked)) delete retailerBrowserBlocked[k];
  const canaryResults = {};
  let discordFailures = 0;
  scanCounter++;
  loadKnownProducts(state);

  // Helper to record results for a store and send alerts
  async function recordResult(retailer, store, inStock) {
    // Filter out miniature bottles (50ml) that slipped past EXCLUDE_TERMS
    inStock = filterMiniatures(inStock);
    // Separate canary from allocated bottles — canary never triggers alerts or state
    // Use per-retailer canary check so retailers that don't reliably stock the default
    // canary (Buffalo Trace) don't get flagged as broken. Falls back to default for
    // retailers without explicit configuration.
    const canaryFound = inStock.some((b) => isCanaryFor(retailer.key, b.name));
    if (canaryFound) {
      canaryResults[retailer.key] = true;
      console.log(`[${retailer.key}:${store.storeId}] 🐤 Canary found (${inStock.filter((b) => isCanaryFor(retailer.key, b.name)).map((b) => b.name).join(", ")})`);
    }
    const realInStock = inStock.filter((b) => !isCanaryFor(retailer.key, b.name));

    // Cross-source confirmation: if a Reddit post in the last 4 hours mentioned the
    // bottle, mark this find as "double-confirmed" (highest signal — both human
    // reporting and scraper agree). buildStoreEmbeds renders 🔥 + distinct title.
    const reddit = state._redditMentions || {};
    const CROSS_SOURCE_WINDOW_MS = 4 * 60 * 60 * 1000;
    for (const b of realInStock) {
      const ts = reddit[b.name];
      if (ts && Date.now() - new Date(ts).getTime() < CROSS_SOURCE_WINDOW_MS) {
        b.crossSourceConfirmed = true;
      }
    }

    const previousStore = state[retailer.key]?.[store.storeId];
    const changes = computeChanges(previousStore, realInStock);
    updateStoreState(state, retailer.key, store.storeId, realInStock);
    storesScanned++;
    retailersSeen.add(retailer.key);
    scannedStores.push({ retailerName: retailer.name, storeName: store.name, storeId: store.storeId });

    totalNewFinds += changes.newFinds.length;
    totalStillInStock += changes.stillInStock.length;
    totalGoneOOS += changes.goneOOS.length;
    // Tier breakdown for summary (Kroger uses confidence; others default to "confirmed")
    totalConfirmed += changes.newFinds.filter((b) => b.confidence !== "lead").length;
    totalLeads += changes.newFinds.filter((b) => b.confidence === "lead").length;

    // Track all found bottles per retailer for metrics
    if (realInStock.length > 0) {
      if (!scanFinds[retailer.key]) scanFinds[retailer.key] = new Set();
      for (const b of realInStock) scanFinds[retailer.key].add(b.name);
    }

    const embeds = buildStoreEmbeds(retailer.key, retailer.name, store, changes);

    // Split embeds by tier — `_urgent: true` (confirmed finds) get @here ping,
    // `_urgent: false` (leads) and tier-less embeds (OOS / re-alerts) go via quiet alert.
    const urgentEmbeds = embeds.filter((e) => e._urgent === true).map((e) => { const { _urgent, ...rest } = e; return rest; });
    const quietEmbeds = embeds.filter((e) => e._urgent !== true).map((e) => { const { _urgent, ...rest } = e; return rest; });

    if (urgentEmbeds.length > 0) {
      const confirmedNames = changes.newFinds.filter((b) => b.confidence !== "lead").map((b) => b.name);
      // Cross-store dedup: if EVERY bottle in this urgent batch was already alerted on
      // earlier in this scan, downgrade to quiet (already pinged for this bottle).
      // Otherwise mark all bottles as alerted and proceed with @here ping.
      const allAlreadyAlerted = confirmedNames.every((n) => urgentlyAlertedThisScan.has(n));
      if (allAlreadyAlerted) {
        console.log(`[${retailer.key}:${store.storeId}] 🟢 New (deduped — already pinged this scan): ${confirmedNames.join(", ")}`);
        // Append "(also at: <store>)" annotation to embed title for context
        const annotated = urgentEmbeds.map((e) => ({ ...e, title: `🎯 ALSO AT — ${e.title.replace(/^🚨 NEW FIND — /, "")}` }));
        try { await sendDiscordAlert(annotated); }
        catch (err) { discordFailures++; console.error(`[discord] Dedup alert failed: ${err.message}`); }
      } else {
        console.log(`[${retailer.key}:${store.storeId}] 🟢 New: ${confirmedNames.join(", ")}`);
        for (const n of confirmedNames) urgentlyAlertedThisScan.add(n);
        try { await sendUrgentAlert(urgentEmbeds); }
        catch (err) { discordFailures++; console.error(`[discord] Urgent alert failed for ${retailer.name} ${store.name}: ${err.message}`); }
      }
    }
    if (quietEmbeds.length > 0) {
      const leadNames = changes.newFinds.filter((b) => b.confidence === "lead").map((b) => b.name);
      const tag = leadNames.length > 0 ? `🟡 Lead: ${leadNames.join(", ")}`
                : changes.goneOOS.length > 0 ? "🔴 OOS"
                : "🔵 Re-alert";
      console.log(`[${retailer.key}:${store.storeId}] ${tag}`);
      try { await sendDiscordAlert(quietEmbeds); }
      catch (err) { discordFailures++; console.error(`[discord] Quiet alert failed for ${retailer.name} ${store.name}: ${err.message}`); }
    }
    if (urgentEmbeds.length === 0 && quietEmbeds.length === 0) {
      nothingCount++;
      console.log(`[${retailer.key}:${store.storeId}] Nothing new`);
    }
  }

  const tasks = [];
  for (const [ri, retailer] of RETAILERS.entries()) {
    // Permanent skip: retailer is shipped but disabled (e.g., scaffolding for a
    // future scraper that's not yet working). No health tracking, no error
    // budget impact — just silent skip.
    if (retailer.disabled) continue;

    const stores = prioritizeStores(retailer.key, storeCache.retailers[retailer.key] || []);
    if (stores.length === 0) continue;

    // Adaptive skipping: back off from retailers with repeated failures
    if (shouldSkipRetailer(retailer.key)) continue;

    // Stagger retailer start times so all 7 don't hit the internet simultaneously.
    // First retailer starts immediately; subsequent ones wait 10-30s each.
    const staggerMs = ri > 0 ? 10000 + Math.floor(Math.random() * 20000) : 0;

    if (retailer.scrapeOnce) {
      // Scrape once, broadcast results to all stores (e.g., Costco has no per-store filter)
      tasks.push(async () => {
        if (staggerMs) await sleep(staggerMs);
        try {
          console.log(`[poll] Checking ${retailer.name} (once for ${stores.length} stores)...`);
          // Wrapper handles its own browser page if needed (fetch-first with browser fallback)
          const inStock = await retailer.scraper();
          // Per-store try/catch: one Discord failure shouldn't skip remaining stores
          for (const store of stores) {
            try {
              await recordResult(retailer, store, inStock);
            } catch (storeErr) {
              console.error(`[poll] ${retailer.name} (${store.name}) recordResult failed: ${storeErr.message}`);
            }
          }
        } catch (err) {
          console.error(`[poll] ${retailer.name} crashed: ${err.message}`);
        }
      });
    } /* v8 ignore start -- no retailers currently use needsPage */ else if (retailer.needsPage) {
      for (const [si, store] of stores.entries()) {
        tasks.push(async () => {
          if (si === 0 && staggerMs) await sleep(staggerMs);
          let page;
          try {
            console.log(`[poll] Checking ${retailer.name} — ${store.name}...`);
            page = await newPage();
            const inStock = await retailer.scraper(store, page);
            await recordResult(retailer, store, inStock);
          } catch (err) {
            console.error(`[poll] ${retailer.name} (${store.name}) crashed: ${err.message}`);
          } finally {
            if (page) await page.context().close().catch(() => {});
          }
        });
      }
    } /* v8 ignore stop */ else {
      // API/fetch-based scrapers (with possible browser fallback). Browser concurrency
      // is handled by acquireRetailerLock, so only light staggering needed for fetch.
      for (const [si, store] of stores.entries()) {
        tasks.push(async () => {
          if (si === 0 && staggerMs) await sleep(staggerMs);
          else if (si > 0) await sleep(si * (2000 + Math.floor(Math.random() * 3000)));
          try {
            console.log(`[poll] Checking ${retailer.name} — ${store.name}...`);
            const inStock = await retailer.scraper(store);
            await recordResult(retailer, store, inStock);
          } catch (err) {
            console.error(`[poll] ${retailer.name} (${store.name}) crashed: ${err.message}`);
          }
        });
      }
    }
  }

  // Run all stores concurrently (limit 8 — 6 retailers, fetch-first scrapers are lightweight)
  await runWithConcurrency(tasks, 8);

  // Per-poll time budget: if the main scan took too long (>12 min of a 15-min budget),
  // skip canary retries to avoid cascading delays into the next poll.
  const POLL_BUDGET_MS = 25 * 60 * 1000; // 25 minutes
  const elapsed = Date.now() - scanStart;
  const budgetRemaining = POLL_BUDGET_MS - elapsed;
  if (budgetRemaining < 3 * 60 * 1000) {
    console.warn(`[poll] ⚠️ Budget low (${Math.round(elapsed / 1000)}s elapsed, ${Math.round(budgetRemaining / 1000)}s remaining) — skipping canary retries`);
  }

  if (discordFailures > 0) {
    console.warn(`[poll] ⚠️ ${discordFailures} Discord alert(s) failed this scan — check webhook status`);
  }

  // Canary-driven retry: if a retailer ran queries but missed canary, the scraper is
  // likely broken (not "nothing in stock" — Buffalo Trace is always available). Retry once
  // with a fresh browser context and rotated proxy IP.
  const canaryRetries = new Set();
  const retryTasks = [];
  if (budgetRemaining >= 3 * 60 * 1000) {
  for (const retailer of RETAILERS) {
    const h = scraperHealth[retailer.key];
    if (!h || h.queries === 0) continue;          // skipped — don't retry
    if (canaryResults[retailer.key]) continue;      // canary found — working fine
    if (h.succeeded === 0) continue;                // total failure — retry won't help
    console.log(`[poll] 🐤 ${retailer.name} missed canary — retrying once`);
    canaryRetries.add(retailer.key);
    // Clear browser state for fresh approach
    if (retailerBrowserCache[retailer.key]) {
      await retailerBrowserCache[retailer.key].context.close().catch(() => {});
      delete retailerBrowserCache[retailer.key];
    }
    rotateRetailerProxy(retailer.key);
    const stores = storeCache.retailers[retailer.key] || [];
    if (retailer.scrapeOnce) {
      retryTasks.push(async () => {
        try {
          const inStock = await retailer.scraper();
          for (const store of stores) {
            try { await recordResult(retailer, store, inStock); }
            catch (e) { console.error(`[retry] ${retailer.name} recordResult: ${e.message}`); }
          }
        } catch (e) { console.error(`[retry] ${retailer.name}: ${e.message}`); }
      });
    } else if (stores[0]) {
      retryTasks.push(async () => {
        try {
          const inStock = await retailer.scraper(stores[0]);
          await recordResult(retailer, stores[0], inStock);
        } catch (e) { console.error(`[retry] ${retailer.name}: ${e.message}`); }
      });
    }
  }
  if (retryTasks.length > 0) {
    console.log(`[poll] Canary retry: ${retryTasks.length} retailer(s)`);
    await runWithConcurrency(retryTasks, 4);
  }
  } // end budget check for canary retries

  // Record retailer outcomes for adaptive skipping (based on health metrics)
  for (const retailer of RETAILERS) {
    const h = scraperHealth[retailer.key];
    if (!h || h.queries === 0) continue;
    const successRate = h.succeeded / h.queries;
    recordRetailerOutcome(retailer.key, successRate >= 0.25);
  }

  await closeBrowser().catch((err) => console.error(`[poll] closeBrowser failed: ${err.message}`));
  await closeRetailerBrowsers().catch((err) => console.error(`[poll] closeRetailerBrowsers failed: ${err.message}`));
  await saveState(state).catch((err) => console.error(`[poll] saveState failed: ${err.message}`));

  // Build scan metrics entry
  const durationSec = Math.round((Date.now() - scanStart) / 1000);
  const metricsRetailers = {};
  for (const retailer of RETAILERS) {
    const h = scraperHealth[retailer.key];
    if (!h) continue;
    metricsRetailers[retailer.key] = {
      queries: h.queries, ok: h.succeeded, blocked: h.blocked, failed: h.failed,
      canary: !!canaryResults[retailer.key],
      found: scanFinds[retailer.key] ? [...scanFinds[retailer.key]] : [],
      path: h.path || { fetch: 0, browser: 0 },
      queryStats: h.queryStats || {},
    };
  }
  const metricsEntry = {
    ts: new Date().toISOString(), durationSec, storesScanned,
    newFinds: totalNewFinds, stillInStock: totalStillInStock, goneOOS: totalGoneOOS,
    retailers: metricsRetailers,
  };

  // Persist metrics, then load 24h trend for summary embed
  await appendMetrics(metricsEntry).catch((err) => console.error(`[poll] Metrics append failed: ${err.message}`));
  const recentMetrics = await loadRecentMetrics(24).catch(() => []);
  const trend = computeMetricsTrend(recentMetrics);
  const longMetrics = await loadRecentMetrics(24 * 14).catch(() => []);
  const peakHours = computePeakHours(longMetrics);
  // Update peak slots cache for dynamic boost scheduling
  if (peakHours) peakSlotsCache = peakHours;

  // Quiet summary at end of every poll
  const summary = buildSummaryEmbed({ storesScanned, retailersScanned: retailersSeen.size, totalNewFinds, totalConfirmed, totalLeads, totalStillInStock, totalGoneOOS, nothingCount, durationSec, scannedStores, health: scraperHealth, canaryResults, trend, peakHours });
  await sendDiscordAlert([summary]).catch((err) => console.error(`[poll] Summary send failed: ${err.message}`));

  // Health-degradation alert: if a retailer's canary has been 0% for 4+ consecutive
  // scans AND its failure mode is dominated by a known reason, send a quiet ping with
  // root-cause hint. Catches WAF rotations / API contract breaks within ~2 hours instead
  // of leaving them invisible until a full audit.
  await maybeSendHealthDegradationAlert(recentMetrics).catch((err) =>
    console.error(`[poll] Health alert failed: ${err.message}`)
  );

  console.log(`[poll] Scan complete — ${storesScanned} stores, ${totalNewFinds} new, ${totalStillInStock} still, ${totalGoneOOS} OOS, ${durationSec}s\n`);
  } finally {
    polling = false;
  }
}

// ─── Exports (for testing) ────────────────────────────────────────────────────

export {
  SEARCH_QUERIES, TARGET_BOTTLES, CANARY_NAMES, CANARY_BY_RETAILER, isCanaryFor, RETAILERS, FETCH_HEADERS,
  normalizeText, parseSize, parsePrice, matchesBottle, EXCLUDE_TERMS, MIN_BOTTLE_PRICE, MAX_BOTTLE_PRICE, filterMiniatures, dedupFound, getGreasedBrand, shuffle, withTimeout, runWithConcurrency, matchWalmartNextData,
  COLORS, SKU_LABELS, formatStoreInfo, parseCity, parseState, timeAgo,
  formatBottleLine, buildOOSList, truncateDescription, truncateTitle, DISCORD_DESC_LIMIT, DISCORD_TITLE_LIMIT, buildStoreEmbeds, buildSummaryEmbed, ULTRA_RARE_BOTTLES, IGNORED_BOTTLES, BOTTLE_INTEREST_TIERS, bottleInterestTier, rerunCadenceFor,
  loadState, saveState, computeChanges, updateStoreState, pruneState,
  METRICS_FILE, appendMetrics, loadRecentMetrics, pruneMetrics, computeMetricsTrend, computePeakHours, detectHealthDegradation,
  postDiscordWebhook, sendDiscordAlert, sendUrgentAlert,
  IS_MAC, CHROME_VERSION, CHROME_PATH, launchBrowser, closeBrowser, closeRetailerBrowsers, newPage, loadBrowserState, saveBrowserState, isBlockedPage, solveHumanChallenge, fetchRetry, scraperFetch, scraperFetchRetry,
  createProxyAgent, refreshProxySession, rotateRetailerProxy, getRetailerProxyUrl, isProxyAvailable, failoverToBackupProxy, getCachedCookies, cacheRetailerCookies, COOKIE_CACHE_TTL_MS,
  PRIORITY_QUERIES, getQueriesForScan, shuffleKeepCanaryFirst, parsePollIntervalMs, getMTTime, isActiveHour, isBoostPeriod,
  shouldSkipRetailer, recordRetailerOutcome, prioritizeStores, loadKnownProducts, SEED_PRODUCT_URLS, checkWalmartKnownUrls, checkCostcoKnownUrls, checkCostcoKnownUrlsViaBrowser, checkTotalWineKnownUrls, navigateCategory, CATEGORY_URLS,
  FETCH_BLOCKED_PATTERNS, isFetchBlocked, isCostcoBlocked,
  matchCostcoProductPage, matchCostcoTiles, scrapeCostcoViaFetch, scrapeCostcoOnce, scrapeCostcoStore,
  matchTotalWineInitialState, scrapeTotalWineViaFetch, scrapeTotalWineViaBrowser, scrapeTotalWineStore,
  scrapeWalmartViaFetch, scrapeWalmartViaBrowser, scrapeWalmartStore,
  KROGER_PRODUCTS, checkKrogerKnownProducts, getKrogerToken, scrapeKrogerStore, classifyKrogerFind, buildKrogerProductUrl, verifyKrogerCandidatesViaWebsite,
  matchSafewayProducts, scrapeSafewayViaFetch, scrapeSafewayViaBrowser, scrapeSafewayStore,
  matchAlbertsonsProducts, scrapeAlbertsonsViaFetch, scrapeAlbertsonsViaBrowser, scrapeAlbertsonsStore, ALBERTSONS_KEY,
  matchCityHiveProducts, fetchCityHiveCategory, makeCityHiveScraper, CITYHIVE_RETAILERS, CITYHIVE_API_KEY,
  scrapeExtraMileStore, scrapeLiquorExpressStore, scrapeChandlerLiquorsStore,
  scrapeWalgreensViaBrowser, scrapeWalgreensStore,
  SAMSCLUB_PRODUCTS, PRIORITY_SAMSCLUB_PRODUCTS, matchSamsClubProduct, scrapeSamsClubViaFetch, scrapeSamsClubViaBrowser, scrapeSamsClubStore,
  trackHealth, HEALTH_REASONS,
  WATCH_LIST, processWatchList, buildWatchListEmbed, watchListKey,
  REDDIT_INTEL_SUBREDDITS, REDDIT_NATIONAL_SUBREDDITS, REDDIT_AZ_FILTER, REDDIT_INTEL_KEYWORDS, scrapeRedditIntel, inferRetailerFromText,
  validateEnv,
  poll,
};

// Test helpers for setting module-level state
export function _setStoreCache(cache) { storeCache = cache; }
export function _resetPolling() { polling = false; }
export function _resetKrogerToken() { krogerToken = null; krogerTokenPromise = null; }
export function _resetBrowserStateCache() { browserStateCache = null; }
export function _resetRetailerBrowserCache() { for (const k of Object.keys(retailerBrowserCache)) delete retailerBrowserCache[k]; }
export function _resetWalgreensCoords() { walgreensCoords = null; }
export function _getScraperHealth() { return scraperHealth; }
export function _resetScraperHealth() { scraperHealth = {}; }
export function _setScanCounter(n) { scanCounter = n; }
export function _getScanCounter() { return scanCounter; }
export function _resetRetailerFailures() { for (const k of Object.keys(retailerFailures)) delete retailerFailures[k]; }
export function _resetKnownProducts() { knownProducts = {}; }
export function _getKnownProducts() { return knownProducts; }
export function _setKnownProducts(v) { knownProducts = v; }
export function _resetRetailerBrowserLocks() { for (const k of Object.keys(retailerBrowserLocks)) delete retailerBrowserLocks[k]; }
export function _resetRetailerBrowserBlocked() { for (const k of Object.keys(retailerBrowserBlocked)) delete retailerBrowserBlocked[k]; }
export function _setProxyExhausted(v) { primaryProxyExhausted = v; backupProxyExhausted = v; }
export function _getProxyExhausted() { return primaryProxyExhausted && (backupProxyExhausted || !BACKUP_PROXY_URL); }
export function _setPrimaryProxyExhausted(v) { primaryProxyExhausted = v; }
export function _setBackupProxyExhausted(v) { backupProxyExhausted = v; }
export { acquireRetailerLock };

// ─── Entry Point ─────────────────────────────────────────────────────────────

function validateEnv() {
  const warnings = [];
  if (!process.env.DISCORD_WEBHOOK_URL) warnings.push("DISCORD_WEBHOOK_URL not set — alerts will be silently skipped");
  if (!process.env.ZIP_CODE && !ZIP_CODE) warnings.push("ZIP_CODE not set — store discovery will fail");
  if (!process.env.KROGER_CLIENT_ID || !process.env.KROGER_CLIENT_SECRET) warnings.push("Kroger API credentials missing — Kroger scraper will be skipped");
  if (!process.env.SAFEWAY_API_KEY) warnings.push("SAFEWAY_API_KEY not set — Safeway scraper will be skipped");
  if (!process.env.ALBERTSONS_API_KEY && !process.env.SAFEWAY_API_KEY) warnings.push("ALBERTSONS_API_KEY not set (no SAFEWAY_API_KEY fallback) — Albertsons scraper will be skipped");
  if (!process.env.PROXY_URL && !IS_MAC) warnings.push("No PROXY_URL on non-Mac platform — fetch paths may be blocked on datacenter IPs");
  return warnings;
}

/* v8 ignore start -- entry point: calls discoverStores + process.exit + setTimeout scheduling */
async function main() {
  console.log("Bourbon Scout 🥃 starting up...");
  const envWarnings = validateEnv();
  for (const w of envWarnings) console.warn(`[startup] ⚠️ ${w}`);
  if (proxyAgent) console.log(`[proxy] Routing scraper traffic through proxy`);

  try {
    storeCache = await discoverStores({
      zipCode: ZIP_CODE,
      radiusMiles: parseInt(SEARCH_RADIUS_MILES, 10),
      maxStores: parseInt(MAX_STORES_PER_RETAILER, 10),
      krogerClientId: KROGER_CLIENT_ID,
      krogerClientSecret: KROGER_CLIENT_SECRET,
      krogerMaxStores: KROGER_MAX_STORES ? parseInt(KROGER_MAX_STORES, 10) : undefined,
      krogerRadiusMiles: KROGER_RADIUS_MILES ? parseInt(KROGER_RADIUS_MILES, 10) : undefined,
    });

    // Secondary zip codes: discover stores in additional areas and merge (dedup by storeId).
    // Useful for monitoring stores near work, family, or rumored drop locations.
    if (SECONDARY_ZIPS) {
      const secondaryZips = SECONDARY_ZIPS.split(",").map((z) => z.trim()).filter(Boolean);
      for (const zip of secondaryZips) {
        console.log(`[discover] Secondary zip: ${zip}`);
        try {
          const extra = await discoverStores({
            zipCode: zip,
            radiusMiles: parseInt(SEARCH_RADIUS_MILES, 10),
            maxStores: parseInt(MAX_STORES_PER_RETAILER, 10),
            krogerClientId: KROGER_CLIENT_ID,
            krogerClientSecret: KROGER_CLIENT_SECRET,
            krogerMaxStores: KROGER_MAX_STORES ? parseInt(KROGER_MAX_STORES, 10) : undefined,
            krogerRadiusMiles: KROGER_RADIUS_MILES ? parseInt(KROGER_RADIUS_MILES, 10) : undefined,
          });
          // Merge stores, dedup by storeId
          for (const [key, stores] of Object.entries(extra.retailers)) {
            if (!storeCache.retailers[key]) storeCache.retailers[key] = [];
            const existingIds = new Set(storeCache.retailers[key].map((s) => s.storeId));
            for (const store of stores) {
              if (!existingIds.has(store.storeId)) {
                storeCache.retailers[key].push(store);
                existingIds.add(store.storeId);
              }
            }
          }
        } catch (err) {
          console.warn(`[discover] Secondary zip ${zip} failed: ${err.message}`);
        }
      }
    }

    // Prune old metrics entries once at startup (prevents unbounded file growth)
    await pruneMetrics(30);

    const totalStores = Object.values(storeCache.retailers).reduce((sum, arr) => sum + arr.length, 0);
    for (const [key, stores] of Object.entries(storeCache.retailers)) {
      if (stores.length > 0) {
        console.log(`  ${key}: ${stores.map((s) => s.name).join(", ")}`);
      }
    }
    console.log(`Tracking ${TARGET_BOTTLES.length} bottles across ${totalStores} stores`);

    if (process.env.RUN_ONCE === "true") {
      console.log("Single-run mode (RUN_ONCE=true)\n");
      await poll();
      process.exit(0);
      return; // Prevent fall-through when process.exit is mocked in tests
    }

    // Schedule-aware poll loop: scans only during active hours (5 PM – 10 AM MT),
    // with 30-min default intervals and 20-min "boost" intervals on Tue/Thu nights.
    const JITTER_MS = 3 * 60 * 1000; // ±3 min
    const BOOST_INTERVAL_MS = 20 * 60 * 1000; // 20 min
    const DEFAULT_INTERVAL_MS = 30 * 60 * 1000; // 30 min

    // Cache peak hours data — refreshed once per scheduling decision, not per poll
    function getNextPollDelayMs() {
      const { hour, day } = getMTTime();
      if (!isActiveHour(hour)) {
        // Sleep until ACTIVE_START (4 AM MT). If it's 10 PM–midnight, sleep wraps past midnight.
        const now = new Date();
        const mtHourStr = new Intl.DateTimeFormat("en-US", {
          timeZone: "America/Phoenix", hour: "numeric", minute: "numeric", hour12: false,
        }).format(now);
        const [h, m] = mtHourStr.split(":").map(Number);
        let minsUntilStart = (ACTIVE_START - h) * 60 - m;
        if (minsUntilStart <= 0) minsUntilStart += 24 * 60; // Wrap past midnight
        const sleepMs = Math.max(60000, minsUntilStart * 60 * 1000);
        console.log(`[scheduler] Outside active hours (${h}:${String(m).padStart(2, "0")} MT ${day}) — sleeping ${Math.round(sleepMs / 60000)}min until ${ACTIVE_START} AM`);
        return sleepMs;
      }
      // Dynamic boost: use peak hours from metrics when available (≥100 scans),
      // fall back to hardcoded Tue/Thu schedule otherwise. peakSlotsCache is populated
      // at the end of each poll() so the scheduler always has fresh data.
      let boost = isBoostPeriod(hour, day);
      if (!boost && peakSlotsCache?.slots) {
        const currentSlot = `${day}-${hour}`;
        const isPeak = peakSlotsCache.slots.slice(0, 5).some(s => s.slot === currentSlot);
        if (isPeak) {
          boost = true;
          console.log(`[scheduler] Dynamic boost: ${currentSlot} is a peak find time`);
        }
      }
      const base = boost ? BOOST_INTERVAL_MS : DEFAULT_INTERVAL_MS;
      const jitter = (Math.random() - 0.5) * 2 * JITTER_MS;
      const label = base === BOOST_INTERVAL_MS ? "boost" : "default";
      const delayMs = Math.max(60000, base + jitter);
      console.log(`[scheduler] ${label} interval (${day} ${hour}:xx MT) — next poll in ${Math.round(delayMs / 1000)}s`);
      return delayMs;
    }

    function scheduleNextPoll() {
      const nextMs = getNextPollDelayMs();
      /* v8 ignore next -- setTimeout scheduling */
      setTimeout(() => {
        poll()
          .catch((err) => console.error("[scheduler] Poll failed:", err))
          .finally(() => scheduleNextPoll());
      }, nextMs);
    }

    console.log(`Schedule: 4 AM – 10 PM MT — 30min default, 20min boost (Tue/Thu nights), sleeps 10 PM – 4 AM\n`);

    // Reddit intel runs on its own independent loop — lightweight (no browser, no proxy),
    // checks every 5-10 min 24/7 regardless of active hours. Drops are time-critical.
    const REDDIT_INTERVAL_MS = 5 * 60 * 1000; // 5 min base
    function scheduleNextRedditCheck() {
      const jitter = Math.random() * 5 * 60 * 1000; // 0-5 min jitter
      /* v8 ignore next -- setTimeout scheduling */
      setTimeout(async () => {
        try {
          const state = await loadState();
          await scrapeRedditIntel(state);
        } catch (err) {
          console.error("[reddit] Check failed:", err.message);
        }
        scheduleNextRedditCheck();
      }, REDDIT_INTERVAL_MS + jitter);
    }
    scheduleNextRedditCheck();
    console.log("Reddit intel: monitoring r/ArizonaWhiskey + r/arizonabourbon + r/bourbon + r/whiskey + r/Costco_alcohol (every 5-10 min)");

    /* v8 ignore next -- fire-and-forget initial poll */
    poll()
      .catch((err) => console.error("[startup] Initial poll failed:", err))
      .finally(() => scheduleNextPoll());
  } catch (err) {
    console.error(`[startup] Store discovery failed: ${err.message}`);
    process.exit(1);
  }
}
/* v8 ignore stop */

export { main };

// ─── Graceful Shutdown ──────────────────────────────────────────────────────
// On SIGTERM/SIGINT, let the current poll finish (up to 60s) so in-flight
// Discord alerts aren't lost. launchd sends SIGTERM on `launchctl stop`.
let shuttingDown = false;
/* v8 ignore start -- shutdown handler uses process.exit + timers, untestable in unit tests */
function handleShutdown(signal) {
  if (shuttingDown) return; // Already shutting down
  shuttingDown = true;
  console.log(`\n[shutdown] ${signal} received — waiting for current poll to finish...`);
  const deadline = setTimeout(() => {
    console.log("[shutdown] Deadline exceeded — forcing exit");
    process.exit(1);
  }, 60000);
  deadline.unref(); // Don't keep the process alive just for the deadline
  const check = setInterval(() => {
    if (!polling) {
      clearInterval(check);
      clearTimeout(deadline);
      console.log("[shutdown] Clean exit");
      process.exit(0);
    }
  }, 500);
  check.unref();
}
/* v8 ignore stop */
export { shuttingDown, handleShutdown };

// Only run when executed directly (not imported by tests)
/* v8 ignore start -- entry point guard */
if (process.argv[1] === fileURLToPath(import.meta.url)) {
  process.on("SIGTERM", () => handleShutdown("SIGTERM"));
  process.on("SIGINT", () => handleShutdown("SIGINT"));
  main();
}
/* v8 ignore stop */
