import "dotenv/config";
import fetch from "node-fetch";
import { gotScraping } from "got-scraping";
import { HttpsProxyAgent } from "https-proxy-agent";
import { SocksProxyAgent } from "socks-proxy-agent";
import { chromium as rebrowserChromium } from "rebrowser-playwright-core";
import { addExtra } from "playwright-extra";
import StealthPlugin from "puppeteer-extra-plugin-stealth";
// node-cron removed — replaced with setTimeout + jitter for variable scan intervals
import { readFile, writeFile, rename, mkdir, unlink, readdir } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { join, dirname } from "node:path";
import * as cheerio from "cheerio";
import { discoverStores } from "./lib/discover-stores.js";
import { zipToCoords } from "./lib/geo.js";

// rebrowser-patches: patched CDP commands to evade Akamai/PerimeterX automation detection
// Stealth plugin: higher-level browser fingerprint spoofing (navigator.webdriver, etc.)
const chromium = addExtra(rebrowserChromium);
chromium.use(StealthPlugin());

// ─── Configuration ───────────────────────────────────────────────────────────

const {
  DISCORD_WEBHOOK_URL,
  ZIP_CODE = "85283",
  SEARCH_RADIUS_MILES = "15",
  MAX_STORES_PER_RETAILER = "5",
  KROGER_CLIENT_ID,
  KROGER_CLIENT_SECRET,
  SAFEWAY_API_KEY,
  POLL_INTERVAL = "*/15 * * * *",
  REALERT_EVERY_N_SCANS = "4",
  PROXY_URL,
  BACKUP_PROXY_URL,
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
// Active hours: 5 PM – 10 AM MT. Boost (20 min) on Tue/Thu nights, else 30 min.
// Arizona = MST year-round (UTC-7, no DST).
const ACTIVE_START = 17; // 5 PM MT
const ACTIVE_END = 10;   // 10 AM MT

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
  return hour >= ACTIVE_START || hour < ACTIVE_END;
}

function isBoostPeriod(hour, day) {
  return (day === "Tue" && hour >= ACTIVE_START) ||
         (day === "Wed" && hour < ACTIVE_END) ||
         (day === "Thu" && hour >= ACTIVE_START) ||
         (day === "Fri" && hour < ACTIVE_END);
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
  const retailers = ["costco", "totalwine", "walmart", "kroger", "safeway", "walgreens", "samsclub"];
  const basePort = 10000 + Math.floor(Math.random() * 9000); // leave room for 7 retailers
  const backupBasePort = 10000 + Math.floor(Math.random() * 9000);
  for (let i = 0; i < retailers.length; i++) {
    const key = retailers[i];
    if (BACKUP_PROXY_URL && BACKUP_PROXY_RETAILERS.has(key)) {
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
    console.log(`[proxy] Rotated ${retailerKey} to port ${newPort} (new IP)`);
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
  const parsed = new URL(url);
  const config = { server: `${parsed.protocol}//${parsed.host}` };
  if (parsed.username) config.username = decodeURIComponent(parsed.username);
  if (parsed.password) config.password = decodeURIComponent(parsed.password);
  return config;
}

// Per-poll health metrics per retailer. Reset each poll().
// Structure: { retailerKey: { queries: 0, succeeded: 0, failed: 0, blocked: 0 } }
let scraperHealth = {};

function initHealth(key) {
  if (!scraperHealth[key]) scraperHealth[key] = { queries: 0, succeeded: 0, failed: 0, blocked: 0 };
}

function trackHealth(key, outcome) {
  initHealth(key);
  scraperHealth[key].queries++;
  if (outcome === "ok") scraperHealth[key].succeeded++;
  else if (outcome === "blocked") scraperHealth[key].blocked++;
  else scraperHealth[key].failed++;
}

const STATE_FILE = new URL("./state.json", import.meta.url);

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

// ─── Known Product URL Tracking ──────────────────────────────────────────────
// Bottles previously found in state.json have product URLs. Checking these directly
// is less suspicious than searching (it's just visiting a product page) and catches
// bottles that search might miss due to anti-bot blocking.
let knownProducts = {};

function loadKnownProducts(state) {
  knownProducts = {};
  for (const [retailerKey, stores] of Object.entries(state)) {
    const urls = new Set();
    const products = [];
    for (const storeData of Object.values(stores)) {
      if (!storeData?.bottles) continue;
      for (const [name, bottle] of Object.entries(storeData.bottles)) {
        if (bottle.url && !urls.has(bottle.url)) {
          urls.add(bottle.url);
          products.push({ name, url: bottle.url, sku: bottle.sku, price: bottle.price });
        }
      }
    }
    if (products.length > 0) knownProducts[retailerKey] = products;
  }
}

// ─── Query Rotation ─────────────────────────────────────────────────────────
// Instead of running all 14 queries every scan, alternate halves to cut per-scan
// request volume. Each session fires ~8 queries instead of 14 — less "hammering"
// behavioral signal. Every bottle still covered within 2 consecutive scans (30 min
// at 15-min intervals). The canary query always runs for health monitoring.
let scanCounter = 0;

function getQueriesForScan(allQueries) {
  const canaryQuery = "buffalo trace";
  const nonCanary = allQueries.filter((q) => q !== canaryQuery);
  // Split into two groups based on scan parity
  const group = nonCanary.filter((_, i) => i % 2 === scanCounter % 2);
  // Always include canary
  return [...group, canaryQuery];
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
  "buffalo trace",              // Canary bottle — always-available health check
];

const TARGET_BOTTLES = [
  { name: "Blanton's Gold",             searchTerms: ["blanton's gold", "blantons gold"] },
  { name: "Blanton's Straight from the Barrel", searchTerms: ["blanton's straight from the barrel", "blantons straight from the barrel", "blantons sftb", "blanton's sftb"] },
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
  { name: "E.H. Taylor Straight Rye",   searchTerms: ["eh taylor rye", "e.h. taylor rye", "e.h. taylor straight rye", "col. e.h. taylor straight rye"] },
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
  { name: "Pappy Van Winkle 15 Year",   searchTerms: ["pappy van winkle 15"] },
  { name: "Pappy Van Winkle 20 Year",   searchTerms: ["pappy van winkle 20"] },
  { name: "Pappy Van Winkle 23 Year",   searchTerms: ["pappy van winkle 23"] },
  { name: "Van Winkle Family Reserve Rye", searchTerms: ["van winkle family reserve rye", "van winkle rye 13"] },
  { name: "Elmer T. Lee",               searchTerms: ["elmer t. lee", "elmer t lee"] },
  { name: "Rock Hill Farms",            searchTerms: ["rock hill farms"] },
  { name: "King of Kentucky",           searchTerms: ["king of kentucky"] },
  { name: "Old Forester Birthday Bourbon", searchTerms: ["old forester birthday"] },
  { name: "Old Forester President's Choice", searchTerms: ["old forester president's choice", "old forester presidents choice", "old forester president", "president's choice bourbon"] },
  { name: "Old Forester 150th Anniversary", searchTerms: ["old forester 150th", "old forester 150"] },
  { name: "Old Forester King Ranch",    searchTerms: ["old forester king ranch"] },
  // Canary — always-available bottle used as a scraper health check
  { name: "Buffalo Trace", searchTerms: ["buffalo trace"], canary: true },
];

// Fast lookup for canary bottles (filtered out of alerts, used in health summary only)
const CANARY_NAMES = new Set(TARGET_BOTTLES.filter((b) => b.canary).map((b) => b.name));

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
function updateStoreState(state, retailerKey, storeId, currentFound) {
  if (!state[retailerKey]) state[retailerKey] = {};
  const prev = state[retailerKey][storeId]?.bottles || {};
  const now = new Date().toISOString();

  const bottles = {};
  for (const b of currentFound) {
    bottles[b.name] = {
      url: b.url,
      price: b.price,
      sku: b.sku || "",
      size: b.size || "",
      fulfillment: b.fulfillment || "",
      firstSeen: prev[b.name]?.firstSeen || now,
      lastSeen: now,
      scanCount: (prev[b.name]?.scanCount || 0) + 1,
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
  for (let attempt = 0; attempt < 3; attempt++) {
    const res = await fetch(DISCORD_WEBHOOK_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
      signal: AbortSignal.timeout(10000),
    });
    if (res.status === 429) {
      const body = await res.json().catch(() => ({}));
      const retryAfter = (body.retry_after || 2) * 1000;
      console.warn(`[discord] Rate limited — retrying in ${retryAfter}ms`);
      await sleep(retryAfter);
      continue;
    }
    if (!res.ok) {
      console.error(`[discord] Webhook failed: ${res.status} ${await res.text()}`);
    }
    return res;
  }
  console.error("[discord] Webhook failed after 3 retries (rate limited)");
  throw new Error("Discord webhook failed after 3 retries (rate limited)");
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
  newFind:  0x2ecc71,  // green — new bottle spotted
  stillIn:  0x3498db,  // blue — still in stock re-alert
  goneOOS:  0xe67e22,  // orange — went out of stock
  summary:  0x9b59b6,  // purple — scan summary
};

// ─── Store Info Formatting ──────────────────────────────────────────────────

const SKU_LABELS = {
  costco: "Item #", totalwine: "Item #", walmart: "Item #",
  kroger: "SKU", safeway: "UPC", samsclub: "Item #",
};

const STORE_TYPE_LABELS = {
  costco: "Warehouse", totalwine: "Store", walmart: "Store",
  kroger: "Store", safeway: "Store", samsclub: "Club",
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
  return line;
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

  // New finds embed (green, urgent)
  if (changes.newFinds.length > 0) {
    const inStockNames = [...changes.newFinds, ...changes.stillInStock].map((b) => b.name);
    let desc = `${info.storeLine}\n${info.addressLine}\n\n🆕 **NEWLY SPOTTED**\n`;
    desc += changes.newFinds.map((b) => formatBottleLine(b, info.skuLabel, "🟢")).join("\n\n");

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
    const shouldReAlert = changes.stillInStock.some((b) => b.scanCount % reAlertN === 0);
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

const RETAILER_ORDER = ["costco", "totalwine", "walmart", "kroger", "safeway", "walgreens", "samsclub"];
const RETAILER_LABELS = { costco: "Costco", totalwine: "Total Wine", walmart: "Walmart", kroger: "Kroger", safeway: "Safeway", walgreens: "Walgreens", samsclub: "Sam's Club" };

function buildSummaryEmbed({ storesScanned, retailersScanned, totalNewFinds, totalStillInStock, totalGoneOOS, nothingCount, durationSec, scannedStores = [], health = {}, canaryResults = {} }) {
  let desc = `🏬 **${storesScanned}** stores  │  🛍️ **${retailersScanned}** retailers  │  ⏱️ **${durationSec}s**\n\n`;
  desc += `🟢 ${totalNewFinds} new finds   🔵 ${totalStillInStock} still in stock\n`;
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
async function scraperFetch(url, { headers, timeout = 15000, proxyUrl, redirect } = {}) {
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
  const response = await gotScraping(gotOpts);
  const statusCode = response.statusCode;
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

function matchesBottle(text, bottle) {
  const lower = normalizeText(text);
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

// Headers for fetch-based scrapers (mimics a real Chrome 145 browser navigation).
// Must include Sec-CH-UA Client Hints alongside Sec-Fetch-* — omitting them creates
// a fingerprint that matches no real browser and trips bot detectors.
// Platform-aware: uses macOS UA on Mac (self-hosted runner) to match TLS fingerprint.
const IS_MAC = process.platform === "darwin";
const FETCH_HEADERS = {
  "User-Agent": IS_MAC
    ? "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
    : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
  "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
  "Accept-Language": "en-US,en;q=0.9",
  "Accept-Encoding": "gzip, deflate",
  "Referer": "https://www.google.com/",
  "Sec-CH-UA": '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
  "Sec-CH-UA-Mobile": "?0",
  "Sec-CH-UA-Platform": IS_MAC ? '"macOS"' : '"Windows"',
  "Sec-Fetch-Dest": "document",
  "Sec-Fetch-Mode": "navigate",
  "Sec-Fetch-Site": "cross-site",
  "Sec-Fetch-User": "?1",
  "Upgrade-Insecure-Requests": "1",
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
async function launchRetailerBrowser(retailerKey, opts = {}) {
  // Reuse existing persistent context if available
  if (retailerBrowserCache[retailerKey]) {
    try {
      const cached = retailerBrowserCache[retailerKey];
      const page = await cached.context.newPage();
      // Re-minimize on Mac when reusing cached context (macOS may have un-minimized)
      if (cached.minimizeWindowId) {
        try {
          const cdp = await cached.context.newCDPSession(page);
          await cdp.send("Browser.setWindowBounds", { windowId: cached.minimizeWindowId, bounds: { windowState: "minimized" } });
          await cdp.detach();
        } catch { /* non-critical */ }
      }
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
  const proxyConfig = getRetailerBrowserProxy(retailerKey);
  if (proxyConfig) contextOpts.proxy = proxyConfig;

  // Clean mode: skip stealth plugin + rebrowser patches.
  // PerimeterX detects CDP modifications from addExtra/stealth and flags as automation.
  // Plain chromium with --disable-blink-features=AutomationControlled passes undetected.
  const launcher = opts.clean ? rebrowserChromium : chromium;
  const context = await launcher.launchPersistentContext(profileDir, contextOpts);
  retailerBrowserCache[retailerKey] = { context };
  // Minimize the browser window on Mac so it doesn't steal focus during scans.
  // Uses CDP Browser.getWindowForTarget + Browser.setWindowBounds to minimize without
  // affecting page rendering or anti-bot sensor execution.
  // Store the windowId so we can re-minimize after navigation events.
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
  retailerBrowserCache[retailerKey] = { context, minimizeWindowId };
  const page = await context.newPage();
  // Re-minimize after new page creation (macOS may un-minimize)
  if (minimizeWindowId) {
    try {
      const cdp = await context.newCDPSession(page);
      await cdp.send("Browser.setWindowBounds", { windowId: minimizeWindowId, bounds: { windowState: "minimized" } });
      await cdp.detach();
    } catch { /* non-critical */ }
  }
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
// Run after homepage pre-warm navigation completes.
async function humanizePage(page) {
  try {
    // Random mouse movements before scrolling (PerimeterX tracks mouse telemetry)
    const viewport = page.viewportSize() || { width: 1366, height: 768 };
    for (let i = 0; i < 2 + Math.floor(Math.random() * 3); i++) {
      await page.mouse.move(
        100 + Math.random() * (viewport.width - 200),
        100 + Math.random() * (viewport.height - 200),
        { steps: 5 + Math.floor(Math.random() * 10) }, // multi-step = realistic curve
      );
      await sleep(200 + Math.random() * 300);
    }
    // Scroll down slowly (2-3 viewport heights)
    const scrollSteps = 2 + Math.floor(Math.random() * 2);
    for (let i = 0; i < scrollSteps; i++) {
      await page.mouse.wheel(0, 300 + Math.random() * 200);
      await sleep(500 + Math.random() * 500);
    }
    // Hover over a random visible link (if any)
    const links = await page.$$("a[href]");
    if (links.length > 0) {
      const target = links[Math.floor(Math.random() * Math.min(links.length, 20))];
      await target.hover().catch(() => {});
      await sleep(300 + Math.random() * 400);
    }
    // Scroll back to top using wheel events (not scrollTo — that's a JS teleport
    // detectable by anti-bot sensors that monitor scroll event patterns)
    for (let i = 0; i < scrollSteps; i++) {
      await page.mouse.wheel(0, -(300 + Math.random() * 200));
      await sleep(400 + Math.random() * 400);
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
async function solveHumanChallenge(page) {
  try {
    const bodyText = String(await page.evaluate(() => document.body?.innerText?.slice(0, 2000) || "").catch(() => ""));
    if (!bodyText.includes("Press & Hold")) return false;

    console.log("[bot] Detected Press & Hold challenge — solving...");

    // PerimeterX always uses #px-captcha as the container
    const target = await page.$("#px-captcha").catch(() => null);
    if (!target) {
      console.warn("[bot] #px-captcha not found — cannot solve");
      return false;
    }

    const box = await target.boundingBox();
    if (!box) {
      console.warn("[bot] #px-captcha has no bounding box");
      return false;
    }

    // The "Press & Hold" button is rendered inside #px-captcha. Click its center.
    // Move mouse naturally, then press and hold for 3-5 seconds.
    const cx = box.x + box.width / 2 + (Math.random() * 10 - 5);
    const cy = box.y + box.height / 2 + (Math.random() * 6 - 3);
    await page.mouse.move(cx, cy, { steps: 15 + Math.floor(Math.random() * 10) });
    await sleep(300 + Math.random() * 400);

    await page.mouse.down();
    const holdMs = 8000 + Math.random() * 4000;
    console.log(`[bot] Holding for ${(holdMs / 1000).toFixed(1)}s...`);
    await sleep(holdMs);
    await page.mouse.up();

    // Wait for challenge to verify and page to navigate/reload
    await Promise.race([
      page.waitForNavigation({ waitUntil: "domcontentloaded", timeout: 15000 }),
      sleep(15000),
    ]).catch(() => {});
    await sleep(1500 + Math.random() * 1000);

    // Check if challenge resolved
    const stillBlocked = await page.evaluate(() =>
      (document.body?.innerText || "").includes("Press & Hold"),
    ).catch(() => true);

    if (!stillBlocked) {
      console.log("[bot] Challenge solved successfully");
      return true;
    }
    console.warn("[bot] Challenge still present after attempt");
    return false;
  } catch (err) {
    console.warn(`[bot] Challenge solve error: ${err.message}`);
    return false;
  }
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
      if (matchesBottle(title, bottle)) {
        found.push({ name: bottle.name, url, price, sku: id || "", size: parseSize(title), fulfillment: "" });
      }
    }
  });
  return found;
}

// Akamai challenge patterns to detect blocked responses in fetch HTML.
const COSTCO_BLOCKED_PATTERNS = [
  "Access Denied", "robot", "captcha",
  "Request unsuccessful", "Incapsula",
  "Enable JavaScript", "verify you are human",
  "_ct_challenge",
];
function isCostcoBlocked(html) {
  return COSTCO_BLOCKED_PATTERNS.some((p) => html.includes(p));
}

// Try fetching Costco search HTML directly and parsing with cheerio (no browser needed).
// Returns found[] on success, null if blocked by Akamai Bot Manager.
async function scrapeCostcoViaFetch() {
  if (!proxyAgent) return null; // fetch will always be blocked without proxy
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

  // SOCKS5 proxies (e.g. NordVPN) cap concurrent connections — go sequential.
  // HTTP proxies use 2 concurrent (down from 4) to reduce Akamai burst detection.
  const concurrency = PROXY_URL?.startsWith("socks") ? 1 : 2;

  // Batch queries with adaptive concurrency
  // isFirst captured in .map() (sequential) to avoid race in concurrent tasks
  const queryTasks = shuffle(getQueriesForScan(SEARCH_QUERIES)).map((query, i) => async () => {
    if (failures > 3) return; // Early-abort without health tracking — browser will track its own
    // Rotate proxy IP after 2 consecutive failures (likely burned IP)
    if (failures >= 2 && !rotated) {
      rotated = true;
      rotateRetailerProxy("costco");
      cookies = ""; // Clear cookies from old IP's session
      await sleep(1000 + Math.random() * 1000);
    }
    // Inter-query delay with jitter to reduce Akamai burst detection
    if (i > 0) await sleep(1500 + Math.random() * 1500);
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
        if (!retryRes?.ok) { failures++; return; }
        const retryHtml = await retryRes.text();
        if (isCostcoBlocked(retryHtml)) { failures++; return; }
        html = retryHtml;
      }
      const $ = cheerio.load(html);
      const hasTiles = $('[data-testid^="ProductTile_"]').length > 0;
      if (!hasTiles) { validPages++; trackHealth("costco", "ok"); return; }
      validPages++;
      trackHealth("costco", "ok");
      found.push(...matchCostcoTiles($));
    } catch {
      failures++;
    }
  });
  await runWithConcurrency(queryTasks, concurrency);
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
  await humanizePage(page);
  await navigateCategory(page, "costco");

  const found = [];
  for (const query of shuffle(getQueriesForScan(SEARCH_QUERIES))) {
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
          trackHealth("costco", "blocked");
          continue;
        }
      }
      if (!tilesLoaded) {
        // No tiles and not blocked — could be a degraded page or JS failed to hydrate.
        const hasContent = await page.$('main, [data-testid="search-results"], [data-testid="no-results"]').then(el => !!el).catch(() => false);
        if (hasContent) {
          trackHealth("costco", "ok"); // Genuine empty result
        } else {
          console.warn(`[costco] No tiles and no content for query "${query}" — marking degraded`);
          trackHealth("costco", "blocked");
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
          if (matchesBottle(p.title, bottle)) {
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

// Wrapper: try got-scraping fetch-first (Chrome TLS fingerprint), fall back to browser.
// Akamai injects sensor JS (_abck cookie) that requires a real browser, but got-scraping
// may pass the initial request before sensors detect it — worth trying for speed.
async function scrapeCostcoStore() {
  // Try fetch-first with got-scraping (Chrome TLS fingerprint)
  const fetchResult = await scrapeCostcoViaFetch();
  if (fetchResult !== null) {
    console.log("[costco] ✓ fast fetch succeeded");
    return fetchResult;
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
    return [];
  }
  try {
    const scraperPromise = scrapeCostcoOnce(page);
    scraperPromise.catch(() => {}); // Prevent unhandled rejection if timeout closes page
    const result = await withTimeout(scraperPromise, 180000, null);
    if (result === null) {
      console.warn("[costco] Browser scraper timed out (180s)");
      trackHealth("costco", "blocked");
      return [];
    }
    return result;
  } finally {
    await page.close().catch(() => {});
  }
}

// ─── Total Wine: fetch-first with browser fallback ───────────────────────────

// Extract matched bottles from Total Wine INITIAL_STATE JSON.
// Shared by both fetch and browser paths.
function matchTotalWineInitialState(state) {
  const found = [];
  const products = state?.search?.results?.products;
  if (!Array.isArray(products)) return found;

  for (const p of products) {
    // Primary: physical stock or eligible shopping options. Use transactional only
    // as fallback when both stockLevel and shoppingOptions data are absent.
    const hasStockData = p.stockLevel != null || p.shoppingOptions != null;
    const inStock = (p.stockLevel?.[0]?.stock > 0) ||
                    p.shoppingOptions?.some((o) => o.eligible) ||
                    (!hasStockData && p.transactional === true);
    if (!inStock) continue;

    for (const bottle of TARGET_BOTTLES) {
      if (matchesBottle(p.name || "", bottle)) {
        found.push({
          name: bottle.name,
          url: p.productUrl ? (p.productUrl.startsWith("http") ? p.productUrl : `https://www.totalwine.com${p.productUrl}`) : "",
          price: p.price?.[0]?.price ? `$${p.price[0].price}` : "",
          sku: (p.productUrl?.match(/\/p\/(\d+)/) || [])[1] || "",
          size: parseSize(p.name || ""),
          fulfillment: (p.shoppingOptions || []).filter((o) => o.eligible).map((o) => o.name || o.type || "").filter(Boolean).join(", "),
        });
      }
    }
  }
  return found;
}

// Try fetching Total Wine search HTML directly and parsing INITIAL_STATE (no browser needed).
// Returns { name, url, price, sku, size, fulfillment }[] on success, null if blocked.
async function scrapeTotalWineViaFetch(store) {
  if (!proxyAgent) return null; // fetch will always be blocked without proxy
  const twProxy = getRetailerProxyUrl("totalwine");
  const found = [];
  let failures = 0;
  let validPages = 0;

  // Pre-warm: fetch homepage to get PerimeterX session cookies (_px3, _px2, _pxvid).
  // Without these cookies, search URLs immediately return a PerimeterX challenge page.
  // Same pattern as Costco's successful fetch pre-warm.
  let cookies = "";
  try {
    const homeRes = await scraperFetchRetry("https://www.totalwine.com/", {
      headers: FETCH_HEADERS,
      timeout: 10000,
      proxyUrl: twProxy,
    });
    const rawSetCookie = homeRes.headers["set-cookie"];
    const setCookies = Array.isArray(rawSetCookie) ? rawSetCookie : rawSetCookie ? [rawSetCookie] : [];
    cookies = setCookies.map((c) => c.split(";")[0]).join("; ");
  } catch { /* continue without cookies */ }

  // Batch queries (2 concurrent) — lower concurrency reduces PerimeterX burst detection.
  // With 4 stores potentially running fetch simultaneously, even 2 concurrent queries
  // per store means up to 8 total requests hitting totalwine.com at once.
  // isFirst captured in .map() (sequential) to avoid race in concurrent tasks
  const queryTasks = shuffle(getQueriesForScan(SEARCH_QUERIES)).map((query, i) => async () => {
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
    await sleep(3000 + Math.random() * 2000);
    await solveHumanChallenge(page);
    await humanizePage(page);
    console.log(`[totalwine:${store.storeId}] Pre-warm done (${elapsed()}), navigating to category...`);
    await navigateCategory(page, "totalwine");
    console.log(`[totalwine:${store.storeId}] Category done (${elapsed()}), starting queries...`);
  } else {
    console.log(`[totalwine:${store.storeId}] Skipping pre-warm (context already warm)`);
  }

  const found = [];
  const queries = shuffle(getQueriesForScan(SEARCH_QUERIES));
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
          trackHealth("totalwine", "blocked");
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
              if (matchesBottle(p.name, bottle)) {
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

// Wrapper: try got-scraping fetch-first (Chrome TLS fingerprint), fall back to browser.
// PerimeterX injects sensor JS (_px* cookies) that requires a real browser, but got-scraping
// may pass the initial requests before sensors detect it — worth trying for speed.
// Proxy browser tier removed — PerimeterX always blocks proxy IPs, wasting 180s per store.
// Direct residential IP browser (headed on Mac) is the only browser fallback.
async function scrapeTotalWineStore(store) {
  // Fetch-first disabled — PerimeterX blocks got-scraping even with proxy (requires JS sensor
  // execution). The clean browser path works reliably so skip the fetch to save ~10s per store.
  // The fetch function is retained for future use if PerimeterX relaxes fetch blocking.

  // Fail-fast: if browser already blocked this poll, skip remaining stores
  if (retailerBrowserBlocked["totalwine"]) {
    console.log(`[totalwine:${store.storeId}] Skipping browser — blocked earlier this poll`);
    trackHealth("totalwine", "blocked");
    return [];
  }

  // Direct residential IP browser (no proxy, clean mode — no stealth/rebrowser plugins).
  // Skip pre-warm if the persistent context was already warmed by a previous store this poll.
  const skipPreWarm = !!retailerBrowserCache["totalwine"];
  console.log(`[totalwine:${store.storeId}] Queuing browser (direct IP${skipPreWarm ? ", warm" : ""})`);
  const releaseLock = await acquireRetailerLock("totalwine");
  let page;
  try {
    ({ page } = await launchRetailerBrowser("totalwine", { clean: true }));
  } catch (err) {
    console.error(`[totalwine:${store.storeId}] Browser launch failed: ${err.message}`);
    trackHealth("totalwine", "fail");
    retailerBrowserBlocked["totalwine"] = true;
    releaseLock();
    return [];
  }
  try {
    const scraperPromise = scrapeTotalWineViaBrowser(store, page, { skipPreWarm });
    scraperPromise.catch(() => {});
    const result = await withTimeout(scraperPromise, 180000, null);
    if (result === null) {
      console.warn(`[totalwine:${store.storeId}] Browser timed out (180s)`);
      trackHealth("totalwine", "blocked");
      retailerBrowserBlocked["totalwine"] = true;
      return [];
    }
    const twHealth = scraperHealth["totalwine"];
    if (twHealth && twHealth.queries > 0 && twHealth.blocked >= twHealth.queries * 0.75) {
      retailerBrowserBlocked["totalwine"] = true;
    }
    return result;
  } finally {
    await page.close().catch(() => {});
    releaseLock();
  }
}

// ─── Walmart: fetch-first with browser fallback ─────────────────────────────

// Extract matched bottles from Walmart __NEXT_DATA__ JSON.
// Iterates ALL itemStacks (not just [0]), filters to actual products only,
// excludes third-party marketplace sellers, and checks fulfillment.
function matchWalmartNextData(nextData) {
  const found = [];
  const allStacks = nextData?.props?.pageProps?.initialData?.searchResult?.itemStacks || [];
  const items = allStacks.filter(Boolean).flatMap((stack) => stack.items || []);

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
      if (matchesBottle(item.name, bottle)) {
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
  const found = [];
  let failures = 0;
  let validPages = 0;
  let rotated = false;
  // Batch queries (2 concurrent) — lower concurrency reduces Akamai/PerimeterX burst detection.
  // With 5 stores potentially running fetch simultaneously, even 2 concurrent queries
  // per store means up to 10 total requests hitting walmart.com at once.
  // isFirst captured in .map() (sequential) to avoid race in concurrent tasks
  const queryTasks = shuffle(getQueriesForScan(SEARCH_QUERIES)).map((query, i) => async () => {
    if (failures > 3) return; // Early-abort without health tracking — browser will track its own
    // Rotate proxy IP after 2 consecutive failures (likely burned IP)
    if (failures >= 2 && !rotated) {
      rotated = true;
      rotateRetailerProxy("walmart");
      await sleep(1000 + Math.random() * 1000);
    }
    // Inter-query delay with jitter to reduce burst detection
    if (i > 0) await sleep(1500 + Math.random() * 1500);
    const url = `https://www.walmart.com/search?q=${encodeURIComponent(query)}&store_id=${store.storeId}`;
    const headers = { ...FETCH_HEADERS };
    if (i > 0) {
      headers["Sec-Fetch-Site"] = "same-origin";
      headers["Referer"] = "https://www.walmart.com/";
    }
    try {
      const res = await scraperFetchRetry(url, { headers, timeout: 15000, proxyUrl: getRetailerProxyUrl("walmart") });
      if (!res.ok) { failures++; return; }
      const html = await res.text();
      // Use brace-counting to extract JSON (handles </script> inside JSON strings)
      const idx = html.indexOf('id="__NEXT_DATA__"');
      if (idx === -1) { failures++; return; }
      const braceStart = html.indexOf("{", idx);
      if (braceStart === -1) { failures++; return; }
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
      const nextData = JSON.parse(html.slice(braceStart, end));
      const hasSearchResult = nextData?.props?.pageProps?.initialData?.searchResult != null;
      if (!hasSearchResult) { failures++; return; }
      validPages++;
      trackHealth("walmart", "ok");
      found.push(...matchWalmartNextData(nextData));
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

// Browser-based Walmart scraper (fallback). Accepts a shared page.
async function scrapeWalmartViaBrowser(store, page) {
  // Pre-warm: visit homepage to let Akamai/PerimeterX sensor set cookies
  await page.goto("https://www.walmart.com/", { waitUntil: "networkidle", timeout: 20000 }).catch(() => {});
  await sleep(3000 + Math.random() * 2000);
  await humanizePage(page);
  await navigateCategory(page, "walmart");

  const found = [];
  for (const query of shuffle(getQueriesForScan(SEARCH_QUERIES))) {
    const url = `https://www.walmart.com/search?q=${encodeURIComponent(query)}&store_id=${store.storeId}`;
    try {
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 });
      await page.waitForFunction(() => document.querySelector('script#__NEXT_DATA__')?.textContent?.length > 100, { timeout: 10000 }).catch(() => {});

      if (await isBlockedPage(page)) {
        console.warn(`[walmart:${store.storeId}] Bot detection page for query "${query}" — skipping`);
        trackHealth("walmart", "blocked");
        continue;
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
              if (matchesBottle(p.title, bottle)) {
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
  // Fail-fast: if a previous store's browser was blocked this poll, skip remaining
  if (retailerBrowserBlocked["walmart"]) {
    console.log(`[walmart:${store.storeId}] Skipping browser — blocked earlier this poll`);
    trackHealth("walmart", "blocked");
    return dedupFound(knownFound);
  }

  // Acquire per-retailer lock so only one store uses the browser at a time.
  // Multiple concurrent pages on one context triggers Akamai/PerimeterX bot detection.
  console.log(`[walmart:${store.storeId}] ${isCI && !proxyAgent ? "CI mode, " : "Fetch blocked, "}queuing clean browser`);
  const releaseLock = await acquireRetailerLock("walmart");
  let page;
  try {
    ({ page } = await launchRetailerBrowser("walmart", { clean: true }));
  } catch (err) {
    console.error(`[walmart:${store.storeId}] Browser launch failed: ${err.message}`);
    trackHealth("walmart", "fail");
    retailerBrowserBlocked["walmart"] = true;
    releaseLock();
    return dedupFound(knownFound);
  }
  try {
    const scraperPromise = scrapeWalmartViaBrowser(store, page);
    scraperPromise.catch(() => {}); // Prevent unhandled rejection if timeout closes page
    const result = await withTimeout(scraperPromise, 180000, null);
    if (result === null) {
      console.warn(`[walmart:${store.storeId}] Browser scraper timed out (180s)`);
      trackHealth("walmart", "blocked");
      retailerBrowserBlocked["walmart"] = true;
      return dedupFound(knownFound);
    }
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
  await page.goto("https://www.walgreens.com/", { waitUntil: "networkidle", timeout: 20000 }).catch(() => {});
  await sleep(3000 + Math.random() * 2000);
  await solveHumanChallenge(page);
  await humanizePage(page);
  await navigateCategory(page, "walgreens");

  for (const query of shuffle(getQueriesForScan(SEARCH_QUERIES))) {
    const url = `https://www.walgreens.com/search/results.jsp?Ntt=${encodeURIComponent(query)}`;
    try {
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 });
      await page.waitForSelector(".card__product, [class*='card__product'], [data-testid*='product']", { timeout: 10000 }).catch(() => {});

      if (await isBlockedPage(page)) {
        const solved = await solveHumanChallenge(page);
        if (solved) {
          await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 }).catch(() => {});
          await page.waitForSelector(".card__product, [class*='card__product'], [data-testid*='product']", { timeout: 10000 }).catch(() => {});
        }
        if (!solved || (await isBlockedPage(page))) {
          console.warn(`[walgreens] Bot detection page for query "${query}" — skipping`);
          trackHealth("walgreens", "blocked");
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
              // Match multiple OOS text variants to survive copy changes
              outOfStock: text.includes("not sold at your store") ||
                          text.includes("not available at this store") ||
                          text.includes("out of stock") ||
                          text.includes("unavailable"),
            };
          })
        /* v8 ignore stop */
      );

      for (const p of products) {
        if (p.outOfStock) continue;
        for (const bottle of TARGET_BOTTLES) {
          if (matchesBottle(p.title, bottle)) {
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
    } catch (err) {
      console.error(`[walgreens] Error searching "${query}": ${err.message}`);
      trackHealth("walgreens", "fail");
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
      const result = await withTimeout(scraperPromise, 180000, null);
      if (result === null) {
        console.warn("[walgreens] Browser scraper timed out (180s)");
        trackHealth("walgreens", "blocked");
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
  "George T. Stagg":          "13735253987",
  "Eagle Rare 17 Year":       "prod24381479",
  "Pappy Van Winkle 15 Year": "prod3160426",
  "Buffalo Trace":            "13791619865",   // canary
};

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
  if (!proxyAgent) return null;
  const found = [];
  let failures = 0;
  let validPages = 0;
  let rotated = false;
  const entries = shuffle(Object.entries(SAMSCLUB_PRODUCTS));

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

  const productTasks = entries.map(([bottleName, productId], i) => async () => {
    if (failures > Math.floor(entries.length / 2)) return; // Early-abort — browser will track its own
    // Rotate proxy IP after 2 consecutive failures (likely burned IP)
    if (failures >= 2 && !rotated) {
      rotated = true;
      rotateRetailerProxy("samsclub");
      cookies = "";
      await sleep(1000 + Math.random() * 1000);
    }
    if (i > 0) await sleep(1500 + Math.random() * 1500);
    const url = `https://www.samsclub.com/ip/${productId}`;
    const headers = { ...FETCH_HEADERS };
    if (cookies) headers["Cookie"] = cookies;
    if (i > 0) {
      headers["Sec-Fetch-Site"] = "same-origin";
      headers["Referer"] = "https://www.samsclub.com/";
    }
    try {
      const res = await scraperFetchRetry(url, { headers, timeout: 15000, proxyUrl: getRetailerProxyUrl("samsclub") });
      if (!res.ok) { failures++; return; }
      const html = await res.text();
      // Brace-counting JSON extraction (same as Walmart path)
      const idx = html.indexOf('id="__NEXT_DATA__"');
      if (idx === -1) { failures++; return; }
      const braceStart = html.indexOf("{", idx);
      if (braceStart === -1) { failures++; return; }
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
      const nextData = JSON.parse(html.slice(braceStart, end));
      const product = nextData?.props?.pageProps?.initialData?.data?.product;
      if (!product?.name) { failures++; return; }
      validPages++;
      trackHealth("samsclub", "ok");
      const match = matchSamsClubProduct(nextData, bottleName);
      if (match) found.push(match);
    } catch {
      failures++;
    }
  });
  await runWithConcurrency(productTasks, 2);
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
  await humanizePage(page);
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
          trackHealth("samsclub", "blocked");
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
        trackHealth("samsclub", "blocked");
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
      const result = await withTimeout(scraperPromise, 180000, null);
      if (result === null) {
        console.warn("[samsclub] Browser scraper timed out (180s)");
        trackHealth("samsclub", "blocked");
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
      const tokenOpts = {
        method: "POST",
        headers: { Authorization: `Basic ${authHeader}`, "Content-Type": "application/x-www-form-urlencoded" },
        body: "grant_type=client_credentials&scope=product.compact",
        signal: AbortSignal.timeout(15000),
      };
      const krogerAgent = getRetailerProxy("kroger");
      if (krogerAgent) tokenOpts.agent = krogerAgent;
      const res = await fetchRetry("https://api.kroger.com/v1/connect/oauth2/token", tokenOpts);
      if (!res.ok) throw new Error(`OAuth HTTP ${res.status}`);
      krogerToken = (await res.json()).access_token;
      return krogerToken;
    })().finally(() => { krogerTokenPromise = null; });
  }
  return krogerTokenPromise;
}

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

  // Use broad SEARCH_QUERIES (12) instead of per-bottle (40) to cut API calls ~70%
  const found = [];

  function matchKrogerProducts(products) {
    for (const product of products) {
      const title = product.description || "";
      // Require a positive inventory signal — null/missing inventory is not "in stock"
      const inStock = product.items?.some(
        (i) =>
          i.fulfillment?.inStore === true &&
          i.inventory?.stockLevel != null &&
          i.inventory.stockLevel !== "TEMPORARILY_OUT_OF_STOCK"
      );
      if (!inStock) continue;
      for (const bottle of TARGET_BOTTLES) {
        if (matchesBottle(title, bottle)) {
          // Prefer the item variant that is actually in-store, not blindly items[0]
          // (which may be a different size at a different price)
          const inStoreItem = product.items?.find(
            (i) => i.fulfillment?.inStore === true && i.inventory?.stockLevel != null && i.inventory.stockLevel !== "TEMPORARILY_OUT_OF_STOCK"
          ) || product.items?.[0];
          const price = inStoreItem?.price?.promo ?? inStoreItem?.price?.regular;
          const fulfillmentParts = [];
          if (inStoreItem?.fulfillment?.inStore) fulfillmentParts.push("In-store");
          if (inStoreItem?.fulfillment?.shipToHome) fulfillmentParts.push("Ship to home");
          found.push({
            name: bottle.name,
            url: product.productId ? `https://www.kroger.com/p/${product.productId}` : "",
            price: price != null ? `$${price.toFixed(2)}` : "",
            sku: product.productId || "",
            size: inStoreItem?.size || "",
            fulfillment: fulfillmentParts.join(", "),
          });
        }
      }
    }
  }

  // Run all queries in parallel (API-key auth, no bot detection risk)
  await Promise.all(shuffle(getQueriesForScan(SEARCH_QUERIES)).map(async (query, i) => {
    await sleep(i * 50); // stagger starts to avoid thundering herd
    const baseUrl = `https://api.kroger.com/v1/products?filter.term=${encodeURIComponent(query)}&filter.locationId=${store.storeId}&filter.limit=50`;
    try {
      const krogerOpts = {
        headers: { Authorization: `Bearer ${token}`, Accept: "application/json" },
        signal: AbortSignal.timeout(15000),
      };
      const krogerAgent2 = getRetailerProxy("kroger");
      if (krogerAgent2) krogerOpts.agent = krogerAgent2;
      const res = await fetchRetry(baseUrl, krogerOpts);
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
          const page2Opts = { ...krogerOpts, signal: AbortSignal.timeout(15000) };
          const res2 = await fetchRetry(`${baseUrl}&filter.start=50`, page2Opts);
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
  return dedupFound(found);
}

async function scrapeSafewayStore(store) {
  /* v8 ignore next 3 -- env guard */
  if (!SAFEWAY_API_KEY) {
    console.warn("[safeway] Skipping — SAFEWAY_API_KEY not set");
    return [];
  }

  // Use broad SEARCH_QUERIES (12) instead of per-bottle (40) to cut API calls ~70%
  const found = [];
  const baseUrl = "https://www.safeway.com/abs/pub/xapi/pgmsearch/v1/search/products";

  function matchSafewayProducts(products) {
    for (const product of products) {
      const title = product.name || product.productTitle || "";
      if (product.inStock !== true && product.inStock !== 1) continue;
      for (const bottle of TARGET_BOTTLES) {
        if (matchesBottle(title, bottle)) {
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
  }

  // Run all queries in parallel (API-key auth, no bot detection risk)
  await Promise.all(shuffle(getQueriesForScan(SEARCH_QUERIES)).map(async (query, i) => {
    await sleep(i * 50); // stagger starts
    const url = `${baseUrl}?request-id=0&url=https://www.safeway.com&pageurl=search&search-type=keyword&q=${encodeURIComponent(query)}&rows=50&start=0&storeid=${store.storeId}`;
    try {
      const safewayOpts = {
        headers: {
          "Ocp-Apim-Subscription-Key": SAFEWAY_API_KEY,
          Accept: "application/json",
        },
        signal: AbortSignal.timeout(15000),
      };
      const safewayAgent = getRetailerProxy("safeway");
      if (safewayAgent) safewayOpts.agent = safewayAgent;
      const res = await fetchRetry(url, safewayOpts);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      const products = data?.primaryProducts?.response?.docs || [];
      matchSafewayProducts(products);
      trackHealth("safeway", "ok");
      // Fetch page 2 if first page was full (may have more results).
      // Isolated try/catch: page 2 failure shouldn't lose page 1 data or mark query as failed.
      if (products.length === 50) {
        try {
          const page2Url = `${baseUrl}?request-id=0&url=https://www.safeway.com&pageurl=search&search-type=keyword&q=${encodeURIComponent(query)}&rows=50&start=50&storeid=${store.storeId}`;
          const page2Opts = { ...safewayOpts, signal: AbortSignal.timeout(15000) };
          const res2 = await fetchRetry(page2Url, page2Opts);
          if (res2.ok) {
            const data2 = await res2.json();
            matchSafewayProducts(data2?.primaryProducts?.response?.docs || []);
          }
        } catch (p2Err) {
          console.warn(`[safeway:${store.storeId}] Page 2 failed for "${query}": ${p2Err.message}`);
        }
      }
    } catch (err) {
      console.error(`[safeway:${store.storeId}] Error searching "${query}": ${err.message}`);
      trackHealth("safeway", "fail");
    }
  }));
  return dedupFound(found);
}

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
  { key: "safeway",   name: "Safeway",    scrapeOnce: false, needsPage: false, scraper: scrapeSafewayStore },
  { key: "walgreens", name: "Walgreens",  scrapeOnce: true,  needsPage: false, scraper: scrapeWalgreensStore },
  { key: "samsclub", name: "Sam's Club",  scrapeOnce: true,  needsPage: false, scraper: scrapeSamsClubStore },
  // BevMo omitted — no AZ locations
];

// ─── Orchestrator ────────────────────────────────────────────────────────────

let polling = false;
let storeCache = null;

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
  let storesScanned = 0;
  let totalNewFinds = 0;
  let totalStillInStock = 0;
  let totalGoneOOS = 0;
  let nothingCount = 0;
  const retailersSeen = new Set();
  const scannedStores = [];

  // Reset per-poll state
  refreshProxySession();
  krogerToken = null;
  browserStateCache = null;
  scraperHealth = {};
  for (const k of Object.keys(retailerBrowserBlocked)) delete retailerBrowserBlocked[k];
  const canaryResults = {};
  scanCounter++;
  loadKnownProducts(state);

  // Helper to record results for a store and send alerts
  async function recordResult(retailer, store, inStock) {
    // Separate canary from allocated bottles — canary never triggers alerts or state
    const canaryFound = inStock.some((b) => CANARY_NAMES.has(b.name));
    if (canaryFound) {
      canaryResults[retailer.key] = true;
      console.log(`[${retailer.key}:${store.storeId}] 🐤 Canary found (${inStock.filter((b) => CANARY_NAMES.has(b.name)).map((b) => b.name).join(", ")})`);
    }
    const realInStock = inStock.filter((b) => !CANARY_NAMES.has(b.name));

    const previousStore = state[retailer.key]?.[store.storeId];
    const changes = computeChanges(previousStore, realInStock);
    updateStoreState(state, retailer.key, store.storeId, realInStock);
    storesScanned++;
    retailersSeen.add(retailer.key);
    scannedStores.push({ retailerName: retailer.name, storeName: store.name, storeId: store.storeId });

    totalNewFinds += changes.newFinds.length;
    totalStillInStock += changes.stillInStock.length;
    totalGoneOOS += changes.goneOOS.length;

    const embeds = buildStoreEmbeds(retailer.key, retailer.name, store, changes);

    if (changes.newFinds.length > 0) {
      console.log(`[${retailer.key}:${store.storeId}] 🟢 New: ${changes.newFinds.map((b) => b.name).join(", ")}`);
      await sendUrgentAlert(embeds);
    } else if (embeds.length > 0) {
      console.log(`[${retailer.key}:${store.storeId}] ${changes.goneOOS.length > 0 ? "🔴 OOS" : "🔵 Re-alert"}`);
      await sendDiscordAlert(embeds);
    } else {
      nothingCount++;
      console.log(`[${retailer.key}:${store.storeId}] Nothing new`);
    }
  }

  const tasks = [];
  for (const [ri, retailer] of RETAILERS.entries()) {
    const stores = storeCache.retailers[retailer.key] || [];
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

  // Quiet summary at end of every poll
  const durationSec = Math.round((Date.now() - scanStart) / 1000);
  const summary = buildSummaryEmbed({ storesScanned, retailersScanned: retailersSeen.size, totalNewFinds, totalStillInStock, totalGoneOOS, nothingCount, durationSec, scannedStores, health: scraperHealth, canaryResults });
  await sendDiscordAlert([summary]).catch((err) => console.error(`[poll] Summary send failed: ${err.message}`));

  console.log(`[poll] Scan complete — ${storesScanned} stores, ${totalNewFinds} new, ${totalStillInStock} still, ${totalGoneOOS} OOS, ${durationSec}s\n`);
  } finally {
    polling = false;
  }
}

// ─── Exports (for testing) ────────────────────────────────────────────────────

export {
  SEARCH_QUERIES, TARGET_BOTTLES, CANARY_NAMES, RETAILERS, FETCH_HEADERS,
  normalizeText, parseSize, parsePrice, matchesBottle, dedupFound, shuffle, withTimeout, runWithConcurrency, matchWalmartNextData,
  COLORS, SKU_LABELS, formatStoreInfo, parseCity, parseState, timeAgo,
  formatBottleLine, buildOOSList, truncateDescription, truncateTitle, DISCORD_DESC_LIMIT, DISCORD_TITLE_LIMIT, buildStoreEmbeds, buildSummaryEmbed,
  loadState, saveState, computeChanges, updateStoreState, pruneState,
  postDiscordWebhook, sendDiscordAlert, sendUrgentAlert,
  IS_MAC, CHROME_PATH, launchBrowser, closeBrowser, closeRetailerBrowsers, newPage, loadBrowserState, saveBrowserState, isBlockedPage, solveHumanChallenge, fetchRetry, scraperFetch, scraperFetchRetry,
  createProxyAgent, refreshProxySession, rotateRetailerProxy, getRetailerProxyUrl, getQueriesForScan, parsePollIntervalMs, getMTTime, isActiveHour, isBoostPeriod,
  shouldSkipRetailer, recordRetailerOutcome, loadKnownProducts, checkWalmartKnownUrls, navigateCategory, CATEGORY_URLS,
  COSTCO_BLOCKED_PATTERNS, isCostcoBlocked,
  matchCostcoTiles, scrapeCostcoViaFetch, scrapeCostcoOnce, scrapeCostcoStore,
  matchTotalWineInitialState, scrapeTotalWineViaFetch, scrapeTotalWineViaBrowser, scrapeTotalWineStore,
  scrapeWalmartViaFetch, scrapeWalmartViaBrowser, scrapeWalmartStore,
  getKrogerToken, scrapeKrogerStore, scrapeSafewayStore,
  scrapeWalgreensViaBrowser, scrapeWalgreensStore,
  SAMSCLUB_PRODUCTS, matchSamsClubProduct, scrapeSamsClubViaFetch, scrapeSamsClubViaBrowser, scrapeSamsClubStore,
  trackHealth,
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
export function _resetRetailerBrowserLocks() { for (const k of Object.keys(retailerBrowserLocks)) delete retailerBrowserLocks[k]; }
export function _resetRetailerBrowserBlocked() { for (const k of Object.keys(retailerBrowserBlocked)) delete retailerBrowserBlocked[k]; }
export { acquireRetailerLock };

// ─── Entry Point ─────────────────────────────────────────────────────────────

async function main() {
  console.log("Bourbon Scout 🥃 starting up...");
  if (proxyAgent) console.log(`[proxy] Routing scraper traffic through proxy`);

  try {
    storeCache = await discoverStores({
      zipCode: ZIP_CODE,
      radiusMiles: parseInt(SEARCH_RADIUS_MILES, 10),
      maxStores: parseInt(MAX_STORES_PER_RETAILER, 10),
      krogerClientId: KROGER_CLIENT_ID,
      krogerClientSecret: KROGER_CLIENT_SECRET,
    });

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

    function getNextPollDelayMs() {
      const { hour, day } = getMTTime();
      if (!isActiveHour(hour)) {
        // Sleep until 5 PM MT today
        const now = new Date();
        const mtHourStr = new Intl.DateTimeFormat("en-US", {
          timeZone: "America/Phoenix", hour: "numeric", minute: "numeric", hour12: false,
        }).format(now);
        const [h, m] = mtHourStr.split(":").map(Number);
        const minsUntil5PM = (ACTIVE_START - h) * 60 - m;
        const sleepMs = Math.max(60000, minsUntil5PM * 60 * 1000);
        console.log(`[scheduler] Outside active hours (${h}:${String(m).padStart(2, "0")} MT ${day}) — sleeping ${Math.round(sleepMs / 60000)}min until 5 PM`);
        return sleepMs;
      }
      const base = isBoostPeriod(hour, day) ? BOOST_INTERVAL_MS : DEFAULT_INTERVAL_MS;
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

    console.log(`Schedule: 30min default, 20min boost (Tue/Thu nights), active 5 PM – 10 AM MT\n`);
    /* v8 ignore next -- fire-and-forget initial poll */
    poll()
      .catch((err) => console.error("[startup] Initial poll failed:", err))
      .finally(() => scheduleNextPoll());
  } catch (err) {
    console.error(`[startup] Store discovery failed: ${err.message}`);
    process.exit(1);
  }
}

export { main };

// Only run when executed directly (not imported by tests)
/* v8 ignore start -- entry point guard */
if (process.argv[1] === fileURLToPath(import.meta.url)) {
  main();
}
/* v8 ignore stop */
