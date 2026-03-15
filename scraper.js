import "dotenv/config";
import fetch from "node-fetch";
import { HttpsProxyAgent } from "https-proxy-agent";
import { SocksProxyAgent } from "socks-proxy-agent";
import { chromium } from "playwright-extra";
import StealthPlugin from "puppeteer-extra-plugin-stealth";
import cron from "node-cron";
import { readFile, writeFile, rename } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import * as cheerio from "cheerio";
import { discoverStores } from "./lib/discover-stores.js";
import { zipToCoords } from "./lib/geo.js";

// Stealth plugin makes Playwright bypass bot detection fingerprinting
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
} = process.env;

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
// dedicated IP that persists for ~30 min. Each poll() picks a random port so all
// requests in that scan share one residential IP. Prevents IP rotation from
// breaking cookie chains (e.g., Costco pre-warm cookies on IP A, searches from IP B).
let proxySessionUrl = PROXY_URL;
function refreshProxySession() {
  if (!PROXY_URL) return;
  const url = new URL(PROXY_URL);
  // Switch to a random sticky port (10000-20000) for this scan session
  const stickyPort = 10000 + Math.floor(Math.random() * 10001);
  url.port = String(stickyPort);
  proxySessionUrl = url.toString();
  proxyAgent = createProxyAgent(proxySessionUrl);
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
  "sazerac rye 18",            // Sazerac Rye 18 Year (BTAC)
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
  { name: "Sazerac Rye 18 Year",        searchTerms: ["sazerac rye 18", "sazerac 18 year"] },
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
    return JSON.parse(raw);
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

// Loud alert with @everyone for in-stock finds
async function sendUrgentAlert(embeds) {
  /* v8 ignore next -- env guard */
  if (!DISCORD_WEBHOOK_URL) return;
  for (let i = 0; i < embeds.length; i += 4) {
    const batch = embeds.slice(i, i + 4);
    await postDiscordWebhook({
      username: "Bourbon Scout 🥃",
      content: "@everyone 🚨 **ALLOCATED BOURBON SPOTTED!**",
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
  const displayName = store.name.startsWith(retailerName) ? store.name : `${retailerName} ${store.name}`;

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
  const allNames = TARGET_BOTTLES.map((b) => b.name);
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
  const fields = [];
  for (const key of RETAILER_ORDER) {
    const h = health[key];
    if (!h) continue;
    const pct = h.queries > 0 ? h.succeeded / h.queries : 0;
    const emoji = pct >= 0.75 ? "✅" : pct >= 0.25 ? "⚠️" : "❌";
    const canary = canaryResults[key] ? " 🐤" : "";
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

// Parse bottle size from product title text (e.g., "750ml", "1.75L", "750 ML")
function parseSize(text) {
  if (!text) return "";
  const match = text.match(/([\d.]+)\s*(ml|cl|l|liter|litre)/i);
  if (!match) return "";
  const [, num, unit] = match;
  const u = unit.toLowerCase();
  if (u === "ml") return `${num}ml`;
  if (u === "cl") return `${Math.round(parseFloat(num) * 10)}ml`;
  return `${num}L`;
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
      "--disable-extensions",
      "--disable-component-update",
    ],
    ignoreDefaultArgs: ["--enable-automation"],
  };
  if (CHROME_PATH) launchOpts.executablePath = CHROME_PATH;
  if (proxySessionUrl) {
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
  const bodyText = await page.evaluate(() => document.body?.innerText?.slice(0, 2000) || "").catch(() => "");
  const bodyLower = bodyText.toLowerCase();
  return bodyLower.includes("please verify") || bodyLower.includes("are you a robot") ||
    bodyLower.includes("security check") || bodyLower.includes("one more step") ||
    bodyLower.includes("checking your browser");
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

  // Pre-warm: fetch homepage to get session cookies (mirrors browser pre-warm).
  // Akamai gives lighter treatment to requests with valid session cookies.
  let cookies = "";
  try {
    const homeRes = await fetchRetry("https://www.costco.com/", {
      headers: FETCH_HEADERS,
      signal: AbortSignal.timeout(10000),
      agent: proxyAgent,
      redirect: "follow",
    });
    const setCookies = homeRes.headers.raw?.()["set-cookie"] || [];
    cookies = setCookies.map((c) => c.split(";")[0]).join("; ");
  } catch { /* continue without cookies */ }

  // SOCKS5 proxies (e.g. NordVPN) cap concurrent connections — go sequential.
  const concurrency = PROXY_URL?.startsWith("socks") ? 1 : 4;

  // Batch queries with adaptive concurrency
  // isFirst captured in .map() (sequential) to avoid race in concurrent tasks
  const queryTasks = shuffle(SEARCH_QUERIES).map((query, i) => async () => {
    if (failures > 3) return;
    // Inter-query delay with jitter to reduce burst detection
    if (i > 0) await sleep(250 + Math.random() * 250);
    const url = `https://www.costco.com/s?keyword=${encodeURIComponent(query)}`;
    const headers = { ...FETCH_HEADERS };
    if (cookies) headers["Cookie"] = cookies;
    if (i > 0) {
      headers["Sec-Fetch-Site"] = "same-origin";
      headers["Referer"] = "https://www.costco.com/";
    }
    try {
      let res = await fetchRetry(url, {
        headers,
        signal: AbortSignal.timeout(15000),
        agent: proxyAgent,
      });
      let html = res.ok ? await res.text() : "";
      // Retry once with backoff if blocked (Akamai may unblock after a pause)
      if (!res.ok || isCostcoBlocked(html)) {
        await sleep(2000 + Math.random() * 1000);
        const retryRes = await fetch(url, {
          headers,
          signal: AbortSignal.timeout(15000),
          agent: proxyAgent,
        }).catch(() => null);
        if (!retryRes?.ok) { failures++; trackHealth("costco", !res.ok ? "fail" : "blocked"); return; }
        const retryHtml = await retryRes.text();
        if (isCostcoBlocked(retryHtml)) { failures++; trackHealth("costco", "blocked"); return; }
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
      trackHealth("costco", "fail");
    }
  });
  await runWithConcurrency(queryTasks, concurrency);
  if (validPages === 0) return null;
  return dedupFound(found);
}

// Browser-based Costco scraper (fallback). Uses stable MUI data-testid attributes.
async function scrapeCostcoOnce(page) {
  // Pre-warm: visit homepage to let Akamai sensor set _abck cookie
  await page.goto("https://www.costco.com/", { waitUntil: "domcontentloaded", timeout: 15000 }).catch(() => {});
  await sleep(2000);

  const found = [];
  for (const query of shuffle(SEARCH_QUERIES)) {
    const url = `https://www.costco.com/s?keyword=${encodeURIComponent(query)}`;
    try {
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 });
      const tilesLoaded = await page.waitForSelector('[data-testid^="ProductTile_"]', { timeout: 8000 }).then(() => true).catch(() => false);
      if (!tilesLoaded && await isBlockedPage(page)) {
        console.warn(`[costco] Bot detection page for query "${query}" — skipping`);
        trackHealth("costco", "blocked");
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
    await sleep(750);
  }
  return dedupFound(found);
}

// Wrapper: try fetch+cheerio first, fall back to browser if blocked by Akamai.
async function scrapeCostcoStore() {
  const fetchResult = await scrapeCostcoViaFetch();
  if (fetchResult !== null) {
    console.log("[costco] Used fast fetch mode (proxied)");
    return fetchResult;
  }
  console.log("[costco] Fetch blocked, using browser");
  const page = await newPage();
  try {
    const scraperPromise = scrapeCostcoOnce(page);
    scraperPromise.catch(() => {}); // Prevent unhandled rejection if timeout closes page
    const result = await withTimeout(scraperPromise, 180000, []);
    await saveBrowserState(page.context()).catch(() => {});
    return result;
  } finally {
    await page.context().close().catch(() => {});
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
  const found = [];
  let failures = 0;
  let validPages = 0;
  // Batch queries (4 concurrent) — balances speed vs PerimeterX detection risk
  // isFirst captured in .map() (sequential) to avoid race in concurrent tasks
  const queryTasks = shuffle(SEARCH_QUERIES).map((query, i) => async () => {
    if (failures > 3) return;
    const url = `https://www.totalwine.com/search/all?text=${encodeURIComponent(query)}&storeId=${store.storeId}`;
    const headers = { ...FETCH_HEADERS };
    if (i > 0) {
      headers["Sec-Fetch-Site"] = "same-origin";
      headers["Referer"] = "https://www.totalwine.com/";
    }
    try {
      const res = await fetchRetry(url, {
        headers,
        signal: AbortSignal.timeout(15000),
        agent: proxyAgent,
      });
      if (!res.ok) { failures++; trackHealth("totalwine", "fail"); return; }
      const html = await res.text();
      const idx = html.indexOf("window.INITIAL_STATE");
      if (idx === -1) { failures++; trackHealth("totalwine", "blocked"); return; }
      const braceStart = html.indexOf("{", idx);
      if (braceStart === -1) { failures++; trackHealth("totalwine", "blocked"); return; }
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
      if (end === -1) { failures++; trackHealth("totalwine", "blocked"); return; }
      const state = JSON.parse(html.slice(braceStart, end));
      if (!state?.search?.results) { failures++; trackHealth("totalwine", "blocked"); return; }
      validPages++;
      trackHealth("totalwine", "ok");
      found.push(...matchTotalWineInitialState(state));
    } catch {
      failures++;
      trackHealth("totalwine", "fail");
    }
  });
  await runWithConcurrency(queryTasks, 4);
  if (validPages === 0) return null;
  return dedupFound(found);
}

// Browser-based Total Wine scraper (fallback). Accepts a shared Playwright page.
// Product data is in window.INITIAL_STATE.search.results.products (structured JSON).
async function scrapeTotalWineViaBrowser(store, page) {
  const found = [];
  for (const query of shuffle(SEARCH_QUERIES)) {
    const url = `https://www.totalwine.com/search/all?text=${encodeURIComponent(query)}&storeId=${store.storeId}`;
    try {
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 });
      await page.waitForFunction(
        () => !!window.INITIAL_STATE?.search?.results,
        { timeout: 10000 }
      ).catch(() => {});

      if (await isBlockedPage(page)) {
        console.warn(`[totalwine:${store.storeId}] Bot detection page for query "${query}" — skipping`);
        trackHealth("totalwine", "blocked");
        continue;
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
      } else {
        // Fallback: if INITIAL_STATE isn't available, try CSS selectors
        const items = await page.$$eval(
          '[class*="productCard"], [data-testid="product-card"], .product-card',
          /* v8 ignore start -- browser-only DOM callback */
          (cards) =>
            cards.map((el) => ({
              name: (el.querySelector('[class*="title"], [class*="name"], h2, a') || {}).textContent || "",
              inStock: !!el.querySelector('button[class*="addToCart"], button[class*="Add"], [data-testid*="add"]'),
              url: (el.querySelector('a[href*="/spirits/"], a[href*="/wine/"], a') || {}).href || "",
              price: (el.querySelector('[class*="price"], [data-testid*="price"]') || {}).textContent?.trim() || "",
            }))
          /* v8 ignore stop */
        );
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
      }
      trackHealth("totalwine", "ok");
    } catch (err) {
      console.error(`[totalwine:${store.storeId}] Error searching "${query}": ${err.message}`);
      trackHealth("totalwine", "fail");
    }
    await sleep(750);
  }
  return dedupFound(found);
}

// Wrapper: try fetch-first, fall back to browser if blocked by PerimeterX.
async function scrapeTotalWineStore(store) {
  const fetchResult = await scrapeTotalWineViaFetch(store);
  if (fetchResult !== null) {
    console.log(`[totalwine:${store.storeId}] Used fast fetch mode (proxied)`);
    return fetchResult;
  }
  console.log(`[totalwine:${store.storeId}] Fetch blocked, using browser`);
  const page = await newPage();
  try {
    const scraperPromise = scrapeTotalWineViaBrowser(store, page);
    scraperPromise.catch(() => {}); // Prevent unhandled rejection if timeout closes page
    const result = await withTimeout(scraperPromise, 180000, []);
    await saveBrowserState(page.context()).catch(() => {});
    return result;
  } finally {
    await page.context().close().catch(() => {});
  }
}

// ─── Walmart: fetch-first with browser fallback ─────────────────────────────

// Extract matched bottles from Walmart __NEXT_DATA__ JSON.
// Iterates ALL itemStacks (not just [0]), filters to actual products only,
// excludes third-party marketplace sellers, and checks fulfillment.
function matchWalmartNextData(nextData) {
  const found = [];
  const allStacks = nextData?.props?.pageProps?.initialData?.searchResult?.itemStacks || [];
  const items = allStacks.flatMap((stack) => stack.items || []);

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
  // Batch queries (4 concurrent) — balances speed vs Akamai/PerimeterX detection risk
  // isFirst captured in .map() (sequential) to avoid race in concurrent tasks
  const queryTasks = shuffle(SEARCH_QUERIES).map((query, i) => async () => {
    if (failures > 3) return;
    const url = `https://www.walmart.com/search?q=${encodeURIComponent(query)}&store_id=${store.storeId}`;
    const headers = { ...FETCH_HEADERS };
    if (i > 0) {
      headers["Sec-Fetch-Site"] = "same-origin";
      headers["Referer"] = "https://www.walmart.com/";
    }
    try {
      const fetchOpts = { headers, signal: AbortSignal.timeout(15000) };
      if (proxyAgent) fetchOpts.agent = proxyAgent;
      const res = await fetchRetry(url, fetchOpts);
      if (!res.ok) { failures++; trackHealth("walmart", "fail"); return; }
      const html = await res.text();
      // Use brace-counting to extract JSON (handles </script> inside JSON strings)
      const idx = html.indexOf('id="__NEXT_DATA__"');
      if (idx === -1) { failures++; trackHealth("walmart", "blocked"); return; }
      const braceStart = html.indexOf("{", idx);
      if (braceStart === -1) { failures++; trackHealth("walmart", "blocked"); return; }
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
      if (end === -1) { failures++; trackHealth("walmart", "blocked"); return; }
      const nextData = JSON.parse(html.slice(braceStart, end));
      const hasSearchResult = nextData?.props?.pageProps?.initialData?.searchResult != null;
      if (!hasSearchResult) { failures++; trackHealth("walmart", "blocked"); return; }
      validPages++;
      trackHealth("walmart", "ok");
      found.push(...matchWalmartNextData(nextData));
    } catch {
      failures++;
      trackHealth("walmart", "fail");
    }
  });
  await runWithConcurrency(queryTasks, 4);
  if (validPages === 0) return null;
  return dedupFound(found);
}

// Browser-based Walmart scraper (fallback). Accepts a shared page.
async function scrapeWalmartViaBrowser(store, page) {
  // Pre-warm: visit homepage to let Akamai/PerimeterX sensor set cookies
  await page.goto("https://www.walmart.com/", { waitUntil: "domcontentloaded", timeout: 15000 }).catch(() => {});
  await sleep(2000);

  const found = [];
  for (const query of shuffle(SEARCH_QUERIES)) {
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
        for (const p of products) {
          for (const bottle of TARGET_BOTTLES) {
            if (matchesBottle(p.title, bottle)) {
              found.push({ name: bottle.name, url: p.url, price: p.price, sku: "", size: parseSize(p.title), fulfillment: "" });
            }
          }
        }
      }
      trackHealth("walmart", "ok");
    } catch (err) {
      console.error(`[walmart:${store.storeId}] Error searching "${query}": ${err.message}`);
      trackHealth("walmart", "fail");
    }
    await sleep(750);
  }
  return dedupFound(found);
}

async function scrapeWalmartStore(store) {
  // Skip fetch attempt on CI unless a proxy is configured (datacenter IPs are blocked)
  const isCI = process.env.CI === "true" || process.env.GITHUB_ACTIONS === "true";
  if (!isCI || proxyAgent) {
    const fetchResult = await scrapeWalmartViaFetch(store);
    if (fetchResult !== null) {
      console.log(`[walmart:${store.storeId}] Used fast fetch mode${proxyAgent ? " (proxied)" : ""}`);
      return fetchResult;
    }
  }
  console.log(`[walmart:${store.storeId}] ${isCI && !proxyAgent ? "CI mode, " : "Fetch blocked, "}using browser`);
  const page = await newPage();
  try {
    const scraperPromise = scrapeWalmartViaBrowser(store, page);
    scraperPromise.catch(() => {}); // Prevent unhandled rejection if timeout closes page
    const result = await withTimeout(scraperPromise, 180000, []);
    await saveBrowserState(page.context()).catch(() => {});
    return result;
  } finally {
    await page.context().close().catch(() => {});
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
  if (!walgreensCoords) walgreensCoords = await zipToCoords(ZIP_CODE);

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

  for (const query of shuffle(SEARCH_QUERIES)) {
    const url = `https://www.walgreens.com/search/results.jsp?Ntt=${encodeURIComponent(query)}`;
    try {
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 });
      await page.waitForSelector(".card__product", { timeout: 10000 }).catch(() => {});

      if (await isBlockedPage(page)) {
        console.warn(`[walgreens] Bot detection page for query "${query}" — skipping`);
        trackHealth("walgreens", "blocked");
        continue;
      }

      const products = await page.$$eval(
        ".card__product",
        /* v8 ignore start -- browser-only DOM callback */
        (cards) =>
          cards.map((el) => ({
            title: (el.querySelector(".product__title, [class*='product__title']") || {}).textContent?.trim() || "",
            price: (el.querySelector(".product__price-contain, [class*='product__price']") || {}).textContent?.trim() || "",
            url: (el.querySelector('a[href*="ID="]') || {}).href || "",
            outOfStock: (el.textContent || "").includes("Not sold at your store"),
          }))
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
    await sleep(750);
  }
  return dedupFound(found);
}

// Wrapper: browser-only (no fetch-first — Akamai blocks direct HTTP).
async function scrapeWalgreensStore() {
  const page = await newPage();
  try {
    const scraperPromise = scrapeWalgreensViaBrowser(page);
    scraperPromise.catch(() => {}); // Prevent unhandled rejection if timeout closes page
    const result = await withTimeout(scraperPromise, 180000, []);
    await saveBrowserState(page.context()).catch(() => {});
    return result;
  } finally {
    await page.context().close().catch(() => {});
  }
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
  const entries = shuffle(Object.entries(SAMSCLUB_PRODUCTS));

  const productTasks = entries.map(([bottleName, productId], i) => async () => {
    if (failures > Math.floor(entries.length / 2)) return;
    if (i > 0) await sleep(250 + Math.random() * 250);
    const url = `https://www.samsclub.com/ip/${productId}`;
    const headers = { ...FETCH_HEADERS };
    if (i > 0) {
      headers["Sec-Fetch-Site"] = "same-origin";
      headers["Referer"] = "https://www.samsclub.com/";
    }
    try {
      const res = await fetchRetry(url, {
        headers,
        signal: AbortSignal.timeout(15000),
        agent: proxyAgent,
      });
      if (!res.ok) { failures++; trackHealth("samsclub", "fail"); return; }
      const html = await res.text();
      // Brace-counting JSON extraction (same as Walmart path)
      const idx = html.indexOf('id="__NEXT_DATA__"');
      if (idx === -1) { failures++; trackHealth("samsclub", "blocked"); return; }
      const braceStart = html.indexOf("{", idx);
      if (braceStart === -1) { failures++; trackHealth("samsclub", "blocked"); return; }
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
      if (end === -1) { failures++; trackHealth("samsclub", "blocked"); return; }
      const nextData = JSON.parse(html.slice(braceStart, end));
      const product = nextData?.props?.pageProps?.initialData?.data?.product;
      if (!product?.name) { failures++; trackHealth("samsclub", "blocked"); return; }
      validPages++;
      trackHealth("samsclub", "ok");
      const match = matchSamsClubProduct(nextData, bottleName);
      if (match) found.push(match);
    } catch {
      failures++;
      trackHealth("samsclub", "fail");
    }
  });
  await runWithConcurrency(productTasks, 4);
  if (validPages === 0) return null;
  return dedupFound(found);
}

// Browser-based Sam's Club scraper (fallback).
async function scrapeSamsClubViaBrowser(page) {
  // Pre-warm: visit homepage to let PerimeterX sensor set cookies
  await page.goto("https://www.samsclub.com/", { waitUntil: "domcontentloaded", timeout: 15000 }).catch(() => {});
  await sleep(2000);

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
        console.warn(`[samsclub] Bot detection on product ${productId} — skipping`);
        trackHealth("samsclub", "blocked");
        continue;
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
      }
      trackHealth("samsclub", "ok");
    } catch (err) {
      console.error(`[samsclub] Error checking ${bottleName}: ${err.message}`);
      trackHealth("samsclub", "fail");
    }
    await sleep(750);
  }
  return dedupFound(found);
}

// Wrapper: try fetch-first, fall back to browser.
async function scrapeSamsClubStore() {
  const fetchResult = await scrapeSamsClubViaFetch();
  if (fetchResult !== null) {
    console.log("[samsclub] Used fast fetch mode (proxied)");
    return fetchResult;
  }
  console.log("[samsclub] Fetch blocked, using browser");
  const page = await newPage();
  try {
    const scraperPromise = scrapeSamsClubViaBrowser(page);
    scraperPromise.catch(() => {}); // Prevent unhandled rejection if timeout closes page
    const result = await withTimeout(scraperPromise, 180000, []);
    await saveBrowserState(page.context()).catch(() => {});
    return result;
  } finally {
    await page.context().close().catch(() => {});
  }
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
      if (proxyAgent) tokenOpts.agent = proxyAgent;
      const res = await fetch("https://api.kroger.com/v1/connect/oauth2/token", tokenOpts);
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
  await Promise.all(shuffle(SEARCH_QUERIES).map(async (query, i) => {
    await sleep(i * 50); // stagger starts to avoid thundering herd
    const baseUrl = `https://api.kroger.com/v1/products?filter.term=${encodeURIComponent(query)}&filter.locationId=${store.storeId}&filter.limit=50`;
    try {
      const krogerOpts = {
        headers: { Authorization: `Bearer ${token}`, Accept: "application/json" },
        signal: AbortSignal.timeout(15000),
      };
      if (proxyAgent) krogerOpts.agent = proxyAgent;
      const res = await fetchRetry(baseUrl, krogerOpts);
      // Clear cached token on 401 so next call re-authenticates
      if (res.status === 401) { krogerToken = null; throw new Error("Token expired (401)"); }
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      matchKrogerProducts(data.data || []);
      // Fetch page 2 if first page was full (may have more results)
      if (data.data?.length === 50) {
        const page2Opts = { ...krogerOpts, signal: AbortSignal.timeout(15000) };
        const res2 = await fetchRetry(`${baseUrl}&filter.start=50`, page2Opts);
        if (res2.ok) {
          const data2 = await res2.json();
          matchKrogerProducts(data2.data || []);
        }
      }
      trackHealth("kroger", "ok");
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
  await Promise.all(shuffle(SEARCH_QUERIES).map(async (query, i) => {
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
      if (proxyAgent) safewayOpts.agent = proxyAgent;
      const res = await fetchRetry(url, safewayOpts);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      const products = data?.primaryProducts?.response?.docs || [];
      matchSafewayProducts(products);
      // Fetch page 2 if first page was full (may have more results)
      if (products.length === 50) {
        const page2Url = `${baseUrl}?request-id=0&url=https://www.safeway.com&pageurl=search&search-type=keyword&q=${encodeURIComponent(query)}&rows=50&start=50&storeid=${store.storeId}`;
        const page2Opts = { ...safewayOpts, signal: AbortSignal.timeout(15000) };
        const res2 = await fetchRetry(page2Url, page2Opts);
        if (res2.ok) {
          const data2 = await res2.json();
          matchSafewayProducts(data2?.primaryProducts?.response?.docs || []);
        }
      }
      trackHealth("safeway", "ok");
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
  const canaryResults = {};

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
  for (const retailer of RETAILERS) {
    const stores = storeCache.retailers[retailer.key] || [];
    if (stores.length === 0) continue;

    if (retailer.scrapeOnce) {
      // Scrape once, broadcast results to all stores (e.g., Costco has no per-store filter)
      tasks.push(async () => {
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
      for (const store of stores) {
        tasks.push(async () => {
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
      // API/fetch-based scrapers: no browser page needed
      for (const store of stores) {
        tasks.push(async () => {
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

  await closeBrowser().catch((err) => console.error(`[poll] closeBrowser failed: ${err.message}`));
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
  IS_MAC, CHROME_PATH, launchBrowser, closeBrowser, newPage, loadBrowserState, saveBrowserState, isBlockedPage, fetchRetry,
  createProxyAgent, refreshProxySession,
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
export function _resetWalgreensCoords() { walgreensCoords = null; }
export function _getScraperHealth() { return scraperHealth; }
export function _resetScraperHealth() { scraperHealth = {}; }

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
    }

    console.log(`Poll schedule: ${POLL_INTERVAL}\n`);
    /* v8 ignore next -- fire-and-forget initial poll */
    poll().catch((err) => console.error("[startup] Initial poll failed:", err));

    cron.schedule(POLL_INTERVAL, /* v8 ignore next */ () => {
      poll().catch((err) => console.error("[cron] Poll failed:", err));
    });
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
