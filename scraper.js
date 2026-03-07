import "dotenv/config";
import fetch from "node-fetch";
import { chromium } from "playwright-extra";
import StealthPlugin from "puppeteer-extra-plugin-stealth";
import cron from "node-cron";
import { readFile, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { discoverStores } from "./lib/discover-stores.js";

// Stealth plugin makes Playwright bypass bot detection fingerprinting
chromium.use(StealthPlugin());

// ─── Configuration ───────────────────────────────────────────────────────────

const {
  DISCORD_WEBHOOK_URL,
  ZIP_CODE = "85281",
  SEARCH_RADIUS_MILES = "15",
  MAX_STORES_PER_RETAILER = "5",
  KROGER_CLIENT_ID,
  KROGER_CLIENT_SECRET,
  SAFEWAY_API_KEY,
  POLL_INTERVAL = "*/15 * * * *",
  REALERT_EVERY_N_SCANS = "4",
} = process.env;

const STATE_FILE = new URL("./state.json", import.meta.url);

// ─── Target Bottles ──────────────────────────────────────────────────────────
// Broad search queries that cover multiple bottles in a single page load.
// Each query should cover at least one TARGET_BOTTLE via matchesBottle.
const SEARCH_QUERIES = [
  "weller bourbon",           // Weller SR/107/12/FP/SB/CYPB + William Larue Weller
  "blantons bourbon",          // Blanton's Gold/SFTB/SR/Red
  "pappy van winkle",          // Pappy 10/12/15/20/23
  "eh taylor bourbon",         // E.H. Taylor Small Batch
  "stagg bourbon",             // Stagg Jr + George T. Stagg
  "eagle rare 17",             // Eagle Rare 17 Year (BTAC)
  "sazerac rye 18",            // Sazerac Rye 18 Year (BTAC)
  "thomas handy sazerac",      // Thomas H. Handy (BTAC)
  "elmer t lee",               // Elmer T. Lee
  "rock hill farms",           // Rock Hill Farms
  "king of kentucky bourbon",  // King of Kentucky
];

const TARGET_BOTTLES = [
  { name: "Blanton's Gold",             searchTerms: ["blanton's gold", "blantons gold"] },
  { name: "Blanton's Straight from the Barrel", searchTerms: ["blanton's straight from the barrel", "blantons straight from the barrel", "blantons sftb"] },
  { name: "Blanton's Special Reserve",  searchTerms: ["blanton's special reserve", "blantons special reserve"] },
  { name: "Blanton's Red",              searchTerms: ["blanton's red", "blantons red"] },
  { name: "Weller Special Reserve",      searchTerms: ["weller special reserve"] },
  { name: "Weller Antique 107",          searchTerms: ["weller antique 107", "old weller antique"] },
  { name: "Weller 12 Year",             searchTerms: ["weller 12", "w.l. weller 12"] },
  { name: "Weller Full Proof",          searchTerms: ["weller full proof"] },
  { name: "Weller Single Barrel",       searchTerms: ["weller single barrel"] },
  { name: "Weller C.Y.P.B.",            searchTerms: ["weller cypb", "weller c.y.p.b", "craft your perfect bourbon"] },
  { name: "E.H. Taylor Small Batch",    searchTerms: ["eh taylor small batch", "e.h. taylor"] },
  { name: "Stagg Jr",                   searchTerms: ["stagg jr", "stagg junior"] },
  { name: "George T. Stagg",            searchTerms: ["george t stagg"] },
  { name: "Eagle Rare 17 Year",         searchTerms: ["eagle rare 17"] },
  { name: "William Larue Weller",       searchTerms: ["william larue weller", "wm larue weller"] },
  { name: "Thomas H. Handy",            searchTerms: ["thomas h. handy", "thomas handy sazerac", "thomas h handy"] },
  { name: "Sazerac Rye 18 Year",        searchTerms: ["sazerac rye 18", "sazerac 18"] },
  { name: "Pappy Van Winkle 10 Year",   searchTerms: ["pappy van winkle 10", "old rip van winkle 10"] },
  { name: "Pappy Van Winkle 12 Year",   searchTerms: ["pappy van winkle 12", "van winkle special reserve 12"] },
  { name: "Pappy Van Winkle 15 Year",   searchTerms: ["pappy van winkle 15"] },
  { name: "Pappy Van Winkle 20 Year",   searchTerms: ["pappy van winkle 20"] },
  { name: "Pappy Van Winkle 23 Year",   searchTerms: ["pappy van winkle 23"] },
  { name: "Elmer T. Lee",               searchTerms: ["elmer t lee"] },
  { name: "Rock Hill Farms",            searchTerms: ["rock hill farms"] },
  { name: "King of Kentucky",           searchTerms: ["king of kentucky"] },
];

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
  await writeFile(STATE_FILE, JSON.stringify(state, null, 2));
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

// ─── Discord Alerts ──────────────────────────────────────────────────────────

// Post a Discord webhook payload with automatic 429 retry (up to 3 attempts).
async function postDiscordWebhook(payload) {
  for (let attempt = 0; attempt < 3; attempt++) {
    const res = await fetch(DISCORD_WEBHOOK_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
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
  kroger: "SKU", safeway: "UPC",
};

const STORE_TYPE_LABELS = {
  costco: "Warehouse", totalwine: "Store", walmart: "Store",
  kroger: "Store", safeway: "Store",
};

function parseCity(address) {
  if (!address) return "";
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

  return {
    title: `${retailerName} ${store.name} (#${store.storeId})${dist}`,
    storeLine: `🏬 ${retailerName} ${store.name}${storeNum}${cityState}`,
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
      title: `🚨 NEW FIND — ${info.title}`,
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
      title: `⚠️ STOCK LOST — ${info.title}`,
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
        title: `🔵 STILL AVAILABLE — ${info.title}`,
        description: truncateDescription(desc),
        color: COLORS.stillIn,
        footer: { text: `Bourbon Scout 🥃 │ ${retailerName}` },
        timestamp: new Date().toISOString(),
      });
    }
  }

  return embeds;
}

function buildSummaryEmbed({ storesScanned, retailersScanned, totalNewFinds, totalStillInStock, totalGoneOOS, nothingCount, durationSec }) {
  let desc = `🏬 **${storesScanned}** stores  │  🛍️ **${retailersScanned}** retailers  │  ⏱️ **${durationSec}s**\n\n`;
  desc += `🟢 ${totalNewFinds} new finds   🔵 ${totalStillInStock} still in stock\n`;
  desc += `🔴 ${totalGoneOOS} went OOS    💤 ${nothingCount} nothing`;

  return {
    title: "📊 Scan Complete",
    description: desc,
    color: COLORS.summary,
    footer: { text: `Bourbon Scout 🥃 │ ${POLL_INTERVAL}` },
    timestamp: new Date().toISOString(),
  };
}

// ─── Utility ─────────────────────────────────────────────────────────────────

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Parse bottle size from product title text (e.g., "750ml", "1.75L", "750 ML")
function parseSize(text) {
  if (!text) return "";
  const match = text.match(/([\d.]+)\s*(ml|l|liter|litre)/i);
  if (!match) return "";
  const [, num, unit] = match;
  if (unit.toLowerCase() === "ml") return `${num}ml`;
  return `${num}L`;
}

function matchesBottle(text, bottle) {
  const lower = text.toLowerCase();
  return bottle.searchTerms.some((term) => lower.includes(term));
}

// Dedup found bottles by name (keeps first occurrence with its url/price)
function dedupFound(found) {
  const seen = new Set();
  return found.filter((f) => {
    if (seen.has(f.name)) return false;
    seen.add(f.name);
    return true;
  });
}

// Run async tasks with a concurrency limit
async function runWithConcurrency(tasks, limit) {
  const executing = new Set();
  for (const task of tasks) {
    const p = task().finally(() => executing.delete(p));
    executing.add(p);
    if (executing.size >= limit) await Promise.race(executing);
  }
  await Promise.all(executing);
}

// Headers for fetch-based scrapers (mimics a real browser)
const FETCH_HEADERS = {
  "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
  "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
  "Accept-Language": "en-US,en;q=0.9",
};

// ─── Browser Management ──────────────────────────────────────────────────────

let browser = null;

async function launchBrowser() {
  browser = await chromium.launch({ headless: true });
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
  const context = await browser.newContext({
    viewport: { width: 1366, height: 768 },
    locale: "en-US",
  });
  return context.newPage();
}

// ─── Retailer Scrapers ───────────────────────────────────────────────────────
// Each scraper accepts a store object and returns an array of { name, url, price }.

// Costco search has no store filter — results are identical across warehouses.
// Scrape once and the poll loop copies results to all stores.
// Uses stable MUI data-testid attributes instead of fragile CSS classes.
async function scrapeCostcoOnce(page) {
  const found = [];
  for (const query of SEARCH_QUERIES) {
    const url = `https://www.costco.com/s?keyword=${encodeURIComponent(query)}`;
    try {
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 });
      // Wait for product tiles or timeout (page may have no results)
      await page.waitForSelector('[data-testid^="ProductTile_"]', { timeout: 8000 }).catch(() => {});

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
        for (const bottle of TARGET_BOTTLES) {
          if (matchesBottle(p.title, bottle)) {
            found.push({ name: bottle.name, url: p.url, price: p.price, sku: p.id || "", size: parseSize(p.title), fulfillment: "" });
          }
        }
      }
    } catch (err) {
      console.error(`[costco] Error searching "${query}": ${err.message}`);
    }
    await sleep(1500);
  }
  return dedupFound(found);
}

// Total Wine uses PerimeterX — no fetch path possible. Browser required.
// Product data is in window.INITIAL_STATE.search.results.products (structured JSON).
// This is far more reliable than CSS selectors (won't break when class names change).
async function scrapeTotalWineStore(store, page) {
  const found = [];
  // Use storeId URL param (more reliable than cookie)
  for (const query of SEARCH_QUERIES) {
    const url = `https://www.totalwine.com/search/all?text=${encodeURIComponent(query)}&storeId=${store.storeId}`;
    try {
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 });
      await page.waitForTimeout(2000);

      // Extract structured product data from INITIAL_STATE (SSR React state)
      const products = await page.evaluate(
        /* v8 ignore start -- browser-only DOM callback */
        () => {
          try {
            const state = window.INITIAL_STATE;
            if (!state?.search?.results?.products) return [];
            return state.search.results.products.map((p) => ({
              name: p.name || "",
              url: p.productUrl || "",
              price: p.price?.[0]?.price ? `$${p.price[0].price}` : "",
              sku: (p.productUrl?.match(/\/p\/(\d+)/) || [])[1] || "",
              fulfillment: (p.shoppingOptions || []).filter((o) => o.eligible).map((o) => o.name || o.type || "").join(", "),
              inStock: (p.stockLevel?.[0]?.stock > 0) ||
                       p.transactional === true ||
                       p.shoppingOptions?.some((o) => o.eligible),
            }));
          } catch { return []; }
        }
        /* v8 ignore stop */
      );

      // Fallback: if INITIAL_STATE isn't available, try CSS selectors
      const items = products.length > 0 ? products : await page.$$eval(
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
              sku: p.sku || "",
              size: parseSize(p.name),
              fulfillment: p.fulfillment || "",
            });
          }
        }
      }
    } catch (err) {
      console.error(`[totalwine:${store.storeId}] Error searching "${query}": ${err.message}`);
    }
    await sleep(1500);
  }
  return dedupFound(found);
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

    const available = item.availabilityStatusV2?.value === "IN_STOCK" || item.canAddToCart === true;
    if (!available || !item.name) continue;

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
// Returns { name, url, price }[] on success, null if blocked/unavailable.
async function scrapeWalmartViaFetch(store) {
  const found = [];
  for (const query of SEARCH_QUERIES) {
    const url = `https://www.walmart.com/search?q=${encodeURIComponent(query)}&cat_id=976759&store_id=${store.storeId}`;
    try {
      const res = await fetch(url, { headers: FETCH_HEADERS, timeout: 15000 });
      if (!res.ok) return null;
      const html = await res.text();
      const match = html.match(/<script id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>/);
      if (!match) return null;
      found.push(...matchWalmartNextData(JSON.parse(match[1])));
    } catch {
      return null;
    }
    await sleep(500);
  }
  return dedupFound(found);
}

// Browser-based Walmart scraper (fallback). Accepts a shared page.
async function scrapeWalmartViaBrowser(store, page) {
  const found = [];
  for (const query of SEARCH_QUERIES) {
    const url = `https://www.walmart.com/search?q=${encodeURIComponent(query)}&cat_id=976759&store_id=${store.storeId}`;
    try {
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 });
      await page.waitForTimeout(2000);

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
    } catch (err) {
      console.error(`[walmart:${store.storeId}] Error searching "${query}": ${err.message}`);
    }
    await sleep(1500);
  }
  return dedupFound(found);
}

async function scrapeWalmartStore(store) {
  const fetchResult = await scrapeWalmartViaFetch(store);
  if (fetchResult !== null) {
    console.log(`[walmart:${store.storeId}] Used fast fetch mode`);
    return fetchResult;
  }
  console.log(`[walmart:${store.storeId}] Fetch blocked, using browser`);
  const page = await newPage();
  try {
    return await scrapeWalmartViaBrowser(store, page);
  } finally {
    await page.context().close();
  }
}

// ─── API-based scrapers ─────────────────────────────────────────────────────

// Fetch Kroger OAuth token once, shared across all store scrapers
let krogerToken = null;
async function getKrogerToken() {
  if (krogerToken) return krogerToken;
  /* v8 ignore next -- env guard */
  if (!KROGER_CLIENT_ID || !KROGER_CLIENT_SECRET) return null;
  const authHeader = Buffer.from(`${KROGER_CLIENT_ID}:${KROGER_CLIENT_SECRET}`).toString("base64");
  const res = await fetch("https://api.kroger.com/v1/connect/oauth2/token", {
    method: "POST",
    headers: { Authorization: `Basic ${authHeader}`, "Content-Type": "application/x-www-form-urlencoded" },
    body: "grant_type=client_credentials&scope=product.compact",
  });
  if (!res.ok) throw new Error(`OAuth HTTP ${res.status}`);
  krogerToken = (await res.json()).access_token;
  return krogerToken;
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

  // Use broad SEARCH_QUERIES (11) instead of per-bottle (25) to cut API calls ~60%
  const found = [];
  for (const query of SEARCH_QUERIES) {
    const url = `https://api.kroger.com/v1/products?filter.term=${encodeURIComponent(query)}&filter.locationId=${store.storeId}&filter.limit=20`;
    try {
      const res = await fetch(url, {
        headers: { Authorization: `Bearer ${token}`, Accept: "application/json" },
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      for (const product of data.data || []) {
        const title = product.description || "";
        const inStock = product.items?.some(
          (i) =>
            i.fulfillment?.inStore === true &&
            i.inventory?.stockLevel !== "TEMPORARILY_OUT_OF_STOCK"
        );
        if (!inStock) continue;
        for (const bottle of TARGET_BOTTLES) {
          if (matchesBottle(title, bottle)) {
            const item0 = product.items?.[0];
            const price = item0?.price?.regular;
            const fulfillmentParts = [];
            if (item0?.fulfillment?.inStore) fulfillmentParts.push("In-store");
            if (item0?.fulfillment?.shipToHome) fulfillmentParts.push("Ship to home");
            found.push({
              name: bottle.name,
              url: product.productId ? `https://www.kroger.com/p/${product.productId}` : "",
              price: price != null ? `$${price.toFixed(2)}` : "",
              sku: product.productId || "",
              size: item0?.size || "",
              fulfillment: fulfillmentParts.join(", "),
            });
          }
        }
      }
    } catch (err) {
      console.error(`[kroger:${store.storeId}] Error searching "${query}": ${err.message}`);
    }
    await sleep(500);
  }
  return dedupFound(found);
}

async function scrapeSafewayStore(store) {
  /* v8 ignore next 3 -- env guard */
  if (!SAFEWAY_API_KEY) {
    console.warn("[safeway] Skipping — SAFEWAY_API_KEY not set");
    return [];
  }

  // Use broad SEARCH_QUERIES (11) instead of per-bottle (25) to cut API calls ~60%
  const found = [];
  const baseUrl = "https://www.safeway.com/abs/pub/xapi/pgmsearch/v1/search/products";

  for (const query of SEARCH_QUERIES) {
    const url = `${baseUrl}?request-id=0&url=https://www.safeway.com&pageurl=search&search-type=keyword&q=${encodeURIComponent(query)}&rows=20&start=0&storeid=${store.storeId}`;
    try {
      const res = await fetch(url, {
        headers: {
          "Ocp-Apim-Subscription-Key": SAFEWAY_API_KEY,
          Accept: "application/json",
        },
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      const products = data?.primaryProducts?.response?.docs || [];
      for (const product of products) {
        const title = product.name || product.productTitle || "";
        if (product.inStock === false) continue;
        for (const bottle of TARGET_BOTTLES) {
          if (matchesBottle(title, bottle)) {
            const fulfillmentParts = [];
            if (product.curbsideEligible) fulfillmentParts.push("Curbside");
            if (product.deliveryEligible) fulfillmentParts.push("Delivery");
            found.push({
              name: bottle.name,
              url: product.url ? `https://www.safeway.com${product.url}` : "",
              price: product.price != null ? `$${product.price}` : "",
              sku: product.upc || product.pid || "",
              size: parseSize(title),
              fulfillment: fulfillmentParts.join(", "),
            });
          }
        }
      }
    } catch (err) {
      console.error(`[safeway:${store.storeId}] Error searching "${query}": ${err.message}`);
    }
    await sleep(750);
  }
  return dedupFound(found);
}

// ─── Retailer Registry ───────────────────────────────────────────────────────
// scrapeOnce: results are identical across stores (no store-specific URL/cookie).
//   Scrape once and broadcast results to all stores.
// needsPage: scraper accepts a shared Playwright page as second arg (browser-based).
//   If false, scraper is API/fetch-based and doesn't need a browser page.

const RETAILERS = [
  { key: "costco",    name: "Costco",     scrapeOnce: true,  needsPage: true  },
  { key: "totalwine", name: "Total Wine", scrapeOnce: false, needsPage: true,  scraper: scrapeTotalWineStore },
  { key: "walmart",   name: "Walmart",    scrapeOnce: false, needsPage: false, scraper: scrapeWalmartStore },
  { key: "kroger",    name: "Kroger",     scrapeOnce: false, needsPage: false, scraper: scrapeKrogerStore },
  { key: "safeway",   name: "Safeway",    scrapeOnce: false, needsPage: false, scraper: scrapeSafewayStore },
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
  const scanStart = Date.now();
  console.log(`[poll] Starting scan at ${new Date().toISOString()}`);

  const state = await loadState();
  let storesScanned = 0;
  let totalNewFinds = 0;
  let totalStillInStock = 0;
  let totalGoneOOS = 0;
  let nothingCount = 0;
  const retailersSeen = new Set();

  // Reset per-poll state
  krogerToken = null;

  await launchBrowser();

  // Helper to record results for a store and send alerts
  async function recordResult(retailer, store, inStock) {
    const previousStore = state[retailer.key]?.[store.storeId];
    const changes = computeChanges(previousStore, inStock);
    updateStoreState(state, retailer.key, store.storeId, inStock);
    storesScanned++;

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
    retailersSeen.add(retailer.key);

    if (retailer.scrapeOnce) {
      // Costco: scrape once, broadcast results to all stores
      tasks.push(async () => {
        try {
          console.log(`[poll] Checking ${retailer.name} (once for ${stores.length} stores)...`);
          const page = await newPage();
          const inStock = await scrapeCostcoOnce(page);
          await page.context().close();
          for (const store of stores) {
            await recordResult(retailer, store, inStock);
          }
        } catch (err) {
          console.error(`[poll] ${retailer.name} crashed: ${err.message}`);
        }
      });
    } else if (retailer.needsPage) {
      // Browser-based per-store scrapers: share one context per retailer
      for (const store of stores) {
        tasks.push(async () => {
          try {
            console.log(`[poll] Checking ${retailer.name} — ${store.name}...`);
            const page = await newPage();
            const inStock = await retailer.scraper(store, page);
            await page.context().close();
            await recordResult(retailer, store, inStock);
          } catch (err) {
            console.error(`[poll] ${retailer.name} (${store.name}) crashed: ${err.message}`);
          }
        });
      }
    } else {
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

  // Run all stores concurrently (limit 4 to manage browser memory)
  await runWithConcurrency(tasks, 4);

  await closeBrowser();
  await saveState(state);

  // Quiet summary at end of every poll
  const durationSec = Math.round((Date.now() - scanStart) / 1000);
  const summary = buildSummaryEmbed({ storesScanned, retailersScanned: retailersSeen.size, totalNewFinds, totalStillInStock, totalGoneOOS, nothingCount, durationSec });
  await sendDiscordAlert([summary]);

  polling = false;
  console.log(`[poll] Scan complete — ${storesScanned} stores, ${totalNewFinds} new, ${totalStillInStock} still, ${totalGoneOOS} OOS, ${durationSec}s\n`);
}

// ─── Exports (for testing) ────────────────────────────────────────────────────

export {
  SEARCH_QUERIES, TARGET_BOTTLES, RETAILERS, FETCH_HEADERS,
  parseSize, matchesBottle, dedupFound, runWithConcurrency, matchWalmartNextData,
  COLORS, SKU_LABELS, formatStoreInfo, parseCity, parseState, timeAgo,
  formatBottleLine, buildOOSList, truncateDescription, DISCORD_DESC_LIMIT, buildStoreEmbeds, buildSummaryEmbed,
  loadState, saveState, computeChanges, updateStoreState,
  postDiscordWebhook, sendDiscordAlert, sendUrgentAlert,
  launchBrowser, closeBrowser, newPage,
  scrapeCostcoOnce, scrapeTotalWineStore,
  scrapeWalmartViaFetch, scrapeWalmartViaBrowser, scrapeWalmartStore,
  getKrogerToken, scrapeKrogerStore, scrapeSafewayStore,
  poll,
};

// Test helpers for setting module-level state
export function _setStoreCache(cache) { storeCache = cache; }
export function _resetPolling() { polling = false; }
export function _resetKrogerToken() { krogerToken = null; }

// ─── Entry Point ─────────────────────────────────────────────────────────────

async function main() {
  console.log("Bourbon Scout 🥃 starting up...");

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
