import "dotenv/config";
import fetch from "node-fetch";
import { chromium } from "playwright-extra";
import StealthPlugin from "puppeteer-extra-plugin-stealth";
import cron from "node-cron";
import { readFile, writeFile } from "node:fs/promises";
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
} = process.env;

const STATE_FILE = new URL("./state.json", import.meta.url);

// ─── Target Bottles ──────────────────────────────────────────────────────────
// Broad search queries that cover multiple bottles in a single page load.
// This reduces the number of requests from 18 per retailer to ~5.
const SEARCH_QUERIES = [
  "weller bourbon",
  "blantons bourbon",
  "pappy van winkle",
  "eh taylor bourbon",
  "king of kentucky bourbon",
  "eagle rare 17",
  "sazerac rye 18",
  "thomas handy sazerac",
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

// ─── Discord Alerts ──────────────────────────────────────────────────────────

async function sendDiscordAlert(embeds) {
  if (!DISCORD_WEBHOOK_URL) {
    console.warn("[discord] No DISCORD_WEBHOOK_URL set — skipping alert");
    return;
  }
  for (let i = 0; i < embeds.length; i += 4) {
    const batch = embeds.slice(i, i + 4);
    const res = await fetch(DISCORD_WEBHOOK_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ username: "Bourbon Scout 🥃", embeds: batch }),
    });
    if (!res.ok) {
      console.error(`[discord] Webhook failed: ${res.status} ${await res.text()}`);
    }
    if (i + 4 < embeds.length) await sleep(1000);
  }
}

// Loud alert with @everyone for in-stock finds
async function sendUrgentAlert(embeds) {
  if (!DISCORD_WEBHOOK_URL) return;
  for (let i = 0; i < embeds.length; i += 4) {
    const batch = embeds.slice(i, i + 4);
    const res = await fetch(DISCORD_WEBHOOK_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        username: "Bourbon Scout 🥃",
        content: "@everyone 🚨 **ALLOCATED BOURBON SPOTTED!**",
        allowed_mentions: { parse: ["everyone"] },
        embeds: batch,
      }),
    });
    if (!res.ok) {
      console.error(`[discord] Urgent alert failed: ${res.status} ${await res.text()}`);
    }
    if (i + 4 < embeds.length) await sleep(1000);
  }
}

function buildStoreEmbed(retailerName, store, inStock) {
  const allNames = TARGET_BOTTLES.map((b) => b.name);
  const inStockNames = inStock.map((b) => b.name);
  const outOfStock = allNames.filter((name) => !inStockNames.includes(name));
  const distStr = store.distanceMiles != null ? ` (${store.distanceMiles} mi)` : "";

  const addressLine = store.address
    ? `📍 [${store.address}](https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(store.address)})`
    : "📍 Address unknown";

  const inStockList = inStock.length > 0
    ? inStock.map((b) => {
        let line = b.url ? `> 🟢 [${b.name}](${b.url})` : `> 🟢 ${b.name}`;
        if (b.price) line += ` — ${b.price}`;
        return line;
      }).join("\n")
    : "> _None found_";
  const outOfStockStr = outOfStock.join(", ");

  return {
    title: `${retailerName} — ${store.name}${distStr}`,
    description: `${addressLine}\n\n**In Stock (${inStock.length}):**\n${inStockList}\n\n**Out of Stock (${outOfStock.length}):**\n${outOfStockStr}`,
    color: 0x2ecc71,
    footer: { text: `Bourbon Scout | ${retailerName}` },
    timestamp: new Date().toISOString(),
  };
}

function buildSummaryEmbed({ storesScanned, retailersScanned, totalFound, durationSec }) {
  const desc = totalFound > 0
    ? `Checked **${storesScanned}** stores across **${retailersScanned}** retailers in **${durationSec}s**\n\n🟢 **${totalFound} bottle(s) found** — see alerts above`
    : `Checked **${storesScanned}** stores across **${retailersScanned}** retailers in **${durationSec}s**\n\nNo allocated bottles found this scan.`;

  return {
    title: "Scan Complete",
    description: desc,
    color: totalFound > 0 ? 0x2ecc71 : 0x3498db,
    footer: { text: `Bourbon Scout | Schedule: ${POLL_INTERVAL}` },
    timestamp: new Date().toISOString(),
  };
}

// ─── Utility ─────────────────────────────────────────────────────────────────

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

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

async function scrapeCostcoStore(store) {
  const found = [];
  const page = await newPage();
  try {
    for (const query of SEARCH_QUERIES) {
      const url = `https://www.costco.com/CatalogSearch?dept=All&keyword=${encodeURIComponent(query)}`;
      try {
        await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 });
        await page.waitForTimeout(2000);

        const products = await page.$$eval(
          ".product-tile, [automation-id='productList'] .product",
          (tiles) =>
            tiles
              .filter((el) => !el.querySelector(".out-of-stock, .oos-overlay"))
              .map((el) => ({
                title: (el.querySelector(".description, .product-title, a[automation-id]") || {}).textContent || "",
                url: (el.querySelector("a.product-tile-link, a[automation-id], .description a, a") || {}).href || "",
                price: (el.querySelector(".price, [class*='price'], [automation-id*='price']") || {}).textContent?.trim() || "",
              }))
        );

        for (const p of products) {
          for (const bottle of TARGET_BOTTLES) {
            if (matchesBottle(p.title, bottle)) {
              found.push({ name: bottle.name, url: p.url, price: p.price });
            }
          }
        }
      } catch (err) {
        console.error(`[costco:${store.storeId}] Error searching "${query}": ${err.message}`);
      }
      await sleep(3000);
    }
  } finally {
    await page.context().close();
  }
  return dedupFound(found);
}

async function scrapeTotalWineStore(store) {
  const found = [];
  const page = await newPage();
  try {
    await page.context().addCookies([
      { name: "TWM_STORE_ID", value: store.storeId, domain: ".totalwine.com", path: "/" },
    ]);

    for (const query of SEARCH_QUERIES) {
      const url = `https://www.totalwine.com/search/all?text=${encodeURIComponent(query)}`;
      try {
        await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 });
        await page.waitForTimeout(3000);

        const products = await page.$$eval(
          '[class*="productCard"], [data-testid="product-card"], .product-card',
          (cards) =>
            cards.map((el) => ({
              title: (el.querySelector('[class*="title"], [class*="name"], h2, a') || {}).textContent || "",
              hasAddToCart: !!el.querySelector('button[class*="addToCart"], button[class*="Add"], [data-testid*="add"]'),
              url: (el.querySelector('a[href*="/spirits/"], a[href*="/wine/"], a') || {}).href || "",
              price: (el.querySelector('[class*="price"], [data-testid*="price"]') || {}).textContent?.trim() || "",
            }))
        );

        for (const p of products) {
          for (const bottle of TARGET_BOTTLES) {
            if (matchesBottle(p.title, bottle) && p.hasAddToCart) {
              found.push({ name: bottle.name, url: p.url, price: p.price });
            }
          }
        }
      } catch (err) {
        console.error(`[totalwine:${store.storeId}] Error searching "${query}": ${err.message}`);
      }
      await sleep(3000);
    }
  } finally {
    await page.context().close();
  }
  return dedupFound(found);
}

async function scrapeWalmartStore(store) {
  const found = [];
  const page = await newPage();
  try {
    for (const query of SEARCH_QUERIES) {
      const url = `https://www.walmart.com/search?q=${encodeURIComponent(query)}&cat_id=976759&store_id=${store.storeId}`;
      try {
        await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 });
        await page.waitForTimeout(3000);

        const nextData = await page.evaluate(() => {
          const el = document.querySelector("script#__NEXT_DATA__");
          if (el) try { return JSON.parse(el.textContent); } catch {}
          return null;
        });

        if (nextData) {
          const items =
            nextData?.props?.pageProps?.initialData?.searchResult?.itemStacks?.[0]?.items || [];
          for (const item of items) {
            const available =
              item.availabilityStatusV2?.value === "IN_STOCK" || item.canAddToCart === true;
            if (available && item.name) {
              for (const bottle of TARGET_BOTTLES) {
                if (matchesBottle(item.name, bottle)) {
                  found.push({
                    name: bottle.name,
                    url: item.canonicalUrl ? `https://www.walmart.com${item.canonicalUrl}` : "",
                    price: item.priceInfo?.currentPrice?.priceString || "",
                  });
                }
              }
            }
          }
        } else {
          const products = await page.$$eval(
            '[data-testid="list-view"] [data-item-id], .search-result-gridview-item',
            (items) =>
              items
                .filter((el) => !el.querySelector('[data-testid="out-of-stock"], .out-of-stock'))
                .map((el) => ({
                  title: (el.querySelector('[data-automation-id="product-title"], .product-title-link span') || {}).textContent || "",
                  url: (el.querySelector('a[href*="/ip/"]') || {}).href || "",
                  price: (el.querySelector('[data-automation-id="product-price"], [class*="price"]') || {}).textContent?.trim() || "",
                }))
          );
          for (const p of products) {
            for (const bottle of TARGET_BOTTLES) {
              if (matchesBottle(p.title, bottle)) {
                found.push({ name: bottle.name, url: p.url, price: p.price });
              }
            }
          }
        }
      } catch (err) {
        console.error(`[walmart:${store.storeId}] Error searching "${query}": ${err.message}`);
      }
      await sleep(3000);
    }
  } finally {
    await page.context().close();
  }
  return dedupFound(found);
}

async function scrapeKrogerStore(store) {
  if (!KROGER_CLIENT_ID || !KROGER_CLIENT_SECRET) {
    console.warn("[kroger] Skipping — KROGER_CLIENT_ID / KROGER_CLIENT_SECRET not set");
    return [];
  }

  let token;
  try {
    const authHeader = Buffer.from(
      `${KROGER_CLIENT_ID}:${KROGER_CLIENT_SECRET}`
    ).toString("base64");
    const tokenRes = await fetch("https://api.kroger.com/v1/connect/oauth2/token", {
      method: "POST",
      headers: {
        Authorization: `Basic ${authHeader}`,
        "Content-Type": "application/x-www-form-urlencoded",
      },
      body: "grant_type=client_credentials&scope=product.compact",
    });
    if (!tokenRes.ok) throw new Error(`HTTP ${tokenRes.status}`);
    token = (await tokenRes.json()).access_token;
  } catch (err) {
    console.error(`[kroger:${store.storeId}] OAuth failed: ${err.message}`);
    return [];
  }

  const found = [];
  for (const bottle of TARGET_BOTTLES) {
    const query = encodeURIComponent(bottle.searchTerms[0]);
    const url = `https://api.kroger.com/v1/products?filter.term=${query}&filter.locationId=${store.storeId}&filter.limit=5`;
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
        if (matchesBottle(title, bottle) && inStock) {
          const price = product.items?.[0]?.price?.regular;
          found.push({
            name: bottle.name,
            url: product.productId ? `https://www.kroger.com/p/${product.productId}` : "",
            price: price != null ? `$${price.toFixed(2)}` : "",
          });
        }
      }
    } catch (err) {
      console.error(`[kroger:${store.storeId}] Error searching "${bottle.name}": ${err.message}`);
    }
    await sleep(1000);
  }
  return dedupFound(found);
}

async function scrapeSafewayStore(store) {
  if (!SAFEWAY_API_KEY) {
    console.warn("[safeway] Skipping — SAFEWAY_API_KEY not set");
    return [];
  }

  const found = [];
  const baseUrl = "https://www.safeway.com/abs/pub/xapi/pgmsearch/v1/search/products";

  for (const bottle of TARGET_BOTTLES) {
    const query = encodeURIComponent(bottle.searchTerms[0]);
    const url = `${baseUrl}?request-id=0&url=https://www.safeway.com&pageurl=search&search-type=keyword&q=${query}&rows=10&start=0&storeid=${store.storeId}`;
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
        if (matchesBottle(title, bottle) && product.inStock !== false) {
          found.push({
            name: bottle.name,
            url: product.url ? `https://www.safeway.com${product.url}` : "",
            price: product.price != null ? `$${product.price}` : "",
          });
        }
      }
    } catch (err) {
      console.error(`[safeway:${store.storeId}] Error searching "${bottle.name}": ${err.message}`);
    }
    await sleep(1500);
  }
  return dedupFound(found);
}

async function scrapeBevMoStore(store) {
  const found = [];
  const page = await newPage();
  try {
    for (const query of SEARCH_QUERIES) {
      const url = `https://www.bevmo.com/search?q=${encodeURIComponent(query)}`;
      try {
        await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 });
        await page.waitForTimeout(3000);

        const products = await page.$$eval(
          '.product-tile, [data-component="product-tile"], .product, .grid-tile',
          (tiles) =>
            tiles
              .filter((el) => !el.querySelector(".out-of-stock, .sold-out, [class*='outOfStock'], .unavailable"))
              .map((el) => ({
                title: (el.querySelector(".product-name, .pdp-link a, .product-title, [class*='productName']") || {}).textContent || "",
                url: (el.querySelector("a.product-link, .pdp-link a, a[href*='/product'], a") || {}).href || "",
                price: (el.querySelector(".product-sales-price, [class*='price'], .price-sales") || {}).textContent?.trim() || "",
              }))
        );

        for (const p of products) {
          for (const bottle of TARGET_BOTTLES) {
            if (matchesBottle(p.title, bottle)) {
              found.push({ name: bottle.name, url: p.url, price: p.price });
            }
          }
        }
      } catch (err) {
        console.error(`[bevmo:${store.storeId}] Error searching "${query}": ${err.message}`);
      }
      await sleep(3000);
    }
  } finally {
    await page.context().close();
  }
  return dedupFound(found);
}

// ─── Retailer Registry ───────────────────────────────────────────────────────

const RETAILERS = [
  { key: "costco",    name: "Costco",     scraper: scrapeCostcoStore,    type: "browser" },
  { key: "totalwine", name: "Total Wine", scraper: scrapeTotalWineStore, type: "browser" },
  { key: "walmart",   name: "Walmart",    scraper: scrapeWalmartStore,   type: "browser" },
  { key: "kroger",    name: "Kroger",     scraper: scrapeKrogerStore,    type: "api" },
  { key: "safeway",   name: "Safeway",    scraper: scrapeSafewayStore,   type: "api" },
  { key: "bevmo",     name: "BevMo",      scraper: scrapeBevMoStore,     type: "browser" },
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
  let retailersScanned = 0;
  let totalFound = 0;

  // Per-store handler: scrape, update state, send urgent alert only if bottles found
  async function handleStore(retailer, store) {
    try {
      console.log(`[poll] Checking ${retailer.name} — ${store.name}...`);
      const inStock = await retailer.scraper(store);
      if (!state[retailer.key]) state[retailer.key] = {};
      state[retailer.key][store.storeId] = inStock.map((b) => b.name);
      storesScanned++;

      if (inStock.length > 0) {
        totalFound += inStock.length;
        console.log(`[${retailer.key}:${store.storeId}] 🟢 Found: ${inStock.map((b) => b.name).join(", ")}`);
        const embed = buildStoreEmbed(retailer.name, store, inStock);
        await sendUrgentAlert([embed]);
      } else {
        console.log(`[${retailer.key}:${store.storeId}] Nothing found`);
      }
    } catch (err) {
      console.error(`[poll] ${retailer.name} (${store.name}) crashed: ${err.message}`);
    }
  }

  const browserRetailers = RETAILERS.filter((r) => r.type === "browser");
  const apiRetailers = RETAILERS.filter((r) => r.type === "api");

  // Run browser-based scrapers sequentially (shared browser)
  const browserWork = async () => {
    await launchBrowser();
    try {
      for (const retailer of browserRetailers) {
        const stores = storeCache.retailers[retailer.key] || [];
        if (stores.length === 0) continue;
        retailersScanned++;
        for (const store of stores) {
          await handleStore(retailer, store);
        }
      }
    } finally {
      await closeBrowser();
    }
  };

  // Run API-based scrapers in parallel (one Promise per retailer, stores iterated within)
  const apiWork = async () => {
    const apiTasks = apiRetailers.map(async (retailer) => {
      const stores = storeCache.retailers[retailer.key] || [];
      if (stores.length === 0) return;
      retailersScanned++;
      for (const store of stores) {
        await handleStore(retailer, store);
      }
    });
    await Promise.all(apiTasks);
  };

  // Browser and API scrapers run concurrently
  await Promise.all([browserWork(), apiWork()]);

  await saveState(state);

  // Quiet summary at end of every poll
  const durationSec = Math.round((Date.now() - scanStart) / 1000);
  const summary = buildSummaryEmbed({ storesScanned, retailersScanned, totalFound, durationSec });
  await sendDiscordAlert([summary]);

  polling = false;
  console.log(`[poll] Scan complete — ${storesScanned} stores, ${totalFound} found, ${durationSec}s\n`);
}

// ─── Entry Point ─────────────────────────────────────────────────────────────

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
  poll().catch((err) => console.error("[startup] Initial poll failed:", err));

  cron.schedule(POLL_INTERVAL, () => {
    poll().catch((err) => console.error("[cron] Poll failed:", err));
  });
} catch (err) {
  console.error(`[startup] Store discovery failed: ${err.message}`);
  process.exit(1);
}
