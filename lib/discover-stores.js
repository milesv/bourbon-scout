import fetch from "node-fetch";
import { chromium } from "playwright-extra";
import StealthPlugin from "puppeteer-extra-plugin-stealth";
import { readFile, writeFile } from "node:fs/promises";
import { zipToCoords, haversine } from "./geo.js";
import { FALLBACK_STORES } from "./fallback-stores.js";

chromium.use(StealthPlugin());

const CACHE_FILE = new URL("../stores.json", import.meta.url);
const CACHE_TTL_DAYS = 7;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ─── Cache Management ─────────────────────────────────────────────────────────

async function loadCache() {
  try {
    const raw = await readFile(CACHE_FILE, "utf-8");
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

async function saveCache(data) {
  await writeFile(CACHE_FILE, JSON.stringify(data, null, 2));
}

function isCacheValid(cache, zipCode, radiusMiles) {
  if (!cache) return false;
  if (cache.zipCode !== zipCode || cache.radiusMiles !== radiusMiles) return false;
  const age = Date.now() - new Date(cache.discoveredAt).getTime();
  return age < CACHE_TTL_DAYS * 24 * 60 * 60 * 1000;
}

// ─── Per-Retailer Store Locators ──────────────────────────────────────────────

async function locateCostco(page, zip, coords, radiusMiles, maxStores) {
  const stores = [];
  try {
    await page.goto(`https://www.costco.com/warehouse-locations?langId=-1&zipCode=${zip}`, {
      waitUntil: "domcontentloaded",
      timeout: 30000,
    });
    await page.waitForTimeout(5000);

    // Costco uses MUI. Each warehouse tile has a data-testid link containing the name/distance,
    // and an expandable details section with the address.
    // Name+distance: h4[data-testid="Text_warehousetile-nameanddistance-name"]
    // Distance: span[data-testid="Text_warehousetile-nameanddistance-distance"]
    // Address line: div[data-testid="Text_warehousetile-seewarehousedetails-address-line2"]
    // Store link: a[href*="/w/-/"] containing the warehouse slug with ID
    const results = await page.evaluate(() => {
      const items = [];
      const nameEls = document.querySelectorAll('[data-testid="Text_warehousetile-nameanddistance-name"]');
      nameEls.forEach((nameEl) => {
        const tile = nameEl.closest('[class*="MuiBox-root"]')?.parentElement?.closest('[class*="MuiBox-root"]');
        const name = nameEl.textContent?.trim() || "";
        const distEl = nameEl.parentElement?.querySelector('[data-testid="Text_warehousetile-nameanddistance-distance"]');
        const distance = distEl?.textContent?.trim() || "";
        // Find the link which has the warehouse number in the URL, e.g. /w/-/az/chandler/736
        const link = nameEl.closest("a") || nameEl.parentElement?.closest("a");
        const href = link?.getAttribute("href") || "";
        const idMatch = href.match(/\/(\d+)$/);
        const id = idMatch ? idMatch[1] : "";
        // Address is in the expandable details section nearby
        const parent = nameEl.closest('[class*="MuiBox-root"]')?.parentElement?.parentElement?.parentElement;
        const addrEl = parent?.querySelector('[data-testid="Text_warehousetile-seewarehousedetails-address-line2"]');
        const address = addrEl?.textContent?.trim() || "";
        items.push({ name, distance, id, address });
      });
      return items;
    });

    for (const r of results) {
      if (stores.length >= maxStores) break;
      const dist = parseFloat(r.distance.replace(/[()mi\s]/g, "")) || null;
      if (dist && dist > radiusMiles) continue;
      if (r.name) {
        stores.push({
          storeId: r.id || `costco-${stores.length}`,
          name: `Costco ${r.name}`,
          address: r.address,
          distanceMiles: dist,
        });
      }
    }
  } catch (err) {
    console.error(`[discover] Costco locator failed: ${err.message}`);
  }
  return stores;
}

async function locateTotalWine(page, zip, coords, radiusMiles, maxStores) {
  const stores = [];
  try {
    await page.goto(`https://www.totalwine.com/store-finder?searchAddress=${zip}`, {
      waitUntil: "domcontentloaded",
      timeout: 30000,
    });
    await page.waitForTimeout(5000);

    // Total Wine uses CSS module classes like storeListItem__*, storeName__*, storeAddress__*.
    // Store data is also in JSON-LD <script type="application/ld+json"> with LiquorStore entries.
    // We'll parse JSON-LD first (more reliable), fall back to DOM.
    const results = await page.evaluate(() => {
      const items = [];
      // Try JSON-LD structured data
      const scripts = document.querySelectorAll('script[type="application/ld+json"]');
      for (const script of scripts) {
        try {
          const data = JSON.parse(script.textContent);
          const graphs = Array.isArray(data) ? data : [data];
          for (const item of graphs) {
            const graph = item["@graph"] || [item];
            for (const entry of graph) {
              if (entry["@type"] === "LiquorStore" && entry.address) {
                const addr = entry.address;
                const urlMatch = entry.url?.match(/\/(\d+)$/);
                items.push({
                  name: entry.name || "",
                  address: [addr.streetAddress, addr.addressLocality, addr.addressRegion, addr.postalCode].filter(Boolean).join(", "),
                  id: urlMatch ? urlMatch[1] : "",
                  lat: entry.geo?.latitude,
                  lng: entry.geo?.longitude,
                });
              }
            }
          }
        } catch {}
      }
      // Fallback: parse DOM store cards
      if (items.length === 0) {
        const cards = document.querySelectorAll('[class*="storeListItem"]');
        cards.forEach((card) => {
          const nameEl = card.querySelector('[class*="storeName"]');
          const addrEl = card.querySelector('[class*="storeAddress"]');
          const linkEl = card.querySelector('a[href*="store-info"]');
          const idMatch = linkEl?.href?.match(/\/(\d+)/);
          items.push({
            name: nameEl?.textContent?.trim() || "",
            address: addrEl?.textContent?.trim() || "",
            id: idMatch ? idMatch[1] : "",
            lat: null,
            lng: null,
          });
        });
      }
      return items;
    });

    for (const r of results) {
      if (stores.length >= maxStores) break;
      let dist = null;
      if (r.lat && r.lng) {
        dist = Math.round(haversine(coords.lat, coords.lng, r.lat, r.lng) * 10) / 10;
      }
      if (dist && dist > radiusMiles) continue;
      if (r.name) {
        stores.push({
          storeId: r.id || `tw-${stores.length}`,
          name: `Total Wine ${r.name}`,
          address: r.address,
          distanceMiles: dist,
        });
      }
    }
  } catch (err) {
    console.error(`[discover] Total Wine locator failed: ${err.message}`);
  }
  return stores;
}

async function locateWalmart(page, zip, coords, radiusMiles, maxStores) {
  const stores = [];
  try {
    await page.goto(`https://www.walmart.com/store/finder?location=${zip}`, {
      waitUntil: "domcontentloaded",
      timeout: 30000,
    });
    await page.waitForTimeout(5000);

    // Walmart store finder renders store cards with:
    // - h3.f4.lh-copy.b.ma0 for store name (e.g., "Tempe East Southern Avenue Supercenter")
    // - p.ma0.lh-copy for "Walmart Supercenter #5768" and address
    // - "X.XX miles away" text
    // - a[href="/store/XXXX"] for store ID
    const results = await page.evaluate(() => {
      const items = [];
      // Each store lives in a container with a "Store details" link
      const storeLinks = document.querySelectorAll('a[href^="/store/"]');
      const seen = new Set();
      for (const link of storeLinks) {
        const idMatch = link.href.match(/\/store\/(\d+)/);
        if (!idMatch || seen.has(idMatch[1])) continue;
        seen.add(idMatch[1]);

        // Walk up to the store card container
        const card = link.closest('[class*="relative z-2 bg-white"]') ||
                     link.closest('[class*="flex-1"]') ||
                     link.parentElement?.parentElement?.parentElement;
        if (!card) continue;

        const h3 = card.querySelector("h3");
        const ps = card.querySelectorAll("p.ma0");
        const distText = card.textContent || "";
        const distMatch = distText.match(/([\d.]+)\s*miles?\s*away/);

        items.push({
          name: h3?.textContent?.trim() || "",
          storeId: idMatch[1],
          storeLabel: ps[0]?.textContent?.trim() || "",
          address: ps[1]?.textContent?.trim() || "",
          distance: distMatch ? parseFloat(distMatch[1]) : null,
        });
      }
      return items;
    });

    for (const r of results) {
      if (stores.length >= maxStores) break;
      if (r.distance && r.distance > radiusMiles) continue;
      if (r.name) {
        stores.push({
          storeId: r.storeId,
          name: r.storeLabel || r.name,
          address: r.address,
          distanceMiles: r.distance ? Math.round(r.distance * 10) / 10 : null,
        });
      }
    }
  } catch (err) {
    console.error(`[discover] Walmart locator failed: ${err.message}`);
  }
  return stores;
}

async function locateKroger(zip, coords, radiusMiles, maxStores, clientId, clientSecret) {
  if (!clientId || !clientSecret) {
    console.warn("[discover] Kroger skipped — no API credentials");
    return [];
  }

  let token;
  try {
    const authHeader = Buffer.from(`${clientId}:${clientSecret}`).toString("base64");
    const tokenRes = await fetch("https://api.kroger.com/v1/connect/oauth2/token", {
      method: "POST",
      headers: {
        Authorization: `Basic ${authHeader}`,
        "Content-Type": "application/x-www-form-urlencoded",
      },
      body: "grant_type=client_credentials&scope=product.compact",
    });
    if (!tokenRes.ok) throw new Error(`OAuth HTTP ${tokenRes.status}`);
    token = (await tokenRes.json()).access_token;
  } catch (err) {
    console.error(`[discover] Kroger OAuth failed: ${err.message}`);
    return [];
  }

  const stores = [];
  try {
    const url = `https://api.kroger.com/v1/locations?filter.zipCode.near=${zip}&filter.radiusInMiles=${radiusMiles}&filter.limit=${maxStores}`;
    const res = await fetch(url, {
      headers: { Authorization: `Bearer ${token}`, Accept: "application/json" },
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();

    for (const loc of data.data || []) {
      if (stores.length >= maxStores) break;
      const addr = loc.address || {};
      let dist = null;
      if (loc.geolocation?.latitude && loc.geolocation?.longitude) {
        dist = Math.round(haversine(coords.lat, coords.lng, loc.geolocation.latitude, loc.geolocation.longitude) * 10) / 10;
      }
      if (dist && dist > radiusMiles) continue;
      stores.push({
        storeId: String(loc.locationId),
        name: loc.name || `Kroger #${loc.locationId}`,
        address: [addr.addressLine1, addr.city, addr.state, addr.zipCode].filter(Boolean).join(", "),
        distanceMiles: dist,
      });
    }
  } catch (err) {
    console.error(`[discover] Kroger locator failed: ${err.message}`);
  }
  return stores;
}

async function locateSafeway(page, zip, coords, radiusMiles, maxStores) {
  const stores = [];
  try {
    // Safeway's store locator is on local.safeway.com (Yext-powered, JS-rendered)
    await page.goto(`https://local.safeway.com/search.html?q=${zip}`, {
      waitUntil: "networkidle",
      timeout: 45000,
    });
    await page.waitForTimeout(5000);

    // Yext renders store results as links/cards. Look for location result items.
    const results = await page.evaluate(() => {
      const items = [];
      // Yext typically renders results in a list. Look for common patterns.
      const cards = document.querySelectorAll(
        '[data-ya-scope="result"], .result, .LocationCard, [class*="result"], a[href*="/safeway/"]'
      );
      for (const card of cards) {
        const nameEl = card.querySelector('[class*="name"], [class*="title"], h2, h3, .location-name');
        const addrEl = card.querySelector('[class*="address"], address, .location-address');
        const distEl = card.querySelector('[class*="distance"], [class*="miles"]');
        const href = card.tagName === "A" ? card.href : card.querySelector("a")?.href || "";
        // Try to extract store ID from the URL pattern like /safeway/az/tempe/1234-e-broadway
        const idMatch = href.match(/\/safeway\/[^/]+\/[^/]+\/(\d+)/);
        items.push({
          name: nameEl?.textContent?.trim() || card.textContent?.trim()?.substring(0, 60) || "",
          address: addrEl?.textContent?.trim() || "",
          id: idMatch?.[1] || "",
          distance: distEl?.textContent?.trim() || "",
          href,
        });
      }
      return items;
    });

    for (const r of results) {
      if (stores.length >= maxStores) break;
      const dist = parseFloat(r.distance) || null;
      if (dist && dist > radiusMiles) continue;
      if (r.name && r.name.length > 2) {
        stores.push({
          storeId: r.id || `sw-${stores.length}`,
          name: `Safeway ${r.name}`,
          address: r.address,
          distanceMiles: dist,
        });
      }
    }
  } catch (err) {
    console.error(`[discover] Safeway locator failed: ${err.message}`);
  }
  return stores;
}

async function locateBevMo(page, zip, coords, radiusMiles, maxStores) {
  const stores = [];
  try {
    await page.goto("https://www.bevmo.com/pages/store-locator", {
      waitUntil: "domcontentloaded",
      timeout: 30000,
    });
    await page.waitForTimeout(3000);

    // BevMo's store locator has an input labeled "Enter city, state, or zip code"
    // and a "Distance away" dropdown. The page lists all stores alphabetically by default.
    // We need to enter the zip and change the distance filter.
    const input = await page.$('input[placeholder*="city, state, or zip"]');
    if (input) {
      await input.fill(zip);
      await input.press("Enter");
      await page.waitForTimeout(5000);
    } else {
      console.warn("[discover] BevMo: could not find zip input");
    }

    // BevMo renders store cards as divs with store name as bold text,
    // address lines, phone, and a "View store details" link.
    // Store link pattern: /pages/stores/{store-slug}
    const results = await page.evaluate(() => {
      const items = [];
      const links = document.querySelectorAll('a[href*="/pages/stores/"]');
      for (const link of links) {
        // Walk up to the card container
        const card = link.closest("div") || link.parentElement;
        if (!card) continue;
        const parent = card.parentElement;
        if (!parent) continue;
        const text = parent.textContent || "";
        // Store name is typically in bold/h2/h3 before the address
        const nameEl = parent.querySelector("h2, h3, strong, b, [class*='bold']");
        const name = nameEl?.textContent?.trim() || "";
        if (!name || name === "View store details →") continue;
        // Address is text lines between name and phone
        const allText = parent.innerText?.split("\n").map(l => l.trim()).filter(Boolean) || [];
        const slug = link.href.match(/\/pages\/stores\/([^/?]+)/)?.[1] || "";
        items.push({ name, slug, allText });
      }
      // Deduplicate by slug
      const seen = new Set();
      return items.filter(i => {
        if (seen.has(i.slug)) return false;
        seen.add(i.slug);
        return true;
      });
    });

    // BevMo doesn't show distance in results by default, so we'd need coords
    // to filter. For now, take the results as-is (the search should already filter by proximity).
    for (const r of results) {
      if (stores.length >= maxStores) break;
      // Extract address from allText (typically lines 1-2 after name)
      const nameIdx = r.allText.findIndex(l => l === r.name);
      const addrLines = r.allText.slice(nameIdx + 1, nameIdx + 3).filter(l => !l.includes("(") && !l.includes("View"));
      stores.push({
        storeId: r.slug || `bm-${stores.length}`,
        name: `BevMo ${r.name}`,
        address: addrLines.join(", "),
        distanceMiles: null,
      });
    }
  } catch (err) {
    console.error(`[discover] BevMo locator failed: ${err.message}`);
  }
  return stores;
}

// ─── Main Discovery Orchestrator ──────────────────────────────────────────────

export async function discoverStores({ zipCode, radiusMiles = 15, maxStores = 5, krogerClientId, krogerClientSecret }) {
  // Check cache first
  const cache = await loadCache();
  if (isCacheValid(cache, zipCode, radiusMiles)) {
    const total = Object.values(cache.retailers).reduce((sum, arr) => sum + arr.length, 0);
    console.log(`[discover] Using cached stores (${total} stores from ${cache.discoveredAt})`);
    return cache;
  }

  console.log(`[discover] Discovering stores near ${zipCode} within ${radiusMiles} miles...`);
  const coords = await zipToCoords(zipCode);
  console.log(`[discover] Zip ${zipCode} → ${coords.lat}, ${coords.lng}`);

  const retailers = {};

  // Run Kroger API locator in parallel with browser-based locators
  const krogerPromise = locateKroger(zipCode, coords, radiusMiles, maxStores, krogerClientId, krogerClientSecret);

  // Browser-based locators run sequentially on a shared browser
  const browser = await chromium.launch({ headless: true });
  try {
    const context = await browser.newContext({
      viewport: { width: 1366, height: 768 },
      locale: "en-US",
    });
    const page = await context.newPage();

    const browserLocators = [
      { key: "costco", fn: locateCostco },
      { key: "totalwine", fn: locateTotalWine },
      { key: "walmart", fn: locateWalmart },
      { key: "safeway", fn: locateSafeway },
      { key: "bevmo", fn: locateBevMo },
    ];

    for (const { key, fn } of browserLocators) {
      console.log(`[discover] Locating ${key} stores...`);
      retailers[key] = await fn(page, zipCode, coords, radiusMiles, maxStores);
      if (retailers[key].length === 0 && FALLBACK_STORES[key]?.length > 0) {
        retailers[key] = FALLBACK_STORES[key];
        console.log(`[discover] Locator returned 0 — using ${retailers[key].length} fallback ${key} store(s)`);
      } else {
        console.log(`[discover] Found ${retailers[key].length} ${key} store(s)`);
      }
      await sleep(1000);
    }

    await context.close();
  } finally {
    await browser.close();
  }

  // Collect Kroger results
  retailers.kroger = await krogerPromise;
  if (retailers.kroger.length === 0 && FALLBACK_STORES.kroger?.length > 0) {
    retailers.kroger = FALLBACK_STORES.kroger;
    console.log(`[discover] Kroger API returned 0 — using ${retailers.kroger.length} fallback store(s)`);
  } else {
    console.log(`[discover] Found ${retailers.kroger.length} kroger store(s)`);
  }

  const result = {
    discoveredAt: new Date().toISOString(),
    zipCode,
    radiusMiles,
    retailers,
  };

  await saveCache(result);
  const total = Object.values(retailers).reduce((sum, arr) => sum + arr.length, 0);
  console.log(`[discover] Discovery complete — ${total} stores cached to stores.json`);
  return result;
}
