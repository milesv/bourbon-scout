import fetch from "node-fetch";
import { chromium as rebrowserChromium } from "rebrowser-playwright-core";
import { addExtra } from "playwright-extra";
import StealthPlugin from "puppeteer-extra-plugin-stealth";
import { readFile, writeFile, rename } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { zipToCoords, haversine } from "./geo.js";
import { FALLBACK_STORES } from "./fallback-stores.js";

const chromium = addExtra(rebrowserChromium);
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
  const tmp = fileURLToPath(CACHE_FILE) + ".tmp";
  await writeFile(tmp, JSON.stringify(data, null, 2));
  await rename(tmp, fileURLToPath(CACHE_FILE));
}

function isCacheValid(cache, zipCode, radiusMiles) {
  if (!cache) return false;
  if (cache.zipCode !== zipCode || cache.radiusMiles !== radiusMiles) return false;
  const age = Date.now() - new Date(cache.discoveredAt).getTime();
  return age < CACHE_TTL_DAYS * 24 * 60 * 60 * 1000;
}

/** Sanitize store names loaded from cache (strips distance text, deduplicates, filters junk entries). */
function cleanStoreEntries(retailers) {
  for (const [key, stores] of Object.entries(retailers)) {
    const seen = new Set();
    retailers[key] = stores.filter((s) => {
      // Filter out non-store entries like "20 locations near ..."
      if (/\d+\s*locations?\s*near/i.test(s.name)) return false;
      // Drop synthetic IDs (sw-0, sw-1) — these are duplicates without real store IDs
      if (/^sw-\d+$/.test(s.storeId)) return false;
      // Deduplicate by storeId
      if (seen.has(s.storeId)) return false;
      seen.add(s.storeId);
      return true;
    }).map((s) => ({
      ...s,
      name: s.name
        // Strip distance text like "1.83 mi to your search"
        .replace(/[\d.]+\s*mi\b.*$/i, "")
        // Normalize non-breaking spaces
        .replace(/\u00a0/g, " ")
        // Fix double retailer prefix ("Safeway Safeway ..." → "Safeway ...")
        .replace(/^(Safeway|Costco|Walmart|Total Wine|Walgreens)\s+\1\s+/i, "$1 ")
        .trim(),
    }));
  }
  return retailers;
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
    const results = await page.evaluate(
      /* v8 ignore start -- browser-only DOM callback */
      () => {
        const items = [];
        const nameEls = document.querySelectorAll('[data-testid="Text_warehousetile-nameanddistance-name"]');
        nameEls.forEach((nameEl) => {
          const tile = nameEl.closest('[class*="MuiBox-root"]')?.parentElement?.closest('[class*="MuiBox-root"]');
          const name = nameEl.textContent?.trim() || "";
          const distEl = nameEl.parentElement?.querySelector('[data-testid="Text_warehousetile-nameanddistance-distance"]');
          const distance = distEl?.textContent?.trim() || "";
          const link = nameEl.closest("a") || nameEl.parentElement?.closest("a");
          const href = link?.getAttribute("href") || "";
          const idMatch = href.match(/\/(\d+)$/);
          const id = idMatch ? idMatch[1] : "";
          const parent = nameEl.closest('[class*="MuiBox-root"]')?.parentElement?.parentElement?.parentElement;
          const addrEl = parent?.querySelector('[data-testid="Text_warehousetile-seewarehousedetails-address-line2"]');
          const address = addrEl?.textContent?.trim() || "";
          items.push({ name, distance, id, address });
        });
        return items;
      }
      /* v8 ignore stop */
    );

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
    const results = await page.evaluate(
      /* v8 ignore start -- browser-only DOM callback */
      () => {
        const items = [];
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
      }
      /* v8 ignore stop */
    );

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
    const results = await page.evaluate(
      /* v8 ignore start -- browser-only DOM callback */
      () => {
        const items = [];
        const storeLinks = document.querySelectorAll('a[href^="/store/"]');
        const seen = new Set();
        for (const link of storeLinks) {
          const idMatch = link.href.match(/\/store\/(\d+)/);
          if (!idMatch || seen.has(idMatch[1])) continue;
          seen.add(idMatch[1]);
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
      }
      /* v8 ignore stop */
    );

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
      signal: AbortSignal.timeout(15000),
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
      signal: AbortSignal.timeout(15000),
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
    const results = await page.evaluate(
      /* v8 ignore start -- browser-only DOM callback */
      () => {
        const items = [];
        const cards = document.querySelectorAll(
          '[data-ya-scope="result"], .LocationCard, a[href*="/safeway/"]'
        );
        for (const card of cards) {
          const nameEl = card.querySelector('[class*="name"], [class*="title"], h2, h3, .location-name');
          const addrEl = card.querySelector('[class*="address"], address, .location-address');
          const distEl = card.querySelector('[class*="distance"], [class*="miles"]');
          const href = card.tagName === "A" ? card.href : card.querySelector("a")?.href || "";
          const idMatch = href.match(/\/safeway\/[^/]+\/[^/]+\/(\d+)/);
          // Only use nameEl text (not full card textContent) to avoid grabbing distance/metadata
          const name = nameEl?.textContent?.trim() || "";
          items.push({
            name,
            address: addrEl?.textContent?.trim() || "",
            id: idMatch?.[1] || "",
            distance: distEl?.textContent?.trim() || "",
            href,
          });
        }
        return items;
      }
      /* v8 ignore stop */
    );

    const seen = new Set();
    for (const r of results) {
      if (stores.length >= maxStores) break;
      const dist = parseFloat(r.distance) || null;
      if (dist && dist > radiusMiles) continue;
      // Skip non-store entries (e.g. "20 locations near ...")
      if (!r.name || r.name.length <= 2 || /\d+\s*locations?\s*near/i.test(r.name)) continue;
      // Strip trailing distance text like "1.83 mi to your search"
      const cleanName = r.name.replace(/[\d.]+\s*mi\b.*$/i, "").replace(/\u00a0/g, " ").trim();
      if (!cleanName) continue;
      // Deduplicate by storeId
      const sid = r.id || `sw-${stores.length}`;
      if (r.id && seen.has(r.id)) continue;
      if (r.id) seen.add(r.id);
      stores.push({
        storeId: sid,
        name: cleanName.startsWith("Safeway") ? cleanName : `Safeway ${cleanName}`,
        address: r.address,
        distanceMiles: dist,
      });
    }
  } catch (err) {
    console.error(`[discover] Safeway locator failed: ${err.message}`);
  }
  return stores;
}

async function locateWalgreens(page, zip, coords, radiusMiles, maxStores) {
  const stores = [];
  try {
    await page.goto(`https://www.walgreens.com/storelocator/find.jsp?RequestType=locator&zip=${zip}`, {
      waitUntil: "domcontentloaded",
      timeout: 30000,
    });
    await page.waitForTimeout(5000);

    // Walgreens store locator renders store cards dynamically via JS.
    // Store detail links follow pattern: /store/walgreens-{address-slug}/id={storeNumber}
    const results = await page.evaluate(
      /* v8 ignore start -- browser-only DOM callback */
      () => {
        const items = [];
        const storeLinks = document.querySelectorAll('a[href*="/store/walgreens"]');
        const seen = new Set();
        for (const link of storeLinks) {
          const idMatch = link.href.match(/\/id=(\d+)/);
          if (!idMatch || seen.has(idMatch[1])) continue;
          seen.add(idMatch[1]);
          const card = link.closest('[class*="card"]') ||
                       link.closest("li") ||
                       link.parentElement?.parentElement?.parentElement;
          if (!card) continue;
          const nameEl = card.querySelector('h2, h3, [class*="storeName"], [class*="name"]');
          const addrEl = card.querySelector('[class*="address"], address, [class*="addr"]');
          const distText = card.textContent || "";
          const distMatch = distText.match(/([\d.]+)\s*mi/);
          items.push({
            storeId: idMatch[1],
            name: nameEl?.textContent?.trim() || "",
            address: addrEl?.textContent?.trim() || "",
            distance: distMatch ? parseFloat(distMatch[1]) : null,
          });
        }
        return items;
      }
      /* v8 ignore stop */
    );

    for (const r of results) {
      if (stores.length >= maxStores) break;
      if (r.distance && r.distance > radiusMiles) continue;
      if (!r.name) continue;
      stores.push({
        storeId: r.storeId,
        name: r.name.startsWith("Walgreens") ? r.name : `Walgreens ${r.name}`,
        address: r.address,
        distanceMiles: r.distance ? Math.round(r.distance * 10) / 10 : null,
      });
    }
  } catch (err) {
    console.error(`[discover] Walgreens locator failed: ${err.message}`);
  }
  return stores;
}

async function locateSamsClub(page, zip, coords, radiusMiles, maxStores) {
  const stores = [];
  try {
    await page.goto(`https://www.samsclub.com/club-finder?q=${zip}`, {
      waitUntil: "domcontentloaded",
      timeout: 30000,
    });
    await page.waitForTimeout(5000);

    const results = await page.evaluate(
      /* v8 ignore start -- browser-only DOM callback */
      () => {
        const items = [];
        // Sam's Club club-finder renders club cards with links to /club/{slug}/{id}
        const clubLinks = document.querySelectorAll('a[href*="/club/"]');
        const seen = new Set();
        for (const link of clubLinks) {
          const idMatch = link.href.match(/\/club\/[^/]+\/(\d+)/);
          if (!idMatch || seen.has(idMatch[1])) continue;
          seen.add(idMatch[1]);
          const card = link.closest('[class*="card"]') ||
                       link.closest("li") ||
                       link.parentElement?.parentElement?.parentElement;
          if (!card) continue;
          const nameEl = card.querySelector('h2, h3, [class*="clubName"], [class*="name"]');
          const addrEl = card.querySelector('[class*="address"], address, [class*="addr"]');
          const distText = card.textContent || "";
          const distMatch = distText.match(/([\d.]+)\s*mi/);
          items.push({
            storeId: idMatch[1],
            name: nameEl?.textContent?.trim() || "",
            address: addrEl?.textContent?.trim() || "",
            distance: distMatch ? parseFloat(distMatch[1]) : null,
          });
        }
        return items;
      }
      /* v8 ignore stop */
    );

    for (const r of results) {
      if (stores.length >= maxStores) break;
      if (r.distance && r.distance > radiusMiles) continue;
      if (!r.name) continue;
      stores.push({
        storeId: r.storeId,
        name: r.name.startsWith("Sam's Club") ? r.name : `Sam's Club ${r.name}`,
        address: r.address,
        distanceMiles: r.distance ? Math.round(r.distance * 10) / 10 : null,
      });
    }
  } catch (err) {
    console.error(`[discover] Sam's Club locator failed: ${err.message}`);
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
    const results = await page.evaluate(
      /* v8 ignore start -- browser-only DOM callback */
      () => {
        const items = [];
        const links = document.querySelectorAll('a[href*="/pages/stores/"]');
        for (const link of links) {
          const card = link.closest("div") || link.parentElement;
          if (!card) continue;
          const parent = card.parentElement;
          if (!parent) continue;
          const nameEl = parent.querySelector("h2, h3, strong, b, [class*='bold']");
          const name = nameEl?.textContent?.trim() || "";
          if (!name || name === "View store details →") continue;
          const allText = parent.innerText?.split("\n").map(l => l.trim()).filter(Boolean) || [];
          const slug = link.href.match(/\/pages\/stores\/([^/?]+)/)?.[1] || "";
          items.push({ name, slug, allText });
        }
        const seen = new Set();
        return items.filter(i => {
          if (seen.has(i.slug)) return false;
          seen.add(i.slug);
          return true;
        });
      }
      /* v8 ignore stop */
    );

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

// Exports for testing
export {
  loadCache, saveCache, isCacheValid, cleanStoreEntries,
  locateCostco, locateTotalWine, locateWalmart,
  locateKroger, locateSafeway, locateWalgreens, locateSamsClub, locateBevMo,
};

export async function discoverStores({ zipCode, radiusMiles = 15, maxStores = 5, krogerClientId, krogerClientSecret }) {
  // Check cache first
  const cache = await loadCache();
  if (isCacheValid(cache, zipCode, radiusMiles)) {
    cleanStoreEntries(cache.retailers);
    const total = Object.values(cache.retailers).reduce((sum, arr) => sum + arr.length, 0);
    console.log(`[discover] Using cached stores (${total} stores from ${cache.discoveredAt})`);
    return cache;
  }

  console.log(`[discover] Discovering stores near ${zipCode} within ${radiusMiles} miles...`);
  const coords = await zipToCoords(zipCode);
  console.log(`[discover] Zip ${zipCode} → ${coords.lat}, ${coords.lng}`);

  const retailers = {};

  // Run Kroger API locator in parallel with browser-based locators
  const krogerPromise = locateKroger(zipCode, coords, radiusMiles, maxStores, krogerClientId, krogerClientSecret)
    .catch((err) => { console.error(`[discover] Kroger locator failed: ${err.message}`); return []; });

  // Browser-based locators run sequentially on a shared browser.
  // Use system Chrome on Mac for authentic TLS fingerprint.
  const chromePath = process.platform === "darwin"
    ? "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
    : null;
  const launchOpts = { headless: true };
  if (chromePath) launchOpts.executablePath = chromePath;
  const browser = await chromium.launch(launchOpts);
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
      { key: "walgreens", fn: locateWalgreens },
      { key: "samsclub", fn: locateSamsClub },
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
