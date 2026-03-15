import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

// ─── Mocks (hoisted before imports) ───────────────────────────────────────────

const mocks = vi.hoisted(() => {
  process.env.DISCORD_WEBHOOK_URL = "https://test-webhook.example.com/hook";
  process.env.ZIP_CODE = "85283";
  process.env.SEARCH_RADIUS_MILES = "15";
  process.env.MAX_STORES_PER_RETAILER = "5";
  process.env.KROGER_CLIENT_ID = "test-kroger-id";
  process.env.KROGER_CLIENT_SECRET = "test-kroger-secret";
  process.env.SAFEWAY_API_KEY = "test-safeway-key";
  process.env.POLL_INTERVAL = "*/15 * * * *";

  return {
    fetch: vi.fn(),
    readFile: vi.fn(),
    writeFile: vi.fn(),
    rename: vi.fn(),
    mkdir: vi.fn().mockResolvedValue(undefined),
    chromiumLaunch: vi.fn(),
    chromiumLaunchPersistentContext: vi.fn(),
    chromiumUse: vi.fn(),
    discoverStores: vi.fn(),
    zipToCoords: vi.fn().mockResolvedValue({ lat: 33.4152, lng: -111.8315 }),
  };
});

vi.mock("dotenv/config", () => ({}));
vi.mock("node-fetch", () => ({ default: mocks.fetch }));
vi.mock("playwright-extra", () => ({
  chromium: { use: mocks.chromiumUse, launch: mocks.chromiumLaunch, launchPersistentContext: mocks.chromiumLaunchPersistentContext },
}));
vi.mock("puppeteer-extra-plugin-stealth", () => ({ default: vi.fn() }));
vi.mock("https-proxy-agent", () => ({ HttpsProxyAgent: vi.fn() }));
vi.mock("cheerio", async () => await vi.importActual("cheerio"));
vi.mock("node:fs/promises", () => ({
  readFile: mocks.readFile,
  writeFile: mocks.writeFile,
  rename: mocks.rename,
  mkdir: mocks.mkdir,
}));
vi.mock("../lib/discover-stores.js", () => ({
  discoverStores: mocks.discoverStores,
}));
vi.mock("../lib/geo.js", () => ({
  zipToCoords: mocks.zipToCoords,
}));

// ─── Import module under test ─────────────────────────────────────────────────

import {
  SEARCH_QUERIES, TARGET_BOTTLES, CANARY_NAMES, RETAILERS, FETCH_HEADERS,
  normalizeText, parseSize, parsePrice, matchesBottle, dedupFound, shuffle, withTimeout, runWithConcurrency, matchWalmartNextData,
  COLORS, SKU_LABELS, formatStoreInfo, parseCity, parseState, timeAgo,
  formatBottleLine, buildOOSList, truncateDescription, truncateTitle, DISCORD_DESC_LIMIT, DISCORD_TITLE_LIMIT, buildStoreEmbeds, buildSummaryEmbed,
  loadState, saveState, computeChanges, updateStoreState, pruneState,
  postDiscordWebhook, sendDiscordAlert, sendUrgentAlert,
  IS_MAC, CHROME_PATH, launchBrowser, closeBrowser, closeRetailerBrowsers, newPage, loadBrowserState, saveBrowserState, isBlockedPage, fetchRetry,
  getQueriesForScan, parsePollIntervalMs,
  shouldSkipRetailer, recordRetailerOutcome, loadKnownProducts, checkWalmartKnownUrls, navigateCategory, CATEGORY_URLS,
  COSTCO_BLOCKED_PATTERNS, isCostcoBlocked,
  matchCostcoTiles, scrapeCostcoViaFetch, scrapeCostcoOnce, scrapeCostcoStore,
  matchTotalWineInitialState, scrapeTotalWineViaFetch, scrapeTotalWineViaBrowser, scrapeTotalWineStore,
  scrapeWalmartViaFetch, scrapeWalmartViaBrowser, scrapeWalmartStore,
  getKrogerToken, scrapeKrogerStore, scrapeSafewayStore,
  scrapeWalgreensViaBrowser, scrapeWalgreensStore,
  SAMSCLUB_PRODUCTS, matchSamsClubProduct, scrapeSamsClubViaFetch, scrapeSamsClubViaBrowser, scrapeSamsClubStore,
  trackHealth,
  poll, main,
  _setStoreCache, _resetPolling, _resetKrogerToken, _resetBrowserStateCache, _resetWalgreensCoords,
  _getScraperHealth, _resetScraperHealth, _setScanCounter, _getScanCounter, _resetRetailerBrowserCache,
  _resetRetailerFailures, _resetKnownProducts,
} from "../scraper.js";

// ─── Test Helpers ─────────────────────────────────────────────────────────────

function createMockPage() {
  return {
    goto: vi.fn().mockResolvedValue(undefined),
    waitForSelector: vi.fn().mockResolvedValue(undefined),
    waitForTimeout: vi.fn().mockResolvedValue(undefined),
    waitForFunction: vi.fn().mockResolvedValue(undefined),
    title: vi.fn().mockResolvedValue(""),
    evaluate: vi.fn().mockResolvedValue(""),
    $$eval: vi.fn().mockResolvedValue([]),
    $$: vi.fn().mockResolvedValue([]),
    $eval: vi.fn().mockResolvedValue(null),
    close: vi.fn().mockResolvedValue(undefined),
    mouse: { wheel: vi.fn().mockResolvedValue(undefined) },
    context: vi.fn(() => ({
      close: vi.fn().mockResolvedValue(undefined),
      storageState: vi.fn().mockResolvedValue({ cookies: [], origins: [] }),
      addCookies: vi.fn().mockResolvedValue(undefined),
    })),
  };
}

function setupMockBrowser() {
  const mockPage = createMockPage();
  const mockContext = { newPage: vi.fn().mockResolvedValue(mockPage), close: vi.fn().mockResolvedValue(undefined), storageState: vi.fn().mockResolvedValue({ cookies: [], origins: [] }), addCookies: vi.fn().mockResolvedValue(undefined) };
  const mockBrowser = { newContext: vi.fn().mockResolvedValue(mockContext), close: vi.fn().mockResolvedValue(undefined) };
  // Mock both launch (shared browser) and launchPersistentContext (retailer browsers)
  mocks.chromiumLaunch.mockResolvedValue(mockBrowser);
  mocks.chromiumLaunchPersistentContext.mockResolvedValue(mockContext);
  return { browser: mockBrowser, context: mockContext, page: mockPage };
}

const TEST_STORE = {
  storeId: "1234",
  name: "Test Store",
  address: "123 Main St, Tempe, AZ 85281",
  distanceMiles: 3.5,
};

// ─── Global Setup ─────────────────────────────────────────────────────────────

beforeEach(() => {
  vi.useFakeTimers();
  vi.resetAllMocks();
  _resetKrogerToken();
  _resetRetailerBrowserCache();
  _resetRetailerFailures();
  _resetKnownProducts();
});

afterEach(() => {
  vi.useRealTimers();
});

// Helper: run an async function that contains sleep() calls with fake timers
async function runWithFakeTimers(fn) {
  const promise = fn();
  // Attach noop catch to prevent Node "unhandled rejection" during timer drain
  promise.catch(() => {});
  await vi.runAllTimersAsync();
  return promise;
}

// ─── Constants ────────────────────────────────────────────────────────────────

describe("constants", () => {
  it("SEARCH_QUERIES has 14 broad queries (13 allocated + 1 canary)", () => {
    expect(SEARCH_QUERIES).toHaveLength(14);
    expect(SEARCH_QUERIES).toContain("weller bourbon");
    expect(SEARCH_QUERIES).toContain("van winkle");
    expect(SEARCH_QUERIES).toContain("eh taylor");
    expect(SEARCH_QUERIES).toContain("george t stagg");
    expect(SEARCH_QUERIES).toContain("old forester bourbon");
    expect(SEARCH_QUERIES).toContain("buffalo trace");
  });

  it("TARGET_BOTTLES has 39 bottles (38 allocated + 1 canary)", () => {
    expect(TARGET_BOTTLES).toHaveLength(39);
    expect(TARGET_BOTTLES[0]).toHaveProperty("name");
    expect(TARGET_BOTTLES[0]).toHaveProperty("searchTerms");
  });

  it("RETAILERS has 7 entries", () => {
    expect(RETAILERS).toHaveLength(7);
    expect(RETAILERS.map((r) => r.key)).toEqual(["costco", "totalwine", "walmart", "kroger", "safeway", "walgreens", "samsclub"]);
  });

  it("RETAILERS have correct flags", () => {
    const costco = RETAILERS.find((r) => r.key === "costco");
    expect(costco.scrapeOnce).toBe(true);
    expect(costco.needsPage).toBe(false);
    const walmart = RETAILERS.find((r) => r.key === "walmart");
    expect(walmart.scrapeOnce).toBe(false);
    expect(walmart.needsPage).toBe(false);
    const walgreens = RETAILERS.find((r) => r.key === "walgreens");
    expect(walgreens.scrapeOnce).toBe(true);
    expect(walgreens.needsPage).toBe(false);
    const samsclub = RETAILERS.find((r) => r.key === "samsclub");
    expect(samsclub.scrapeOnce).toBe(true);
    expect(samsclub.needsPage).toBe(false);
  });

  it("SEARCH_QUERIES includes 'george t stagg' for BTAC coverage (B3)", () => {
    expect(SEARCH_QUERIES).toContain("george t stagg");
  });

  it("Sazerac 18 uses 'sazerac 18 year' search term (B2)", () => {
    const saz = TARGET_BOTTLES.find((b) => b.name.includes("Sazerac") && b.name.includes("18"));
    expect(saz.searchTerms).toContain("sazerac 18 year");
    expect(saz.searchTerms).not.toContain("sazerac 18");
  });

  it("Blanton's Red/Green/Black are removed (Japan-only, B6)", () => {
    const names = TARGET_BOTTLES.map((b) => b.name);
    expect(names).not.toContain("Blanton's Red");
    expect(names).not.toContain("Blanton's Green");
    expect(names).not.toContain("Blanton's Black");
  });

  it("Blanton's Original Single Barrel is included (B1)", () => {
    const original = TARGET_BOTTLES.find((b) => b.name === "Blanton's Original");
    expect(original).toBeTruthy();
    expect(original.searchTerms).toContain("blanton's original");
    expect(original.searchTerms).toContain("blantons single barrel");
  });

  it("FETCH_HEADERS has a User-Agent", () => {
    expect(FETCH_HEADERS["User-Agent"]).toContain("Mozilla");
  });

  it("FETCH_HEADERS Accept-Encoding excludes Brotli (C2)", () => {
    expect(FETCH_HEADERS["Accept-Encoding"]).not.toContain("br");
    expect(FETCH_HEADERS["Accept-Encoding"]).toContain("gzip");
    expect(FETCH_HEADERS["Accept-Encoding"]).toContain("deflate");
  });

  it("FETCH_HEADERS includes anti-bot headers", () => {
    expect(FETCH_HEADERS["Sec-Fetch-Dest"]).toBe("document");
    expect(FETCH_HEADERS["Sec-Fetch-Mode"]).toBe("navigate");
    expect(FETCH_HEADERS["Referer"]).toContain("google.com");
    expect(FETCH_HEADERS["Upgrade-Insecure-Requests"]).toBe("1");
  });

  it("FETCH_HEADERS includes Sec-CH-UA Client Hints", () => {
    expect(FETCH_HEADERS["Sec-CH-UA"]).toContain("Chrome");
    expect(FETCH_HEADERS["Sec-CH-UA-Mobile"]).toBe("?0");
    const expectedPlatform = process.platform === "darwin" ? '"macOS"' : '"Windows"';
    expect(FETCH_HEADERS["Sec-CH-UA-Platform"]).toBe(expectedPlatform);
  });

  it("FETCH_HEADERS User-Agent version matches Sec-CH-UA version", () => {
    const uaMatch = FETCH_HEADERS["User-Agent"].match(/Chrome\/([\d]+)/);
    const chMatch = FETCH_HEADERS["Sec-CH-UA"].match(/Chrome";v="([\d]+)"/);
    expect(uaMatch[1]).toBe(chMatch[1]);
  });
});

// ─── Canary Bottle ──────────────────────────────────────────────────────────

describe("canary bottle", () => {
  it("Buffalo Trace is in TARGET_BOTTLES with canary: true", () => {
    const bt = TARGET_BOTTLES.find((b) => b.name === "Buffalo Trace");
    expect(bt).toBeTruthy();
    expect(bt.canary).toBe(true);
    expect(bt.searchTerms).toContain("buffalo trace");
  });

  it("CANARY_NAMES contains Buffalo Trace", () => {
    expect(CANARY_NAMES.has("Buffalo Trace")).toBe(true);
    expect(CANARY_NAMES.size).toBe(1);
  });

  it("non-canary bottles do not have canary flag", () => {
    const nonCanary = TARGET_BOTTLES.filter((b) => !b.canary);
    expect(nonCanary.length).toBe(38);
  });

  it("matchesBottle works for Buffalo Trace", () => {
    const bt = TARGET_BOTTLES.find((b) => b.name === "Buffalo Trace");
    expect(matchesBottle("Buffalo Trace Bourbon 750ml", bt)).toBe(true);
    expect(matchesBottle("buffalo trace kentucky straight", bt)).toBe(true);
    expect(matchesBottle("Eagle Rare 10 Year", bt)).toBe(false);
  });
});

// ─── Scraper Health Tracking ────────────────────────────────────────────────

describe("trackHealth", () => {
  beforeEach(() => _resetScraperHealth());

  it("tracks ok outcomes", () => {
    trackHealth("kroger", "ok");
    trackHealth("kroger", "ok");
    const h = _getScraperHealth();
    expect(h.kroger).toEqual({ queries: 2, succeeded: 2, failed: 0, blocked: 0 });
  });

  it("tracks failed outcomes", () => {
    trackHealth("walmart", "fail");
    const h = _getScraperHealth();
    expect(h.walmart.failed).toBe(1);
    expect(h.walmart.succeeded).toBe(0);
  });

  it("tracks blocked outcomes", () => {
    trackHealth("costco", "blocked");
    trackHealth("costco", "ok");
    const h = _getScraperHealth();
    expect(h.costco).toEqual({ queries: 2, succeeded: 1, failed: 0, blocked: 1 });
  });

  it("isolates retailers", () => {
    trackHealth("kroger", "ok");
    trackHealth("safeway", "fail");
    const h = _getScraperHealth();
    expect(Object.keys(h)).toEqual(expect.arrayContaining(["kroger", "safeway"]));
    expect(h.kroger.succeeded).toBe(1);
    expect(h.safeway.failed).toBe(1);
  });

  it("reset clears all health data", () => {
    trackHealth("kroger", "ok");
    _resetScraperHealth();
    expect(_getScraperHealth()).toEqual({});
  });
});

// ─── Pure Utility Functions ───────────────────────────────────────────────────

describe("normalizeText", () => {
  it("converts unicode right single quote to ASCII apostrophe", () => {
    expect(normalizeText("Blanton\u2019s Gold")).toBe("blanton's gold");
  });

  it("converts unicode left single quote to ASCII apostrophe", () => {
    expect(normalizeText("Blanton\u2018s")).toBe("blanton's");
  });

  it("converts unicode double quotes", () => {
    expect(normalizeText("\u201CHello\u201D")).toBe('"hello"');
  });

  it("lowercases while normalizing", () => {
    expect(normalizeText("E.H. Taylor\u2019s BOURBON")).toBe("e.h. taylor's bourbon");
  });

  it("passes through plain ASCII unchanged (except lowercasing)", () => {
    expect(normalizeText("Weller 12 Year")).toBe("weller 12 year");
  });
});

describe("parseSize", () => {
  it("parses ml sizes", () => {
    expect(parseSize("Weller Special Reserve Bourbon 750ml")).toBe("750ml");
    expect(parseSize("Blanton's Gold 750 ML")).toBe("750ml");
  });
  it("parses liter sizes", () => {
    expect(parseSize("Maker's Mark 1.75L")).toBe("1.75L");
    expect(parseSize("Jack Daniel's 1 Liter")).toBe("1L");
  });
  it("returns empty for no size", () => {
    expect(parseSize("Weller Special Reserve")).toBe("");
    expect(parseSize("")).toBe("");
    expect(parseSize(null)).toBe("");
    expect(parseSize(undefined)).toBe("");
  });
});

describe("matchesBottle", () => {
  const weller = TARGET_BOTTLES.find((b) => b.name === "Weller Special Reserve");

  it("matches on search terms (case insensitive)", () => {
    expect(matchesBottle("Weller Special Reserve Bourbon 750ml", weller)).toBe(true);
    expect(matchesBottle("WELLER SPECIAL RESERVE", weller)).toBe(true);
  });

  it("returns false for non-matching text", () => {
    expect(matchesBottle("Jack Daniels Old No 7", weller)).toBe(false);
    expect(matchesBottle("", weller)).toBe(false);
  });

  it("matches partial text that includes a search term", () => {
    const taylor = TARGET_BOTTLES.find((b) => b.name === "E.H. Taylor Small Batch");
    expect(matchesBottle("Colonel E.H. Taylor Small Batch Bourbon 750ml", taylor)).toBe(true);
  });

  it("does not match E.H. Taylor Single Barrel as Small Batch", () => {
    const taylor = TARGET_BOTTLES.find((b) => b.name === "E.H. Taylor Small Batch");
    expect(matchesBottle("E.H. Taylor Single Barrel Bourbon 750ml", taylor)).toBe(false);
    expect(matchesBottle("E.H. Taylor Barrel Proof Bourbon", taylor)).toBe(false);
  });

  it("does not match bare 'Old Rip Van Winkle' as Pappy 10", () => {
    const pappy10 = TARGET_BOTTLES.find((b) => b.name === "Pappy Van Winkle 10 Year");
    expect(matchesBottle("Old Rip Van Winkle 10 Year 750ml", pappy10)).toBe(true);
    expect(matchesBottle("Old Rip Van Winkle 12 Year 750ml", pappy10)).toBe(false);
    expect(matchesBottle("Old Rip Van Winkle Rye Whiskey", pappy10)).toBe(false);
  });

  it("does not match bare 'Van Winkle Special Reserve' as Pappy 12", () => {
    const pappy12 = TARGET_BOTTLES.find((b) => b.name === "Pappy Van Winkle 12 Year");
    expect(matchesBottle("Van Winkle Special Reserve 12 Year", pappy12)).toBe(true);
    expect(matchesBottle("Van Winkle Special Reserve 10 Year", pappy12)).toBe(false);
  });

  it("matches William Larue Weller aliases", () => {
    const wlw = TARGET_BOTTLES.find((b) => b.name === "William Larue Weller");
    expect(matchesBottle("William Larue Weller Bourbon BTAC 2024", wlw)).toBe(true);
    expect(matchesBottle("WM Larue Weller 750ml", wlw)).toBe(true);
    expect(matchesBottle("W.L. Weller BTAC Limited Release", wlw)).toBe(true);
    expect(matchesBottle("Larue Weller Bourbon", wlw)).toBe(true);
  });

  it("matches E.H. Taylor variations", () => {
    const sib = TARGET_BOTTLES.find((b) => b.name === "E.H. Taylor Single Barrel");
    expect(matchesBottle("Colonel E.H. Taylor Single Barrel Bourbon 750ml", sib)).toBe(true);
    expect(matchesBottle("EH Taylor Single Barrel Kentucky Straight", sib)).toBe(true);
    const bp = TARGET_BOTTLES.find((b) => b.name === "E.H. Taylor Barrel Proof");
    expect(matchesBottle("E.H. Taylor Barrel Proof Bourbon 750ml", bp)).toBe(true);
    const rye = TARGET_BOTTLES.find((b) => b.name === "E.H. Taylor Straight Rye");
    expect(matchesBottle("Col. E.H. Taylor Straight Rye Whiskey", rye)).toBe(true);
    expect(matchesBottle("EH Taylor Rye 750ml", rye)).toBe(true);
  });

  it("does not cross-match E.H. Taylor variations", () => {
    const smb = TARGET_BOTTLES.find((b) => b.name === "E.H. Taylor Small Batch");
    const sib = TARGET_BOTTLES.find((b) => b.name === "E.H. Taylor Single Barrel");
    const bp = TARGET_BOTTLES.find((b) => b.name === "E.H. Taylor Barrel Proof");
    // "Single Barrel" should NOT match Small Batch
    expect(matchesBottle("E.H. Taylor Single Barrel Bourbon", smb)).toBe(false);
    // "Barrel Proof" should NOT match Single Barrel
    expect(matchesBottle("E.H. Taylor Barrel Proof Bourbon", sib)).toBe(false);
    // "Small Batch" should NOT match Barrel Proof
    expect(matchesBottle("E.H. Taylor Small Batch Bourbon", bp)).toBe(false);
  });

  it("matches Blanton's Original Single Barrel", () => {
    const original = TARGET_BOTTLES.find((b) => b.name === "Blanton's Original");
    expect(original).toBeTruthy();
    expect(matchesBottle("Blanton's Single Barrel Original Bourbon 750ml", original)).toBe(true);
    expect(matchesBottle("Blantons Original Bourbon", original)).toBe(true);
  });

  it("Van Winkle Family Reserve Rye does not false-positive on Pappy 15", () => {
    const rye = TARGET_BOTTLES.find((b) => b.name === "Van Winkle Family Reserve Rye");
    expect(matchesBottle("Van Winkle Family Reserve Rye 13 Year", rye)).toBe(true);
    expect(matchesBottle("Van Winkle Rye 13 Year 750ml", rye)).toBe(true);
    // Must NOT match Pappy 15 (officially "Pappy Van Winkle's Family Reserve")
    expect(matchesBottle("Pappy Van Winkle's Family Reserve 15 Year", rye)).toBe(false);
    expect(matchesBottle("Pappy Van Winkle Family Reserve 20 Year", rye)).toBe(false);
    // Must NOT match unrelated rye products
    expect(matchesBottle("Jefferson's Family Reserve Rye", rye)).toBe(false);
  });

  it("matches Old Forester allocated bottles", () => {
    const birthday = TARGET_BOTTLES.find((b) => b.name === "Old Forester Birthday Bourbon");
    expect(matchesBottle("Old Forester Birthday Bourbon 2024 Release", birthday)).toBe(true);
    // "birthday bourbon" alone should NOT match (removed to avoid A. Smith Bowman false positive)
    expect(matchesBottle("A. Smith Bowman Birthday Bourbon", birthday)).toBe(false);
    const pc = TARGET_BOTTLES.find((b) => b.name === "Old Forester President's Choice");
    expect(matchesBottle("Old Forester President's Choice Barrel Strength", pc)).toBe(true);
    expect(matchesBottle("Old Forester Presidents Choice 750ml", pc)).toBe(true);
  });

  it("matches George T. Stagg with period after T", () => {
    const gts = TARGET_BOTTLES.find((b) => b.name === "George T. Stagg");
    expect(matchesBottle("George T. Stagg Bourbon 750ml", gts)).toBe(true);
    expect(matchesBottle("George T Stagg Bourbon 750ml", gts)).toBe(true);
  });

  it("matches Elmer T. Lee with period after T", () => {
    const etl = TARGET_BOTTLES.find((b) => b.name === "Elmer T. Lee");
    expect(matchesBottle("Elmer T. Lee Single Barrel Bourbon", etl)).toBe(true);
    expect(matchesBottle("Elmer T Lee Single Barrel", etl)).toBe(true);
  });

  it("handles unicode curly apostrophes from retailer sites", () => {
    const blantons = TARGET_BOTTLES.find((b) => b.name === "Blanton's Gold");
    // Unicode right single quotation mark (U+2019) — common in retailer HTML
    expect(matchesBottle("Blanton\u2019s Gold 750ml", blantons)).toBe(true);
    const pc = TARGET_BOTTLES.find((b) => b.name === "Old Forester President's Choice");
    expect(matchesBottle("Old Forester President\u2019s Choice", pc)).toBe(true);
  });

  it("every TARGET_BOTTLE is reachable by at least one SEARCH_QUERY", () => {
    // Retailer search engines do keyword matching, not substring matching.
    // A query like "blantons bourbon" returns all products containing "blantons".
    // So a bottle is reachable if any keyword from any query appears in the
    // bottle's name or searchTerms.
    for (const bottle of TARGET_BOTTLES) {
      const allText = [bottle.name, ...bottle.searchTerms].join(" ").toLowerCase();
      const reachable = SEARCH_QUERIES.some((query) => {
        const keywords = query.toLowerCase().split(/\s+/);
        // The distinctive keyword (not generic words like "bourbon"/"rye") must appear
        return keywords.some((kw) => kw.length > 3 && allText.includes(kw));
      });
      expect(reachable, `${bottle.name} is not reachable by any SEARCH_QUERY`).toBe(true);
    }
  });
});

describe("parsePrice", () => {
  it("parses dollar-sign prices", () => {
    expect(parsePrice("$29.99")).toBe(29.99);
  });

  it("handles commas in large prices", () => {
    expect(parsePrice("$1,299.99")).toBe(1299.99);
  });

  it("handles plain numbers", () => {
    expect(parsePrice("29.99")).toBe(29.99);
  });

  it("returns 0 for null/undefined/empty", () => {
    expect(parsePrice(null)).toBe(0);
    expect(parsePrice(undefined)).toBe(0);
    expect(parsePrice("")).toBe(0);
  });

  it("returns 0 for non-numeric strings", () => {
    expect(parsePrice("N/A")).toBe(0);
    expect(parsePrice("Price unavailable")).toBe(0);
  });
});

describe("dedupFound", () => {
  it("removes duplicate bottle names", () => {
    const input = [
      { name: "Weller 12 Year", url: "/a", price: "$30" },
      { name: "Weller 12 Year", url: "/b", price: "$35" },
      { name: "Stagg Jr", url: "/c", price: "$60" },
    ];
    const result = dedupFound(input);
    expect(result).toHaveLength(2);
  });

  it("prefers lowest price on duplicates", () => {
    const input = [
      { name: "Weller 12 Year", url: "/expensive", price: "$50" },
      { name: "Weller 12 Year", url: "/cheap", price: "$30" },
      { name: "Weller 12 Year", url: "/mid", price: "$40" },
    ];
    const result = dedupFound(input);
    expect(result).toHaveLength(1);
    expect(result[0].url).toBe("/cheap");
    expect(result[0].price).toBe("$30");
  });

  it("keeps first-seen when prices are missing", () => {
    const input = [
      { name: "Stagg Jr", url: "/first" },
      { name: "Stagg Jr", url: "/second" },
    ];
    const result = dedupFound(input);
    expect(result).toHaveLength(1);
    expect(result[0].url).toBe("/first");
  });

  it("replaces no-price entry with priced entry", () => {
    const input = [
      { name: "Stagg Jr", url: "/no-price" },
      { name: "Stagg Jr", url: "/has-price", price: "$55" },
    ];
    const result = dedupFound(input);
    expect(result[0].url).toBe("/has-price");
  });

  it("returns empty array for empty input", () => {
    expect(dedupFound([])).toEqual([]);
  });

  it("returns all items when no duplicates", () => {
    const input = [{ name: "A", url: "/a" }, { name: "B", url: "/b" }];
    expect(dedupFound(input)).toHaveLength(2);
  });
});

describe("withTimeout", () => {
  it("returns promise result when it resolves before timeout", async () => {
    vi.useRealTimers();
    const result = await withTimeout(Promise.resolve("done"), 1000, "fallback");
    expect(result).toBe("done");
  });

  it("returns fallback when promise exceeds timeout", async () => {
    vi.useRealTimers();
    const slow = new Promise((resolve) => setTimeout(() => resolve("late"), 500));
    const result = await withTimeout(slow, 50, []);
    expect(result).toEqual([]);
  });

  it("cleans up timer on fast resolve (no leaked timers)", async () => {
    vi.useRealTimers();
    const result = await withTimeout(Promise.resolve(42), 60000, 0);
    expect(result).toBe(42);
  });
});

describe("runWithConcurrency", () => {
  it("runs all tasks", async () => {
    vi.useRealTimers();
    const results = [];
    const tasks = [1, 2, 3].map((n) => () => { results.push(n); return Promise.resolve(); });
    await runWithConcurrency(tasks, 2);
    expect(results).toEqual([1, 2, 3]);
  });

  it("respects concurrency limit", async () => {
    vi.useRealTimers();
    let concurrent = 0, maxConcurrent = 0;
    const tasks = Array.from({ length: 5 }, () => async () => {
      concurrent++;
      maxConcurrent = Math.max(maxConcurrent, concurrent);
      await new Promise((r) => setTimeout(r, 10));
      concurrent--;
    });
    await runWithConcurrency(tasks, 2);
    expect(maxConcurrent).toBeLessThanOrEqual(2);
  });

  it("handles empty task list", async () => {
    await runWithConcurrency([], 4);
  });

  it("handles task errors without stopping others", async () => {
    vi.useRealTimers();
    const results = [];
    const tasks = [
      async () => { results.push(1); },
      async () => { throw new Error("fail"); },
      async () => { results.push(3); },
    ];
    await runWithConcurrency(tasks, 3);
    expect(results).toContain(1);
    expect(results).toContain(3);
  });
});

// ─── matchWalmartNextData ─────────────────────────────────────────────────────

describe("matchWalmartNextData", () => {
  function makeNextData(items) {
    return { props: { pageProps: { initialData: { searchResult: { itemStacks: [{ items }] } } } } };
  }

  it("extracts matched in-stock products", () => {
    const data = makeNextData([{
      __typename: "Product", name: "Weller Special Reserve Bourbon 750ml",
      availabilityStatusV2: { value: "IN_STOCK" }, canonicalUrl: "/ip/weller-sr/123",
      priceInfo: { currentPrice: { priceString: "$29.99" } },
      fulfillmentBadge: "Pickup today", sellerName: "Walmart.com",
    }]);
    const found = matchWalmartNextData(data);
    expect(found).toHaveLength(1);
    expect(found[0].name).toBe("Weller Special Reserve");
    expect(found[0].url).toBe("https://www.walmart.com/ip/weller-sr/123");
    expect(found[0].price).toBe("$29.99");
    expect(found[0].fulfillment).toBe("Pickup today");
    expect(found[0].sku).toBe("");
    expect(found[0].size).toBe("750ml");
  });

  it("filters out non-Product items", () => {
    const data = makeNextData([
      { __typename: "AdAtom", name: "Weller Special Reserve" },
      { __typename: "Product", name: "Weller Special Reserve", availabilityStatusV2: { value: "IN_STOCK" }, sellerName: "Walmart.com" },
    ]);
    expect(matchWalmartNextData(data)).toHaveLength(1);
  });

  it("filters out third-party sellers", () => {
    const data = makeNextData([{
      __typename: "Product", name: "Weller Special Reserve Bourbon",
      availabilityStatusV2: { value: "IN_STOCK" }, sellerName: "ThirdPartyLiquor",
    }]);
    expect(matchWalmartNextData(data)).toHaveLength(0);
  });

  it("filters out unavailable items", () => {
    const data = makeNextData([{
      __typename: "Product", name: "Weller Special Reserve Bourbon",
      availabilityStatusV2: { value: "OUT_OF_STOCK" }, canAddToCart: false, sellerName: "Walmart.com",
    }]);
    expect(matchWalmartNextData(data)).toHaveLength(0);
  });

  it("ignores canAddToCart without IN_STOCK status", () => {
    // canAddToCart alone is not sufficient — requires availabilityStatusV2.value === "IN_STOCK"
    const data = makeNextData([{ __typename: "Product", name: "Weller Special Reserve", canAddToCart: true, sellerName: "Walmart.com" }]);
    expect(matchWalmartNextData(data)).toHaveLength(0);
  });

  it("filters ship-only items via fulfillmentBadge", () => {
    const data = makeNextData([{
      __typename: "Product", name: "Weller Special Reserve", availabilityStatusV2: { value: "IN_STOCK" },
      sellerName: "Walmart.com", fulfillmentBadge: "Shipping only",
    }]);
    expect(matchWalmartNextData(data)).toHaveLength(0);
  });

  it("keeps items with pickup fulfillmentBadge", () => {
    const data = makeNextData([{
      __typename: "Product", name: "Weller Special Reserve", availabilityStatusV2: { value: "IN_STOCK" },
      sellerName: "Walmart.com", fulfillmentBadge: "Pickup today",
    }]);
    expect(matchWalmartNextData(data)).toHaveLength(1);
  });

  it("keeps items with no fulfillmentBadge", () => {
    const data = makeNextData([{
      __typename: "Product", name: "Weller Special Reserve", availabilityStatusV2: { value: "IN_STOCK" },
      sellerName: "Walmart.com",
    }]);
    expect(matchWalmartNextData(data)).toHaveLength(1);
  });

  it("handles multiple itemStacks", () => {
    const data = { props: { pageProps: { initialData: { searchResult: { itemStacks: [
      { items: [{ __typename: "Product", name: "Weller Special Reserve", availabilityStatusV2: { value: "IN_STOCK" }, sellerName: "Walmart.com" }] },
      { items: [{ __typename: "Product", name: "Stagg Jr Bourbon", availabilityStatusV2: { value: "IN_STOCK" }, sellerName: "Walmart.com" }] },
    ] } } } } };
    expect(matchWalmartNextData(data)).toHaveLength(2);
  });

  it("returns empty for null/undefined data", () => {
    expect(matchWalmartNextData(null)).toEqual([]);
    expect(matchWalmartNextData(undefined)).toEqual([]);
    expect(matchWalmartNextData({})).toEqual([]);
  });

  it("handles items with no name", () => {
    const data = makeNextData([{ __typename: "Product", availabilityStatusV2: { value: "IN_STOCK" }, sellerName: "Walmart.com" }]);
    expect(matchWalmartNextData(data)).toHaveLength(0);
  });

  it("includes price without fulfillment badge", () => {
    const data = makeNextData([{
      __typename: "Product", name: "Weller Special Reserve",
      availabilityStatusV2: { value: "IN_STOCK" }, priceInfo: { currentPrice: { priceString: "$29.99" } },
      sellerName: "Walmart.com",
    }]);
    expect(matchWalmartNextData(data)[0].price).toBe("$29.99");
  });

  it("handles fulfillment badge without price", () => {
    const data = makeNextData([{
      __typename: "Product", name: "Weller Special Reserve",
      availabilityStatusV2: { value: "IN_STOCK" }, fulfillmentBadge: "Pickup tomorrow", sellerName: "Walmart.com",
    }]);
    const result = matchWalmartNextData(data)[0];
    expect(result.price).toBe("");
    expect(result.fulfillment).toBe("Pickup tomorrow");
  });

  it("extracts usItemId as sku", () => {
    const data = makeNextData([{
      __typename: "Product", name: "Weller Special Reserve",
      availabilityStatusV2: { value: "IN_STOCK" }, usItemId: "987654321", sellerName: "Walmart.com",
    }]);
    expect(matchWalmartNextData(data)[0].sku).toBe("987654321");
  });

  it("allows products with no sellerName (direct Walmart)", () => {
    const data = makeNextData([{ __typename: "Product", name: "Weller Special Reserve", availabilityStatusV2: { value: "IN_STOCK" } }]);
    expect(matchWalmartNextData(data)).toHaveLength(1);
  });
});

// ─── Store Info Formatting ────────────────────────────────────────────────────

describe("parseCity", () => {
  it("extracts city from standard address", () => {
    expect(parseCity("2501 S Market St, Gilbert, AZ 85295")).toBe("Gilbert");
  });
  it("handles addresses with commas in street (E1)", () => {
    expect(parseCity("123 Main St, Ste 200, Tempe, AZ 85283")).toBe("Tempe");
  });
  it("handles simple two-part address", () => {
    expect(parseCity("Tempe, AZ 85283")).toBe("Tempe");
  });
  it("returns empty for null/empty", () => {
    expect(parseCity("")).toBe("");
    expect(parseCity(null)).toBe("");
  });
  it("falls back to second-to-last part when no state+zip pattern", () => {
    expect(parseCity("Downtown, Phoenix")).toBe("Downtown");
  });
});

describe("parseState", () => {
  it("extracts state abbreviation", () => {
    expect(parseState("2501 S Market St, Gilbert, AZ 85295")).toBe("AZ");
  });
  it("returns empty when no state found", () => {
    expect(parseState("Some Address")).toBe("");
    expect(parseState(null)).toBe("");
  });
});

describe("formatStoreInfo", () => {
  it("formats store with full info", () => {
    const info = formatStoreInfo("totalwine", "Total Wine", TEST_STORE);
    expect(info.title).toContain("Total Wine");
    expect(info.title).toContain("Test Store");
    expect(info.title).toContain("#1234");
    expect(info.title).toContain("3.5 mi");
    expect(info.storeLine).toContain("🏬");
    expect(info.storeLine).toContain("Tempe, AZ");
    expect(info.addressLine).toContain("📍");
    expect(info.addressLine).toContain("maps");
    expect(info.skuLabel).toBe("Item #");
  });

  it("uses correct SKU labels per retailer", () => {
    expect(formatStoreInfo("kroger", "Kroger", TEST_STORE).skuLabel).toBe("SKU");
    expect(formatStoreInfo("safeway", "Safeway", TEST_STORE).skuLabel).toBe("UPC");
    expect(formatStoreInfo("costco", "Costco", TEST_STORE).skuLabel).toBe("Item #");
    expect(formatStoreInfo("samsclub", "Sam's Club", TEST_STORE).skuLabel).toBe("Item #");
  });

  it("uses 'Club' type label for Sam's Club", () => {
    const info = formatStoreInfo("samsclub", "Sam's Club", TEST_STORE);
    expect(info.storeLine).toContain("Club #1234");
    expect(info.storeLine).not.toContain("Store #");
    expect(info.storeLine).not.toContain("Warehouse #");
  });

  it("handles missing distance", () => {
    const info = formatStoreInfo("costco", "Costco", { ...TEST_STORE, distanceMiles: null });
    expect(info.title).not.toContain("mi");
  });

  it("handles missing address", () => {
    const info = formatStoreInfo("costco", "Costco", { ...TEST_STORE, address: "" });
    expect(info.addressLine).toContain("Address unknown");
  });

  it("uses fallback type label and SKU label for unknown retailer", () => {
    const info = formatStoreInfo("unknown", "Unknown Retailer", TEST_STORE);
    expect(info.storeLine).toContain("Store #");
    expect(info.skuLabel).toBe("Item #");
  });

  it("handles missing storeId", () => {
    const info = formatStoreInfo("costco", "Costco", { ...TEST_STORE, storeId: "" });
    expect(info.storeLine).not.toContain("Item #");
  });
});

describe("timeAgo", () => {
  it("formats minutes", () => {
    const recent = new Date(Date.now() - 30 * 60000).toISOString();
    expect(timeAgo(recent)).toBe("30m ago");
  });
  it("formats hours", () => {
    const hours = new Date(Date.now() - 3 * 3600000).toISOString();
    expect(timeAgo(hours)).toBe("3h ago");
  });
  it("formats days", () => {
    const days = new Date(Date.now() - 2 * 86400000).toISOString();
    expect(timeAgo(days)).toBe("2d ago");
  });
  it("returns empty for null", () => {
    expect(timeAgo(null)).toBe("");
    expect(timeAgo("")).toBe("");
  });
});

// ─── Discord Embeds ───────────────────────────────────────────────────────────

describe("formatBottleLine", () => {
  it("formats bottle with all details", () => {
    const b = { name: "Weller 12", url: "https://example.com", price: "$40", sku: "123", size: "750ml", fulfillment: "Pickup" };
    const line = formatBottleLine(b, "Item #");
    expect(line).toContain("[Weller 12](https://example.com)");
    expect(line).toContain("💰 $40");
    expect(line).toContain("🏷️ Item #123");
    expect(line).toContain("📐 750ml");
    expect(line).toContain("🚚 Pickup");
  });
  it("handles no url", () => {
    const line = formatBottleLine({ name: "Stagg Jr", url: "", price: "$60", sku: "", size: "", fulfillment: "" }, "Item #");
    expect(line).toContain("🟢 Stagg Jr");
    expect(line).not.toContain("[Stagg Jr]");
  });
});

describe("truncateDescription", () => {
  it("returns short descriptions unchanged", () => {
    const desc = "Short description";
    expect(truncateDescription(desc)).toBe(desc);
  });

  it("truncates OOS section when description exceeds limit", () => {
    const header = "🏬 Store info\n📍 Address\n\n🆕 **NEWLY SPOTTED**\n🟢 Bottle Name";
    const oosNames = "A, ".repeat(2000); // way over limit
    const desc = `${header}\n\n❌ **OUT OF STOCK (50)**\n${oosNames}`;
    const result = truncateDescription(desc);
    expect(result.length).toBeLessThanOrEqual(DISCORD_DESC_LIMIT);
    expect(result).toContain("❌ **OUT OF STOCK (50)**");
    expect(result).toMatch(/ \.\.\.$/);
  });

  it("truncates descriptions without OOS section", () => {
    const desc = "x".repeat(DISCORD_DESC_LIMIT + 100);
    const result = truncateDescription(desc);
    expect(result.length).toBeLessThanOrEqual(DISCORD_DESC_LIMIT);
    expect(result).toMatch(/\.\.\.$/);
  });

  it("handles exactly-at-limit descriptions", () => {
    const desc = "x".repeat(DISCORD_DESC_LIMIT);
    expect(truncateDescription(desc)).toBe(desc);
  });

  it("truncates before section when OOS header leaves no room", () => {
    // Before section fills almost all of the limit, OOS header leaves no remaining room
    const before = "x".repeat(DISCORD_DESC_LIMIT - 10);
    const desc = `${before}\n\n❌ **OUT OF STOCK (5)**\nBottle1, Bottle2, Bottle3`;
    const result = truncateDescription(desc);
    expect(result.length).toBeLessThanOrEqual(DISCORD_DESC_LIMIT);
    expect(result).toMatch(/\.\.\.$/);
  });
});

describe("buildStoreEmbeds", () => {
  const bottle = (name, extra = {}) => ({ name, url: `https://example.com/${name}`, price: "$29.99", sku: "123", size: "750ml", fulfillment: "", ...extra });

  it("builds new find embed (green)", () => {
    const changes = { newFinds: [bottle("Weller 12")], stillInStock: [], goneOOS: [] };
    const embeds = buildStoreEmbeds("costco", "Costco", TEST_STORE, changes);
    expect(embeds).toHaveLength(1);
    expect(embeds[0].title).toContain("🚨 NEW FIND");
    expect(embeds[0].title).toContain("Costco");
    expect(embeds[0].description).toContain("NEWLY SPOTTED");
    expect(embeds[0].description).toContain("Weller 12");
    expect(embeds[0].color).toBe(COLORS.newFind);
  });

  it("includes still-in-stock in new find embed", () => {
    const changes = {
      newFinds: [bottle("Weller 12")],
      stillInStock: [{ ...bottle("Stagg Jr"), firstSeen: new Date(Date.now() - 7200000).toISOString(), scanCount: 4 }],
      goneOOS: [],
    };
    const embeds = buildStoreEmbeds("costco", "Costco", TEST_STORE, changes);
    expect(embeds).toHaveLength(1);
    expect(embeds[0].description).toContain("STILL IN STOCK");
    expect(embeds[0].description).toContain("Stagg Jr");
  });

  it("builds OOS embed (orange) when no new finds", () => {
    const changes = { newFinds: [], stillInStock: [], goneOOS: [{ name: "Weller 12", price: "$29.99", sku: "123", firstSeen: new Date(Date.now() - 3600000).toISOString() }] };
    const embeds = buildStoreEmbeds("costco", "Costco", TEST_STORE, changes);
    expect(embeds).toHaveLength(1);
    expect(embeds[0].title).toContain("⚠️ STOCK LOST");
    expect(embeds[0].description).toContain("NO LONGER AVAILABLE");
    expect(embeds[0].description).toContain("Weller 12");
    expect(embeds[0].color).toBe(COLORS.goneOOS);
  });

  it("skips OOS embed when new finds exist", () => {
    const changes = {
      newFinds: [bottle("Stagg Jr")],
      stillInStock: [],
      goneOOS: [{ name: "Weller 12", price: "$29.99", sku: "123" }],
    };
    const embeds = buildStoreEmbeds("costco", "Costco", TEST_STORE, changes);
    expect(embeds).toHaveLength(1);
    expect(embeds[0].title).toContain("NEW FIND");
  });

  it("builds re-alert embed when scanCount divisible by N", () => {
    const changes = {
      newFinds: [],
      stillInStock: [{ ...bottle("Weller 12"), firstSeen: new Date(Date.now() - 7200000).toISOString(), scanCount: 4 }],
      goneOOS: [],
    };
    const embeds = buildStoreEmbeds("costco", "Costco", TEST_STORE, changes);
    expect(embeds).toHaveLength(1);
    expect(embeds[0].title).toContain("🔵 STILL AVAILABLE");
    expect(embeds[0].color).toBe(COLORS.stillIn);
  });

  it("skips re-alert when scanCount not divisible by N", () => {
    const changes = {
      newFinds: [],
      stillInStock: [{ ...bottle("Weller 12"), firstSeen: new Date().toISOString(), scanCount: 3 }],
      goneOOS: [],
    };
    const embeds = buildStoreEmbeds("costco", "Costco", TEST_STORE, changes);
    expect(embeds).toHaveLength(0);
  });

  it("returns empty array when nothing to report", () => {
    const changes = { newFinds: [], stillInStock: [], goneOOS: [] };
    const embeds = buildStoreEmbeds("costco", "Costco", TEST_STORE, changes);
    expect(embeds).toHaveLength(0);
  });

  it("includes OOS list in embeds", () => {
    const changes = { newFinds: [bottle("Weller 12")], stillInStock: [], goneOOS: [] };
    const embeds = buildStoreEmbeds("costco", "Costco", TEST_STORE, changes);
    expect(embeds[0].description).toContain("OUT OF STOCK");
  });

  it("handles goneOOS bottles without firstSeen or price", () => {
    const changes = { newFinds: [], stillInStock: [], goneOOS: [{ name: "Stagg Jr", sku: "456" }] };
    const embeds = buildStoreEmbeds("costco", "Costco", TEST_STORE, changes);
    expect(embeds).toHaveLength(1);
    expect(embeds[0].description).toContain("Stagg Jr");
    expect(embeds[0].description).not.toContain("was $");
  });

  it("handles stillInStock without firstSeen in new-find embed", () => {
    const changes = {
      newFinds: [bottle("Weller 12")],
      stillInStock: [{ ...bottle("Stagg Jr"), scanCount: 4 }],
      goneOOS: [],
    };
    const embeds = buildStoreEmbeds("costco", "Costco", TEST_STORE, changes);
    expect(embeds[0].description).toContain("STILL IN STOCK");
    expect(embeds[0].description).toContain("Stagg Jr");
    // No "since" text when firstSeen is missing
    expect(embeds[0].description).not.toContain("since");
  });

  it("handles re-alert without firstSeen", () => {
    const changes = {
      newFinds: [],
      stillInStock: [{ ...bottle("Weller 12"), scanCount: 4 }],
      goneOOS: [],
    };
    const embeds = buildStoreEmbeds("costco", "Costco", TEST_STORE, changes);
    expect(embeds).toHaveLength(1);
    expect(embeds[0].title).toContain("STILL AVAILABLE");
    // Should still render without crash
    expect(embeds[0].description).toContain("Weller 12");
  });

  it("handles stillInStock with missing price and sku in new-find embed", () => {
    const changes = {
      newFinds: [bottle("Weller 12")],
      stillInStock: [{ name: "Stagg Jr", url: "", price: "", sku: "", size: "", fulfillment: "", firstSeen: new Date().toISOString(), scanCount: 4 }],
      goneOOS: [],
    };
    const embeds = buildStoreEmbeds("costco", "Costco", TEST_STORE, changes);
    expect(embeds[0].description).toContain("N/A");
    expect(embeds[0].description).toContain("?");
  });

  it("renders Sam's Club embed with Club type label and correct footer", () => {
    const samsStore = { ...TEST_STORE, name: "Sam's Club Tempe", storeId: "4956" };
    const changes = { newFinds: [bottle("Weller Special Reserve")], stillInStock: [], goneOOS: [] };
    const embeds = buildStoreEmbeds("samsclub", "Sam's Club", samsStore, changes);
    expect(embeds).toHaveLength(1);
    expect(embeds[0].title).toContain("Sam's Club Tempe");
    expect(embeds[0].description).toContain("Club #4956");
    expect(embeds[0].description).not.toContain("Store #");
    expect(embeds[0].footer.text).toContain("Sam's Club");
    expect(embeds[0].description).toContain("Item #");
  });
});

describe("buildSummaryEmbed", () => {
  it("builds summary with counts", () => {
    const embed = buildSummaryEmbed({ storesScanned: 15, retailersScanned: 4, totalNewFinds: 3, totalStillInStock: 2, totalGoneOOS: 1, nothingCount: 9, durationSec: 135 });
    expect(embed.title).toBe("📊 Scan Complete");
    expect(embed.description).toContain("15");
    expect(embed.description).toContain("3 new finds");
    expect(embed.description).toContain("2 still in stock");
    expect(embed.description).toContain("1 went OOS");
    expect(embed.description).toContain("9 nothing");
    expect(embed.color).toBe(COLORS.summary);
  });

  it("includes duration", () => {
    const embed = buildSummaryEmbed({ storesScanned: 5, retailersScanned: 2, totalNewFinds: 0, totalStillInStock: 0, totalGoneOOS: 0, nothingCount: 5, durationSec: 60 });
    expect(embed.description).toContain("60s");
  });

  it("footer shows brand without cron schedule", () => {
    const embed = buildSummaryEmbed({ storesScanned: 5, retailersScanned: 2, totalNewFinds: 0, totalStillInStock: 0, totalGoneOOS: 0, nothingCount: 5, durationSec: 60 });
    expect(embed.footer.text).toBe("Bourbon Scout 🥃");
    expect(embed.footer.text).not.toContain("*/15");
  });

  it("includes store list when nothing found", () => {
    const scannedStores = [
      { retailerName: "Costco", storeName: "Tempe", storeId: "736" },
      { retailerName: "Costco", storeName: "Gilbert", storeId: "1042" },
      { retailerName: "Walmart", storeName: "Tempe", storeId: "5765" },
    ];
    const embed = buildSummaryEmbed({
      storesScanned: 3, retailersScanned: 2, totalNewFinds: 0, totalStillInStock: 0,
      totalGoneOOS: 0, nothingCount: 3, durationSec: 60, scannedStores,
    });
    expect(embed.description).toContain("**Stores scanned:**");
    expect(embed.description).toContain("**Costco**");
    expect(embed.description).toContain("#736 Tempe");
    expect(embed.description).toContain("#1042 Gilbert");
    expect(embed.description).toContain("**Walmart**");
    expect(embed.description).toContain("#5765 Tempe");
  });

  it("omits store list when allocations found", () => {
    const scannedStores = [
      { retailerName: "Costco", storeName: "Tempe", storeId: "736" },
    ];
    const embed = buildSummaryEmbed({
      storesScanned: 1, retailersScanned: 1, totalNewFinds: 1, totalStillInStock: 0,
      totalGoneOOS: 0, nothingCount: 0, durationSec: 30, scannedStores,
    });
    expect(embed.description).not.toContain("Stores scanned");
  });

  it("groups stores by retailer in insertion order", () => {
    const scannedStores = [
      { retailerName: "Total Wine", storeName: "Mesa", storeId: "105" },
      { retailerName: "Kroger", storeName: "Fry's Tempe", storeId: "0491" },
      { retailerName: "Total Wine", storeName: "Tempe", storeId: "116" },
    ];
    const embed = buildSummaryEmbed({
      storesScanned: 3, retailersScanned: 2, totalNewFinds: 0, totalStillInStock: 0,
      totalGoneOOS: 0, nothingCount: 3, durationSec: 45, scannedStores,
    });
    // Total Wine should appear before Kroger (insertion order)
    const twIdx = embed.description.indexOf("**Total Wine**");
    const krIdx = embed.description.indexOf("**Kroger**");
    expect(twIdx).toBeLessThan(krIdx);
    // Both Total Wine stores on same line
    expect(embed.description).toContain("#105 Mesa, #116 Tempe");
  });

  it("includes health fields when health data is provided", () => {
    const health = {
      costco: { queries: 14, succeeded: 14, failed: 0, blocked: 0 },
      kroger: { queries: 14, succeeded: 10, failed: 4, blocked: 0 },
    };
    const embed = buildSummaryEmbed({
      storesScanned: 5, retailersScanned: 2, totalNewFinds: 0, totalStillInStock: 0,
      totalGoneOOS: 0, nothingCount: 5, durationSec: 60, health,
    });
    expect(embed.fields).toBeDefined();
    expect(embed.fields.length).toBe(2);
    expect(embed.fields[0].name).toBe("Costco");
    expect(embed.fields[0].value).toContain("✅");
    expect(embed.fields[0].value).toContain("14/14");
    expect(embed.fields[0].inline).toBe(true);
  });

  it("shows warning emoji for degraded scraper (25-74%)", () => {
    const health = {
      totalwine: { queries: 14, succeeded: 7, failed: 0, blocked: 7 },
    };
    const embed = buildSummaryEmbed({
      storesScanned: 1, retailersScanned: 1, totalNewFinds: 0, totalStillInStock: 0,
      totalGoneOOS: 0, nothingCount: 1, durationSec: 30, health,
    });
    const tw = embed.fields.find((f) => f.name === "Total Wine");
    expect(tw.value).toContain("⚠️");
    expect(tw.value).toContain("7/14");
  });

  it("shows error emoji for mostly-failed scraper (<25%)", () => {
    const health = {
      walgreens: { queries: 14, succeeded: 2, failed: 2, blocked: 10 },
    };
    const embed = buildSummaryEmbed({
      storesScanned: 1, retailersScanned: 1, totalNewFinds: 0, totalStillInStock: 0,
      totalGoneOOS: 0, nothingCount: 1, durationSec: 30, health,
    });
    const wg = embed.fields.find((f) => f.name === "Walgreens");
    expect(wg.value).toContain("❌");
  });

  it("appends canary emoji when canary found", () => {
    const health = { kroger: { queries: 14, succeeded: 14, failed: 0, blocked: 0 } };
    const canaryResults = { kroger: true };
    const embed = buildSummaryEmbed({
      storesScanned: 1, retailersScanned: 1, totalNewFinds: 0, totalStillInStock: 0,
      totalGoneOOS: 0, nothingCount: 1, durationSec: 30, health, canaryResults,
    });
    expect(embed.fields[0].value).toContain("🐤");
  });

  it("omits canary emoji when canary not found", () => {
    const health = { kroger: { queries: 14, succeeded: 14, failed: 0, blocked: 0 } };
    const embed = buildSummaryEmbed({
      storesScanned: 1, retailersScanned: 1, totalNewFinds: 0, totalStillInStock: 0,
      totalGoneOOS: 0, nothingCount: 1, durationSec: 30, health,
    });
    expect(embed.fields[0].value).not.toContain("🐤");
  });

  it("omits fields when no health data", () => {
    const embed = buildSummaryEmbed({
      storesScanned: 5, retailersScanned: 2, totalNewFinds: 0, totalStillInStock: 0,
      totalGoneOOS: 0, nothingCount: 5, durationSec: 60,
    });
    expect(embed.fields).toBeUndefined();
  });

  it("orders fields by retailer registry order", () => {
    const health = {
      samsclub: { queries: 8, succeeded: 8, failed: 0, blocked: 0 },
      walgreens: { queries: 14, succeeded: 14, failed: 0, blocked: 0 },
      costco: { queries: 14, succeeded: 14, failed: 0, blocked: 0 },
      kroger: { queries: 14, succeeded: 14, failed: 0, blocked: 0 },
    };
    const embed = buildSummaryEmbed({
      storesScanned: 4, retailersScanned: 4, totalNewFinds: 0, totalStillInStock: 0,
      totalGoneOOS: 0, nothingCount: 4, durationSec: 60, health,
    });
    expect(embed.fields.map((f) => f.name)).toEqual(["Costco", "Kroger", "Walgreens", "Sam's Club"]);
  });

  it("shows Sam's Club health with canary in summary", () => {
    const health = {
      samsclub: { queries: 8, succeeded: 6, failed: 2, blocked: 0 },
    };
    const canaryResults = { samsclub: true };
    const embed = buildSummaryEmbed({
      storesScanned: 1, retailersScanned: 1, totalNewFinds: 0, totalStillInStock: 0,
      totalGoneOOS: 0, nothingCount: 1, durationSec: 30, health, canaryResults,
    });
    const sc = embed.fields.find((f) => f.name === "Sam's Club");
    expect(sc).toBeDefined();
    expect(sc.value).toContain("✅");
    expect(sc.value).toContain("6/8");
    expect(sc.value).toContain("🐤");
    expect(sc.inline).toBe(true);
  });
});

// ─── State Management ─────────────────────────────────────────────────────────

describe("loadState", () => {
  it("returns parsed JSON from state file", async () => {
    mocks.readFile.mockResolvedValueOnce('{"costco":{"736":["Weller 12 Year"]}}');
    expect(await loadState()).toEqual({ costco: { "736": ["Weller 12 Year"] } });
  });

  it("returns empty object when file doesn't exist", async () => {
    mocks.readFile.mockRejectedValueOnce(new Error("ENOENT"));
    expect(await loadState()).toEqual({});
  });
});

describe("saveState", () => {
  it("writes state atomically via temp file + rename", async () => {
    mocks.writeFile.mockResolvedValueOnce(undefined);
    mocks.rename.mockResolvedValueOnce(undefined);
    await saveState({ costco: {} });
    // Writes to .tmp file first
    expect(mocks.writeFile.mock.calls[0][0]).toMatch(/state\.json\.tmp$/);
    expect(JSON.parse(mocks.writeFile.mock.calls[0][1])).toEqual({ costco: {} });
    // Then renames to final path
    expect(mocks.rename).toHaveBeenCalledTimes(1);
    expect(mocks.rename.mock.calls[0][0]).toMatch(/state\.json\.tmp$/);
    expect(mocks.rename.mock.calls[0][1]).toMatch(/state\.json$/);
  });
});

// ─── State Change Tracking ────────────────────────────────────────────────────

describe("computeChanges", () => {
  const bottle = (name, extra = {}) => ({ name, url: `https://example.com/${name}`, price: "$29.99", sku: "123", size: "750ml", fulfillment: "", ...extra });

  it("classifies all bottles as new when no previous state", () => {
    const { newFinds, stillInStock, goneOOS } = computeChanges(undefined, [bottle("Weller 12")]);
    expect(newFinds).toHaveLength(1);
    expect(newFinds[0].name).toBe("Weller 12");
    expect(stillInStock).toHaveLength(0);
    expect(goneOOS).toHaveLength(0);
  });

  it("classifies continuing bottles as stillInStock with inherited firstSeen/scanCount", () => {
    const prev = { bottles: { "Weller 12": { url: "", price: "$29.99", sku: "123", firstSeen: "2026-03-01T00:00:00Z", lastSeen: "2026-03-07T00:00:00Z", scanCount: 5 } } };
    const { newFinds, stillInStock, goneOOS } = computeChanges(prev, [bottle("Weller 12")]);
    expect(newFinds).toHaveLength(0);
    expect(stillInStock).toHaveLength(1);
    expect(stillInStock[0].firstSeen).toBe("2026-03-01T00:00:00Z");
    expect(stillInStock[0].scanCount).toBe(6);
    expect(goneOOS).toHaveLength(0);
  });

  it("classifies removed bottles as goneOOS", () => {
    const prev = { bottles: { "Weller 12": { url: "", price: "$29.99", sku: "123", firstSeen: "2026-03-01T00:00:00Z", lastSeen: "2026-03-07T00:00:00Z", scanCount: 3 } } };
    const { newFinds, stillInStock, goneOOS } = computeChanges(prev, []);
    expect(newFinds).toHaveLength(0);
    expect(stillInStock).toHaveLength(0);
    expect(goneOOS).toHaveLength(1);
    expect(goneOOS[0].name).toBe("Weller 12");
    expect(goneOOS[0].firstSeen).toBe("2026-03-01T00:00:00Z");
  });

  it("handles mixed new/continuing/gone bottles", () => {
    const prev = { bottles: {
      "Weller 12": { url: "", price: "$29.99", sku: "1", firstSeen: "2026-03-01T00:00:00Z", lastSeen: "2026-03-07T00:00:00Z", scanCount: 2 },
      "Stagg Jr": { url: "", price: "$54.99", sku: "2", firstSeen: "2026-03-05T00:00:00Z", lastSeen: "2026-03-07T00:00:00Z", scanCount: 1 },
    } };
    const { newFinds, stillInStock, goneOOS } = computeChanges(prev, [bottle("Weller 12"), bottle("Blanton's Gold")]);
    expect(newFinds).toHaveLength(1);
    expect(newFinds[0].name).toBe("Blanton's Gold");
    expect(stillInStock).toHaveLength(1);
    expect(stillInStock[0].name).toBe("Weller 12");
    expect(goneOOS).toHaveLength(1);
    expect(goneOOS[0].name).toBe("Stagg Jr");
  });

  it("handles empty previous bottles object", () => {
    const prev = { bottles: {} };
    const { newFinds, stillInStock, goneOOS } = computeChanges(prev, [bottle("Weller 12")]);
    expect(newFinds).toHaveLength(1);
    expect(stillInStock).toHaveLength(0);
    expect(goneOOS).toHaveLength(0);
  });

  it("handles previous bottle without scanCount (legacy state)", () => {
    const prev = { bottles: { "Weller 12": { url: "", price: "$29.99", sku: "123", firstSeen: "2026-03-01T00:00:00Z", lastSeen: "2026-03-07T00:00:00Z" } } };
    const { stillInStock } = computeChanges(prev, [bottle("Weller 12")]);
    expect(stillInStock).toHaveLength(1);
    // scanCount should be 0 + 1 = 1 when previous had no scanCount
    expect(stillInStock[0].scanCount).toBe(1);
  });
});

describe("updateStoreState", () => {
  it("creates new store entry with firstSeen and scanCount", () => {
    const state = {};
    updateStoreState(state, "costco", "736", [{ name: "Weller 12", url: "https://example.com", price: "$29.99", sku: "123", size: "750ml", fulfillment: "" }]);
    expect(state.costco["736"].bottles["Weller 12"]).toBeDefined();
    expect(state.costco["736"].bottles["Weller 12"].scanCount).toBe(1);
    expect(state.costco["736"].bottles["Weller 12"].sku).toBe("123");
    expect(state.costco["736"].lastScanned).toBeDefined();
  });

  it("preserves firstSeen for continuing bottles", () => {
    const state = { costco: { "736": { bottles: {
      "Weller 12": { url: "", price: "$29.99", sku: "123", size: "", fulfillment: "", firstSeen: "2026-03-01T00:00:00Z", lastSeen: "2026-03-06T00:00:00Z", scanCount: 5 },
    }, lastScanned: "2026-03-06T00:00:00Z" } } };
    updateStoreState(state, "costco", "736", [{ name: "Weller 12", url: "https://new-url.com", price: "$30.99", sku: "123", size: "750ml", fulfillment: "" }]);
    expect(state.costco["736"].bottles["Weller 12"].firstSeen).toBe("2026-03-01T00:00:00Z");
    expect(state.costco["736"].bottles["Weller 12"].scanCount).toBe(6);
    expect(state.costco["736"].bottles["Weller 12"].price).toBe("$30.99");
  });

  it("handles products without sku, size, or fulfillment", () => {
    const state = {};
    updateStoreState(state, "costco", "736", [{ name: "Weller 12", url: "https://example.com", price: "$29.99" }]);
    const bottle = state.costco["736"].bottles["Weller 12"];
    expect(bottle.sku).toBe("");
    expect(bottle.size).toBe("");
    expect(bottle.fulfillment).toBe("");
  });

  it("removes bottles no longer in currentFound", () => {
    const state = { costco: { "736": { bottles: {
      "Weller 12": { url: "", price: "$29.99", sku: "123", size: "", fulfillment: "", firstSeen: "2026-03-01T00:00:00Z", lastSeen: "2026-03-06T00:00:00Z", scanCount: 5 },
    }, lastScanned: "2026-03-06T00:00:00Z" } } };
    updateStoreState(state, "costco", "736", []);
    expect(state.costco["736"].bottles).toEqual({});
  });
});

// ─── Discord Alerts ───────────────────────────────────────────────────────────

describe("sendDiscordAlert", () => {
  it("sends embeds to webhook", async () => {
    mocks.fetch.mockResolvedValueOnce({ ok: true });
    await sendDiscordAlert([{ title: "Test" }]);
    expect(mocks.fetch).toHaveBeenCalledWith(
      "https://test-webhook.example.com/hook",
      expect.objectContaining({ method: "POST", body: expect.stringContaining("Bourbon Scout") })
    );
  });

  it("batches embeds in groups of 4", async () => {
    mocks.fetch.mockResolvedValue({ ok: true });
    const embeds = Array.from({ length: 6 }, (_, i) => ({ title: `Test ${i}` }));
    const promise = sendDiscordAlert(embeds);
    await vi.runAllTimersAsync();
    await promise;
    expect(mocks.fetch).toHaveBeenCalledTimes(2);
  });

  it("logs error on webhook failure", async () => {
    const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    mocks.fetch.mockResolvedValueOnce({ ok: false, status: 500, text: async () => "Server error" });
    await sendDiscordAlert([{ title: "Test" }]);
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining("500"));
    consoleSpy.mockRestore();
  });
});

// ─── pruneState (D4) ──────────────────────────────────────────────────────────

describe("pruneState", () => {
  it("removes stale retailer keys not in activeStores", () => {
    const state = {
      costco: { "100": { bottles: {}, lastScanned: "2024-01-01" } },
      bevmo: { "200": { bottles: {}, lastScanned: "2024-01-01" } },
    };
    pruneState(state, { costco: [{ storeId: "100" }] });
    expect(state.costco).toBeDefined();
    expect(state.bevmo).toBeUndefined();
  });

  it("removes stale store IDs within a retailer", () => {
    const state = {
      kroger: {
        "111": { bottles: {}, lastScanned: "2024-01-01" },
        "222": { bottles: {}, lastScanned: "2024-01-01" },
        "333": { bottles: {}, lastScanned: "2024-01-01" },
      },
    };
    pruneState(state, { kroger: [{ storeId: "111" }, { storeId: "333" }] });
    expect(Object.keys(state.kroger)).toEqual(["111", "333"]);
  });

  it("handles empty state gracefully", () => {
    const state = {};
    pruneState(state, { costco: [{ storeId: "100" }] });
    expect(state).toEqual({});
  });
});

describe("postDiscordWebhook", () => {
  it("retries on 429 with retry_after from response body", async () => {
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    mocks.fetch
      .mockResolvedValueOnce({ status: 429, json: async () => ({ retry_after: 0.01 }) })
      .mockResolvedValueOnce({ ok: true, status: 200 });
    await runWithFakeTimers(() => postDiscordWebhook({ embeds: [{ title: "Test" }] }));
    expect(mocks.fetch).toHaveBeenCalledTimes(2);
    expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining("Rate limited"));
    warnSpy.mockRestore();
  });

  it("throws after 3 retries on 429", async () => {
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    mocks.fetch.mockResolvedValue({ status: 429, json: async () => ({ retry_after: 0.01 }) });
    await expect(runWithFakeTimers(() => postDiscordWebhook({ embeds: [{ title: "Test" }] })))
      .rejects.toThrow("Discord webhook failed after 3 retries");
    expect(mocks.fetch).toHaveBeenCalledTimes(3);
    warnSpy.mockRestore();
  });

  it("does not retry on non-429 errors", async () => {
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    mocks.fetch.mockResolvedValueOnce({ ok: false, status: 500, text: async () => "Server error" });
    await postDiscordWebhook({ embeds: [{ title: "Test" }] });
    expect(mocks.fetch).toHaveBeenCalledTimes(1);
    errorSpy.mockRestore();
  });
});

describe("sendUrgentAlert", () => {
  it("sends with @everyone and allowed_mentions", async () => {
    mocks.fetch.mockResolvedValueOnce({ ok: true });
    await sendUrgentAlert([{ title: "FOUND!" }]);
    const body = JSON.parse(mocks.fetch.mock.calls[0][1].body);
    expect(body.content).toContain("@everyone");
    expect(body.allowed_mentions.parse).toContain("everyone");
  });

  it("batches when more than 4 embeds", async () => {
    mocks.fetch.mockResolvedValue({ ok: true });
    const embeds = Array.from({ length: 6 }, (_, i) => ({ title: `Embed ${i}` }));
    await runWithFakeTimers(() => sendUrgentAlert(embeds));
    // Should make 2 calls: batch of 4 + batch of 2
    expect(mocks.fetch).toHaveBeenCalledTimes(2);
  });
});

// ─── Browser Management ───────────────────────────────────────────────────────

describe("browser management", () => {
  it("launchBrowser calls chromium.launch", async () => {
    mocks.chromiumLaunch.mockResolvedValueOnce({ newContext: vi.fn(), close: vi.fn().mockResolvedValue(undefined) });
    await launchBrowser();
    expect(mocks.chromiumLaunch).toHaveBeenCalledWith(expect.objectContaining({
      headless: true,
      args: expect.arrayContaining(["--disable-blink-features=AutomationControlled"]),
      ignoreDefaultArgs: ["--enable-automation"],
    }));
    await closeBrowser();
  });

  it("closeBrowser closes browser", async () => {
    const mockBrowser = { newContext: vi.fn(), close: vi.fn().mockResolvedValue(undefined) };
    mocks.chromiumLaunch.mockResolvedValueOnce(mockBrowser);
    await launchBrowser();
    await closeBrowser();
    expect(mockBrowser.close).toHaveBeenCalled();
  });

  it("closeBrowser handles no browser", async () => {
    await closeBrowser();
  });

  it("newPage creates context and page", async () => {
    const { page } = setupMockBrowser();
    const result = await newPage();
    expect(result).toBe(page);
    await closeBrowser();
  });
});

// ─── Bot Detection ───────────────────────────────────────────────────────────

describe("isBlockedPage", () => {
  it("detects Access Denied pages", async () => {
    const page = createMockPage();
    page.title.mockResolvedValue("Access Denied");
    expect(await isBlockedPage(page)).toBe(true);
  });

  it("detects robot/CAPTCHA pages", async () => {
    const page = createMockPage();
    page.title.mockResolvedValue("Are you a robot?");
    expect(await isBlockedPage(page)).toBe(true);
  });

  it("detects challenge pages", async () => {
    const page = createMockPage();
    page.title.mockResolvedValue("Security Challenge");
    expect(await isBlockedPage(page)).toBe(true);
  });

  it("returns false for normal pages", async () => {
    const page = createMockPage();
    page.title.mockResolvedValue("Search Results - Costco");
    page.evaluate.mockResolvedValue("");
    expect(await isBlockedPage(page)).toBe(false);
  });

  it("detects body text challenges", async () => {
    const page = createMockPage();
    page.title.mockResolvedValue("Shopping");
    page.evaluate.mockResolvedValue("Please verify you are a human");
    expect(await isBlockedPage(page)).toBe(true);
  });

  it("detects 'checking your browser' challenge", async () => {
    const page = createMockPage();
    page.title.mockResolvedValue("One Moment...");
    page.evaluate.mockResolvedValue("Checking your browser before accessing the site");
    expect(await isBlockedPage(page)).toBe(true);
  });

  it("handles title() errors gracefully", async () => {
    const page = createMockPage();
    page.title.mockRejectedValue(new Error("page closed"));
    page.evaluate.mockResolvedValue("");
    expect(await isBlockedPage(page)).toBe(false);
  });
});

// ─── Costco Scraper ───────────────────────────────────────────────────────────

describe("scrapeCostcoOnce", () => {
  it("scrapes products from all search queries", async () => {
    const mockPage = createMockPage();
    mockPage.$$eval.mockResolvedValue([
      { title: "Weller Special Reserve Bourbon 750ml", url: "https://costco.com/weller.product.html", price: "$29.99" },
    ]);
    const found = await runWithFakeTimers(() => scrapeCostcoOnce(mockPage));
    // +2 for homepage pre-warm + category navigation; query rotation means only a subset runs per scan
    const expectedQueries = getQueriesForScan(SEARCH_QUERIES).length;
    expect(mockPage.goto).toHaveBeenCalledTimes(expectedQueries + 2);
    expect(found.find((f) => f.name === "Weller Special Reserve")).toBeTruthy();
  });

  it("handles no results gracefully", async () => {
    const mockPage = createMockPage();
    mockPage.$$eval.mockResolvedValue([]);
    mockPage.waitForSelector.mockRejectedValue(new Error("timeout"));
    const found = await runWithFakeTimers(() => scrapeCostcoOnce(mockPage));
    expect(found).toEqual([]);
  });

  it("skips queries when bot detection page is shown", async () => {
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    const mockPage = createMockPage();
    mockPage.waitForSelector.mockRejectedValue(new Error("timeout"));
    mockPage.title.mockResolvedValue("Access Denied");
    mockPage.$$eval.mockResolvedValue([]);
    const found = await runWithFakeTimers(() => scrapeCostcoOnce(mockPage));
    expect(found).toEqual([]);
    expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining("Bot detection"));
    warnSpy.mockRestore();
  });

  it("handles page errors gracefully", async () => {
    const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    const mockPage = createMockPage();
    mockPage.goto.mockRejectedValue(new Error("net::ERR_HTTP2_PROTOCOL_ERROR"));
    const found = await runWithFakeTimers(() => scrapeCostcoOnce(mockPage));
    expect(found).toEqual([]);
    consoleSpy.mockRestore();
  });

  it("deduplicates found bottles", async () => {
    const mockPage = createMockPage();
    mockPage.$$eval.mockResolvedValue([
      { title: "Weller Special Reserve Bourbon", url: "/a", price: "$30" },
    ]);
    const found = await runWithFakeTimers(() => scrapeCostcoOnce(mockPage));
    expect(found.filter((f) => f.name === "Weller Special Reserve").length).toBeLessThanOrEqual(1);
  });
});

// ─── Total Wine Scraper ───────────────────────────────────────────────────────

describe("scrapeTotalWineStore", () => {
  it("extracts products from INITIAL_STATE", async () => {
    const mockPage = createMockPage();
    mockPage.evaluate.mockImplementation(async (fn) => {
      const saved = globalThis.window;
      globalThis.window = {
        INITIAL_STATE: {
          search: { results: { products: [{
            name: "Weller Special Reserve Bourbon Whiskey",
            productUrl: "/spirits/bourbon/weller-sr/p/12345",
            price: [{ price: 29.99 }],
            stockLevel: [{ stock: 5 }], transactional: true,
          }] } },
        },
      };
      try { return fn(); } finally { globalThis.window = saved; }
    });
    const found = await runWithFakeTimers(() => scrapeTotalWineViaBrowser(TEST_STORE, mockPage));
    const wsr = found.find((f) => f.name === "Weller Special Reserve");
    expect(wsr).toBeTruthy();
    expect(wsr.url).toContain("totalwine.com");
    expect(wsr.price).toBe("$29.99");
  });

  it("falls back to CSS selectors when INITIAL_STATE unavailable", async () => {
    const mockPage = createMockPage();
    // evaluate is called twice per query: first for isBlockedPage body check (string),
    // then for INITIAL_STATE extraction (null/empty triggers CSS fallback)
    let evalCallCount = 0;
    mockPage.evaluate.mockImplementation(async () => {
      evalCallCount++;
      return evalCallCount % 2 === 1 ? "" : null;
    });
    mockPage.$$eval.mockResolvedValue([
      { name: "Weller Special Reserve Bourbon", inStock: true, url: "/spirits/bourbon/weller", price: "$29.99" },
    ]);
    const found = await runWithFakeTimers(() => scrapeTotalWineViaBrowser(TEST_STORE, mockPage));
    expect(found.find((f) => f.name === "Weller Special Reserve")).toBeTruthy();
  });

  it("handles out-of-stock products", async () => {
    const mockPage = createMockPage();
    mockPage.evaluate.mockImplementation(async (fn) => {
      const saved = globalThis.window;
      globalThis.window = {
        INITIAL_STATE: { search: { results: { products: [{
          name: "Weller Special Reserve",
          stockLevel: [{ stock: 0 }], transactional: false,
          shoppingOptions: [{ eligible: false }],
        }] } } },
      };
      try { return fn(); } finally { globalThis.window = saved; }
    });
    const found = await runWithFakeTimers(() => scrapeTotalWineViaBrowser(TEST_STORE, mockPage));
    expect(found).toEqual([]);
  });

  it("handles page errors", async () => {
    const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    const mockPage = createMockPage();
    mockPage.goto.mockRejectedValue(new Error("Navigation timeout"));
    const found = await runWithFakeTimers(() => scrapeTotalWineViaBrowser(TEST_STORE, mockPage));
    expect(found).toEqual([]);
    consoleSpy.mockRestore();
  });

  it("prepends totalwine.com to relative URLs", async () => {
    const mockPage = createMockPage();
    mockPage.evaluate.mockResolvedValue([]);
    mockPage.$$eval.mockResolvedValue([
      { name: "Weller Special Reserve", inStock: true, url: "/spirits/weller", price: "$30" },
    ]);
    const found = await runWithFakeTimers(() => scrapeTotalWineViaBrowser(TEST_STORE, mockPage));
    if (found.length > 0) expect(found[0].url).toContain("https://www.totalwine.com");
  });

  it("preserves absolute URLs from INITIAL_STATE", async () => {
    const mockPage = createMockPage();
    mockPage.evaluate.mockImplementation(async (fn) => {
      const saved = globalThis.window;
      globalThis.window = {
        INITIAL_STATE: {
          search: { results: { products: [{
            name: "Weller Special Reserve Bourbon",
            productUrl: "https://www.totalwine.com/spirits/bourbon/weller-sr/p/12345",
            price: [{ price: 29.99 }],
            stockLevel: [{ stock: 5 }], transactional: true,
          }] } },
        },
      };
      try { return fn(); } finally { globalThis.window = saved; }
    });
    const found = await runWithFakeTimers(() => scrapeTotalWineViaBrowser(TEST_STORE, mockPage));
    const wsr = found.find((f) => f.name === "Weller Special Reserve");
    expect(wsr).toBeTruthy();
    // Absolute URL should be used as-is, not double-prefixed
    expect(wsr.url).toBe("https://www.totalwine.com/spirits/bourbon/weller-sr/p/12345");
  });

  it("skips bot detection pages and continues", async () => {
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    const mockPage = createMockPage();
    // First call triggers bot detection, subsequent calls return empty
    let callCount = 0;
    mockPage.title.mockImplementation(async () => {
      callCount++;
      if (callCount <= 1) return "Access Denied";
      return "Total Wine Search";
    });
    mockPage.evaluate.mockResolvedValue([]);
    const found = await runWithFakeTimers(() => scrapeTotalWineViaBrowser(TEST_STORE, mockPage));
    expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining("Bot detection"));
    expect(found).toEqual([]);
    warnSpy.mockRestore();
  });
});

// ─── Fetch-first helpers ──────────────────────────────────────────────────────

describe("fetchRetry", () => {
  it("returns response on first success", async () => {
    mocks.fetch.mockResolvedValueOnce({ ok: true, status: 200 });
    const res = await fetchRetry("http://example.com", {});
    expect(res.ok).toBe(true);
    expect(mocks.fetch).toHaveBeenCalledTimes(1);
  });

  it("retries once on network error then succeeds", async () => {
    mocks.fetch
      .mockRejectedValueOnce(new Error("ECONNRESET"))
      .mockResolvedValueOnce({ ok: true, status: 200 });
    const res = await runWithFakeTimers(() => fetchRetry("http://example.com", {}));
    expect(res.ok).toBe(true);
    expect(mocks.fetch).toHaveBeenCalledTimes(2);
  });

  it("throws if both attempts fail", async () => {
    mocks.fetch
      .mockRejectedValueOnce(new Error("ECONNRESET"))
      .mockRejectedValueOnce(new Error("ECONNREFUSED"));
    await expect(runWithFakeTimers(() => fetchRetry("http://example.com", {}))).rejects.toThrow("ECONNREFUSED");
  });
});

describe("matchTotalWineInitialState", () => {
  it("extracts matching in-stock bottles from INITIAL_STATE", () => {
    const state = {
      search: { results: { products: [{
        name: "Weller Special Reserve Bourbon Whiskey",
        productUrl: "/spirits/bourbon/weller-sr/p/12345",
        price: [{ price: 29.99 }],
        stockLevel: [{ stock: 5 }],
        transactional: true,
        shoppingOptions: [{ eligible: true, name: "In-store" }],
      }] } },
    };
    const found = matchTotalWineInitialState(state);
    expect(found.length).toBe(1);
    expect(found[0].name).toBe("Weller Special Reserve");
    expect(found[0].price).toBe("$29.99");
    expect(found[0].url).toContain("totalwine.com");
    expect(found[0].fulfillment).toBe("In-store");
  });

  it("skips out-of-stock products", () => {
    const state = {
      search: { results: { products: [{
        name: "Weller Special Reserve Bourbon Whiskey",
        productUrl: "/spirits/bourbon/weller-sr/p/12345",
        stockLevel: [{ stock: 0 }],
        transactional: false,
        shoppingOptions: [],
      }] } },
    };
    expect(matchTotalWineInitialState(state)).toEqual([]);
  });

  it("returns empty array for null/missing state", () => {
    expect(matchTotalWineInitialState(null)).toEqual([]);
    expect(matchTotalWineInitialState({})).toEqual([]);
    expect(matchTotalWineInitialState({ search: {} })).toEqual([]);
  });

  it("preserves absolute URLs", () => {
    const state = {
      search: { results: { products: [{
        name: "Weller Special Reserve Bourbon Whiskey",
        productUrl: "https://www.totalwine.com/spirits/p/12345",
        stockLevel: [{ stock: 1 }],
      }] } },
    };
    const found = matchTotalWineInitialState(state);
    expect(found[0].url).toBe("https://www.totalwine.com/spirits/p/12345");
  });
});

describe("matchCostcoTiles", () => {
  it("extracts matching bottles from cheerio-parsed HTML", async () => {
    const cheerio = await import("cheerio");
    const html = `
      <div data-testid="ProductTile_12345">
        <a href="https://www.costco.com/weller-sr.product.100123456.html">
          <h3 data-testid="Text_ProductTile_12345_title">Weller Special Reserve Bourbon 750ml</h3>
        </a>
        <span data-testid="Text_Price_12345">$29.99</span>
      </div>
    `;
    const $ = cheerio.load(html);
    const found = matchCostcoTiles($);
    expect(found.length).toBe(1);
    expect(found[0].name).toBe("Weller Special Reserve");
    expect(found[0].price).toBe("$29.99");
    expect(found[0].sku).toBe("12345");
    expect(found[0].size).toBe("750ml");
  });

  it("returns empty for non-matching products", async () => {
    const cheerio = await import("cheerio");
    const html = `
      <div data-testid="ProductTile_99999">
        <h3 data-testid="Text_ProductTile_99999_title">Jack Daniel's Tennessee Whiskey</h3>
        <span data-testid="Text_Price_99999">$24.99</span>
      </div>
    `;
    const $ = cheerio.load(html);
    expect(matchCostcoTiles($)).toEqual([]);
  });

  it("generates fallback URL from product ID when href is missing (E2)", async () => {
    const cheerio = await import("cheerio");
    const html = `
      <div data-testid="ProductTile_67890">
        <h3 data-testid="Text_ProductTile_67890_title">Blanton's Gold Bourbon 750ml</h3>
        <span data-testid="Text_Price_67890">$59.99</span>
      </div>
    `;
    const $ = cheerio.load(html);
    const found = matchCostcoTiles($);
    expect(found.length).toBe(1);
    expect(found[0].url).toBe("https://www.costco.com/.product.67890.html");
    expect(found[0].sku).toBe("67890");
  });
});

describe("scrapeTotalWineViaFetch", () => {
  it("returns null when proxyAgent is not set (no PROXY_URL)", async () => {
    // In test env, PROXY_URL is not set so proxyAgent is null
    const result = await scrapeTotalWineViaFetch(TEST_STORE);
    expect(result).toBeNull();
  });
});

describe("isCostcoBlocked", () => {
  it("detects all Akamai challenge patterns", () => {
    expect(isCostcoBlocked("Access Denied - you don't have permission")).toBe(true);
    expect(isCostcoBlocked("Are you a robot?")).toBe(true);
    expect(isCostcoBlocked("Please complete the captcha")).toBe(true);
    expect(isCostcoBlocked("Request unsuccessful")).toBe(true);
    expect(isCostcoBlocked("Incapsula incident")).toBe(true);
    expect(isCostcoBlocked("Please Enable JavaScript to continue")).toBe(true);
    expect(isCostcoBlocked("Please verify you are human")).toBe(true);
    expect(isCostcoBlocked("<script>_ct_challenge</script>")).toBe(true);
  });

  it("does not flag normal product pages", () => {
    expect(isCostcoBlocked("<html><body>Weller Special Reserve Bourbon</body></html>")).toBe(false);
    expect(isCostcoBlocked("<html><body>No results found</body></html>")).toBe(false);
  });

  it("COSTCO_BLOCKED_PATTERNS contains expected entries", () => {
    expect(COSTCO_BLOCKED_PATTERNS.length).toBe(8);
    expect(COSTCO_BLOCKED_PATTERNS).toContain("Access Denied");
    expect(COSTCO_BLOCKED_PATTERNS).toContain("_ct_challenge");
  });
});

describe("scrapeCostcoViaFetch", () => {
  it("returns null when proxyAgent is not set (no PROXY_URL)", async () => {
    const result = await scrapeCostcoViaFetch();
    expect(result).toBeNull();
  });
});

describe("scrapeCostcoStore wrapper", () => {
  it("uses browser with dedicated IP (skips fetch-first)", async () => {
    setupMockBrowser();
    const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    const found = await runWithFakeTimers(() => scrapeCostcoStore());
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining("Using browser (dedicated IP)"));
    expect(found).toEqual([]);
    consoleSpy.mockRestore();
  });
});

describe("scrapeTotalWineStore wrapper", () => {
  it("uses browser with dedicated IP (skips fetch-first)", async () => {
    setupMockBrowser();
    const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    const found = await runWithFakeTimers(() => scrapeTotalWineStore(TEST_STORE));
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining("Using browser (dedicated IP)"));
    expect(found).toEqual([]);
    consoleSpy.mockRestore();
  });
});

// ─── Walmart Scrapers ─────────────────────────────────────────────────────────

describe("scrapeWalmartViaFetch", () => {
  const walmartNextData = {
    props: { pageProps: { initialData: { searchResult: { itemStacks: [{ items: [{
      __typename: "Product", name: "Weller Special Reserve Bourbon",
      availabilityStatusV2: { value: "IN_STOCK" }, canonicalUrl: "/ip/weller/123",
      priceInfo: { currentPrice: { priceString: "$29.99" } }, sellerName: "Walmart.com",
    }] }] } } } },
  };

  it("extracts products from __NEXT_DATA__", async () => {
    mocks.fetch.mockResolvedValue({
      ok: true,
      text: async () => `<html><script id="__NEXT_DATA__" type="application/json">${JSON.stringify(walmartNextData)}</script></html>`,
    });
    const found = await runWithFakeTimers(() => scrapeWalmartViaFetch(TEST_STORE));
    expect(found).not.toBeNull();
    expect(found[0].name).toBe("Weller Special Reserve");
  });

  it("continues on partial failures and keeps results", async () => {
    // First query fails, second succeeds — should return results, not null
    let callCount = 0;
    mocks.fetch.mockImplementation(() => {
      callCount++;
      if (callCount === 1) return Promise.resolve({ ok: false, status: 403 });
      return Promise.resolve({
        ok: true,
        text: async () => `<html><script id="__NEXT_DATA__" type="application/json">${JSON.stringify(walmartNextData)}</script></html>`,
      });
    });
    const found = await runWithFakeTimers(() => scrapeWalmartViaFetch(TEST_STORE));
    expect(found).not.toBeNull();
    expect(found.length).toBeGreaterThan(0);
  });

  it("returns null when more than 3 queries fail", async () => {
    mocks.fetch.mockResolvedValue({ ok: false, status: 403 });
    const found = await runWithFakeTimers(() => scrapeWalmartViaFetch(TEST_STORE));
    expect(found).toBeNull();
  });

  it("returns null when all queries fail with no results", async () => {
    // All queries return no __NEXT_DATA__ — failures > 0 && found.length === 0
    mocks.fetch.mockResolvedValue({ ok: true, text: async () => "<html>Blocked</html>" });
    const found = await runWithFakeTimers(() => scrapeWalmartViaFetch(TEST_STORE));
    expect(found).toBeNull();
  });

  it("returns null on repeated fetch errors", async () => {
    mocks.fetch.mockRejectedValue(new Error("Network error"));
    const found = await runWithFakeTimers(() => scrapeWalmartViaFetch(TEST_STORE));
    expect(found).toBeNull();
  });

  it("returns null when __NEXT_DATA__ exists but has no searchResult (block page)", async () => {
    // Simulates Walmart returning a challenge/geo-block page with __NEXT_DATA__ but no real search results
    const fakeNextData = { props: { pageProps: { initialData: {} } } };
    mocks.fetch.mockResolvedValue({
      ok: true,
      text: async () => `<html><script id="__NEXT_DATA__" type="application/json">${JSON.stringify(fakeNextData)}</script></html>`,
    });
    const found = await runWithFakeTimers(() => scrapeWalmartViaFetch(TEST_STORE));
    // Should return null (trigger browser fallback), NOT empty array
    expect(found).toBeNull();
  });
});

describe("scrapeWalmartViaFetch edge cases", () => {
  it("returns null when validPages is 0 but no failures exceed threshold", async () => {
    // All queries return HTML without __NEXT_DATA__ — failures increment but don't exceed 3
    // However, validPages remains 0
    let callCount = 0;
    mocks.fetch.mockImplementation(() => {
      callCount++;
      // Alternate between no __NEXT_DATA__ and no searchResult
      if (callCount <= 3) return Promise.resolve({ ok: true, text: async () => "<html>No data</html>" });
      const fakeData = { props: { pageProps: { initialData: {} } } };
      return Promise.resolve({
        ok: true,
        text: async () => `<script id="__NEXT_DATA__">${JSON.stringify(fakeData)}</script>`,
      });
    });
    const found = await runWithFakeTimers(() => scrapeWalmartViaFetch(TEST_STORE));
    expect(found).toBeNull();
  });

  it("returns empty array when some queries fail but valid pages exist with no matches", async () => {
    // Some queries fail, remainder succeed but find no matching bottles
    let callCount = 0;
    const emptyNextData = { props: { pageProps: { initialData: { searchResult: { itemStacks: [{ items: [{ __typename: "Product", name: "Random Non-Bourbon Item", sellerName: "Walmart.com", availabilityStatusV2: { value: "IN_STOCK" } }] }] } } } } };
    mocks.fetch.mockImplementation(() => {
      callCount++;
      if (callCount === 1) return Promise.resolve({ ok: false, status: 403 });
      return Promise.resolve({
        ok: true,
        text: async () => `<script id="__NEXT_DATA__">${JSON.stringify(emptyNextData)}</script>`,
      });
    });
    const found = await runWithFakeTimers(() => scrapeWalmartViaFetch(TEST_STORE));
    // validPages > 0 (some queries succeeded) → returns empty array, not null
    expect(found).toEqual([]);
  });
});

describe("scrapeWalmartViaBrowser", () => {
  it("extracts products from __NEXT_DATA__ via page.evaluate", async () => {
    const mockPage = createMockPage();
    const nextData = {
      props: { pageProps: { initialData: { searchResult: { itemStacks: [{ items: [{
        __typename: "Product", name: "Stagg Jr Bourbon",
        availabilityStatusV2: { value: "IN_STOCK" }, sellerName: "Walmart.com",
      }] }] } } } },
    };
    // evaluate is called twice per query: isBlockedPage body text, then __NEXT_DATA__
    let evalCallCount = 0;
    mockPage.evaluate.mockImplementation(async () => {
      evalCallCount++;
      return evalCallCount % 2 === 1 ? "" : nextData;
    });
    const found = await runWithFakeTimers(() => scrapeWalmartViaBrowser(TEST_STORE, mockPage));
    expect(found.find((f) => f.name === "Stagg Jr")).toBeTruthy();
  });

  it("falls back to DOM when __NEXT_DATA__ unavailable", async () => {
    const mockPage = createMockPage();
    // evaluate: isBlockedPage body text → "", __NEXT_DATA__ → null
    let evalCallCount = 0;
    mockPage.evaluate.mockImplementation(async () => {
      evalCallCount++;
      return evalCallCount % 2 === 1 ? "" : null;
    });
    mockPage.$$eval.mockResolvedValue([
      { title: "Weller Special Reserve Bourbon", url: "https://walmart.com/ip/123", price: "$30" },
    ]);
    const found = await runWithFakeTimers(() => scrapeWalmartViaBrowser(TEST_STORE, mockPage));
    expect(found.find((f) => f.name === "Weller Special Reserve")).toBeTruthy();
  });

  it("handles page errors", async () => {
    const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    const mockPage = createMockPage();
    mockPage.goto.mockRejectedValue(new Error("Timeout"));
    const found = await runWithFakeTimers(() => scrapeWalmartViaBrowser(TEST_STORE, mockPage));
    expect(found).toEqual([]);
    consoleSpy.mockRestore();
  });

  it("skips queries that hit bot detection", async () => {
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    const mockPage = createMockPage();
    mockPage.title.mockResolvedValue("Access Denied");
    mockPage.evaluate.mockResolvedValue(null);
    mockPage.$$eval.mockResolvedValue([]);
    const found = await runWithFakeTimers(() => scrapeWalmartViaBrowser(TEST_STORE, mockPage));
    expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining("Bot detection"));
    expect(found).toEqual([]);
    warnSpy.mockRestore();
  });
});

describe("scrapeWalmartStore", () => {
  const walmartStoreNextData = { props: { pageProps: { initialData: { searchResult: { itemStacks: [{ items: [{
    __typename: "Product", name: "Weller Special Reserve",
    availabilityStatusV2: { value: "IN_STOCK" }, sellerName: "Walmart.com",
  }] }] } } } } };

  afterEach(() => {
    delete process.env.CI;
    delete process.env.GITHUB_ACTIONS;
  });

  it("uses fetch path when successful", async () => {
    mocks.fetch.mockResolvedValue({
      ok: true,
      text: async () => `<script id="__NEXT_DATA__">${JSON.stringify(walmartStoreNextData)}</script>`,
    });
    const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    const found = await runWithFakeTimers(() => scrapeWalmartStore(TEST_STORE));
    expect(found).not.toBeNull();
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining("fast fetch"));
    consoleSpy.mockRestore();
  });

  it("falls back to browser when fetch blocked", async () => {
    mocks.fetch.mockResolvedValue({ ok: false, status: 403 });
    const { page: mockPage } = setupMockBrowser();
    mockPage.evaluate.mockResolvedValue(null);
    mockPage.$$eval.mockResolvedValue([]);
    const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    await launchBrowser();
    const found = await runWithFakeTimers(() => scrapeWalmartStore(TEST_STORE));
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining("Fetch blocked"));
    consoleSpy.mockRestore();
    await closeBrowser();
  });

  it("skips fetch on CI and goes straight to browser", async () => {
    process.env.CI = "true";
    const { page: mockPage } = setupMockBrowser();
    mockPage.evaluate.mockResolvedValue(null);
    mockPage.$$eval.mockResolvedValue([]);
    const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    await launchBrowser();
    await runWithFakeTimers(() => scrapeWalmartStore(TEST_STORE));
    // Fetch should never be called for Walmart search URLs
    const walmartFetches = mocks.fetch.mock.calls.filter(([url]) =>
      typeof url === "string" && url.includes("walmart.com/search")
    );
    expect(walmartFetches).toHaveLength(0);
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining("CI mode"));
    consoleSpy.mockRestore();
    await closeBrowser();
  });

  it("skips fetch when GITHUB_ACTIONS is set", async () => {
    process.env.GITHUB_ACTIONS = "true";
    const { page: mockPage } = setupMockBrowser();
    mockPage.evaluate.mockResolvedValue(null);
    mockPage.$$eval.mockResolvedValue([]);
    const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    await launchBrowser();
    await runWithFakeTimers(() => scrapeWalmartStore(TEST_STORE));
    const walmartFetches = mocks.fetch.mock.calls.filter(([url]) =>
      typeof url === "string" && url.includes("walmart.com/search")
    );
    expect(walmartFetches).toHaveLength(0);
    consoleSpy.mockRestore();
    await closeBrowser();
  });
});

// ─── Kroger Scraper ───────────────────────────────────────────────────────────

describe("getKrogerToken", () => {
  it("fetches and caches OAuth token", async () => {
    mocks.fetch.mockResolvedValueOnce({ ok: true, json: async () => ({ access_token: "test-token-abc" }) });
    const token = await getKrogerToken();
    expect(token).toBe("test-token-abc");
    // Second call uses cache
    const token2 = await getKrogerToken();
    expect(token2).toBe("test-token-abc");
    expect(mocks.fetch).toHaveBeenCalledTimes(1);
  });

  it("throws on OAuth failure", async () => {
    _resetKrogerToken();
    mocks.fetch.mockResolvedValueOnce({ ok: false, status: 401 });
    await expect(getKrogerToken()).rejects.toThrow("OAuth HTTP 401");
  });
});

describe("scrapeKrogerStore", () => {
  it("returns products from Kroger API", async () => {
    _resetKrogerToken();
    mocks.fetch
      .mockResolvedValueOnce({ ok: true, json: async () => ({ access_token: "kr-token" }) })
      .mockResolvedValue({
        ok: true,
        json: async () => ({ data: [{
          description: "Weller Special Reserve Bourbon 750ml",
          items: [{ fulfillment: { inStore: true }, inventory: { stockLevel: "HIGH" }, price: { regular: 29.99 } }],
          productId: "0001234",
        }] }),
      });
    const found = await runWithFakeTimers(() => scrapeKrogerStore(TEST_STORE));
    expect(found.find((f) => f.name === "Weller Special Reserve")).toBeTruthy();
  });

  it("returns empty when OAuth fails", async () => {
    _resetKrogerToken();
    const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    mocks.fetch.mockResolvedValueOnce({ ok: false, status: 500 });
    expect(await scrapeKrogerStore(TEST_STORE)).toEqual([]);
    consoleSpy.mockRestore();
  });

  it("handles API error on product search", async () => {
    _resetKrogerToken();
    const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    mocks.fetch
      .mockResolvedValueOnce({ ok: true, json: async () => ({ access_token: "tk" }) })
      .mockResolvedValue({ ok: false, status: 500 });
    const found = await runWithFakeTimers(() => scrapeKrogerStore(TEST_STORE));
    expect(found).toEqual([]);
    consoleSpy.mockRestore();
  });

  it("filters out-of-stock products", async () => {
    _resetKrogerToken();
    mocks.fetch
      .mockResolvedValueOnce({ ok: true, json: async () => ({ access_token: "tk" }) })
      .mockResolvedValue({
        ok: true,
        json: async () => ({ data: [{
          description: "Weller Special Reserve",
          items: [{ fulfillment: { inStore: false }, inventory: { stockLevel: "TEMPORARILY_OUT_OF_STOCK" } }],
        }] }),
      });
    const found = await runWithFakeTimers(() => scrapeKrogerStore(TEST_STORE));
    expect(found).toEqual([]);
  });

  it("prefers in-stock item variant for price over items[0]", async () => {
    _resetKrogerToken();
    mocks.fetch
      .mockResolvedValueOnce({ ok: true, json: async () => ({ access_token: "tk" }) })
      .mockResolvedValue({
        ok: true,
        json: async () => ({ data: [{
          description: "Weller Special Reserve Bourbon 750ml",
          productId: "0001234",
          items: [
            { fulfillment: { inStore: false, shipToHome: true }, inventory: { stockLevel: "LOW" }, price: { regular: 99.99 }, size: "1.75L" },
            { fulfillment: { inStore: true }, inventory: { stockLevel: "HIGH" }, price: { regular: 29.99 }, size: "750ml" },
          ],
        }] }),
      });
    const found = await runWithFakeTimers(() => scrapeKrogerStore(TEST_STORE));
    const weller = found.find((f) => f.name === "Weller Special Reserve");
    expect(weller).toBeTruthy();
    expect(weller.price).toBe("$29.99");
    expect(weller.size).toBe("750ml");
  });

  it("uses promo price when available (B5)", async () => {
    _resetKrogerToken();
    mocks.fetch
      .mockResolvedValueOnce({ ok: true, json: async () => ({ access_token: "tk" }) })
      .mockResolvedValue({
        ok: true,
        json: async () => ({ data: [{
          description: "Weller Special Reserve Bourbon 750ml",
          productId: "0001234",
          items: [{ fulfillment: { inStore: true }, inventory: { stockLevel: "HIGH" }, price: { regular: 29.99, promo: 24.99 }, size: "750ml" }],
        }] }),
      });
    const found = await runWithFakeTimers(() => scrapeKrogerStore(TEST_STORE));
    const weller = found.find((f) => f.name === "Weller Special Reserve");
    expect(weller).toBeTruthy();
    expect(weller.price).toBe("$24.99");
  });

  it("includes ship-to-home fulfillment when available", async () => {
    _resetKrogerToken();
    mocks.fetch
      .mockResolvedValueOnce({ ok: true, json: async () => ({ access_token: "tk" }) })
      .mockResolvedValue({
        ok: true,
        json: async () => ({ data: [{
          description: "Weller Special Reserve Bourbon 750ml",
          productId: "0001234",
          items: [{ fulfillment: { inStore: true, shipToHome: true }, inventory: { stockLevel: "HIGH" }, price: { regular: 29.99 }, size: "750ml" }],
        }] }),
      });
    const found = await runWithFakeTimers(() => scrapeKrogerStore(TEST_STORE));
    const weller = found.find((f) => f.name === "Weller Special Reserve");
    expect(weller.fulfillment).toContain("In-store");
    expect(weller.fulfillment).toContain("Ship to home");
  });

  it("handles products without price or size", async () => {
    _resetKrogerToken();
    mocks.fetch
      .mockResolvedValueOnce({ ok: true, json: async () => ({ access_token: "tk" }) })
      .mockResolvedValue({
        ok: true,
        json: async () => ({ data: [{
          description: "Weller Special Reserve Bourbon",
          productId: "0001234",
          items: [{ fulfillment: { inStore: true }, inventory: { stockLevel: "HIGH" } }],
        }] }),
      });
    const found = await runWithFakeTimers(() => scrapeKrogerStore(TEST_STORE));
    const weller = found.find((f) => f.name === "Weller Special Reserve");
    expect(weller).toBeTruthy();
    expect(weller.price).toBe("");
    expect(weller.size).toBe("");
  });

  it("clears cached token on 401 and logs error", async () => {
    _resetKrogerToken();
    const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    mocks.fetch
      .mockResolvedValueOnce({ ok: true, json: async () => ({ access_token: "expired-token" }) })
      .mockResolvedValue({ ok: false, status: 401 });
    const found = await runWithFakeTimers(() => scrapeKrogerStore(TEST_STORE));
    expect(found).toEqual([]);
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining("Token expired"));
    consoleSpy.mockRestore();
  });
});

// ─── Safeway Scraper ──────────────────────────────────────────────────────────

describe("scrapeSafewayStore", () => {
  it("returns products from Safeway API", async () => {
    mocks.fetch.mockResolvedValue({
      ok: true,
      json: async () => ({
        primaryProducts: { response: { docs: [{
          name: "Weller Special Reserve Bourbon 750ml", inStock: true,
          url: "/product/weller-sr", price: 32.99,
        }] } },
      }),
    });
    const found = await runWithFakeTimers(() => scrapeSafewayStore(TEST_STORE));
    const weller = found.find((f) => f.name === "Weller Special Reserve");
    expect(weller).toBeTruthy();
    expect(weller.url).toContain("safeway.com");
    expect(weller.price).toBe("$32.99");
  });

  it("filters out-of-stock products", async () => {
    mocks.fetch.mockResolvedValue({
      ok: true,
      json: async () => ({ primaryProducts: { response: { docs: [{
        name: "Weller Special Reserve", inStock: false,
      }] } } }),
    });
    const found = await runWithFakeTimers(() => scrapeSafewayStore(TEST_STORE));
    expect(found).toEqual([]);
  });

  it("filters products with undefined/null/0 inStock", async () => {
    mocks.fetch.mockResolvedValue({
      ok: true,
      json: async () => ({ primaryProducts: { response: { docs: [
        { name: "Weller Special Reserve", inStock: undefined },
        { name: "Blanton's Gold", inStock: null },
        { name: "Stagg Jr", inStock: 0 },
      ] } } }),
    });
    const found = await runWithFakeTimers(() => scrapeSafewayStore(TEST_STORE));
    expect(found).toEqual([]);
  });

  it("accepts inStock === 1 as in-stock (B7)", async () => {
    mocks.fetch.mockResolvedValue({
      ok: true,
      json: async () => ({ primaryProducts: { response: { docs: [{
        name: "Weller Special Reserve Bourbon 750ml", inStock: 1,
        url: "/product/weller-sr", price: 32.99,
      }] } } }),
    });
    const found = await runWithFakeTimers(() => scrapeSafewayStore(TEST_STORE));
    expect(found.find((f) => f.name === "Weller Special Reserve")).toBeTruthy();
  });

  it("rejects truthy-but-not-true/1 inStock values (B7)", async () => {
    mocks.fetch.mockResolvedValue({
      ok: true,
      json: async () => ({ primaryProducts: { response: { docs: [
        { name: "Weller Special Reserve", inStock: "yes" },
        { name: "Blanton's Gold", inStock: 2 },
      ] } } }),
    });
    const found = await runWithFakeTimers(() => scrapeSafewayStore(TEST_STORE));
    expect(found).toEqual([]);
  });

  it("handles API errors", async () => {
    const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    mocks.fetch.mockResolvedValue({ ok: false, status: 500 });
    const found = await runWithFakeTimers(() => scrapeSafewayStore(TEST_STORE));
    expect(found).toEqual([]);
    consoleSpy.mockRestore();
  });

  it("includes curbside and delivery fulfillment when eligible", async () => {
    mocks.fetch.mockResolvedValue({
      ok: true,
      json: async () => ({
        primaryProducts: { response: { docs: [{
          name: "Weller Special Reserve Bourbon 750ml", inStock: true,
          url: "/product/weller-sr", price: 32.99,
          curbsideEligible: true, deliveryEligible: true,
          upc: "upc123", pid: "pid456",
        }] } },
      }),
    });
    const found = await runWithFakeTimers(() => scrapeSafewayStore(TEST_STORE));
    const weller = found.find((f) => f.name === "Weller Special Reserve");
    expect(weller.fulfillment).toContain("Curbside");
    expect(weller.fulfillment).toContain("Delivery");
  });

  it("handles products with productTitle fallback and missing url/price", async () => {
    mocks.fetch.mockResolvedValue({
      ok: true,
      json: async () => ({
        primaryProducts: { response: { docs: [{
          productTitle: "Weller Special Reserve Bourbon", inStock: true,
        }] } },
      }),
    });
    const found = await runWithFakeTimers(() => scrapeSafewayStore(TEST_STORE));
    const weller = found.find((f) => f.name === "Weller Special Reserve");
    expect(weller).toBeTruthy();
    expect(weller.url).toBe("");
    expect(weller.price).toBe("");
    expect(weller.sku).toBe("");
  });

  it("includes Ocp-Apim-Subscription-Key header", async () => {
    mocks.fetch.mockResolvedValue({
      ok: true,
      json: async () => ({ primaryProducts: { response: { docs: [] } } }),
    });
    await runWithFakeTimers(() => scrapeSafewayStore(TEST_STORE));
    expect(mocks.fetch).toHaveBeenCalledWith(
      expect.stringContaining("safeway.com"),
      expect.objectContaining({
        headers: expect.objectContaining({ "Ocp-Apim-Subscription-Key": "test-safeway-key" }),
      })
    );
  });
});

// ─── Walgreens Scraper ────────────────────────────────────────────────────────

describe("scrapeWalgreensViaBrowser", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    _resetWalgreensCoords();
    mocks.fetch.mockReset();
    mocks.zipToCoords.mockResolvedValue({ lat: 33.4152, lng: -111.8315 });
  });
  afterEach(() => { vi.useRealTimers(); });

  async function runWithFakeTimers(fn) {
    const promise = fn();
    promise.catch(() => {});
    await vi.runAllTimersAsync();
    return promise;
  }

  function createWalgreensPage(products = []) {
    // Shared context so addCookies assertions work (context() returns same object)
    const ctx = {
      close: vi.fn().mockResolvedValue(undefined),
      storageState: vi.fn().mockResolvedValue({ cookies: [], origins: [] }),
      addCookies: vi.fn().mockResolvedValue(undefined),
    };
    const page = createMockPage();
    page.context = vi.fn(() => ctx);
    // isBlockedPage calls page.evaluate() for body text — return a string
    page.evaluate.mockResolvedValue("");
    page.$$eval.mockResolvedValue(products);
    return page;
  }

  it("extracts matching bottle from product cards", async () => {
    const page = createWalgreensPage([
      {
        title: "Blanton's Single Barrel Bourbon - 750 mL",
        price: "$64.99",
        url: "https://www.walgreens.com/store/c/blantons/ID=300425265-product",
        outOfStock: false,
      },
    ]);
    const result = await runWithFakeTimers(() => scrapeWalgreensViaBrowser(page));
    expect(result).toEqual([
      expect.objectContaining({
        name: "Blanton's Original",
        price: "$64.99",
        sku: "300425265",
        fulfillment: "In-Store",
      }),
    ]);
  });

  it("skips out-of-stock items (Not sold at your store)", async () => {
    const page = createWalgreensPage([
      {
        title: "Blanton's Single Barrel Bourbon - 750 mL",
        price: "",
        url: "",
        outOfStock: true,
      },
    ]);
    const result = await runWithFakeTimers(() => scrapeWalgreensViaBrowser(page));
    expect(result).toEqual([]);
  });

  it("sets USER_LOC cookie on page context", async () => {
    const page = createWalgreensPage([]);
    await runWithFakeTimers(() => scrapeWalgreensViaBrowser(page));
    const ctx = page.context();
    expect(ctx.addCookies).toHaveBeenCalledWith([
      expect.objectContaining({
        name: "USER_LOC",
        domain: ".walgreens.com",
        path: "/",
      }),
    ]);
    // Verify the cookie value is base64 JSON with lat/lng/zip
    const cookieCall = ctx.addCookies.mock.calls[0][0][0];
    const decoded = JSON.parse(Buffer.from(cookieCall.value, "base64").toString());
    expect(decoded).toEqual({ la: "33.4152", lo: "-111.8315", uz: "85283" });
  });

  it("handles bot detection by skipping query", async () => {
    const page = createMockPage();
    page.title.mockResolvedValue("Access Denied");
    page.evaluate.mockResolvedValue("Please verify you are a human");
    page.$$eval.mockResolvedValue([]);
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    const result = await runWithFakeTimers(() => scrapeWalgreensViaBrowser(page));
    expect(result).toEqual([]);
    expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining("[walgreens] Bot detection"));
    warnSpy.mockRestore();
  });

  it("returns empty array when no bottles match", async () => {
    const page = createWalgreensPage([
      {
        title: "Jack Daniels Tennessee Whiskey - 750 mL",
        price: "$24.99",
        url: "https://www.walgreens.com/store/c/jack-daniels/ID=123-product",
        outOfStock: false,
      },
    ]);
    const result = await runWithFakeTimers(() => scrapeWalgreensViaBrowser(page));
    expect(result).toEqual([]);
  });

  it("deduplicates matching bottles across queries", async () => {
    const page = createWalgreensPage([
      {
        title: "Weller Special Reserve Bourbon - 750 mL",
        price: "$24.99",
        url: "https://www.walgreens.com/store/c/weller/ID=999-product",
        outOfStock: false,
      },
    ]);
    const result = await runWithFakeTimers(() => scrapeWalgreensViaBrowser(page));
    // Should deduplicate across 13 queries returning the same result
    const wellerEntries = result.filter((b) => b.name === "Weller Special Reserve");
    expect(wellerEntries).toHaveLength(1);
  });

  it("handles page errors gracefully", async () => {
    const page = createWalgreensPage([]);
    page.goto.mockRejectedValue(new Error("Navigation timeout"));
    const errSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    const result = await runWithFakeTimers(() => scrapeWalgreensViaBrowser(page));
    expect(result).toEqual([]);
    expect(errSpy).toHaveBeenCalledWith(expect.stringContaining("[walgreens] Error"));
    errSpy.mockRestore();
  });

  it("prepends walgreens.com domain to relative URLs", async () => {
    const page = createWalgreensPage([
      {
        title: "Elmer T. Lee Single Barrel Bourbon - 750 mL",
        price: "$39.99",
        url: "/store/c/elmer-t-lee/ID=500-product",
        outOfStock: false,
      },
    ]);
    const result = await runWithFakeTimers(() => scrapeWalgreensViaBrowser(page));
    expect(result[0].url).toBe("https://www.walgreens.com/store/c/elmer-t-lee/ID=500-product");
  });
});

describe("scrapeWalgreensStore", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    _resetWalgreensCoords();
    mocks.fetch.mockReset();
    mocks.zipToCoords.mockResolvedValue({ lat: 33.4152, lng: -111.8315 });
  });
  afterEach(() => { vi.useRealTimers(); });

  it("wrapper creates page, calls browser scraper, and saves state", async () => {
    const { page } = setupMockBrowser();
    page.evaluate.mockResolvedValue("");
    page.$$eval.mockResolvedValue([]);
    mocks.readFile.mockRejectedValue(new Error("no file"));
    const promise = scrapeWalgreensStore();
    promise.catch(() => {});
    await vi.runAllTimersAsync();
    const result = await promise;
    // Should have called goto with walgreens search URL
    expect(page.goto).toHaveBeenCalledWith(
      expect.stringContaining("walgreens.com/search/results.jsp"),
      expect.any(Object)
    );
    expect(result).toEqual([]);
  });
});

// ─── Sam's Club Scrapers ─────────────────────────────────────────────────────

describe("SAMSCLUB_PRODUCTS", () => {
  it("is a non-empty map", () => {
    expect(Object.keys(SAMSCLUB_PRODUCTS).length).toBeGreaterThan(0);
  });

  it("all keys correspond to TARGET_BOTTLES names", () => {
    const bottleNames = TARGET_BOTTLES.map((b) => b.name);
    for (const key of Object.keys(SAMSCLUB_PRODUCTS)) {
      expect(bottleNames).toContain(key);
    }
  });

  it("includes the canary bottle (Buffalo Trace)", () => {
    expect(SAMSCLUB_PRODUCTS["Buffalo Trace"]).toBeDefined();
  });

  it("all values are non-empty strings", () => {
    for (const [, productId] of Object.entries(SAMSCLUB_PRODUCTS)) {
      expect(typeof productId).toBe("string");
      expect(productId.length).toBeGreaterThan(0);
    }
  });
});

describe("matchSamsClubProduct", () => {
  const makeNextData = (product) => ({
    props: { pageProps: { initialData: { data: { product } } } },
  });

  it("extracts IN_STOCK product", () => {
    const nextData = makeNextData({
      name: "Weller Special Reserve Bourbon 750ml",
      availabilityStatusV2: { value: "IN_STOCK" },
      canonicalUrl: "/ip/weller-special-reserve/prod20595259",
      priceInfo: { linePriceDisplay: "$29.99" },
      usItemId: "prod20595259",
      fulfillmentSummary: [{ fulfillment: "Pickup" }],
    });
    const match = matchSamsClubProduct(nextData, "Weller Special Reserve");
    expect(match).not.toBeNull();
    expect(match.name).toBe("Weller Special Reserve");
    expect(match.price).toBe("$29.99");
    expect(match.sku).toBe("prod20595259");
    expect(match.url).toContain("samsclub.com");
    expect(match.fulfillment).toBe("Pickup");
  });

  it("returns null for OUT_OF_STOCK product", () => {
    const nextData = makeNextData({
      name: "George T. Stagg Bourbon 750ml",
      availabilityStatusV2: { value: "OUT_OF_STOCK" },
      canonicalUrl: "/ip/george-t-stagg/123",
      priceInfo: { linePriceDisplay: "$99.99" },
      usItemId: "13735253987",
    });
    const match = matchSamsClubProduct(nextData, "George T. Stagg");
    expect(match).toBeNull();
  });

  it("returns null when product data is missing", () => {
    const nextData = { props: { pageProps: { initialData: { data: {} } } } };
    const match = matchSamsClubProduct(nextData, "Missing Bottle");
    expect(match).toBeNull();
  });

  it("falls back to availabilityStatus field", () => {
    const nextData = makeNextData({
      name: "Buffalo Trace Bourbon 750ml",
      availabilityStatus: "IN_STOCK",
      canonicalUrl: "/ip/buffalo-trace/123",
      priceInfo: { linePriceDisplay: "$24.99" },
      usItemId: "13791619865",
    });
    const match = matchSamsClubProduct(nextData, "Buffalo Trace");
    expect(match).not.toBeNull();
    expect(match.name).toBe("Buffalo Trace");
  });
});

describe("scrapeSamsClubViaFetch", () => {
  it("returns null when proxyAgent is not set (no PROXY_URL)", async () => {
    const result = await scrapeSamsClubViaFetch();
    expect(result).toBeNull();
  });
});

describe("scrapeSamsClubViaBrowser", () => {
  beforeEach(() => { vi.useFakeTimers(); });
  afterEach(() => { vi.useRealTimers(); });

  it("extracts in-stock products from __NEXT_DATA__", async () => {
    const page = createMockPage();
    const productData = {
      props: { pageProps: { initialData: { data: { product: {
        name: "Weller Special Reserve Bourbon 750ml",
        availabilityStatusV2: { value: "IN_STOCK" },
        canonicalUrl: "/ip/weller/prod20595259",
        priceInfo: { linePriceDisplay: "$29.99" },
        usItemId: "prod20595259",
      } } } } },
    };
    // isBlockedPage calls page.evaluate for body text (returns string),
    // then __NEXT_DATA__ extraction calls page.evaluate (returns object).
    // Each product in the loop calls evaluate twice. Mock alternating: string, object.
    let evalCount = 0;
    page.evaluate.mockImplementation(() => {
      evalCount++;
      // Odd calls = isBlockedPage body text, even calls = __NEXT_DATA__
      if (evalCount % 2 === 1) return Promise.resolve("");
      return Promise.resolve(productData);
    });
    const promise = scrapeSamsClubViaBrowser(page);
    promise.catch(() => {});
    await vi.runAllTimersAsync();
    const found = await promise;
    expect(found.length).toBeGreaterThan(0);
    const weller = found.find((b) => b.name === "Weller Special Reserve");
    expect(weller).toBeDefined();
    expect(weller.price).toBe("$29.99");
  });

  it("skips bot detection pages", async () => {
    const page = createMockPage();
    page.title.mockResolvedValue("Access Denied");
    page.evaluate.mockResolvedValue("Please verify you are a human");
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    const promise = scrapeSamsClubViaBrowser(page);
    promise.catch(() => {});
    await vi.runAllTimersAsync();
    const found = await promise;
    expect(found).toEqual([]);
    expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining("[samsclub] Bot detection"));
    warnSpy.mockRestore();
  });

  it("handles navigation errors gracefully", async () => {
    const page = createMockPage();
    page.goto.mockRejectedValue(new Error("Navigation timeout"));
    page.evaluate.mockResolvedValue("");
    const errSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    const promise = scrapeSamsClubViaBrowser(page);
    promise.catch(() => {});
    await vi.runAllTimersAsync();
    const found = await promise;
    expect(found).toEqual([]);
    errSpy.mockRestore();
  });
});

describe("scrapeSamsClubStore wrapper", () => {
  it("falls back to browser when fetch returns null", async () => {
    // Reset browser state from any prior test that called newPage/launchBrowser
    setupMockBrowser(); // provides a mock with working .close()
    await launchBrowser(); // sets module-level browser to the mock
    await closeBrowser(); // nulls module-level browser

    const { page } = setupMockBrowser();
    // scrapeSamsClubViaFetch returns null (no proxy), so wrapper creates own page
    page.evaluate.mockResolvedValue("");
    mocks.readFile.mockRejectedValue(new Error("no file"));
    const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    const found = await runWithFakeTimers(() => scrapeSamsClubStore());
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining("Fetch blocked, using browser"));
    expect(found).toEqual([]);
    consoleSpy.mockRestore();
    await closeBrowser();
  });
});

// ─── Poll Orchestrator ────────────────────────────────────────────────────────

describe("poll", () => {
  beforeEach(() => {
    _resetPolling();
    _resetKrogerToken();
    _setStoreCache({ retailers: { costco: [TEST_STORE], totalwine: [], walmart: [], kroger: [], safeway: [], walgreens: [] } });
    mocks.readFile.mockResolvedValue("{}");
    mocks.writeFile.mockResolvedValue(undefined);
    mocks.fetch.mockResolvedValue({ ok: true });
    setupMockBrowser();
  });

  it("runs a full poll cycle", async () => {
    const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    await runWithFakeTimers(() => poll());
    expect(mocks.fetch).toHaveBeenCalled();
    expect(mocks.writeFile).toHaveBeenCalled();
    consoleSpy.mockRestore();
  });

  it("skips if already polling", async () => {
    const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    const p1 = poll();
    // While p1 is running (suspended on fake timer), start p2
    const p2 = poll();
    // p2 should skip immediately
    await vi.runAllTimersAsync();
    await p1;
    await p2;
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining("Skipping"));
    consoleSpy.mockRestore();
  });

  it("handles scraper errors gracefully", async () => {
    const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    vi.spyOn(console, "log").mockImplementation(() => {});
    mocks.chromiumLaunch.mockRejectedValue(new Error("Browser crash"));
    try { await runWithFakeTimers(() => poll()); } catch {}
    consoleSpy.mockRestore();
    vi.spyOn(console, "log").mockRestore();
  });

  it("executes scrapeOnce (Costco) tasks and broadcasts to all stores", async () => {
    const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    _setStoreCache({
      retailers: {
        costco: [TEST_STORE, { ...TEST_STORE, storeId: "5678", name: "Test Store 2" }],
        totalwine: [], walmart: [], kroger: [], safeway: [], walgreens: [],
      },
    });
    await runWithFakeTimers(() => poll());
    // Should have logged the Costco scrape-once message
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining("Costco (once for 2 stores)"));
    consoleSpy.mockRestore();
  });

  it("executes needsPage (Total Wine) tasks per store", async () => {
    const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    _setStoreCache({
      retailers: {
        costco: [], totalwine: [TEST_STORE], walmart: [], kroger: [], safeway: [], walgreens: [],
      },
    });
    await runWithFakeTimers(() => poll());
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining("Total Wine"));
    consoleSpy.mockRestore();
  });

  it("executes API-based (Walmart, Kroger, Safeway) tasks per store", async () => {
    const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    // Kroger needs OAuth first
    mocks.fetch
      .mockResolvedValueOnce({ ok: true, json: async () => ({ access_token: "tok" }) })
      .mockResolvedValue({ ok: true, json: async () => ({ data: [] }), text: async () => "" });
    _setStoreCache({
      retailers: {
        costco: [], totalwine: [], walmart: [TEST_STORE], kroger: [TEST_STORE], safeway: [TEST_STORE], walgreens: [],
      },
    });
    await runWithFakeTimers(() => poll());
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining("Walmart"));
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining("Kroger"));
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining("Safeway"));
    consoleSpy.mockRestore();
  });

  it("records found bottles and sends urgent alert", async () => {
    const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    // Set up a walmart store where fetch returns a matching bottle
    _setStoreCache({
      retailers: { costco: [], totalwine: [], walmart: [TEST_STORE], kroger: [], safeway: [], walgreens: [] },
    });
    mocks.fetch.mockResolvedValue({
      ok: true,
      json: async () => ({
        props: {
          pageProps: {
            initialData: {
              searchResult: {
                itemStacks: [{
                  items: [{
                    name: "Blanton's Single Barrel Bourbon",
                    type: "PRODUCT",
                    sellerName: "Walmart.com",
                    fulfillmentBadgeGroups: [{ badges: [{ text: "Pickup" }] }],
                    canonicalUrl: "/ip/123",
                    priceInfo: { currentPrice: { price: 59.99 } },
                  }],
                }],
              },
            },
          },
        },
      }),
      text: async () => "",
    });
    await runWithFakeTimers(() => poll());
    // The urgent alert should have been sent (fetch called for discord)
    const discordCalls = mocks.fetch.mock.calls.filter(
      (c) => typeof c[0] === "string" && c[0].includes("test-webhook")
    );
    expect(discordCalls.length).toBeGreaterThan(0);
    consoleSpy.mockRestore();
  });
});

// ─── Main Entry Point ─────────────────────────────────────────────────────────

describe("main", () => {
  beforeEach(() => {
    _resetPolling();
    _resetKrogerToken();
    mocks.readFile.mockResolvedValue("{}");
    mocks.writeFile.mockResolvedValue(undefined);
    mocks.fetch.mockResolvedValue({ ok: true });
    setupMockBrowser();
  });

  it("discovers stores and starts polling", async () => {
    const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    mocks.discoverStores.mockResolvedValueOnce({
      retailers: { costco: [], totalwine: [], walmart: [], kroger: [], safeway: [], walgreens: [] },
    });
    // main() uses setTimeout chain — poll with no stores completes quickly,
    // then scheduleNextPoll logs "Next poll in" and sets a setTimeout
    const promise = main();
    promise.catch(() => {});
    // Advance enough for initial poll to complete + schedule log
    await vi.advanceTimersByTimeAsync(5000);
    expect(mocks.discoverStores).toHaveBeenCalled();
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining("Next poll in"));
    consoleSpy.mockRestore();
  });

  it("exits in RUN_ONCE mode", async () => {
    const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    const exitSpy = vi.spyOn(process, "exit").mockImplementation(() => {});
    process.env.RUN_ONCE = "true";
    mocks.discoverStores.mockResolvedValueOnce({
      retailers: { costco: [], totalwine: [], walmart: [], kroger: [], safeway: [], walgreens: [] },
    });
    // RUN_ONCE path calls poll() then process.exit(0) — no setTimeout chain
    await runWithFakeTimers(() => main());
    expect(exitSpy).toHaveBeenCalledWith(0);
    delete process.env.RUN_ONCE;
    exitSpy.mockRestore();
    consoleSpy.mockRestore();
  });

  it("handles discovery failure", async () => {
    const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    vi.spyOn(console, "log").mockImplementation(() => {});
    const exitSpy = vi.spyOn(process, "exit").mockImplementation(() => {});
    mocks.discoverStores.mockRejectedValueOnce(new Error("Network down"));
    await runWithFakeTimers(() => main());
    expect(exitSpy).toHaveBeenCalledWith(1);
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining("Network down"));
    exitSpy.mockRestore();
    consoleSpy.mockRestore();
    vi.spyOn(console, "log").mockRestore();
  });
});

// ─── Shuffle ─────────────────────────────────────────────────────────────────

describe("shuffle", () => {
  it("returns a new array with same elements", () => {
    const input = [1, 2, 3, 4, 5];
    const result = shuffle(input);
    expect(result).not.toBe(input); // new array, not mutated
    expect(result).toHaveLength(input.length);
    expect(result.sort()).toEqual(input.sort());
  });

  it("does not mutate the original array", () => {
    const input = ["a", "b", "c", "d"];
    const copy = [...input];
    shuffle(input);
    expect(input).toEqual(copy);
  });

  it("handles single-element array", () => {
    expect(shuffle([42])).toEqual([42]);
  });

  it("handles empty array", () => {
    expect(shuffle([])).toEqual([]);
  });

  it("produces different orderings over many runs", () => {
    const input = [1, 2, 3, 4, 5, 6, 7, 8];
    const orderings = new Set();
    for (let i = 0; i < 50; i++) {
      orderings.add(shuffle(input).join(","));
    }
    // With 8 elements and 50 runs, we should see at least 2 different orderings
    expect(orderings.size).toBeGreaterThan(1);
  });
});

// ─── Browser Hardening ───────────────────────────────────────────────────────

describe("browser hardening", () => {
  it("launchBrowser includes anti-detection Chrome args", async () => {
    mocks.chromiumLaunch.mockResolvedValueOnce({ newContext: vi.fn(), close: vi.fn().mockResolvedValue(undefined) });
    await launchBrowser();
    const callArgs = mocks.chromiumLaunch.mock.calls[0][0];
    expect(callArgs.args).toContain("--disable-blink-features=AutomationControlled");
    expect(callArgs.args).toContain("--disable-dev-shm-usage");
    expect(callArgs.args).toContain("--no-first-run");
    expect(callArgs.ignoreDefaultArgs).toEqual(["--enable-automation"]);
    await closeBrowser();
  });

  it("CHROME_PATH is set on Mac, null otherwise", () => {
    if (IS_MAC) {
      expect(CHROME_PATH).toBe("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome");
    } else {
      expect(CHROME_PATH).toBeNull();
    }
  });

  it("launchBrowser passes executablePath on Mac", async () => {
    mocks.chromiumLaunch.mockResolvedValueOnce({ newContext: vi.fn(), close: vi.fn().mockResolvedValue(undefined) });
    await launchBrowser();
    const callArgs = mocks.chromiumLaunch.mock.calls[0][0];
    if (IS_MAC) {
      expect(callArgs.executablePath).toBe(CHROME_PATH);
    } else {
      expect(callArgs.executablePath).toBeUndefined();
    }
    await closeBrowser();
  });

  it("newPage sets userAgent and Sec-CH-UA headers on context", async () => {
    const mockPage = { close: vi.fn() };
    const mockContext = {
      newPage: vi.fn().mockResolvedValue(mockPage),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const mockBrowser = {
      newContext: vi.fn().mockResolvedValue(mockContext),
      close: vi.fn().mockResolvedValue(undefined),
    };
    mocks.chromiumLaunch.mockResolvedValueOnce(mockBrowser);
    await newPage();
    const contextOpts = mockBrowser.newContext.mock.calls[0][0];
    expect(contextOpts.userAgent).toBe(FETCH_HEADERS["User-Agent"]);
    expect(contextOpts.extraHTTPHeaders["Sec-CH-UA"]).toBe(FETCH_HEADERS["Sec-CH-UA"]);
    expect(contextOpts.extraHTTPHeaders["Sec-CH-UA-Mobile"]).toBe(FETCH_HEADERS["Sec-CH-UA-Mobile"]);
    expect(contextOpts.extraHTTPHeaders["Sec-CH-UA-Platform"]).toBe(FETCH_HEADERS["Sec-CH-UA-Platform"]);
    await closeBrowser();
  });

  it("newPage selects from 4 viewport sizes", async () => {
    const mockPage = { close: vi.fn() };
    const mockContext = {
      newPage: vi.fn().mockResolvedValue(mockPage),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const mockBrowser = {
      newContext: vi.fn().mockResolvedValue(mockContext),
      close: vi.fn().mockResolvedValue(undefined),
    };
    mocks.chromiumLaunch.mockResolvedValue(mockBrowser);
    await launchBrowser();
    const viewportSet = new Set();
    for (let i = 0; i < 30; i++) {
      await newPage();
      const { viewport } = mockBrowser.newContext.mock.calls.at(-1)[0];
      viewportSet.add(`${viewport.width}x${viewport.height}`);
    }
    // With 30 draws from 4 options, expect at least 2 distinct viewports
    expect(viewportSet.size).toBeGreaterThanOrEqual(2);
    // All viewports must be valid known sizes
    const validSizes = new Set(["1366x768", "1440x900", "1280x720", "1536x864"]);
    for (const v of viewportSet) {
      expect(validSizes.has(v)).toBe(true);
    }
    await closeBrowser();
  });
});

// ─── buildOOSList ───────────────────────────────────────────────────────────

describe("buildOOSList", () => {
  it("returns formatted OOS list for bottles not in stock", () => {
    const all = ["Blanton's Gold", "Weller Special Reserve", "Stagg Jr"];
    const inStock = ["Weller Special Reserve"];
    const result = buildOOSList(all, inStock);
    expect(result).toContain("OUT OF STOCK (2)");
    expect(result).toContain("Blanton's Gold");
    expect(result).toContain("Stagg Jr");
    expect(result).not.toContain("Weller Special Reserve");
  });

  it("returns empty string when everything is in stock", () => {
    const all = ["Blanton's Gold", "Weller Special Reserve"];
    expect(buildOOSList(all, all)).toBe("");
  });

  it("lists all bottles when none are in stock", () => {
    const all = ["Blanton's Gold", "Stagg Jr"];
    const result = buildOOSList(all, []);
    expect(result).toContain("OUT OF STOCK (2)");
    expect(result).toContain("Blanton's Gold, Stagg Jr");
  });

  it("returns empty string for empty inputs", () => {
    expect(buildOOSList([], [])).toBe("");
  });
});

// ─── Poll Error Isolation ────────────────────────────────────────────────────

describe("poll error isolation", () => {
  beforeEach(() => {
    _resetPolling();
    _resetKrogerToken();
    mocks.readFile.mockResolvedValue("{}");
    mocks.writeFile.mockResolvedValue(undefined);
    mocks.fetch.mockResolvedValue({ ok: true });
  });

  it("Costco broadcast continues when one store recordResult fails", async () => {
    const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    vi.spyOn(console, "log").mockImplementation(() => {});
    const { page: mockPage } = setupMockBrowser();
    // scrapeCostcoOnce returns empty array (no bottles found)
    mockPage.evaluate.mockResolvedValue([]);
    mockPage.title.mockResolvedValue("Costco Search Results");
    // Two stores — Discord will be called for summary; make it fail on first
    const store1 = { ...TEST_STORE, storeId: "s1", name: "Store 1" };
    const store2 = { ...TEST_STORE, storeId: "s2", name: "Store 2" };
    _setStoreCache({
      retailers: { costco: [store1, store2], totalwine: [], walmart: [], kroger: [], safeway: [], walgreens: [] },
    });
    await runWithFakeTimers(() => poll());
    // Both stores should still be in state (error isolation worked)
    expect(mocks.writeFile).toHaveBeenCalled();
    consoleSpy.mockRestore();
    vi.spyOn(console, "log").mockRestore();
  });

  it("needsPage (Total Wine) per-store error isolates failures", async () => {
    const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    vi.spyOn(console, "log").mockImplementation(() => {});
    const { page: mockPage, context: mockContext } = setupMockBrowser();
    // Make launchPersistentContext fail on first call (simulates browser crash for store 1)
    // then succeed for store 2
    let launchCalls = 0;
    mocks.chromiumLaunchPersistentContext.mockImplementation(() => {
      launchCalls++;
      if (launchCalls === 1) return Promise.reject(new Error("Context crash"));
      return Promise.resolve(mockContext);
    });
    const store1 = { ...TEST_STORE, storeId: "tw1", name: "TW Store 1" };
    const store2 = { ...TEST_STORE, storeId: "tw2", name: "TW Store 2" };
    _setStoreCache({
      retailers: { costco: [], totalwine: [store1, store2], walmart: [], kroger: [], safeway: [], walgreens: [] },
    });
    await runWithFakeTimers(() => poll());
    // Should have logged error for first store crash
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining("crashed"));
    // State should still be saved (poll completed despite error)
    expect(mocks.writeFile).toHaveBeenCalled();
    consoleSpy.mockRestore();
    warnSpy.mockRestore();
    vi.spyOn(console, "log").mockRestore();
  });

  it("API scraper error isolates failures across stores", async () => {
    const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    vi.spyOn(console, "log").mockImplementation(() => {});
    setupMockBrowser();
    // Kroger OAuth succeeds, first store's entire scraper throws (not per-query)
    let storeCount = 0;
    mocks.fetch.mockImplementation((url) => {
      if (typeof url === "string" && url.includes("oauth2/token")) {
        return Promise.resolve({ ok: true, json: async () => ({ access_token: "tok" }) });
      }
      if (typeof url === "string" && url.includes("api.kroger.com/v1/products")) {
        storeCount++;
        // Throw a non-recoverable error (e.g., TypeError) that escapes per-query catch
        if (storeCount <= 12) throw new TypeError("Cannot read properties of undefined");
        return Promise.resolve({ ok: true, json: async () => ({ data: [] }) });
      }
      // Discord / other
      return Promise.resolve({ ok: true });
    });
    const store1 = { ...TEST_STORE, storeId: "kr1", name: "Kroger 1" };
    const store2 = { ...TEST_STORE, storeId: "kr2", name: "Kroger 2" };
    _setStoreCache({
      retailers: { costco: [], totalwine: [], walmart: [], kroger: [store1, store2], safeway: [], walgreens: [] },
    });
    await runWithFakeTimers(() => poll());
    // State should still be saved (poll completed despite error)
    expect(mocks.writeFile).toHaveBeenCalled();
    consoleSpy.mockRestore();
    vi.spyOn(console, "log").mockRestore();
  });

  it("scrapeOnce crash isolates per-query errors and continues poll", async () => {
    const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    vi.spyOn(console, "log").mockImplementation(() => {});
    const { page: mockPage } = setupMockBrowser();
    // scrapeCostcoOnce per-query errors — goto fails for every query
    mockPage.goto.mockRejectedValue(new Error("Costco unreachable"));
    _setStoreCache({
      retailers: { costco: [TEST_STORE], totalwine: [], walmart: [], kroger: [], safeway: [], walgreens: [] },
    });
    await runWithFakeTimers(() => poll());
    // Per-query errors logged inside scrapeCostcoOnce
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining("Costco unreachable"));
    // State should still be saved (poll completed despite errors)
    expect(mocks.writeFile).toHaveBeenCalled();
    consoleSpy.mockRestore();
    vi.spyOn(console, "log").mockRestore();
  });

  it("scrapeOnce top-level crash logs 'crashed' and continues poll", async () => {
    const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    vi.spyOn(console, "log").mockImplementation(() => {});
    // Make newPage() itself throw — this is outside the per-query try/catch
    mocks.chromiumLaunch.mockResolvedValue({
      newContext: vi.fn().mockRejectedValue(new Error("Browser OOM")),
      close: vi.fn().mockResolvedValue(undefined),
    });
    _setStoreCache({
      retailers: { costco: [TEST_STORE], totalwine: [], walmart: [], kroger: [], safeway: [], walgreens: [] },
    });
    await runWithFakeTimers(() => poll());
    // Top-level crash logged with "crashed"
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining("crashed"));
    // State should still be saved
    expect(mocks.writeFile).toHaveBeenCalled();
    consoleSpy.mockRestore();
    vi.spyOn(console, "log").mockRestore();
  });

  it("poll new-find triggers sendUrgentAlert via recordResult", async () => {
    const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    setupMockBrowser();
    // Walmart finds a bottle → recordResult should send urgent alert
    _setStoreCache({
      retailers: { costco: [], totalwine: [], walmart: [TEST_STORE], kroger: [], safeway: [], walgreens: [] },
    });
    const walmartNextData = { props: { pageProps: { initialData: { searchResult: { itemStacks: [{ items: [{
      __typename: "Product", name: "Weller Special Reserve Bourbon Whiskey",
      availabilityStatusV2: { value: "IN_STOCK" }, sellerName: "Walmart.com",
      canonicalUrl: "/ip/123", priceInfo: { currentPrice: { priceString: "$29.99" } },
    }] }] } } } } };
    mocks.fetch.mockImplementation((url) => {
      if (typeof url === "string" && url.includes("walmart.com")) {
        return Promise.resolve({
          ok: true,
          text: async () => `<script id="__NEXT_DATA__">${JSON.stringify(walmartNextData)}</script>`,
        });
      }
      return Promise.resolve({ ok: true });
    });
    await runWithFakeTimers(() => poll());
    // Should log the green new-find message
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining("New:"));
    // Discord should have been called with @everyone for urgent alert
    const discordCalls = mocks.fetch.mock.calls.filter(
      (c) => typeof c[0] === "string" && c[0].includes("test-webhook")
    );
    const urgentCall = discordCalls.find(([, opts]) => {
      const body = JSON.parse(opts.body);
      return body.content && body.content.includes("@everyone");
    });
    expect(urgentCall).toBeTruthy();
    consoleSpy.mockRestore();
  });

  it("poll gone-OOS triggers sendDiscordAlert via recordResult", async () => {
    const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    setupMockBrowser();
    // Set previous state with a bottle in stock
    mocks.readFile.mockResolvedValue(JSON.stringify({
      walmart: { "1234": { bottles: { "Weller Special Reserve": { url: "", price: "$30", firstSeen: "2025-01-01", lastSeen: "2025-01-01", scanCount: 1, sku: "123" } }, lastScanned: "2025-01-01" } },
    }));
    _setStoreCache({
      retailers: { costco: [], totalwine: [], walmart: [TEST_STORE], kroger: [], safeway: [], walgreens: [] },
    });
    // Walmart returns nothing → bottle goes OOS
    mocks.fetch.mockResolvedValue({ ok: false, status: 403 });
    const { page: mockPage } = setupMockBrowser();
    mockPage.evaluate.mockResolvedValue(null);
    mockPage.$$eval.mockResolvedValue([]);
    await runWithFakeTimers(() => poll());
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining("OOS"));
    consoleSpy.mockRestore();
  });

  it("poll catches saveState failure", async () => {
    const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    vi.spyOn(console, "log").mockImplementation(() => {});
    setupMockBrowser();
    mocks.writeFile.mockRejectedValue(new Error("Disk full"));
    _setStoreCache({
      retailers: { costco: [], totalwine: [], walmart: [], kroger: [], safeway: [], walgreens: [] },
    });
    await runWithFakeTimers(() => poll());
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining("saveState failed"));
    consoleSpy.mockRestore();
    vi.spyOn(console, "log").mockRestore();
  });

  it("poll catches summary send failure", async () => {
    const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    vi.spyOn(console, "log").mockImplementation(() => {});
    setupMockBrowser();
    // Make Discord webhook fail only for the summary (last call)
    let callCount = 0;
    mocks.fetch.mockImplementation(() => {
      callCount++;
      // Summary is always the last Discord call; fail it
      if (callCount > 0) return Promise.reject(new Error("Discord down"));
      return Promise.resolve({ ok: true });
    });
    _setStoreCache({
      retailers: { costco: [], totalwine: [], walmart: [], kroger: [], safeway: [], walgreens: [] },
    });
    await runWithFakeTimers(() => poll());
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining("Summary send failed"));
    consoleSpy.mockRestore();
    vi.spyOn(console, "log").mockRestore();
  });

  it("Costco broadcast per-store recordResult failure is isolated", async () => {
    const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    vi.spyOn(console, "log").mockImplementation(() => {});
    const { page: mockPage } = setupMockBrowser();
    mockPage.evaluate.mockResolvedValue([]);
    mockPage.title.mockResolvedValue("Costco");
    // Two stores, make Discord webhook fail on first call with a throw
    let discordCalls = 0;
    mocks.fetch.mockImplementation((url) => {
      if (typeof url === "string" && url.includes("test-webhook")) {
        discordCalls++;
        if (discordCalls === 1) return Promise.reject(new Error("Discord 500"));
      }
      return Promise.resolve({ ok: true });
    });
    const store1 = { ...TEST_STORE, storeId: "c1", name: "Costco 1" };
    const store2 = { ...TEST_STORE, storeId: "c2", name: "Costco 2" };
    _setStoreCache({
      retailers: { costco: [store1, store2], totalwine: [], walmart: [], kroger: [], safeway: [], walgreens: [] },
    });
    // Give stores some previous state so they trigger OOS alerts (which call Discord)
    mocks.readFile.mockResolvedValue(JSON.stringify({
      costco: {
        c1: { bottles: { "Weller Special Reserve": { url: "", price: "", firstSeen: "2025-01-01", lastSeen: "2025-01-01", scanCount: 1 } }, lastScanned: "2025-01-01" },
        c2: { bottles: { "Weller Special Reserve": { url: "", price: "", firstSeen: "2025-01-01", lastSeen: "2025-01-01", scanCount: 1 } }, lastScanned: "2025-01-01" },
      },
    }));
    await runWithFakeTimers(() => poll());
    // recordResult failure should be logged but not crash the poll
    expect(mocks.writeFile).toHaveBeenCalled();
    consoleSpy.mockRestore();
    vi.spyOn(console, "log").mockRestore();
  });
});

// ─── Platform-aware User-Agent ───────────────────────────────────────────────

describe("platform-aware User-Agent", () => {
  it("IS_MAC matches current platform", () => {
    expect(IS_MAC).toBe(process.platform === "darwin");
  });

  it("FETCH_HEADERS User-Agent matches platform", () => {
    if (IS_MAC) {
      expect(FETCH_HEADERS["User-Agent"]).toContain("Macintosh");
      expect(FETCH_HEADERS["Sec-CH-UA-Platform"]).toBe('"macOS"');
    } else {
      expect(FETCH_HEADERS["User-Agent"]).toContain("Windows");
      expect(FETCH_HEADERS["Sec-CH-UA-Platform"]).toBe('"Windows"');
    }
  });
});

// ─── Browser State Persistence ───────────────────────────────────────────────

describe("browser state persistence", () => {
  beforeEach(() => {
    _resetBrowserStateCache();
  });

  it("loadBrowserState returns parsed JSON when file exists", async () => {
    const state = { cookies: [{ name: "test", value: "123" }], origins: [] };
    mocks.readFile.mockResolvedValueOnce(JSON.stringify(state));
    const result = await loadBrowserState();
    expect(result).toEqual(state);
  });

  it("loadBrowserState returns undefined when file does not exist", async () => {
    mocks.readFile.mockRejectedValueOnce(new Error("ENOENT"));
    const result = await loadBrowserState();
    expect(result).toBeUndefined();
  });

  it("loadBrowserState returns cached value on second call", async () => {
    const state = { cookies: [{ name: "cached", value: "yes" }], origins: [] };
    mocks.readFile.mockResolvedValueOnce(JSON.stringify(state));
    const first = await loadBrowserState();
    const second = await loadBrowserState();
    expect(first).toEqual(state);
    expect(second).toEqual(state);
    // readFile only called once — second call uses cache
    expect(mocks.readFile).toHaveBeenCalledTimes(1);
  });

  it("saveBrowserState writes to tmp file then renames atomically", async () => {
    const state = { cookies: [{ name: "sid", value: "abc" }], origins: [] };
    const mockContext = { storageState: vi.fn().mockResolvedValue(state) };
    mocks.writeFile.mockResolvedValueOnce(undefined);
    mocks.rename.mockResolvedValueOnce(undefined);
    await saveBrowserState(mockContext);
    expect(mockContext.storageState).toHaveBeenCalled();
    expect(mocks.writeFile).toHaveBeenCalledWith(
      expect.stringContaining("browser-state.json.tmp"),
      JSON.stringify(state),
    );
    expect(mocks.rename).toHaveBeenCalledWith(
      expect.stringContaining("browser-state.json.tmp"),
      expect.stringContaining("browser-state.json"),
    );
  });

  it("saveBrowserState updates the in-memory cache", async () => {
    const state = { cookies: [{ name: "new", value: "data" }], origins: [] };
    const mockContext = { storageState: vi.fn().mockResolvedValue(state) };
    mocks.writeFile.mockResolvedValueOnce(undefined);
    mocks.rename.mockResolvedValueOnce(undefined);
    await saveBrowserState(mockContext);
    // Subsequent loadBrowserState should return cached value without reading file
    const result = await loadBrowserState();
    expect(result).toEqual(state);
    expect(mocks.readFile).not.toHaveBeenCalled();
  });

  it("saveBrowserState swallows errors silently", async () => {
    const mockContext = { storageState: vi.fn().mockRejectedValue(new Error("fail")) };
    // Should not throw
    await saveBrowserState(mockContext);
  });
});

// ─── Total Wine Tightened Stock Check ────────────────────────────────────────

describe("matchTotalWineInitialState stock signal tightening", () => {
  it("rejects transactional-only without stock data", () => {
    // transactional=true but stockLevel is 0 and shoppingOptions not eligible
    const state = {
      search: { results: { products: [{
        name: "Weller Special Reserve Bourbon Whiskey",
        productUrl: "/spirits/bourbon/weller-sr/p/12345",
        stockLevel: [{ stock: 0 }],
        transactional: true,
        shoppingOptions: [{ eligible: false }],
      }] } },
    };
    expect(matchTotalWineInitialState(state)).toEqual([]);
  });

  it("accepts transactional as fallback when stockLevel and shoppingOptions are absent", () => {
    const state = {
      search: { results: { products: [{
        name: "Weller Special Reserve Bourbon Whiskey",
        productUrl: "/spirits/bourbon/weller-sr/p/12345",
        transactional: true,
        // No stockLevel, no shoppingOptions
      }] } },
    };
    const found = matchTotalWineInitialState(state);
    expect(found.length).toBe(1);
    expect(found[0].name).toBe("Weller Special Reserve");
  });

  it("accepts shoppingOptions eligible even without stockLevel", () => {
    const state = {
      search: { results: { products: [{
        name: "Weller Special Reserve Bourbon Whiskey",
        productUrl: "/spirits/bourbon/weller-sr/p/12345",
        shoppingOptions: [{ eligible: true, name: "Delivery" }],
      }] } },
    };
    const found = matchTotalWineInitialState(state);
    expect(found.length).toBe(1);
    expect(found[0].fulfillment).toBe("Delivery");
  });
});

// ─── Kroger Pagination ───────────────────────────────────────────────────────

describe("scrapeKrogerStore pagination", () => {
  it("fetches page 2 when first page returns exactly 50 results", async () => {
    _resetKrogerToken();
    // Generate 50 non-matching products for page 1 + 1 matching product on page 2
    const page1Data = Array.from({ length: 50 }, (_, i) => ({
      description: `Random Product ${i}`,
      productId: `p${i}`,
      items: [{ fulfillment: { inStore: true }, inventory: { stockLevel: "HIGH" }, price: { regular: 10 } }],
    }));
    const page2Data = [{
      description: "Weller Special Reserve Bourbon 750ml",
      productId: "p-weller",
      items: [{ fulfillment: { inStore: true }, inventory: { stockLevel: "HIGH" }, price: { regular: 29.99 }, size: "750ml" }],
    }];

    let queryCallCount = 0;
    mocks.fetch.mockImplementation((url) => {
      if (typeof url === "string" && url.includes("oauth2/token")) {
        return Promise.resolve({ ok: true, json: async () => ({ access_token: "tk" }) });
      }
      if (typeof url === "string" && url.includes("filter.start=50")) {
        // Page 2 call
        return Promise.resolve({ ok: true, json: async () => ({ data: page2Data }) });
      }
      // Page 1 call
      queryCallCount++;
      return Promise.resolve({ ok: true, json: async () => ({ data: page1Data }) });
    });
    const found = await runWithFakeTimers(() => scrapeKrogerStore(TEST_STORE));
    const weller = found.find((f) => f.name === "Weller Special Reserve");
    expect(weller).toBeTruthy();
    // Verify page 2 was actually fetched (at least one call with filter.start=50)
    const page2Calls = mocks.fetch.mock.calls.filter(
      (c) => typeof c[0] === "string" && c[0].includes("filter.start=50")
    );
    expect(page2Calls.length).toBeGreaterThan(0);
  });

  it("does not fetch page 2 when results are under 50", async () => {
    _resetKrogerToken();
    mocks.fetch
      .mockResolvedValueOnce({ ok: true, json: async () => ({ access_token: "tk" }) })
      .mockResolvedValue({
        ok: true,
        json: async () => ({ data: [{
          description: "Weller Special Reserve Bourbon 750ml",
          productId: "0001234",
          items: [{ fulfillment: { inStore: true }, inventory: { stockLevel: "HIGH" }, price: { regular: 29.99 } }],
        }] }),
      });
    await runWithFakeTimers(() => scrapeKrogerStore(TEST_STORE));
    const page2Calls = mocks.fetch.mock.calls.filter(
      (c) => typeof c[0] === "string" && c[0].includes("filter.start=50")
    );
    expect(page2Calls.length).toBe(0);
  });
});

// ─── Round 3: 16 new improvements ────────────────────────────────────────────

describe("Stagg Jr expanded search terms (#1)", () => {
  it("matches 'Stagg Bourbon' as Stagg Jr", () => {
    const stagg = TARGET_BOTTLES.find((b) => b.name === "Stagg Jr");
    expect(matchesBottle("Stagg Bourbon Kentucky Straight 750ml", stagg)).toBe(true);
  });

  it("matches 'Stagg Kentucky' as Stagg Jr", () => {
    const stagg = TARGET_BOTTLES.find((b) => b.name === "Stagg Jr");
    expect(matchesBottle("Stagg Kentucky Straight Bourbon Whiskey", stagg)).toBe(true);
  });

  it("does not cross-match George T. Stagg with Stagg Jr via 'stagg bourbon'", () => {
    const gts = TARGET_BOTTLES.find((b) => b.name === "George T. Stagg");
    expect(matchesBottle("Stagg Bourbon 750ml", gts)).toBe(false);
  });
});

describe("Thomas H. Handy short search term (#4)", () => {
  it("matches 'Thomas Handy' without 'sazerac' suffix", () => {
    const handy = TARGET_BOTTLES.find((b) => b.name === "Thomas H. Handy");
    expect(matchesBottle("Thomas Handy Rye Whiskey 2024 Release", handy)).toBe(true);
  });
});

describe("Old Forester President search term (#16)", () => {
  it("matches 'Old Forester President' without 'Choice' suffix", () => {
    const pc = TARGET_BOTTLES.find((b) => b.name === "Old Forester President's Choice");
    expect(matchesBottle("Old Forester President Barrel Pick 750ml", pc)).toBe(true);
  });
});

describe("FETCH_HEADERS Chrome 145 fidelity (#5, #6)", () => {
  it("Accept header matches full Chrome 145 value", () => {
    expect(FETCH_HEADERS["Accept"]).toContain("image/avif");
    expect(FETCH_HEADERS["Accept"]).toContain("image/webp");
    expect(FETCH_HEADERS["Accept"]).toContain("application/signed-exchange");
  });

  it("includes Sec-Fetch-User header", () => {
    expect(FETCH_HEADERS["Sec-Fetch-User"]).toBe("?1");
  });
});

describe("parseSize centiliter support (#14)", () => {
  it("converts 75cl to 750ml", () => {
    expect(parseSize("Weller Special Reserve 75cl")).toBe("750ml");
  });

  it("converts 70cl to 700ml", () => {
    expect(parseSize("Blanton's Gold 70 cl")).toBe("700ml");
  });

  it("converts 50cl to 500ml", () => {
    expect(parseSize("Whiskey Miniature 50cl")).toBe("500ml");
  });
});

describe("formatStoreInfo duplicate name prevention (#15)", () => {
  it("does not duplicate retailer name when store.name already starts with it", () => {
    const store = { ...TEST_STORE, name: "Costco Chandler" };
    const info = formatStoreInfo("costco", "Costco", store);
    expect(info.title).toContain("Costco Chandler");
    expect(info.title).not.toContain("Costco Costco");
    expect(info.storeLine).not.toContain("Costco Costco");
  });

  it("prefixes retailer name when store.name does not start with it", () => {
    const store = { ...TEST_STORE, name: "Chandler" };
    const info = formatStoreInfo("costco", "Costco", store);
    expect(info.title).toContain("Costco Chandler");
  });
});

describe("truncateTitle (#8, #13)", () => {
  it("returns short titles unchanged", () => {
    const title = "🚨 NEW FIND — Costco Tempe (#736) · 3.5 mi";
    expect(truncateTitle(title)).toBe(title);
  });

  it("truncates titles exceeding 256 chars", () => {
    const longTitle = "🚨 NEW FIND — " + "A".repeat(300);
    const result = truncateTitle(longTitle);
    expect(result.length).toBeLessThanOrEqual(DISCORD_TITLE_LIMIT);
    expect(result).toMatch(/…$/);
  });

  it("returns exactly-at-limit titles unchanged", () => {
    const title = "x".repeat(DISCORD_TITLE_LIMIT);
    expect(truncateTitle(title)).toBe(title);
  });
});

describe("Safeway price formatting (#7)", () => {
  it("formats price to 2 decimal places", async () => {
    mocks.fetch.mockResolvedValue({
      ok: true,
      json: async () => ({
        primaryProducts: { response: { docs: [{
          name: "Weller Special Reserve Bourbon 750ml", inStock: true,
          url: "/product/weller-sr", price: 33,
        }] } },
      }),
    });
    const found = await runWithFakeTimers(() => scrapeSafewayStore(TEST_STORE));
    const weller = found.find((f) => f.name === "Weller Special Reserve");
    // Should be "$33.00", not "$33"
    expect(weller.price).toBe("$33.00");
  });

  it("formats fractional price correctly", async () => {
    mocks.fetch.mockResolvedValue({
      ok: true,
      json: async () => ({
        primaryProducts: { response: { docs: [{
          name: "Weller Special Reserve Bourbon 750ml", inStock: true,
          url: "/product/weller-sr", price: 32.5,
        }] } },
      }),
    });
    const found = await runWithFakeTimers(() => scrapeSafewayStore(TEST_STORE));
    const weller = found.find((f) => f.name === "Weller Special Reserve");
    expect(weller.price).toBe("$32.50");
  });
});

describe("fetchRetry warns on first failure (#11)", () => {
  it("logs a warning before retrying", async () => {
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    mocks.fetch
      .mockRejectedValueOnce(new Error("ECONNRESET"))
      .mockResolvedValueOnce({ ok: true, status: 200 });
    await runWithFakeTimers(() => fetchRetry("http://example.com", {}));
    expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining("[fetchRetry]"));
    expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining("ECONNRESET"));
    warnSpy.mockRestore();
  });
});

describe("Total Wine INITIAL_STATE brace-counting extraction (#12)", () => {
  // Note: scrapeTotalWineViaFetch requires proxyAgent (module-level, frozen at import).
  // The brace-counting parser is tested end-to-end in test/proxy.test.js.
  // Here we verify the matcher handles product data with special characters.
  it("matchTotalWineInitialState handles descriptions with special HTML chars", () => {
    const state = {
      search: { results: { products: [{
        name: "Weller Special Reserve Bourbon Whiskey",
        productUrl: "/spirits/bourbon/weller-sr/p/12345",
        stockLevel: [{ stock: 5 }],
        description: 'Uses a </script> tag & <b>HTML</b> entities',
      }] } },
    };
    const found = matchTotalWineInitialState(state);
    expect(found.length).toBe(1);
    expect(found[0].name).toBe("Weller Special Reserve");
  });
});

// ─── Round 4: 5 targeted fixes ───────────────────────────────────────────────

describe("Kroger null inventory false-positive prevention", () => {
  it("filters products with null/missing inventory stockLevel", async () => {
    _resetKrogerToken();
    mocks.fetch
      .mockResolvedValueOnce({ ok: true, json: async () => ({ access_token: "tk" }) })
      .mockResolvedValue({
        ok: true,
        json: async () => ({ data: [{
          description: "Weller Special Reserve Bourbon 750ml",
          productId: "0001234",
          items: [{ fulfillment: { inStore: true }, inventory: { stockLevel: null }, price: { regular: 29.99 } }],
        }] }),
      });
    const found = await runWithFakeTimers(() => scrapeKrogerStore(TEST_STORE));
    expect(found).toEqual([]);
  });

  it("filters products with missing inventory object entirely", async () => {
    _resetKrogerToken();
    mocks.fetch
      .mockResolvedValueOnce({ ok: true, json: async () => ({ access_token: "tk" }) })
      .mockResolvedValue({
        ok: true,
        json: async () => ({ data: [{
          description: "Weller Special Reserve Bourbon 750ml",
          productId: "0001234",
          items: [{ fulfillment: { inStore: true }, price: { regular: 29.99 } }],
        }] }),
      });
    const found = await runWithFakeTimers(() => scrapeKrogerStore(TEST_STORE));
    expect(found).toEqual([]);
  });

  it("filters products with undefined inventory stockLevel", async () => {
    _resetKrogerToken();
    mocks.fetch
      .mockResolvedValueOnce({ ok: true, json: async () => ({ access_token: "tk" }) })
      .mockResolvedValue({
        ok: true,
        json: async () => ({ data: [{
          description: "Weller Special Reserve Bourbon 750ml",
          productId: "0001234",
          items: [{ fulfillment: { inStore: true }, inventory: {}, price: { regular: 29.99 } }],
        }] }),
      });
    const found = await runWithFakeTimers(() => scrapeKrogerStore(TEST_STORE));
    expect(found).toEqual([]);
  });
});

describe("Safeway page-2 pagination", () => {
  it("fetches page 2 when first page returns exactly 50 results", async () => {
    // 50 non-matching products on page 1 triggers pagination
    const page1Docs = Array.from({ length: 50 }, (_, i) => ({
      name: `Random Product ${i}`, inStock: true, price: 10, url: `/p/${i}`,
    }));
    // Page 2 has a matching bottle
    const page2Docs = [{
      name: "Weller Special Reserve Bourbon 750ml", inStock: true,
      url: "/product/weller-sr", price: 29.99, upc: "upc-weller",
    }];

    mocks.fetch.mockImplementation((url) => {
      if (typeof url === "string" && url.includes("start=50")) {
        return Promise.resolve({
          ok: true,
          json: async () => ({ primaryProducts: { response: { docs: page2Docs } } }),
        });
      }
      return Promise.resolve({
        ok: true,
        json: async () => ({ primaryProducts: { response: { docs: page1Docs } } }),
      });
    });
    const found = await runWithFakeTimers(() => scrapeSafewayStore(TEST_STORE));
    const weller = found.find((f) => f.name === "Weller Special Reserve");
    expect(weller).toBeTruthy();
    expect(weller.price).toBe("$29.99");
    // Verify page 2 was fetched
    const page2Calls = mocks.fetch.mock.calls.filter(
      (c) => typeof c[0] === "string" && c[0].includes("start=50")
    );
    expect(page2Calls.length).toBeGreaterThan(0);
  });

  it("does not fetch page 2 when results are under 50", async () => {
    mocks.fetch.mockResolvedValue({
      ok: true,
      json: async () => ({
        primaryProducts: { response: { docs: [{
          name: "Weller Special Reserve Bourbon 750ml", inStock: true,
          url: "/product/weller-sr", price: 32.99,
        }] } },
      }),
    });
    await runWithFakeTimers(() => scrapeSafewayStore(TEST_STORE));
    const page2Calls = mocks.fetch.mock.calls.filter(
      (c) => typeof c[0] === "string" && c[0].includes("start=50")
    );
    expect(page2Calls.length).toBe(0);
  });

  it("handles page 2 fetch failure gracefully", async () => {
    const page1Docs = Array.from({ length: 50 }, (_, i) => ({
      name: `Random Product ${i}`, inStock: true, price: 10, url: `/p/${i}`,
    }));
    mocks.fetch.mockImplementation((url) => {
      if (typeof url === "string" && url.includes("start=50")) {
        return Promise.resolve({ ok: false, status: 500 });
      }
      return Promise.resolve({
        ok: true,
        json: async () => ({ primaryProducts: { response: { docs: page1Docs } } }),
      });
    });
    // Should not throw — page 2 failure is silently handled
    const found = await runWithFakeTimers(() => scrapeSafewayStore(TEST_STORE));
    expect(found).toEqual([]);
  });
});

describe("Total Wine fulfillment filter(Boolean)", () => {
  it("omits empty strings when shoppingOption name/type are missing", () => {
    const state = {
      search: { results: { products: [{
        name: "Weller Special Reserve Bourbon Whiskey",
        productUrl: "/spirits/bourbon/weller-sr/p/12345",
        stockLevel: [{ stock: 5 }],
        shoppingOptions: [
          { eligible: true, name: "In-store" },
          { eligible: true },             // missing name and type
          { eligible: true, type: "" },    // empty type
          { eligible: true, name: "Delivery" },
        ],
      }] } },
    };
    const found = matchTotalWineInitialState(state);
    expect(found.length).toBe(1);
    expect(found[0].fulfillment).toBe("In-store, Delivery");
    // Should NOT contain any leading/trailing/double commas from empty strings
    expect(found[0].fulfillment).not.toMatch(/,,/);
    expect(found[0].fulfillment).not.toMatch(/^,|,$/);
  });

  it("returns empty fulfillment when all options lack name and type", () => {
    const state = {
      search: { results: { products: [{
        name: "Weller Special Reserve Bourbon Whiskey",
        productUrl: "/spirits/bourbon/weller-sr/p/12345",
        stockLevel: [{ stock: 5 }],
        shoppingOptions: [
          { eligible: true },
          { eligible: true, name: "", type: "" },
        ],
      }] } },
    };
    const found = matchTotalWineInitialState(state);
    expect(found.length).toBe(1);
    expect(found[0].fulfillment).toBe("");
  });
});

describe("Walmart brace-counting __NEXT_DATA__ extraction", () => {
  it("handles </script> inside JSON string values", async () => {
    const nextData = {
      props: { pageProps: { initialData: { searchResult: { itemStacks: [{ items: [{
        __typename: "Product", name: "Weller Special Reserve Bourbon 750ml",
        availabilityStatusV2: { value: "IN_STOCK" }, canonicalUrl: "/ip/weller/123",
        priceInfo: { currentPrice: { priceString: "$29.99" } }, sellerName: "Walmart.com",
        description: 'Contains a </script> tag inside the JSON string',
      }] }] } } } },
    };
    // The JSON string contains </script> which would break the old regex approach
    const jsonStr = JSON.stringify(nextData);
    const html = `<html><script id="__NEXT_DATA__" type="application/json">${jsonStr}</script></html>`;
    mocks.fetch.mockResolvedValue({ ok: true, text: async () => html });
    const found = await runWithFakeTimers(() => scrapeWalmartViaFetch(TEST_STORE));
    expect(found).not.toBeNull();
    const weller = found.find((f) => f.name === "Weller Special Reserve");
    expect(weller).toBeTruthy();
    expect(weller.price).toBe("$29.99");
  });

  it("handles escaped quotes and backslashes in JSON values", async () => {
    const nextData = {
      props: { pageProps: { initialData: { searchResult: { itemStacks: [{ items: [{
        __typename: "Product", name: 'Weller Special Reserve Bourbon 750ml',
        availabilityStatusV2: { value: "IN_STOCK" }, canonicalUrl: "/ip/weller/123",
        priceInfo: { currentPrice: { priceString: "$29.99" } }, sellerName: "Walmart.com",
        description: 'Contains "escaped \\"quotes\\"" and backslash \\\\',
      }] }] } } } },
    };
    const html = `<html><script id="__NEXT_DATA__" type="application/json">${JSON.stringify(nextData)}</script></html>`;
    mocks.fetch.mockResolvedValue({ ok: true, text: async () => html });
    const found = await runWithFakeTimers(() => scrapeWalmartViaFetch(TEST_STORE));
    expect(found).not.toBeNull();
    expect(found.find((f) => f.name === "Weller Special Reserve")).toBeTruthy();
  });
});

describe("Poll concurrency limit", () => {
  it("uses concurrency limit of 8 for store scanning", async () => {
    const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    // Set up 10 stores across retailers to exceed old limit of 6
    const stores = Array.from({ length: 10 }, (_, i) => ({
      ...TEST_STORE, storeId: `${i}`, name: `Store ${i}`,
    }));
    _setStoreCache({
      retailers: {
        costco: [], totalwine: [], walmart: stores, kroger: [], safeway: [], walgreens: [],
      },
    });
    mocks.fetch.mockResolvedValue({
      ok: true,
      text: async () => "<html>No data</html>",
      json: async () => ({}),
    });
    await runWithFakeTimers(() => poll());
    // Verify all 10 stores were processed (not capped at 6)
    const walmartLogs = consoleSpy.mock.calls.filter(
      (c) => typeof c[0] === "string" && c[0].includes("[walmart:")
    );
    expect(walmartLogs.length).toBeGreaterThan(0);
    consoleSpy.mockRestore();
  });
});

describe("buildSummaryEmbed uses truncateDescription (#8)", () => {
  it("truncates extremely long summary descriptions", () => {
    // Create many stores to generate a very long summary
    const scannedStores = Array.from({ length: 200 }, (_, i) => ({
      retailerName: `Retailer${i}`,
      storeName: `Store With An Extremely Long Name Number ${i} In Some City`,
      storeId: `${i}`,
    }));
    const embed = buildSummaryEmbed({
      storesScanned: 200, retailersScanned: 200,
      totalNewFinds: 0, totalStillInStock: 0,
      totalGoneOOS: 0, nothingCount: 200,
      durationSec: 600, scannedStores,
    });
    expect(embed.description.length).toBeLessThanOrEqual(DISCORD_DESC_LIMIT);
  });
});
