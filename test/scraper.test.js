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
    chromiumLaunch: vi.fn(),
    chromiumUse: vi.fn(),
    cronSchedule: vi.fn(),
    discoverStores: vi.fn(),
  };
});

vi.mock("dotenv/config", () => ({}));
vi.mock("node-fetch", () => ({ default: mocks.fetch }));
vi.mock("playwright-extra", () => ({
  chromium: { use: mocks.chromiumUse, launch: mocks.chromiumLaunch },
}));
vi.mock("puppeteer-extra-plugin-stealth", () => ({ default: vi.fn() }));
vi.mock("node-cron", () => ({ default: { schedule: mocks.cronSchedule } }));
vi.mock("node:fs/promises", () => ({
  readFile: mocks.readFile,
  writeFile: mocks.writeFile,
}));
vi.mock("../lib/discover-stores.js", () => ({
  discoverStores: mocks.discoverStores,
}));

// ─── Import module under test ─────────────────────────────────────────────────

import {
  SEARCH_QUERIES, TARGET_BOTTLES, RETAILERS, FETCH_HEADERS,
  normalizeText, parseSize, parsePrice, matchesBottle, dedupFound, runWithConcurrency, matchWalmartNextData,
  COLORS, SKU_LABELS, formatStoreInfo, parseCity, parseState, timeAgo,
  formatBottleLine, buildOOSList, truncateDescription, DISCORD_DESC_LIMIT, buildStoreEmbeds, buildSummaryEmbed,
  loadState, saveState, computeChanges, updateStoreState,
  postDiscordWebhook, sendDiscordAlert, sendUrgentAlert,
  launchBrowser, closeBrowser, newPage, isBlockedPage,
  scrapeCostcoOnce, scrapeTotalWineStore,
  scrapeWalmartViaFetch, scrapeWalmartViaBrowser, scrapeWalmartStore,
  getKrogerToken, scrapeKrogerStore, scrapeSafewayStore,
  poll, main,
  _setStoreCache, _resetPolling, _resetKrogerToken,
} from "../scraper.js";

// ─── Test Helpers ─────────────────────────────────────────────────────────────

function createMockPage() {
  return {
    goto: vi.fn().mockResolvedValue(undefined),
    waitForSelector: vi.fn().mockResolvedValue(undefined),
    waitForTimeout: vi.fn().mockResolvedValue(undefined),
    waitForFunction: vi.fn().mockResolvedValue(undefined),
    title: vi.fn().mockResolvedValue(""),
    evaluate: vi.fn().mockResolvedValue([]),
    $$eval: vi.fn().mockResolvedValue([]),
    $eval: vi.fn().mockResolvedValue(null),
    context: vi.fn(() => ({ close: vi.fn().mockResolvedValue(undefined) })),
  };
}

function setupMockBrowser() {
  const mockPage = createMockPage();
  const mockContext = { newPage: vi.fn().mockResolvedValue(mockPage), close: vi.fn().mockResolvedValue(undefined) };
  const mockBrowser = { newContext: vi.fn().mockResolvedValue(mockContext), close: vi.fn().mockResolvedValue(undefined) };
  mocks.chromiumLaunch.mockResolvedValue(mockBrowser);
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
  it("SEARCH_QUERIES has 12 broad queries", () => {
    expect(SEARCH_QUERIES).toHaveLength(12);
    expect(SEARCH_QUERIES).toContain("weller bourbon");
    expect(SEARCH_QUERIES).toContain("van winkle");
    expect(SEARCH_QUERIES).toContain("eh taylor");
    expect(SEARCH_QUERIES).toContain("old forester bourbon");
  });

  it("TARGET_BOTTLES has 40 bottles", () => {
    expect(TARGET_BOTTLES).toHaveLength(40);
    expect(TARGET_BOTTLES[0]).toHaveProperty("name");
    expect(TARGET_BOTTLES[0]).toHaveProperty("searchTerms");
  });

  it("RETAILERS has 5 entries", () => {
    expect(RETAILERS).toHaveLength(5);
    expect(RETAILERS.map((r) => r.key)).toEqual(["costco", "totalwine", "walmart", "kroger", "safeway"]);
  });

  it("RETAILERS have correct flags", () => {
    const costco = RETAILERS.find((r) => r.key === "costco");
    expect(costco.scrapeOnce).toBe(true);
    expect(costco.needsPage).toBe(true);
    const walmart = RETAILERS.find((r) => r.key === "walmart");
    expect(walmart.scrapeOnce).toBe(false);
    expect(walmart.needsPage).toBe(false);
  });

  it("FETCH_HEADERS has a User-Agent", () => {
    expect(FETCH_HEADERS["User-Agent"]).toContain("Mozilla");
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
    expect(FETCH_HEADERS["Sec-CH-UA-Platform"]).toBe('"Windows"');
  });

  it("FETCH_HEADERS User-Agent version matches Sec-CH-UA version", () => {
    const uaMatch = FETCH_HEADERS["User-Agent"].match(/Chrome\/([\d]+)/);
    const chMatch = FETCH_HEADERS["Sec-CH-UA"].match(/Chrome";v="([\d]+)"/);
    expect(uaMatch[1]).toBe(chMatch[1]);
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

  it("matches Blanton's Green and Black", () => {
    const green = TARGET_BOTTLES.find((b) => b.name === "Blanton's Green");
    expect(matchesBottle("Blanton's Green Label Bourbon 700ml", green)).toBe(true);
    const black = TARGET_BOTTLES.find((b) => b.name === "Blanton's Black");
    expect(matchesBottle("Blantons Black Label 750ml", black)).toBe(true);
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

  it("handles canAddToCart availability", () => {
    const data = makeNextData([{ __typename: "Product", name: "Weller Special Reserve", canAddToCart: true, sellerName: "Walmart.com" }]);
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
  it("extracts city from address", () => {
    expect(parseCity("2501 S Market St, Gilbert, AZ 85295")).toBe("Gilbert");
  });
  it("returns empty for null/empty", () => {
    expect(parseCity("")).toBe("");
    expect(parseCity(null)).toBe("");
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
  });

  it("handles missing distance", () => {
    const info = formatStoreInfo("costco", "Costco", { ...TEST_STORE, distanceMiles: null });
    expect(info.title).not.toContain("mi");
  });

  it("handles missing address", () => {
    const info = formatStoreInfo("costco", "Costco", { ...TEST_STORE, address: "" });
    expect(info.addressLine).toContain("Address unknown");
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
  it("writes state as JSON", async () => {
    mocks.writeFile.mockResolvedValueOnce(undefined);
    await saveState({ costco: {} });
    expect(JSON.parse(mocks.writeFile.mock.calls[0][1])).toEqual({ costco: {} });
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

  it("gives up after 3 retries", async () => {
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    mocks.fetch.mockResolvedValue({ status: 429, json: async () => ({ retry_after: 0.01 }) });
    await runWithFakeTimers(() => postDiscordWebhook({ embeds: [{ title: "Test" }] }));
    expect(mocks.fetch).toHaveBeenCalledTimes(3);
    expect(errorSpy).toHaveBeenCalledWith(expect.stringContaining("after 3 retries"));
    warnSpy.mockRestore();
    errorSpy.mockRestore();
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
    expect(mocks.chromiumLaunch).toHaveBeenCalledWith({ headless: true });
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
    expect(await isBlockedPage(page)).toBe(false);
  });

  it("handles title() errors gracefully", async () => {
    const page = createMockPage();
    page.title.mockRejectedValue(new Error("page closed"));
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
    expect(mockPage.goto).toHaveBeenCalledTimes(SEARCH_QUERIES.length);
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
    const found = await runWithFakeTimers(() => scrapeTotalWineStore(TEST_STORE, mockPage));
    const wsr = found.find((f) => f.name === "Weller Special Reserve");
    expect(wsr).toBeTruthy();
    expect(wsr.url).toContain("totalwine.com");
    expect(wsr.price).toBe("$29.99");
  });

  it("falls back to CSS selectors when INITIAL_STATE unavailable", async () => {
    const mockPage = createMockPage();
    mockPage.evaluate.mockResolvedValue([]);
    mockPage.$$eval.mockResolvedValue([
      { name: "Weller Special Reserve Bourbon", inStock: true, url: "/spirits/bourbon/weller", price: "$29.99" },
    ]);
    const found = await runWithFakeTimers(() => scrapeTotalWineStore(TEST_STORE, mockPage));
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
    const found = await runWithFakeTimers(() => scrapeTotalWineStore(TEST_STORE, mockPage));
    expect(found).toEqual([]);
  });

  it("handles page errors", async () => {
    const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    const mockPage = createMockPage();
    mockPage.goto.mockRejectedValue(new Error("Navigation timeout"));
    const found = await runWithFakeTimers(() => scrapeTotalWineStore(TEST_STORE, mockPage));
    expect(found).toEqual([]);
    consoleSpy.mockRestore();
  });

  it("prepends totalwine.com to relative URLs", async () => {
    const mockPage = createMockPage();
    mockPage.evaluate.mockResolvedValue([]);
    mockPage.$$eval.mockResolvedValue([
      { name: "Weller Special Reserve", inStock: true, url: "/spirits/weller", price: "$30" },
    ]);
    const found = await runWithFakeTimers(() => scrapeTotalWineStore(TEST_STORE, mockPage));
    if (found.length > 0) expect(found[0].url).toContain("https://www.totalwine.com");
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

describe("scrapeWalmartViaBrowser", () => {
  it("extracts products from __NEXT_DATA__ via page.evaluate", async () => {
    const mockPage = createMockPage();
    mockPage.evaluate.mockResolvedValue({
      props: { pageProps: { initialData: { searchResult: { itemStacks: [{ items: [{
        __typename: "Product", name: "Stagg Jr Bourbon",
        availabilityStatusV2: { value: "IN_STOCK" }, sellerName: "Walmart.com",
      }] }] } } } },
    });
    const found = await runWithFakeTimers(() => scrapeWalmartViaBrowser(TEST_STORE, mockPage));
    expect(found.find((f) => f.name === "Stagg Jr")).toBeTruthy();
  });

  it("falls back to DOM when __NEXT_DATA__ unavailable", async () => {
    const mockPage = createMockPage();
    mockPage.evaluate.mockResolvedValue(null);
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

  it("handles API errors", async () => {
    const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    mocks.fetch.mockResolvedValue({ ok: false, status: 500 });
    const found = await runWithFakeTimers(() => scrapeSafewayStore(TEST_STORE));
    expect(found).toEqual([]);
    consoleSpy.mockRestore();
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

// ─── Poll Orchestrator ────────────────────────────────────────────────────────

describe("poll", () => {
  beforeEach(() => {
    _resetPolling();
    _resetKrogerToken();
    _setStoreCache({ retailers: { costco: [TEST_STORE], totalwine: [], walmart: [], kroger: [], safeway: [] } });
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
        totalwine: [], walmart: [], kroger: [], safeway: [],
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
        costco: [], totalwine: [TEST_STORE], walmart: [], kroger: [], safeway: [],
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
        costco: [], totalwine: [], walmart: [TEST_STORE], kroger: [TEST_STORE], safeway: [TEST_STORE],
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
      retailers: { costco: [], totalwine: [], walmart: [TEST_STORE], kroger: [], safeway: [] },
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
      retailers: { costco: [TEST_STORE], totalwine: [], walmart: [], kroger: [], safeway: [] },
    });
    await runWithFakeTimers(() => main());
    expect(mocks.discoverStores).toHaveBeenCalled();
    expect(mocks.cronSchedule).toHaveBeenCalled();
    consoleSpy.mockRestore();
  });

  it("exits in RUN_ONCE mode", async () => {
    const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    const exitSpy = vi.spyOn(process, "exit").mockImplementation(() => {});
    process.env.RUN_ONCE = "true";
    mocks.discoverStores.mockResolvedValueOnce({
      retailers: { costco: [], totalwine: [], walmart: [], kroger: [], safeway: [] },
    });
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
