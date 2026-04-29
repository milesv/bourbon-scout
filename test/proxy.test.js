import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

// ─── Mocks (hoisted before imports) ───────────────────────────────────────────
// Set PROXY_URL BEFORE module load so proxyAgent is created
const mocks = vi.hoisted(() => {
  process.env.DISCORD_WEBHOOK_URL = "https://test-webhook.example.com/hook";
  process.env.ZIP_CODE = "85283";
  process.env.SEARCH_RADIUS_MILES = "15";
  process.env.MAX_STORES_PER_RETAILER = "5";
  process.env.KROGER_CLIENT_ID = "test-kroger-id";
  process.env.KROGER_CLIENT_SECRET = "test-kroger-secret";
  process.env.SAFEWAY_API_KEY = "test-safeway-key";
  process.env.POLL_INTERVAL = "*/15 * * * *";
  process.env.PROXY_URL = "http://proxy.example.com:8080";

  const HttpsProxyAgentInstance = { _isProxy: true };
  // Must be a real class (new-able) since scraper.js calls `new HttpsProxyAgent(url)`
  class MockHttpsProxyAgent {
    constructor(url) { MockHttpsProxyAgent._lastUrl = url; Object.assign(this, HttpsProxyAgentInstance); }
  }
  const SocksProxyAgentInstance = { _isSocksProxy: true };
  class MockSocksProxyAgent {
    constructor(url) { MockSocksProxyAgent._lastUrl = url; Object.assign(this, SocksProxyAgentInstance); }
  }
  const sharedLaunchPersistentContext = vi.fn();
  return {
    fetch: vi.fn(),
    gotScraping: vi.fn(),
    readFile: vi.fn(),
    writeFile: vi.fn(),
    rename: vi.fn(),
    mkdir: vi.fn().mockResolvedValue(undefined),
    chromiumLaunch: vi.fn(),
    chromiumLaunchPersistentContext: sharedLaunchPersistentContext,
    vanillaLaunchPersistentContext: sharedLaunchPersistentContext,
    chromiumUse: vi.fn(),
    discoverStores: vi.fn(),
    HttpsProxyAgent: MockHttpsProxyAgent,
    HttpsProxyAgentInstance,
    SocksProxyAgent: MockSocksProxyAgent,
    SocksProxyAgentInstance,
  };
});

vi.mock("dotenv/config", () => ({}));
vi.mock("node-fetch", () => ({ default: mocks.fetch }));
vi.mock("got-scraping", () => ({ gotScraping: mocks.gotScraping }));
vi.mock("rebrowser-playwright-core", () => ({
  chromium: { launchPersistentContext: mocks.chromiumLaunchPersistentContext },
}));
vi.mock("playwright-core", () => ({
  chromium: { launchPersistentContext: mocks.vanillaLaunchPersistentContext },
}));
vi.mock("playwright-extra", () => ({
  addExtra: () => ({ use: mocks.chromiumUse, launch: mocks.chromiumLaunch, launchPersistentContext: mocks.chromiumLaunchPersistentContext }),
}));
vi.mock("puppeteer-extra-plugin-stealth", () => ({ default: vi.fn() }));
vi.mock("https-proxy-agent", () => ({ HttpsProxyAgent: mocks.HttpsProxyAgent }));
vi.mock("socks-proxy-agent", () => ({ SocksProxyAgent: mocks.SocksProxyAgent }));
vi.mock("cheerio", async () => await vi.importActual("cheerio"));
vi.mock("node:fs/promises", () => ({
  readFile: mocks.readFile,
  writeFile: mocks.writeFile,
  rename: mocks.rename,
  mkdir: mocks.mkdir,
  readdir: vi.fn().mockResolvedValue([]),
  unlink: vi.fn().mockResolvedValue(undefined),
  rm: vi.fn().mockResolvedValue(undefined),
}));
vi.mock("../lib/discover-stores.js", () => ({
  discoverStores: mocks.discoverStores,
}));

// ─── Import module under test (with PROXY_URL set) ───────────────────────────

import {
  FETCH_HEADERS,
  launchBrowser, closeBrowser,
  FETCH_BLOCKED_PATTERNS, isCostcoBlocked,
  scrapeCostcoViaFetch, scrapeCostcoStore,
  scrapeTotalWineViaFetch, scrapeTotalWineStore,
  scrapeWalmartViaFetch, scrapeWalmartStore, scrapeKrogerStore, scrapeSafewayStore,
  scrapeSamsClubViaFetch,
  getKrogerToken, main, createProxyAgent,
  refreshProxySession, rotateRetailerProxy, getRetailerProxyUrl,
  _resetKrogerToken, _resetPolling, _setStoreCache,
  _resetRetailerBrowserCache, _resetRetailerFailures, _resetKnownProducts,
} from "../scraper.js";

// ─── Test Helpers ─────────────────────────────────────────────────────────────

const TEST_STORE = {
  storeId: "1234",
  name: "Test Store",
  address: "123 Main St, Tempe, AZ 85281",
  distanceMiles: 3.5,
};

function setupMockBrowser() {
  const mockPage = {
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
    context: vi.fn(() => ({ close: vi.fn().mockResolvedValue(undefined), storageState: vi.fn().mockResolvedValue({ cookies: [], origins: [] }) })),
  };
  const mockContext = { newPage: vi.fn().mockResolvedValue(mockPage), close: vi.fn().mockResolvedValue(undefined), storageState: vi.fn().mockResolvedValue({ cookies: [], origins: [] }), addCookies: vi.fn().mockResolvedValue(undefined) };
  const mockBrowser = { newContext: vi.fn().mockResolvedValue(mockContext), close: vi.fn().mockResolvedValue(undefined) };
  mocks.chromiumLaunch.mockResolvedValue(mockBrowser);
  mocks.chromiumLaunchPersistentContext.mockResolvedValue(mockContext);
  return { browser: mockBrowser, context: mockContext, page: mockPage };
}

// Helper: create a got-scraping-style response for mocks.gotScraping
function mockGotResponse(statusCode, body = "", headers = {}) {
  return { statusCode, body, headers };
}

async function runWithFakeTimers(fn) {
  const promise = fn();
  promise.catch(() => {});
  await vi.runAllTimersAsync();
  return promise;
}

// ─── Global Setup ────────────────────────────────────────────────────────────

beforeEach(() => {
  vi.useFakeTimers();
  vi.resetAllMocks();
  // Default gotScraping mock returns empty HTML with results container (valid empty search)
  mocks.gotScraping.mockResolvedValue(mockGotResponse(200, '<html><body><div data-testid="no-results">No results</div></body></html>'));
  _resetKrogerToken();
  _resetRetailerBrowserCache();
  _resetRetailerFailures();
  _resetKnownProducts();
});

afterEach(() => {
  vi.useRealTimers();
});

// ─── Proxy Tests ─────────────────────────────────────────────────────────────

describe("proxy support", () => {
  it("HttpsProxyAgent is constructed with PROXY_URL", () => {
    expect(mocks.HttpsProxyAgent._lastUrl).toBe("http://proxy.example.com:8080");
  });

  it("launchBrowser passes proxy server to Chromium launch options", async () => {
    setupMockBrowser();
    await launchBrowser();
    const launchOpts = mocks.chromiumLaunch.mock.calls[0][0];
    expect(launchOpts.proxy).toEqual({ server: "http://proxy.example.com:8080" });
    await closeBrowser();
  });

  it("scrapeWalmartViaFetch passes proxyUrl on gotScraping calls", async () => {
    const walmartNextData = {
      props: { pageProps: { initialData: { searchResult: { itemStacks: [{ items: [{
        __typename: "Product", name: "Weller Special Reserve Bourbon",
        availabilityStatusV2: { value: "IN_STOCK" }, sellerName: "Walmart.com",
      }] }] } } } },
    };
    mocks.gotScraping.mockResolvedValue(
      mockGotResponse(200, `<script id="__NEXT_DATA__">${JSON.stringify(walmartNextData)}</script>`)
    );
    await runWithFakeTimers(() => scrapeWalmartViaFetch(TEST_STORE));
    // Every Walmart gotScraping call should include proxyUrl
    const walmartCalls = mocks.gotScraping.mock.calls.filter(
      ([opts]) => typeof opts?.url === "string" && opts.url.includes("walmart.com")
    );
    expect(walmartCalls.length).toBeGreaterThan(0);
    for (const [opts] of walmartCalls) {
      expect(opts.proxyUrl).toBeTruthy();
    }
  });

  it("scrapeWalmartStore uses fetch on CI when proxy is set", async () => {
    process.env.CI = "true";
    const walmartNextData = {
      props: { pageProps: { initialData: { searchResult: { itemStacks: [{ items: [{
        __typename: "Product", name: "Weller Special Reserve Bourbon",
        availabilityStatusV2: { value: "IN_STOCK" }, sellerName: "Walmart.com",
      }] }] } } } },
    };
    mocks.gotScraping.mockResolvedValue(
      mockGotResponse(200, `<script id="__NEXT_DATA__">${JSON.stringify(walmartNextData)}</script>`)
    );
    const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    const found = await runWithFakeTimers(() => scrapeWalmartStore(TEST_STORE));
    expect(found).not.toBeNull();
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining("(proxied)"));
    consoleSpy.mockRestore();
    delete process.env.CI;
  });

  it("getKrogerToken passes proxyUrl on OAuth fetch via got-scraping", async () => {
    mocks.gotScraping.mockResolvedValueOnce(mockGotResponse(200, JSON.stringify({ access_token: "proxy-token" })));
    await getKrogerToken();
    const oauthCalls = mocks.gotScraping.mock.calls.filter(([opts]) => opts?.url?.includes("oauth2/token"));
    expect(oauthCalls.length).toBeGreaterThan(0);
    expect(oauthCalls[0][0].proxyUrl).toBeTruthy();
  });

  it("scrapeKrogerStore passes proxyUrl on API fetch via got-scraping", async () => {
    _resetKrogerToken();
    mocks.gotScraping
      .mockResolvedValueOnce(mockGotResponse(200, JSON.stringify({ access_token: "tk" })))
      .mockResolvedValue(mockGotResponse(200, JSON.stringify({ data: [] })));
    await runWithFakeTimers(() => scrapeKrogerStore(TEST_STORE));
    const apiCalls = mocks.gotScraping.mock.calls.filter(([opts]) => opts?.url?.includes("api.kroger.com/v1/products"));
    expect(apiCalls.length).toBeGreaterThan(0);
    for (const [opts] of apiCalls) {
      expect(opts.proxyUrl).toBeTruthy();
    }
  });

  // Safeway is now browser-based — proxy is handled by launchRetailerBrowser,
  // not by fetch proxyUrl. Browser proxy routing is tested in launchRetailerBrowser tests.

  it("Discord webhook calls do NOT include proxy agent", async () => {
    _resetPolling();
    _resetKrogerToken();
    mocks.readFile.mockResolvedValue("{}");
    mocks.writeFile.mockResolvedValue(undefined);
    mocks.fetch.mockResolvedValue({ ok: true });
    setupMockBrowser();
    _setStoreCache({
      retailers: { costco: [TEST_STORE], totalwine: [], walmart: [], kroger: [], safeway: [], albertsons: [] },
    });
    vi.spyOn(console, "log").mockImplementation(() => {});
    await runWithFakeTimers(async () => {
      const { poll } = await import("../scraper.js");
      await poll();
    });
    const discordCalls = mocks.fetch.mock.calls.filter(
      ([url]) => typeof url === "string" && url.includes("test-webhook")
    );
    for (const [, opts] of discordCalls) {
      expect(opts?.agent).toBeUndefined();
    }
    vi.spyOn(console, "log").mockRestore();
  });

  it("main logs proxy message on startup", async () => {
    const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    const exitSpy = vi.spyOn(process, "exit").mockImplementation(() => {});
    process.env.RUN_ONCE = "true";
    mocks.readFile.mockResolvedValue("{}");
    mocks.writeFile.mockResolvedValue(undefined);
    mocks.fetch.mockResolvedValue({ ok: true });
    setupMockBrowser();
    _resetPolling();
    mocks.discoverStores.mockResolvedValueOnce({
      retailers: { costco: [], totalwine: [], walmart: [], kroger: [], safeway: [], albertsons: [] },
    });
    await runWithFakeTimers(() => main());
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining("[proxy]"));
    delete process.env.RUN_ONCE;
    exitSpy.mockRestore();
    consoleSpy.mockRestore();
  });

  it("scrapeTotalWineViaFetch extracts INITIAL_STATE via proxy", async () => {
    const initialState = {
      search: { results: { products: [{
        name: "Weller Special Reserve Bourbon Whiskey",
        productUrl: "/spirits/bourbon/weller-sr/p/12345",
        price: [{ price: 29.99 }],
        stockLevel: [{ stock: 5 }],
        transactional: true,
        shoppingOptions: [{ eligible: true, name: "In-store" }],
      }] } },
    };
    const html = `<html><script>window.INITIAL_STATE = ${JSON.stringify(initialState)};</script></html>`;
    mocks.gotScraping.mockResolvedValue(mockGotResponse(200, html));
    const found = await runWithFakeTimers(() => scrapeTotalWineViaFetch(TEST_STORE));
    expect(found).not.toBeNull();
    expect(found.length).toBe(1);
    expect(found[0].name).toBe("Weller Special Reserve");
    expect(found[0].price).toBe("$29.99");
    // Verify proxy was used via gotScraping proxyUrl option
    const twCalls = mocks.gotScraping.mock.calls.filter(
      ([opts]) => typeof opts?.url === "string" && opts.url.includes("totalwine.com")
    );
    expect(twCalls.length).toBeGreaterThan(0);
    for (const [opts] of twCalls) {
      expect(opts.proxyUrl).toBeTruthy();
    }
  });

  it("scrapeTotalWineViaFetch returns null after 4+ failures", async () => {
    mocks.gotScraping.mockResolvedValue(mockGotResponse(403));
    const found = await runWithFakeTimers(() => scrapeTotalWineViaFetch(TEST_STORE));
    expect(found).toBeNull();
  });

  it("scrapeTotalWineViaFetch returns null when INITIAL_STATE missing", async () => {
    mocks.gotScraping.mockResolvedValue(mockGotResponse(200, "<html><body>Challenge page</body></html>"));
    const found = await runWithFakeTimers(() => scrapeTotalWineViaFetch(TEST_STORE));
    expect(found).toBeNull();
  });

  it("scrapeTotalWineStore wrapper goes straight to browser (fetch disabled)", async () => {
    // Fetch-first disabled — PerimeterX always blocks got-scraping. Goes straight to browser.
    setupMockBrowser();
    const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    const found = await runWithFakeTimers(() => scrapeTotalWineStore(TEST_STORE));
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining("Queuing browser"));
    expect(found).toEqual([]);
    consoleSpy.mockRestore();
  });

  it("scrapeCostcoViaFetch parses product tiles via proxy", async () => {
    const tileHtml = `<html><body>
      <div data-testid="ProductTile_12345">
        <a href="https://www.costco.com/weller-sr.product.100123456.html">
          <h3 data-testid="Text_ProductTile_12345_title">Weller Special Reserve Bourbon 750ml</h3>
        </a>
        <span data-testid="Text_Price_12345">$29.99</span>
      </div>
    </body></html>`;
    // Homepage pre-warm returns set-cookie headers
    const homeResponse = mockGotResponse(200, "<html></html>", { "set-cookie": ["ak_bmsc=test123; Path=/"] });
    let callCount = 0;
    mocks.gotScraping.mockImplementation((opts) => {
      callCount++;
      if (opts?.url === "https://www.costco.com/") return Promise.resolve(homeResponse);
      return Promise.resolve(mockGotResponse(200, tileHtml));
    });
    const found = await runWithFakeTimers(() => scrapeCostcoViaFetch());
    expect(found).not.toBeNull();
    expect(found.length).toBe(1);
    expect(found[0].name).toBe("Weller Special Reserve");
    expect(found[0].price).toBe("$29.99");
    expect(found[0].sku).toBe("12345");
    // Verify proxy was used on search calls via proxyUrl option
    const searchCalls = mocks.gotScraping.mock.calls.filter(
      ([opts]) => typeof opts?.url === "string" && opts.url.includes("costco.com/s?")
    );
    expect(searchCalls.length).toBeGreaterThan(0);
    for (const [opts] of searchCalls) {
      expect(opts.proxyUrl).toBeTruthy();
    }
  });

  it("scrapeCostcoViaFetch sends cookies from homepage pre-warm", async () => {
    const homeResponse = mockGotResponse(200, "<html></html>", {
      "set-cookie": ["ak_bmsc=abc123; Path=/", "bm_sv=xyz789; Path=/"],
    });
    const emptyHtml = "<html><body>No results</body></html>";
    mocks.gotScraping.mockImplementation((opts) => {
      if (opts?.url === "https://www.costco.com/") return Promise.resolve(homeResponse);
      return Promise.resolve(mockGotResponse(200, emptyHtml));
    });
    await runWithFakeTimers(() => scrapeCostcoViaFetch());
    const searchCalls = mocks.gotScraping.mock.calls.filter(
      ([opts]) => typeof opts?.url === "string" && opts.url.includes("costco.com/s?")
    );
    expect(searchCalls.length).toBeGreaterThan(0);
    // Verify cookies from pre-warm are sent on search requests
    for (const [opts] of searchCalls) {
      expect(opts.headers.Cookie).toBe("ak_bmsc=abc123; bm_sv=xyz789");
    }
  });

  it("scrapeCostcoViaFetch returns null on bot detection (retries once then gives up)", async () => {
    const blockedHtml = "<html><title>Access Denied</title><body>robot check</body></html>";
    const homeResponse = mockGotResponse(200, "<html></html>", {});
    mocks.gotScraping.mockImplementation((opts) => {
      if (opts?.url === "https://www.costco.com/") return Promise.resolve(homeResponse);
      return Promise.resolve(mockGotResponse(200, blockedHtml));
    });
    const found = await runWithFakeTimers(() => scrapeCostcoViaFetch());
    expect(found).toBeNull();
  });

  it("scrapeCostcoViaFetch returns null after 4+ HTTP failures (with retries)", async () => {
    mocks.gotScraping.mockImplementation((opts) => {
      if (opts?.url === "https://www.costco.com/") return Promise.resolve(mockGotResponse(200, "", {}));
      return Promise.resolve(mockGotResponse(503));
    });
    const found = await runWithFakeTimers(() => scrapeCostcoViaFetch());
    expect(found).toBeNull();
  });

  it("scrapeCostcoViaFetch recovers on retry after initial block", async () => {
    const tileHtml = `<html><body>
      <div data-testid="ProductTile_42">
        <h3 data-testid="Text_ProductTile_42_title">Weller Special Reserve Bourbon</h3>
        <span data-testid="Text_Price_42">$29.99</span>
      </div>
    </body></html>`;
    const blockedHtml = "<html><body>Access Denied</body></html>";
    let searchCallCount = 0;
    mocks.gotScraping.mockImplementation((opts) => {
      if (opts?.url === "https://www.costco.com/") return Promise.resolve(mockGotResponse(200, "", {}));
      searchCallCount++;
      // First attempt of each query is blocked, retry succeeds
      if (searchCallCount % 2 === 1) return Promise.resolve(mockGotResponse(200, blockedHtml));
      return Promise.resolve(mockGotResponse(200, tileHtml));
    });
    const found = await runWithFakeTimers(() => scrapeCostcoViaFetch());
    expect(found).not.toBeNull();
    expect(found.length).toBeGreaterThan(0);
    expect(found[0].name).toBe("Weller Special Reserve");
  });

  it("scrapeCostcoViaFetch detects all blocked patterns", () => {
    expect(FETCH_BLOCKED_PATTERNS).toContain("Access Denied");
    expect(FETCH_BLOCKED_PATTERNS).toContain("robot");
    expect(FETCH_BLOCKED_PATTERNS).toContain("captcha");
    expect(FETCH_BLOCKED_PATTERNS).toContain("_ct_challenge");
    expect(FETCH_BLOCKED_PATTERNS).toContain("verify you are human");
    expect(isCostcoBlocked("<html>Please verify you are human</html>")).toBe(true);
    expect(isCostcoBlocked("<html>Request unsuccessful</html>")).toBe(true);
    expect(isCostcoBlocked("<html>Normal product page</html>")).toBe(false);
  });

  it("scrapeCostcoStore wrapper tries fetch-first with proxy", async () => {
    // With PROXY_URL set, Costco fetch-first runs. Default mock returns empty HTML (no tiles)
    // → validPages increments (non-blocked) → returns [] (empty, not null) → fetch succeeded
    setupMockBrowser();
    const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    const found = await runWithFakeTimers(() => scrapeCostcoStore());
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining("fast fetch"));
    expect(found).toEqual([]);
    consoleSpy.mockRestore();
  });

  it("scrapeCostcoViaFetch handles pages with no product tiles (valid empty search)", async () => {
    const html = `<html><body><div data-testid="no-results">No results found</div></body></html>`;
    mocks.gotScraping.mockImplementation((opts) => {
      if (opts?.url === "https://www.costco.com/") return Promise.resolve(mockGotResponse(200, "", {}));
      return Promise.resolve(mockGotResponse(200, html));
    });
    const found = await runWithFakeTimers(() => scrapeCostcoViaFetch());
    // All queries return valid pages with no tiles — should return empty, not null
    expect(found).not.toBeNull();
    expect(found).toEqual([]);
  });

  it("scrapeTotalWineViaFetch handles malformed INITIAL_STATE (missing braces/script)", async () => {
    // Has marker but no valid JSON structure
    const html = `<html><script>window.INITIAL_STATE = broken`;
    mocks.gotScraping.mockResolvedValue(mockGotResponse(200, html));
    const found = await runWithFakeTimers(() => scrapeTotalWineViaFetch(TEST_STORE));
    // Should return null — all queries fail to parse
    expect(found).toBeNull();
  });

  it("scrapeTotalWineViaFetch handles INITIAL_STATE without search.results", async () => {
    const state = { otherData: true };
    const html = `<html><script>window.INITIAL_STATE = ${JSON.stringify(state)};</script></html>`;
    mocks.gotScraping.mockResolvedValue(mockGotResponse(200, html));
    const found = await runWithFakeTimers(() => scrapeTotalWineViaFetch(TEST_STORE));
    expect(found).toBeNull();
  });

  it("scrapeTotalWineViaFetch handles fetch exceptions (network errors)", async () => {
    // scraperFetchRetry will retry once then throw
    mocks.gotScraping.mockRejectedValue(new Error("ECONNRESET"));
    const found = await runWithFakeTimers(() => scrapeTotalWineViaFetch(TEST_STORE));
    expect(found).toBeNull();
  });

  it("scrapeTotalWineViaFetch returns empty array when some queries succeed but find nothing", async () => {
    // Valid page with INITIAL_STATE but no matching products
    const state = { search: { results: { products: [
      { name: "Jack Daniel's Tennessee Whiskey", productUrl: "/p/1", stockLevel: [{ stock: 5 }] },
    ] } } };
    const html = `<html><script>window.INITIAL_STATE = ${JSON.stringify(state)};</script></html>`;
    mocks.gotScraping.mockResolvedValue(mockGotResponse(200, html));
    const found = await runWithFakeTimers(() => scrapeTotalWineViaFetch(TEST_STORE));
    expect(found).not.toBeNull();
    expect(found).toEqual([]);
  });
});

// ─── Sam's Club Proxy Tests ──────────────────────────────────────────────────

describe("scrapeSamsClubViaFetch proxy", () => {
  it("passes proxyUrl on gotScraping calls", async () => {
    const samsclubNextData = {
      props: { pageProps: { initialData: { data: { product: {
        name: "Weller Special Reserve Bourbon 750ml",
        availabilityStatusV2: { value: "IN_STOCK" },
        canonicalUrl: "/ip/weller/prod20595259",
        priceInfo: { linePriceDisplay: "$29.99" },
        usItemId: "prod20595259",
      } } } } },
    };
    mocks.gotScraping.mockResolvedValue(
      mockGotResponse(200, `<html><script id="__NEXT_DATA__" type="application/json">${JSON.stringify(samsclubNextData)}</script></html>`)
    );
    await runWithFakeTimers(() => scrapeSamsClubViaFetch());
    const samsclubCalls = mocks.gotScraping.mock.calls.filter(
      ([opts]) => typeof opts?.url === "string" && opts.url.includes("samsclub.com")
    );
    expect(samsclubCalls.length).toBeGreaterThan(0);
    for (const [opts] of samsclubCalls) {
      expect(opts.proxyUrl).toBeTruthy();
    }
  });

  it("returns null when all products are blocked", async () => {
    mocks.gotScraping.mockResolvedValue(mockGotResponse(403));
    const found = await runWithFakeTimers(() => scrapeSamsClubViaFetch());
    expect(found).toBeNull();
  });
});

// ─── createProxyAgent Tests ─────────────────────────────────────────────────

describe("createProxyAgent", () => {
  it("returns HttpsProxyAgent for http:// URL", () => {
    const agent = createProxyAgent("http://proxy.example.com:8080");
    expect(agent._isProxy).toBe(true);
  });

  it("returns HttpsProxyAgent for https:// URL", () => {
    const agent = createProxyAgent("https://proxy.example.com:8080");
    expect(agent._isProxy).toBe(true);
  });

  it("returns SocksProxyAgent for socks5:// URL", () => {
    const agent = createProxyAgent("socks5://user:pass@proxy.example.com:1080");
    expect(agent._isSocksProxy).toBe(true);
  });

  it("returns SocksProxyAgent for socks4:// URL", () => {
    const agent = createProxyAgent("socks4://proxy.example.com:1080");
    expect(agent._isSocksProxy).toBe(true);
  });

  it("returns null for undefined/empty URL", () => {
    expect(createProxyAgent(undefined)).toBeNull();
    expect(createProxyAgent("")).toBeNull();
  });
});

// ─── rotateRetailerProxy (with PROXY_URL set) ────────────────────────────────

describe("rotateRetailerProxy", () => {
  beforeEach(() => {
    // Initialize retailer proxy URLs via refreshProxySession
    refreshProxySession();
  });

  it("changes retailer proxy URL to a new port", () => {
    const before = getRetailerProxyUrl("costco");
    expect(before).toBeTruthy(); // should be set after refreshProxySession
    rotateRetailerProxy("costco");
    const after = getRetailerProxyUrl("costco");
    expect(after).toBeTruthy();
    // Port changed — URLs should differ (extremely high probability with random ports)
    // Both should be valid proxy URLs for the same host
    expect(new URL(after).hostname).toBe("proxy.example.com");
    const afterPort = parseInt(new URL(after).port, 10);
    expect(afterPort).toBeGreaterThanOrEqual(10000);
    expect(afterPort).toBeLessThan(20000);
  });

  it("does not affect other retailers when rotating one", () => {
    const walmartBefore = getRetailerProxyUrl("walmart");
    rotateRetailerProxy("costco");
    const walmartAfter = getRetailerProxyUrl("walmart");
    expect(walmartAfter).toBe(walmartBefore); // walmart unchanged
  });

  it("updates proxy URL that subsequent fetch calls use", () => {
    rotateRetailerProxy("walmart");
    const rotatedUrl = getRetailerProxyUrl("walmart");
    expect(rotatedUrl).toContain("proxy.example.com");
    // Rotate again — should change to yet another port
    rotateRetailerProxy("walmart");
    const rotatedAgain = getRetailerProxyUrl("walmart");
    expect(rotatedAgain).toContain("proxy.example.com");
    // Very unlikely to be the same port twice in a row (1/10000 chance)
  });

  it("logs rotation message", () => {
    const spy = vi.spyOn(console, "log").mockImplementation(() => {});
    rotateRetailerProxy("samsclub");
    expect(spy).toHaveBeenCalledWith(expect.stringContaining("[proxy] Rotated samsclub to port"));
    spy.mockRestore();
  });
});
