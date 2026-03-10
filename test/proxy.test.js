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
  return {
    fetch: vi.fn(),
    readFile: vi.fn(),
    writeFile: vi.fn(),
    chromiumLaunch: vi.fn(),
    chromiumUse: vi.fn(),
    cronSchedule: vi.fn(),
    discoverStores: vi.fn(),
    HttpsProxyAgent: MockHttpsProxyAgent,
    HttpsProxyAgentInstance,
  };
});

vi.mock("dotenv/config", () => ({}));
vi.mock("node-fetch", () => ({ default: mocks.fetch }));
vi.mock("playwright-extra", () => ({
  chromium: { use: mocks.chromiumUse, launch: mocks.chromiumLaunch },
}));
vi.mock("puppeteer-extra-plugin-stealth", () => ({ default: vi.fn() }));
vi.mock("https-proxy-agent", () => ({ HttpsProxyAgent: mocks.HttpsProxyAgent }));
vi.mock("node-cron", () => ({ default: { schedule: mocks.cronSchedule } }));
vi.mock("node:fs/promises", () => ({
  readFile: mocks.readFile,
  writeFile: mocks.writeFile,
}));
vi.mock("../lib/discover-stores.js", () => ({
  discoverStores: mocks.discoverStores,
}));

// ─── Import module under test (with PROXY_URL set) ───────────────────────────

import {
  FETCH_HEADERS,
  launchBrowser, closeBrowser,
  scrapeWalmartViaFetch, scrapeWalmartStore, scrapeKrogerStore, scrapeSafewayStore,
  getKrogerToken, main,
  _resetKrogerToken, _resetPolling, _setStoreCache,
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
    evaluate: vi.fn().mockResolvedValue([]),
    $$eval: vi.fn().mockResolvedValue([]),
    $eval: vi.fn().mockResolvedValue(null),
    context: vi.fn(() => ({ close: vi.fn().mockResolvedValue(undefined) })),
  };
  const mockContext = { newPage: vi.fn().mockResolvedValue(mockPage), close: vi.fn().mockResolvedValue(undefined) };
  const mockBrowser = { newContext: vi.fn().mockResolvedValue(mockContext), close: vi.fn().mockResolvedValue(undefined) };
  mocks.chromiumLaunch.mockResolvedValue(mockBrowser);
  return { browser: mockBrowser, context: mockContext, page: mockPage };
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
  _resetKrogerToken();
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

  it("scrapeWalmartViaFetch passes proxy agent on fetch calls", async () => {
    const walmartNextData = {
      props: { pageProps: { initialData: { searchResult: { itemStacks: [{ items: [{
        __typename: "Product", name: "Weller Special Reserve Bourbon",
        availabilityStatusV2: { value: "IN_STOCK" }, sellerName: "Walmart.com",
      }] }] } } } },
    };
    mocks.fetch.mockResolvedValue({
      ok: true,
      text: async () => `<script id="__NEXT_DATA__">${JSON.stringify(walmartNextData)}</script>`,
    });
    await runWithFakeTimers(() => scrapeWalmartViaFetch(TEST_STORE));
    // Every Walmart fetch call should include the proxy agent
    const walmartCalls = mocks.fetch.mock.calls.filter(
      ([url]) => typeof url === "string" && url.includes("walmart.com")
    );
    expect(walmartCalls.length).toBeGreaterThan(0);
    for (const [, opts] of walmartCalls) {
      expect(opts.agent._isProxy).toBe(true);
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
    mocks.fetch.mockResolvedValue({
      ok: true,
      text: async () => `<script id="__NEXT_DATA__">${JSON.stringify(walmartNextData)}</script>`,
    });
    const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    const found = await runWithFakeTimers(() => scrapeWalmartStore(TEST_STORE));
    expect(found).not.toBeNull();
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining("(proxied)"));
    consoleSpy.mockRestore();
    delete process.env.CI;
  });

  it("getKrogerToken passes proxy agent on OAuth fetch", async () => {
    mocks.fetch.mockResolvedValueOnce({ ok: true, json: async () => ({ access_token: "proxy-token" }) });
    await getKrogerToken();
    const oauthCall = mocks.fetch.mock.calls.find(
      ([url]) => typeof url === "string" && url.includes("oauth2/token")
    );
    expect(oauthCall).toBeTruthy();
    expect(oauthCall[1].agent._isProxy).toBe(true);
  });

  it("scrapeKrogerStore passes proxy agent on API fetch", async () => {
    _resetKrogerToken();
    mocks.fetch
      .mockResolvedValueOnce({ ok: true, json: async () => ({ access_token: "tk" }) })
      .mockResolvedValue({ ok: true, json: async () => ({ data: [] }) });
    await runWithFakeTimers(() => scrapeKrogerStore(TEST_STORE));
    const apiCalls = mocks.fetch.mock.calls.filter(
      ([url]) => typeof url === "string" && url.includes("api.kroger.com/v1/products")
    );
    expect(apiCalls.length).toBeGreaterThan(0);
    for (const [, opts] of apiCalls) {
      expect(opts.agent._isProxy).toBe(true);
    }
  });

  it("scrapeSafewayStore passes proxy agent on API fetch", async () => {
    mocks.fetch.mockResolvedValue({
      ok: true,
      json: async () => ({ primaryProducts: { response: { docs: [] } } }),
    });
    await runWithFakeTimers(() => scrapeSafewayStore(TEST_STORE));
    const safewayCalls = mocks.fetch.mock.calls.filter(
      ([url]) => typeof url === "string" && url.includes("safeway.com")
    );
    expect(safewayCalls.length).toBeGreaterThan(0);
    for (const [, opts] of safewayCalls) {
      expect(opts.agent._isProxy).toBe(true);
    }
  });

  it("Discord webhook calls do NOT include proxy agent", async () => {
    _resetPolling();
    _resetKrogerToken();
    mocks.readFile.mockResolvedValue("{}");
    mocks.writeFile.mockResolvedValue(undefined);
    mocks.fetch.mockResolvedValue({ ok: true });
    setupMockBrowser();
    _setStoreCache({
      retailers: { costco: [TEST_STORE], totalwine: [], walmart: [], kroger: [], safeway: [] },
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
      retailers: { costco: [], totalwine: [], walmart: [], kroger: [], safeway: [] },
    });
    await runWithFakeTimers(() => main());
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining("[proxy]"));
    delete process.env.RUN_ONCE;
    exitSpy.mockRestore();
    consoleSpy.mockRestore();
  });
});
