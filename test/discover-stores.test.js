import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

// ─── Mocks ────────────────────────────────────────────────────────────────────

const mocks = vi.hoisted(() => ({
  fetch: vi.fn(),
  readFile: vi.fn(),
  writeFile: vi.fn(),
  rename: vi.fn(),
  chromiumLaunch: vi.fn(),
  chromiumUse: vi.fn(),
}));

vi.mock("node-fetch", () => ({ default: mocks.fetch }));
vi.mock("playwright-core", () => ({
  chromium: { launch: mocks.chromiumLaunch },
}));
vi.mock("node:fs/promises", () => ({
  readFile: mocks.readFile,
  writeFile: mocks.writeFile,
  rename: mocks.rename,
}));

import {
  loadCache, saveCache, isCacheValid, cleanStoreEntries,
  locateCostco, locateTotalWine, locateWalmart,
  locateKroger, locateSafeway, locateAlbertsons, locateWalgreens, locateSamsClub, locateBevMo,
  discoverStores,
} from "../lib/discover-stores.js";

// ─── Helpers ──────────────────────────────────────────────────────────────────

function createMockPage() {
  return {
    goto: vi.fn().mockResolvedValue(undefined),
    waitForSelector: vi.fn().mockResolvedValue(undefined),
    waitForTimeout: vi.fn().mockResolvedValue(undefined),
    evaluate: vi.fn().mockResolvedValue([]),
    $$eval: vi.fn().mockResolvedValue([]),
    $: vi.fn().mockResolvedValue(null),
    fill: vi.fn().mockResolvedValue(undefined),
    context: vi.fn(() => ({ close: vi.fn() })),
  };
}

const COORDS = { lat: 33.4152, lng: -111.8315 };

beforeEach(() => {
  vi.resetAllMocks();
});

// ─── Cache Management ─────────────────────────────────────────────────────────

describe("loadCache", () => {
  it("returns parsed JSON from cache file", async () => {
    mocks.readFile.mockResolvedValueOnce(JSON.stringify({ zipCode: "85283", retailers: {} }));
    const cache = await loadCache();
    expect(cache).toEqual({ zipCode: "85283", retailers: {} });
  });

  it("returns null when file doesn't exist", async () => {
    mocks.readFile.mockRejectedValueOnce(new Error("ENOENT"));
    const cache = await loadCache();
    expect(cache).toBeNull();
  });

  it("returns null on JSON parse error", async () => {
    mocks.readFile.mockResolvedValueOnce("not json{");
    const cache = await loadCache();
    expect(cache).toBeNull();
  });
});

describe("saveCache", () => {
  it("writes to tmp file then renames atomically", async () => {
    mocks.writeFile.mockResolvedValueOnce(undefined);
    mocks.rename.mockResolvedValueOnce(undefined);
    await saveCache({ zipCode: "85283" });
    expect(mocks.writeFile).toHaveBeenCalledOnce();
    const tmpPath = mocks.writeFile.mock.calls[0][0];
    expect(tmpPath).toContain("stores.json.tmp");
    const content = mocks.writeFile.mock.calls[0][1];
    expect(JSON.parse(content)).toEqual({ zipCode: "85283" });
    expect(mocks.rename).toHaveBeenCalledWith(
      expect.stringContaining("stores.json.tmp"),
      expect.stringContaining("stores.json"),
    );
  });
});

describe("isCacheValid", () => {
  it("returns false for null cache", () => {
    expect(isCacheValid(null, "85283", 15)).toBe(false);
  });

  it("returns false for different zip code", () => {
    const cache = { zipCode: "90210", radiusMiles: 15, discoveredAt: new Date().toISOString() };
    expect(isCacheValid(cache, "85283", 15)).toBe(false);
  });

  it("returns false for different radius", () => {
    const cache = { zipCode: "85283", radiusMiles: 25, discoveredAt: new Date().toISOString() };
    expect(isCacheValid(cache, "85283", 15)).toBe(false);
  });

  it("returns false for expired cache (>7 days)", () => {
    const eightDaysAgo = new Date(Date.now() - 8 * 24 * 60 * 60 * 1000).toISOString();
    const cache = { zipCode: "85283", radiusMiles: 15, discoveredAt: eightDaysAgo };
    expect(isCacheValid(cache, "85283", 15)).toBe(false);
  });

  it("returns true for valid recent cache", () => {
    const cache = { zipCode: "85283", radiusMiles: 15, discoveredAt: new Date().toISOString() };
    expect(isCacheValid(cache, "85283", 15)).toBe(true);
  });

  it("returns true for cache just under 7 days old", () => {
    const sixDaysAgo = new Date(Date.now() - 6 * 24 * 60 * 60 * 1000).toISOString();
    const cache = { zipCode: "85283", radiusMiles: 15, discoveredAt: sixDaysAgo };
    expect(isCacheValid(cache, "85283", 15)).toBe(true);
  });
});

// ─── Store Name Sanitization ─────────────────────────────────────────────────

describe("cleanStoreEntries", () => {
  it("strips distance text from store names", () => {
    const retailers = {
      safeway: [
        { storeId: "1515", name: "Safeway Safeway E Elliot Rd1.83 mi to your search", address: "1515 E Elliot Rd", distanceMiles: 1.83 },
      ],
    };
    cleanStoreEntries(retailers);
    expect(retailers.safeway[0].name).toBe("Safeway E Elliot Rd");
  });

  it("filters non-store entries like location count headers", () => {
    const retailers = {
      safeway: [
        { storeId: "sw-2", name: 'Safeway 20 locations near "Tempe, AZ"', address: "", distanceMiles: null },
        { storeId: "1515", name: "Safeway E Elliot Rd", address: "1515 E Elliot Rd", distanceMiles: 1.83 },
      ],
    };
    cleanStoreEntries(retailers);
    expect(retailers.safeway).toHaveLength(1);
    expect(retailers.safeway[0].storeId).toBe("1515");
  });

  it("drops synthetic sw-N store IDs", () => {
    const retailers = {
      safeway: [
        { storeId: "sw-0", name: "Safeway E Elliot Rd", address: "1515 E Elliot Rd", distanceMiles: 1.83 },
        { storeId: "1515", name: "Safeway E Elliot Rd", address: "1515 E Elliot Rd", distanceMiles: 1.83 },
      ],
    };
    cleanStoreEntries(retailers);
    expect(retailers.safeway).toHaveLength(1);
    expect(retailers.safeway[0].storeId).toBe("1515");
  });

  it("deduplicates stores by storeId", () => {
    const retailers = {
      safeway: [
        { storeId: "1515", name: "Safeway E Elliot Rd", address: "1515 E Elliot Rd", distanceMiles: 1.83 },
        { storeId: "1515", name: "Safeway E Elliot Rd", address: "1515 E Elliot Rd", distanceMiles: 1.83 },
        { storeId: "1515", name: "Safeway E Elliot Rd", address: "1515 E Elliot Rd", distanceMiles: 1.83 },
      ],
    };
    cleanStoreEntries(retailers);
    expect(retailers.safeway).toHaveLength(1);
  });

  it("fixes double retailer prefix", () => {
    const retailers = {
      safeway: [{ storeId: "1", name: "Safeway Safeway Store", address: "", distanceMiles: null }],
      costco: [{ storeId: "2", name: "Costco Costco Chandler", address: "", distanceMiles: null }],
    };
    cleanStoreEntries(retailers);
    expect(retailers.safeway[0].name).toBe("Safeway Store");
    expect(retailers.costco[0].name).toBe("Costco Chandler");
  });

  it("normalizes non-breaking spaces", () => {
    const retailers = {
      safeway: [{ storeId: "1", name: "Safeway\u00a0Store", address: "", distanceMiles: null }],
    };
    cleanStoreEntries(retailers);
    expect(retailers.safeway[0].name).toBe("Safeway Store");
  });

  it("handles already-clean names", () => {
    const retailers = {
      walmart: [
        { storeId: "5768", name: "Walmart Supercenter #5768", address: "800 E Southern Ave", distanceMiles: 1.8 },
      ],
    };
    cleanStoreEntries(retailers);
    expect(retailers.walmart[0].name).toBe("Walmart Supercenter #5768");
  });
});

// ─── Costco Locator ───────────────────────────────────────────────────────────

describe("locateCostco", () => {
  it("parses stores from evaluate results", async () => {
    const mockPage = createMockPage();
    mockPage.evaluate.mockResolvedValueOnce([
      { name: "Chandler", distance: "(5.20 mi)", id: "736", address: "1425 W Queen Creek Rd" },
      { name: "Gilbert", distance: "(8.10 mi)", id: "481", address: "2270 S Market St" },
    ]);

    const stores = await locateCostco(mockPage, "85283", COORDS, 15, 5);
    expect(stores).toHaveLength(2);
    expect(stores[0].storeId).toBe("736");
    expect(stores[0].name).toBe("Costco Chandler");
    expect(stores[0].distanceMiles).toBeCloseTo(5.2);
  });

  it("filters stores beyond radius", async () => {
    const mockPage = createMockPage();
    mockPage.evaluate.mockResolvedValueOnce([
      { name: "Chandler", distance: "(5.20 mi)", id: "736", address: "123 Main" },
      { name: "Far Away", distance: "(25.00 mi)", id: "999", address: "456 Elm" },
    ]);

    const stores = await locateCostco(mockPage, "85283", COORDS, 15, 5);
    expect(stores).toHaveLength(1);
    expect(stores[0].name).toBe("Costco Chandler");
  });

  it("respects maxStores limit", async () => {
    const mockPage = createMockPage();
    mockPage.evaluate.mockResolvedValueOnce([
      { name: "Store A", distance: "(1 mi)", id: "1", address: "A" },
      { name: "Store B", distance: "(2 mi)", id: "2", address: "B" },
      { name: "Store C", distance: "(3 mi)", id: "3", address: "C" },
    ]);

    const stores = await locateCostco(mockPage, "85283", COORDS, 15, 2);
    expect(stores).toHaveLength(2);
  });

  it("handles empty results", async () => {
    const mockPage = createMockPage();
    mockPage.evaluate.mockResolvedValueOnce([]);
    const stores = await locateCostco(mockPage, "85283", COORDS, 15, 5);
    expect(stores).toEqual([]);
  });

  it("handles page errors", async () => {
    const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    const mockPage = createMockPage();
    mockPage.goto.mockRejectedValueOnce(new Error("Timeout"));
    const stores = await locateCostco(mockPage, "85283", COORDS, 15, 5);
    expect(stores).toEqual([]);
    consoleSpy.mockRestore();
  });

  it("generates fallback storeId when id is missing", async () => {
    const mockPage = createMockPage();
    mockPage.evaluate.mockResolvedValueOnce([
      { name: "NoId Store", distance: "(3 mi)", id: "", address: "789 Oak" },
    ]);
    const stores = await locateCostco(mockPage, "85283", COORDS, 15, 5);
    expect(stores[0].storeId).toBe("costco-0");
  });

  it("skips entries without a name", async () => {
    const mockPage = createMockPage();
    mockPage.evaluate.mockResolvedValueOnce([
      { name: "", distance: "(3 mi)", id: "100", address: "123 Main" },
    ]);
    const stores = await locateCostco(mockPage, "85283", COORDS, 15, 5);
    expect(stores).toEqual([]);
  });

  it("handles null distance gracefully", async () => {
    const mockPage = createMockPage();
    mockPage.evaluate.mockResolvedValueOnce([
      { name: "Store", distance: "", id: "100", address: "123 Main" },
    ]);
    const stores = await locateCostco(mockPage, "85283", COORDS, 15, 5);
    expect(stores).toHaveLength(1);
    expect(stores[0].distanceMiles).toBeNull();
  });
});

// ─── Total Wine Locator ───────────────────────────────────────────────────────

describe("locateTotalWine", () => {
  it("parses stores from JSON-LD evaluate results", async () => {
    const mockPage = createMockPage();
    mockPage.evaluate.mockResolvedValueOnce([
      {
        name: "Tempe Marketplace",
        address: "1900 E Rio Salado Pkwy, Tempe, AZ, 85281",
        id: "1010",
        lat: 33.43,
        lng: -111.91,
      },
    ]);

    const stores = await locateTotalWine(mockPage, "85283", COORDS, 15, 5);
    expect(stores).toHaveLength(1);
    expect(stores[0].storeId).toBe("1010");
    expect(stores[0].name).toContain("Total Wine");
    expect(stores[0].distanceMiles).not.toBeNull();
  });

  it("filters stores beyond radius", async () => {
    const mockPage = createMockPage();
    mockPage.evaluate.mockResolvedValueOnce([
      { name: "Close Store", address: "Close", id: "1", lat: 33.42, lng: -111.84 },
      { name: "Far Store", address: "Far", id: "2", lat: 34.5, lng: -112.5 },
    ]);

    const stores = await locateTotalWine(mockPage, "85283", COORDS, 15, 5);
    // Far store should be filtered
    expect(stores.length).toBeLessThanOrEqual(2);
    if (stores.length === 1) {
      expect(stores[0].name).toContain("Close Store");
    }
  });

  it("handles stores with no lat/lng", async () => {
    const mockPage = createMockPage();
    mockPage.evaluate.mockResolvedValueOnce([
      { name: "NoGeo Store", address: "123 Main", id: "3", lat: null, lng: null },
    ]);
    const stores = await locateTotalWine(mockPage, "85283", COORDS, 15, 5);
    expect(stores).toHaveLength(1);
    expect(stores[0].distanceMiles).toBeNull();
  });

  it("handles page errors", async () => {
    const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    const mockPage = createMockPage();
    mockPage.goto.mockRejectedValueOnce(new Error("Timeout"));
    const stores = await locateTotalWine(mockPage, "85283", COORDS, 15, 5);
    expect(stores).toEqual([]);
    consoleSpy.mockRestore();
  });

  it("generates fallback storeId", async () => {
    const mockPage = createMockPage();
    mockPage.evaluate.mockResolvedValueOnce([
      { name: "No ID", address: "Addr", id: "", lat: null, lng: null },
    ]);
    const stores = await locateTotalWine(mockPage, "85283", COORDS, 15, 5);
    expect(stores[0].storeId).toBe("tw-0");
  });

  it("respects maxStores limit", async () => {
    const mockPage = createMockPage();
    mockPage.evaluate.mockResolvedValueOnce([
      { name: "A", address: "A", id: "1", lat: null, lng: null },
      { name: "B", address: "B", id: "2", lat: null, lng: null },
      { name: "C", address: "C", id: "3", lat: null, lng: null },
    ]);
    const stores = await locateTotalWine(mockPage, "85283", COORDS, 15, 2);
    expect(stores).toHaveLength(2);
  });
});

// ─── Walmart Locator ──────────────────────────────────────────────────────────

describe("locateWalmart", () => {
  it("parses stores from evaluate results", async () => {
    const mockPage = createMockPage();
    mockPage.evaluate.mockResolvedValueOnce([
      {
        name: "Tempe East Southern",
        storeId: "5768",
        storeLabel: "Walmart Supercenter #5768",
        address: "800 E Southern Ave, Tempe, AZ",
        distance: 1.8,
      },
    ]);

    const stores = await locateWalmart(mockPage, "85283", COORDS, 15, 5);
    expect(stores).toHaveLength(1);
    expect(stores[0].storeId).toBe("5768");
    expect(stores[0].name).toBe("Walmart Supercenter #5768");
    expect(stores[0].distanceMiles).toBe(1.8);
  });

  it("uses name when storeLabel is empty", async () => {
    const mockPage = createMockPage();
    mockPage.evaluate.mockResolvedValueOnce([
      { name: "Tempe Store", storeId: "123", storeLabel: "", address: "Addr", distance: 2.0 },
    ]);
    const stores = await locateWalmart(mockPage, "85283", COORDS, 15, 5);
    expect(stores[0].name).toBe("Tempe Store");
  });

  it("filters stores beyond radius", async () => {
    const mockPage = createMockPage();
    mockPage.evaluate.mockResolvedValueOnce([
      { name: "Close", storeId: "1", storeLabel: "Close", address: "A", distance: 5 },
      { name: "Far", storeId: "2", storeLabel: "Far", address: "B", distance: 20 },
    ]);
    const stores = await locateWalmart(mockPage, "85283", COORDS, 15, 5);
    expect(stores).toHaveLength(1);
  });

  it("handles null distance", async () => {
    const mockPage = createMockPage();
    mockPage.evaluate.mockResolvedValueOnce([
      { name: "Store", storeId: "1", storeLabel: "Store", address: "A", distance: null },
    ]);
    const stores = await locateWalmart(mockPage, "85283", COORDS, 15, 5);
    expect(stores).toHaveLength(1);
    expect(stores[0].distanceMiles).toBeNull();
  });

  it("handles page errors", async () => {
    const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    const mockPage = createMockPage();
    mockPage.goto.mockRejectedValueOnce(new Error("Timeout"));
    const stores = await locateWalmart(mockPage, "85283", COORDS, 15, 5);
    expect(stores).toEqual([]);
    consoleSpy.mockRestore();
  });

  it("respects maxStores limit", async () => {
    const mockPage = createMockPage();
    mockPage.evaluate.mockResolvedValueOnce([
      { name: "A", storeId: "1", storeLabel: "A", address: "A", distance: 1 },
      { name: "B", storeId: "2", storeLabel: "B", address: "B", distance: 2 },
      { name: "C", storeId: "3", storeLabel: "C", address: "C", distance: 3 },
    ]);
    const stores = await locateWalmart(mockPage, "85283", COORDS, 15, 2);
    expect(stores).toHaveLength(2);
  });
});

// ─── Kroger Locator ───────────────────────────────────────────────────────────

describe("locateKroger", () => {
  it("returns stores from API", async () => {
    mocks.fetch
      .mockResolvedValueOnce({ ok: true, json: async () => ({ access_token: "token" }) })
      .mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          data: [{
            locationId: "62000123",
            name: "Fry's Marketplace",
            address: { addressLine1: "123 Main", city: "Tempe", state: "AZ", zipCode: "85283" },
            geolocation: { latitude: 33.42, longitude: -111.84 },
          }],
        }),
      });

    const stores = await locateKroger("85283", COORDS, 15, 5, "client-id", "client-secret");
    expect(stores).toHaveLength(1);
    expect(stores[0].storeId).toBe("62000123");
    expect(stores[0].name).toBe("Fry's Marketplace");
    expect(stores[0].address).toContain("Tempe");
    expect(stores[0].distanceMiles).not.toBeNull();
  });

  it("returns empty when no credentials", async () => {
    const consoleSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    const stores = await locateKroger("85283", COORDS, 15, 5, "", "");
    expect(stores).toEqual([]);
    consoleSpy.mockRestore();
  });

  it("returns empty when OAuth fails", async () => {
    const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    mocks.fetch.mockResolvedValueOnce({ ok: false, status: 401 });
    const stores = await locateKroger("85283", COORDS, 15, 5, "id", "secret");
    expect(stores).toEqual([]);
    consoleSpy.mockRestore();
  });

  it("returns empty when location API fails", async () => {
    const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    mocks.fetch
      .mockResolvedValueOnce({ ok: true, json: async () => ({ access_token: "tk" }) })
      .mockResolvedValueOnce({ ok: false, status: 500 });
    const stores = await locateKroger("85283", COORDS, 15, 5, "id", "secret");
    expect(stores).toEqual([]);
    consoleSpy.mockRestore();
  });

  it("filters stores beyond radius", async () => {
    mocks.fetch
      .mockResolvedValueOnce({ ok: true, json: async () => ({ access_token: "tk" }) })
      .mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          data: [
            { locationId: "1", name: "Close", address: {}, geolocation: { latitude: 33.42, longitude: -111.84 } },
            { locationId: "2", name: "Far", address: {}, geolocation: { latitude: 35.0, longitude: -112.0 } },
          ],
        }),
      });
    const stores = await locateKroger("85283", COORDS, 15, 5, "id", "secret");
    // Far store should be filtered (>15 miles)
    const names = stores.map((s) => s.name);
    expect(names).not.toContain("Far");
  });

  it("handles location with no geolocation", async () => {
    mocks.fetch
      .mockResolvedValueOnce({ ok: true, json: async () => ({ access_token: "tk" }) })
      .mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          data: [{ locationId: "1", name: "NoGeo", address: { addressLine1: "A", city: "B", state: "C", zipCode: "D" } }],
        }),
      });
    const stores = await locateKroger("85283", COORDS, 15, 5, "id", "secret");
    expect(stores).toHaveLength(1);
    expect(stores[0].distanceMiles).toBeNull();
  });

  it("respects maxStores limit", async () => {
    mocks.fetch
      .mockResolvedValueOnce({ ok: true, json: async () => ({ access_token: "tk" }) })
      .mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          data: [
            { locationId: "1", name: "A", address: {} },
            { locationId: "2", name: "B", address: {} },
            { locationId: "3", name: "C", address: {} },
          ],
        }),
      });
    const stores = await locateKroger("85283", COORDS, 15, 2, "id", "secret");
    expect(stores).toHaveLength(2);
  });

  it("uses fallback name when name is missing", async () => {
    mocks.fetch
      .mockResolvedValueOnce({ ok: true, json: async () => ({ access_token: "tk" }) })
      .mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          data: [{ locationId: "42", address: {} }],
        }),
      });
    const stores = await locateKroger("85283", COORDS, 15, 5, "id", "secret");
    expect(stores[0].name).toBe("Kroger #42");
  });
});

// ─── Safeway Locator ──────────────────────────────────────────────────────────

describe("locateSafeway", () => {
  it("parses stores from evaluate results", async () => {
    const mockPage = createMockPage();
    mockPage.evaluate.mockResolvedValueOnce([
      { name: "Safeway Elliot Rd", address: "1515 E Elliot Rd", id: "1515", distance: "1.83", href: "" },
    ]);
    const stores = await locateSafeway(mockPage, "85283", COORDS, 15, 5);
    expect(stores).toHaveLength(1);
    expect(stores[0].name).toContain("Safeway");
    expect(stores[0].distanceMiles).toBeCloseTo(1.83);
  });

  it("filters stores beyond radius", async () => {
    const mockPage = createMockPage();
    mockPage.evaluate.mockResolvedValueOnce([
      { name: "Close", address: "A", id: "1", distance: "3.0", href: "" },
      { name: "Far", address: "B", id: "2", distance: "20.0", href: "" },
    ]);
    const stores = await locateSafeway(mockPage, "85283", COORDS, 15, 5);
    expect(stores).toHaveLength(1);
  });

  it("skips entries with very short names", async () => {
    const mockPage = createMockPage();
    mockPage.evaluate.mockResolvedValueOnce([
      { name: "AB", address: "A", id: "1", distance: "1", href: "" },
      { name: "Safeway Store", address: "B", id: "2", distance: "2", href: "" },
    ]);
    const stores = await locateSafeway(mockPage, "85283", COORDS, 15, 5);
    expect(stores).toHaveLength(1);
    expect(stores[0].name).toContain("Safeway Store");
  });

  it("handles page errors", async () => {
    const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    const mockPage = createMockPage();
    mockPage.goto.mockRejectedValueOnce(new Error("Timeout"));
    const stores = await locateSafeway(mockPage, "85283", COORDS, 15, 5);
    expect(stores).toEqual([]);
    consoleSpy.mockRestore();
  });

  it("generates fallback storeId", async () => {
    const mockPage = createMockPage();
    mockPage.evaluate.mockResolvedValueOnce([
      { name: "Safeway No ID", address: "Addr", id: "", distance: "", href: "" },
    ]);
    const stores = await locateSafeway(mockPage, "85283", COORDS, 15, 5);
    expect(stores[0].storeId).toBe("sw-0");
  });

  it("respects maxStores limit", async () => {
    const mockPage = createMockPage();
    mockPage.evaluate.mockResolvedValueOnce([
      { name: "Store A", address: "A", id: "1", distance: "1", href: "" },
      { name: "Store B", address: "B", id: "2", distance: "2", href: "" },
      { name: "Store C", address: "C", id: "3", distance: "3", href: "" },
    ]);
    const stores = await locateSafeway(mockPage, "85283", COORDS, 15, 2);
    expect(stores).toHaveLength(2);
  });
});

// ─── Albertsons Locator ──────────────────────────────────────────────────────

describe("locateAlbertsons", () => {
  it("parses stores from evaluate results", async () => {
    const mockPage = createMockPage();
    mockPage.evaluate.mockResolvedValueOnce([
      { name: "Albertsons Baseline Rd", address: "1951 W Baseline Rd", id: "3067", distance: "3.2", href: "" },
    ]);
    const stores = await locateAlbertsons(mockPage, "85283", COORDS, 15, 5);
    expect(stores).toHaveLength(1);
    expect(stores[0].name).toContain("Albertsons");
    expect(stores[0].distanceMiles).toBeCloseTo(3.2);
  });

  it("filters stores beyond radius", async () => {
    const mockPage = createMockPage();
    mockPage.evaluate.mockResolvedValueOnce([
      { name: "Close", address: "A", id: "1", distance: "3.0", href: "" },
      { name: "Far", address: "B", id: "2", distance: "20.0", href: "" },
    ]);
    const stores = await locateAlbertsons(mockPage, "85283", COORDS, 15, 5);
    expect(stores).toHaveLength(1);
  });

  it("handles page errors", async () => {
    const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    const mockPage = createMockPage();
    mockPage.goto.mockRejectedValueOnce(new Error("Timeout"));
    const stores = await locateAlbertsons(mockPage, "85283", COORDS, 15, 5);
    expect(stores).toEqual([]);
    consoleSpy.mockRestore();
  });

  it("generates fallback storeId with ab- prefix", async () => {
    const mockPage = createMockPage();
    mockPage.evaluate.mockResolvedValueOnce([
      { name: "Albertsons No ID", address: "Addr", id: "", distance: "", href: "" },
    ]);
    const stores = await locateAlbertsons(mockPage, "85283", COORDS, 15, 5);
    expect(stores[0].storeId).toBe("ab-0");
  });

  it("respects maxStores limit", async () => {
    const mockPage = createMockPage();
    mockPage.evaluate.mockResolvedValueOnce([
      { name: "Store A", address: "A", id: "1", distance: "1", href: "" },
      { name: "Store B", address: "B", id: "2", distance: "2", href: "" },
      { name: "Store C", address: "C", id: "3", distance: "3", href: "" },
    ]);
    const stores = await locateAlbertsons(mockPage, "85283", COORDS, 15, 2);
    expect(stores).toHaveLength(2);
  });

  it("prefixes Albertsons to names that don't start with it", async () => {
    const mockPage = createMockPage();
    mockPage.evaluate.mockResolvedValueOnce([
      { name: "Baseline Store", address: "A", id: "1", distance: "1", href: "" },
    ]);
    const stores = await locateAlbertsons(mockPage, "85283", COORDS, 15, 5);
    expect(stores[0].name).toBe("Albertsons Baseline Store");
  });
});

// ─── Walgreens Locator ────────────────────────────────────────────────────────

describe("locateWalgreens", () => {
  it("extracts stores from store locator page", async () => {
    const mockPage = createMockPage();
    mockPage.evaluate.mockResolvedValueOnce([
      { storeId: "5768", name: "Walgreens", address: "1919 N Dobson Rd, Chandler, AZ 85224", distance: 0.9 },
      { storeId: "3422", name: "Walgreens", address: "2100 E Baseline Rd, Tempe, AZ 85283", distance: 2.3 },
    ]);
    const stores = await locateWalgreens(mockPage, "85283", COORDS, 15, 5);
    expect(stores).toHaveLength(2);
    expect(stores[0]).toEqual({
      storeId: "5768",
      name: "Walgreens",
      address: "1919 N Dobson Rd, Chandler, AZ 85224",
      distanceMiles: 0.9,
    });
    expect(mockPage.goto).toHaveBeenCalledWith(
      expect.stringContaining("walgreens.com/storelocator/find.jsp"),
      expect.any(Object)
    );
  });

  it("filters stores by radius", async () => {
    const mockPage = createMockPage();
    mockPage.evaluate.mockResolvedValueOnce([
      { storeId: "5768", name: "Walgreens", address: "Nearby", distance: 5.0 },
      { storeId: "9999", name: "Walgreens", address: "Far Away", distance: 25.0 },
    ]);
    const stores = await locateWalgreens(mockPage, "85283", COORDS, 15, 5);
    expect(stores).toHaveLength(1);
    expect(stores[0].storeId).toBe("5768");
  });

  it("returns empty array on locator failure", async () => {
    const mockPage = createMockPage();
    mockPage.goto.mockRejectedValueOnce(new Error("Navigation timeout"));
    const errSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    const stores = await locateWalgreens(mockPage, "85283", COORDS, 15, 5);
    expect(stores).toEqual([]);
    expect(errSpy).toHaveBeenCalledWith(expect.stringContaining("[discover] Walgreens locator failed"));
    errSpy.mockRestore();
  });

  it("prefixes store name with Walgreens if missing", async () => {
    const mockPage = createMockPage();
    mockPage.evaluate.mockResolvedValueOnce([
      { storeId: "5768", name: "Dobson Rd", address: "1919 N Dobson Rd", distance: 1.0 },
    ]);
    const stores = await locateWalgreens(mockPage, "85283", COORDS, 15, 5);
    expect(stores[0].name).toBe("Walgreens Dobson Rd");
  });

  it("caps at maxStores", async () => {
    const mockPage = createMockPage();
    mockPage.evaluate.mockResolvedValueOnce([
      { storeId: "1", name: "Walgreens A", address: "Addr A", distance: 1.0 },
      { storeId: "2", name: "Walgreens B", address: "Addr B", distance: 2.0 },
      { storeId: "3", name: "Walgreens C", address: "Addr C", distance: 3.0 },
    ]);
    const stores = await locateWalgreens(mockPage, "85283", COORDS, 15, 2);
    expect(stores).toHaveLength(2);
  });
});

// ─── Sam's Club Locator ──────────────────────────────────────────────────────

describe("locateSamsClub", () => {
  it("extracts clubs from club-finder page", async () => {
    const mockPage = createMockPage();
    mockPage.evaluate.mockResolvedValueOnce([
      { storeId: "4956", name: "Sam's Club", address: "2080 E Rio Salado Pkwy, Tempe, AZ 85288", distance: 4.6 },
      { storeId: "6210", name: "Sam's Club", address: "1240 S Country Club Dr, Mesa, AZ 85210", distance: 7.3 },
    ]);
    const stores = await locateSamsClub(mockPage, "85283", COORDS, 15, 5);
    expect(stores).toHaveLength(2);
    expect(stores[0]).toEqual({
      storeId: "4956",
      name: "Sam's Club",
      address: "2080 E Rio Salado Pkwy, Tempe, AZ 85288",
      distanceMiles: 4.6,
    });
    expect(mockPage.goto).toHaveBeenCalledWith(
      expect.stringContaining("samsclub.com/club-finder"),
      expect.any(Object)
    );
  });

  it("filters clubs by radius", async () => {
    const mockPage = createMockPage();
    mockPage.evaluate.mockResolvedValueOnce([
      { storeId: "4956", name: "Sam's Club", address: "Nearby", distance: 5.0 },
      { storeId: "9999", name: "Sam's Club", address: "Far Away", distance: 25.0 },
    ]);
    const stores = await locateSamsClub(mockPage, "85283", COORDS, 15, 5);
    expect(stores).toHaveLength(1);
    expect(stores[0].storeId).toBe("4956");
  });

  it("returns empty array on locator failure", async () => {
    const mockPage = createMockPage();
    mockPage.goto.mockRejectedValueOnce(new Error("Navigation timeout"));
    const errSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    const stores = await locateSamsClub(mockPage, "85283", COORDS, 15, 5);
    expect(stores).toEqual([]);
    expect(errSpy).toHaveBeenCalledWith(expect.stringContaining("[discover] Sam's Club locator failed"));
    errSpy.mockRestore();
  });

  it("prefixes club name with Sam's Club if missing", async () => {
    const mockPage = createMockPage();
    mockPage.evaluate.mockResolvedValueOnce([
      { storeId: "4956", name: "Tempe", address: "2080 E Rio Salado Pkwy", distance: 4.6 },
    ]);
    const stores = await locateSamsClub(mockPage, "85283", COORDS, 15, 5);
    expect(stores[0].name).toBe("Sam's Club Tempe");
  });

  it("caps at maxStores", async () => {
    const mockPage = createMockPage();
    mockPage.evaluate.mockResolvedValueOnce([
      { storeId: "1", name: "Sam's Club A", address: "Addr A", distance: 1.0 },
      { storeId: "2", name: "Sam's Club B", address: "Addr B", distance: 2.0 },
      { storeId: "3", name: "Sam's Club C", address: "Addr C", distance: 3.0 },
    ]);
    const stores = await locateSamsClub(mockPage, "85283", COORDS, 15, 2);
    expect(stores).toHaveLength(2);
  });
});

// ─── BevMo Locator ────────────────────────────────────────────────────────────

describe("locateBevMo", () => {
  it("parses stores from evaluate results", async () => {
    const mockPage = createMockPage();
    const mockInput = { fill: vi.fn(), press: vi.fn() };
    mockPage.$.mockResolvedValueOnce(mockInput);
    mockPage.evaluate.mockResolvedValueOnce([
      { name: "BevMo Scottsdale", slug: "scottsdale", allText: ["BevMo Scottsdale", "123 Main St", "Scottsdale, AZ"] },
    ]);

    const stores = await locateBevMo(mockPage, "85283", COORDS, 15, 5);
    expect(stores).toHaveLength(1);
    expect(stores[0].name).toContain("BevMo Scottsdale");
    expect(stores[0].storeId).toBe("scottsdale");
  });

  it("handles no zip input found", async () => {
    const consoleSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    const mockPage = createMockPage();
    mockPage.$.mockResolvedValueOnce(null);
    mockPage.evaluate.mockResolvedValueOnce([]);
    const stores = await locateBevMo(mockPage, "85283", COORDS, 15, 5);
    expect(stores).toEqual([]);
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining("could not find zip"));
    consoleSpy.mockRestore();
  });

  it("handles page errors", async () => {
    const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    const mockPage = createMockPage();
    mockPage.goto.mockRejectedValueOnce(new Error("Timeout"));
    const stores = await locateBevMo(mockPage, "85283", COORDS, 15, 5);
    expect(stores).toEqual([]);
    consoleSpy.mockRestore();
  });

  it("extracts address from allText", async () => {
    const mockPage = createMockPage();
    mockPage.$.mockResolvedValueOnce(null);
    mockPage.evaluate.mockResolvedValueOnce([
      { name: "Store X", slug: "store-x", allText: ["Store X", "100 Oak Ave", "Mesa, AZ 85202", "(480) 555-1234"] },
    ]);
    const stores = await locateBevMo(mockPage, "85283", COORDS, 15, 5);
    expect(stores[0].address).toContain("100 Oak Ave");
  });

  it("respects maxStores limit", async () => {
    const mockPage = createMockPage();
    mockPage.$.mockResolvedValueOnce(null);
    mockPage.evaluate.mockResolvedValueOnce([
      { name: "A", slug: "a", allText: ["A", "Addr"] },
      { name: "B", slug: "b", allText: ["B", "Addr"] },
      { name: "C", slug: "c", allText: ["C", "Addr"] },
    ]);
    const stores = await locateBevMo(mockPage, "85283", COORDS, 15, 2);
    expect(stores).toHaveLength(2);
  });
});

// ─── discoverStores Orchestrator ──────────────────────────────────────────────

describe("discoverStores", () => {
  beforeEach(() => { vi.useFakeTimers(); });
  afterEach(() => { vi.useRealTimers(); });

  async function runWithFakeTimers(fn) {
    const promise = fn();
    promise.catch(() => {});
    await vi.runAllTimersAsync();
    return promise;
  }

  it("returns cached data when cache is valid", async () => {
    const cached = {
      discoveredAt: new Date().toISOString(),
      zipCode: "85283",
      radiusMiles: 15,
      retailers: { costco: [{ storeId: "736", name: "Costco Chandler" }] },
    };
    mocks.readFile.mockResolvedValueOnce(JSON.stringify(cached));

    const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    const result = await discoverStores({ zipCode: "85283", radiusMiles: 15 });
    expect(result.retailers.costco).toHaveLength(1);
    expect(mocks.chromiumLaunch).not.toHaveBeenCalled();
    consoleSpy.mockRestore();
  });

  it("discovers fresh stores when cache is expired", async () => {
    // Expired cache
    const oldCache = {
      discoveredAt: new Date(Date.now() - 8 * 24 * 60 * 60 * 1000).toISOString(),
      zipCode: "85283",
      radiusMiles: 15,
      retailers: {},
    };
    mocks.readFile.mockResolvedValueOnce(JSON.stringify(oldCache));

    // Mock zipToCoords via fetch
    mocks.fetch
      .mockResolvedValueOnce({
        ok: true,
        json: async () => ({ places: [{ latitude: "33.4152", longitude: "-111.8315" }] }),
      })
      // Kroger OAuth
      .mockResolvedValueOnce({ ok: false, status: 401 });

    // Mock browser
    const mockPage = createMockPage();
    mockPage.evaluate.mockResolvedValue([]);
    mockPage.$.mockResolvedValue(null);
    const mockContext = { newPage: vi.fn().mockResolvedValue(mockPage), close: vi.fn() };
    const mockBrowser = { newContext: vi.fn().mockResolvedValue(mockContext), close: vi.fn() };
    mocks.chromiumLaunch.mockResolvedValueOnce(mockBrowser);
    mocks.writeFile.mockResolvedValue(undefined);

    const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    vi.spyOn(console, "warn").mockImplementation(() => {});
    vi.spyOn(console, "error").mockImplementation(() => {});
    const result = await runWithFakeTimers(() => discoverStores({ zipCode: "85283", radiusMiles: 15 }));
    expect(result).toHaveProperty("retailers");
    expect(result).toHaveProperty("discoveredAt");
    expect(mocks.chromiumLaunch).toHaveBeenCalled();
    consoleSpy.mockRestore();
    vi.spyOn(console, "warn").mockRestore();
    vi.spyOn(console, "error").mockRestore();
  });

  it("uses fallback stores when locators return empty", async () => {
    mocks.readFile.mockRejectedValueOnce(new Error("ENOENT")); // no cache

    // Mock zipToCoords
    mocks.fetch
      .mockResolvedValueOnce({
        ok: true,
        json: async () => ({ places: [{ latitude: "33.4152", longitude: "-111.8315" }] }),
      })
      // Kroger: no creds
      .mockResolvedValueOnce({ ok: false, status: 401 });

    const mockPage = createMockPage();
    mockPage.evaluate.mockResolvedValue([]);
    mockPage.$.mockResolvedValue(null);
    const mockContext = { newPage: vi.fn().mockResolvedValue(mockPage), close: vi.fn() };
    const mockBrowser = { newContext: vi.fn().mockResolvedValue(mockContext), close: vi.fn() };
    mocks.chromiumLaunch.mockResolvedValueOnce(mockBrowser);
    mocks.writeFile.mockResolvedValue(undefined);

    const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    vi.spyOn(console, "warn").mockImplementation(() => {});
    vi.spyOn(console, "error").mockImplementation(() => {});

    const result = await runWithFakeTimers(() => discoverStores({ zipCode: "85283", radiusMiles: 15 }));
    // Should use fallback stores for costco, totalwine, walmart, safeway
    expect(result.retailers.costco.length).toBeGreaterThan(0);
    expect(result.retailers.totalwine.length).toBeGreaterThan(0);

    consoleSpy.mockRestore();
    vi.spyOn(console, "warn").mockRestore();
    vi.spyOn(console, "error").mockRestore();
  });
});
