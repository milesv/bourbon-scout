import { describe, it, expect } from "vitest";
import { FALLBACK_STORES, EXTRA_STORES } from "../lib/fallback-stores.js";

describe("FALLBACK_STORES", () => {
  const EXPECTED_RETAILERS = ["costco", "totalwine", "walmart", "safeway", "albertsons", "walgreens", "samsclub", "bevmo", "kroger"];

  it("contains all expected retailer keys", () => {
    for (const key of EXPECTED_RETAILERS) {
      expect(FALLBACK_STORES).toHaveProperty(key);
      expect(Array.isArray(FALLBACK_STORES[key])).toBe(true);
    }
  });

  it("has stores for costco, totalwine, walmart, safeway, albertsons, walgreens, samsclub", () => {
    expect(FALLBACK_STORES.costco.length).toBeGreaterThan(0);
    expect(FALLBACK_STORES.totalwine.length).toBeGreaterThan(0);
    expect(FALLBACK_STORES.walmart.length).toBeGreaterThan(0);
    expect(FALLBACK_STORES.safeway.length).toBeGreaterThan(0);
    expect(FALLBACK_STORES.albertsons.length).toBeGreaterThan(0);
    expect(FALLBACK_STORES.walgreens.length).toBeGreaterThan(0);
    expect(FALLBACK_STORES.samsclub.length).toBeGreaterThan(0);
  });

  it("has empty arrays for bevmo and kroger (no AZ stores / API-only)", () => {
    expect(FALLBACK_STORES.bevmo).toEqual([]);
    expect(FALLBACK_STORES.kroger).toEqual([]);
  });

  it("each store has required fields", () => {
    for (const [retailer, stores] of Object.entries(FALLBACK_STORES)) {
      for (const store of stores) {
        expect(store).toHaveProperty("storeId");
        expect(store).toHaveProperty("name");
        expect(store).toHaveProperty("address");
        expect(store).toHaveProperty("distanceMiles");
        expect(typeof store.storeId).toBe("string");
        expect(typeof store.name).toBe("string");
        expect(typeof store.address).toBe("string");
        expect(typeof store.distanceMiles).toBe("number");
      }
    }
  });

  it("store IDs are unique within each retailer", () => {
    for (const [retailer, stores] of Object.entries(FALLBACK_STORES)) {
      const ids = stores.map((s) => s.storeId);
      expect(new Set(ids).size).toBe(ids.length);
    }
  });

  it("all addresses are in Arizona", () => {
    for (const stores of Object.values(FALLBACK_STORES)) {
      for (const store of stores) {
        if (store.address) {
          expect(store.address).toContain("AZ");
        }
      }
    }
  });
});

describe("EXTRA_STORES", () => {
  it("each store has required fields", () => {
    for (const [retailer, stores] of Object.entries(EXTRA_STORES)) {
      for (const store of stores) {
        expect(store).toHaveProperty("storeId");
        expect(store).toHaveProperty("name");
        expect(store).toHaveProperty("address");
        expect(typeof store.storeId).toBe("string");
        expect(typeof store.name).toBe("string");
        expect(typeof store.address).toBe("string");
      }
    }
  });

  it("store IDs do not overlap with FALLBACK_STORES", () => {
    for (const [retailer, extras] of Object.entries(EXTRA_STORES)) {
      const fallbackIds = new Set((FALLBACK_STORES[retailer] || []).map((s) => s.storeId));
      for (const store of extras) {
        expect(fallbackIds.has(store.storeId), `${store.name} (#${store.storeId}) duplicates a fallback store`).toBe(false);
      }
    }
  });

  it("contains Costco Scottsdale and Paradise Valley", () => {
    const ids = EXTRA_STORES.costco.map((s) => s.storeId);
    expect(ids).toContain("427");
    expect(ids).toContain("1058");
  });
});
