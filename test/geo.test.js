import { describe, it, expect, vi, beforeEach } from "vitest";

// Mock node-fetch before importing geo
vi.mock("node-fetch", () => ({
  default: vi.fn(),
}));

import fetch from "node-fetch";
import { zipToCoords, haversine } from "../lib/geo.js";

describe("haversine", () => {
  it("returns 0 for identical points", () => {
    expect(haversine(33.4, -111.9, 33.4, -111.9)).toBe(0);
  });

  it("calculates distance between Tempe and Phoenix (~10 mi)", () => {
    const dist = haversine(33.4255, -111.9400, 33.4484, -112.0740);
    expect(dist).toBeGreaterThan(7);
    expect(dist).toBeLessThan(12);
  });

  it("calculates distance between LA and NYC (~2450 mi)", () => {
    const dist = haversine(34.0522, -118.2437, 40.7128, -74.0060);
    expect(dist).toBeGreaterThan(2400);
    expect(dist).toBeLessThan(2500);
  });

  it("handles cross-hemisphere points", () => {
    const dist = haversine(40.0, -74.0, -33.9, 151.2); // NYC to Sydney
    expect(dist).toBeGreaterThan(9000);
    expect(dist).toBeLessThan(10500);
  });

  it("handles zero latitude/longitude", () => {
    const dist = haversine(0, 0, 0, 1);
    expect(dist).toBeGreaterThan(68);
    expect(dist).toBeLessThan(70);
  });
});

describe("zipToCoords", () => {
  beforeEach(() => {
    vi.resetAllMocks();
  });

  it("returns lat/lng for a valid zip code", async () => {
    fetch.mockResolvedValueOnce({
      ok: true,
      json: async () => ({
        places: [{ latitude: "33.4152", longitude: "-111.8315" }],
      }),
    });

    const result = await zipToCoords("85283");
    expect(result).toEqual({ lat: 33.4152, lng: -111.8315 });
    expect(fetch).toHaveBeenCalledWith("https://api.zippopotam.us/us/85283");
  });

  it("throws on invalid zip (HTTP error)", async () => {
    fetch.mockResolvedValueOnce({ ok: false, status: 404 });
    await expect(zipToCoords("00000")).rejects.toThrow("Invalid zip code: 00000 (HTTP 404)");
  });

  it("throws when no places in response", async () => {
    fetch.mockResolvedValueOnce({
      ok: true,
      json: async () => ({ places: [] }),
    });
    await expect(zipToCoords("99999")).rejects.toThrow("No location data for zip: 99999");
  });

  it("throws when places is undefined", async () => {
    fetch.mockResolvedValueOnce({
      ok: true,
      json: async () => ({}),
    });
    await expect(zipToCoords("99999")).rejects.toThrow("No location data for zip: 99999");
  });
});
