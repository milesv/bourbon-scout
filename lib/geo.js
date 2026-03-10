import fetch from "node-fetch";

/**
 * Convert a US zip code to lat/lng using the free zippopotam.us API.
 * Returns { lat, lng } or throws on invalid/missing zip.
 */
export async function zipToCoords(zip) {
  const res = await fetch(`https://api.zippopotam.us/us/${zip}`, {
    signal: AbortSignal.timeout(10000),
  });
  if (!res.ok) throw new Error(`Invalid zip code: ${zip} (HTTP ${res.status})`);
  const data = await res.json();
  const place = data.places?.[0];
  if (!place) throw new Error(`No location data for zip: ${zip}`);
  return { lat: parseFloat(place.latitude), lng: parseFloat(place.longitude) };
}

/**
 * Haversine formula — returns distance in miles between two lat/lng points.
 */
export function haversine(lat1, lng1, lat2, lng2) {
  const R = 3958.8; // Earth radius in miles
  const toRad = (deg) => (deg * Math.PI) / 180;
  const dLat = toRad(lat2 - lat1);
  const dLng = toRad(lng2 - lng1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLng / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}
