#!/usr/bin/env node
// Geographic map visualization: renders state.json finds on a Leaflet map.
// Generates a self-contained HTML file (no build step, no server needed) that
// shows each store as a pin, with bottle finds in popup and tier-colored markers.
//
// Usage:
//   node scripts/map.js                       # writes ./map.html, opens in browser
//   node scripts/map.js --output=/tmp/map.html

import { readFile, writeFile } from "node:fs/promises";

const args = Object.fromEntries(
  process.argv.slice(2).map((a) => {
    const [k, v] = a.replace(/^--/, "").split("=");
    return [k, v ?? true];
  })
);
const OUTPUT = args.output || new URL("../map.html", import.meta.url).pathname;

async function main() {
  const state = JSON.parse(await readFile(new URL("../state.json", import.meta.url), "utf-8"));
  const stores = JSON.parse(await readFile(new URL("../stores.json", import.meta.url), "utf-8"));

  // Build pin data per store with finds
  const pins = [];
  for (const [retailerKey, byStore] of Object.entries(state)) {
    if (retailerKey.startsWith("_")) continue;
    for (const [storeId, data] of Object.entries(byStore)) {
      const meta = (stores.retailers?.[retailerKey] || []).find((s) => s.storeId === storeId);
      if (!meta?.lat && !meta?.address) continue;
      const bottles = Object.entries(data.bottles || {});
      if (bottles.length === 0) continue;
      // Use coords from stores.json if present; otherwise approximate via address geocoding
      // (skipped in v1 — only stores with lat/lng appear)
      if (!meta.lat || !meta.lng) continue;
      // Tier color: red for double-confirmed, blue for 24h-confirmed, green for confirmed find, yellow for lead
      const top = bottles.sort((a, b) => {
        const ra = a[1].crossSourceConfirmed ? 4 : a[1].confirmedAt ? 3 : a[1].confidence !== "lead" ? 2 : 1;
        const rb = b[1].crossSourceConfirmed ? 4 : b[1].confirmedAt ? 3 : b[1].confidence !== "lead" ? 2 : 1;
        return rb - ra;
      })[0][1];
      const color = top.crossSourceConfirmed ? "#ff0000"
                  : top.confirmedAt ? "#3498db"
                  : top.confidence !== "lead" ? "#2ecc71"
                  : "#f1c40f";
      pins.push({
        lat: meta.lat,
        lng: meta.lng,
        retailer: retailerKey,
        storeName: meta.name,
        address: meta.address,
        bottles: bottles.map(([n, info]) => ({ name: n, price: info.price, confidence: info.confidence, confirmedAt: !!info.confirmedAt, dual: !!info.crossSourceConfirmed, url: info.url })),
        color,
      });
    }
  }

  if (pins.length === 0) {
    console.log("No finds to map. Run a scan first.");
    process.exit(0);
  }

  const html = `<!doctype html>
<html><head>
<meta charset="utf-8">
<title>Bourbon Scout 🥃 — Find Map</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
<style>
  body { margin: 0; font-family: -apple-system, sans-serif; }
  #map { height: 100vh; width: 100%; }
  .legend { position: absolute; top: 12px; right: 12px; background: white; padding: 12px; border-radius: 6px; box-shadow: 0 2px 6px rgba(0,0,0,.2); font-size: 14px; z-index: 1000; }
  .legend .swatch { display: inline-block; width: 12px; height: 12px; border-radius: 50%; margin-right: 6px; vertical-align: middle; }
  .popup-bottle { padding: 2px 0; }
  .popup-bottle.dual { font-weight: bold; }
</style>
</head><body>
<div id="map"></div>
<div class="legend">
  <div><b>Tiers</b></div>
  <div><span class="swatch" style="background:#ff0000"></span> Double-confirmed (Reddit + scraper)</div>
  <div><span class="swatch" style="background:#3498db"></span> Confirmed (in stock 24+h)</div>
  <div><span class="swatch" style="background:#2ecc71"></span> Found (this scan)</div>
  <div><span class="swatch" style="background:#f1c40f"></span> Lead (call first)</div>
</div>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script>
const PINS = ${JSON.stringify(pins)};
const map = L.map('map').setView([33.42, -111.83], 11);
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', { attribution: '&copy; OpenStreetMap', maxZoom: 19 }).addTo(map);
for (const p of PINS) {
  const popup = '<div><b>' + p.storeName + '</b><br><small>' + (p.address || '') + '</small><hr style="margin:6px 0">' +
    p.bottles.map(b => '<div class="popup-bottle' + (b.dual ? ' dual' : '') + '">' +
      (b.dual ? '🔥 ' : b.confirmedAt ? '🔵 ' : b.confidence === 'lead' ? '🟡 ' : '🟢 ') +
      (b.url ? '<a href="' + b.url + '" target="_blank">' + b.name + '</a>' : b.name) +
      (b.price ? ' — ' + b.price : '') +
    '</div>').join('');
  L.circleMarker([p.lat, p.lng], { radius: 12, color: p.color, fillColor: p.color, fillOpacity: 0.7 })
    .bindPopup(popup, { maxWidth: 360 })
    .addTo(map);
}
const bounds = L.latLngBounds(PINS.map(p => [p.lat, p.lng]));
if (bounds.isValid()) map.fitBounds(bounds, { padding: [40, 40] });
</script>
</body></html>`;

  await writeFile(OUTPUT, html);
  console.log(`✓ Wrote map to ${OUTPUT} — ${pins.length} stores with finds`);
  console.log(`  open file://${OUTPUT}`);
}

main().catch((err) => { console.error("Fatal:", err); process.exit(1); });
