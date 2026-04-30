#!/usr/bin/env node
// Multi-retailer unified CLI search across state.json. Lets you query current
// inventory without grepping JSON manually.
//
// Usage:
//   node scripts/find.js pappy           # find all Pappys currently in stock
//   node scripts/find.js stagg --hot     # only confirmed/double-confirmed finds
//   node scripts/find.js --retailer=kroger --price-max=200
//   node scripts/find.js --confirmed     # all 24h+ confirmed bottles

import { readFile } from "node:fs/promises";

const args = Object.fromEntries(
  process.argv.slice(2).filter((a) => a.startsWith("--")).map((a) => {
    const [k, v] = a.replace(/^--/, "").split("=");
    return [k, v ?? true];
  })
);
const QUERY = process.argv.slice(2).filter((a) => !a.startsWith("--")).join(" ").toLowerCase();
const FILTER_RETAILER = args.retailer || null;
const PRICE_MAX = args["price-max"] ? parseFloat(args["price-max"]) : null;
const HOT_ONLY = !!args.hot;          // only confirmed/double-confirmed finds
const CONFIRMED_ONLY = !!args.confirmed;  // only 24h-confirmed bottles

async function main() {
  let state;
  try {
    state = JSON.parse(await readFile(new URL("../state.json", import.meta.url), "utf-8"));
  } catch (err) {
    console.error("Could not read state.json:", err.message);
    process.exit(1);
  }

  let stores;
  try {
    stores = JSON.parse(await readFile(new URL("../stores.json", import.meta.url), "utf-8"));
  } catch { stores = { retailers: {} }; }

  const results = [];
  for (const [retailer, byStore] of Object.entries(state)) {
    if (FILTER_RETAILER && retailer !== FILTER_RETAILER) continue;
    if (retailer.startsWith("_")) continue; // skip _watchList, _redditSeen, _redditMentions
    for (const [storeId, data] of Object.entries(byStore)) {
      const storeMeta = (stores.retailers?.[retailer] || []).find((s) => s.storeId === storeId) || {};
      for (const [name, info] of Object.entries(data.bottles || {})) {
        if (QUERY && !name.toLowerCase().includes(QUERY)) continue;
        if (HOT_ONLY && !info.confirmedAt && info.confidence !== "confirmed" && !info.htmlVerified) continue;
        if (CONFIRMED_ONLY && !info.confirmedAt) continue;
        if (PRICE_MAX) {
          const p = parseFloat((info.price || "").replace(/[^0-9.]/g, ""));
          if (!isFinite(p) || p > PRICE_MAX) continue;
        }
        results.push({
          retailer,
          storeName: storeMeta.name || storeId,
          storeId,
          name,
          price: info.price,
          aisle: info.aisle,
          facings: info.facings,
          confidence: info.confidence,
          confirmedAt: info.confirmedAt,
          firstSeen: info.firstSeen,
          lastSeen: info.lastSeen,
          url: info.url,
        });
      }
    }
  }

  if (results.length === 0) {
    console.log("\n(no matches)");
    return;
  }

  // Group by bottle for readability
  const byBottle = {};
  for (const r of results) {
    if (!byBottle[r.name]) byBottle[r.name] = [];
    byBottle[r.name].push(r);
  }

  console.log(`\n${results.length} match${results.length === 1 ? "" : "es"} across ${Object.keys(byBottle).length} bottles`);
  console.log("─".repeat(80));

  for (const [bottle, hits] of Object.entries(byBottle).sort((a, b) => b[1].length - a[1].length)) {
    const tier = hits[0].confidence === "lead" ? "🟡 LEAD"
              : hits[0].confirmedAt ? "🔵 CONFIRMED"
              : "🟢 FOUND";
    console.log(`\n${tier}  ${bottle}  (${hits.length} store${hits.length === 1 ? "" : "s"})`);
    for (const h of hits.sort((a, b) => (a.lastSeen || "").localeCompare(b.lastSeen || ""))) {
      const ageMs = Date.now() - new Date(h.lastSeen).getTime();
      const ageHours = Math.floor(ageMs / 3600000);
      const ageMins = Math.floor((ageMs % 3600000) / 60000);
      const ageStr = ageHours > 0 ? `${ageHours}h${ageMins}m ago` : `${ageMins}m ago`;
      const aisle = h.aisle ? ` · ${h.aisle}` : "";
      const facings = h.facings ? ` · ${h.facings}f` : "";
      const conf = h.confidence === "lead" ? " (lead)" : "";
      console.log(`  ${h.retailer.padEnd(11)} ${(h.price || "?").padStart(8)}  ${h.storeName.slice(0, 40).padEnd(40)} ${ageStr}${aisle}${facings}${conf}`);
      if (h.url) console.log(`              ${h.url}`);
    }
  }
  console.log();
}

main().catch((err) => { console.error("Fatal:", err); process.exit(1); });
