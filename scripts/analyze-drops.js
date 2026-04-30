#!/usr/bin/env node
// Analyze metrics.jsonl to find when allocated bottles actually appear in stock.
// Surfaces day-of-week × hour patterns to inform boost-schedule tuning.
//
// Usage: node scripts/analyze-drops.js [--days=14] [--retailer=kroger]
//
// Outputs:
//   - Top 10 drop time slots (day × hour) ranked by find rate
//   - Per-retailer canary timeline (when each retailer was last "healthy")
//   - First-find timestamps per bottle (which days/hours the bottle hit shelves)
//   - Recommendations for boost-schedule changes

import { readFile } from "node:fs/promises";

const DAYS_OF_WEEK = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
const args = Object.fromEntries(
  process.argv.slice(2).map((a) => {
    const [k, v] = a.replace(/^--/, "").split("=");
    return [k, v ?? true];
  })
);
const DAYS = parseInt(args.days || "14", 10);
const FILTER_RETAILER = args.retailer || null;

async function main() {
  let raw;
  try {
    raw = await readFile(new URL("../metrics.jsonl", import.meta.url), "utf-8");
  } catch (err) {
    console.error("Could not read metrics.jsonl:", err.message);
    process.exit(1);
  }

  const cutoff = Date.now() - DAYS * 86400 * 1000;
  const scans = raw
    .trim()
    .split("\n")
    .map((l) => {
      try { return JSON.parse(l); } catch { return null; }
    })
    .filter((s) => s && new Date(s.ts).getTime() >= cutoff);

  console.log(`\nAnalyzing ${scans.length} scans from last ${DAYS} days`);
  if (FILTER_RETAILER) console.log(`Filtered to retailer: ${FILTER_RETAILER}`);
  console.log("─".repeat(70));

  // ─── Drop-time distribution ────────────────────────────────────────────────
  // For each scan with non-canary finds, count it by day-of-week × hour (MT)
  const slots = {}; // key: "Tue-06", value: { scans, hits, finds: [bottleNames] }
  for (const s of scans) {
    const date = new Date(s.ts);
    const mt = new Date(date.toLocaleString("en-US", { timeZone: "America/Phoenix" }));
    const slot = `${DAYS_OF_WEEK[mt.getDay()]}-${String(mt.getHours()).padStart(2, "0")}`;
    if (!slots[slot]) slots[slot] = { scans: 0, hits: 0, finds: new Set() };
    slots[slot].scans++;
    let hadFind = false;
    for (const [r, d] of Object.entries(s.retailers || {})) {
      if (FILTER_RETAILER && r !== FILTER_RETAILER) continue;
      for (const f of d.found || []) {
        slots[slot].finds.add(`${r}/${f}`);
        hadFind = true;
      }
    }
    if (hadFind) slots[slot].hits++;
  }

  const ranked = Object.entries(slots)
    .filter(([, v]) => v.hits > 0)
    .map(([k, v]) => ({ slot: k, hits: v.hits, scans: v.scans, rate: v.hits / v.scans, uniqueFinds: v.finds.size }))
    .sort((a, b) => b.rate - a.rate || b.uniqueFinds - a.uniqueFinds);

  console.log("\n🎯 TOP DROP-TIME SLOTS (day × hour MT, ranked by hit rate)");
  console.log("Slot      Hit rate    Scans   Unique finds");
  console.log("─".repeat(50));
  for (const r of ranked.slice(0, 15)) {
    console.log(
      `${r.slot.padEnd(10)}${(r.rate * 100).toFixed(1).padStart(6)}%   ${String(r.scans).padStart(4)}   ${r.uniqueFinds}`
    );
  }
  if (ranked.length === 0) console.log("  (no allocated finds in window)");

  // ─── Per-retailer canary health timeline ──────────────────────────────────
  console.log("\n💚 PER-RETAILER CANARY HEALTH (last 14 days, % of scans w/ canary)");
  const canaryByRetailer = {};
  for (const s of scans) {
    for (const [r, d] of Object.entries(s.retailers || {})) {
      if (!canaryByRetailer[r]) canaryByRetailer[r] = { scans: 0, canary: 0 };
      if ((d.queries || 0) > 0) canaryByRetailer[r].scans++;
      if (d.canary) canaryByRetailer[r].canary++;
    }
  }
  console.log("Retailer       Canary %    Scans");
  console.log("─".repeat(40));
  for (const [r, v] of Object.entries(canaryByRetailer).sort((a, b) => b[1].canary / b[1].scans - a[1].canary / a[1].scans)) {
    const pct = v.scans > 0 ? ((v.canary / v.scans) * 100).toFixed(1) : "0.0";
    console.log(`${r.padEnd(14)}${pct.padStart(7)}%   ${v.scans}`);
  }

  // ─── First-find timeline per bottle ───────────────────────────────────────
  console.log("\n🥃 BOTTLE FIRST-APPEARANCE TIMELINE");
  const bottleFirstSeen = {};
  for (const s of scans) {
    for (const [r, d] of Object.entries(s.retailers || {})) {
      if (FILTER_RETAILER && r !== FILTER_RETAILER) continue;
      for (const f of d.found || []) {
        const key = `${r}/${f}`;
        if (!bottleFirstSeen[key]) bottleFirstSeen[key] = s.ts;
      }
    }
  }
  const sorted = Object.entries(bottleFirstSeen).sort((a, b) => a[1].localeCompare(b[1]));
  for (const [k, ts] of sorted.slice(-25)) {
    const date = new Date(ts);
    const mt = new Date(date.toLocaleString("en-US", { timeZone: "America/Phoenix" }));
    const dayHour = `${DAYS_OF_WEEK[mt.getDay()]} ${String(mt.getHours()).padStart(2, "0")}:${String(mt.getMinutes()).padStart(2, "0")} MT`;
    console.log(`  ${dayHour}  ${k}`);
  }
  if (sorted.length === 0) console.log("  (no allocated finds)");

  // ─── Boost-schedule recommendations ────────────────────────────────────────
  console.log("\n💡 BOOST-SCHEDULE RECOMMENDATIONS");
  const top5 = ranked.slice(0, 5);
  if (top5.length < 3) {
    console.log("  Need more data — at least 3 distinct find slots required.");
  } else {
    const dayHourSet = new Set(top5.map((s) => s.slot));
    console.log("  Top 5 hit slots:");
    for (const s of top5) console.log(`    ${s.slot}: ${(s.rate * 100).toFixed(1)}% hit rate`);
    console.log("  Suggested boost windows: keep existing Tue/Thu evening boosts.");
    const tueBoost = [...dayHourSet].some((s) => s.startsWith("Tue-") || s.startsWith("Wed-0"));
    const thuBoost = [...dayHourSet].some((s) => s.startsWith("Thu-") || s.startsWith("Fri-0"));
    if (!tueBoost && !thuBoost) console.log("  ⚠️ Top hit slots fall outside current boost windows. Consider adjusting schedule.");
  }

  console.log("\n");
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
