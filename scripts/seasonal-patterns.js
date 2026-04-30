#!/usr/bin/env node
// Seasonal pattern detection: mines metrics.jsonl across months/years to surface
// drop windows for specific bottles. BTAC drops in October. KoK drops periodically.
// Auto-detects from history and predicts next drop window per bottle.
//
// Usage: node scripts/seasonal-patterns.js [--bottle="George T. Stagg"] [--days=365]

import { readFile } from "node:fs/promises";

const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
const args = Object.fromEntries(
  process.argv.slice(2).map((a) => {
    const [k, v] = a.replace(/^--/, "").split("=");
    return [k, v ?? true];
  })
);
const FILTER_BOTTLE = args.bottle || null;
const DAYS = parseInt(args.days || "365", 10);

async function main() {
  let raw;
  try {
    raw = await readFile(new URL("../metrics.jsonl", import.meta.url), "utf-8");
  } catch (err) {
    console.error("Could not read metrics.jsonl:", err.message);
    process.exit(1);
  }

  const cutoff = Date.now() - DAYS * 86400 * 1000;
  const scans = raw.trim().split("\n")
    .map((l) => { try { return JSON.parse(l); } catch { return null; } })
    .filter((s) => s && new Date(s.ts).getTime() >= cutoff);

  console.log(`\nAnalyzing ${scans.length} scans across ${DAYS} days`);
  if (FILTER_BOTTLE) console.log(`Filtered to bottle: ${FILTER_BOTTLE}`);
  console.log("─".repeat(70));

  // Aggregate finds by bottle × month
  const bottleMonth = {}; // { "Pappy 23": { "Oct": 12, "Nov": 3, ... } }
  const monthScans = {};  // { "Oct": 800 } — total scans per month for normalization

  for (const s of scans) {
    const month = MONTHS[new Date(s.ts).getMonth()];
    monthScans[month] = (monthScans[month] || 0) + 1;
    for (const [, d] of Object.entries(s.retailers || {})) {
      for (const f of d.found || []) {
        if (FILTER_BOTTLE && f !== FILTER_BOTTLE) continue;
        if (!bottleMonth[f]) bottleMonth[f] = {};
        bottleMonth[f][month] = (bottleMonth[f][month] || 0) + 1;
      }
    }
  }

  if (Object.keys(bottleMonth).length === 0) {
    console.log("\n  (no allocated finds in window)");
    return;
  }

  // Print per-bottle monthly hit rate as ASCII heatmap
  console.log("\n📅 BOTTLE × MONTH HEATMAP (hit rate %)");
  console.log("Bottle".padEnd(38) + MONTHS.map(m => m.padStart(5)).join(""));
  console.log("─".repeat(98));

  const sortedBottles = Object.entries(bottleMonth)
    .map(([n, m]) => ({ name: n, total: Object.values(m).reduce((a, b) => a + b, 0), months: m }))
    .sort((a, b) => b.total - a.total);

  for (const { name, months, total } of sortedBottles.slice(0, 25)) {
    const cells = MONTHS.map((m) => {
      const finds = months[m] || 0;
      const totalForMonth = monthScans[m] || 0;
      const rate = totalForMonth > 0 ? finds / totalForMonth : 0;
      return rate === 0 ? "    .".padStart(5) : `${(rate * 100).toFixed(0).padStart(3)}%`.padStart(5);
    });
    console.log(name.slice(0, 36).padEnd(38) + cells.join(""));
  }

  // ─── Drop-window predictions ──────────────────────────────────────────────
  console.log("\n🎯 PEAK DROP MONTH(S) PER BOTTLE");
  for (const { name, months } of sortedBottles.slice(0, 15)) {
    const ranked = Object.entries(months).sort((a, b) => b[1] - a[1]);
    if (ranked.length === 0) continue;
    const top = ranked.slice(0, 3).map(([m, n]) => `${m} (${n} finds)`).join(", ");
    console.log(`  ${name}: ${top}`);
  }

  // ─── Known industry releases (manual reference) ──────────────────────────
  console.log("\n💡 KNOWN INDUSTRY DROP WINDOWS (industry knowledge):");
  console.log("  BTAC          → typically late Sep / Oct");
  console.log("  Pappy lottery → Mar-May (some retailers); main release Oct-Dec");
  console.log("  KoK           → quarterly, irregular");
  console.log("  EHT specials  → spring (Cured Oak), summer (Seasoned Wood)");
  console.log("  OF Birthday   → Sep/Oct");

  // ─── Boost-window recommendation ──────────────────────────────────────────
  console.log("\n🔔 BOOST-WINDOW SUGGESTIONS");
  const currentMonth = MONTHS[new Date().getMonth()];
  const candidates = sortedBottles
    .filter(b => (b.months[currentMonth] || 0) > 0)
    .slice(0, 5);
  if (candidates.length > 0) {
    console.log(`  Active drops THIS MONTH (${currentMonth}):`);
    for (const c of candidates) console.log(`    - ${c.name} (${c.months[currentMonth]} finds in ${currentMonth})`);
  } else {
    console.log(`  No bottles historically hit in ${currentMonth} (within ${DAYS}d data).`);
  }
  const nextMonth = MONTHS[(new Date().getMonth() + 1) % 12];
  const nextCandidates = sortedBottles
    .filter(b => (b.months[nextMonth] || 0) > 0)
    .slice(0, 5);
  if (nextCandidates.length > 0) {
    console.log(`\n  EXPECTED NEXT MONTH (${nextMonth}):`);
    for (const c of nextCandidates) console.log(`    - ${c.name} (${c.months[nextMonth]} finds historically)`);
  }
  console.log();
}

main().catch((err) => { console.error("Fatal:", err); process.exit(1); });
