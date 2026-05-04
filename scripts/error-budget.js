#!/usr/bin/env node
// Per-retailer error budget analysis from metrics.jsonl.
//
// Goal: spot retailers whose 24h failure rate is materially worse than their
// 7-day baseline. The existing health-degradation Discord alert catches *acute*
// failure (4 scans in a row missing canary). This catches *trends* — slow
// degradation, IP reputation burn, or cookie-expiry cycles you'd otherwise
// only notice after weeks of silent badness.
//
// Usage: node scripts/error-budget.js [--days=7]
//
// Outputs:
//   Per-retailer table with: 24h success%, 7d success%, delta, error budget
//   used. Retailers burning their budget faster than baseline get flagged.

import { readFile } from "node:fs/promises";

const args = Object.fromEntries(
  process.argv.slice(2).map((a) => {
    const [k, v] = a.replace(/^--/, "").split("=");
    return [k, v ?? true];
  })
);
const BASELINE_DAYS = parseInt(args.days || "7", 10);
const RECENT_HOURS = 24;

// Failure threshold for "burning budget" — recent error rate ≥ baseline + this delta
// flags as concerning. 15 percentage-point degradation is the watershed.
const DEGRADATION_THRESHOLD_PP = 15;

async function main() {
  let raw;
  try {
    raw = await readFile(new URL("../metrics.jsonl", import.meta.url), "utf-8");
  } catch (err) {
    console.error("Could not read metrics.jsonl:", err.message);
    process.exit(1);
  }

  const baselineCutoff = Date.now() - BASELINE_DAYS * 86400 * 1000;
  const recentCutoff = Date.now() - RECENT_HOURS * 3600 * 1000;
  const scans = raw
    .trim()
    .split("\n")
    .map((l) => {
      try { return JSON.parse(l); } catch { return null; }
    })
    .filter((s) => s && new Date(s.ts).getTime() >= baselineCutoff);

  console.log(`\nError budget analysis — last ${BASELINE_DAYS} days (${scans.length} scans)`);
  console.log(`Recent window: last ${RECENT_HOURS}h`);
  console.log("─".repeat(80));

  // Aggregate per-retailer counters for baseline + recent windows
  const stats = {}; // key: retailer, value: { baseline: {ok, fail}, recent: {ok, fail} }
  for (const scan of scans) {
    const t = new Date(scan.ts).getTime();
    const isRecent = t >= recentCutoff;
    for (const [retailer, d] of Object.entries(scan.retailers || {})) {
      if (!stats[retailer]) stats[retailer] = { baseline: { ok: 0, fail: 0 }, recent: { ok: 0, fail: 0 }, reasons: {} };
      // Count succeeded vs (failed + blocked)
      const ok = d.ok || 0;
      const bad = (d.failed || 0) + (d.blocked || 0);
      stats[retailer].baseline.ok += ok;
      stats[retailer].baseline.fail += bad;
      if (isRecent) {
        stats[retailer].recent.ok += ok;
        stats[retailer].recent.fail += bad;
      }
      // Track top failure reasons
      for (const [r, n] of Object.entries(d.reasons || {})) {
        stats[retailer].reasons[r] = (stats[retailer].reasons[r] || 0) + n;
      }
    }
  }

  // Compute success% and flag burning retailers
  console.log(
    "Retailer".padEnd(18) +
    "24h success%".padStart(15) +
    `${BASELINE_DAYS}d success%`.padStart(15) +
    "delta (pp)".padStart(15) +
    "  top reason"
  );
  console.log("─".repeat(80));

  const rows = [];
  for (const [r, s] of Object.entries(stats)) {
    const recentTotal = s.recent.ok + s.recent.fail;
    const baseTotal = s.baseline.ok + s.baseline.fail;
    if (baseTotal === 0) continue;
    const recentPct = recentTotal > 0 ? (s.recent.ok / recentTotal) * 100 : null;
    const basePct = (s.baseline.ok / baseTotal) * 100;
    const delta = recentPct !== null ? (recentPct - basePct) : null;
    const topReason = Object.entries(s.reasons).sort((a, b) => b[1] - a[1])[0];
    rows.push({
      retailer: r,
      recentPct,
      basePct,
      delta,
      topReason: topReason ? `${topReason[0]}=${topReason[1]}` : "—",
      degraded: delta !== null && delta < -DEGRADATION_THRESHOLD_PP,
    });
  }
  rows.sort((a, b) => (a.delta ?? 0) - (b.delta ?? 0)); // worst first

  for (const row of rows) {
    const flag = row.degraded ? "⚠️ " : "  ";
    const recent = row.recentPct === null ? "—" : `${row.recentPct.toFixed(1)}%`;
    const base = `${row.basePct.toFixed(1)}%`;
    const delta = row.delta === null ? "—" : (row.delta > 0 ? "+" : "") + row.delta.toFixed(1);
    console.log(
      flag + row.retailer.padEnd(16) +
      recent.padStart(15) +
      base.padStart(15) +
      delta.padStart(15) +
      "  " + row.topReason
    );
  }

  // Summary recommendations
  const burning = rows.filter(r => r.degraded);
  console.log("\n" + "─".repeat(80));
  if (burning.length === 0) {
    console.log("✅ No retailers burning their error budget faster than baseline.");
  } else {
    console.log(`⚠️  ${burning.length} retailer(s) degrading vs baseline:`);
    for (const r of burning) {
      console.log(`   - ${r.retailer}: ${r.delta.toFixed(1)}pp drop, top reason: ${r.topReason}`);
    }
    console.log("\nNext steps:");
    console.log("  • Run `node scripts/probe-waf.js <retailer>` to check WAF state");
    console.log("  • Check daemon logs for the dominant reason (waf/proxy/contract_drift)");
    console.log("  • If 'proxy' dominates, check DataImpulse credit balance");
    console.log("  • If 'waf' dominates, retailer may have rotated WAF — re-audit");
  }
  console.log();
}

main().catch((err) => {
  console.error("Fatal:", err);
  process.exit(1);
});
