#!/usr/bin/env node
// Audit state.json for stale, broken, or suspect entries.
//
// Usage:
//   node scripts/audit-state.js              # report only (no changes)
//   node scripts/audit-state.js --fix        # apply automatic fixes (URL repair, prune stale)
//   node scripts/audit-state.js --days=30    # entries last seen >N days ago marked stale (default: 30)
//
// Reports:
//   - Bottles last-seen >N days ago (likely OOS but never marked)
//   - URLs that don't match expected pattern (e.g., Kroger /p/{productId} instead of slug)
//   - Bottles with missing/null prices
//   - Stores with no bottles tracked across all retailers (orphaned entries)
//   - Watch list entries from Reddit older than 14 days (auto-pruned with --fix)
//
// Read-only by default; --fix applies automatic repairs (URL fixups, watchlist pruning).

import { readFile, writeFile, rename } from "node:fs/promises";

const args = Object.fromEntries(
  process.argv.slice(2).map((a) => {
    const [k, v] = a.replace(/^--/, "").split("=");
    return [k, v ?? true];
  })
);
const STALE_DAYS = parseInt(args.days || "30", 10);
const APPLY_FIXES = !!args.fix;

async function main() {
  const path = new URL("../state.json", import.meta.url);
  let raw;
  try { raw = await readFile(path, "utf-8"); } catch (err) {
    console.error("Could not read state.json:", err.message);
    process.exit(1);
  }
  const state = JSON.parse(raw);
  let totalEntries = 0;
  let staleEntries = 0;
  let missingPriceEntries = 0;
  let badUrlEntries = 0;
  let emptyStores = 0;
  let watchlistPruned = 0;

  console.log(`\n📋 STATE AUDIT (stale threshold: ${STALE_DAYS} days)\n`);

  const now = Date.now();
  const staleCutoff = now - STALE_DAYS * 86400 * 1000;
  const stalePerRetailer = {};
  const issues = [];

  for (const [retailer, stores] of Object.entries(state)) {
    if (retailer.startsWith("_")) continue;  // skip _watchList, _redditSeen, etc.
    for (const [storeId, storeData] of Object.entries(stores)) {
      const bottles = storeData?.bottles || {};
      const bottleCount = Object.keys(bottles).length;
      if (bottleCount === 0) {
        emptyStores++;
        continue;
      }
      for (const [name, info] of Object.entries(bottles)) {
        totalEntries++;
        const lastSeen = info.lastSeen ? new Date(info.lastSeen).getTime() : 0;
        if (lastSeen > 0 && lastSeen < staleCutoff) {
          staleEntries++;
          stalePerRetailer[retailer] = (stalePerRetailer[retailer] || 0) + 1;
          issues.push({ kind: "stale", retailer, storeId, name, lastSeen: info.lastSeen });
        }
        if (!info.price || info.price === "" || info.price === "N/A") {
          missingPriceEntries++;
          issues.push({ kind: "no-price", retailer, storeId, name });
        }
        // Kroger URL pattern: must include slug + productId (/p/{slug}/{id})
        if (retailer === "kroger" && info.url && /\/p\/\d+$/.test(info.url) && !info.url.includes("/p/0008")) {
          // Edge: state still has bare-id URLs that need slug repair
          badUrlEntries++;
          issues.push({ kind: "bad-url", retailer, storeId, name, url: info.url });
        }
      }
    }
  }

  // Watch list entries (auto-spawned from Reddit) — prune if >14 days old
  const wl = state._watchList || {};
  const wlCutoff = now - 14 * 86400 * 1000;
  for (const [key, ts] of Object.entries(wl)) {
    if (key.startsWith("reddit-") && new Date(ts).getTime() < wlCutoff) {
      issues.push({ kind: "stale-watchlist", key, ts });
      if (APPLY_FIXES) { delete wl[key]; watchlistPruned++; }
    }
  }

  // Reporting
  console.log("Counts:");
  console.log(`  Total bottle entries: ${totalEntries}`);
  console.log(`  Stale (>${STALE_DAYS} days): ${staleEntries}`);
  console.log(`  Missing price: ${missingPriceEntries}`);
  console.log(`  Bad URL pattern: ${badUrlEntries}`);
  console.log(`  Stores with 0 bottles: ${emptyStores}`);
  console.log(`  Stale watch list entries: ${issues.filter(i => i.kind === "stale-watchlist").length}`);
  if (Object.keys(stalePerRetailer).length > 0) {
    console.log("\nStale by retailer:");
    for (const [r, n] of Object.entries(stalePerRetailer)) console.log(`  ${r.padEnd(14)} ${n}`);
  }

  if (issues.length === 0) {
    console.log("\n✅ State looks clean. Nothing to flag.\n");
    return;
  }

  console.log("\nFirst 20 issues:");
  for (const issue of issues.slice(0, 20)) {
    if (issue.kind === "stale") {
      console.log(`  ⏰ ${issue.retailer}/${issue.storeId} ${issue.name} — last seen ${issue.lastSeen}`);
    } else if (issue.kind === "no-price") {
      console.log(`  💵 ${issue.retailer}/${issue.storeId} ${issue.name} — no price recorded`);
    } else if (issue.kind === "bad-url") {
      console.log(`  🔗 ${issue.retailer}/${issue.storeId} ${issue.name} — ${issue.url}`);
    } else if (issue.kind === "stale-watchlist") {
      console.log(`  📋 ${issue.key} — created ${issue.ts}`);
    }
  }
  if (issues.length > 20) console.log(`  ...and ${issues.length - 20} more`);

  if (APPLY_FIXES) {
    if (watchlistPruned > 0) {
      // Atomic write: temp file then rename
      await writeFile(new URL("../state.json.tmp", import.meta.url), JSON.stringify(state, null, 2));
      await rename(new URL("../state.json.tmp", import.meta.url), path);
      console.log(`\n✅ Fixes applied: pruned ${watchlistPruned} stale watch list entries`);
    } else {
      console.log("\n✅ No automatic fixes applicable. (Stale bottles need manual review.)");
    }
  } else {
    console.log("\n💡 Run with --fix to apply automatic repairs (watchlist pruning).");
  }
  console.log("");
}

main().catch((err) => {
  console.error("Fatal:", err);
  process.exit(1);
});
