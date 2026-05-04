#!/usr/bin/env node
// Probes each retailer's homepage and inspects the cookies the WAF issues.
// Surfaces WAF migrations (e.g., Akamai → Incapsula) BEFORE they cause weeks of
// silent canary-zero metrics.
//
// Usage:   node scripts/probe-waf.js [retailer]
// Example: node scripts/probe-waf.js              # all retailers
//          node scripts/probe-waf.js safeway      # one retailer
//
// Detects:
//   ✅ Cookies match expected WAF (no migration)
//   ⚠️ Cookies don't match expected WAF — WAF migration may have occurred
//   ❌ No cookies issued / homepage returned non-200 — connectivity issue
//
// Run weekly or after seeing canary degradation alerts.

import { chromium } from "playwright-core";
import { readFile, writeFile } from "node:fs/promises";

const STATE_FILE = new URL("../waf-state.json", import.meta.url);

const CHROME_PATH = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome";

// Per-retailer expected WAF state. Used to detect migrations.
const RETAILER_PROBES = {
  costco:    { url: "https://www.costco.com/",     expected: "akamai" },
  totalwine: { url: "https://www.totalwine.com/",  expected: "perimeterx" },
  walmart:   { url: "https://www.walmart.com/",    expected: "akamai+perimeterx" },
  walgreens: { url: "https://www.walgreens.com/",  expected: "akamai" },
  samsclub:  { url: "https://www.samsclub.com/",   expected: "perimeterx" },
  safeway:   { url: "https://www.safeway.com/",    expected: "incapsula" },  // changed from akamai in 2026
  albertsons:{ url: "https://www.albertsons.com/", expected: "incapsula" },
  kroger:    { url: "https://www.frysfood.com/",   expected: "akamai" },
};

// Cookie name patterns per WAF
const WAF_PATTERNS = {
  akamai:     /^_abck|^bm_sz|^ak_bmsc/,
  incapsula:  /^incap_ses|^visid_incap|^nlbi_/,
  perimeterx: /^_px[\d]?|^_pxvid|^_pxhd/,
};

function classifyCookies(cookies) {
  const detected = new Set();
  for (const c of cookies) {
    for (const [waf, pattern] of Object.entries(WAF_PATTERNS)) {
      if (pattern.test(c.name)) detected.add(waf);
    }
  }
  return [...detected];
}

async function probe(retailerKey, { url, expected }) {
  const browser = await chromium.launch({
    headless: true,
    executablePath: CHROME_PATH,
    args: ["--disable-blink-features=AutomationControlled"],
    ignoreDefaultArgs: ["--enable-automation"],
  });
  const ctx = await browser.newContext({ viewport: { width: 1366, height: 768 } });
  const page = await ctx.newPage();
  let result;
  try {
    const resp = await page.goto(url, { waitUntil: "domcontentloaded", timeout: 25000 });
    await page.waitForTimeout(6000);
    const cookies = await ctx.cookies();
    const detected = classifyCookies(cookies);
    const status = resp?.status() || 0;
    const expectedSet = new Set(expected.split("+"));
    const matchesExpected = expected === "any"
      ? detected.length > 0
      : [...expectedSet].every((e) => detected.includes(e));
    let verdict = "❌ NO COOKIES";
    if (cookies.length === 0 || status >= 400) verdict = `❌ HTTP ${status} or no cookies`;
    else if (detected.length === 0) verdict = "⚠️ No known WAF cookies (could be new WAF or down)";
    else if (matchesExpected) verdict = `✅ OK (${detected.join(",")})`;
    else verdict = `⚠️ MIGRATION? Expected ${expected}, got ${detected.join(",")}`;
    result = { status, totalCookies: cookies.length, detected, verdict };
  } catch (err) {
    result = { error: err.message, verdict: `❌ ERROR (${err.message.slice(0, 60)})` };
  } finally {
    await browser.close().catch(() => {});
  }
  return result;
}

async function loadPriorState() {
  try {
    const raw = await readFile(STATE_FILE, "utf-8");
    return JSON.parse(raw);
  } catch { return {}; }
}

async function savePriorState(state) {
  await writeFile(STATE_FILE, JSON.stringify(state, null, 2));
}

async function main() {
  const filterKey = process.argv[2];
  const targets = filterKey ? [filterKey] : Object.keys(RETAILER_PROBES);
  const prior = await loadPriorState();
  const newState = { ...prior };

  console.log("\n🔍 WAF cookie probe — detects migrations early\n");
  console.log("Retailer       Expected               Detected               Verdict");
  console.log("─".repeat(95));
  const rotations = [];
  for (const k of targets) {
    const probe_ = RETAILER_PROBES[k];
    if (!probe_) {
      console.log(`Unknown retailer: ${k}`);
      continue;
    }
    process.stdout.write(`${k.padEnd(14)} ${probe_.expected.padEnd(22)} `);
    const r = await probe(k, probe_);
    process.stdout.write(`${(r.detected || []).join(",").padEnd(22)} ${r.verdict}\n`);

    // Compare with prior probe — flag rotations even when expected/actual still match
    const detectedKey = [...(r.detected || [])].sort().join(",");
    if (prior[k]?.detected && prior[k].detected !== detectedKey) {
      rotations.push({
        retailer: k,
        prior: prior[k].detected || "(none)",
        current: detectedKey || "(none)",
        priorAt: prior[k].lastSeen,
      });
    }
    newState[k] = {
      detected: detectedKey,
      verdict: r.verdict,
      lastSeen: new Date().toISOString(),
    };
  }
  console.log();

  if (rotations.length > 0) {
    console.log("\n🚨 WAF ROTATIONS DETECTED SINCE LAST PROBE:\n");
    for (const r of rotations) {
      console.log(`  • ${r.retailer}: ${r.prior} → ${r.current}  (prior probe: ${r.priorAt})`);
    }
    console.log("\nNext steps:");
    console.log("  • Update RETAILER_PROBES.expected in this script");
    console.log("  • Update CLAUDE.md retailer table to reflect new WAF");
    console.log("  • Audit affected scraper's cookie/pre-warm logic");
    console.log("  • Run `node scripts/error-budget.js` to see if scraper is degrading");
  } else if (Object.keys(prior).length > 0) {
    console.log("✅ No WAF rotations detected since last probe.");
  } else {
    console.log("(First run — baseline saved. Re-run later to detect rotations.)");
  }
  await savePriorState(newState);
  console.log(`\nState saved to ${STATE_FILE.pathname.replace(process.cwd(), ".")}\n`);
}

main().catch((err) => {
  console.error("Fatal:", err);
  process.exit(1);
});
