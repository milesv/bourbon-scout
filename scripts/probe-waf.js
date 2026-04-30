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

async function main() {
  const filterKey = process.argv[2];
  const targets = filterKey ? [filterKey] : Object.keys(RETAILER_PROBES);
  console.log("\n🔍 WAF cookie probe — detects migrations early\n");
  console.log("Retailer       Expected               Detected               Verdict");
  console.log("─".repeat(95));
  for (const k of targets) {
    const probe_ = RETAILER_PROBES[k];
    if (!probe_) {
      console.log(`Unknown retailer: ${k}`);
      continue;
    }
    process.stdout.write(`${k.padEnd(14)} ${probe_.expected.padEnd(22)} `);
    const r = await probe(k, probe_);
    process.stdout.write(`${(r.detected || []).join(",").padEnd(22)} ${r.verdict}\n`);
  }
  console.log("\n");
}

main().catch((err) => {
  console.error("Fatal:", err);
  process.exit(1);
});
