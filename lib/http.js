// HTTP fingerprint helpers — Chrome version detection, FETCH_HEADERS, GREASE
// brand string, and a node-fetch retry wrapper.
//
// Phase 2 of the modularization roadmap (see CLAUDE.md "Modularization plan").
// Reads the system Chrome version once at import time (synchronously, <50ms).
// Otherwise no env reads, no module state. `scraperFetch` / `scraperFetchRetry`
// remain in scraper.js because they're tightly coupled to the proxy-exhaustion
// state machine (primaryProxyExhausted, failoverToBackupProxy, trackBandwidth).
//
// Future per-retailer modules import FETCH_HEADERS + CHROME_VERSION from here,
// then call `scraperFetch` from scraper.js — keeps proxy state in one place.

import fetch from "node-fetch";

// Inline sleep to keep this module dependency-free. Used only in fetchRetry's
// 1s backoff pause; the original used the jittered sleep from scraper.js.
const sleep = (ms) => {
  const jitter = ms * 0.3 * (Math.random() * 2 - 1);
  return new Promise((r) => setTimeout(r, Math.max(0, Math.round(ms + jitter))));
};

// ─── Platform Detection ──────────────────────────────────────────────────────
// macOS-aware UA + TLS fingerprint matching. Mac runs system Chrome (authentic
// TLS handshake); non-Mac (CI) uses Windows Chrome UA + bundled Chromium.
export const IS_MAC = process.platform === "darwin";

// ─── Chrome Version Auto-Detection ───────────────────────────────────────────
// Auto-detect Chrome version from the system binary to keep UA + Sec-CH-UA in
// sync with the TLS fingerprint. Hardcoded versions drift when Chrome auto-
// updates, creating a TLS-vs-UA mismatch that anti-bot systems specifically
// flag. Synchronous version detection at module load — execFileSync is fast
// (<50ms). Falls back to a known-good version when Chrome isn't found (e.g. CI).
let detectedVersion = "146";
try {
  if (IS_MAC) {
    const { execFileSync } = await import("node:child_process");
    const out = execFileSync(
      "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
      ["--version"],
      { timeout: 5000 },
    ).toString().trim();
    const match = out.match(/Chrome\s+(\d+)/);
    if (match) detectedVersion = match[1];
  }
} catch { /* use fallback */ }
export const CHROME_VERSION = detectedVersion;

// ─── Chrome Path ─────────────────────────────────────────────────────────────
// Use system Chrome on Mac for authentic TLS fingerprint (Playwright's bundled
// Chromium has a recognizable TLS signature that Akamai/PerimeterX flag).
export const CHROME_PATH = IS_MAC
  ? "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
  : null;

// ─── getGreasedBrand ─────────────────────────────────────────────────────────
// Replicate Chromium's Sec-CH-UA GREASE algorithm (user_agent_utils.cc).
// Each Chrome major version deterministically selects a grease brand name,
// version, and entry ordering. Hardcoding a single brand string creates a
// version mismatch signal when Chrome auto-updates — this function auto-matches
// any version.
export function getGreasedBrand(majorVersion) {
  const chars = [" ", "(", ":", "-", ".", "/", ")", ";", "=", "?", "_"];
  const versions = ["8", "99", "24"];
  const orders = [[0,1,2], [0,2,1], [1,0,2], [1,2,0], [2,0,1], [2,1,0]];
  const brand = `Not${chars[majorVersion % 11]}A${chars[(majorVersion + 1) % 11]}Brand`;
  const ver = versions[majorVersion % 3];
  const order = orders[majorVersion % 6];
  const entries = [
    `"${brand}";v="${ver}"`,
    `"Chromium";v="${majorVersion}"`,
    `"Google Chrome";v="${majorVersion}"`,
  ];
  return order.map(i => entries[i]).join(", ");
}

// ─── FETCH_HEADERS ───────────────────────────────────────────────────────────
// Headers for fetch-based scrapers — property insertion order matches Chrome's
// HTTP/2 wire order. got-scraping's TransformHeadersAgent only sorts HTTP/1.1
// headers; on H2, Node's http2.request() sends headers in JS object iteration
// order (= insertion order). Akamai pattern-matches header order as a zero-CPU
// bot signal — Client Hints before User-Agent is critical. Platform-aware:
// macOS UA on Mac to match TLS fingerprint.
export const FETCH_HEADERS = {
  "Sec-CH-UA": getGreasedBrand(Number(CHROME_VERSION)),
  "Sec-CH-UA-Mobile": "?0",
  "Sec-CH-UA-Platform": IS_MAC ? '"macOS"' : '"Windows"',
  "Upgrade-Insecure-Requests": "1",
  "User-Agent": IS_MAC
    ? `Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/${CHROME_VERSION}.0.0.0 Safari/537.36`
    : `Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/${CHROME_VERSION}.0.0.0 Safari/537.36`,
  "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
  "Sec-Fetch-Site": "cross-site",
  "Sec-Fetch-Mode": "navigate",
  "Sec-Fetch-User": "?1",
  "Sec-Fetch-Dest": "document",
  "Cache-Control": "max-age=0",
  "Accept-Encoding": "gzip, deflate",
  "Accept-Language": "en-US,en;q=0.9",
  "Referer": "https://www.google.com/",
  "priority": "u=0, i",
};

// ─── fetchRetry ──────────────────────────────────────────────────────────────
// Fetch with one retry on transient network errors (timeouts, DNS failures).
// HTTP error responses (4xx/5xx) are NOT retried — caller handles those.
// Used by node-fetch API paths (Kroger, Safeway, Discord webhooks). The
// got-scraping equivalent (`scraperFetchRetry`) lives in scraper.js because
// it's coupled to the proxy-exhaustion state machine.
export async function fetchRetry(url, opts) {
  try {
    return await fetch(url, opts);
  } catch (err) {
    console.warn(`[fetchRetry] ${err.message} — retrying in 1s`);
    await sleep(1000);
    return await fetch(url, opts);
  }
}
