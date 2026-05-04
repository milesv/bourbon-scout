// Bot-detection / block-page heuristics + PerimeterX challenge solver.
//
// Phase 2 of the modularization roadmap (see CLAUDE.md "Modularization plan").
// Pure body-text heuristics for the fetch helpers; the only browser-dependent
// function is `solveHumanChallenge`, which takes a Playwright `page` and a
// `setTimeout`-based sleep (no env reads, no module state).

// Inline sleep to keep this module dependency-free. Matches the original
// `rawSleep` behavior in scraper.js — has jitter, but solveHumanChallenge
// chooses hold durations large enough to absorb ±30% jitter without dropping
// below PerimeterX's minimum-hold threshold.
const sleep = (ms) => {
  const jitter = ms * 0.3 * (Math.random() * 2 - 1);
  return new Promise((r) => setTimeout(r, Math.max(0, Math.round(ms + jitter))));
};

// ─── isBlockedPage ───────────────────────────────────────────────────────────
// Detect bot challenge/block pages that return HTTP 200 but no real content.
// Returns true if the page appears to be a challenge, access denied, or CAPTCHA page.
export async function isBlockedPage(page) {
  const title = await page.title().catch(() => "");
  const lower = title.toLowerCase();
  if (lower.includes("access denied") || lower.includes("robot") ||
      lower.includes("captcha") || lower.includes("challenge") ||
      lower.includes("blocked") || lower.includes("verify")) {
    return true;
  }
  // Check body text for challenges that use normal-looking titles
  const bodyText = String(await page.evaluate(() => document.body?.innerText?.slice(0, 5000) || "").catch(() => ""));
  const bodyLower = bodyText.toLowerCase();
  return bodyLower.includes("please verify") || bodyLower.includes("are you a robot") ||
    bodyLower.includes("security check") || bodyLower.includes("one more step") ||
    bodyLower.includes("checking your browser") || bodyLower.includes("press & hold");
}

// ─── isFetchBlocked / isCostcoBlocked ────────────────────────────────────────
// Akamai/PerimeterX challenge patterns to detect blocked responses in fetch HTML.
// Shared across all fetch paths (Costco, Walmart, Sam's Club).
export const FETCH_BLOCKED_PATTERNS = [
  "Access Denied", "robot", "captcha",
  "Request unsuccessful", "Incapsula",
  "Enable JavaScript", "verify you are human",
  "_ct_challenge", "px-captcha",
  "Please verify", "security check",
  "one more step", "checking your browser",
];

export function isFetchBlocked(html) {
  const lower = html.length > 10000 ? html.slice(0, 10000).toLowerCase() : html.toLowerCase();
  return FETCH_BLOCKED_PATTERNS.some((p) => lower.includes(p.toLowerCase()));
}

// Backward-compatible alias for Costco-specific callers
export const isCostcoBlocked = isFetchBlocked;

// ─── solveHumanChallenge ─────────────────────────────────────────────────────
// Detect and solve PerimeterX "Press & Hold" challenge.
// PerimeterX renders the button via captcha.js inside #px-captcha — the "Press & Hold"
// text is drawn by the script (canvas/shadow DOM), not as a regular DOM element.
// Returns true if a challenge was found and solved, false otherwise.
export async function solveHumanChallenge(page) {
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      const bodyText = String(await page.evaluate(() => document.body?.innerText?.slice(0, 2000) || "").catch(() => ""));
      if (!bodyText.includes("Press & Hold")) return false;

      if (attempt === 0) console.log("[bot] Detected Press & Hold challenge — solving...");
      else console.log("[bot] Retry attempt #2...");

      /* v8 ignore start -- PerimeterX challenge solver requires real browser mouse/hold interactions */
      const target = await page.waitForSelector("#px-captcha", { timeout: 5000 }).catch(() => null);
      if (!target) {
        console.warn("[bot] #px-captcha not found — cannot solve");
        return false;
      }
      await sleep(1000 + Math.random() * 1000);

      const box = await target.boundingBox();
      if (!box) {
        console.warn("[bot] #px-captcha has no bounding box");
        if (attempt === 0) { await sleep(2000); continue; }
        return false;
      }

      const cx = box.x + box.width / 2 + (Math.random() * 10 - 5);
      const cy = box.y + box.height / 2 + (Math.random() * 6 - 3);
      await page.mouse.move(cx, cy, { steps: 15 + Math.floor(Math.random() * 10) });
      await sleep(300 + Math.random() * 400);

      await page.mouse.down();
      const holdMs = 14000 + Math.random() * 4000;
      console.log(`[bot] Holding for ${(holdMs / 1000).toFixed(1)}s...`);

      const microMoves = Math.floor(holdMs / 2000);
      const moveInterval = holdMs / (microMoves + 1);
      for (let m = 0; m < microMoves; m++) {
        await sleep(moveInterval);
        await page.mouse.move(
          cx + (Math.random() * 4 - 2),
          cy + (Math.random() * 4 - 2),
          { steps: 2 },
        ).catch(() => {});
      }
      await sleep(moveInterval);
      await page.mouse.up();

      await Promise.race([
        page.waitForNavigation({ waitUntil: "domcontentloaded", timeout: 15000 }),
        sleep(15000),
      ]).catch(() => {});
      await sleep(1500 + Math.random() * 1000);

      const stillBlocked = await page.evaluate(() =>
        (document.body?.innerText || "").includes("Press & Hold"),
      ).catch(() => true);

      if (!stillBlocked) {
        console.log("[bot] Challenge solved successfully");
        return true;
      }
      if (attempt === 0) {
        console.warn("[bot] Challenge still present — retrying with longer hold...");
        await sleep(2000 + Math.random() * 2000);
        continue;
      }
      console.warn("[bot] Challenge still present after 2 attempts");
      /* v8 ignore stop */
      return false;
    } catch (err) {
      console.warn(`[bot] Challenge solve error: ${err.message}`);
      if (attempt === 0) { await sleep(2000).catch(() => {}); continue; }
      return false;
    }
  }
  return false;
}
