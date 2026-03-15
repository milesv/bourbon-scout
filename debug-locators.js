import { chromium as rebrowserChromium } from "rebrowser-playwright-core";
import { addExtra } from "playwright-extra";
import StealthPlugin from "puppeteer-extra-plugin-stealth";
import { mkdir, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import path from "node:path";

const chromium = addExtra(rebrowserChromium);
chromium.use(StealthPlugin());

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DEBUG_DIR = path.join(__dirname, "debug");

const ZIP = "85283";

const RETAILERS = [
  {
    name: "costco",
    url: `https://www.costco.com/warehouse-locations?langId=-1&zipCode=${ZIP}`,
    interact: null,
  },
  {
    name: "totalwine",
    url: `https://www.totalwine.com/store-finder?searchAddress=${ZIP}`,
    interact: null,
  },
  {
    name: "walmart",
    url: `https://www.walmart.com/store/finder?location=${ZIP}`,
    interact: null,
  },
  {
    name: "safeway",
    url: `https://www.safeway.com/stores/search?search=${ZIP}`,
    interact: null,
  },
  {
    name: "bevmo",
    url: "https://www.bevmo.com/pages/store-locator",
    interact: async (page) => {
      // Try multiple selectors for the search input
      const inputSelectors = [
        'input[type="text"]',
        'input[name="address"]',
        'input[placeholder*="zip" i]',
        'input[placeholder*="address" i]',
        'input[placeholder*="city" i]',
        "#search-input",
        'input[type="search"]',
      ];

      let filled = false;
      for (const sel of inputSelectors) {
        try {
          const input = await page.$(sel);
          if (input && (await input.isVisible())) {
            await input.click();
            await input.fill(ZIP);
            console.log(`    [bevmo] Filled input with selector: ${sel}`);
            filled = true;

            // Try to submit
            const submitSelectors = [
              'button[type="submit"]',
              ".search-button",
              '[class*="search"] button',
              'button[aria-label*="search" i]',
              "form button",
            ];
            let submitted = false;
            for (const btnSel of submitSelectors) {
              try {
                const btn = await page.$(btnSel);
                if (btn && (await btn.isVisible())) {
                  await btn.click();
                  console.log(`    [bevmo] Clicked submit with selector: ${btnSel}`);
                  submitted = true;
                  break;
                }
              } catch { /* try next */ }
            }
            if (!submitted) {
              await input.press("Enter");
              console.log("    [bevmo] Pressed Enter to submit");
            }
            break;
          }
        } catch { /* try next */ }
      }

      if (!filled) {
        console.log("    [bevmo] WARNING: Could not find any search input");
      }

      // Extra wait for search results to render
      await page.waitForTimeout(3000);
    },
  },
];

/**
 * Extract the first 200 unique tag+class combos from the page.
 */
async function extractSelectors(page) {
  return page.evaluate(() => {
    const seen = new Set();
    const results = [];
    const allEls = document.querySelectorAll("*");
    for (const el of allEls) {
      if (results.length >= 200) break;
      const tag = el.tagName.toLowerCase();
      const classes = el.className && typeof el.className === "string"
        ? el.className.trim().split(/\s+/).sort().join(".")
        : "";
      const combo = classes ? `${tag}.${classes}` : tag;
      if (!seen.has(combo)) {
        seen.add(combo);
        results.push(combo);
      }
    }
    return results;
  });
}

async function main() {
  // Ensure debug/ directory exists
  await mkdir(DEBUG_DIR, { recursive: true });

  console.log("Launching browser...");
  const chromePath = process.platform === "darwin"
    ? "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
    : null;
  const launchOpts = { headless: true };
  if (chromePath) launchOpts.executablePath = chromePath;
  const browser = await chromium.launch(launchOpts);

  try {
    const context = await browser.newContext({
      viewport: { width: 1366, height: 768 },
      locale: "en-US",
    });
    const page = await context.newPage();

    for (const retailer of RETAILERS) {
      const { name, url, interact } = retailer;
      console.log(`\n--- ${name.toUpperCase()} ---`);
      console.log(`  Navigating to: ${url}`);

      try {
        await page.goto(url, {
          waitUntil: "domcontentloaded",
          timeout: 30000,
        });
      } catch (err) {
        console.log(`  Navigation error (continuing anyway): ${err.message}`);
      }

      // If this retailer needs interaction (e.g. BevMo search), do it now
      if (interact) {
        console.log("  Running interaction steps...");
        await interact(page);
      }

      // Wait 5 seconds for JS rendering
      console.log("  Waiting 5s for JS rendering...");
      await page.waitForTimeout(5000);

      // 1) Screenshot
      const screenshotPath = path.join(DEBUG_DIR, `${name}.png`);
      await page.screenshot({ path: screenshotPath, fullPage: true });
      console.log(`  Screenshot saved: ${screenshotPath}`);

      // 2) Full page HTML
      const html = await page.content();
      const htmlPath = path.join(DEBUG_DIR, `${name}.html`);
      await writeFile(htmlPath, html, "utf-8");
      console.log(`  HTML saved: ${htmlPath} (${(html.length / 1024).toFixed(1)} KB)`);

      // 3) Unique tag+class selector combos
      const selectors = await extractSelectors(page);
      const selectorsPath = path.join(DEBUG_DIR, `${name}-selectors.txt`);
      await writeFile(selectorsPath, selectors.join("\n") + "\n", "utf-8");
      console.log(`  Selectors saved: ${selectorsPath} (${selectors.length} unique combos)`);
    }

    await context.close();
  } finally {
    await browser.close();
    console.log("\nBrowser closed. Debug artifacts saved to debug/");
  }
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
