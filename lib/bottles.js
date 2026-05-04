// Bottle catalog + canary configuration + interest tiers.
//
// This module is deliberately PURE DATA + PURE FUNCTIONS — no env-var reads, no
// network calls, no module state outside the exported constants. That makes it
// safe to import from any retailer scraper module without circular-import risk
// (the original scraper.js was 5800+ lines with everything tangled together).
//
// Phase 1 of the modularization roadmap (see CLAUDE.md "Modularization plan").

// ─── Search Queries ──────────────────────────────────────────────────────────
// Broad search queries that cover multiple bottles in a single page load.
// Each query should cover at least one TARGET_BOTTLE via matchesBottle.
export const SEARCH_QUERIES = [
  "weller bourbon",           // Weller SR/107/12/FP/SB/CYPB + William Larue Weller
  "blantons bourbon",          // Blanton's Gold/SFTB/SR/Red/Green/Black
  "van winkle",                // Pappy 10/12/15/20/23 + Family Reserve Rye 13
  "eh taylor",                 // All Taylor: SmB/SiB/BP/Rye/Seasoned/4Grain/Amaranth/CuredOak/18yr
  "stagg bourbon",             // Stagg Jr + George T. Stagg
  "george t stagg",            // George T. Stagg (BTAC) — keyword search needs exact name
  "eagle rare 17",             // Eagle Rare 17 Year (BTAC)
  "thomas handy sazerac",      // Thomas H. Handy (BTAC)
  "elmer t lee",               // Elmer T. Lee
  "rock hill farms",           // Rock Hill Farms
  "king of kentucky bourbon",  // King of Kentucky
  "old forester bourbon",      // Birthday, President's Choice, 150th Anniversary, King Ranch
  "michters bourbon",           // Michter's 10 Year Single Barrel (Costco + Total Wine + Walmart only)
  "penelope bourbon",           // Penelope Founder's Reserve + Estate Collection (Costco + Total Wine + Walmart only)
  "jack daniels aged",           // Jack Daniel's 10/12/14 Year Tennessee Whiskey
  "heaven hill bourbon",         // Heaven Hill 90th Anniversary
  "heaven hill heritage",        // Heaven Hill Heritage Collection 22 Year
  "buffalo trace",              // Canary bottle — always-available health check
];

// ─── Target Bottles ──────────────────────────────────────────────────────────
export const TARGET_BOTTLES = [
  { name: "Blanton's Gold",             searchTerms: ["blanton's gold", "blantons gold"] },
  { name: "Blanton's Straight from the Barrel", searchTerms: ["blanton's straight from the barrel", "blantons straight from the barrel", "blantons sftb", "blanton's sftb", "blanton's sfb", "blantons sfb"] },
  { name: "Blanton's Special Reserve",  searchTerms: ["blanton's special reserve", "blantons special reserve"] },
  { name: "Blanton's Original",         searchTerms: ["blanton's single barrel", "blantons single barrel", "blanton's original", "blantons original"] },
  { name: "Weller Special Reserve",      searchTerms: ["weller special reserve", "weller sr", "w.l. weller special"] },
  { name: "Weller Antique 107",          searchTerms: ["weller antique 107", "old weller antique"] },
  { name: "Weller 12 Year",             searchTerms: ["weller 12", "w.l. weller 12", "weller 12 year"] },
  { name: "Weller Full Proof",          searchTerms: ["weller full proof"] },
  { name: "Weller Single Barrel",       searchTerms: ["weller single barrel"] },
  { name: "Weller C.Y.P.B.",            searchTerms: ["weller cypb", "weller c.y.p.b", "craft your perfect bourbon"] },
  { name: "E.H. Taylor Small Batch",    searchTerms: ["eh taylor small batch", "e.h. taylor small batch", "colonel e.h. taylor small batch", "col. e.h. taylor small batch", "col e.h. taylor small batch"] },
  { name: "E.H. Taylor Single Barrel",  searchTerms: ["eh taylor single barrel", "e.h. taylor single barrel", "col. e.h. taylor single barrel", "colonel e.h. taylor single barrel"] },
  { name: "E.H. Taylor Barrel Proof",   searchTerms: ["eh taylor barrel proof", "e.h. taylor barrel proof", "col. e.h. taylor barrel proof"] },
  { name: "E.H. Taylor Straight Rye",   searchTerms: ["eh taylor rye", "e.h. taylor rye", "e.h. taylor straight rye", "col. e.h. taylor straight rye", "colonel e.h. taylor rye"] },
  { name: "E.H. Taylor Seasoned Wood",  searchTerms: ["eh taylor seasoned wood", "e.h. taylor seasoned wood"] },
  { name: "E.H. Taylor Four Grain",     searchTerms: ["eh taylor four grain", "e.h. taylor four grain"] },
  { name: "E.H. Taylor Amaranth",       searchTerms: ["eh taylor amaranth", "e.h. taylor amaranth"] },
  { name: "E.H. Taylor Cured Oak",      searchTerms: ["eh taylor cured oak", "e.h. taylor cured oak"] },
  { name: "E.H. Taylor 18 Year Marriage", searchTerms: ["eh taylor 18 year", "e.h. taylor 18 year", "eh taylor 18 year marriage", "e.h. taylor 18 year marriage"] },
  { name: "Stagg Jr",                   searchTerms: ["stagg jr", "stagg junior", "stagg bourbon", "stagg kentucky"] },
  { name: "George T. Stagg",            searchTerms: ["george t. stagg", "george t stagg"] },
  { name: "Eagle Rare 17 Year",         searchTerms: ["eagle rare 17"] },
  { name: "William Larue Weller",       searchTerms: ["william larue weller", "wm larue weller", "william l weller", "w.l. weller btac", "larue weller"] },
  { name: "Thomas H. Handy",            searchTerms: ["thomas h. handy", "thomas handy sazerac", "thomas h handy", "thomas handy"] },
  { name: "Pappy Van Winkle 10 Year",   searchTerms: ["pappy van winkle 10", "old rip van winkle 10"] },
  { name: "Pappy Van Winkle 12 Year",   searchTerms: ["pappy van winkle 12", "van winkle special reserve 12"] },
  { name: "Pappy Van Winkle 15 Year",   searchTerms: ["pappy van winkle 15", "old rip van winkle 15"] },
  { name: "Pappy Van Winkle 20 Year",   searchTerms: ["pappy van winkle 20", "old rip van winkle 20"] },
  { name: "Pappy Van Winkle 23 Year",   searchTerms: ["pappy van winkle 23"] },
  { name: "Van Winkle Family Reserve Rye", searchTerms: ["van winkle family reserve rye", "van winkle rye 13"] },
  { name: "Elmer T. Lee",               searchTerms: ["elmer t. lee", "elmer t lee"] },
  { name: "Rock Hill Farms",            searchTerms: ["rock hill farms"] },
  { name: "King of Kentucky",           searchTerms: ["king of kentucky"] },
  { name: "Old Forester Birthday Bourbon", searchTerms: ["old forester birthday"] },
  { name: "Old Forester President's Choice", searchTerms: ["old forester president's choice", "old forester presidents choice", "old forester president", "president's choice bourbon"] },
  { name: "Old Forester 150th Anniversary", searchTerms: ["old forester 150th", "old forester 150"] },
  { name: "Old Forester King Ranch",    searchTerms: ["old forester king ranch"] },
  { name: "Michter's 10 Year",            searchTerms: ["michter's 10", "michters 10", "michter's 10 year", "michters 10 year"], retailers: ["costco", "totalwine", "walmart"] },
  { name: "Penelope Founder's Reserve",   searchTerms: ["penelope founder", "penelope founder's reserve", "penelope founders reserve"], retailers: ["costco", "totalwine", "walmart"] },
  { name: "Penelope Estate Collection",   searchTerms: ["penelope estate", "penelope estate collection"], retailers: ["costco", "totalwine", "walmart"] },
  { name: "Jack Daniel's 10 Year",       searchTerms: ["jack daniel's 10 year", "jack daniels 10 year", "jack daniel's 10yr", "jack daniels 10yr", "jack daniel's aged 10"] },
  { name: "Jack Daniel's 12 Year",       searchTerms: ["jack daniel's 12 year", "jack daniels 12 year", "jack daniel's 12yr", "jack daniels 12yr", "jack daniel's aged 12"] },
  { name: "Jack Daniel's 14 Year",       searchTerms: ["jack daniel's 14 year", "jack daniels 14 year", "jack daniel's 14yr", "jack daniels 14yr", "jack daniel's aged 14"] },
  { name: "Heaven Hill 90th Anniversary", searchTerms: ["heaven hill 90th", "heaven hill 90", "heaven hill anniversary"] },
  { name: "Heaven Hill Heritage Collection 22 Year", searchTerms: ["heaven hill heritage 22", "heaven hill 22 year", "heaven hill 22yr", "heaven hill heritage collection 22", "heritage collection 22"] },
  // Canary — always-available bottle used as a scraper health check
  { name: "Buffalo Trace", searchTerms: ["buffalo trace"], canary: true },
];

// ─── Canary Bottles ──────────────────────────────────────────────────────────
// Fast lookup for canary bottles (filtered out of alerts, used in health summary only)
export const CANARY_NAMES = new Set(TARGET_BOTTLES.filter((b) => b.canary).map((b) => b.name));

// Per-retailer canary configuration — bottles that are realistically always-stocked at
// a specific retailer, used as a scraper-health probe. Default canary is Buffalo Trace,
// which works for most retailers. But Walgreens (small liquor sections) and Costco
// (Kirkland-heavy) rarely carry Buffalo Trace, making the default canary metric a false
// alarm there. To replace the canary for a specific retailer, add the bottle to
// TARGET_BOTTLES (without `canary: true`) and list the bottle's name here.
//
// IMPORTANT: each canary bottle MUST exist as a regular TARGET_BOTTLES entry. The
// `isCanaryFor(retailerKey, bottleName)` helper looks up the per-retailer canary list
// and falls back to the default Buffalo Trace.
//
// Empty array means "no canary check for this retailer" — the canary metric will
// always be false but won't trigger health-degradation alerts.
export const CANARY_BY_RETAILER = {
  // costco:    [],  // TODO: pick a Kirkland or Maker's Mark variant if we add it to TARGET_BOTTLES
  // walgreens: [],  // TODO: most reliable Walgreens stock is Jim Beam / Maker's Mark, not in TARGET_BOTTLES
};

// Returns true if `bottleName` is a canary for `retailerKey` — used to filter canary
// matches out of state/alerts (canary should never trigger user-facing notifications).
export function isCanaryFor(retailerKey, bottleName) {
  const perRetailer = CANARY_BY_RETAILER[retailerKey];
  if (perRetailer) return perRetailer.includes(bottleName);
  return CANARY_NAMES.has(bottleName);  // default
}

// ─── Bottle Interest Tiers ───────────────────────────────────────────────────
// Per-bottle interest tiers — controls alert cadence and routing.
//   obsess: every scan re-alerts; always urgent @here; ignores re-alert N config
//   track:  standard cadence (REALERT_EVERY_N_SCANS); standard urgent alert
//   ignore: silently records state, no Discord alerts at all (useful for "found
//           but not interested" cases like clearance JD 14yr at MSRP+200%)
//
// Bottles not listed here default to "track". Add a bottle to "ignore" to suppress
// alerts WITHOUT removing it from TARGET_BOTTLES (still tracked in state.json).
export const BOTTLE_INTEREST_TIERS = {
  obsess: [
    "Pappy Van Winkle 23 Year",
    "Pappy Van Winkle 20 Year",
    "Pappy Van Winkle 15 Year",
    "Van Winkle Family Reserve Rye 13",
    "King of Kentucky",
    "George T. Stagg",
    "William Larue Weller",
    "Eagle Rare 17 Year",
    "Thomas H. Handy",
    "E.H. Taylor 18 Year Marriage",
    "Old Forester Birthday Bourbon",
    "Old Forester President's Choice",
    "Old Forester 150th Anniversary",
    "Old Forester King Ranch",
    "Heaven Hill Heritage Collection 22 Year",
  ],
  track: [], // populated implicitly — anything not in obsess/ignore is "track"
  ignore: [], // bottles to silently track without alerts
};

// Backward-compat alias for code that uses the old names
export const ULTRA_RARE_BOTTLES = new Set(BOTTLE_INTEREST_TIERS.obsess);
export const IGNORED_BOTTLES = new Set(BOTTLE_INTEREST_TIERS.ignore);

export function bottleInterestTier(bottleName) {
  if (ULTRA_RARE_BOTTLES.has(bottleName)) return "obsess";
  if (IGNORED_BOTTLES.has(bottleName)) return "ignore";
  return "track";
}
