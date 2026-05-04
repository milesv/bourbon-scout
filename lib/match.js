// Bottle matching + price/size parsing + Discord title truncation.
//
// Phase 2 of the modularization roadmap (see CLAUDE.md "Modularization plan").
// Pure functions and pure constants — no env reads, no network, no module state.
// Safe to import from any retailer module.

// ─── Text Normalization ──────────────────────────────────────────────────────
// Normalize unicode curly quotes/apostrophes to ASCII before matching.
// Retailers inconsistently use ‘/’ ("Blanton’s") vs ASCII ("Blanton's").
export function normalizeText(text) {
  return text.toLowerCase().replace(/[‘’‚‛′]/g, "'").replace(/[“”„‟″]/g, '"');
}

// ─── EXCLUDE_TERMS ───────────────────────────────────────────────────────────
// Reject any product whose title contains these terms before matching against
// `searchTerms`. Catches three classes of false positives:
//   1. Multi-bottle bundles that match many bottle names at once
//   2. Miniature bottles (50ml) that aren't the bottle we want
//   3. Merchandise/apparel that happens to share a bottle name (e.g. Walmart's
//      "Elmer T. Lee S to 5XL T-Shirt" — the matcher caught this in production).
export const EXCLUDE_TERMS = [
  // Multi-bottle bundles
  "sampler", "gift set", "variety pack", "combo pack", "bundle",
  // Miniatures
  "miniature", "mini bottle", " 50ml", " 50 ml",
  // Merchandise / non-bottle products (Elmer T. Lee T-Shirt false positive at Walmart)
  "t-shirt", "t shirt", "tshirt", "shirt", "hoodie", "sweatshirt",
  "hat", "cap", "mug", "glass set", "glasses set", "tumbler", "decanter",
  "poster", "sign", "barrel head", "stave", "keychain", "keyring",
];

// ─── matchesBottle ───────────────────────────────────────────────────────────
// Returns true if `text` (a product title) matches `bottle` (a TARGET_BOTTLES
// entry) for the given retailer. Filters out EXCLUDE_TERMS first, then enforces
// per-retailer restrictions if present, then requires at least one searchTerm
// substring match.
export function matchesBottle(text, bottle, retailerKey) {
  const lower = normalizeText(text);
  if (EXCLUDE_TERMS.some((t) => lower.includes(t))) return false;
  if (retailerKey && bottle.retailers && !bottle.retailers.includes(retailerKey)) return false;
  return bottle.searchTerms.some((term) => lower.includes(term));
}

// ─── parsePrice / dedupFound ─────────────────────────────────────────────────
// Extract numeric price from strings like "$29.99", "29.99", "$1,299.99"
export function parsePrice(str) {
  if (!str) return 0;
  const match = str.replace(/,/g, "").match(/([\d.]+)/);
  return match ? parseFloat(match[1]) : 0;
}

// Dedup found bottles by name, preferring the entry with the lowest price.
// Falls back to first occurrence if prices are equal or unparseable.
export function dedupFound(found) {
  const byName = new Map();
  for (const f of found) {
    const prev = byName.get(f.name);
    if (!prev) { byName.set(f.name, f); continue; }
    const prevPrice = parsePrice(prev.price);
    const currPrice = parsePrice(f.price);
    if (currPrice > 0 && (prevPrice === 0 || currPrice < prevPrice)) {
      byName.set(f.name, f);
    }
  }
  return Array.from(byName.values());
}

// ─── parseSize ───────────────────────────────────────────────────────────────
// Parse bottle size from product title text (e.g., "750ml", "1.75L", "750 ML")
export function parseSize(text) {
  if (!text) return "";
  const match = text.match(/([\d.]+)\s*(ml|cl|l|liter|litre)/i);
  if (!match) return "";
  const [, num, unit] = match;
  const u = unit.toLowerCase();
  if (u === "ml") return `${num}ml`;
  if (u === "cl") return `${Math.round(parseFloat(num) * 10)}ml`;
  return `${parseFloat(num)}L`;
}

// ─── Discord title truncation ────────────────────────────────────────────────
// Discord limits embed titles to 256 chars. truncateDescription (which depends
// on Discord-specific OOS layout) stays in scraper.js.
export const DISCORD_TITLE_LIMIT = 256;
export function truncateTitle(title) {
  if (title.length <= DISCORD_TITLE_LIMIT) return title;
  return title.slice(0, DISCORD_TITLE_LIMIT - 1) + "…";
}

// ─── filterMiniatures ────────────────────────────────────────────────────────
// Filter out miniature bottles (50ml) that slip past EXCLUDE_TERMS because the
// product title doesn't mention the size. Price is the reliable signal: no
// allocated 750ml bourbon is under $20, but 50ml miniatures are typically
// $8-15. Also catches size when available.
//
// MAX_BOTTLE_PRICE = 500: no allocated bourbon retails above $350 (Pappy 23).
// $500+ = secondary market resellers (ReserveBar/Caskers); rejected to avoid
// alerting on prices the user wouldn't pay anyway.
export const MIN_BOTTLE_PRICE = 20;
export const MAX_BOTTLE_PRICE = 500;
export function filterMiniatures(found) {
  return found.filter((f) => {
    const size = (f.size || "").toLowerCase();
    if (size && size !== "" && size !== "750ml" && size !== "1l" && size !== "1.75l") {
      const ml = parseInt(size);
      if (!isNaN(ml) && ml < 200) return false; // Explicit small size (50ml, 100ml)
    }
    const price = parsePrice(f.price);
    if (price > 0 && price < MIN_BOTTLE_PRICE) return false; // Miniature price range
    if (price > 0 && price > MAX_BOTTLE_PRICE) {
      console.warn(`[filter] Rejected "${f.name}" at ${f.price} — exceeds $${MAX_BOTTLE_PRICE} ceiling`);
      return false;
    }
    return true;
  });
}
