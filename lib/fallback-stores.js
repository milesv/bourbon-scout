// Static store list for 85283 (Tempe, AZ) area.
// Used when browser-based store locators fail (e.g., CI datacenter IPs get blocked).
// To refresh: run discovery locally, review stores.json, update this file.

export const FALLBACK_STORES = {
  // Addresses verified 2026-05-05 via independent multi-source check
  // (DuckDuckGo aggregating Yelp, Chamber of Commerce, mystore411, mallscenters,
  // Mapdoor, MapQuest, City-Data + Bing Maps). The storeIds are the source of
  // truth — Costco's API confirms them. Prior addresses (1425 W Queen Creek,
  // 2270 S Market, 777 S Priest, 5101 S Power, 2601 E Thomas) were stale and
  // pointed to non-Costco locations; surfaced when the alert Maps links would
  // have sent the user to wrong buildings.
  // Mesa #1028 (1444 S Sossaman Rd) added 2026-05-05 to expand east-Mesa
  // coverage that was missing — fills the gap between SE Gilbert (#644) and
  // Paradise Valley (#1058).
  costco: [
    { storeId: "736",  name: "Costco Chandler",     address: "595 S Galleria Way, Chandler, AZ 85226",  distanceMiles: 4.5,  phone: "(480) 375-2051" },
    { storeId: "481",  name: "Costco NW Gilbert",   address: "1415 N Arizona Ave, Gilbert, AZ 85233",   distanceMiles: 5.5,  phone: "(480) 293-0053" },
    { storeId: "436",  name: "Costco Tempe",        address: "1445 W Elliot Rd, Tempe, AZ 85284",       distanceMiles: 2.5,  phone: "(480) 496-6651" },
    { storeId: "644",  name: "Costco SE Gilbert",   address: "2887 S Market St, Gilbert, AZ 85295",     distanceMiles: 10.5, phone: "(480) 366-3950" },
    { storeId: "465",  name: "Costco Phoenix Oak",  address: "4502 E Oak St, Phoenix, AZ 85008",        distanceMiles: 9.5,  phone: "(602) 808-0101" },
    { storeId: "1028", name: "Costco East Mesa",    address: "1444 S Sossaman Rd, Mesa, AZ 85209",      distanceMiles: 12.5, phone: "(480) 333-6565" },
  ],
  totalwine: [
    { storeId: "1010", name: "Total Wine Tempe Marketplace", address: "1900 E Rio Salado Pkwy Ste 120, Tempe, AZ 85281", distanceMiles: 4.6, phone: "(480) 967-0313" },
    { storeId: "1005", name: "Total Wine Gilbert", address: "2224 E Williams Field Rd, Gilbert, AZ 85295", distanceMiles: 7.5, phone: "(480) 786-0270" },
    { storeId: "1014", name: "Total Wine Mesa", address: "1834 S Signal Butte Rd, Mesa, AZ 85209", distanceMiles: 13.2, phone: "(623) 306-7300" },
    { storeId: "1011", name: "Total Wine Queen Creek", address: "21072 S Ellsworth Loop Rd, Queen Creek, AZ 85142", distanceMiles: 14.8, phone: "(602) 325-1653" },
  ],
  walmart: [
    { storeId: "5768", name: "Walmart Supercenter #5768", address: "800 E Southern Ave, Tempe, AZ 85282", distanceMiles: 1.8, phone: "(480) 966-0264" },
    { storeId: "1746", name: "Walmart Supercenter #1746", address: "1380 W Elliot Rd, Tempe, AZ 85284", distanceMiles: 2.0, phone: "(480) 345-8686" },
    { storeId: "6480", name: "Walmart Supercenter #6480", address: "3460 W Chandler Blvd, Chandler, AZ 85226", distanceMiles: 4.6, phone: "(480) 333-2654" },
    { storeId: "1512", name: "Walmart Supercenter #1512", address: "800 W Warner Rd, Chandler, AZ 85225", distanceMiles: 5.1, phone: "(480) 786-0062" },
    { storeId: "4293", name: "Neighborhood Market #4293", address: "2435 E Baseline Rd, Phoenix, AZ 85042", distanceMiles: 5.5, phone: "(602) 232-2115" },
  ],
  safeway: [
    { storeId: "1515", name: "Safeway Elliot Rd", address: "1515 E Elliot Rd, Tempe, AZ 85284", distanceMiles: 1.8, phone: "(480) 755-1844" },
  ],
  albertsons: [
    { storeId: "3067", name: "Albertsons Baseline Rd", address: "1951 W Baseline Rd, Mesa, AZ 85202", distanceMiles: 3.2, phone: "(480) 456-4373" },
    { storeId: "3073", name: "Albertsons Ray Rd", address: "4060 W Ray Rd, Chandler, AZ 85226", distanceMiles: 3.5, phone: "(480) 491-1026" },
    { storeId: "3015", name: "Albertsons Alma School Rd", address: "3145 S Alma School Rd, Chandler, AZ 85248", distanceMiles: 7.8, phone: "(480) 899-7102" },
  ],
  walgreens: [
    { storeId: "3768", name: "Walgreens Tempe McClintock", address: "6404 S McClintock Dr, Tempe, AZ 85283", distanceMiles: 1.0, phone: "(480) 838-9200" },
    { storeId: "2398", name: "Walgreens Tempe Baseline", address: "925 W Baseline Rd, Tempe, AZ 85283", distanceMiles: 1.5, phone: "(480) 820-1990" },
    { storeId: "1197", name: "Walgreens Tempe Southern", address: "1745 E Southern Ave, Tempe, AZ 85282", distanceMiles: 2.5, phone: "(480) 838-3642" },
    { storeId: "4376", name: "Walgreens Tempe Warner", address: "1825 E Warner Rd, Tempe, AZ 85284", distanceMiles: 3.0, phone: "(480) 820-9984" },
  ],
  samsclub: [
    { storeId: "4956", name: "Sam's Club Tempe", address: "2080 E Rio Salado Pkwy, Tempe, AZ 85288", distanceMiles: 4.6, phone: "(480) 606-1805" },
  ],
  // CityHive single-store retailers. Each is its own retailer (separate inventory,
  // brand, and merchant_id) but shares the same scraper code. Distances measured
  // from 2028 E Libra Drive, Tempe AZ 85283.
  extramile: [
    { storeId: "chandler", name: "ExtraMile Liquors (Chandler Chevron)", address: "7000 W Chandler Blvd, Chandler, AZ 85226", distanceMiles: 6.0, phone: "(480) 961-7900" },
  ],
  liquorexpress: [
    { storeId: "tempe-apache", name: "Liquor Express Tempe", address: "1605 E Apache Blvd, Tempe, AZ 85281", distanceMiles: 3.0 },
  ],
  chandlerliquors: [
    { storeId: "chandler-arizona-ave", name: "Chandler Liquors", address: "554 N Arizona Ave, Chandler, AZ 85225", distanceMiles: 7.0, phone: "(480) 963-5100" },
  ],
  bevmo: [],
  kroger: [],
};

// Stores explicitly added by the user that are outside the discovery radius.
// Always merged after discovery (unlike FALLBACK_STORES which only apply when
// discovery returns 0). Deduped by storeId to prevent duplicates.
export const EXTRA_STORES = {
  costco: [
    { storeId: "427", name: "Costco Scottsdale", address: "15255 N Hayden Rd, Scottsdale, AZ 85260", distanceMiles: 17, phone: "(480) 948-5040" },
    { storeId: "1058", name: "Costco Paradise Valley", address: "4570 E Cactus Rd, Phoenix, AZ 85032", distanceMiles: 19, phone: "(480) 308-7044" },
  ],
};
