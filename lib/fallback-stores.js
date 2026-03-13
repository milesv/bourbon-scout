// Static store list for 85283 (Tempe, AZ) area.
// Used when browser-based store locators fail (e.g., CI datacenter IPs get blocked).
// To refresh: run discovery locally, review stores.json, update this file.

export const FALLBACK_STORES = {
  costco: [
    { storeId: "736", name: "Costco Chandler", address: "1425 W Queen Creek Rd, Chandler, AZ 85248", distanceMiles: 5.2 },
    { storeId: "481", name: "Costco Gilbert", address: "2270 S Market St, Gilbert, AZ 85295", distanceMiles: 8.1 },
    { storeId: "436", name: "Costco Tempe", address: "777 S Priest Dr, Tempe, AZ 85281", distanceMiles: 3.5 },
    { storeId: "644", name: "Costco SE Mesa", address: "5101 S Power Rd, Mesa, AZ 85212", distanceMiles: 12.3 },
    { storeId: "465", name: "Costco Phoenix Thomas Rd", address: "2601 E Thomas Rd, Phoenix, AZ 85016", distanceMiles: 11.0 },
  ],
  totalwine: [
    { storeId: "1010", name: "Total Wine Tempe Marketplace", address: "1900 E Rio Salado Pkwy Ste 120, Tempe, AZ 85281", distanceMiles: 4.6 },
    { storeId: "1005", name: "Total Wine Gilbert", address: "2224 E Williams Field Rd, Gilbert, AZ 85295", distanceMiles: 7.5 },
    { storeId: "1014", name: "Total Wine Mesa", address: "1834 S Signal Butte Rd, Mesa, AZ 85209", distanceMiles: 13.2 },
    { storeId: "1011", name: "Total Wine Queen Creek", address: "21072 S Ellsworth Loop Rd, Queen Creek, AZ 85142", distanceMiles: 14.8 },
  ],
  walmart: [
    { storeId: "5768", name: "Walmart Supercenter #5768", address: "800 E Southern Ave, Tempe, AZ 85282", distanceMiles: 1.8 },
    { storeId: "1746", name: "Walmart Supercenter #1746", address: "1380 W Elliot Rd, Tempe, AZ 85284", distanceMiles: 2.0 },
    { storeId: "6480", name: "Walmart Supercenter #6480", address: "3460 W Chandler Blvd, Chandler, AZ 85226", distanceMiles: 4.6 },
    { storeId: "1512", name: "Walmart Supercenter #1512", address: "800 W Warner Rd, Chandler, AZ 85225", distanceMiles: 5.1 },
    { storeId: "4293", name: "Neighborhood Market #4293", address: "2435 E Baseline Rd, Phoenix, AZ 85042", distanceMiles: 5.5 },
  ],
  safeway: [
    { storeId: "1515", name: "Safeway Elliot Rd", address: "1515 E Elliot Rd, Tempe, AZ 85284", distanceMiles: 1.8 },
  ],
  walgreens: [],
  samsclub: [
    { storeId: "4956", name: "Sam's Club Tempe", address: "2080 E Rio Salado Pkwy, Tempe, AZ 85288", distanceMiles: 4.6 },
  ],
  bevmo: [],
  kroger: [],
};
