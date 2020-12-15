module.exports = {
  PG_CACHE_DAYS: 90, // days of logs to import into cache

  ATHENA_OUTPUT_BUCKET: 'ml-fusion-cache',
  ATHENA_WORKGROUP: 'locus_ml', // use to segregate billing and history

  ONE_HOUR_MS: 60 * 60 * 1000,
  ONE_DAY_MS: 24 * 60 * 60 * 1000,

  CU_AGENCY: 1, // customer type enum
  CU_ADVERTISER: 2, // customer type enum

  ACCESS_INTERNAL: 1, // access type enum
  ACCESS_CUSTOMER: 2, // access type enum
  ACCESS_PRIVATE: 3, // access type enum - not exposed to UI

  ATOM_CONNECTION_NAME: 'locus_atom_fdw',
}
