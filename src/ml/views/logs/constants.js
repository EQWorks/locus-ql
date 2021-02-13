module.exports = {
  PG_CACHE_DAYS: 90, // days of logs to import into cache

  ONE_HOUR_MS: 60 * 60 * 1000,
  ONE_DAY_MS: 24 * 60 * 60 * 1000,

  CU_AGENCY: 1, // customer type enum
  CU_ADVERTISER: 2, // customer type enum

  ACCESS_CUSTOMER: 1, // access type enum
  ACCESS_INTERNAL: 2, // access type enum
  ACCESS_PRIVATE: 3, // access type enum - not exposed to UI

  ATOM_CONNECTION_NAME: 'locus_atom_fdw',

  ML_SCHEMA: process.env.ML_SCHEMA,
}
