const { types } = require('pg')

// categories for data types
const CAT_STRING = 'String'
const CAT_NUMERIC = 'Numeric'
const CAT_DATE = 'Date'
const CAT_JSON = 'JSON'
const CAT_BOOL = 'Boolean'
const CAT_GEOMETRY = 'Geometry'

const typeToCatMap = new Map([
  // CAT_STRING
  ['text', CAT_STRING],
  ['character varying', CAT_STRING],
  [types.builtins.CHAR, CAT_STRING],
  [types.builtins.TEXT, CAT_STRING],
  [types.builtins.VARCHAR, CAT_STRING],

  // CAT_NUMERIC
  ['integer', CAT_NUMERIC],
  ['money', CAT_NUMERIC],
  ['numeric', CAT_NUMERIC],
  ['decimal', CAT_NUMERIC],
  ['real', CAT_NUMERIC],
  ['double precision', CAT_NUMERIC],
  ['small int', CAT_NUMERIC],
  ['smallint', CAT_NUMERIC],
  ['big int', CAT_NUMERIC],
  ['bigint', CAT_NUMERIC],
  ['serial', CAT_NUMERIC],
  ['bigserial', CAT_NUMERIC],
  [types.builtins.INT2, CAT_NUMERIC],
  [types.builtins.INT4, CAT_NUMERIC],
  [types.builtins.INT8, CAT_NUMERIC],
  [types.builtins.FLOAT4, CAT_NUMERIC],
  [types.builtins.FLOAT8, CAT_NUMERIC],
  [types.builtins.MONEY, CAT_NUMERIC],
  [types.builtins.NUMERIC, CAT_NUMERIC],
  [types.builtins.OID, CAT_NUMERIC],

  // CAT_DATE
  ['date', CAT_DATE],
  [types.builtins.DATE, CAT_DATE],
  [types.builtins.TIMESTAMP, CAT_DATE],
  [types.builtins.TIMESTAMPTZ, CAT_DATE],

  // CAT_JSON
  ['json', CAT_JSON],
  ['jsonb', CAT_JSON],
  [types.builtins.JSON, CAT_JSON],
  [types.builtins.JSONB, CAT_JSON],

  // CAT_BOOL
  ['boolean', CAT_BOOL],
  [types.builtins.BOOL, CAT_BOOL],

  // ...others like postgis stuff
  ['geometry', CAT_GEOMETRY],

  // TODO:
  // 1) standarize these types:
  // 2) keep up to date with connection hub
  // connection hub types:
  ['Mobile Ad ID', CAT_STRING],
  ['IP', CAT_STRING],
  ['Number', CAT_NUMERIC],
  ['Timestamp (UTC)', CAT_DATE],
  ['JSON', CAT_JSON],
  ['email', CAT_STRING],
  ['string', CAT_STRING],
])

module.exports = {
  CAT_STRING,
  CAT_NUMERIC,
  CAT_DATE,
  CAT_JSON,
  CAT_BOOL,
  typeToCatMap,
}
