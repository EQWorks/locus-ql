const { types } = require('pg')

// categories for data types
const CAT_STRING = 'String'
const CAT_NUMERIC = 'Numeric'
const CAT_DATE = 'Date'
const CAT_JSON = 'JSON'
const CAT_BOOL = 'Boolean'
const CAT_GEOMETRY = 'Geometry'

// parquet types
const PRQ_STRING = 'UTF8'
const PRQ_DOUBLE = 'DOUBLE'
const PRQ_FLOAT = 'FLOAT'
const PRQ_INT = 'INT64'
const PRQ_DATE = 'TIMESTAMP_MILLIS'
const PRQ_JSON = 'JSON'
const PRQ_BOOL = 'BOOLEAN'

const typeToCatMap = new Map([
  // CAT_STRING
  ['text', CAT_STRING],
  ['character varying', CAT_STRING],
  ['varchar', CAT_STRING],
  ['char', CAT_STRING],
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
  ['double', CAT_NUMERIC],
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
  ['timestamp', CAT_DATE],
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

const typeToPrqMap = new Map([
  // PRQ_STRING
  ['text', PRQ_STRING],
  ['character varying', PRQ_STRING],
  [types.builtins.CHAR, PRQ_STRING],
  [types.builtins.TEXT, PRQ_STRING],
  [types.builtins.VARCHAR, PRQ_STRING],

  // NUMERIC
  ['integer', PRQ_INT],
  ['money', PRQ_DOUBLE],
  ['numeric', PRQ_DOUBLE],
  ['decimal', PRQ_DOUBLE],
  ['real', PRQ_DOUBLE],
  ['double precision', PRQ_DOUBLE],
  ['small int', PRQ_INT],
  ['smallint', PRQ_INT],
  ['big int', PRQ_INT],
  ['bigint', PRQ_INT],
  ['serial', PRQ_INT],
  ['bigserial', PRQ_INT],
  [types.builtins.INT2, PRQ_INT],
  [types.builtins.INT4, PRQ_INT],
  [types.builtins.INT8, PRQ_INT],
  [types.builtins.FLOAT4, PRQ_FLOAT],
  [types.builtins.FLOAT8, PRQ_FLOAT],
  [types.builtins.MONEY, PRQ_DOUBLE],
  [types.builtins.NUMERIC, PRQ_DOUBLE],
  [types.builtins.OID, PRQ_DOUBLE],

  // PRQ_DATE
  ['date', PRQ_DATE],
  [types.builtins.DATE, PRQ_DATE],
  [types.builtins.TIMESTAMP, PRQ_DATE],
  [types.builtins.TIMESTAMPTZ, PRQ_DATE],

  // PRQ_JSON
  ['json', PRQ_JSON],
  ['jsonb', PRQ_JSON],
  [types.builtins.JSON, PRQ_JSON],
  [types.builtins.JSONB, PRQ_JSON],

  // PRQ_BOOL
  ['boolean', PRQ_BOOL],
  [types.builtins.BOOL, PRQ_BOOL],

  // ...others like postgis stuff
  // ['geometry', PRQ_GEOMETRY],

  // connection hub types:
  ['Mobile Ad ID', PRQ_STRING],
  ['IP', PRQ_STRING],
  ['Number', PRQ_DOUBLE],
  ['Timestamp (UTC)', PRQ_DATE],
  ['JSON', PRQ_JSON],
  ['email', PRQ_STRING],
  ['string', PRQ_STRING],
])

module.exports = {
  CAT_STRING,
  CAT_NUMERIC,
  CAT_DATE,
  CAT_JSON,
  CAT_BOOL,
  PRQ_STRING,
  typeToCatMap,
  typeToPrqMap,
}
