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

  // CAT_NUMERIC
  ['integer', CAT_NUMERIC],
  ['money', CAT_NUMERIC],
  ['numeric', CAT_NUMERIC],
  ['double precision', CAT_NUMERIC],
  ['small int', CAT_NUMERIC],
  ['big int', CAT_NUMERIC],

  // CAT_DATE
  ['date', CAT_DATE],

  // CAT_JSON
  ['json', CAT_JSON],
  ['jsonb', CAT_JSON],

  // CAT_BOOL
  ['boolean', CAT_BOOL],

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
