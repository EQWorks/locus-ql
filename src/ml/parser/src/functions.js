module.exports = {
  any: { argsLength: 1 },
  all: { argsLength: 1 },
  nullif: { argsLength: 2 },
  coalesce: { minArgsLength: 1 },
  // aggregation functions
  sum: {
    // category: CAT_NUMERIC,
    defaultCast: 'real',
    argsLength: 1,
  },
  count: { argsLength: 1 },
  avg: {
    // category: CAT_NUMERIC,
    defaultCast: 'real',
    argsLength: 1,
  },
  min: {
    // category: CAT_NUMERIC,
    defaultCast: 'real',
    argsLength: 1,
  },
  max: {
    // category: CAT_NUMERIC,
    defaultCast: 'real',
    argsLength: 1,
  },
  // min_date: {
  //   value: 'min',
  //   category: CAT_NUMERIC,
  //   defaultCast: 'date',
  // },
  // max_date: {
  //   value: 'max',
  //   category: CAT_NUMERIC,
  //   defaultCast: 'date',
  // },

  round: {
    // category: CAT_NUMERIC,
    argsLength: 1,
  },

  // time/date functions
  // field can be year, month, day, hour etc
  date_part: { // date_part(field, timestamp)
    value: 'date_part',
    // category: CAT_NUMERIC,
  },
  date_trunc: { // date_trunc(field, timestamp)
    value: 'date_trunc',
    // category: CAT_DATE,
  },

  // JSON functions
  json_extract_path: { // json_extract_path(field, key)
    value: 'json_extract_path',
    // category: CAT_JSON,
  },
}
