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
    minArgsLength: 1,
    maxArgsLength: 2,
  },

  // time/date functions
  // field can be year, month, day, hour etc
  date: {
    argsLength: 1,
    // category: CAT_NUMERIC,
  },
  date_part: { // date_part(field, timestamp)
    argsLength: 2,
    // category: CAT_NUMERIC,
  },
  date_trunc: { // date_trunc(field, timestamp)
    argsLength: 2,
    // category: CAT_DATE,
  },

  // JSON functions
  json_extract_path: { // json_extract_path(field, keys)
    // category: CAT_JSON,
    minArgsLength: 2,
  },

  // Geo functions
  geo_intersects: { // geo_intersect(geoA, geoB)
    argsLength: 2,
  },
  geo_intersection_area: { // geo_intersection_area(geoA, geoB)
    argsLength: 2,
  },
  geo_intersection_area_ratio: { // geo_intersection_area(geoA, geoB)
    argsLength: 2,
  },
  geo_area: { // geo_area(geoA)
    argsLength: 1,
  },
  geo_distance: { // geo_distance(geoA, geoB)
    argsLength: 2,
  },
}
