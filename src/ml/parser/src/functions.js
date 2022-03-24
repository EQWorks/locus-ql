const { castTypes } = require('./types')


const functions = {
  nullif: { argsLength: 2 },
  coalesce: { minArgsLength: 1 },

  // aggregation functions
  sum: {
    // category: CAT_NUMERIC,
    defaultCast: castTypes.FLOAT,
    argsLength: 1,
  },
  count: { argsLength: 1 },
  avg: {
    // category: CAT_NUMERIC,
    defaultCast: castTypes.FLOAT,
    argsLength: 1,
  },
  min: {
    // category: CAT_NUMERIC,
    defaultCast: castTypes.FLOAT,
    argsLength: 1,
  },
  max: {
    // category: CAT_NUMERIC,
    defaultCast: castTypes.FLOAT,
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

  // date_part: { // date_part(field, timestamp)
  //   argsLength: 2,
  //   // category: CAT_NUMERIC,
  // },
  // date_trunc: { // date_trunc(field, timestamp)
  //   argsLength: 2,
  //   // category: CAT_DATE,
  // },

  // JSON functions
  json_extract_path: { // json_extract_path(field, keys)
    // category: CAT_JSON,
    minArgsLength: 2,
  },
}

// type cast functions
Object.values(castTypes).forEach((cast) => {
  functions[cast] = {
    argsLength: 1,
    validate: (node) => {
      node.args[0]._validateCastAndAliasLayer(cast)
      node._populateCastAndAliasProxies(node.args[0])
    },
  }
})

// String/text functions
functions.lower = { argsLength: 1 }
functions.upper = { argsLength: 1 }
functions.trim = { argsLength: 1 }
functions.ltrim = { argsLength: 1 }
functions.rtrim = { argsLength: 1 }
functions.lpad = { argsLength: 3 }
functions.rpad = { argsLength: 3 }
functions.length = { argsLength: 1 }
functions.replace = { argsLength: 3 }

// Date/time functions
functions.date = { argsLength: 1 }
functions.time = { argsLength: 1 }
functions.datetime = { argsLength: 1 }
functions.timestamptz = functions.datetime
functions.timedelta = { argsLength: 2 }

// Geo functions
functions.geometry = {
  minArgsLength: 2,
  maxArgsLength: 4,
}

functions.geo = functions.geometry
functions.geo_intersects = { // geo_intersect(geoStringA, geoStringB)
  argsLength: 2,
}
functions.geo_intersection_area = { // geo_intersection_area(geoStringA, geoStringB)
  argsLength: 2,
}
functions.geo_area = { // geo_area(geoStringA)
  argsLength: 1,
}
functions.geo_json = { // geo_json(geoStringA)
  argsLength: 1,
}
functions.geo_distance = { // geo_distance(geoStringA, geoStringB)
  argsLength: 2,
}

module.exports = functions
