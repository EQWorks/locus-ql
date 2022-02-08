// const { parserError, sanitizeString } = require('./utils')
const { castTypes } = require('./types')
// const { geometries } = require('./geometries')
// const PrimitiveNode = require('./tree/primitive')


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
  // validate: (node) => {
  //   const [type, ...args] = node.args
  //   // geo type must be known at parsing time
  //   if (!(type instanceof PrimitiveNode)) {
  //     throw parserError("Geometry type must be a string in function 'geometry'")
  //   }
  //   type.value = sanitizeString(type.value)
  //   const geometry = geometries[type.value]
  //   if (!geometry) {
  //     throw parserError(`Invalid geometry type: ${type.value}`)
  //   }
  //   const { argsLength, minArgsLength, maxArgsLength } = geometry
  //   if (
  //     argsLength !== undefined
  //       ? args.length !== argsLength
  //       : (
  //         (minArgsLength && args.length < minArgsLength)
  //         || (maxArgsLength !== undefined && args.length > maxArgsLength)
  //       )
  //   ) {
  //     throw parserError(`Too few or too many arguments in geometry: ${type.value}`)
  //   }
  // },
}

functions.geo = functions.geometry

functions.geo_intersects = { // geo_intersect(geoA, geoB)
  argsLength: 2,
}
functions.geo_intersection_area = { // geo_intersection_area(geoA, geoB)
  argsLength: 2,
}
functions.geo_intersection_area_ratio = { // geo_intersection_area(geoA, geoB)
  argsLength: 2,
}
functions.geo_area = { // geo_area(geoA)
  argsLength: 1,
}
functions.geo_distance = { // geo_distance(geoA, geoB)
  argsLength: 2,
}

module.exports = functions
