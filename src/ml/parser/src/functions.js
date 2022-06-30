const { castTypes } = require('./types')


const functions = {
  nullif: { argsLength: 2 },
  coalesce: { minArgsLength: 1 },

  // aggregate functions
  count: {
    defaultCast: castTypes.INTEGER,
    argsLength: 1,
    isAggregate: true,
  },
  sum: {
    defaultCast: castTypes.FLOAT,
    argsLength: 1,
    isAggregate: true,
  },
  avg: {
    defaultCast: castTypes.FLOAT,
    argsLength: 1,
    isAggregate: true,
  },
  min: {
    defaultCast: castTypes.FLOAT,
    argsLength: 1,
    isAggregate: true,
  },
  max: {
    defaultCast: castTypes.FLOAT,
    argsLength: 1,
    isAggregate: true,
  },
  array_agg: {
    argsLength: 1,
    isAggregate: true,
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
}

// math functions
functions.abs = { argsLength: 1 }
functions.cbrt = { argsLength: 1 }
functions.ceil = { argsLength: 1 }
functions.ceiling = { argsLength: 1 }
functions.exp = { argsLength: 1 }
functions.floor = { argsLength: 1 }
functions.ln = { argsLength: 1 }
functions.log = { // log(val) or log(base, val)
  minArgsLength: 1,
  maxArgsLength: 2,
}
functions.mod = { argsLength: 2 }
functions.pi = { argsLength: 0 }
functions.pow = { argsLength: 2 }
functions.power = { argsLength: 2 }
functions.rand = { argsLength: 0 }
functions.random = { argsLength: 0 }
functions.round = { // round(val) or round(val, decimal places)
  minArgsLength: 1,
  maxArgsLength: 2,
}
functions.sign = { argsLength: 1 }
functions.sqrt = { argsLength: 1 }
functions.trunc = { argsLength: 1 }
functions.truncate = { argsLength: 1 }

// JSON functions
functions.json_extract_path = { // json_extract_path(field, ...path)
  minArgsLength: 2,
}
functions.json_extract_path_text = { // json_extract_path_text(field, ...path)
  minArgsLength: 2,
}

// type cast functions
Object.values(castTypes).forEach((cast) => {
  functions[`cast_${cast}`] = {
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
functions.substr = {
  minArgsLength: 2,
  maxArgsLength: 3,
}

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
// geo_json deactivated at this time
// functions.geo_json = { // geo_json(geoStringA)
//   argsLength: 1,
// }
functions.geo_distance = { // geo_distance(geoStringA, geoStringB)
  argsLength: 2,
}

module.exports = functions
