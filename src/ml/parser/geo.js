/* eslint-disable no-continue */
const { useAPIErrorOptions } = require('../../util/api-error')
const { geometryTypes: geoTypes, geometryTypeValues } = require('./src/geometries')


const { apiError } = useAPIErrorOptions({ tags: { service: 'ql', module: 'parser' } })

const geoTypeTables = {
  [geoTypes.CA_FSA]: {
    schema: 'canada_geo',
    table: 'fsa_simplified',
    // idType: CAT_STRING,
    idColumn: 'fsa',
    geometryColumn: 'wkb_geometry',
    intersectionQueryType: 'fsa',
    intersectionSourceType: 'fsa',
  },
  [geoTypes.CA_DA]: {
    schema: 'canada_geo',
    table: 'da',
    // idType: CAT_NUMERIC,
    idColumn: 'dauid',
    geometryColumn: 'wkb_geometry',
    intersectionSourceType: 'da',
  },
  [geoTypes.CA_CT]: {
    schema: 'canada_geo',
    table: 'ct',
    // idType: CAT_STRING,
    idColumn: 'ctuid',
    geometryColumn: 'wkb_geometry',
    intersectionSourceType: 'ct',
  },
  [geoTypes.CA_CSD]: {
    schema: 'canada_geo',
    table: 'csd',
    // idType: CAT_NUMERIC,
    idColumn: 'gid',
    geometryColumn: 'geom',
  },
  [geoTypes.CA_POSTALCODE]: {
    schema: 'canada_geo',
    table: 'postalcode_simplified',
    // idType: CAT_STRING,
    idColumn: 'postalcode',
    geometryColumn: 'wkb_geometry',
    intersectionQueryType: 'postalcode',
  },
  [geoTypes.CA_PROVINCE]: {
    schema: 'canada_geo',
    table: 'province',
    // idType: CAT_STRING,
    idColumn: 'province_code',
    geometryColumn: 'wkb_geometry',
  },
  [geoTypes.CA_CITY]: {
    schema: 'canada_geo',
    table: 'city_dev',
    // idType: CAT_STRING,
    idColumn: 'name',
    geometryColumn: "geo->>'geom'",
  },
  [geoTypes.POI]: {
    schema: 'public',
    table: 'poi',
    // idType: CAT_NUMERIC,
    idColumn: 'poi_id',
    geometryColumn: 'polygon',
    latColumn: 'lat',
    longColumn: 'lon',
    radiusColumn: 'default_radius',
    whitelabelColumn: 'whitelabelid',
    customerColumn: 'customerid',
  },
}

// geo with table
const getKnownGeometry = (geo, { whitelabelID, customerID, engine = 'pg' } = {}) => {
  const type = geoTypeTables[geo.type]
  if (!type) {
    return
  }
  const geometries = []
  if (type.geometryColumn) {
    geometries.push(engine === 'trino'
      ? `ST_GeomFromBinary(${type.geometryColumn})`
      : `ST_MakeValid(${type.geometryColumn})`)
  }
  if (type.longColumn && type.latColumn) {
    geometries.push(engine === 'trino'
      // trino
      ? `
        ST_Buffer(
          ST_Point(${type.longColumn}, ${type.latColumn}),
          ${type.radiusColumn ? `${type.radiusColumn}` : '500'}
        )
      `
      // pg
      : `
        ST_Transform(
          ST_Buffer(
            ST_Transform(
              ST_SetSRID(ST_Point(${type.longColumn}, ${type.latColumn}), 4326),
              3347
            ),
            ${type.radiusColumn ? `${type.radiusColumn}` : '500'}
          ),
          4326
        )
      `)
  }
  if (!geometries.length) {
    throw apiError('Geometry is not retrievable')
  }
  return `
    SELECT
      COALESCE(${geometries.join(', ')}) AS geometry
    FROM ${engine === 'trino' ? 'locus_place.' : ''}${type.schema}.${type.table}
    WHERE
      ${type.idColumn} = ${geo.id}
      ${whitelabelID && type.whitelabelColumn
    ? `AND ${type.whitelabelColumn} = ${whitelabelID}`
    : ''}
      ${customerID && type.customerColumn ? `AND ${type.customerColumn} = ${customerID}` : ''}
  `
}

const getPointGeometry = (geo, { engine } = {}) => {
  if (geo.type !== geoTypes.POINT) {
    return
  }
  return engine === 'trino'
    // trino
    ? `
      ST_Buffer(
        ST_Point(CAST(${geo.long} AS double precision), CAST(${geo.lat} AS double precision)),
        ${geo.radius ? `${geo.radius}` : '500'}
      )
    `
    // pg
    : `
      ST_Transform(
        ST_Buffer(
          ST_Transform(
            ST_SetSRID(
              ST_Point(CAST(${geo.long} AS double precision), CAST(${geo.lat} AS double precision)),
              4326
            ),
            3347
          ),
          ${geo.radius ? `${geo.radius}` : '500'}
        ),
        4326
      )
    `
}

const getGGIDGeometry = (geo, { engine = 'pg' } = {}) => {
  if (geo.type !== geoTypes.GGID) {
    return
  }
  return `
    SELECT
      CASE g.type
        WHEN 'fsa' THEN (
          ${getKnownGeometry({ type: geoTypes.CA_FSA, id: 'g.local_id' }, { engine })}
        )
        WHEN 'postalcode' THEN (
          ${getKnownGeometry({ type: geoTypes.CA_POSTALCODE, id: 'g.local_id' }, { engine })}
        )
        WHEN 'ct' THEN (
          ${getKnownGeometry({ type: geoTypes.CA_CT, id: 'g.local_id' }, { engine })}
        )
        WHEN 'da' THEN (
          ${getKnownGeometry({ type: geoTypes.CA_DA, id: 'g.local_id' }, { engine })}
        )
      END AS geometry
    FROM ${engine === 'trino' ? 'locus_place.' : ''}config.ggid_map g
    WHERE g.ggid = ${geo.id}
  `
}

const getKnownIntersectionGeometry = (geoA, geoB, { engine = 'pg' } = {}) => {
  const typeA = geoTypeTables[geoA.type]
  const typeB = geoTypeTables[geoB.type]
  if (!typeA || !typeB) {
    return
  }
  let query
  let source
  if (typeA.intersectionQueryType && typeB.intersectionSourceType) {
    query = { type: typeA.intersectionQueryType, id: geoA.id }
    source = { type: typeB.intersectionQueryType, id: geoB.id }
  } else if (typeA.intersectionSourceType && typeB.intersectionQueryType) {
    query = { type: typeB.intersectionQueryType, id: geoB.id }
    source = { type: typeA.intersectionQueryType, id: geoA.id }
  } else {
    // can't use intersection table
    return
  }
  const geometry = engine === 'trino'
    ? 'ST_GeomFromBinary(intersect_geometry)'
    : 'intersect_geometry'
  return `
    SELECT
      ${geometry} AS geometry
    FROM ${engine === 'trino' ? 'locus_place.' : ''}canada_geo.intersection
    WHERE
      query_geo_type = '${query.type}'
      AND query_geo_id = ${query.id}
      AND source_geo_type = '${source.type}'
      AND source_geo_id = ${source.id}
  `
}

const getGeometry = (geo, options) => {
  if (geo.type in geoTypeTables) {
    return getKnownGeometry(geo, options)
  }
  if (geo.type === geoTypes.POINT) {
    return getPointGeometry(geo, options)
  }
  if (geo.type === geoTypes.GGID) {
    return getGGIDGeometry(geo, options)
  }
  throw apiError('Invalid geometry')
}

const getIntersectionGeometry = (geoA, geoB, options) => {
  // use pre-computed if available
  const preComputed = getKnownIntersectionGeometry(geoA, geoB)
  if (preComputed) {
    return preComputed
  }
  // get individual geometries
  const [geomA, geomB] = [geoA, geoB].map(geo => getGeometry(geo, options))
  return `
    SELECT
      i.geometry
    FROM (
      SELECT ST_Intersection((${geomA}), (${geomB})) AS geometry
    ) AS i
    WHERE NOT ST_IsEmpty(i.geometry)
  `
}

// sql: "'geo:<type>:' || (arg)[ || ':' || (arg)]"
const extractGeoSQLValues = (sql) => {
  const vals = []
  let val = ''
  let valEnding = false
  let quote = ''
  let quoteEnding = false
  let inBlock = false
  let blockDepth = 0
  for (const char of sql) {
    // quote in progress
    if (quote) {
      // quote continues
      if (!quoteEnding || char === quote) {
        if (char === quote) {
          quoteEnding = !quoteEnding
        }
        val += char
        continue
      }
      // quote ends, need to deal with char
      quote = ''
      quoteEnding = false
    }

    if (valEnding) {
      valEnding = false
      // val ended
      if (char === '|') {
        vals.push(val)
        val = ''
        continue
      }
      // val continues
      val += '|'
    }

    // no quote or quote has just ended
    // new quote starting
    if (char === '"' || char === "'") {
      quote = char
      val += char
      continue
    }

    if (!inBlock) {
      // end of val
      if (char === '|') {
        valEnding = true
        continue
      }
      // new block starting
      if (char === '(') {
        inBlock = true
      }
      val += char
      continue
    }

    // block in progress
    // nested block
    if (char === '(') {
      blockDepth += 1
    } else if (char === ')') {
      // end block (main or nested)
      if (!blockDepth) {
        inBlock = false
      } else {
        blockDepth -= 1
      }
    }
    val += char
  }
  // last val
  if (val) {
    vals.push(val)
  }
  if (vals.length < 2) {
    return []
  }
  return [
    ...vals[0].slice(1, -3).split(':'),
    ...vals.slice(1).filter(v => v !== " ':' ").map(v => v.trim()),
  ]
}

const parseGeoSQL = (sql) => {
  const [check, type, ...args] = extractGeoSQLValues(sql)
  if (check !== 'geo' || !(type in geometryTypeValues)) {
    throw apiError('Invalid geometry', 400)
  }
  // geo with id
  if (type in geoTypeTables || type === geoTypes.GGID) {
    if (args.length !== 1) {
      throw apiError(`Invalid ${type} geometry`, 400)
    }
    return { type, id: args[0] }
  }
  // point
  if (args.length !== 2) {
    throw apiError('Invalid point geometry', 400)
  }
  return { type, long: args[0], lat: args[1], radius: args[2] }
}

const geoIntersectsParser = engine => (node, options) => {
  const { whitelabelID, customerID } = options
  const [geoA, geoB] = node.args.map(e => parseGeoSQL(e.to(engine, options)))
  const geometry = getIntersectionGeometry(geoA, geoB, { whitelabelID, customerID, engine })
  return `EXISTS (${geometry})`
}

const geoIntersectionAreaParser = engine => (node, options) => {
  const { whitelabelID, customerID } = options
  const [geoA, geoB] = node.args.map(e => parseGeoSQL(e.to(engine, options)))
  const geometry = getIntersectionGeometry(geoA, geoB, { whitelabelID, customerID, engine })
  return `ST_Area((${geometry}))`
}

const geoIntersectionAreaRatioParser = engine => (node, options) => {
  const { whitelabelID, customerID } = options
  const [geoA, geoB] = node.args.map(e => parseGeoSQL(e.to(engine, options)))
  const numerator = getIntersectionGeometry(geoA, geoB, { whitelabelID, customerID, engine })
  const denominator = getGeometry(geoB, { whitelabelID, customerID, engine })
  return `ST_Area((${numerator})) / ST_Area((${denominator}))`
}

const geoAreaParser = engine => (node, options) => {
  const { whitelabelID, customerID } = options
  const [geo] = node.args.map(e => parseGeoSQL(e.to(engine, options)))
  const geometry = getGeometry(geo, { whitelabelID, customerID, engine })
  return `ST_Area((${geometry}))`
}

module.exports = {
  geoIntersectsParser,
  geoIntersectionAreaParser,
  geoIntersectionAreaRatioParser,
  geoAreaParser,
}
