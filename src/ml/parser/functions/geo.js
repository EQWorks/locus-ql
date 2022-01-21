/* eslint-disable no-continue */
const { useAPIErrorOptions } = require('../../../util/api-error')
const { geometryTypes: geoTypes, geometryTypeValues } = require('../src/geometries')


const { apiError } = useAPIErrorOptions({ tags: { service: 'ql', module: 'parser' } })

const DEFAULT_POINT_RADIUS = 500 // 500 metres

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

const getGeoMeta = (geo, { engine = 'pg' } = {}) => (geo.type === geoTypes.GGID
  ? `
    SELECT
      type,
      local_id AS id
    FROM ${engine === 'trino' ? 'locus_place.' : ''}config.ggid_map
    WHERE ggid = ${geo.id}
  `
  : `
    SELECT
      '${geo.type.replace('ca-', '')}' AS type,
      ${geo.id || 'NULL'} AS id
  `)

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
          ${type.radiusColumn !== undefined ? type.radiusColumn : DEFAULT_POINT_RADIUS}
        )
      `
      // pg
      : `
        ST_Buffer(
          ST_SetSRID(ST_Point(${type.longColumn}, ${type.latColumn}), 4326),
          ${type.radiusColumn !== undefined ? type.radiusColumn : DEFAULT_POINT_RADIUS}
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

const getPointGeometry = (geo, { engine = 'pg' } = {}) => {
  if (geo.type !== geoTypes.POINT) {
    return
  }
  const geometry = engine === 'trino'
    // trino
    ? `
      ST_Buffer(
        ST_Point(CAST(${geo.long} AS double), CAST(${geo.lat} AS double)),
        ${geo.radius !== undefined ? geo.radius : DEFAULT_POINT_RADIUS}
      )
    `
    // pg
    : `
      ST_Buffer(
        ST_SetSRID(
          ST_Point(CAST(${geo.long} AS double precision), CAST(${geo.lat} AS double precision)),
          4326
        ),
        ${geo.radius !== undefined ? geo.radius : DEFAULT_POINT_RADIUS}
      )
    `
  return `SELECT ${geometry} AS geometry`
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

const getCalculatedIntersectionGeometry = (geoA, geoB, options) => {
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

// one of the geo's has type GGID
const getGGIDKnownIntersectionGeometry = (geoA, geoB, { engine = 'pg' } = {}) => {
  // no need to try if one of the types not in getTables
  const typesWithKnownIntersections = [
    geoTypes.CA_FSA,
    geoTypes.CA_POSTALCODE,
    geoTypes.CA_CT,
    geoTypes.CA_DA,
    geoTypes.GGID,
  ]
  if (
    (geoA.type !== geoTypes.GGID || !typesWithKnownIntersections.includes(geoB.type))
    && (geoB.type !== geoTypes.GGID || !typesWithKnownIntersections.includes(geoA.type))
  ) {
    return
  }
  const geoAMeta = getGeoMeta(geoA, { engine })
  const geoBMeta = getGeoMeta(geoB, { engine })
  const geometry = engine === 'trino'
    ? 'ST_GeomFromBinary(intersect_geometry)'
    : 'intersect_geometry'

  return `
    WITH geo_a AS (${geoAMeta}),
    geo_b AS (${geoBMeta})
    SELECT
      CASE
        WHEN (SELECT type FROM geo_a) = (SELECT type FROM geo_b) THEN (
          SELECT
            (${getGeometry(geoA, { engine })})
          WHERE (SELECT id FROM geo_a) = (SELECT id FROM geo_b)
        )
        WHEN
          (SELECT type FROM geo_a) IN ('fsa', 'postalcode')
          AND (SELECT type FROM geo_b) IN ('fsa', 'ct', 'da')
        THEN (
          SELECT
            ${geometry}
          FROM ${engine === 'trino' ? 'locus_place.' : ''}canada_geo.intersection
          WHERE
            query_geo_type = (SELECT type FROM geo_a)
            AND query_geo_id = (SELECT id FROM geo_a)
            AND source_geo_type = (SELECT type FROM geo_b)
            AND source_geo_id = (SELECT id FROM geo_b)
        )
        WHEN
          (SELECT type FROM geo_b) IN ('fsa', 'postalcode')
          AND (SELECT type FROM geo_a) IN ('fsa', 'ct', 'da')
        THEN (
          SELECT
            ${geometry}
          FROM ${engine === 'trino' ? 'locus_place.' : ''}canada_geo.intersection
          WHERE
            query_geo_type = (SELECT type FROM geo_b)
            AND query_geo_id = (SELECT id FROM geo_b)
            AND source_geo_type = (SELECT type FROM geo_a)
            AND source_geo_id = (SELECT id FROM geo_a)
        )
        ELSE (${getCalculatedIntersectionGeometry(geoA, geoB, { engine })})
        END AS geometry
  `
}

const getSameTypeIntersectionGeometry = (geoA, geoB, options) => {
  if (geoA.type !== geoB.type) {
    return
  }
  return geoA.type in geoTypeTables || geoA.type === geoTypes.GGID
    ? `
      SELECT
        (${getGeometry(geoA, options)})
      WHERE ${geoA.id} = ${geoB.id}
    `
    : `
      SELECT
        i.geometry
      FROM (
        SELECT
          CASE
            WHEN
              ${geoA.radius !== undefined ? geoA.radius : DEFAULT_POINT_RADIUS}
              <= ${geoB.radius !== undefined ? geoB.radius : DEFAULT_POINT_RADIUS}
            THEN (${getGeometry(geoA, options)})
            ELSE (${getGeometry(geoB, options)})
          END AS geometry
        WHERE
          ${geoA.long} = ${geoB.long}
          AND ${geoA.lat} = ${geoB.lat}
      ) AS i
      WHERE NOT ST_IsEmpty(i.geometry)
    `
}

const getIntersectionGeometry = (geoA, geoB, options) =>
  getSameTypeIntersectionGeometry(geoA, geoB, options) // same geo type
  || getKnownIntersectionGeometry(geoA, geoB, options) // ggid + types in intersection tbl
  || getGGIDKnownIntersectionGeometry(geoA, geoB, options) // types in intersection tbl
  || getCalculatedIntersectionGeometry(geoA, geoB, options) // compute on demand

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
  if (args.length !== 2 && args.length !== 3) {
    throw apiError('Invalid point geometry', 400)
  }
  return { type, long: args[0], lat: args[1], radius: args[2] }
}

const geoIntersectsParser = engine => (node, options) => {
  const { whitelabelID, customerID } = options
  const [geoA, geoB] = node.args.map(e => parseGeoSQL(e.to(engine, options)))
  const geometry = getIntersectionGeometry(geoA, geoB, { whitelabelID, customerID, engine })
  return `SELECT EXISTS (${geometry}) AS geo_intersects`
}

const geoIntersectionAreaParser = engine => (node, options) => {
  const { whitelabelID, customerID } = options
  const [geoA, geoB] = node.args.map(e => parseGeoSQL(e.to(engine, options)))
  const geometry = getIntersectionGeometry(geoA, geoB, { whitelabelID, customerID, engine })
  return `SELECT ST_Area((${geometry})) AS geo_intersection_area`
}

const geoIntersectionAreaRatioParser = engine => (node, options) => {
  const { whitelabelID, customerID } = options
  const [geoA, geoB] = node.args.map(e => parseGeoSQL(e.to(engine, options)))
  const numerator = getIntersectionGeometry(geoA, geoB, { whitelabelID, customerID, engine })
  const denominator = getGeometry(geoB, { whitelabelID, customerID, engine })
  return `SELECT ST_Area((${numerator})) / ST_Area((${denominator})) AS geo_intersection_area_ratio`
}

const geoAreaParser = engine => (node, options) => {
  const { whitelabelID, customerID } = options
  const [geo] = node.args.map(e => parseGeoSQL(e.to(engine, options)))
  const geometry = getGeometry(geo, { whitelabelID, customerID, engine })
  return `SELECT ST_Area((${geometry})) AS geo_area`
}

module.exports = {
  geoIntersectsParser,
  geoIntersectionAreaParser,
  geoIntersectionAreaRatioParser,
  geoAreaParser,
}
