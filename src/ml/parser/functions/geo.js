/* eslint-disable no-use-before-define */
/* eslint-disable no-continue */
const { useAPIErrorOptions } = require('../../../util/api-error')
const { geometryTypes: geoTypes, geometryTypeValues } = require('../src/types')
const geoTables = require('../../geo-tables')
const { escapeLiteral } = require('../src/utils')


const { apiError } = useAPIErrorOptions({ tags: { service: 'ql', module: 'parser' } })

const DEFAULT_POINT_RADIUS = 500 // 500 metres
const ggidTypes = [geoTypes.CA_CT, geoTypes.CA_DA, geoTypes.CA_FSA, geoTypes.CA_POSTALCODE]

const castAsInteger = (sql, engine = 'pg') => (engine === 'trino'
  ? `TRY_CAST(${sql} AS int)`
  : `CAST(substring(${sql} from '^\\d+$') AS int)`)

const castAsDouble = (sql, engine = 'pg') => (engine === 'trino'
  ? `TRY_CAST(${sql} AS double)`
  : `CAST(substring(${sql} from '^\\d+(?:\\.\\d+)?$') AS double precision)`)

const getGeoInfoNoGGID = (geo, { engine, getRef }) => {
  if (geo.type === geoTypes.GGID) {
    const ref = getRef()
    return `
      SELECT
        'ca-' || ${ref}.type AS type,
        ${ref}.local_id AS id,
        NULL as long,
        NULL as lat,
        NULL as radius
      FROM ${engine === 'trino' ? 'locus_place.' : ''}config.ggid_map AS ${ref}
      WHERE ${ref}.ggid = ${castAsInteger(geo.id, engine)}
    `
  }
  return `
    SELECT
      '${geo.type}' AS type,
      ${geo.id || 'NULL'} AS id,
      ${geo.long || 'NULL'} AS long,
      ${geo.lat || 'NULL'} AS lat,
      ${geo.radius || 'NULL'} AS radius
  `
}

const getGeoInfoFromString = (geoString, { engine, getRef }) => {
  const ref = getRef()
  // return `
  //   SELECT
  //     ${ref}[2] AS type,
  //     ${ref}[3] AS id,
  //     ${ref}[3] AS long,
  //     ${ref}[4] AS lat,
  //     COALESCE(${ref}[5], ${ref}[4]) AS radius
  //   FROM ${engine === 'trino' ? 'split' : 'regexp_split_to_array'}(${geoString}, ':') AS ${ref}
  //   WHERE
  //     ${ref}[1] = 'geo'
  //     AND ${ref}[2] IN (${Object.values(geoTypes).map(t => `'${t}'`).join(',')})
  // `
  return `
    SELECT
      ${ref}[2] AS type,
      CASE WHEN ${ref}[2] <> '${geoTypes.POINT}' THEN ${ref}[3] END AS id,
      CASE
        WHEN ${ref}[2] = '${geoTypes.POINT}' THEN ${ref}[3]
      END AS long,
      CASE
        WHEN ${ref}[2] = '${geoTypes.POINT}' THEN ${ref}[4]
      END AS lat,
      CASE ${ref}[2]
        WHEN '${geoTypes.POINT}' THEN ${ref}[5]
        ELSE ${ref}[4]
      END AS radius
    FROM ${engine === 'trino' ? 'split' : 'regexp_split_to_array'}(${geoString}, ':') AS ${ref}
    WHERE
      ${ref}[1] = 'geo'
      AND ${ref}[2] IN (${Object.values(geoTypes).map(t => `'${t}'`).join(',')})
  `
}

const getGeoInfoNoGGIDFromString = (geoString, { engine, getRef }) => {
  const geo = getRef()
  const ggid = getRef()
  return `
    SELECT
      COALESCE('ca-' || ${ggid}.type, ${geo}.type) AS type,
      COALESCE(${ggid}.local_id, ${geo}.id) AS id,
      ${geo}.long,
      ${geo}.lat,
      ${geo}.radius
    FROM (${getGeoInfoFromString(geoString, { engine, getRef })}) AS ${geo}
    LEFT JOIN ${engine === 'trino' ? 'locus_place.' : ''}config.ggid_map AS ${ggid} ON
      ${geo}.type = '${geoTypes.GGID}'
      AND ${ggid}.ggid = ${castAsInteger(`${geo}.id`, engine)}
    WHERE
      ${geo}.type <> '${geoTypes.GGID}'
      OR ${ggid}.local_id IS NOT NULL
  `
}

// geo with table
const getKnownGeometry = (geo, { whitelabelID, customerID, engine, getRef }) => {
  const type = geoTables[geo.type]
  if (!type) {
    return
  }
  const ref = getRef()
  const geometries = []
  const safeRadius = geo.radius !== undefined ? castAsInteger(geo.radius, engine) : undefined
  if (type.geometryColumn) {
    let geometry = engine === 'trino'
      // TODO: test if trino needs wrapping with 'from_hex' if
      // pg user-defined -> trino varchar (vs. varbinary)
      ? `ST_GeomFromBinary(${ref},${type.geometryColumn})`
      : `ST_Transform(${ref}.${type.geometryColumn}, 3347)`
    if (safeRadius) {
      geometry = `ST_Buffer(${geometry}, ${safeRadius})`
    }
    geometries.push(geometry)
  }
  if (type.longColumn && type.latColumn) {
    const radius = []
    if (safeRadius) {
      radius.push(safeRadius)
    }
    if (type.radiusColumn) {
      radius.push(`${ref}.${type.radiusColumn}`)
    }
    radius.push(DEFAULT_POINT_RADIUS)
    geometries.push(engine === 'trino'
      // trino
      ? `
        ST_Buffer(
          ST_Point(${ref}.${type.longColumn}, ${ref}.${type.latColumn}),
          COALESCE(${radius.join(', ')})
        )
      `
      // pg
      : `
        ST_Transform(
          ST_Buffer(
            ST_SetSRID(ST_Point(${ref}.${type.longColumn}, ${ref}.${type.latColumn}), 4326),
            COALESCE(${radius.join(', ')})
          ),
          3347
        )
      `)
  }
  if (!geometries.length) {
    throw apiError('Geometry is not retrievable')
  }
  const customerfilters = []
  if (whitelabelID && type.whitelabelColumn) {
    customerfilters.push(`
      (${ref}.${type.whitelabelColumn} = ${whitelabelID} OR ${ref}.${type.whitelabelColumn} IS NULL)
    `)
  }
  if (customerID && type.customerColumn) {
    customerfilters.push(`
      (${ref}.${type.customerColumn} = ${customerID} OR ${ref}.${type.customerColumn} IS NULL)
    `)
  }
  const accessFilters = []
  if (customerfilters.length) {
    accessFilters.push(`(${customerfilters.join(' AND ')})`)
  }
  if (type.publicColumn) {
    accessFilters.push(`${ref}.${type.publicColumn} IS TRUE`)
  }
  return `(
    SELECT
      COALESCE(${geometries.join(', ')}) AS geometry
    FROM ${engine === 'trino' ? 'locus_place.' : ''}${type.schema}.${type.table} AS ${ref}
    WHERE
      ${ref}.${type.idColumn} = ${type.idType === 'Numeric'
  ? castAsInteger(geo.id, engine) : geo.id}
      ${accessFilters.length ? `AND (${accessFilters.join(' OR ')})` : ''}
  )`
}

const getPointGeometry = (geo, { engine }) => {
  if (geo.type !== geoTypes.POINT) {
    return
  }
  const radius = []
  if (geo.radius !== undefined) {
    radius.push(castAsInteger(geo.radius, engine))
  }
  radius.push(DEFAULT_POINT_RADIUS)
  const geometry = engine === 'trino'
    // trino
    ? `
      ST_Buffer(
        ST_Point(${castAsDouble(geo.long, engine)}, ${castAsDouble(geo.lat, engine)}),
        COALESCE(${radius.join(', ')})
      )
    `
    // pg
    : `
      ST_Transform(
        ST_Buffer(
          ST_SetSRID(
            ST_Point(${castAsDouble(geo.long, engine)}, ${castAsDouble(geo.lat, engine)}),
            4326
          ),
          COALESCE(${radius.join(', ')})
        ),
        3347
      )
    `
  return `(SELECT ${geometry} AS geometry)`
}

const getGGIDGeometry = (geo, options) => {
  if (geo.type !== geoTypes.GGID) {
    return
  }
  const ref = options.getRef()
  const outerRef = options.getRef()
  return `(
    SELECT
      ${outerRef}.geometry
    FROM (
      SELECT
        CASE ${ref}.type
          ${ggidTypes.map(type =>
    `WHEN '${type}' THEN ${getKnownGeometry({ type, id: `${ref}.id` }, options)}`).join('\n')}
        END AS geometry
      FROM (${getGeoInfoNoGGID(geo, options)}) AS ${ref}
    ) AS ${outerRef}
    WHERE ${outerRef}.geometry IS NOT NULL
  )`
}

const getGeometry = (geo, options) => {
  if (geo.type in geoTables) {
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

const isValidGeometry = (geo, options) =>
  `(SELECT EXISTS ${getGeometry(geo, options)} AS is_valid_geometry)`

const resolveStringToGeo = (geoString, callback, options) => {
  const ref = options.getRef()
  const cases = Object.keys(geoTables)
    .map(type => [`${ref}.type = '${type}'`, { type, id: `${ref}.id`, radius: `${ref}.radius` }])
  // point
  cases.push([
    `${ref}.type = '${geoTypes.POINT}'`,
    { type: geoTypes.POINT, long: `${ref}.long`, lat: `${ref}.lat`, radius: `${ref}.radius` },
  ])
  return `(
    SELECT
      CASE
        ${cases.map(([cond, geo]) => `WHEN ${cond} THEN ${callback(geo, options)}`).join('\n')}
      END AS result
    FROM (${getGeoInfoNoGGIDFromString(geoString, options)}) AS ${ref}
  )`
}

const getGeometryFromString = (geoString, options) => {
  const res = resolveStringToGeo(geoString, getGeometry, options)
  const ref = options.getRef()
  return `(
    SELECT ${ref}.result AS geometry
    FROM ${res} AS ${ref}
    WHERE ${ref}.result IS NOT NULL
  )`
}

// const isValidGeometryFromString = (geoString, options) => {
//   const res = resolveStringToGeo(geoString, isValidGeometry, options)
//   return `(SELECT COALESCE(${res}, FALSE) AS is_valid_geometry)`
// }

const getKnownIntersectionGeometry = (geoA, geoB, { engine, getRef }) => {
  const typeA = geoTables[geoA.type]
  const typeB = geoTables[geoB.type]
  if (!typeA || !typeB) {
    return
  }
  let query
  let source
  if (typeA.intersectionQueryType && typeB.intersectionSourceType) {
    query = { type: typeA.intersectionQueryType, id: geoA.id }
    source = { type: typeB.intersectionSourceType, id: geoB.id }
  } else if (typeA.intersectionSourceType && typeB.intersectionQueryType) {
    query = { type: typeB.intersectionQueryType, id: geoB.id }
    source = { type: typeA.intersectionSourceType, id: geoA.id }
  } else {
    // can't use intersection table
    return
  }
  const ref = getRef()
  const geometry = engine === 'trino'
    ? `ST_GeomFromBinary(${ref}.intersect_geometry)`
    : `ST_Transform(${ref}.intersect_geometry, 3347)`
  return `(
    SELECT
      ${geometry} AS geometry
    FROM ${engine === 'trino' ? 'locus_place.' : ''}canada_geo.intersection AS ${ref}
    WHERE
      ${ref}.query_geo_id = ${query.id}
      AND ${ref}.query_geo_type = '${query.type}'
      AND ${ref}.source_geo_id = ${source.id}
      AND ${ref}.source_geo_type = '${source.type}'
  )`
}

const geosHaveKnownIntersection = (geoA, geoB, { engine, getRef }) => {
  const typeA = geoTables[geoA.type]
  const typeB = geoTables[geoB.type]
  if (!typeA || !typeB) {
    return
  }
  let query
  let source
  if (typeA.intersectionQueryType && typeB.intersectionSourceType) {
    query = { type: typeA.intersectionQueryType, id: geoA.id }
    source = { type: typeB.intersectionSourceType, id: geoB.id }
  } else if (typeA.intersectionSourceType && typeB.intersectionQueryType) {
    query = { type: typeB.intersectionQueryType, id: geoB.id }
    source = { type: typeA.intersectionSourceType, id: geoA.id }
  } else {
    // can't use intersection table
    return
  }
  const ref = getRef()
  return `(
    SELECT EXISTS (
      SELECT 1
      FROM ${engine === 'trino' ? 'locus_place.' : ''}canada_geo.intersection AS ${ref}
      WHERE
        ${ref}.query_geo_id = ${query.id}
        AND ${ref}.query_geo_type = '${query.type}'
        AND ${ref}.source_geo_id = ${source.id}
        AND ${ref}.source_geo_type = '${source.type}'
    )  AS intersects
  )`
}

const getCalculatedIntersectionGeometry = (geoA, geoB, options) => {
  const [geomA, geomB] = [geoA, geoB].map(geo => getGeometry(geo, options))
  const ref = options.getRef()
  return `(
    SELECT
      ${ref}.geometry
    FROM (
      SELECT ST_Intersection(${geomA}, ${geomB}) AS geometry
    ) AS ${ref}
    WHERE
      ${ref}.geometry IS NOT NULL
      AND NOT ST_IsEmpty(${ref}.geometry)
  )`
}

const geosHaveCalculatedIntersection = (geoA, geoB, options) => {
  const [geomA, geomB] = [geoA, geoB].map(geo => getGeometry(geo, options))
  return `(SELECT COALESCE(ST_Intersects(${geomA}, ${geomB}), FALSE) AS intersects)`
}

// one of the geo's has type GGID
const resolveGGIDStringsToGeos = (geoA, geoB, callback, options) => {
  if (geoA.type !== geoTypes.GGID && geoB.type !== geoTypes.GGID) {
    return
  }
  const { from, cases: [casesA, casesB] } = [geoA, geoB].reduce((acc, geo) => {
    if (geo.type !== geoTypes.GGID) {
      acc.cases.push([['TRUE', geo]])
      return acc
    }
    const ref = options.getRef()
    acc.from.push(`(${getGeoInfoNoGGID(geo, options)}) AS ${ref}`)
    acc.cases.push(ggidTypes.map(type =>
      [`${ref}.type = '${type}'`, { type, id: `${ref}.id` }]))
    return acc
  }, { from: [], cases: [] })
  const cases = casesA.map(([condA, geoA]) => `
    WHEN ${condA} THEN
      CASE
        ${casesB.map(([condB, geoB]) =>
    `WHEN ${condB} THEN ${callback(geoA, geoB, options)}`).join('\n')}
      END
  `)
  return `(
    SELECT
      CASE
        ${cases.join('\n')}
      END AS result
    FROM ${from.join(', ')}
  )`
}

// one of the geo's has type GGID
const getGGIDIntersectionGeometry = (geoA, geoB, options) => {
  const res = resolveGGIDStringsToGeos(geoA, geoB, getIntersectionGeometry, options)
  if (!res) {
    return
  }
  const ref = options.getRef()
  return `(
    SELECT result AS geometry
    FROM ${res} AS ${ref}
    WHERE ${ref}.result IS NOT NULL
  )`
}

// one of the geo's has type GGID
const ggidGeosHaveIntersection = (geoA, geoB, options) => {
  const res = resolveGGIDStringsToGeos(geoA, geoB, geosHaveIntersection, options)
  if (!res) {
    return
  }
  return `(SELECT COALESCE(${res}, FALSE) AS intersects)`
}

const reduceSameTypeGeosToSingleGeo = (geoA, geoB, callback, options) => {
  if (geoA.type !== geoB.type) {
    return
  }
  const radiuses = [geoA, geoB]
    .map(({ radius }) => (radius !== undefined
      ? `COALESCE(${castAsInteger(radius, options.engine)}, ${DEFAULT_POINT_RADIUS})`
      : DEFAULT_POINT_RADIUS))
  const result = radiuses[0] !== radiuses[1]
    ? `
      CASE
        WHEN ${radiuses.join(' <= ')}
        THEN ${callback(geoA, options)}
        ELSE ${callback(geoB, options)}
      END
    `
    : callback(geoA, options)
  const idCond = geoA.type === geoTypes.POINT
    ? `${geoA.long} = ${geoB.long} AND ${geoA.lat} = ${geoB.lat}`
    : `${geoA.id} = ${geoB.id}`
  return `(
    SELECT
      ${result} AS result
    WHERE
      ${idCond}
  )`
}

const getSameTypeIntersectionGeometry = (geoA, geoB, options) => {
  const res = reduceSameTypeGeosToSingleGeo(geoA, geoB, getGeometry, options)
  if (!res) {
    return
  }
  const ref = options.getRef()
  return `(
    SELECT result AS geometry
    FROM ${res} AS ${ref}
    WHERE ${ref}.result IS NOT NULL
  )`
}

const sameTypeGeosHaveIntersection = (geoA, geoB, options) => {
  const res = reduceSameTypeGeosToSingleGeo(geoA, geoB, isValidGeometry, options)
  if (!res) {
    return
  }
  return `(SELECT COALESCE(${res}, FALSE) AS intersects)`
}

const getIntersectionGeometry = (geoA, geoB, options) =>
  getSameTypeIntersectionGeometry(geoA, geoB, options) // same geo type
  || getKnownIntersectionGeometry(geoA, geoB, options) // ggid + types in intersection tbl
  || getGGIDIntersectionGeometry(geoA, geoB, options) // at least one ggid
  || getCalculatedIntersectionGeometry(geoA, geoB, options) // compute on demand

const geosHaveIntersection = (geoA, geoB, options) =>
  sameTypeGeosHaveIntersection(geoA, geoB, options) // same geo type
  || geosHaveKnownIntersection(geoA, geoB, options) // ggid + types in intersection tbl
  || ggidGeosHaveIntersection(geoA, geoB, options) // at least one ggid
  || geosHaveCalculatedIntersection(geoA, geoB, options) // compute on demand


const resolveStringsToGeos = (geoStringA, geoStringB, callback, options) => {
  const refA = options.getRef()
  const refB = options.getRef()
  const [casesA, casesB] = [refA, refB].map((ref) => {
    const cases = Object.keys(geoTables)
      .map(type => [`${ref}.type = '${type}'`, { type, id: `${ref}.id`, radius: `${ref}.radius` }])
    // point
    cases.push([
      `${ref}.type = '${geoTypes.POINT}'`,
      { type: geoTypes.POINT, long: `${ref}.long`, lat: `${ref}.lat`, radius: `${ref}.radius` },
    ])
    return cases
  })
  const cases = casesA.map(([condA, geoA]) => `
    WHEN ${condA} THEN
      CASE
        ${casesB.map(([condB, geoB]) =>
    `WHEN ${condB} THEN ${callback(geoA, geoB, options)}`).join('\n')}
      END
  `)
  return `(
    SELECT
      CASE
        ${cases.join('\n')}
      END AS result
    FROM
      (${getGeoInfoNoGGIDFromString(geoStringA, options)}) AS ${refA},
      (${getGeoInfoNoGGIDFromString(geoStringB, options)}) AS ${refB}
  )`
}

const getIntersectionGeometryFromString = (geoStringA, geoStringB, options) => {
  const res = resolveStringsToGeos(geoStringA, geoStringB, getIntersectionGeometry, options)
  const ref = options.getRef()
  return `(
    SELECT result AS geometry
    FROM ${res} AS ${ref}
    WHERE ${ref}.result IS NOT NULL
  )`
}

const geosHaveIntersectionFromString = (geoStringA, geoStringB, options) => {
  const res = resolveStringsToGeos(geoStringA, geoStringB, geosHaveIntersection, options)
  return `(SELECT COALESCE(${res}, FALSE) AS intersects)`
}

const parseGeoStringLiteral = (sql, engine) => {
  if (!sql.startsWith("'") || !sql.endsWith("'")) {
    return
  }
  const safeSQL = sql.slice(1, -1).trim()
  if (/^geo:[^']+$/.test(safeSQL)) {
    return
  }
  return safeSQL.slice(4).split(':').map((arg, i) => {
    if (i) {
      return engine === 'trino' ? `'${arg.replace(/'/g, "''")}'` : escapeLiteral(arg)
    }
    // geo type
    return arg
  })
}

const blockChars = {
  '[': ']',
  '(': ')',
  '{': '}',
}

// input comes from geo constructor function
// sql: "(SELECT 'geo:<type>:' || (arg)[ || ':' || (arg)] AS geometry)"
const parseGeoStringConstruct = (sql) => {
  if (!sql.startsWith("(SELECT 'geo:") || !sql.endsWith(' AS geometry)')) {
    return
  }
  const vals = []
  let val = ''
  let valEnding = false
  let quote = ''
  let quoteEnding = false
  let block = ''
  let blockEnd = ''
  let blockDepth = 0
  for (const char of sql.slice(8, -13)) {
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

    if (!block) {
      // end of val
      if (char === '|') {
        valEnding = true
        continue
      }
      // new block starting
      if (char in blockChars) {
        block = char
        blockEnd = blockChars[char]
      }
      val += char
      continue
    }

    // block in progress
    // nested block
    if (char === block) {
      blockDepth += 1
    } else if (char === blockEnd) {
      // end block (main or nested)
      if (!blockDepth) {
        block = ''
        blockEnd = ''
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
    return
  }
  return [
    vals[0].slice(5, -3),
    ...vals.slice(1).filter(v => v !== " ':' ").map(v => v.trim()),
  ]
}

const parseGeoString = (sql) => {
  const parsed = parseGeoStringLiteral(sql) || parseGeoStringConstruct(sql)
  if (!parsed) {
    return
  }
  const [type, ...args] = parsed
  if (!(type in geometryTypeValues)) {
    throw apiError('Invalid geometry', 400)
  }
  // geo with id
  if (type in geoTables || type === geoTypes.GGID) {
    if (args.length !== 1 && args.length !== 2) {
      throw apiError(`Invalid ${type} geometry`, 400)
    }
    return { type, id: args[0], radius: args[1] }
  }
  // point
  if (args.length !== 2 && args.length !== 3) {
    throw apiError('Invalid point geometry', 400)
  }
  return { type, long: args[0], lat: args[1], radius: args[2] }
}

const geoParser = engine => (node, options) => {
  const [type, ...args] = node.args.map(e => e.to(engine, options))
  const upperTrimmedArgs = args.map(sql => `upper(trim(${sql}))`)
  const noSpaceArgs = upperTrimmedArgs.map(sql => (engine === 'trino'
    ? `replace(${sql}, ' ', '')`
    : `regexp_replace(${sql}, '\\s+', '', 'g')`))
  const hyphensNoAccentsArgs = upperTrimmedArgs.map((sql) => {
    const hyphenated = engine === 'trino'
      ? `array_join(array_remove(split(${sql}, ' '), ''), '-')`
      : `regexp_replace(${sql}, '\\s+', '-', 'g')`
    return `
      translate(
        ${hyphenated},
        'ÂÃÄÅĀĂĄÁÀÇÉÈËÊĒĔĖĘĚÌÍÎÏĨĪĬÒÓÔÕÖŌŎŐÙÚÛÜŨŪŬŮ',
        'AAAAAAAAACEEEEEEEEEIIIIIIIOOOOOOOOUUUUUUUU'
      )
    `
  })
  const safeType = type.slice(1, -1).trim().toLowerCase()
  // type can be determined from args
  if (safeType in geometryTypeValues) {
    let parsedArgs
    if ([geoTypes.CA_FSA, geoTypes.CA_POSTALCODE].includes(safeType)) {
      parsedArgs = noSpaceArgs
    } else if (safeType === geoTypes.CA_CITY) {
      parsedArgs = hyphensNoAccentsArgs
    } else {
      parsedArgs = upperTrimmedArgs
    }
    return `(SELECT 'geo:${safeType}:' || ${parsedArgs.join(" || ':' || ")} AS geometry)`
  }
  // type will be evaluated at exec time
  const ref = options.getRef()
  return `(
    SELECT
      'geo:' || ${ref}.type || ':' || CASE
        WHEN ${ref}.type IN ('${geoTypes.CA_FSA}', '${geoTypes.CA_POSTALCODE}')
        THEN ${noSpaceArgs.join(" || ':' || ")}
        WHEN ${ref}.type = '${geoTypes.CA_CITY}'
        THEN ${hyphensNoAccentsArgs.join(" || ':' || ")}
        ELSE ${upperTrimmedArgs.join(" || ':' || ")}
      END AS geometry
    FROM (SELECT lower(trim(${type})) AS type) AS ${ref}
  )`
}

const geoIntersectsParser = engine => (node, options) => {
  const [geoStringA, geoStringB] = node.args.map(e => e.to(engine, options))
  const [geoA, geoB] = [geoStringA, geoStringB].map(geo => parseGeoString(geo))
  const hasIntersection = geoA && geoB
    ? geosHaveIntersection(geoA, geoB, { engine, ...options })
    : geosHaveIntersectionFromString(geoStringA, geoStringB, { engine, ...options })
  return `(SELECT ${hasIntersection} AS geo_intersects)`
}

const geoIntersectionAreaParser = engine => (node, options) => {
  const [geoStringA, geoStringB] = node.args.map(e => e.to(engine, options))
  const [geoA, geoB] = [geoStringA, geoStringB].map(geo => parseGeoString(geo))
  const geometry = geoA && geoB
    ? getIntersectionGeometry(geoA, geoB, { engine, ...options })
    : getIntersectionGeometryFromString(geoStringA, geoStringB, { engine, ...options })
  return `(SELECT ST_Area(${geometry}) AS geo_intersection_area)`
}

const geoAreaParser = engine => (node, options) => {
  const [geoString] = node.args.map(e => e.to(engine, options))
  const geo = parseGeoString(geoString)
  const geometry = geo
    ? getGeometry(geo, { engine, ...options })
    : getGeometryFromString(geoString, { engine, ...options })
  return `(SELECT ST_Area(${geometry}) AS geo_area)`
}

const geoJSONParser = engine => (node, options) => {
  const [geoString] = node.args.map(e => e.to(engine, options))
  const geo = parseGeoString(geoString)
  const geometry = geo
    ? getGeometry(geo, { engine, ...options })
    : getGeometryFromString(geoString, { engine, ...options })
  const geoJSON = engine === 'trino'
    ? `CAST(to_geojson_geometry(${geometry}) AS json)` // from varchar
    : `CAST(ST_AsGeoJSON(ST_Transform(${geometry}, 4326)) AS jsonb)` // from text
  return `(SELECT ${geoJSON} AS geo_json)`
}

const geoDistanceParser = engine => (node, options) => {
  const [geoStringA, geoStringB] = node.args.map(e => e.to(engine, options))
  const [geoA, geoB] = [geoStringA, geoStringB].map(geo => parseGeoString(geo))
  const geometryA = geoA
    ? getGeometry(geoA, { engine, ...options })
    : getGeometryFromString(geoStringA, { engine, ...options })
  const geometryB = geoB
    ? getGeometry(geoB, { engine, ...options })
    : getGeometryFromString(geoStringB, { engine, ...options })
  return `(SELECT ST_Distance(${geometryA}, ${geometryB}) AS geo_distance)`
}

module.exports = {
  geoParser,
  geoIntersectsParser,
  geoIntersectionAreaParser,
  geoAreaParser,
  geoJSONParser,
  geoDistanceParser,
}
