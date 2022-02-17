/* eslint-disable no-use-before-define */
/* eslint-disable no-continue */
const { useAPIErrorOptions } = require('../../../util/api-error')
// const { geometryTypes: geoTypes, geometryTypeValues } = require('../src/types')
const { geometryTypes: geoTypes } = require('../src/types')
const geoTables = require('../../geo-tables')


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

const getGeoStringInfo = (geoString, { engine, getRef }) => {
  const ref = getRef()
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
        WHEN '${geoTypes.POI}' THEN ${ref}[4]
      END AS radius
    FROM ${engine === 'trino' ? 'split' : 'regexp_split_to_array'}(${geoString}, ':') AS ${ref}
    WHERE
      ${ref}[1] = 'geo'
      AND ${ref}[2] IN (${Object.values(geoTypes).map(t => `'${t}'`).join(',')})
  `
}

const getGeoStringInfoNoGGID = (geoString, { engine, getRef }) => {
  const geo = getRef()
  const ggid = getRef()
  return `
    WITH ${geo} AS (
      ${getGeoStringInfo(geoString, { engine, getRef })}
    )
    SELECT
      CASE
        WHEN ${geo}.type = '${geoTypes.GGID}' THEN 'ca-' || ${ggid}.type
        ELSE ${geo}.type
      END AS type,
      COALESCE(${ggid}.local_id, ${geo}.id) AS id,
      ${geo}.long,
      ${geo}.lat,
      ${geo}.radius
    FROM ${geo}
    LEFT JOIN ${engine === 'trino' ? 'locus_place.' : ''}config.ggid_map AS ${ggid} ON
      ${geo}.type = '${geoTypes.GGID}'
      AND ${ggid}.ggid = ${castAsInteger(`${geo}.id`, engine)}
    WHERE CASE
      WHEN ${geo}.type = '${geoTypes.GGID}' THEN ${ggid}.local_id IS NOT NULL
      ELSE TRUE
    END
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
  if (type.geometryColumn) {
    geometries.push(engine === 'trino'
      ? `ST_GeomFromBinary(${ref},${type.geometryColumn})`
      : `ST_Transform(ST_MakeValid(${ref}.${type.geometryColumn}), 3347)`)
  }
  if (type.longColumn && type.latColumn) {
    const radius = []
    if (geo.radius !== undefined) {
      radius.push(castAsInteger(geo.radius, engine))
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
  return `(
    SELECT
      COALESCE(${geometries.join(', ')}) AS geometry
    FROM ${engine === 'trino' ? 'locus_place.' : ''}${type.schema}.${type.table} AS ${ref}
    WHERE
      ${ref}.${type.idColumn} = ${type.idType === 'Numeric'
  ? castAsInteger(geo.id, engine) : geo.id}
      ${whitelabelID && type.whitelabelColumn
    ? `AND ${ref}.${type.whitelabelColumn} = ${whitelabelID}`
    : ''}
      ${customerID && type.customerColumn
    ? `AND (
      ${ref}.${type.customerColumn} = ${customerID}
      OR ${ref}.${type.customerColumn} IS NULL
    )` : ''}
      ${type.publicColumn ? `AND ${ref}.${type.publicColumn} IS TRUE` : ''}
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

// const getCityGeometry = (geo, { engine = 'pg' } = {}) => {
//   if (geo.type !== geoTypes.CA_CITY) {
//     return
//   }
//   const { city, province } = geo
//   let id
//   if (province) {
//     id = `ARRAY['CA', ${province}, ${city}]`
//   } else {
//     id = engine === 'trino'
//       ? `split(${geo.id}, '$')`
//       : `regexp_split_to_array(${geo.id}, '\\$')`
//   }
//   const geometry = engine === 'trino'
//     ? 'ST_GeomFromBinary(wkb_geometry)'
//     : 'ST_Transform(ST_MakeValid(wkb_geometry), 3347)'

//   return `
//     SELECT
//       ${geometry} AS geometry
//     FROM ${id} geo
//     JOIN canada_geo.province p ON p.province_code = geo[2]
//     JOIN canada_geo.city c ON c.pruid = p.pruid AND c.city = geo[3]
//     WHERE geo[1] = 'CA' AND geo[3] IS NOT NULL
//   `
// }

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

const getGeometryFromString = (geoString, options) => {
  const ref = options.getRef()
  const outerRef = options.getRef()
  const cases = Object.keys(geoTables)
    .map(type => [`${ref}.type = '${type}'`, { type, id: `${ref}.id`, radius: `${ref}.radius` }])
  // point
  cases.push([
    `${ref}.type = '${geoTypes.POINT}'`,
    { type: geoTypes.POINT, long: `${ref}.long`, lat: `${ref}.lat`, radius: `${ref}.radius` },
  ])
  return `(
    SELECT
      ${outerRef}.geometry
    FROM (
      SELECT
        CASE
          ${cases.map(([cond, geo]) => `WHEN ${cond} THEN ${getGeometry(geo, options)}`).join('\n')}
        END AS geometry
      FROM (${getGeoStringInfoNoGGID(geoString, options)}) AS ${ref}
    ) AS ${outerRef}
    WHERE ${outerRef}.geometry IS NOT NULL
  )`
}

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
      ${ref}.query_geo_type = '${query.type}'
      AND ${ref}.query_geo_id = ${query.id}
      AND ${ref}.source_geo_type = '${source.type}'
      AND ${ref}.source_geo_id = ${source.id}
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
    WHERE NOT ST_IsEmpty(${ref}.geometry)
  )`
}

// one of the geo's has type GGID
const getGGIDIntersectionGeometry = (geoA, geoB, options) => {
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
    `WHEN ${condB} THEN ${getIntersectionGeometry(geoA, geoB, options)}`).join('\n')}
      END
  `)
  const outerRef = options.getRef()
  return `(
    SELECT
      ${outerRef}.geometry
    FROM (
      SELECT
        CASE
          ${cases.join('\n')}
        END AS geometry
      FROM ${from.join(', ')}
    ) AS ${outerRef}
    WHERE ${outerRef}.geometry IS NOT NULL
  )`
}

const getSameTypeIntersectionGeometry = (geoA, geoB, options) => {
  if (geoA.type !== geoB.type) {
    return
  }
  const radiusCond = [geoA, geoB]
    .map(({ radius }) => (radius !== undefined
      ? `COALESCE(${castAsInteger(radius, options.engine)}, ${DEFAULT_POINT_RADIUS})`
      : DEFAULT_POINT_RADIUS))
    .join(' <= ')
  const idCond = geoA.type === geoTypes.POINT
    ? `${geoA.long} = ${geoB.long} AND ${geoA.lat} = ${geoB.lat}`
    : `${geoA.id} = ${geoB.id}`
  const ref = options.getRef()
  return `(
    SELECT
      ${ref}.geometry
    FROM (
      SELECT
        CASE
          WHEN ${radiusCond}
          THEN ${getGeometry(geoA, options)}
          ELSE ${getGeometry(geoB, options)}
        END AS geometry
      WHERE ${idCond}
    ) AS ${ref}
    WHERE ${ref}.geometry IS NOT NULL
  )`
}

const getIntersectionGeometry = (geoA, geoB, options) =>
  getSameTypeIntersectionGeometry(geoA, geoB, options) // same geo type
  || getKnownIntersectionGeometry(geoA, geoB, options) // ggid + types in intersection tbl
  || getGGIDIntersectionGeometry(geoA, geoB, options) // at least one ggid
  || getCalculatedIntersectionGeometry(geoA, geoB, options) // compute on demand


const getIntersectionGeometryFromString = (geoStringA, geoStringB, options) => {
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
    `WHEN ${condB} THEN ${getIntersectionGeometry(geoA, geoB, options)}`).join('\n')}
      END
  `)
  const outerRef = options.getRef()
  return `(
    SELECT
      ${outerRef}.geometry
    FROM (
      SELECT
        CASE
          ${cases.join('\n')}
        END AS geometry
      FROM
        (${getGeoStringInfoNoGGID(geoStringA, options)}) AS ${refA},
        (${getGeoStringInfoNoGGID(geoStringB, options)}) AS ${refB}
      ) AS ${outerRef}
      WHERE ${outerRef}.geometry IS NOT NULL
  )`
}

// // sql: "(SELECT 'geo:<type>:' || (arg)[ || ':' || (arg)] AS geometry)"
// const extractGeoSQLValues = (sql) => {
//   if (!sql.startsWith("(SELECT 'geo:") || !sql.endsWith(' AS geometry)')) {
//     return
//   }
//   const vals = []
//   let val = ''
//   let valEnding = false
//   let quote = ''
//   let quoteEnding = false
//   let inBlock = false
//   let blockDepth = 0
//   for (const char of sql.slice(8, -13)) {
//     // quote in progress
//     if (quote) {
//       // quote continues
//       if (!quoteEnding || char === quote) {
//         if (char === quote) {
//           quoteEnding = !quoteEnding
//         }
//         val += char
//         continue
//       }
//       // quote ends, need to deal with char
//       quote = ''
//       quoteEnding = false
//     }

//     if (valEnding) {
//       valEnding = false
//       // val ended
//       if (char === '|') {
//         vals.push(val)
//         val = ''
//         continue
//       }
//       // val continues
//       val += '|'
//     }

//     // no quote or quote has just ended
//     // new quote starting
//     if (char === '"' || char === "'") {
//       quote = char
//       val += char
//       continue
//     }

//     if (!inBlock) {
//       // end of val
//       if (char === '|') {
//         valEnding = true
//         continue
//       }
//       // new block starting
//       if (char === '(') {
//         inBlock = true
//       }
//       val += char
//       continue
//     }

//     // block in progress
//     // nested block
//     if (char === '(') {
//       blockDepth += 1
//     } else if (char === ')') {
//       // end block (main or nested)
//       if (!blockDepth) {
//         inBlock = false
//       } else {
//         blockDepth -= 1
//       }
//     }
//     val += char
//   }
//   // last val
//   if (val) {
//     vals.push(val)
//   }
//   if (vals.length < 2) {
//     return
//   }
//   return [
//     vals[0].slice(5, -3),
//     ...vals.slice(1).filter(v => v !== " ':' ").map(v => v.trim()),
//   ]
// }

// const parseGeoSQL = (sql) => {
//   const [type, ...args] = extractGeoSQLValues(sql) || []
//   if (!(type in geometryTypeValues)) {
//     throw apiError('Invalid geometry', 400)
//   }
//   if (type === geoTypes.POI) {
//     if (args.length !== 1 && args.length !== 2) {
//       throw apiError(`Invalid ${type} geometry`, 400)
//     }
//     return { type, id: args[0], radius: args[1] }
//   }
//   // geo with id
//   if (type in geoTables || type === geoTypes.GGID) {
//     if (args.length !== 1) {
//       throw apiError(`Invalid ${type} geometry`, 400)
//     }
//     return { type, id: args[0] }
//   }
//   // point
//   if (args.length !== 2 && args.length !== 3) {
//     throw apiError('Invalid point geometry', 400)
//   }
//   return { type, long: args[0], lat: args[1], radius: args[2] }
// }

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

// const geoParserKnownType = engine => (node, options) => {
//   const [type, ...args] = node.args
//   const parsedArgs = args.map((e) => {
//     let sql = `upper(trim(${e.to(engine, options)}))`
//     if ([geoTypes.CA_FSA, geoTypes.CA_POSTALCODE].includes(type.value)) {
//       // remove all spaces
//       return engine === 'trino'
//         ? `replace(${sql}, ' ', '')`
//         : `regexp_replace(${sql}, '\\s+', '', 'g')`
//     }
//     if (type.value !== geoTypes.CA_CITY) {
//       return sql
//     }
//     // remove duplicate spaces and replace with hyphen
//     sql = engine === 'trino'
//       ? `array_join(array_remove(split(${sql}, ' '), ''), '-')`
//       : `regexp_replace(${sql}, '\\s+', '-', 'g')`
//     return `
//       translate(
//         ${sql},
//         'ÂÃÄÅĀĂĄÁÀÇÉÈËÊĒĔĖĘĚÌÍÎÏĨĪĬÒÓÔÕÖŌŎŐÙÚÛÜŨŪŬŮ',
//         'AAAAAAAAACEEEEEEEEEIIIIIIIOOOOOOOOUUUUUUUU'
//       )
//     `
//   }).join(" || ':' || ")
//   return `(SELECT 'geo:${type.value}:' || ${parsedArgs} AS geometry)`
// }

const geoIntersectsParser = engine => (node, options) => {
  // const [geoA, geoB] = node.args.map(e => parseGeoSQL(e.to(engine, options)))
  // const geometry = getIntersectionGeometry(geoA, geoB, { engine, ...options })
  const [geoA, geoB] = node.args.map(e => e.to(engine, options))
  const geometry = getIntersectionGeometryFromString(geoA, geoB, { engine, ...options })
  return `(SELECT EXISTS ${geometry} AS geo_intersects)`
}

const geoIntersectionAreaParser = engine => (node, options) => {
  // const [geoA, geoB] = node.args.map(e => parseGeoSQL(e.to(engine, options)))
  // const geometry = getIntersectionGeometry(geoA, geoB, { engine, ...options })
  const [geoA, geoB] = node.args.map(e => e.to(engine, options))
  const geometry = getIntersectionGeometryFromString(geoA, geoB, { engine, ...options })
  return `(SELECT ST_Area(${geometry}) AS geo_intersection_area)`
}

const geoIntersectionAreaRatioParser = engine => (node, options) => {
  // const [geoA, geoB] = node.args.map(e => parseGeoSQL(e.to(engine, options)))
  // const numerator = getIntersectionGeometry(geoA, geoB, { engine, ...options })
  // const denominator = getGeometry(geoB, { engine, ...options })
  const [geoA, geoB] = node.args.map(e => e.to(engine, options))
  const numerator = getIntersectionGeometryFromString(geoA, geoB, { engine, ...options })
  const denominator = getGeometryFromString(geoB, { engine, ...options })
  return `(SELECT ST_Area(${numerator}) / ST_Area(${denominator}) AS geo_intersection_area_ratio)`
}

const geoAreaParser = engine => (node, options) => {
  // const [geo] = node.args.map(e => parseGeoSQL(e.to(engine, options)))
  // const geometry = getGeometry(geo, { engine, ...options })
  const [geo] = node.args.map(e => e.to(engine, options))
  const geometry = getGeometryFromString(geo, { engine, ...options })
  return `(SELECT ST_Area(${geometry}) AS geo_area)`
}

const geoDistanceParser = engine => (node, options) => {
  // const [geoA, geoB] = node.args.map(e => parseGeoSQL(e.to(engine, options)))
  // const geometryA = getGeometry(geoA, { engine, ...options })
  // const geometryB = getGeometry(geoB, { engine, ...options })
  const [geoA, geoB] = node.args.map(e => e.to(engine, options))
  const geometryA = getGeometryFromString(geoA, { engine, ...options })
  const geometryB = getGeometryFromString(geoB, { engine, ...options })
  return `(SELECT ST_Distance(${geometryA}, ${geometryB}) AS geo_distance)`
}

module.exports = {
  geoParser,
  geoIntersectsParser,
  geoIntersectionAreaParser,
  geoIntersectionAreaRatioParser,
  geoAreaParser,
  geoDistanceParser,
}
