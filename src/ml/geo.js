/* eslint-disable indent */
/* eslint-disable no-loop-func */
/* eslint-disable no-plusplus */
/* eslint-disable no-continue */
const { knex } = require('../util/db')
const { useAPIErrorOptions } = require('../util/api-error')
const { CAT_STRING, CAT_NUMERIC } = require('./type')
const { Expression } = require('./expressions')


const { apiError } = useAPIErrorOptions({ tags: { service: 'ql' } })

const DEFAULT_RADIUS = 500 // 500 metres

/**
 * @enum
 */
const geoTypes = {
  CA_FSA: 'ca-fsa',
  CA_DA: 'ca-da',
  CA_CT: 'ca-ct',
  CA_CSD: 'ca-csd',
  CA_POSTALCODE: 'ca-postalcode',
  CA_PROVINCE: 'ca-province',
  CA_CITY: 'ca-city',
  POI: 'poi',
}

const geoMapping = {
  [geoTypes.CA_FSA]: {
    schema: 'canada_geo',
    table: 'fsa_simplified',
    idType: CAT_STRING,
    idColumn: 'fsa',
    geometryColumn: 'wkb_geometry',
    intersectionQueryType: 'fsa',
    intersectionSourceType: 'fsa',
  },
  [geoTypes.CA_DA]: {
    schema: 'canada_geo',
    table: 'da',
    idType: CAT_NUMERIC,
    idColumn: 'dauid',
    geometryColumn: 'wkb_geometry',
    intersectionSourceType: 'da',
  },
  [geoTypes.CA_CT]: {
    schema: 'canada_geo',
    table: 'ct',
    idType: CAT_STRING,
    idColumn: 'ctuid',
    geometryColumn: 'wkb_geometry',
    intersectionSourceType: 'ct',
  },
  [geoTypes.CA_CSD]: {
    schema: 'canada_geo',
    table: 'csd',
    idType: CAT_NUMERIC,
    idColumn: 'gid',
    geometryColumn: 'geom',
  },
  [geoTypes.CA_POSTALCODE]: {
    schema: 'canada_geo',
    table: 'postalcode_simplified',
    idType: CAT_STRING,
    idColumn: 'postalcode',
    geometryColumn: 'wkb_geometry',
    intersectionQueryType: 'postalcode',
  },
  [geoTypes.CA_PROVINCE]: {
    schema: 'canada_geo',
    table: 'province',
    idType: CAT_STRING,
    idColumn: 'province_code',
    geometryColumn: 'wkb_geometry',
  },
  [geoTypes.CA_CITY]: {
    schema: 'canada_geo',
    table: 'city_dev',
    idType: CAT_STRING,
    idColumn: 'name',
    geometryColumn: "geo->>'geom'",
  },
  [geoTypes.POI]: {
    schema: 'public',
    table: 'poi',
    idType: CAT_NUMERIC,
    idColumn: 'poi_id',
    geometryColumn: 'polygon',
    latColumn: 'lat',
    longColumn: 'lon',
    radiusColumn: 'default_radius',
    whitelabelColumn: 'whitelabelid',
    customerColumn: 'customerid',
  },
}

// const makeGeoOverlapMacro = (colA, colB) => ({
//   key: `[GEO_OVERLAP_PCT|${`${colA.view}.${colA.key}|${colB.view}.${colB.key}`.toUpperCase()}]`,
//   expression: colA.geo_type === colB.geo_type
//     ? `((${colA.view}.${colA.key} = ${colB.view}.${colB.key}) OR FALSE)::int`
//     : `
//       ST_Area(
//         ST_Intersection(${colA.view}._${colA.key}_geometry, ${colB.view}._${colB.key}_geometry)
//       ) / ST_Area(${colB.view}._${colB.key}_geometry)`,
// })

// substitue geo id with geometry when there are geo intersections of different types
// injects geo into views and expression and returns same as new objects
// no mutations to args
const insertGeo = ({ whitelabel, customers }, views, viewColumns, fdwConnections, expression) => {
  // make copy of expression using JSON stringify + parse so as to not mutate the original object
  const expressionWithGeo = JSON.parse(JSON.stringify(expression))
  const viewsWithGeo = { ...views } // shallow copy
  const fdwConnectionsWithGeo = { ...fdwConnections } // shallow copy
  const geoIntersections = {}
  const geoJoins = {}
  let geoJoinCount = 0
  // const macros = {}
  const queue = [expressionWithGeo]
  const exp = new Expression(viewColumns)
  // look for geo intersections
  while (queue.length) {
    const item = queue.shift()
    // if not object, contiue
    if (typeof item !== 'object' || item === null) {
      continue
    }
    let argA
    let argB
    let throwOnNonGeoColumn = false
    // check if array condition
    if (Array.isArray(item) && item.length === 3 && item[1] === '=') {
      [argA, , argB] = item
    // check if operator object
    } else if (item.type === 'operator' && item.values.length === 3 && item.values[0] === '=') {
      [, argA, argB] = item.values
    } else if (
      item.type === 'function'
      && item.values.length === 3
      // && ['geo_intersects', 'geo_within'].includes(item.values[0])
      && item.values[0] === 'geo_intersects'
    ) {
      [, argA, argB] = item.values
      // args must be 2 geo columns
      throwOnNonGeoColumn = true
    // else push object values to queue for processing
    } else {
      queue.push(...Object.values(item))
      continue
    }

    const colA = exp.extractColumn(argA)
    if (!colA || colA.column === '*') {
      if (throwOnNonGeoColumn) {
        throw apiError(`Argument must be of type geo column: ${argA}`, 400)
      }
      if (!colA) {
        queue.push(argA)
      }
    }
    const colB = exp.extractColumn(argB)
    if (!colB || colB.column === '*') {
      if (throwOnNonGeoColumn) {
        throw apiError(`Argument must be of type geo column: ${argB}`, 400)
      }
      if (!colB) {
        queue.push(argB)
      }
    }
    // proceed with geo join if argA and argB are geo columns of different types
    if (!colA || !colB) {
      continue
    }

    // attach geo_type
    colA.geo_type = viewColumns[colA.view][colA.column].geo_type
    colB.geo_type = viewColumns[colB.view][colB.column].geo_type

    if (!(colA.geo_type in geoMapping) || !(colB.geo_type in geoMapping)) {
      if (throwOnNonGeoColumn) {
        throw apiError(`Argument(s) must be of type geo column: ${argB}, ${argB}`, 400)
      }
      continue
    }

    // add cols to joins and prepare intersect macros
    // [colA, colB].forEach((col) => {
    //   // if not same geo type, need to join with geo to get geometry
    //   if (colA.geo_type !== colB.geo_type) {
    //     joins[col.view] = joins[col.view] || {}
    //     joins[col.view][col.key] = geoMapping[col.geo_type]
    //   }

    //   const { key, expression } = makeGeoOverlapMacro(col, col === colA ? colB : colA)
    //   macros[key] = expression
    // })

    // if different geo type need to change equality condition to geo intersection
    if (colA.geo_type === colB.geo_type) {
      continue
    }

    // add bridge table
    const intersectionKey = [colA.geo_type, colB.geo_type].sort().join('__')
    // register id filters for each of the resolutions to limit the size of the bridge table
    geoIntersections[intersectionKey] = geoIntersections[intersectionKey] || {
      [colA.geo_type]: {},
      [colB.geo_type]: {},
    }
    geoIntersections[intersectionKey][colA.geo_type][`${colA.view}$${colA.column}`] = colA
    geoIntersections[intersectionKey][colB.geo_type][`${colB.view}$${colB.column}`] = colB

    // keep track of joins between resolutions
    const colAJoinKey = `${colA.view}$${colA.column}$${colB.geo_type}`
    const colBJoinKey = `${colB.view}$${colB.column}$${colA.geo_type}`
    geoJoins[colAJoinKey] = geoJoins[colAJoinKey] || { joinKeys: new Set(), alias: geoJoinCount++ }
    geoJoins[colBJoinKey] = geoJoins[colBJoinKey] || { joinKeys: new Set(), alias: geoJoinCount++ }
    geoJoins[colAJoinKey].joinKeys.add(colBJoinKey)
    geoJoins[colBJoinKey].joinKeys.add(colAJoinKey)

    // move original views that will need to be rewritten in order to expose join
    // geo cols as '__source__<view>'
    viewsWithGeo[`__source__${colA.view}`] = views[colA.view]
    viewsWithGeo[`__source__${colB.view}`] = views[colB.view]

    // mutate item
    // should be straight equality condition based on geo_id, no more geometry
    // we don't know yet where the join to the intersection table will live in order to do
    // a straigh equality join between the views (using geo id's in the same resolution) so let's
    // add new columns that will eventually be mapped to the column or to the geo intersection table
    if (Array.isArray(item)) {
      item[0] = `${colA.view}.__geo_id_${geoJoins[colAJoinKey].alias}`
      item[1] = '='
      item[2] = `${colB.view}.__geo_id_${geoJoins[colBJoinKey].alias}`
      continue
    }
    Object.assign(item, {
      type: 'operator',
      values: [
        '=',
        `${colA.view}.__geo_id_${geoJoins[colAJoinKey].alias}`,
        `${colB.view}.__geo_id_${geoJoins[colBJoinKey].alias}`,
      ],
    })
  }

  // append geo intersections (bridge tables) to views
  Object.entries(geoIntersections).forEach(([key, geos]) => {
    const [geoTypeA, geoTypeB] = Object.keys(geos)
    const geoA = geoMapping[geoTypeA]
    const geoB = geoMapping[geoTypeB]

    const idFilterA = Object.values(geos[geoTypeA]).reduce((filter, col) => {
      filter.push(`SELECT ${
        geoA.idType === CAT_STRING ? `upper("${col.column}")` : `"${col.column}"`
      } AS id FROM "__source__${col.view}"`)
      return filter
    }, [])
    const idFilterB = Object.values(geos[geoTypeB]).reduce((filter, col) => {
      filter.push(`SELECT ${
        geoB.idType === CAT_STRING ? `upper("${col.column}")` : `"${col.column}"`
      } AS id FROM "__source__${col.view}"`)
      return filter
    }, [])

    // check if can use pre-computed intersections
    let intersectionQuery
    let intersectionSource
    if (geoA.intersectionQueryType && geoB.intersectionSourceType) {
      intersectionQuery = { ...geoA, geoType: geoTypeA, idFilter: idFilterA }
      intersectionSource = { ...geoB, geoType: geoTypeB, idFilter: idFilterB }
    } else if (geoA.intersectionSourceType && geoB.intersectionQueryType) {
      intersectionQuery = { ...geoB, geoType: geoTypeB, idFilter: idFilterB }
      intersectionSource = { ...geoA, geoType: geoTypeA, idFilter: idFilterA }
    }
    if (intersectionQuery) {
      const queryIdFilter = `
        ${intersectionQuery.idType === CAT_STRING ? 'upper(query_geo_id)' : 'query_geo_id'} IN (
          SELECT f.id FROM (${intersectionQuery.idFilter.join(' UNION ')}) f
        )
      `
      const sourceIdFilter = `
        ${intersectionSource.idType === CAT_STRING ? 'upper(source_geo_id)' : 'source_geo_id'} IN (
          SELECT f.id FROM (${intersectionSource.idFilter.join(' UNION ')}) f
        )
      `
      viewsWithGeo[`__geo__${key}`] = knex.raw(`
        SELECT
          ${
            intersectionQuery.idType === CAT_STRING ? 'upper(query_geo_id)' : 'query_geo_id'
          } AS "${intersectionQuery.geoType}",
          ${
            intersectionSource.idType === CAT_STRING ? 'upper(source_geo_id)' : 'source_geo_id'
          } AS "${intersectionSource.geoType}"
        FROM canada_geo.intersection
        WHERE
          query_geo_type = '${intersectionQuery.intersectionQueryType}'
          AND source_geo_type = '${intersectionSource.intersectionSourceType}'
          AND ${queryIdFilter}
          AND ${sourceIdFilter}
      `)

      return
    }

    // otherwise join geometries
    const sourceGeometriesA = []
    const joinGeometriesA = []
    const sourceGeometriesB = []
    const joinGeometriesB = []

    if (geoA.geometryColumn) {
      sourceGeometriesA.push(`a.${geoA.geometryColumn}`)
      joinGeometriesA.push(`a.${geoA.geometryColumn}`)
    }
    if (geoA.latColumn && geoA.longColumn) {
      sourceGeometriesA.push(`ST_MakePoint(a.${geoA.longColumn}, a.${geoA.latColumn})`)
      joinGeometriesA.push(`
        ST_Transform(
          ST_Buffer(
            ST_Transform(
              ST_SetSRID(ST_MakePoint(a.${geoA.longColumn}, a.${geoA.latColumn}), 4326),
              3347
            ),
            ${geoA.radiusColumn ? `a.${geoA.radiusColumn}` : DEFAULT_RADIUS}
          ),
          4326
        )
      `)
    }

    if (geoB.geometryColumn) {
      sourceGeometriesB.push(`b.${geoB.geometryColumn}`)
      joinGeometriesB.push(`b.${geoB.geometryColumn}`)
    }
    if (geoB.latColumn && geoB.longColumn) {
      sourceGeometriesB.push(`ST_MakePoint(b.${geoB.longColumn}, b.${geoB.latColumn})`)
      joinGeometriesB.push(`
        ST_Transform(
          ST_Buffer(
            ST_Transform(
              ST_SetSRID(ST_MakePoint(b.${geoB.longColumn}, b.${geoB.latColumn}), 4326),
              3347
            ),
            ${geoB.radiusColumn ? `b.${geoB.radiusColumn}` : DEFAULT_RADIUS}
          ),
          4326
        )
      `)
    }

    viewsWithGeo[`__geo__${key}`] = knex
      .select({
        [geoTypeA]: knex.raw(geoA.idType === CAT_STRING
          ? `upper(a."${geoA.idColumn}")`
          : `a."${geoA.idColumn}"`),
        [geoTypeB]: knex.raw(geoB.idType === CAT_STRING
          ? `upper(b."${geoB.idColumn}")`
          : `b."${geoB.idColumn}"`),
      })
      .from({ a: `${geoA.schema}.${geoA.table}` })
      .where(function filterGeoA() {
        if (geoA.whitelabelColumn && whitelabel !== -1) {
          const customerFilter = geoA.customerColumn && customers !== -1
            ? `AND (
              a.${geoA.customerColumn} IS NULL
              OR a.${geoA.customerColumn} = ANY (:customers)
            )`
            : ''
          this.where(knex.raw(`(
            a.${geoA.whitelabelColumn} IS NULL
            OR (
              a.${geoA.whitelabelColumn} = ANY (:whitelabel)
              ${customerFilter}
            )
          )`, { whitelabel, customers }))
        }
        if (idFilterA.length) {
          this.where(knex.raw(`${
            geoA.idType === CAT_STRING ? `upper(a."${geoA.idColumn}")` : `a."${geoA.idColumn}"`
          } IN (${idFilterA.join(' UNION ')})`))
        }
        // make sure geometry is valid
        this.andWhere(knex.raw(`ST_IsValid(${
          sourceGeometriesA.length > 1
            ? `COALESCE(${sourceGeometriesA.join(', ')})`
            : sourceGeometriesA
        })`))
      })
      .join(
        { b: `${geoB.schema}.${geoB.table}` },
        function filterGeoBAndJoinWithGeoB() {
          if (geoB.whitelabelColumn && whitelabel !== -1) {
            const customerFilter = geoB.customerColumn && customers !== -1
              ? `AND (
                b.${geoB.customerColumn} IS NULL
                OR b.${geoB.customerColumn} = ANY (:customers)
              )`
              : ''
            this.on(knex.raw(`(
              b.${geoB.whitelabelColumn} IS NULL
              OR (
                b.${geoB.whitelabelColumn} = ANY (:whitelabel)
                ${customerFilter}
              )
            )`, { whitelabel, customers }))
          }
          if (idFilterB.length) {
            this.andOn(knex.raw(`${
              geoB.idType === CAT_STRING ? `upper(b."${geoB.idColumn}")` : `b."${geoB.idColumn}"`
            } IN (${idFilterB.join(' UNION ')})`))
          }
          this
            // make sure geometry is valid
            .andOn(knex.raw(`ST_IsValid(${
              sourceGeometriesB.length > 1
                ? `COALESCE(${sourceGeometriesB.join(', ')})`
                : sourceGeometriesB
            })`))
            // intersect geometries
            .andOn(knex.raw(`ST_Intersects(${joinGeometriesA.length > 1
              ? `COALESCE(${joinGeometriesA.join(', ')})`
              : joinGeometriesA
            }, ${joinGeometriesB.length > 1
              ? `COALESCE(${joinGeometriesB.join(', ')})`
              : joinGeometriesB
            })`))
        },
      )
  })

  // for each view requiring a join, determine geo id columns to expose and join condtions
  const viewJoins = {}
  // eslint-disable-next-line no-constant-condition
  while (true) {
    // place the join to the intersection table in the view that join to the highest number of
    // other views (same column & geo type)
    const { colJoinKey, size } = Object.entries(geoJoins).reduce(
      (largest, [colJoinKey, { joinKeys }]) => (
        joinKeys.size > largest.size ? { colJoinKey, size: joinKeys.size } : largest
      ),
      { size: 0 },
    )

    if (size === 0) {
      break
    }

    const [view, col, foreignGeoType] = colJoinKey.split('$')
    const { joinKeys, alias } = geoJoins[colJoinKey]
    const geoType = viewColumns[view][col].geo_type
    const geo = geoMapping[geoType]
    const foreignGeo = geoMapping[foreignGeoType]
    const intersectionKey = [geoType, foreignGeoType].sort().join('__')
    const geoTable = `__geo__${col}__${intersectionKey}`

    viewJoins[view] = viewJoins[view] || { cols: [], conditions: [] }
    // expose foreign view's geo id from intersection table
    viewJoins[view].cols.push({ [`__geo_id_${alias}`]: `${geoTable}.${foreignGeoType}` })
    // join with geo intersection table
    viewJoins[view].conditions.push([
      { [geoTable]: `__geo__${intersectionKey}` },
      `${geoTable}.${geoType}`,
      knex.raw(geo.idType === CAT_STRING
        ? `upper("__source__${view}"."${col}")`
        : `"__source__${view}"."${col}"`),
    ])

    // expose foreign view's geo id for 1:1 join with above
    joinKeys.forEach((joinKey) => {
      const [view, col] = joinKey.split('$')
      const { joinKeys, alias } = geoJoins[joinKey]

      // COMMENTED PART EXPOSES OWN GEO ID WITHOUT JOINING TO GEO INTERSECTION TABLE
      // HANGS WHEN LEFT JOIN
      // viewJoins[view] = viewJoins[view] || { cols: [], conditions: [] }
      // // expose view's own geo id
      // viewJoins[view].cols.push({
      //   [`__geo_id_${alias}`]: knex.raw(foreignGeo.idType === CAT_STRING
      //     ? `upper("__source__${view}"."${col}")`
      //     : `"__source__${view}"."${col}"`),
      // })
      // joinKeys.delete(colJoinKey)

      viewJoins[view] = viewJoins[view] || { cols: [], conditions: [] }
      // expose view's own geo id (normalized) from intersection table
      viewJoins[view].cols.push({ [`__geo_id_${alias}`]: `${geoTable}.${foreignGeoType}` })
      // join with geo intersection table
      viewJoins[view].conditions.push([
        { [geoTable]: `__geo__${intersectionKey}` },
        `${geoTable}.${foreignGeoType}`,
        // foreignGeo.idType === CAT_STRING ? 'ilike' : '=',
        // `__source__${view}.${col}`,
        knex.raw(foreignGeo.idType === CAT_STRING
          ? `upper("__source__${view}"."${col}")`
          : `"__source__${view}"."${col}"`),
      ])
      joinKeys.delete(colJoinKey)
    })

    joinKeys.clear()
  }

  // rewrite the views to expose the geo id's
  Object.entries(viewJoins).forEach(([view, { cols, conditions }]) => {
    const query = knex.select(`__source__${view}.*`, ...cols).from(`__source__${view}`)
    conditions.forEach(j => query.leftJoin(...j))

    // substitute view
    viewsWithGeo[view] = query
  })

  // insert macros
  // const expressionWithMacros = Object.keys(macros).length
  //   ? JSON.parse(Object.entries(macros).reduce(
  //     (expression, [macro, macroExpression]) => expression.replace(
  //       new RegExp(macro.replace(/[-/\\^$*+?.()|[\]{}]/g, '\\$&'), 'gi'),
  //       JSON.stringify(macroExpression).slice(1, -1), // remove head and tail quotes
  //     ),
  //     JSON.stringify(expressionWithGeo),
  //   ))
  //   : expressionWithGeo

  // return [expressionWithMacros, viewsWithGeo]
  return [expressionWithGeo, viewsWithGeo, fdwConnectionsWithGeo]
}

module.exports = {
  insertGeo,
  geoTypes,
  geoMapping,
}
