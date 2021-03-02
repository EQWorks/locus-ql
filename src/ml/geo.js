/* eslint-disable no-continue */
const { knex } = require('../util/db')
const { apiError } = require('../util/api-error')


const TYPE_STRING = 'string'
const TYPE_NUMBER = 'number'
const DEFAULT_POINT_RADIUS = 20

/**
 * @enum
 */
const geoTypes = {
  CA_FSA: 'ca-fsa',
  CA_DA: 'ca-da',
  CA_CT: 'ca-ct',
  CA_POSTALCODE: 'ca-postalcode',
  CA_PROVINCE: 'ca-province',
  CA_CITY: 'ca-city',
  POI: 'poi',
}

const geoMapping = {
  [geoTypes.CA_FSA]: {
    schema: 'canada_geo',
    table: 'fsa',
    idType: TYPE_STRING,
    idColumn: 'fsa',
    geometryColumn: 'wkb_geometry',
  },
  [geoTypes.CA_DA]: {
    schema: 'canada_geo',
    table: 'da',
    idType: TYPE_NUMBER,
    idColumn: 'dauid',
    geometryColumn: 'wkb_geometry',
  },
  [geoTypes.CA_CT]: {
    schema: 'canada_geo',
    table: 'ct',
    idType: TYPE_NUMBER,
    idColumn: 'ctuid',
    geometryColumn: 'wkb_geometry',
  },
  [geoTypes.CA_POSTALCODE]: {
    schema: 'canada_geo',
    table: 'postalcode_2018',
    idType: TYPE_STRING,
    idColumn: 'postalcode',
    geometryColumn: 'wkb_geometry',
  },
  [geoTypes.CA_PROVINCE]: {
    schema: 'canada_geo',
    table: 'province',
    idType: TYPE_STRING,
    idColumn: 'province_code',
    geometryColumn: 'wkb_geometry',
  },
  [geoTypes.CA_CITY]: {
    schema: 'canada_geo',
    table: 'city',
    idType: TYPE_STRING,
    idColumn: 'city',
    geometryColumn: 'wkb_geometry',
  },
  [geoTypes.POI]: {
    schema: 'public',
    table: 'poi',
    idType: TYPE_NUMBER,
    idColumn: 'poi_id',
    geometryColumn: 'polygon',
    pointColumn: 'display_point',
    radiusColumn: 'default_radius',
  },
}

const makeGeoOverlapMacro = (colA, colB) => ({
  key: `[GEO_OVERLAP_PCT|${`${colA.view}.${colA.key}|${colB.view}.${colB.key}`.toUpperCase()}]`,
  expression: colA.geo_type === colB.geo_type
    ? `((${colA.view}.${colA.key} = ${colB.view}.${colB.key}) OR FALSE)::int`
    : `
      ST_Area(
        ST_Intersection(${colA.view}._${colA.key}_geometry, ${colB.view}._${colB.key}_geometry)
      ) / ST_Area(${colB.view}._${colB.key}_geometry)`,
})

// extracts explicit column
// does not handle wildcard *
const extractColumn = (viewColumns, expression) => {
  let column
  let view
  if (typeof expression === 'string' && expression.indexOf('.') !== -1) {
    [column, view] = expression.split('.', 2)
  } else if (typeof expression !== 'object' || expression === null) {
    return
  } else if (expression.type === 'column') {
    ({ column, view } = expression)
  } else if (Array.isArray(expression) && expression.length === 2) {
    [column, view] = expression
  }
  if (!(view in viewColumns && column in viewColumns[view])) {
    return
  }

  return {
    ...viewColumns[view][column],
    view,
  }
}

// substitue geo id with geometry when there are geo intersections of different types
// return list of affected geo columns (view_id, column name, geo type) + expression with substitutions
// returns list of necessary joins + new expression
const insertGeo = (views, viewColumns, expression) => {
  // make copy of expression using JSON stringify + parse so as to not mutate the original object
  const expressionWithGeo = JSON.parse(JSON.stringify(expression))
  const joins = {}
  const macros = {}
  const queue = [expressionWithGeo]
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
      && ['geo_intersects', 'geo_within'].includes(item.values[0])
    ) {
      [, argA, argB] = item.values
      // args must be 2 geo columns
      throwOnNonGeoColumn = true
    // else push object values to queue for processing
    } else {
      queue.push(...Object.values(item))
      continue
    }

    const colA = extractColumn(viewColumns, argA)
    if (!colA) {
      if (throwOnNonGeoColumn) {
        throw apiError(`Argument must be of type geo column: ${argA}`, 400)
      }
      queue.push(argA)
    }
    const colB = extractColumn(viewColumns, argB)
    if (!colB) {
      if (throwOnNonGeoColumn) {
        throw apiError(`Argument must be of type geo column: ${argB}`, 400)
      }
      queue.push(argB)
    }
    // proceed with geo join if argA and argB are geo columns of different types
    if (!colA || !colB) {
      continue
    }

    if (!(colA.geo_type in geoMapping) || !(colB.geo_type in geoMapping)) {
      if (throwOnNonGeoColumn) {
        throw apiError(`Argument(s) must be of type geo column: ${argB}, ${argB}`, 400)
      }
      continue
    }

    // add cols to joins and prepare intersect macros
    [colA, colB].forEach((col) => {
      // if not same geo type, need to join with geo to get geometry
      if (colA.geo_type !== colB.geo_type) {
        joins[col.view] = joins[col.view] || {}
        joins[col.view][col.key] = geoMapping[col.geo_type]
      }

      const { key, expression } = makeGeoOverlapMacro(col, col === colA ? colB : colA)
      macros[key] = expression
    })

    // if different geo type need to change equality condition to geo intersection
    if (colA.geo_type === colB.geo_type) {
      continue
    }

    // create new join condition
    const condition = {
      type: 'function',
      values: [
        'geo_intersects',
        `${colA.view}._${colA.key}_geometry`,
        `${colB.view}._${colB.key}_geometry`,
      ],
    }

    // mutate expression object
    if (Array.isArray(item)) {
      item.splice(0, item.length, condition)
      continue
    }
    Object.assign(item, condition)
  }

  // rewrite the views to add the geometries
  const viewsWithGeo = Object.entries(joins).reduce((views, [view, cols]) => {
    const [geoCols, joinConditions] = Object.entries(cols).reduce((acc, [col, geo]) => {
      const outGeometries = []
      const inGeometries = []
      if (geo.geometryColumn) {
        outGeometries.push(`${col}_geo.${geo.geometryColumn}`)
        inGeometries.push(`${col}_geo.${geo.geometryColumn}`)
      }
      if (geo.pointColumn) {
        outGeometries.push(`
          ST_Transform(
            ST_Buffer(
              ST_Transform(
                ST_SetSRID(${col}_geo.${geo.pointColumn}, 4326),
                3347
              ),
              ${geo.radiusColumn ? `${col}_geo.${geo.radiusColumn}` : DEFAULT_POINT_RADIUS}
            ),
            4326
          )
        `)
        inGeometries.push(`${col}_geo.${geo.pointColumn}`)
      }
      // geo column with alias
      acc[0].push({ [`_${col}_geometry`]: knex.raw(`COALESCE(${outGeometries.join(', ')})`) })
      // join with geo table
      acc[1].push([
        `${geo.schema}.${geo.table} AS ${col}_geo`,
        function joinOnValidGeo() {
          this.on(
            `${col}_geo.${geo.idColumn}`,
            geo.idType === TYPE_STRING ? 'ilike' : '=',
            `${view}.${col}`,
          ).andOn(knex.raw(`ST_IsValid(COALESCE(${inGeometries.join(', ')}))`)) // make sure
          // geometry is valid
        },
      ])
      return acc
    }, [[], []])

    const query = knex.select(`${view}.*`, ...geoCols).from(views[view]).as(view)
    joinConditions.forEach(j => query.leftJoin(...j))

    // substitute view
    views[view] = query
    return views
  }, { ...views }) // shallow copy

  // insert macros
  const expressionWithMacros = Object.keys(macros).length
    ? JSON.parse(Object.entries(macros).reduce(
      (expression, [macro, macroExpression]) => expression.replace(
        new RegExp(macro.replace(/[-/\\^$*+?.()|[\]{}]/g, '\\$&'), 'gi'),
        JSON.stringify(macroExpression).slice(1, -1), // remove head and tail quotes
      ),
      JSON.stringify(expressionWithGeo),
    ))
    : expressionWithGeo

  return [expressionWithMacros, viewsWithGeo]
}

module.exports = { insertGeo }
