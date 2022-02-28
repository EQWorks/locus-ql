const { useAPIErrorOptions } = require('../util/api-error')
const { parseQueryToTree, ParserError } = require('./parser')


const { apiError, getSetAPIError } = useAPIErrorOptions({ tags: { service: 'ql' } })
const columnRefRE = /^\w+\.\w+$/

const extractGeoColumn = (viewColumns, exp) => {
  let column
  let view
  if (columnRefRE.test(exp)) {
    [column, view] = exp.split('.', 2)
  } else if (typeof exp !== 'object' || exp === null) {
    return
  } else if (typeof exp.type === 'string' && exp.type.toLowerCase() === 'column') {
    ({ column, view } = exp)
  } else if (Array.isArray(exp) && [2, 3].includes(exp.length)) {
    [column, view] = exp
  }
  if (!(
    view in viewColumns
    && column in viewColumns[view]
    && 'geo_type' in viewColumns[view][column]
  )) {
    // not a geo column
    return
  }
  return { view, column, geoType: viewColumns[view][column].geo_type }
}

// return { expression, inserted: true|false }
const insertGeoIntersects = (viewColumns, exp) => {
  // look for geo intersections
  // if not object, return copy
  if (typeof exp !== 'object' || exp === null) {
    return { expression: exp, inserted: false }
  }
  let argA
  let argB
  // check if array condition
  if (Array.isArray(exp) && exp.length === 3 && exp[1] === '=') {
    [argA, , argB] = exp
  // check if operator object
  } else if (exp.type === 'operator' && exp.values.length === 3 && exp.values[0] === '=') {
    [, argA, argB] = exp.values
  // else dissect values
  } else {
    if (Array.isArray(exp)) {
      return exp.reduce((acc, v) => {
        const { expression, inserted } = insertGeoIntersects(viewColumns, v)
        acc.expression.push(expression)
        acc.inserted = acc.inserted || inserted
        return acc
      }, { expression: [], inserted: false })
    }
    return Object.entries(exp).reduce((acc, [k, v]) => {
      const { expression, inserted } = insertGeoIntersects(viewColumns, v)
      acc.expression[k] = expression
      acc.inserted = acc.inserted || inserted
      return acc
    }, { expression: {}, inserted: false })
  }
  const [valA, valB] = [argA, argB].map((arg) => {
    const col = extractGeoColumn(viewColumns, arg)
    return col
      ? { geoType: col.geoType, expression: arg, inserted: false }
      : insertGeoIntersects(viewColumns, arg)
  })
  // at least one is non geo
  if (!valA.geoType || !valB.geoType) {
    return {
      expression: {
        ...(Array.isArray(exp) ? { type: 'operator' } : exp),
        values: ['=', valA.expression, valB.expression],
      },
      inserted: valA.inserted || valB.inserted,
    }
  }
  // replace with geo_intersects
  const [geoA, geoB] = [valA, valB].map(({ geoType, expression }) => ({
    type: 'function',
    values: ['geometry', geoType, expression],
  }))
  return {
    expression: {
      type: 'function',
      values: ['geo_intersects', geoA, geoB],
    },
    inserted: true,
  }
}

const insertGeoIntersectsInTree = (views, tree) => {
  const { parameters } = tree._context.options
  const ql = tree.toQL({ keepShorts: false, keepParamRefs: false })
  const viewColumns = Object.entries(views).reduce((acc, [id, { columns }]) => {
    acc[id] = columns
    return acc
  }, {})
  const { expression: qlWithGeo, inserted } = insertGeoIntersects(viewColumns, ql)
  // need to regenerate tree if geo inserted
  return inserted ? parseQueryToTree(qlWithGeo, { type: 'ql', parameters }) : tree
}

const insertGeoIntersectsInTreeMW = (req, _, next) => {
  try {
    const { views, tree } = req.ql
    req.ql.tree = insertGeoIntersectsInTree(views, tree)
    next()
  } catch (err) {
    if (err instanceof ParserError) {
      return next(apiError(err.message, 400))
    }
    next(getSetAPIError(err, 'Failed to insert geo intersects in query', 500))
  }
}

module.exports = {
  insertGeoIntersects,
  insertGeoIntersectsInTree,
  insertGeoIntersectsInTreeMW,
}
