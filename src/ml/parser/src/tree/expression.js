/* eslint-disable no-use-before-define */
const {
  isString,
  isArray,
  isNonArrayObject,
  isObjectExpression,
  sanitizeString,
  parserError,
  expressionTypes: expTypes,
} = require('../utils')
const { isShortExpression, parseShortExpression } = require('../short')


const columnRefRE = /^\w+\.\w+$/

/** @type {Object.<string, (exp, context: { views, ctes, refs, params }) => Node} */
const objectParsers = {}

const parseObjectExpression = (exp, context) => {
  const type = sanitizeString(exp.type, true)
  const parser = objectParsers[type]
  if (!parser) {
    throw parserError(`Invalid object expression type: ${type}`)
  }
  return parser(exp, context)
}

const parseArrayExpression = (exp, context) => {
  // single expression: [expression]
  if (exp.length === 1) {
    return parseExpression(exp[0], context)
  }

  if (exp.length === 2) {
    const [column, view] = exp
    return parseExpression({ type: expTypes.COLUMN, column, view }, context)
  }

  if (exp.length === 3) {
    try {
      // column
      const [column, view, as] = exp
      return parseExpression({ type: expTypes.COLUMN, column, view, as }, context)
    } catch (_) {
      // condition/operator
      const [oA, operator, oB] = exp
      return parseExpression({ type: expTypes.OPERATOR, values: [operator, oA, oB] }, context)
    }
  }

  if (exp.length === 4) {
    // condition/operator
    const [argA, operator, argB, argC] = exp
    return parseExpression(
      { type: expTypes.OPERATOR, values: [operator, argA, argB, argC] },
      context,
    )
  }
  throw parserError('Invalid array expression')
}

const parseExpression = (exp, context) => {
  switch (typeof exp) {
    case 'string':
      if (exp.toLowerCase() === 'null') {
        return parseExpression(null, context)
      }
      if (isShortExpression(exp)) {
        return parseExpression({ type: expTypes.SHORT, value: exp }, context)
      }
      // try column
      if (columnRefRE.test(exp)) {
        try {
          const [column, view] = exp.split('.')
          return parseExpression({ type: expTypes.COLUMN, column, view }, context)
        } catch (_) {
          return parseExpression({ type: expTypes.PRIMITIVE, value: exp }, context)
        }
      }
      return parseExpression({ type: expTypes.PRIMITIVE, value: exp }, context)

    case 'boolean':
    case 'number':
      return parseExpression({ type: expTypes.PRIMITIVE, value: exp }, context)

    case 'object':
      // NULL value
      if (exp === null) {
        return parseExpression({ type: expTypes.PRIMITIVE, value: exp }, context)
      }
      // array expression
      if (isArray(exp)) {
        return parseArrayExpression(exp, context)
      }
      // object expression
      return parseObjectExpression(exp, context)

    default:
      throw parserError(`Invalid expression: ${JSON.stringify(exp)}`)
  }
}

// used for 'for' and 'joins'
const parseViewExpression = (exp, context) => {
  if (isString(exp)) {
    if (isShortExpression(exp, 'view')) {
      return parseShortExpression(exp)
    }
    return parseExpression({ type: expTypes.VIEW, view: exp }, context)
  }
  if (isObjectExpression(exp, expTypes.SELECT)) {
    return parseExpression({ ...exp, type: expTypes.SELECT_RANGE }, context)
  }
  if (isObjectExpression(exp, expTypes.VIEW) || isObjectExpression(exp, expTypes.SELECT_RANGE)) {
    return parseExpression(exp, context)
  }
  throw parserError(`Invalid view identifier/subquery syntax: ${JSON.stringify(exp)}`)
}

// used for 'with'
const parseCTEExpression = (exp, context) => {
  if (isObjectExpression(exp, expTypes.SELECT_CTE)) {
    return parseExpression(exp, context)
  }
  if (isObjectExpression(exp, expTypes.SELECT)) {
    return parseExpression({ ...exp, type: expTypes.SELECT_CTE }, context)
  }
  throw parserError(`Invalid with syntax: ${JSON.stringify(exp)}`)
}

const parseJoinExpression = (exp, context) => {
  if (isObjectExpression(exp, expTypes.JOIN)) {
    return parseExpression(exp, context)
  }
  if (isNonArrayObject(exp)) {
    return parseExpression({ ...exp, type: expTypes.JOIN }, context)
  }
  throw parserError(`Invalid join syntax: ${JSON.stringify(exp)}`)
}

module.exports = {
  objectParsers,
  parseExpression,
  parseViewExpression,
  parseCTEExpression,
  parseJoinExpression,
}
