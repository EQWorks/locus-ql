/* eslint-disable no-use-before-define */

// comlpex expressions
// reference: https://github.com/EQWorks/firstorder/wiki/Locus-ML-Expression
const { knex } = require('../util/db')
const apiError = require('../util/api-error')


const TYPE_STRING = 'string'
const TYPE_OBJECT = 'object'
const TYPE_NUMBER = 'number'

const functions = {
  // aggregation functions
  sum: {
    value: 'sum',
    category: 'Numeric',
    defaultCast: 'real',
  },
  count: {
    value: 'count',
    // category: '',
  },
  avg: {
    value: 'avg',
    category: 'Numeric',
    defaultCast: 'real',
  },
  min: {
    value: 'min',
    category: 'Numeric',
    defaultCast: 'real',
  },
  max: {
    value: 'max',
    category: 'Numeric',
    defaultCast: 'real',
  },
}

const operators = {
  // logic operators
  and: { value: 'and' },
  or: { value: 'or' },

  // comparison operators
  '>': { value: '>' },
  '>=': { value: '>=' },
  '<': { value: '<' },
  '<=': { value: '<=' },
  '=': { value: '=' },
  in: { value: 'in' },
  'not in': { value: 'not in' },
  like: { value: 'like' },
  'not like': { value: 'not like' },
  'is null': { value: 'is null' },
  'is not null': { value: 'is not null' },

  // arithmatic operators
  '+': { value: '+' },
  '-': { value: '-' },
  '*': { value: '*' },
  '/': { value: '/' },
}

const parseComplex = ({ type, ...exp }) => {
  if (type === 'column') {
    // TODO: validate against view and column?
    const { view, column } = exp
    return `${view}.${column}`
  }

  if (type === 'function') {
    const { values: [funcName, ...args], cast, as } = exp
    const func = functions[funcName]
    if (!func) {
      throw apiError(`Invalid function: ${funcName}`, 403)
    }
    const castTo = cast || func.defaultCast

    const [argA] = args
    // const { category } = func // check argument with category

    // e.g. SUM(visits)::real as visits
    // eslint-disable-next-line max-len
    return knex.raw(`${func.value}(${parseExpression(argA)})${castTo ? `::${castTo}` : ''}${as ? ` as "${as}"` : ''}`)
  }

  if (type === 'operator') {
    const { values: [opName, ...args], cast, as } = exp
    const op = operators[opName]
    if (!op) {
      throw apiError(`Invalid operator: ${opName}`, 403)
    }

    const [argA, argB] = args.map(exp => parseExpression(exp))

    // eslint-disable-next-line max-len
    return knex.raw(`(${argA} ${op.value} ${argB})${cast ? `::${cast}` : ''}${as ? ` as "${as}"` : ''}`)
  }
}

const parseExpression = (expression) => {
  const type = typeof expression

  if (type === TYPE_STRING) {
    return `'${expression}'`
  }

  if (type === TYPE_NUMBER) {
    return expression
  }

  if (type === TYPE_OBJECT) {
    // column shorthand
    if (Array.isArray(expression)) {
      if (expression.length === 0) {
        throw apiError('Empty column array', 403)
      } else {
        // TODO: validate against view and column
        const [column, view] = expression
        return `${view}.${column}`
      }
    }

    return parseComplex(expression)
  }
}

module.exports = {
  functions,
  parseExpression,
}
