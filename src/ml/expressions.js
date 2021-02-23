/* eslint-disable no-use-before-define */

// comlpex expressions
// reference: https://github.com/EQWorks/firstorder/wiki/Locus-ML-Expression
const { knex } = require('../util/db')
const { apiError } = require('../util/api-error')
const { CAT_DATE, CAT_NUMERIC, CAT_JSON } = require('./type')


const TYPE_STRING = 'string'
const TYPE_OBJECT = 'object'
const TYPE_NUMBER = 'number'

const functions = {
  // aggregation functions
  sum: {
    value: 'sum',
    category: CAT_NUMERIC,
    defaultCast: 'real',
  },
  count: {
    value: 'count',
    // category: '',
  },
  avg: {
    value: 'avg',
    category: CAT_NUMERIC,
    defaultCast: 'real',
  },
  min: {
    value: 'min',
    category: CAT_NUMERIC,
    defaultCast: 'real',
  },
  max: {
    value: 'max',
    category: CAT_NUMERIC,
    defaultCast: 'real',
  },
  minDate: {
    value: 'min',
    category: CAT_NUMERIC,
    defaultCast: 'date',
  },
  maxDate: {
    value: 'max',
    category: CAT_NUMERIC,
    defaultCast: 'date',
  },

  round: {
    value: 'round',
    category: CAT_NUMERIC,
  },

  // time/date functions
  // field can be year, month, day, hour etc
  date_part: { // date_part(field, timestamp)
    value: 'date_part',
    category: CAT_NUMERIC,
  },
  date_trunc: { // date_trunc(field, timestamp)
    value: 'date_trunc',
    category: CAT_DATE,
  },

  // JSON functions
  json_extract_path: { // json_extract_path(field, key)
    value: 'json_extract_path',
    category: CAT_JSON,
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
  is: { value: 'is' },
  between: { value: 'between' },
  'not between': { value: 'not between' },

  // arithmatic operators
  '+': { value: '+' },
  '-': { value: '-' },
  '*': { value: '*' },
  '/': { value: '/' },

  // JSON operators
  'json array element at': { value: '->' },
  'json object field with key': { value: '->' },
  'json array element as text at': { value: '->>' },
  'json object field as text with key': { value: '->>' },
  'json object at path': { value: '#>' },
  'json object as text at path': { value: '#>>' },
}

class Expression {
  constructor(viewColumns) {
    this.viewColumns = viewColumns
  }

  constructColumn(view, column) {
    const validView = (this.viewColumns || {})[view]

    // validate view and column, skip checks if no viewColumns as a fall back
    if (this.viewColumns && !validView) {
      throw apiError(`Invalid view for column: ${view} ${column}`, 403)
    }
    if (this.viewColumns && column !== '*' && !validView[column]) {
      throw apiError(`Column: ${column} not found for view: ${view}`, 403)
    }
    return `${view}.${column}`
  }

  parseComplex({ type, ...exp }) {
    if (type === 'column') {
      return this.constructColumn(exp.view, exp.column)
    }

    if (type === 'array') {
      return exp.values
    }

    if (type === 'function') {
      const { values: [funcName, ...args], cast, as } = exp
      const func = functions[funcName]
      if (!func) {
        throw apiError(`Invalid function: ${funcName}`, 403)
      }
      const castToVal = cast || func.defaultCast || false
      const castTo = castToVal ? `::${castToVal}` : ''
      const alias = as ? ` as "${as}"` : ''

      // const { category } = func // check argument with category

      const argsString = args.map(this.parseExpression.bind(this)).join(', ')


      // e.g. SUM(visits)::real as visits
      // eslint-disable-next-line max-len
      return knex.raw(`${func.value}(${argsString})${castTo}${alias}`)
    }

    if (type === 'operator') {
      const { values: [opName, ...args], cast, as } = exp
      const op = operators[opName]
      const castTo = cast ? `::${cast}` : ''
      const alias = as ? ` as "${as}"` : ''
      if (!op) {
        throw apiError(`Invalid operator: ${opName}`, 403)
      }

      const [argA, argB, argC = null] = args.map(this.parseExpression.bind(this))

      if ((op.value === 'between' || op.value === 'not between') && !argC) {
        throw apiError(`Too few arguments for operator: ${opName}`, 403)
      }

      const thirdArgument = argC ? 'AND :argC' : ''
      // eslint-disable-next-line max-len
      return knex.raw(`(:argA: ${op.value} :argB ${thirdArgument})${castTo}${alias}`, {
        argA,
        argB,
        argC,
      })
    }

    if (type === 'case') {
      // values: [defaultValue,[expression1, result1], [expression2, result2]]
      const { values: [defaultResult, ...statements] } = exp
      // eslint-disable-next-line max-len
      const whenStatements = statements.map(statement => `WHEN ${this.parseExpression(statement[0])} THEN ${statement[1]} `)
      return knex.raw(`CASE ${whenStatements.join(' ')} ELSE ${defaultResult} END`)
    }

    if (['AND', 'OR'].includes(type)) {
      const { values: [expA, expB] } = exp
      return knex.raw(`(${this.parseExpression(expA)}) ${type} (${this.parseExpression(expB)})`)
    }

    throw apiError(`Invalid expression type: ${type}`, 403)
  }

  parseExpression(expression) {
    const type = typeof expression

    if (type === TYPE_STRING) {
      return expression
    }

    if (type === TYPE_NUMBER) {
      return expression
    }

    if (type === TYPE_OBJECT) {
      // column shorthand
      if (Array.isArray(expression)) {
        if (expression.length < 2) {
          throw apiError('Column array requires 2 elements', 403)
        } else {
          return this.constructColumn(expression[1], expression[0])
        }
      }

      return this.parseComplex(expression)
    }

    throw apiError(`Invalid expression: ${expression}`, 403)
  }
}


module.exports = {
  functions,
  Expression,
}
