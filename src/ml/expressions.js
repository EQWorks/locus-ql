/* eslint-disable no-continue */
/* eslint-disable no-use-before-define */

// comlpex expressions
// reference: https://github.com/EQWorks/firstorder/wiki/Locus-ML-Expression
const { knex } = require('../util/db')
const { apiError } = require('../util/api-error')
const { CAT_DATE, CAT_NUMERIC, CAT_JSON, CAT_STRING } = require('./type')


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

  // Geo functions
  geo_intersects: {
    value: 'ST_Intersects',
    category: CAT_STRING,
  },
  // geo_within: {
  //   value: 'ST_Within',
  //   category: CAT_STRING,
  // },
  // geo_population_ratio: {
  //   value: 'ST_Within',
  //   category: CAT_STRING,
  // },
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
  'is not': { value: 'is not' },
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

  // extracts explicit column
  extractColumn(expression) {
    let column
    let view
    let alias
    if (typeof expression === 'string' && expression.indexOf('.') !== -1) {
      [column, view] = expression.split('.', 2)
    } else if (typeof expression !== 'object' || expression === null) {
      return
    } else if (expression.type === 'column') {
      ({ column, view, as: alias } = expression)
    } else if (Array.isArray(expression) && [2, 3].includes(expression.length)) {
      [column, view, alias] = expression
    }

    if (alias === null) {
      alias = undefined
    }
    // make sure it's a valid column or wildcard
    if (!(
      view in this.viewColumns
      && (
        (column in this.viewColumns[view] && ['undefined', 'string'].includes(typeof alias))
        || (column === '*' && alias === undefined)
      )
    )) {
      return
    }
    return { view, column, alias }
  }

  constructColumn(expression) {
    const col = this.extractColumn(expression)
    if (col) {
      return knex.raw(`"${col.view}".${
        col.column === '*'
          ? '*'
          : `"${col.column}"${col.alias ? ` AS "${col.alias}"` : ''}`
      }`)
    }
  }

  parseComplex({ type, ...exp }) {
    if (type === 'column') {
      const col = this.constructColumn({ type, ...exp })
      if (!col) {
        throw apiError('Malformed column object', 400)
      }
      return col
    }

    if (type === 'array') {
      const values = exp.values.reduce((acc, val, i) => {
        acc[`value_${i}`] = val
        return acc
      }, {})
      return knex.raw(`(${exp.values.map((_, i) => `:value_${i}`).join(', ')})`, values)
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

      const [argA, argB, argC] = args.map(this.parseExpression.bind(this))

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
      const whenStatements = statements.map(statement => `
        WHEN ${this.parseExpression(statement[0])}
        THEN ${this.parseExpression(statement[1])}
      `)
      return knex.raw(`
        CASE ${whenStatements.join(' ')}
        ELSE ${this.parseExpression(defaultResult)} END
      `)
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
      // check if column
      return this.constructColumn(expression) || knex.raw(`'${expression}'`)
    }

    if (type === TYPE_NUMBER) {
      return expression
    }

    if (type === TYPE_OBJECT) {
      // NULL value
      if (expression === null) {
        return knex.raw('NULL')
      }
      // column shorthand
      if (Array.isArray(expression)) {
        const col = this.constructColumn(expression)
        if (!col) {
          throw apiError('Malformed column array', 400)
        }
        return col
      }

      return this.parseComplex(expression)
    }

    throw apiError(`Invalid expression: ${expression}`, 403)
  }

  parseConditions(conditions, knex, knexRaw) {
    if (typeof conditions !== 'object' || conditions === null) {
      return
    }
    // conditions is an array
    if (Array.isArray(conditions)) {
      return conditions.forEach((condition) => {
        if (!condition) {
          return
        }
        // handle easy array form condition
        // where condition is in format of: [argA[, operator, argB]]
        if (Array.isArray(condition)) {
          const [argA, operator, argB] = condition
          const parsedArgA = typeof argA === 'object' ? this.parseExpression(argA) : argA
          const parsedArgB = typeof argB === 'object' ? this.parseExpression(argB) : argB
          const args = [parsedArgA]
          if (operator && parsedArgB !== undefined) {
            args.push(operator, parsedArgB)
          }
          return knex(...args)
        }
        // handle complex condition
        return (knexRaw || knex)(this.parseExpression(condition))
      })
    }
    // conditions is an object (e.g. 'AND')
    return (knexRaw || knex)(this.parseExpression(conditions))
  }
}

module.exports = {
  functions,
  Expression,
}
