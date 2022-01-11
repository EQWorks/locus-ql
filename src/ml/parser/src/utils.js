/* eslint-disable no-continue */

// TODO: catToTypeMap

// public parser errors
class ParserError extends Error {}
const parserError = args => new ParserError(args)

/**
 * @enum
 */
const expressionTypes = {
  SELECT: 'select',
  SELECT_CTE: 'cte',
  SELECT_RANGE: 'view_select',
  JOIN: 'join',
  VIEW: 'view',
  COLUMN: 'column',
  PARAMETER: 'parameter',
  CAST: 'cast',
  PRIMITIVE: 'primitive',
  SQL: 'sql',
  CASE: 'case',
  ARRAY: 'array',
  LIST: 'list',
  FUNCTION: 'function',
  SORT: 'sort',
  OPERATOR: 'operator',
  AND: 'and',
  OR: 'or',
}

// reverse lookup
const expressionTypeValues = Object.entries(expressionTypes).reduce((acc, [k, v]) => {
  acc[v] = k
  return acc
}, {})

const isString = (val, nonEmpty = false) => typeof val === 'string' && (!nonEmpty || val !== '')

// returns undefined is not string else lower case string
const sanitizeString = (val, nonEmpty = false) => {
  if (!isString(val, nonEmpty)) {
    return
  }
  return val.toLowerCase()
}

const isNull = val => val === undefined || val === null
const isNonNull = val => !isNull(val)

const isArray = (val, { minLength, maxLength, length } = {}) => {
  if (!Array.isArray(val)) {
    return false
  }
  if (length !== undefined) {
    return val.length === length
  }
  return (!minLength || val.length >= minLength)
    && (maxLength === undefined || val.length <= maxLength)
}
const isNonArrayObject = val => typeof val === 'object' && val !== null && !isArray(val)
// const isBool = (val) => typeof val === 'boolean'
// const isNumber = (val, minVal) => typeof val === 'number' && (!minVal || val >= minVal)
const isInt = (val, minVal) => Number.isInteger(val) && (!minVal || val >= minVal)

const isObjectExpression = (val, type) => {
  if (typeof val !== 'object' || val === null) {
    return false
  }
  const safeType = sanitizeString(val.type, true)
  return type ? safeType === type : safeType in expressionTypeValues
}

const sanitizeAlias = (val) => {
  if (isNonNull(val) && (!isString(val, true) || val.startsWith('__'))) {
    throw parserError(`Invalid alias: ${val}`)
  }
  return val || undefined
}

const sanitizeCast = (val) => {
  if (isNull(val)) {
    return
  }
  const safeCast = sanitizeString(val)
  // if (!(safeCast in catToTypeMap)) {
  //   throw parserError(`Invalid cast type: ${val}`)
  // }
  return safeCast
}

// inspired by https://github.com/datalanche/node-pg-format/blob/master/lib/index.js
const escapeLiteral = (val) => {
  let escaped = ''
  let hasBackslash = false
  for (const char of val) {
    if (char === "'") {
      escaped += "''"
      continue
    }
    if (char === '\\') {
      escaped += '\\\\'
      hasBackslash = true
      continue
    }
    escaped += char
  }
  return hasBackslash ? `E'${escaped}'` : `'${escaped}'`
}

const escapeIdentifier = (val) => {
  let escaped = ''
  for (const char of val) {
    if (char === '"') {
      escaped += '""'
      continue
    }
    escaped += char
  }
  return `"${escaped}"`
}

// removes new lines, double/leading/trailing spaces
// except in literals and identifiers
const trimSQL = (sql) => {
  let trimmed = ''
  let leadingSpace = false
  let quote = ''
  let quoteEnding = false
  for (const char of sql) {
    // quote in progress
    if (quote) {
      // quote ending or escaped quote char
      if (char === quote) {
        quoteEnding = !quoteEnding
      // end quote
      } else if (quoteEnding) {
        quoteEnding = false
        quote = ''
      }
    }
    // no quote or quote has just ended
    if (!quote) {
      // remove new lines + extra spaces when not inside quote
      if (char === '\n' || char === ' ') {
        if (!leadingSpace && trimmed && trimmed.slice(-1) !== '(') {
          leadingSpace = true
        }
        continue
      }
      // new quote starting
      if (char === '"' || char === "'") {
        quote = char
      }
    }
    // add leading space
    if (leadingSpace && char !== ')') {
      trimmed += ' '
      leadingSpace = false
    }
    trimmed += char
  }
  return trimmed
}

module.exports = {
  isNull,
  isNonNull,
  isString,
  isArray,
  isNonArrayObject,
  isInt,
  isObjectExpression,
  sanitizeString,
  sanitizeAlias,
  sanitizeCast,
  escapeLiteral,
  escapeIdentifier,
  trimSQL,
  ParserError,
  parserError,
  expressionTypes,
  expressionTypeValues,
}
