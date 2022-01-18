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
  SELECT_CTE: 'select_cte',
  SELECT_RANGE: 'select_range',
  JOIN: 'join',
  VIEW: 'view',
  COLUMN: 'column',
  PARAMETER: 'parameter',
  SHORT: 'short',
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
const isInt = (val, minVal) => Number.isInteger(val) && (minVal === undefined || val >= minVal)

const isObjectExpression = (val, type) => {
  if (!isNonArrayObject(val)) {
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

const csvBlockChars = {
  '[': ']', // array
  '(': ')', // function
  '{': '}', // object
}

// splits csv values into array
const splitCSV = (csv) => {
  const vals = []
  let val = ''
  let quote = ''
  let quoteEnding = false
  let block = ''
  let blockEnd = ''
  let blockDepth = 0
  for (const char of csv) {
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

    // no quote or quote has just ended
    // space
    if (char === '\n' || char === ' ') {
      continue
    }

    // new quote starting
    if (char === '"' || char === "'") {
      quote = char
      val += char
      continue
    }

    if (!block) {
      // end of val
      if (char === ',') {
        vals.push(val)
        val = ''
        continue
      }
      // new block starting
      if (char in csvBlockChars) {
        block = char
        blockEnd = csvBlockChars[char]
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

  return vals
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

// add leading + trailing brackets, as needed
const wrapSQL = sql => (sql[0] === '(' && sql[sql.length - 1] === ')' ? sql : `(${sql})`)

// identify shorts and replace by $<index>
// identify user supplied params and throw error
// remove spaces and new lines
const extractShortExpressionsFromSQL = (sql) => {
  const shorts = []
  let sqlWithoutShorts = ''
  let quote = ''
  let quoteEnding = false
  let shortName = ''
  let shortArgs = ''
  let shortDepth = 0
  for (const char of sql) {
    // quote in progress
    if (quote) {
      // quote continues
      if (!quoteEnding || char === quote) {
        if (char === quote) {
          quoteEnding = !quoteEnding
        }
        // quote within short
        if (shortArgs) {
          shortArgs += char
          continue
        }
        sqlWithoutShorts += char
        continue
      }
      // quote ends, need to deal with char
      quote = ''
      quoteEnding = false
    }

    // no quote or quote has just ended
    // new quote starting
    if (char === '"' || char === "'") {
      quote = char
      // quote within short
      if (shortName) {
        // quote is in short args
        if (shortDepth) {
          shortArgs += char
          continue
        }
        // still working on short name = invalid short
        sqlWithoutShorts += shortName
        shortName = ''
      }
      sqlWithoutShorts += char
      continue
    }

    if (!shortName) {
      if (char === '$') {
        throw parserError('$ parameter syntax not supported')
      }
      // nothing short related
      if (char !== '@') {
        sqlWithoutShorts += char
        continue
      }
      // new short starting
      shortName = '@'
      continue
    }

    // short in progress
    // set name
    if (!shortDepth) {
      // done with the name, starting args
      if (char === '(' && shortName.length > 1) {
        // validate name ?
        shortDepth += 1
        shortArgs += char
        continue
      }
      // invalid name
      const charCode = char.charCodeAt(0)
      if (!(
        (charCode >= 48 && charCode <= 57) // 0-9
        || (charCode >= 65 && charCode <= 90) // A-Z
        || (charCode >= 97 && charCode <= 122) // a-z
        || char === '_' // _
      )) {
        sqlWithoutShorts += shortName + char
        shortName = ''
        continue
      }
      shortName += char
      continue
    }
    // working on args
    // space
    if (char === '\n' || char === ' ') {
      continue
    }
    shortArgs += char

    // new block starting
    if (char === '(') {
      shortDepth += 1
      continue
    }

    // block ending
    if (char === ')') {
      shortDepth -= 1
      // end of short
      if (!shortDepth) {
        shorts.push(shortName + shortArgs)
        shortName = ''
        shortArgs = ''
        // replace value with $ shorts.length
        sqlWithoutShorts += `$${shorts.length}`
      }
    }
  }
  // add residual
  sqlWithoutShorts += shortName + shortArgs
  return { sql: sqlWithoutShorts, shorts }
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
  splitCSV,
  trimSQL,
  wrapSQL,
  extractShortExpressionsFromSQL,
  ParserError,
  parserError,
  expressionTypes,
  expressionTypeValues,
}
