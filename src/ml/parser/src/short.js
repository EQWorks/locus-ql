const {
  parserError,
  ParserError,
  expressionTypes,
  isNonArrayObject,
  isArray,
  isString,
  splitCSV,
} = require('./utils')


const shortExpressions = {
  param: {
    template: ['name', 'as', 'cast'],
    parser: ({ name, as, cast }) => ({ type: expressionTypes.PARAMETER, value: name, as, cast }),
  },
  view: {
    template: ['name', 'as'],
    parser: ({ name, as }) => ({ type: expressionTypes.VIEW, view: name, as }),
  },
  column: {
    template: ['column', 'view', 'as', 'cast'],
    parser: ({ column, view, as, cast }) =>
      ({ type: expressionTypes.COLUMN, column, view, as, cast }),
  },
  function: {
    template: ['name', 'args', 'as', 'cast'],
    parser: ({ name, args = [], as, cast }) => {
      // if (!isString(name, true) || !isArray(args)) {
      if (!isArray(args)) {
        throw parserError('Invalid arguments supplied to @fn')
      }
      return { type: expressionTypes.FUNCTION, values: [name, ...args], as, cast }
    },
  },
  array: {
    template: ['values', 'as', 'cast'],
    parser: ({ values = [], as, cast }) => {
      if (!isArray(values)) {
        throw parserError('Invalid arguments supplied to @array')
      }
      return { type: expressionTypes.ARRAY, values, as, cast }
    },
  },
  list: {
    template: ['values', 'as', 'cast'],
    parser: ({ values = [], as, cast }) => {
      if (!isArray(values)) {
        throw parserError('Invalid arguments supplied to @list')
      }
      return { type: expressionTypes.LIST, values, as, cast }
    },
  },
  cast: {
    template: ['value', 'cast', 'as'],
    parser: ({ value, cast, as }) => ({ type: expressionTypes.CAST, value, as, cast }),
  },
  primitive: {
    template: ['value', 'cast', 'as'],
    parser: ({ value, cast, as }) => ({ type: expressionTypes.PRIMITIVE, value, as, cast }),
  },
  date: {
    template: ['year', 'month', 'day', 'as'],
    parser: ({ year, month, day, as }) => {
      if (![year, month, day].every(Number.isInteger)) {
        throw parserError('Invalid arguments supplied to @date')
      }
      const value = `${year}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}`
      if (Number.isNaN(Date.parse(value))) {
        throw parserError('Invalid arguments supplied to @date')
      }
      return { type: expressionTypes.CAST, value, as, cast: 'date' }
    },
  },
  datetime: {
    template: ['year', 'month', 'day', 'hour', 'minute', 'second', 'tz', 'as'],
    parser: ({
      year, month, day,
      hour = 0, minute = 0, second = 0,
      tz = 'America/Toronto', as,
    }) => {
      if (![year, month, day, hour, minute, second].every(Number.isInteger) || !isString(tz)) {
        throw parserError('Invalid arguments supplied to @datetime')
      }
      const date = `${year}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}`
      // eslint-disable-next-line max-len
      const time = `${String(hour).padStart(2, '0')}:${String(minute).padStart(2, '0')}:${String(second).padStart(2, '0')}`
      if (Number.isNaN(Date.parse(`${date}T${time}Z`))) {
        throw parserError('Invalid arguments supplied to @datetime')
      }
      return { type: expressionTypes.CAST, value: `${date} ${time} ${tz}`, as, cast: 'timestamptz' }
    },
  },
  operator: {
    template: ['operator', 'operands', 'cast', 'as'],
    parser: ({ operator, operands = [], as, cast }) => {
      if (!isArray(operands)) {
        throw parserError('Invalid operands supplied to @operator')
      }
      return { type: expressionTypes.OPERATOR, values: [operator, ...operands], as, cast }
    },
  },
  and: {
    template: ['operands', 'cast', 'as'],
    parser: ({ operands = [], as, cast }) => {
      if (!isArray(operands)) {
        throw parserError('Invalid operands supplied to @and')
      }
      return { type: expressionTypes.OPERATOR, values: ['and', ...operands], as, cast }
    },
  },
  or: {
    template: ['operands', 'cast', 'as'],
    parser: ({ operands = [], as, cast }) => {
      if (!isArray(operands)) {
        throw parserError('Invalid operands supplied to @or')
      }
      return { type: expressionTypes.OPERATOR, values: ['or', ...operands], as, cast }
    },
  },
  sql: {
    template: ['sql', 'cast', 'as'],
    parser: ({ sql, cast, as }) => ({ type: expressionTypes.SQL, value: sql, as, cast }),
  },
}

const namedArgRE = /^\w+=.+$/
const numberRE = /^(-|\+)?\d+(\.\d+)?$/

const isShortExpression = (val, name) => {
  if (typeof val !== 'string' || !val.startsWith('@') || !val.endsWith(')')) {
    return false
  }
  const split = val.indexOf('(')
  if (split === -1) {
    return
  }
  const safeName = val.slice(1, split).toLowerCase()
  return name ? safeName === name : safeName in shortExpressions
}

const resolveArguments = (name, args, kwargs, template) => {
  const resolved = {}
  if (args.length) {
    if (args.length > template.length) {
      throw parserError(`Too many arguments supplied to @${name}`)
    }
    args.forEach((a, i) => { resolved[template[i]] = a })
  }
  Object.entries(kwargs).forEach(([key, value]) => {
    if (!template.includes(key)) {
      throw parserError(`@${name} received unexpected argument: ${key}`)
    }
    if (key in resolved) {
      throw parserError(`@${name} received multiple values for argument: ${key}`)
    }
    resolved[key] = value
  })
  return resolved
}

const sortArguments = (name, args) => args.reduce((acc, arg, i) => {
  if (isNonArrayObject(arg)) {
    Object.assign(acc.kwargs, arg)
    return acc
  }
  if (i !== acc.args.length) {
    throw parserError(`Positional arguments must be placed ahead of named arguments in @${name}`)
  }
  acc.args.push(arg)
  return acc
}, { args: [], kwargs: {} })

// string value
const parseShortArgument = (arg) => {
  try {
    const normalized = arg.toLowerCase()
    // boolean
    if (normalized === 'true' || normalized === 'false') {
      return Boolean(normalized)
    }
    // NULL
    if (normalized === 'null') {
      return null
    }
    // string
    if ((arg[0] === "'" || arg[0] === '"') && arg[arg.length - 1] === arg[0]) {
      return arg.slice(1, -1)
    }
    // array block
    if (arg[0] === '[' && arg[arg.length - 1] === ']') {
      return splitCSV(arg.slice(1, -1)).map(parseShortArgument)
    }
    // object block
    if (arg[0] === '{' && arg[arg.length - 1] === '}') {
      return JSON.parse(arg)
    }
    // short form
    if (isShortExpression(arg)) {
      // eslint-disable-next-line no-use-before-define
      return parseShortExpression(arg)
    }
    // named arg
    if (namedArgRE.test(arg)) {
      const split = arg.indexOf('=')
      const name = arg.slice(0, split).toLowerCase()
      const value = parseShortArgument(arg.slice(split + 1))
      return { [name]: value }
    }
    // number
    if (numberRE.test(arg)) {
      return Number(arg)
    }
    throw parserError(`Invalid short argument: ${arg}`)
  } catch (err) {
    if (err instanceof ParserError) {
      throw err
    }
    throw parserError(`Invalid short argument: ${arg}`)
  }
}

const sanitizeShortArgument = (arg) => {
  try {
    const normalized = arg.toLowerCase()
    // boolean and NULL
    if (normalized === 'true' || normalized === 'false' || normalized === 'null') {
      return normalized
    }
    // string
    if ((arg[0] === "'" || arg[0] === '"') && arg[arg.length - 1] === arg[0]) {
      return `'${arg.slice(1, -1)}'`
    }
    // array block
    if (arg[0] === '[' && arg[arg.length - 1] === ']') {
      return `[${splitCSV(arg.slice(1, -1)).map(sanitizeShortArgument).join(',')}]`
    }
    // object block
    if (arg[0] === '{' && arg[arg.length - 1] === '}') {
      return JSON.stringify(JSON.parse(arg))
    }
    // short form
    if (isShortExpression(arg)) {
      // eslint-disable-next-line no-use-before-define
      return sanitizeShortExpression(arg)
    }
    // named arg
    if (namedArgRE.test(arg)) {
      const split = arg.indexOf('=')
      const name = arg.slice(0, split).toLowerCase()
      const value = sanitizeShortArgument(arg.slice(split + 1))
      return `${name}=${value}`
    }
    // number
    if (numberRE.test(arg)) {
      return String(Number(arg))
    }
    throw parserError(`Invalid short argument: ${arg}`)
  } catch (err) {
    if (err instanceof ParserError) {
      throw err
    }
    throw parserError(`Invalid short argument: ${arg}`)
  }
}

// checks that syntax is compliant with short expression
// does not invoke parser (i.e. does not confirm arg types are correct)
const isValidShortExpression = (exp) => {
  try {
    const split = exp.indexOf('(')
    const name = exp.slice(1, split).toLowerCase()
    if (!(name in shortExpressions)) {
      return false
    }
    const parsedArgs = splitCSV(exp.slice(split + 1, -1)).map(parseShortArgument)
    const { args, kwargs } = sortArguments(name, parsedArgs)
    const { template } = shortExpressions[name]
    resolveArguments(name, args, kwargs, template)
    return true
  } catch (_) {
    return false
  }
}

// normalizes syntax - does not check arg validity re: parser
const sanitizeShortExpression = (exp) => {
  const split = exp.indexOf('(')
  const name = exp.slice(1, split).toLowerCase()
  if (!(name in shortExpressions)) {
    throw parserError(`Invalid short expression: ${name}`)
  }
  const parsedArgs = splitCSV(exp.slice(split + 1, -1)).map(sanitizeShortArgument)
  return `@${name}(${parsedArgs.join(',')})`
}

const parseShortExpression = (exp) => {
  const split = exp.indexOf('(')
  const name = exp.slice(1, split).toLowerCase()
  if (!(name in shortExpressions)) {
    throw parserError(`Invalid short expression: ${name}`)
  }
  const parsedArgs = splitCSV(exp.slice(split + 1, -1)).map(parseShortArgument)
  const { args, kwargs } = sortArguments(name, parsedArgs)
  const { parser, template } = shortExpressions[name]
  const resolvedArgs = resolveArguments(name, args, kwargs, template)
  return parser(resolvedArgs)
}

module.exports = {
  isShortExpression,
  isValidShortExpression,
  parseShortExpression,
  parseShortArgument,
  sanitizeShortExpression,
  sanitizeShortArgument,
}
