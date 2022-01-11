const {
  parserError,
  expressionTypes,
  isNonArrayObject,
  isArray,
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
        throw parserError('Invalid arguments supplied to in @fn')
      }
      return { type: expressionTypes.FUNCTION, values: [name, ...args], as, cast }
    },
  },
  array: {
    template: ['values', 'as', 'cast'],
    parser: ({ values = [], as, cast }) => {
      if (!isArray(values)) {
        throw parserError('Invalid arguments supplied to in @array')
      }
      return { type: expressionTypes.ARRAY, values, as, cast }
    },
  },
  list: {
    template: ['values', 'as', 'cast'],
    parser: ({ values = [], as, cast }) => {
      if (!isArray(values)) {
        throw parserError('Invalid arguments supplied to in @list')
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
    template: ['value', 'as'],
    parser: ({ value, as }) => ({ type: expressionTypes.CAST, value, as, cast: 'date' }),
  },
  operator: {
    template: ['operator', 'operands', 'cast', 'as'],
    parser: ({ operator, operands = [], as, cast }) => {
      if (!isArray(operands)) {
        throw parserError('Invalid operands supplied to in @operator')
      }
      return { type: expressionTypes.OPERATOR, values: [operator, ...operands], as, cast }
    },
  },
  and: {
    template: ['operands', 'cast', 'as'],
    parser: ({ operands = [], as, cast }) => {
      if (!isArray(operands)) {
        throw parserError('Invalid operands supplied to in @and')
      }
      return { type: expressionTypes.OPERATOR, values: ['and', ...operands], as, cast }
    },
  },
  or: {
    template: ['operands', 'cast', 'as'],
    parser: ({ operands = [], as, cast }) => {
      if (!isArray(operands)) {
        throw parserError('Invalid operands supplied to in @or')
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
const parseShortValue = (val) => {
  const lowerStr = val.toLowerCase()
  // boolean
  if (lowerStr === 'true' || lowerStr === 'false') {
    return Boolean(lowerStr)
  }
  // NULL
  if (lowerStr === 'null') {
    return null
  }
  // string
  if ((val[0] === "'" || val[0] === '"') && val[val.length - 1] === val[0]) {
    return val.slice(1, -1)
  }
  // array block
  if (val[0] === '[' && val[val.length - 1] === ']') {
    return splitCSV(val.slice(1, -1)).map(parseShortValue)
  }
  // object block
  if (val[0] === '{' && val[val.length - 1] === '}') {
    return JSON.parse(val)
  }
  // short form
  if (isShortExpression(val)) {
    // eslint-disable-next-line no-use-before-define
    return parseShortExpression(val)
  }
  // named arg
  if (namedArgRE.test(val)) {
    const split = val.indexOf('=')
    const name = val.slice(0, split)
    const value = parseShortValue(val.slice(split + 1))
    return { [name]: value }
  }
  // number
  if (numberRE.test(val)) {
    return Number(val)
  }
  throw parserError(`Invalid short value: ${val}`)
}

const parseShortExpression = (exp) => {
  const split = exp.indexOf('(')
  const name = exp.slice(1, split).toLowerCase()
  if (!(name in shortExpressions)) {
    throw parserError(`Invalid short expression: ${name}`)
  }
  const parsedArgs = splitCSV(exp.slice(split + 1, -1)).map(parseShortValue)
  const { args, kwargs } = sortArguments(name, parsedArgs)
  const { parser, template } = shortExpressions[name]
  const resolvedArgs = resolveArguments(name, args, kwargs, template)
  return parser(resolvedArgs)
}

module.exports = {
  isShortExpression,
  parseShortExpression,
  parseShortValue,
}
