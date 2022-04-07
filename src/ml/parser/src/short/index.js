const { parserError, ParserError, isNonArrayObject, splitCSV } = require('../utils')
const shortExpressions = require('./expressions')


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

const sortArguments = (name, args) => {
  const sorted = args.reduce((acc, arg, i) => {
    if (isNonArrayObject(arg) && arg._isNamedArg) {
      Object.assign(acc.kwargs, arg)
      return acc
    }
    if (i !== acc.args.length) {
      throw parserError(`Positional arguments must be placed ahead of named arguments in @${name}`)
    }
    acc.args.push(arg)
    return acc
  }, { args: [], kwargs: {} })
  delete sorted.kwargs._isNamedArg
  return sorted
}

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
      return { [name]: value, _isNamedArg: true }
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
  return `@${name}(${parsedArgs.join(', ')})`
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
