const { expressionTypes, geometryTypes } = require('../types')
const { parserError, isArray, isString } = require('../utils')


const shortExpressions = {}

shortExpressions.param = {
  template: ['name', 'as', 'cast'],
  parser: ({ name, as, cast }) => ({ type: expressionTypes.PARAMETER, value: name, as, cast }),
}

shortExpressions.view = {
  template: ['name', 'as'],
  parser: ({ name, as }) => ({ type: expressionTypes.VIEW, view: name, as }),
}

shortExpressions.column = {
  template: ['column', 'view', 'as', 'cast'],
  parser: ({ column, view, as, cast }) =>
    ({ type: expressionTypes.COLUMN, column, view, as, cast }),
}

shortExpressions.function = {
  template: ['name', 'args', 'as', 'cast'],
  parser: ({ name, args = [], as, cast }) => {
    if (!isArray(args)) {
      throw parserError('Invalid arguments supplied to @function')
    }
    return { type: expressionTypes.FUNCTION, values: [name, ...args], as, cast }
  },
}

shortExpressions.geo = {
  template: ['name', 'args', 'as', 'cast'],
  parser: ({ name, args = [], as, cast }) => {
    if (!isArray(args)) {
      throw parserError('Invalid arguments supplied to @geo')
    }
    return { type: expressionTypes.FUNCTION, values: ['geometry', name, ...args], as, cast }
  },
}

shortExpressions.ggid = {
  template: ['id', 'as', 'cast'],
  parser: ({ id, as, cast }) =>
    ({ type: expressionTypes.FUNCTION, values: ['geometry', geometryTypes.GGID, id], as, cast }),
}

shortExpressions.fsa = {
  template: ['fsa', 'as', 'cast'],
  parser: ({ fsa, as, cast }) =>
    ({ type: expressionTypes.FUNCTION, values: ['geometry', geometryTypes.CA_FSA, fsa], as, cast }),
}

shortExpressions.postalcode = {
  template: ['pc', 'as', 'cast'],
  parser: ({ pc, as, cast }) => ({
    type: expressionTypes.FUNCTION,
    values: ['geometry', geometryTypes.CA_POSTALCODE, pc],
    as,
    cast,
  }),
}

shortExpressions.da = {
  template: ['da', 'as', 'cast'],
  parser: ({ da, as, cast }) =>
    ({ type: expressionTypes.FUNCTION, values: ['geometry', geometryTypes.CA_DA, da], as, cast }),
}

shortExpressions.ct = {
  template: ['ct', 'as', 'cast'],
  parser: ({ ct, as, cast }) =>
    ({ type: expressionTypes.FUNCTION, values: ['geometry', geometryTypes.CA_CT, ct], as, cast }),
}

shortExpressions.poi = {
  template: ['poi', 'radius', 'as', 'cast'],
  parser: ({ poi, radius, as, cast }) => {
    const values = ['geometry', geometryTypes.POI, poi]
    if (radius !== undefined) {
      values.push(radius)
    }
    return { type: expressionTypes.FUNCTION, values, as, cast }
  },
}

shortExpressions.point = {
  template: ['long', 'lat', 'radius', 'as', 'cast'],
  parser: ({ long, lat, radius, as, cast }) => {
    const values = ['geometry', geometryTypes.POINT, long, lat]
    if (radius !== undefined) {
      values.push(radius)
    }
    return { type: expressionTypes.FUNCTION, values, as, cast }
  },
}

shortExpressions.array = {
  template: ['values', 'as', 'cast'],
  parser: ({ values = [], as, cast }) => {
    if (!isArray(values)) {
      throw parserError('Invalid arguments supplied to @array')
    }
    return { type: expressionTypes.ARRAY, values, as, cast }
  },
}

shortExpressions.list = {
  template: ['values', 'as', 'cast'],
  parser: ({ values = [], as, cast }) => {
    if (!isArray(values)) {
      throw parserError('Invalid arguments supplied to @list')
    }
    return { type: expressionTypes.LIST, values, as, cast }
  },
}

shortExpressions.cast = {
  template: ['value', 'cast', 'as'],
  parser: ({ value, cast, as }) => ({ type: expressionTypes.CAST, value, as, cast }),
}

shortExpressions.primitive = {
  template: ['value', 'cast', 'as'],
  parser: ({ value, cast, as }) => ({ type: expressionTypes.PRIMITIVE, value, as, cast }),
}

shortExpressions.date = {
  template: ['year', 'month', 'day', 'as'],
  parser: ({ year, month, day, as }) => {
    if (![year, month, day].every(Number.isInteger)) {
      throw parserError('Invalid arguments supplied to @date')
    }
    const value = `${year}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}`
    if (Number.isNaN(Date.parse(value))) {
      throw parserError('Invalid arguments supplied to @date')
    }
    return { type: expressionTypes.FUNCTION, values: ['date', value], as }
  },
}

shortExpressions.datetime = {
  template: ['year', 'month', 'day', 'hour', 'minute', 'second', 'tz', 'as'],
  parser: ({
    year, month, day,
    hour = 0, minute = 0, second = 0,
    tz = 'UTC', as,
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
    // timestamp in trino
    return { type: expressionTypes.FUNCTION, values: ['datetime', `${date} ${time} ${tz}`], as }
  },
}

shortExpressions.timedelta = {
  template: ['millisecond', 'second', 'minute', 'hour', 'day', 'week', 'month', 'year', 'as'],
  parser: ({ millisecond, second, minute, hour, day, week, month, year, as }) => {
    const intervals = [millisecond, second, minute, hour, day, week, month, year]
      .reduce((acc, v, i) => {
        if (v === undefined || v === 0) {
          return acc
        }
        if (!Number.isInteger(v) || v < 0) {
          throw parserError('Invalid arguments supplied to @timedelta')
        }
        acc.push({
          type: expressionTypes.FUNCTION,
          values: ['timedelta', shortExpressions.timedelta.template[i], v],
        })
        return acc
      }, [])
    if (!intervals.length) {
      return { type: expressionTypes.FUNCTION, values: ['timedelta', 'millisecond', 0], as }
    }
    if (intervals.length === 1) {
      return { ...intervals[0], as }
    }
    return {
      type: expressionTypes.OPERATOR,
      values: ['+', ...intervals],
      as,
    }
  },
}

shortExpressions.operator = {
  template: ['operator', 'operands', 'cast', 'as'],
  parser: ({ operator, operands = [], as, cast }) => {
    if (!isArray(operands)) {
      throw parserError('Invalid operands supplied to @operator')
    }
    return { type: expressionTypes.OPERATOR, values: [operator, ...operands], as, cast }
  },
}

shortExpressions.and = {
  template: ['operands', 'qualifier', 'cast', 'as'],
  parser: ({ operands = [], qualifier, as, cast }) => {
    if (!isArray(operands)) {
      throw parserError('Invalid operands supplied to @and')
    }
    return {
      type: expressionTypes.OPERATOR,
      values: [qualifier !== undefined ? [qualifier, 'and'] : 'and', ...operands],
      as,
      cast,
    }
  },
}

shortExpressions.or = {
  template: ['operands', 'qualifier', 'cast', 'as'],
  parser: ({ operands = [], qualifier, as, cast }) => {
    if (!isArray(operands)) {
      throw parserError('Invalid operands supplied to @or')
    }
    return {
      type: expressionTypes.OPERATOR,
      values: [qualifier !== undefined ? [qualifier, 'or'] : 'or', ...operands],
      as,
      cast,
    }
  },
}

shortExpressions.sql = {
  template: ['sql', 'cast', 'as'],
  parser: ({ sql, cast, as }) => ({ type: expressionTypes.SQL, value: sql, as, cast }),
}

module.exports = shortExpressions
