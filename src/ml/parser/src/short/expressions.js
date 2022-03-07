const { expressionTypes, geometryTypes, castTypes } = require('../types')
const { parserError, isArray, isString, isNull, isNonNull } = require('../utils')


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
  template: ['type', 'args', 'as'],
  parser: ({ type, args = [], as }) => {
    if (!isArray(args)) {
      throw parserError('Invalid arguments supplied to @geo')
    }
    return { type: expressionTypes.FUNCTION, values: ['geometry', type, ...args], as }
  },
}

shortExpressions.ggid = {
  template: ['id', 'as'],
  parser: ({ id, as }) =>
    ({
      type: expressionTypes.FUNCTION,
      values: [
        'geometry',
        geometryTypes.GGID,
        { type: expressionTypes.CAST, value: id, cast: castTypes.STRING },
      ],
      as,
    }),
}

shortExpressions.fsa = {
  template: ['fsa', 'as'],
  parser: ({ fsa, as }) =>
    ({ type: expressionTypes.FUNCTION, values: ['geometry', geometryTypes.CA_FSA, fsa], as }),
}

shortExpressions.postalcode = {
  template: ['pc', 'as'],
  parser: ({ pc, as }) => ({
    type: expressionTypes.FUNCTION,
    values: ['geometry', geometryTypes.CA_POSTALCODE, pc],
    as,
  }),
}

shortExpressions.da = {
  template: ['da', 'as'],
  parser: ({ da, as }) =>
    ({ type: expressionTypes.FUNCTION, values: ['geometry', geometryTypes.CA_DA, da], as }),
}

shortExpressions.ct = {
  template: ['ct', 'as'],
  parser: ({ ct, as }) =>
    ({ type: expressionTypes.FUNCTION, values: ['geometry', geometryTypes.CA_CT, ct], as }),
}

shortExpressions.city = {
  template: ['city', 'province', 'as'],
  parser: ({ city, province, as }) => {
    const id = isNonNull(province)
      ? { type: expressionTypes.OPERATOR, values: ['||', 'CA$', province, '$', city] }
      : city
    const values = ['geometry', geometryTypes.CA_CITY, id]
    return { type: expressionTypes.FUNCTION, values, as }
  },
}

shortExpressions.poi = {
  template: ['id', 'radius', 'as'],
  parser: ({ id, radius, as }) => {
    const values = [
      'geometry',
      geometryTypes.POI,
      { type: expressionTypes.CAST, value: id, cast: castTypes.STRING },
    ]
    if (isNonNull(radius)) {
      values.push(radius)
    }
    return { type: expressionTypes.FUNCTION, values, as }
  },
}

shortExpressions.point = {
  template: ['long', 'lat', 'radius', 'as'],
  parser: ({ long, lat, radius, as }) => {
    const values = ['geometry', geometryTypes.POINT, long, lat]
    if (isNonNull(radius)) {
      values.push(radius)
    }
    return { type: expressionTypes.FUNCTION, values, as }
  },
}

shortExpressions.array = {
  template: ['values', 'as'],
  parser: ({ values = [], as }) => {
    if (!isArray(values)) {
      throw parserError('Invalid arguments supplied to @array')
    }
    return { type: expressionTypes.ARRAY, values, as }
  },
}

shortExpressions.list = {
  template: ['values', 'as'],
  parser: ({ values = [], as }) => {
    if (!isArray(values)) {
      throw parserError('Invalid arguments supplied to @list')
    }
    return { type: expressionTypes.LIST, values, as }
  },
}

shortExpressions.cast = {
  template: ['value', 'as'],
  parser: ({ value, as }) => ({ type: expressionTypes.CAST, value, as }),
}

shortExpressions.primitive = {
  template: ['value', 'as', 'cast'],
  parser: ({ value, as, cast }) => ({ type: expressionTypes.PRIMITIVE, value, as, cast }),
}

shortExpressions.null = {
  template: ['as', 'cast'],
  parser: ({ as, cast }) => ({ type: expressionTypes.PRIMITIVE, value: null, as, cast }),
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
    if (![year, month, day, hour, minute, second].every(Number.isInteger) || !isString(tz, true)) {
      throw parserError('Invalid arguments supplied to @datetime')
    }
    const date = `${year}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}`
    // eslint-disable-next-line max-len
    const time = `${String(hour).padStart(2, '0')}:${String(minute).padStart(2, '0')}:${String(second).padStart(2, '0')}`
    if (Number.isNaN(Date.parse(`${date}T${time}Z`))) {
      throw parserError('Invalid arguments supplied to @datetime')
    }
    return { type: expressionTypes.FUNCTION, values: ['datetime', `${date} ${time} ${tz}`], as }
  },
}

shortExpressions.time = {
  template: ['hour', 'minute', 'second', 'tz', 'as'],
  parser: ({ hour, minute, second, tz = 'UTC', as }) => {
    if (![hour, minute, second].every(Number.isInteger) || !isString(tz, true)) {
      throw parserError('Invalid arguments supplied to @time')
    }
    // eslint-disable-next-line max-len
    const time = `${String(hour).padStart(2, '0')}:${String(minute).padStart(2, '0')}:${String(second).padStart(2, '0')}`
    return { type: expressionTypes.FUNCTION, values: ['time', `${time} ${tz}`], as }
  },
}

shortExpressions.timedelta = {
  template: ['millisecond', 'second', 'minute', 'hour', 'day', 'week', 'month', 'year', 'as'],
  parser: ({ millisecond, second, minute, hour, day, week, month, year, as }) => {
    const intervals = [millisecond, second, minute, hour, day, week, month, year]
      .reduce((acc, v, i) => {
        if (isNull(v) || v === 0) {
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
  template: ['operator', 'operands', 'qualifier', 'as', 'cast'],
  parser: ({ operator, operands = [], qualifier, as, cast }) => {
    if (!isArray(operands)) {
      throw parserError('Invalid operands supplied to @operator')
    }
    return {
      type: expressionTypes.OPERATOR,
      values: [isNonNull(qualifier) ? [qualifier, operator] : operator, ...operands],
      as,
      cast,
    }
  },
}

shortExpressions.and = {
  template: ['operands', 'qualifier', 'as', 'cast'],
  parser: ({ operands = [], qualifier, as, cast }) => {
    if (!isArray(operands)) {
      throw parserError('Invalid operands supplied to @and')
    }
    return {
      type: expressionTypes.OPERATOR,
      values: [isNonNull(qualifier) ? [qualifier, 'and'] : 'and', ...operands],
      as,
      cast,
    }
  },
}

shortExpressions.or = {
  template: ['operands', 'qualifier', 'as', 'cast'],
  parser: ({ operands = [], qualifier, as, cast }) => {
    if (!isArray(operands)) {
      throw parserError('Invalid operands supplied to @or')
    }
    return {
      type: expressionTypes.OPERATOR,
      values: [isNonNull(qualifier) ? [qualifier, 'or'] : 'or', ...operands],
      as,
      cast,
    }
  },
}

shortExpressions.sql = {
  template: ['sql', 'as', 'cast'],
  parser: ({ sql, as, cast }) => ({ type: expressionTypes.SQL, value: sql, as, cast }),
}

module.exports = shortExpressions
