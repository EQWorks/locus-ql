// QL allowed cast

/**
 * @enum
 */
const expressionTypes = {
  SELECT: 'select',
  SELECT_CTE: 'select_cte',
  SELECT_RANGE: 'select_range',
  SELECT_RANGE_LATERAL: 'select_range_lateral',
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
  GEOMETRY: 'geometry',
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

/**
 * @enum
 */
const castTypes = {
  NUMERIC: 'numeric',
  FLOAT: 'float', // same as numeric
  INTEGER: 'integer',
  STRING: 'string',
  TEXT: 'text', // same as string
  BOOLEAN: 'boolean',
  JSON: 'json',
}
// reverse lookup
const castTypeValues = Object.entries(castTypes).reduce((acc, [k, v]) => {
  acc[v] = k
  return acc
}, {})

/**
 * @enum
 */
const joinTypes = {
  LEFT: 'left',
  RIGHT: 'right',
  INNER: 'inner',
  CROSS: 'cross',
  LATERAL: 'lateral',
}
// reverse lookup
const joinTypeValues = Object.entries(joinTypes).reduce((acc, [k, v]) => {
  acc[v] = k
  return acc
}, {})

/**
 * @enum
 */
const geometryTypes = {
  CA_FSA: 'ca-fsa',
  CA_DA: 'ca-da',
  CA_CT: 'ca-ct',
  CA_CSD: 'ca-csd',
  CA_POSTALCODE: 'ca-postalcode',
  CA_PROVINCE: 'ca-province',
  CA_CITY: 'ca-city',
  POI: 'poi',
  POINT: 'point',
  GGID: 'ggid',
}

// reverse lookup
const geometryTypeValues = Object.entries(geometryTypes).reduce((acc, [k, v]) => {
  acc[v] = k
  return acc
}, {})

module.exports = {
  expressionTypes,
  expressionTypeValues,
  castTypes,
  castTypeValues,
  joinTypes,
  joinTypeValues,
  geometryTypes,
  geometryTypeValues,
}
