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

const geometries = {
  [geometryTypes.CA_FSA]: { argsLength: 1 },
  [geometryTypes.CA_DA]: { argsLength: 1 },
  [geometryTypes.CA_CT]: { argsLength: 1 },
  [geometryTypes.CA_CSD]: { argsLength: 1 },
  [geometryTypes.CA_POSTALCODE]: { argsLength: 1 },
  [geometryTypes.CA_PROVINCE]: { argsLength: 1 },
  [geometryTypes.CA_CITY]: { argsLength: 1 },
  [geometryTypes.POI]: { argsLength: 1 },
  [geometryTypes.POINT]: { argsLength: 2 },
  [geometryTypes.GGID]: { argsLength: 1 },
}

module.exports = {
  geometryTypes,
  geometryTypeValues,
  geometries,
}
