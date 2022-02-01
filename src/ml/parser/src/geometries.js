const { geometryTypes } = require('./types')


const geometries = {
  [geometryTypes.CA_FSA]: { argsLength: 1 },
  [geometryTypes.CA_DA]: { argsLength: 1 },
  [geometryTypes.CA_CT]: { argsLength: 1 },
  [geometryTypes.CA_CSD]: { argsLength: 1 },
  [geometryTypes.CA_POSTALCODE]: { argsLength: 1 },
  [geometryTypes.CA_PROVINCE]: { argsLength: 1 },
  [geometryTypes.CA_CITY]: { argsLength: 1 },
  [geometryTypes.POI]: { argsLength: 1 },
  [geometryTypes.POINT]: { minArgsLength: 2, maxArgsLength: 3 },
  [geometryTypes.GGID]: { argsLength: 1 },
}

module.exports = { geometries }
