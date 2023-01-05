const { CAT_STRING, CAT_NUMERIC } = require('./type')
const { geometryTypes } = require('./parser/src')


module.exports = {
  [geometryTypes.CA_FSA]: {
    schema: 'canada_geo',
    table: 'fsa_simplified',
    idType: CAT_STRING,
    idColumn: 'fsa',
    geometryColumn: 'wkb_geometry',
    intersectionQueryType: 'fsa',
    intersectionSourceType: 'fsa',
  },
  [geometryTypes.CA_DA]: {
    schema: 'canada_geo',
    table: 'da',
    idType: CAT_STRING,
    idColumn: 'dauid',
    geometryColumn: 'wkb_geometry',
    intersectionSourceType: 'da',
  },
  [geometryTypes.CA_CT]: {
    schema: 'canada_geo',
    table: 'ct',
    idType: CAT_STRING,
    idColumn: 'ctuid',
    geometryColumn: 'wkb_geometry',
    intersectionSourceType: 'ct',
  },
  [geometryTypes.CA_CSD]: {
    schema: 'canada_geo',
    table: 'csd',
    idType: CAT_STRING,
    idColumn: 'csduid',
    geometryColumn: 'geom',
  },
  [geometryTypes.CA_POSTALCODE]: {
    schema: 'canada_geo',
    table: 'postalcode_simplified',
    idType: CAT_STRING,
    idColumn: 'postalcode',
    geometryColumn: 'wkb_geometry',
    intersectionQueryType: 'postalcode',
  },
  [geometryTypes.CA_PROVINCE]: {
    schema: 'canada_geo',
    table: 'province',
    idType: CAT_STRING,
    idColumn: 'province_code',
    geometryColumn: 'wkb_geometry',
  },
  [geometryTypes.CA_CITY]: {
    schema: 'canada_geo',
    table: 'city',
    idType: CAT_STRING,
    idColumn: 'geo_id',
    geometryColumn: 'wkb_geometry',
  },
  [geometryTypes.POI]: {
    schema: 'public',
    table: 'poi',
    idType: CAT_NUMERIC,
    idColumn: 'poi_id',
    geometryColumn: 'polygon',
    latColumn: 'lat',
    longColumn: 'lon',
    radiusColumn: 'default_radius',
    whitelabelColumn: 'whitelabelid',
    customerColumn: 'customerid',
    publicColumn: 'public',
  },
}
