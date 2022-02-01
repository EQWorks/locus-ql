const { castTypes } = require('./src/types')


module.exports = {
  pg: {
    [castTypes.NUMERIC]: 'double precision',
    [castTypes.FLOAT]: 'double precision',
    [castTypes.INTEGER]: 'integer',
    [castTypes.STRING]: 'text',
    [castTypes.TEXT]: 'text',
    [castTypes.BOOLEAN]: 'boolean',
    [castTypes.JSON]: 'jsonb',
  },
  trino: {
    [castTypes.NUMERIC]: 'double',
    [castTypes.FLOAT]: 'double',
    [castTypes.INTEGER]: 'integer',
    [castTypes.STRING]: 'varchar',
    [castTypes.TEXT]: 'varchar',
    [castTypes.BOOLEAN]: 'boolean',
    [castTypes.JSON]: 'json',
  },
}
