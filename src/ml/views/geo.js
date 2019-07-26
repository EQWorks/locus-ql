/* eslint-disable no-use-before-define */

const { typeToCatMap } = require('../type')
const { knex } = require('../../util/db')
const apiError = require('../../util/api-error')


const GEO_TABLES = {
  city: {
    schema: 'canada_geo',
    table: 'city_dev',
  },
}

// public available
module.exports.getView = async (_, reqViews, { tableKey }) => {
  const viewID = `geo_${tableKey}`
  const { schema, table } = GEO_TABLES[tableKey]

  if (!schema || !table) {
    throw apiError('Invalid geo view', 403)
  }

  reqViews[viewID] = knex.raw(`
    (
      SELECT *
      FROM ${schema}.${table}
    ) as ${viewID}
  `)
}

module.exports.listViews = async () => {
  const tablePromises = Object.entries(GEO_TABLES).map(async ([tableKey, { schema, table }]) => {
    const tableColumns = await knex('information_schema.columns')
      .columns(['column_name', 'data_type', 'udt_name'])
      .where({ table_schema: schema, table_name: table })

    const columns = {}
    tableColumns.forEach(({ column_name, data_type, udt_name }) => {
      const type = data_type === 'USER-DEFINED' ? udt_name : data_type
      columns[column_name] = {
        category: typeToCatMap.get(type),
        key: column_name,
        type,
      }
    })

    return {
      name: tableKey,
      view: {
        type: 'geo',
        id: `geo_${tableKey}`,
        tableKey,
      },
      columns,
    }
  })

  return Promise.all(tablePromises)
}
