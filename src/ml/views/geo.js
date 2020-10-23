/* eslint-disable no-use-before-define */

const { typeToCatMap } = require('../type')
const { knex } = require('../../util/db')
const apiError = require('../../util/api-error')


const GEO_TABLES = {
  city: {
    schema: 'canada_geo',
    table: 'city_dev',
  },
  ggid: {
    schema: 'config',
    table: 'ggid_map',
  },
}

// public available
const getView = async (_, reqViews, reqViewColumns, { tableKey }) => {
  const viewID = `geo_${tableKey}`
  const { schema, table } = GEO_TABLES[tableKey]

  if (!schema || !table) {
    throw apiError('Invalid geo view', 403)
  }

  // inject view columns
  const viewMeta = await listViews({ filter: { tableKey } })
  reqViewColumns[viewID] = (viewMeta[0] || {}).columns

  // inject view
  reqViews[viewID] = knex.raw(`
    (
      SELECT *
      FROM ${schema}.${table}
    ) as ${viewID}
  `)
}

const listViews = async ({ filter, inclMeta = true }) => {
  let geoTableList = []
  if (filter && filter.tableKey) {
    geoTableList = [
      [filter.tableKey, GEO_TABLES[filter.tableKey]],
    ]
  } else {
    geoTableList = Object.entries(GEO_TABLES)
  }

  const tablePromises = geoTableList.map(async ([tableKey, { schema, table }]) => {
    const view = {
      name: tableKey,
      view: {
        type: 'geo',
        id: `geo_${tableKey}`,
        tableKey,
      },
    }
    if (inclMeta) {
      const tableColumns = await knex('information_schema.columns')
        .columns(['column_name', 'data_type', 'udt_name'])
        .where({ table_schema: schema, table_name: table })
      view.columns = {}
      tableColumns.forEach(({ column_name, data_type, udt_name }) => {
        const type = data_type === 'USER-DEFINED' ? udt_name : data_type
        view.columns[column_name] = {
          category: typeToCatMap.get(type),
          key: column_name,
          type,
        }
      })
    }
    return view
  })

  return Promise.all(tablePromises)
}

const listView = async (_, viewID) => {
  const [, tableKey] = viewID.match(/^geo_(\w+)$/) || []
  // eslint-disable-next-line radix
  if (!(tableKey in GEO_TABLES)) {
    throw apiError(`Invalid view: ${viewID}`, 403)
  }
  const { schema, table } = GEO_TABLES[tableKey]
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
}

module.exports = {
  getView,
  listViews,
  listView,
}
