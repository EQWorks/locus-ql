/* eslint-disable no-use-before-define */
const { typeToCatMap } = require('../type')
const { knex } = require('../../util/db')
const { useAPIErrorOptions } = require('../../util/api-error')
const { knexWithCache } = require('../../util/cache')
const { geoMapping } = require('../geo')
const { viewTypes, viewCategories } = require('./taxonomies')


const { apiError } = useAPIErrorOptions({ tags: { service: 'ql' } })

const GEO_TABLES = {
  ...Object.entries(geoMapping).reduce((acc, [key, val]) => {
    // skip poi as a bridge table for now due to high cardinality (geo joins all customer POI's
    // instead of only the ones required to bridge the view)
    if (key === 'poi') {
      return acc
    }
    const tableKey = key.replace(/-/g, '_')
    acc[tableKey] = {
      ...val,
      geoType: key,
    }
    return acc
  }, {}),
  ggid: {
    schema: 'config',
    table: 'ggid_map',
  },
}

// public available
const getQueryView = async ({ whitelabel, customers }, { tableKey }) => {
  const viewID = `${viewTypes.GEO}_${tableKey}`
  const { schema, table, whitelabelColumn, customerColumn, idColumn } = GEO_TABLES[tableKey]

  if (!schema || !table) {
    throw apiError('Invalid geo view', 403)
  }

  // inject view columns
  const viewMeta = await listViews({ filter: { tableKey } })
  const mlViewColumns = (viewMeta[0] || {}).columns

  // inject view
  const mlView = knex
    .select(idColumn ? { [`geo_${tableKey}`]: idColumn } : '*')
    .from(`${schema}.${table}`)

  // scoped to WL/CU for now
  // in the future, might expose POI's where WL IS NULL
  // issue: slows down geo joins
  if (whitelabelColumn && whitelabel !== -1) {
    const customerFilter = customerColumn && customers !== -1
      ? `AND (
        ${table}.${customerColumn} IS NULL
        OR ${table}.${customerColumn} = ANY (:customers)
      )`
      : ''
    mlView.where(knex.raw(`(
      ${table}.${whitelabelColumn} = ANY (:whitelabel)
      ${customerFilter}
    )`, { whitelabel, customers }))
  }

  return { viewID, mlView, mlViewColumns }
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

  const tablePromises = geoTableList.map(async ([tableKey, { schema, table, idType, geoType }]) => {
    const view = {
      name: tableKey,
      view: {
        id: `${viewTypes.GEO}_${tableKey}`,
        type: viewTypes.GEO,
        category: viewCategories.GEO,
        tableKey,
      },
    }
    if (inclMeta) {
      if (idType) {
        view.columns = {
          [`geo_${tableKey}`]: {
            category: idType,
            geo_type: geoType,
            key: `geo_${tableKey}`,
          },
        }
        return view
      }

      const tableColumns = await knexWithCache(
        knex('information_schema.columns')
          .columns(['column_name', 'data_type', 'udt_name'])
          .where({ table_schema: schema, table_name: table }),
        { ttl: 3600 }, // 1 hour
      )
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

const getView = async (_, viewID) => {
  const [, tableKey] = viewID.match(/^geo_([\w]+)$/) || []
  // eslint-disable-next-line radix
  if (!(tableKey in GEO_TABLES)) {
    throw apiError(`Invalid view: ${viewID}`, 403)
  }

  const view = {
    name: tableKey,
    view: {
      id: `${viewTypes.GEO}_${tableKey}`,
      type: viewTypes.GEO,
      category: viewCategories.GEO,
      tableKey,
    },
  }

  const { schema, table, idType, geoType } = GEO_TABLES[tableKey]

  if (idType) {
    view.columns = {
      [`geo_${tableKey}`]: {
        category: idType,
        geo_type: geoType,
        key: `geo_${tableKey}`,
      },
    }
    return view
  }

  const tableColumns = await knexWithCache(
    knex('information_schema.columns')
      .columns(['column_name', 'data_type', 'udt_name'])
      .where({ table_schema: schema, table_name: table }),
    { ttl: 3600 }, // 1 hour
  )

  const columns = {}
  tableColumns.forEach(({ column_name, data_type, udt_name }) => {
    const type = data_type === 'USER-DEFINED' ? udt_name : data_type
    columns[column_name] = {
      category: typeToCatMap.get(type),
      key: column_name,
      type,
    }
  })

  view.columns = columns
  return view
}

module.exports = {
  getQueryView,
  listViews,
  getView,
}
