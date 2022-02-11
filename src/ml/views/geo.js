const { typeToCatMap } = require('../type')
const { knex } = require('../../util/db')
const { filterViewColumns } = require('./utils')
const { useAPIErrorOptions } = require('../../util/api-error')
const { knexWithCache } = require('../../util/cache')
const geoTables = require('../geo-tables')
const { viewTypes, viewCategories } = require('./taxonomies')


const { apiError } = useAPIErrorOptions({ tags: { service: 'ql' } })

const GEO_TABLES = {
  ...Object.entries(geoTables).reduce((acc, [key, val]) => {
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

const parseViewID = (viewID) => {
  const [, tableKey] = viewID.match(/^geo_([\w]+)$/) || []
  // eslint-disable-next-line radix
  if (!(tableKey in GEO_TABLES)) {
    throw apiError(`Invalid view: ${viewID}`, 400)
  }
  return { tableKey }
}

const getTableColumns = async (schema, table) => {
  const tableColumns = await knexWithCache(
    knex('information_schema.columns')
      .columns(['column_name', 'data_type', 'udt_name'])
      .where({ table_schema: schema, table_name: table }),
    { ttl: 3600 }, // 1 hour
  )
  return tableColumns.reduce((acc, { column_name, data_type, udt_name }) => {
    const type = data_type === 'USER-DEFINED' ? udt_name : data_type
    acc[column_name] = {
      category: typeToCatMap.get(type),
      key: column_name,
      type,
    }
    return acc
  }, {})
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

  return Promise.all(geoTableList.map(async ([tableKey, { schema, table, idType, geoType }]) => {
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
      view.columns = await getTableColumns(schema, table)
    }
    return view
  }))
}

const getQueryView = async ({ whitelabel, customers }, viewID, queryColumns, engine) => {
  const { tableKey } = parseViewID(viewID)
  const { schema, table, whitelabelColumn, customerColumn, idColumn } = GEO_TABLES[tableKey]

  if (!schema || !table) {
    throw apiError('Invalid geo view', 400)
  }

  // inject view columns
  const viewMeta = await listViews({ filter: { tableKey } })
  const columns = filterViewColumns((viewMeta[0] || {}).columns, queryColumns)
  if (!Object.keys(columns).length) {
    throw apiError(`No column selected from view: ${viewID}`, 400)
  }

  const where = []
  // scoped to WL/CU for now
  // in the future, might expose POI's where WL IS NULL
  if (whitelabelColumn && whitelabel !== -1) {
    where.push(`"${table}"."${whitelabelColumn}" = ANY (ARRAY[${whitelabel.join(', ')}])`)
    if (customerColumn && customers !== -1) {
      where.push(`(
        "${table}"."${customerColumn}" IS NULL
        OR "${table}"."${customerColumn}" = (ARRAY[${customers.join(', ')}])
      )`)
    }
  }

  const catalog = engine === 'trino' ? 'locus_place.' : ''

  const query = `
    SELECT
      ${idColumn ? `"${idColumn}" AS "geo_${tableKey}"` : '*'}
    FROM ${catalog}"${schema}"."${table}"
    ${where.length ? `WHERE ${where.join(' AND ')}` : ''}
  `

  return { viewID, query, columns }
}

const getView = async (_, viewID) => {
  const { tableKey } = parseViewID(viewID)

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
  view.columns = await getTableColumns(schema, table)
  return view
}

module.exports = {
  getQueryView,
  listViews,
  getView,
}
