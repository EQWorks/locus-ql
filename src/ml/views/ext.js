const { pool } = require('../../util/db')
const { typeToCatMap, CAT_JSON } = require('../type')
const { filterViewColumns } = require('./utils')
const { useAPIErrorOptions } = require('../../util/api-error')
const { pgWithCache } = require('../../util/cache')
const { viewTypes, viewCategories } = require('./taxonomies')


const { apiError } = useAPIErrorOptions({ tags: { service: 'ql' } })

const CONNECTION_TABLE = 'ext_conn.connections'
const SETS_TABLE = 'ext_conn.sets'

const viewCategoryToConnType = {
  [viewCategories.EXT_AZURE_BLOB]: 'azure blob',
  [viewCategories.EXT_DIRECT]: 'direct',
  [viewCategories.EXT_GOOGLE_ANALYTICS]: 'google analytics 4',
  [viewCategories.EXT_GOOGLE_GCP_CS]: 'gcp cs',
  [viewCategories.EXT_GOOGLE_SHEET]: 'google sheet',
  // [viewCategories.EXT_HUBSPOT]: 'hubspot',
  [viewCategories.EXT_S3]: 's3',
  [viewCategories.EXT_SHOPIFY]: 'shopify',
  [viewCategories.EXT_STRIPE]: 'stripe',
}
const connTypeToViewCategory = Object.entries(viewCategoryToConnType).reduce((acc, [k, v]) => {
  acc[v] = k
  return acc
}, {})

const parseViewID = (viewID) => {
  const [, idStr] = viewID.match(/^ext_(\d+)$/) || []
  // eslint-disable-next-line radix
  const conn_id = parseInt(idStr, 10)
  if (Number.isNaN(conn_id)) {
    throw apiError(`Invalid view: ${viewID}`, 400)
  }
  return { conn_id }
}

const getConnections = ({ whitelabel, customers, conn_id, categories } = {}) => {
  const filters = []
  const values = []

  // filters
  if (conn_id) {
    values.push(conn_id)
    filters.push(`c.id = $${values.length}`)
  }

  if (whitelabel !== -1) {
    values.push(whitelabel)
    filters.push(`c.whitelabel = ANY ($${values.length})`)
    if (customers !== -1) {
      values.push(customers)
      filters.push(`c.customer = ANY ($${values.length})`)
    }
  }

  if (categories) {
    const connTypeFilters = []
    if (categories.includes(viewCategories.EXT_OTHER)) {
      values.push(Object.values(viewCategoryToConnType))
      connTypeFilters.push(`NOT (c.type = ANY($${values.length}))`)
    }
    const connTypes = categories.reduce((types, cat) => {
      if (cat in viewCategoryToConnType) {
        types.push(viewCategoryToConnType[cat])
      }
      return types
    }, [])
    if (connTypes.length) {
      values.push(connTypes)
      connTypeFilters.push(`c.type = ANY($${values.length})`)
    }
    if (connTypeFilters.length) {
      filters.push(`(${connTypeFilters.join(' OR ')})`)
    }
  }

  return pgWithCache(
    `
      SELECT
        c.id,
        c.set_id,
        c.type,
        c.name,
        c.whitelabel,
        c.customer,
        s.name AS set_name,
        s.columns,
        greatest(pc.reltuples, pt.n_live_tup) AS records,
        c.created,
        c.updated,
        c.last_sync,
        CASE c.is_syncing
          WHEN '1' THEN True
          ELSE False
        END AS is_syncing,
        dest,
        c.info
      FROM ${CONNECTION_TABLE} AS c
      INNER JOIN ${SETS_TABLE} AS s ON s.id = c.set_id
      INNER JOIN pg_stat_user_tables AS pt
        ON pt.relname = c.dest->>'table'
        AND pt.schemaname = c.dest->>'schema'
      INNER JOIN pg_class AS pc
        ON pc.relname = c.dest->>'table'
      -- this is to ensure filter on schema name as well
      INNER JOIN pg_catalog.pg_namespace AS n
        ON n.oid = pc.relnamespace
        AND n.nspname = c.dest->>'schema'
        AND pc.relkind = 'r'
      WHERE
        c.last_sync IS NOT NULL
        AND NOT c.deprecated
        ${filters.length ? `AND ${filters.join(' AND ')}` : ''}
    `,
    values,
    pool,
    { maxAge: 600 }, // 10 minutes
  )
}

const getViewObject = ({
  id,
  set_id,
  type,
  name,
  columns,
  whitelabel,
  customer,
  set_name,
  created,
  updated,
  last_sync,
  is_syncing,
}, inclMeta = true) => {
  const view = {
    name,
    set_id,
    type,
    view: {
      id: `${viewTypes.EXT}_${id}`,
      type: viewTypes.EXT,
      category: connTypeToViewCategory[type] || viewCategories.EXT_OTHER,
      conn_id: id,
    },
  }
  if (inclMeta) {
    Object.entries(columns).forEach(([key, column]) => {
      column.key = key
      column.category = typeToCatMap.get(column.type)
    })
    Object.assign(view, {
      columns,
      whitelabel,
      customer,
      set_name,
      created,
      updated,
      last_sync,
      is_syncing,
    })
  }
  return view
}

const listViews = async ({ access, filter: { categories } = {}, inclMeta = true }) => {
  const { whitelabel, customers } = access
  const connections = await getConnections({ whitelabel, customers, categories })
  return connections.map(conn => getViewObject(conn, inclMeta))
}

const getView = async (access, viewID) => {
  const { whitelabel, customers } = access
  const { conn_id } = parseViewID(viewID)
  const [connection] = await getConnections({ whitelabel, customers, conn_id })
  if (!connection) {
    throw apiError(`View not found: ${viewID}`, 404)
  }
  return getViewObject(connection, true)
}

const getQueryView = async (access, viewID, queryColumns, defaultEngine) => {
  const { whitelabel, customers } = access
  const { conn_id } = parseViewID(viewID)

  // check access to ext connection table and get table name
  const [connection] = await getConnections({ whitelabel, customers, conn_id })
  if (!connection) {
    throw apiError(`View not found: ${viewID}`, 404)
  }

  const { table, schema } = connection.dest
  let { columns } = connection

  columns = filterViewColumns(columns, queryColumns)
  if (!Object.keys(columns).length) {
    throw apiError(`No column selected from view: ${viewID}`, 400)
  }
  // inject view columns
  Object.entries(columns).forEach(([key, column]) => {
    column.key = key
    column.category = typeToCatMap.get(column.type)
  })

  // maybe this should be based in pg type instead
  // const getColExp = (columns, column) => {
  //   const { category } = columns[column]
  //   // trino compatibility
  //   // if (category === CAT_DATE) {
  //   //   return `cast("${column}" AS timestamp) AS ${column}`
  //   // }
  //   return `"${column}"`
  // }

  const engine = connection.info === 'trino' ? 'trino' : defaultEngine
  const catalog = engine === 'trino' ? 'locus_place.' : ''
  const columnExp = Object.values(columns)
    .map(({ key, category }) =>
      `"${key}"${category === CAT_JSON && engine === 'pg' ? '::jsonb' : ''}`)
    .join(', ')

  // inject view
  const query = `
    SELECT
      ${columnExp}
    FROM ${catalog}${schema}."${table}"
  `
  const dependencies = [['ext', conn_id]]

  return { viewID, query, columns, dependencies, engine }
}

module.exports = {
  getQueryView,
  listViews,
  getView,
}
