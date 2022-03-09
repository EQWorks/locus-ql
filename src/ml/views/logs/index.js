/* eslint-disable function-paren-newline */
/* eslint-disable no-continue */
const { createHash } = require('crypto')

const { knex } = require('../../../util/db')
const { filterViewColumns } = require('../utils')
const { useAPIErrorOptions } = require('../../../util/api-error')
const { knexWithCache } = require('../../../util/cache')
const impView = require('./imp')
const bcnView = require('./bcn')
const {
  CU_AGENCY,
  CU_ADVERTISER,
  ACCESS_INTERNAL,
  ACCESS_CUSTOMER,
} = require('./constants')
const { QL_SCHEMA } = require('../../constants')
const { getPgView } = require('./pg-views')
const { viewTypes } = require('../taxonomies')


const { apiError } = useAPIErrorOptions({ tags: { service: 'ql' } })

const logTypes = {
  imp: impView,
  bcn: bcnView,
}
const accessMap = {
  dev: ACCESS_INTERNAL,
  internal: ACCESS_INTERNAL,
  wl: ACCESS_CUSTOMER,
  customers: ACCESS_CUSTOMER,
}

// columns excluded from the viewHash and included in all log pulls from Athena
const excludedViewColumns = ['date', '_date', 'hour', '_hour']

const parseViewID = (viewID) => {
  const [, logType, agencyIDStr] = viewID.match(/^logs_([a-z]+)_(\d+)$/) || []
  // eslint-disable-next-line radix
  const agencyID = parseInt(agencyIDStr, 10)
  if (!logType || !agencyID) {
    throw apiError(`Invalid view: ${viewID}`, 400)
  }
  if (!(logType in logTypes)) {
    throw apiError(`Invalid log type: ${logType}`, 400)
  }
  return { logType, agencyID }
}

/**
 * Extracts all columns from a query or an expression for a specific view
 * @param {object} viewColumns An object with column names as keys
 * @param {Set} queryColumns An object with column names as keys
 * @param {number} [accessType=2] Access type. See access enum values.
 * @returns {[string[], string[], number]} The view's columns contained in the
 * query along with the minimum access required to access the said view.
 * [cacheColumns, queryColumns, minAccess]
 */
const getQueryColumns = (viewColumns, queryColumns, accessType = ACCESS_CUSTOMER) => {
  const filteredColumns = filterViewColumns(viewColumns, queryColumns)
  const cacheColumns = new Set() // aliases/dependents substituted with the columns they reference
  let minAccess = 0
  Object.entries(filteredColumns).forEach(([col, config]) => {
    // add columns to sets
    const { access, aliasFor } = config
    const { dependsOn } = viewColumns[aliasFor || col]
    // check access
    if (access && access > accessType) {
      throw apiError(`Invalid column: ${col}`, 400)
    }
    minAccess = Math.max(access || 0, minAccess)
    // add dependents
    if (dependsOn) {
      dependsOn.forEach(column => cacheColumns.add(column))
      return
    }
    cacheColumns.add(aliasFor || col)
  })
  return [[...cacheColumns], Object.keys(filteredColumns), minAccess]
}

/**
 * Returns the intersection of two arrays using strict equality
 * @param {any[]} a First array
 * @param {any[]} b Second array
 * @returns {any[]} A new array being the intersection of a and b
 */
const intersectArrays = (a, b) => a.filter(x => b.includes(x))

/**
 * Returns a collection of fast views including all the columns in cacheColumns
 * @param {object} viewColumns An object with column names as keys
 * @param {string[]} cacheColumns The columns that must be included in the fast views
 * @returns {object[]} The fast views satisfying the above constraints
 * query. [cacheColumns, queryColumns]
 */
const getFastViews = (viewColumns, cacheColumns) => {
  let fastViews
  for (const col of cacheColumns) {
    if (!viewColumns[col].inFastViews || !viewColumns[col].inFastViews.length) {
      return []
    }
    if (!fastViews) {
      fastViews = viewColumns[col].inFastViews
      continue
    }
    fastViews = intersectArrays(fastViews, viewColumns[col].inFastViews)
    if (!fastViews.length) {
      break
    }
  }
  return fastViews
}

/**
 * Returns all customers of a certain types (advertisers vs. agency) given some filters
 * @param {number[]|-1} [whitelabelIDs=-1] Whitelabel filter. -1 means all.
 * @param {number[]|-1} [agencyIDs=-1] Agency filter. -1 means all.
 * @param {number} [type=1] Customer type. See customer enum values.
 * @returns {Promise<{ customerID: number, customerName: string }[]>} Array of customers
 */
const getCustomers = (whitelabelIDs = -1, agencyIDs = -1, type = CU_AGENCY) => {
  const filters = ['isactive', 'whitelabelid <> 0']
  const filterValues = []
  filters.push(type === CU_AGENCY ? 'agencyid = 0' : 'agencyid <> 0')
  if (Array.isArray(agencyIDs)) {
    filterValues.push(agencyIDs)
    filters.push(`${type === CU_AGENCY ? 'customerid' : 'agencyid'} = ANY(?)`)
  }
  if (Array.isArray(whitelabelIDs)) {
    filterValues.push(whitelabelIDs)
    filters.push('whitelabelid = ANY(?)')
  }

  return knexWithCache(
    knex.raw(`
      SELECT
        customerid AS "customerID",
        companyname AS "customerName"
      FROM public.customers
      WHERE
        ${filters.join(' AND ')}
    `, filterValues)
    , { ttl: 600, gzip: false }) // 10 minutes
}

/**
 * Returns the customer's time zone
 * @param {number} customerID Customer ID
 * @returns {Promise<string>} Time zone
 */
const getCustomerTimeZone = async (customerID) => {
  const [{ timeZone } = {}] = await knexWithCache(
    knex.raw(`
      SELECT
        timezone AS "timeZone"
      FROM public.customers
      WHERE
        customerid = ?
      LIMIT 1
    `, [customerID])
    , { ttl: 600, gzip: false }) // 10 minutes
  return timeZone
}

/**
 * Returns a unique hash per unordered collection of columns
 * @param {string[]} cols The view's columns
 * @returns {string} The view's hash
 */
const getViewHash = (cols) => {
  const hash = createHash('sha256')
  cols.sort().forEach((col) => {
    if (excludedViewColumns.includes(col)) {
      return
    }
    hash.update(col)
  })
  return hash.digest('base64')
}

/**
 * Returns the public representation of a log type's columns
 * @param {string} logType Log type
 * @param {number} [accessType=2] Access type. See access enum values.
 * @returns {object} Columns
 */
// eslint-disable-next-line arrow-body-style
const getPublicViewColumns = (logType, accessType = ACCESS_CUSTOMER) => {
  return Object.entries(logTypes[logType].columns)
    // order will be honoured by the most common browsers but is not guaranteed by JS specs
    .sort(([a], [b]) => {
      if (a > b) {
        return 1
      }
      if (a < b) {
        return -1
      }
      return 0
    })
    .reduce(
      (cols, [key, { category, geo_type, access, aliasFor }]) => {
        if (!access || access === accessType) {
          cols[key] = {
            category: aliasFor ? logTypes[logType].columns[aliasFor].category : category,
            geo_type: aliasFor ? logTypes[logType].columns[aliasFor].geo_type : geo_type,
            key,
          }
        }
        return cols
      },
      {},
    )
}

/**
 * Assembles the Athena query necessary to retrieve the view's data
 * @param {number} agencyID Customer ID (agency)
 * @param {number} advertiserID Customer ID (advertiser)
 * @param {string} logType Log type
 * @param {string[]} viewColumns Columns which make up the view
 * @returns {Promise<string>} Athena query
 */
const prepareAthenaQuery = (agencyID, advertiserID, logType, viewColumns) => {
  const groupByColumns = [
    '"date"',
    'hour',
  ]
  const aggColumns = []
  const crossJoins = []

  viewColumns.forEach((col) => {
    if (excludedViewColumns.includes(col)) {
      return
    }
    const { expression, isAggregate, crossJoin } = logTypes[logType].columns[col]
    if (crossJoin) {
      crossJoins.push(crossJoin)
    }
    if (isAggregate) {
      aggColumns.push(`${expression || `SUM("${col}")`} AS "${col}"`)
      return
    }
    groupByColumns.push(expression ? `${expression} AS "${col}"` : `"${col}"`)
  })

  const customerID = logTypes[logType].owner === CU_ADVERTISER ? advertiserID : agencyID
  return `
    SELECT
      ${[...groupByColumns, ...aggColumns].join(', ')}
    FROM ${logTypes[logType].table}
    ${crossJoins.join(', ')}
    WHERE
      customer_id = ${customerID}
      AND "date" IS NOT NULL
      AND hour IS NOT NULL
      AND (
        "date" > date '[START_DATE]' AND "date" < date '[END_DATE]'
        OR [START_HOUR] < 23 AND "date" = date '[START_DATE]' AND hour > [START_HOUR]
        OR "date" = date '[END_DATE]' AND ([END_HOUR] = 23 OR hour <= [END_HOUR])
      )
    GROUP BY ${groupByColumns.map((_, i) => i + 1).join(', ')}
  `
}

/**
 * Returns the view cache's ID
 * @param {number} agencyID Customer ID (agency)
 * @param {string} logType Log type
 * @param {string} viewHash Hash representation of the view
 * @returns {Promise<number>} undefined if the view does not exist in cache
 */
const getViewCacheID = async (agencyID, logType, viewHash) => {
  const { rows: [{ viewID } = {}] } = await knex.raw(`
    SELECT view_id AS "viewID"
    FROM ${QL_SCHEMA}.log_views
    WHERE
      log_type = ?
      AND customer_id = ?
      AND view_hash = ?
  `, [logType, agencyID, viewHash])

  return viewID
}

/**
 * Inserts the view in the view table and creates a corresponding table to store
 * the cached data
 * @param {number} agencyID Customer ID (agency)
 * @param {string} logType Log type
 * @param {string} viewHash Hash representation of the view
 * @param {string[]} viewColumns Columns which make up the view
 * @param {string} athenaQuery Query to use to source thew view's data
 * @returns {Knex.Transaction<number>} View ID
 */
const createViewCache = (agencyID, logType, viewHash, viewColumns, athenaQuery) => knex
  .transaction(async (trx) => {
    // insert view
    const { rows: [{ viewID }] } = await trx.raw(`
      INSERT INTO ${QL_SCHEMA}.log_views
        (log_type, customer_id, view_hash, athena_query)
      VALUES
        (:logType, :agencyID, :viewHash, :athenaQuery)
      ON CONFLICT (log_type, customer_id, view_hash) DO NOTHING
      RETURNING view_id AS "viewID"
    `, { logType, agencyID, viewHash, athenaQuery })

    const groupByColumns = [
      'id serial PRIMARY KEY',
      '"date" date NOT NULL',
      'hour smallint NOT NULL',
    ]
    const aggColumns = []

    // create view table
    viewColumns.forEach((col) => {
      if (excludedViewColumns.includes(col)) {
        return
      }
      if (logTypes[logType].columns[col].isAggregate) {
        aggColumns.push(`${col} ${logTypes[logType].columns[col].pgType}`)
        return
      }
      groupByColumns.push(`${col} ${logTypes[logType].columns[col].pgType}`)
    })
    await trx.raw(`
      CREATE TABLE IF NOT EXISTS ${QL_SCHEMA}.log_view_${viewID} (
        ${[...groupByColumns, ...aggColumns].join(', ')}
      )
    `)

    return viewID
  })

const getQueryView = async (access, viewID, queryColumns, engine) => {
  const { logType, agencyID } = parseViewID(viewID)
  // check access
  const { whitelabel, customers, prefix } = access
  if (!(Array.isArray(customers) && customers.includes(agencyID)) && customers !== -1) {
    throw apiError('Invalid access permissions', 403)
  }
  const [
    { customerID: advertiserID } = {},
  ] = await getCustomers(whitelabel, [agencyID], CU_ADVERTISER)
  if (!advertiserID) {
    throw apiError('Invalid access permissions', 403)
  }

  const [cacheColumns, publicColumns, minAccess] = getQueryColumns(
    logTypes[logType].columns,
    queryColumns,
    accessMap[prefix],
  )
  if (!cacheColumns.length || cacheColumns.length > 15) {
    throw apiError('Log views are restricted to queries pulling between 1 and 15 columns', 400)
  }
  if (cacheColumns.some(col => !(col in logTypes[logType].columns))) {
    throw apiError(`Unknow column for log type: ${logType}`, 400)
  }

  // check if can use camphistory view instead of log
  const [fastView] = getFastViews(logTypes[logType].columns, cacheColumns)

  // otherwise update pg view cache with log data, as applicable
  let cacheID
  let dependencies
  if (!fastView) {
    const viewHash = getViewHash(cacheColumns)
    cacheID = await getViewCacheID(agencyID, logType, viewHash)
    if (!cacheID) {
      const athenaQuery = prepareAthenaQuery(agencyID, advertiserID, logType, cacheColumns)
      cacheID = await createViewCache(agencyID, logType, viewHash, cacheColumns, athenaQuery)
    }
    dependencies = [['log', cacheID]]
  }

  const groupByColumns = []
  const aggColumns = []
  const joins = {}
  const fdwConnections = new Set()

  publicColumns.forEach((col) => {
    const { aliasFor } = logTypes[logType].columns[col]
    const { viewExpression, viewJoins, isAggregate } = logTypes[logType].columns[aliasFor || col]
    if (viewJoins) {
      viewJoins.forEach((viewJoin) => {
        joins[viewJoin.view] = viewJoin
      })
    }
    if (isAggregate) {
      aggColumns.push(`${viewExpression || `SUM(log."${aliasFor || col}")`} AS "${col}"`)
      return
    }
    groupByColumns.push(`${viewExpression || `log."${aliasFor || col}"`} AS "${col}"`)
  })

  const pg = knex
    .select(knex.raw([...groupByColumns, ...aggColumns].join(', ')))

  if (fastView) {
    const { view, fdwConnection } = getPgView(fastView, agencyID, advertiserID, engine)
    if (fdwConnection) {
      fdwConnections.add(fdwConnection)
    }
    pg.from({ log: knex.select().from(view) })
  } else {
    const timeZone = await getCustomerTimeZone(agencyID)
    pg
      // convert dates from utc to customer's tz
      .from({
        log: knex.select('*', knex.raw(`
            timezone(
              ?,
              timezone('UTC', date + hour * INTERVAL '1 hour')
            )::timestamptz AS time_tz
          `, timeZone))
          .from(`${QL_SCHEMA}.log_view_${cacheID}`),
      })
  }

  if (groupByColumns.length) {
    pg.groupByRaw(groupByColumns.map((_, i) => i + 1).join(', '))
  }

  // join enrich views
  Object.values(joins).forEach(({ type, view, condition }) => {
    const { view: knexView, fdwConnection } = getPgView(view, agencyID, advertiserID, engine)
    if (fdwConnection) {
      fdwConnections.add(fdwConnection)
    }
    pg[`${type}Join`](knexView, condition)
  })

  return {
    viewID,
    query: pg.toString(),
    columns: filterViewColumns(getPublicViewColumns(logType, accessMap[prefix]), queryColumns),
    dependencies,
    isInternal: minAccess >= ACCESS_INTERNAL,
    fdwConnections: [...fdwConnections],
  }
}

const listViews = async ({ access, filter = {}, inclMeta = true }) => {
  const { whitelabel, customers, prefix } = access
  const agencies = await getCustomers(whitelabel, customers, CU_AGENCY)
  return agencies.reduce(
    (views, { customerID, customerName }) => views.concat(
      Object.entries(logTypes).reduce((views, [type, { name, category }]) => {
        if (filter.categories && !filter.categories.includes(category)) {
          return views
        }
        const view = {
          name: `${name} - ${customerName} (${customerID})`,
          view: {
            id: `${viewTypes.LOGS}_${type}_${customerID}`,
            type: viewTypes.LOGS,
            category,
            logType: type,
            agencyID: customerID,
          },
        }
        if (inclMeta) {
          view.columns = getPublicViewColumns(type, accessMap[prefix])
        }
        views.push(view)
        return views
      }, []),
    ),
    [],
  )
}

const getView = async (access, viewID) => {
  const { logType, agencyID } = parseViewID(viewID)
  // check access
  const { whitelabel, customers, prefix } = access
  if (!(Array.isArray(customers) && customers.includes(agencyID)) && customers !== -1) {
    throw apiError('Invalid access permissions', 403)
  }
  const [{ customerID, customerName } = {}] = await getCustomers(whitelabel, [agencyID], CU_AGENCY)
  if (!customerID) {
    throw apiError('Invalid access permissions', 403)
  }

  const { name, category } = logTypes[logType]

  return {
    name: `${name} - ${customerName} (${customerID})`,
    view: {
      id: `${viewTypes.LOGS}_${logType}_${customerID}`,
      type: viewTypes.LOGS,
      category,
      logType,
      agencyID,
    },
    columns: getPublicViewColumns(logType, accessMap[prefix]),
  }
}


module.exports = {
  getQueryView,
  listViews,
  getView,
}
