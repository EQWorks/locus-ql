/* eslint-disable function-paren-newline */
/* eslint-disable no-continue */
const { createHash } = require('crypto')

const { knex, fdwConnect } = require('../../../util/db')
const apiError = require('../../../util/api-error')
const { knexWithCache } = require('../../cache')
const impView = require('./imp')
const bcnView = require('./bcn')
const {
  // PG_CACHE_DAYS,
  // ONE_HOUR_MS,
  // ONE_DAY_MS,
  CU_AGENCY,
  CU_ADVERTISER,
  ACCESS_INTERNAL,
  ACCESS_CUSTOMER,
  ML_SCHEMA,
} = require('./constants')
const { getPgView } = require('./pg-views')


// const STATUS_SUCCEEDED = 'SUCCEEDED'
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

/**
 * Extracts all columns from a query or an expression for a specific view
 * @param {string} viewID ID of the view which columns are to be extracted
 * @param {object} viewColumns An object with column names as keys
 * @param {object} query Query or expression
 * @param {number} [accessType=2] Access type. See access enum values.
 * @returns {[string[], string[], number]} The view's columns contained in the
 * query along with the minimum access required to access the said view.
 * [cacheColumns, queryColumns, minAccess]
 */
const getQueryColumns = (viewID, viewColumns, query, accessType = ACCESS_CUSTOMER) => {
  const cacheColumns = new Set() // aliases/dependents substituted with the columns they reference
  const queryColumns = new Set() // aliases/dependents not substituted
  let minAccess = 0
  const queue = [query]
  while (queue.length) {
    const item = queue.shift()
    if (typeof item === 'string' && item.indexOf('.') !== -1) {
      queue.push(item.split('.', 2))
      continue
    }
    if (typeof item !== 'object' || item === null) {
      continue
    }
    if (item.type === 'column' && item.view === viewID && item.column in viewColumns) {
      const { access, aliasFor, dependsOn } = viewColumns[item.column]
      if (access && access > accessType) {
        continue
      }
      minAccess = Math.max(access || 0, minAccess)
      queryColumns.add(item.column)
      if (dependsOn) {
        dependsOn.forEach(column => cacheColumns.add(column))
        continue
      }
      cacheColumns.add(aliasFor || item.column)
      continue
    }
    if (
      Array.isArray(item) && item.length === 2 && typeof item[0] === 'string' && item[1] === viewID
    ) {
      if (item[0] === '*') {
        // eslint-disable-next-line no-loop-func
        Object.entries(viewColumns).forEach(([col, { access, aliasFor, dependsOn }]) => {
          if (access && access > accessType) {
            return
          }
          minAccess = Math.max(access || 0, minAccess)
          queryColumns.add(col)
          if (dependsOn) {
            dependsOn.forEach(column => cacheColumns.add(column))
            return
          }
          cacheColumns.add(aliasFor || col)
        })
        continue
      }
      if (item[0] in viewColumns) {
        const { access, aliasFor, dependsOn } = viewColumns[item[0]]
        if (access && access > accessType) {
          continue
        }
        minAccess = Math.max(access || 0, minAccess)
        queryColumns.add(item[0])
        if (dependsOn) {
          dependsOn.forEach(column => cacheColumns.add(column))
          continue
        }
        cacheColumns.add(aliasFor || item[0])
        continue
      }
    }
    queue.push(...Object.values(item))
  }
  return [[...cacheColumns], [...queryColumns], minAccess]
}

/**
 * Returns the intersection of two arrays using strict equality
 * @param {any[]} a First array
 * @param {any[]} b Second array
 * @returns {any[]} A new array being the intersection of a and b
 */
const intersectArrays = (a, b) => (
  a.length < b.length
    ? a.filter(x => b.includes(x))
    : b.filter(x => a.includes(x))
)

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

// /**
//  * Returns string following ISO 8601 'yyyy-mm-dd'
//  * @param {Date} date Date to represent as ISO
//  * @returns {string} ISO 8601 representation of the date
//  */
// const toISODate = date => `${date.getUTCFullYear()}-${
//   (date.getUTCMonth() + 1).toString().padStart(2, '0')
// }-${date.getUTCDate().toString().padStart(2, '0')
// }`

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
const getMlViewColumns = (logType, accessType = ACCESS_CUSTOMER) => {
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

// /**
//  * Assembles Athena query necessary to retrieve a view's data
//  * @param {number} agencyID Customer ID (agency)
//  * @param {number} advertiserID Customer ID (advertiser)
//  * @param {string} logType Log type
//  * @param {string[]} viewColumns Columns which make up the view
//  * @param {Date} start Start date (exclusive) from which to pull data
//  * @param {Date} end End date (inclusive) up until which data will be considered
//  * @returns {Promise<string>} Athena's 'Query Execution ID'
//  */
// const generateAthenaQuery = (agencyID, advertiserID, logType, viewColumns, start, end) => {
//   const groupByColumns = [
//     '"date"',
//     'hour',
//   ]
//   const aggColumns = []
//   const crossJoins = []

//   viewColumns.forEach((col) => {
//     if (excludedViewColumns.includes(col)) {
//       return
//     }
//     const { expression, isAggregate, crossJoin } = logTypes[logType].columns[col]
//     if (crossJoin) {
//       crossJoins.push(crossJoin)
//     }
//     if (isAggregate) {
//       aggColumns.push(`${expression || `SUM("${col}")`} AS "${col}"`)
//       return
//     }
//     groupByColumns.push(expression ? `${expression} AS "${col}"` : `"${col}"`)
//   })

//   const isoStart = toISODate(start)
//   const isoEnd = toISODate(end)
//   const startHour = start.getUTCHours()
//   const endHour = end.getUTCHours()
//   const dateFilter = []
//   if (isoStart === isoEnd) {
//     // eslint-disable-next-line max-len
//     dateFilter.push(`"date" = date '${isoEnd}' AND hour BETWEEN ${startHour + 1} AND ${endHour}`)
//   } else {
//     dateFilter.push(`"date" > date '${isoStart}' AND "date" < date '${isoEnd}'`)
//     // start is exclusive
//     if (startHour !== 23) {
//       dateFilter.push(`"date" = date '${isoStart}' AND hour > ${startHour}`)
//     }
//     // end is inclusive
//     if (endHour !== 23) {
//       dateFilter.push(`"date" = date '${isoEnd}' AND hour <= ${endHour}`)
//     } else {
//       dateFilter.push(`"date" = date '${isoEnd}'`)
//     }
//   }

//   const customerID = logTypes[logType].owner === CU_ADVERTISER ? advertiserID : agencyID
//   return `
//     SELECT
//       ${[...groupByColumns, ...aggColumns].join(', ')}
//     FROM ${logTypes[logType].table}
//     ${crossJoins.join(', ')}
//     WHERE
//       customer_id = ${customerID}
//       AND "date" IS NOT NULL
//       AND hour IS NOT NULL
//       AND (
//         ${dateFilter.join(' OR ')}
//       )
//     GROUP BY ${groupByColumns.map((_, i) => i + 1).join(', ')}
//   `
// }

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

// /**
//  * Returns the view cache's meta data: the view ID, the latest end date for which an
//  * update was queued along with the number of pending updates
//  * @param {number} agencyID Customer ID (agency)
//  * @param {string} logType Log type
//  * @param {string} viewHash Hash representation of the view
//  * @returns {Promise<{ viewID: number, cacheUntil: Date, pendingUpdates: number }>} undefined
//  * if the view does not exist in cache
//  */
// const getViewCacheMeta = async (agencyID, logType, viewHash) => {
//   const { rows: [viewMeta] } = await knex.raw(`
//     SELECT
//       v.view_id AS "viewID",
//       v.cached_until::timestamptz AS "cachedUntil",
//       COALESCE(COUNT(u.*), 0)::int AS "pendingUpdates"
//     FROM ${ML_SCHEMA}.log_views v
//     LEFT JOIN ${ML_SCHEMA}.log_updates u ON
//       u.view_id = v.view_id
//       AND u.status <> '${STATUS_SUCCEEDED}'
//     WHERE
//       v.log_type = ?
//       AND v.customer_id = ?
//       AND v.view_hash = ?
//     GROUP BY 1, 2
//   `, [logType, agencyID, viewHash])

//   return viewMeta
// }

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
    FROM ${ML_SCHEMA}.log_views
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
      INSERT INTO ${ML_SCHEMA}.log_views
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
      CREATE TABLE IF NOT EXISTS ${ML_SCHEMA}.log_view_${viewID} (
        ${[...groupByColumns, ...aggColumns].join(', ')}
      )
    `)

    return viewID
  })

// /**
//  * Updates the view cache's end date and inserts pending update
//  * @param {number} viewID Log view ID
//  * @param {Array<{ start: Date, end: Date, query: String}>} updates Array of
//  * update start date (exclusive), end date (inclusive) and Athena query
//  * @returns {Knex.Transaction}
//  */
// // 1. create view if not exists (ml_log_views) -> return view id
// // 2. create table if does not exists (use view id in name ml_log_view_1)
// // 3. queue updates (ml_log_updates)
// const queueViewCacheUpdates = (viewID, updates) => {
//   const [updateRows, updateValues] = updates.reduce(([rows, values], { start, end, query }, i) => {
//     rows.push(`(:viewID, :start_${i}, :end_${i}, :query_${i})`)
//     values[`start_${i}`] = start.toISOString()
//     values[`end_${i}`] = end.toISOString()
//     values[`query_${i}`] = query
//     return [rows, values]
//   }, [[], {}])

//   return knex.transaction(trx => Promise.all([
//     // insert cache update requests in log_updates
//     trx.raw(`
//       INSERT INTO ${ML_SCHEMA}.log_updates
//         (view_id, start_date, end_date, athena_query)
//       VALUES
//         ${updateRows.join(', ')}
//       ON CONFLICT (view_id, start_date, end_date) DO NOTHING
//     `, {
//       viewID,
//       ...updateValues,
//     }),
//     // update the cached_until date in log_views
//     trx.raw(`
//       UPDATE ${ML_SCHEMA}.log_views
//       SET cached_until = :end
//       WHERE
//         view_id = :viewID
//         AND (
//           cached_until < :end
//           OR cached_until IS NULL
//         )
//     `, {
//       viewID,
//       end: updates[updates.length - 1].end.toISOString(),
//     }),
//   ]))
// }

// /**
//  * Queues Athena queries to fill the view's PG cache with data for the last CACHE_DAYS days
//  * @param {number} agencyID Customer ID (agency)
//  * @param {number} advertiserID Customer ID (advertiser)
//  * @param {string} logType Log type
//  * @param {Object} viewMeta View metadata
//  * @param {string[]} viewColumns Columns which make up the view
//  * @returns {Promise<boolean>} Whether or not the view's PG cache was updated
//  */
// const updateViewCache = async (agencyID, advertiserID, logType, viewMeta, viewColumns) => {
//   try {
//     const { viewID, cachedUntil, pendingUpdates } = viewMeta
//     const thisHour = new Date()
//     thisHour.setUTCMinutes(0, 0, 0)
//     // start is exclusive
//     let start = cachedUntil
//       ? new Date(Math.max(
//         new Date(cachedUntil).valueOf(),
//         thisHour.valueOf() - (PG_CACHE_DAYS * ONE_DAY_MS),
//       ))
//       : new Date(thisHour.valueOf() - (PG_CACHE_DAYS * ONE_DAY_MS))
//     let end
//     const viewColumnsCount = viewColumns.reduce(
//       (count, col) => (excludedViewColumns.includes(col) ? count : count + 1),
//       0,
//     )
//     // split time range into multiple queries
//     const daysOfDataMs = Math.ceil(2 ** Math.max(5 - (0.75 * viewColumnsCount), 1)) * ONE_DAY_MS
//     const updates = []
//     while (start < thisHour) {
//       // end is inclusive
//       // except for the last day 'end' ends at 23h so that it queries full
//       // partitions (logs partitioned by day)
//       // the more fields the view includes the less dates the athena query spans
//       end = new Date(Math.min(
//         start.valueOf()
//         + daysOfDataMs
//         + ((23 - start.getUTCHours()) * ONE_HOUR_MS),
//         thisHour.valueOf(),
//       ))
//       const query = prepareAthenaQuery(agencyID, advertiserID, logType, viewColumns, start, end)
//       updates.push({ start, end, query })
//       start = end
//     }
//     if (updates.length) {
//       await queueViewCacheUpdates(viewID, updates)
//     }

//     return pendingUpdates > 0 || updates.length > 0
//   } catch (err) {
//     throw apiError('Error updating the view cache', 500)
//   }
// }

const getQueryView = async (access, { logType, query, agencyID }) => {
  if (!(logType in logTypes)) {
    throw apiError(`Invalid log type: ${logType}`, 403)
  }
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

  const viewID = `logs_${logType}_${agencyID}`
  const [cacheColumns, queryColumns, minAccess] = getQueryColumns(
    viewID,
    logTypes[logType].columns,
    query,
    accessMap[prefix],
  )
  if (!cacheColumns.length || cacheColumns.length > 10) {
    throw apiError('Log views are restricted to queries pulling between 1 and 10 columns', 403)
  }
  if (cacheColumns.some(col => !(col in logTypes[logType].columns))) {
    throw apiError(`Unknow column for log type: ${logType}`, 403)
  }

  // check if can use camphistory view instead of log
  const [fastView] = getFastViews(logTypes[logType].columns, cacheColumns)

  // otherwise update pg view cache with log data, as applicable
  let cacheID
  let mlViewDependencies
  if (!fastView) {
    const viewHash = getViewHash(cacheColumns)
    // let viewMeta = await getViewCacheMeta(agencyID, logType, viewHash)
    cacheID = await getViewCacheID(agencyID, logType, viewHash)
    if (!cacheID) {
      const athenaQuery = prepareAthenaQuery(agencyID, advertiserID, logType, cacheColumns)
      cacheID = await createViewCache(agencyID, logType, viewHash, cacheColumns, athenaQuery)
    // if (!viewMeta) {
    //   const athenaQuery = prepareAthenaQuery(agencyID, advertiserID, logType, cacheColumns)
    //   const viewID = await createViewCache(agencyID, logType, viewHash, cacheColumns, athenaQuery)
    //   viewMeta = { viewID }
    }
    mlViewDependencies = [['log', cacheID]]
    // cacheID = viewMeta.viewID
    // const cacheIsUpdating = await updateViewCache(
    //   agencyID, advertiserID, logType, viewMeta, cacheColumns,
    // )
    // if (cacheIsUpdating) {
    //   // async query
    //   return false
    // }
  }

  const groupByColumns = []
  const aggColumns = []
  const joins = {}
  const fdwConnections = new Set()

  queryColumns.forEach((col) => {
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

  const mlView = knex
    .select(knex.raw([...groupByColumns, ...aggColumns].join(', ')))
    .as(viewID)

  if (fastView) {
    const { view, fdwConnection } = getPgView(fastView, agencyID, advertiserID)
    if (fdwConnection) {
      fdwConnections.add(fdwConnection)
    }
    mlView.from({ log: knex.select().from(view) })
  } else {
    const timeZone = await getCustomerTimeZone(agencyID)
    mlView
      // convert dates from utc to customer's tz
      .from({
        log: knex.select('*', knex.raw(`
            timezone(
              ?,
              timezone('UTC', date + hour * INTERVAL '1 hour')
            )::timestamptz AS time_tz
          `, timeZone))
          .from(`${ML_SCHEMA}.log_view_${cacheID}`),
      })
  }

  if (groupByColumns.length) {
    mlView.groupByRaw(groupByColumns.map((_, i) => i + 1).join(', '))
  }

  // join enrich views
  Object.values(joins).forEach(({ type, view, condition }) => {
    const { view: knexView, fdwConnection } = getPgView(view, agencyID, advertiserID)
    if (fdwConnection) {
      fdwConnections.add(fdwConnection)
    }
    mlView[`${type}Join`](knexView, condition)
  })

  // init connections to foreign db's
  await Promise.all([...fdwConnections].map(connectionName => fdwConnect({ connectionName })))

  const mlViewColumns = getMlViewColumns(logType, accessMap[prefix])
  const mlViewIsInternal = minAccess >= ACCESS_INTERNAL

  return { viewID, mlView, mlViewColumns, mlViewDependencies, mlViewIsInternal }
}

const listViews = async ({ access, inclMeta = true }) => {
  const { whitelabel, customers, prefix } = access
  const agencies = await getCustomers(whitelabel, customers, CU_AGENCY)
  return agencies.reduce(
    (views, { customerID, customerName }) => views.concat(
      Object.entries(logTypes).map(([type, { name }]) => {
        const view = {
          name: `${name} - ${customerName} (${customerID})`,
          view: {
            type: 'logs',
            id: `logs_${type}_${customerID}`,
            logType: type,
            agencyID: customerID,
          },
        }
        if (inclMeta) {
          view.columns = getMlViewColumns(type, accessMap[prefix])
        }
        return view
      }),
    ),
    [],
  )
}

const getView = async (access, viewID) => {
  const [, logType, agencyIDStr] = viewID.match(/^logs_([a-z]+)_(\d+)$/) || []
  // eslint-disable-next-line radix
  const agencyID = parseInt(agencyIDStr, 10)
  if (!logType || !agencyID) {
    throw apiError(`Invalid view: ${viewID}`, 403)
  }
  if (!(logType in logTypes)) {
    throw apiError(`Invalid log type: ${logType}`, 403)
  }
  // check access
  const { whitelabel, customers, prefix } = access
  if (!(Array.isArray(customers) && customers.includes(agencyID)) && customers !== -1) {
    throw apiError('Invalid access permissions', 403)
  }
  const [{ customerID, customerName } = {}] = await getCustomers(whitelabel, [agencyID], CU_AGENCY)
  if (!customerID) {
    throw apiError('Invalid access permissions', 403)
  }

  return {
    name: `${logTypes[logType].name} - ${customerName} (${customerID})`,
    view: {
      type: 'logs',
      id: `logs_${logType}_${customerID}`,
      logType,
      agencyID,
    },
    columns: getMlViewColumns(logType, accessMap[prefix]),
  }
}


module.exports = {
  getQueryView,
  listViews,
  getView,
}
