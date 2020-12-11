/* eslint-disable function-paren-newline */
/* eslint-disable no-continue */
const { createHash } = require('crypto')

const { knex, atomPool, fdwConnect } = require('../../../util/db')
const apiError = require('../../../util/api-error')
const { athena } = require('../../../util/aws')
const { pgWithCache } = require('../../cache')
const impView = require('./imp')
const bcnView = require('./bcn')
const {
  PG_CACHE_DAYS,
  ATHENA_OUTPUT_BUCKET,
  ATHENA_WORKGROUP,
  ONE_HOUR_MS,
  ONE_DAY_MS,
  CU_AGENCY,
  ACCESS_INTERNAL,
  ACCESS_CUSTOMER,
} = require('./constants')
const { getPgView } = require('./pg-views')


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

/**
 * Extracts all columns from a query or an expression for a specific view
 * @param {string} viewID ID of the view which columns are to be extracted
 * @param {object} viewColumns An object with column names as keys
 * @param {object} query Query or expression
 * @param {number} [accessType=2] Access type. See access enum values.
 * @returns {[string[], string[]]} The view's columns contained in the
 * query. [cacheColumns, queryColumns]
 */
const getQueryColumns = (viewID, viewColumns, query, accessType = ACCESS_CUSTOMER) => {
  const cacheColumns = new Set() // aliases/dependents substituted with the columns they reference
  const queryColumns = new Set() // aliases/dependents not substituted
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
      if (access && access !== accessType) {
        continue
      }
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
        Object.entries(viewColumns).forEach(([col, { access, aliasFor, dependsOn }]) => {
          if (access && access !== accessType) {
            return
          }
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
        if (access && access !== accessType) {
          continue
        }
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
  return [[...cacheColumns], [...queryColumns]]
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

/**
 * Returns string following ISO 8601 'yyyy-mm-dd'
 * @param {Date} date Date to represent as ISO
 * @returns {string} ISO 8601 representation of the date
 */
const toISODate = date => `${date.getUTCFullYear()}-${
  (date.getUTCMonth() + 1).toString().padStart(2, '0')
}-${date.getUTCDate().toString().padStart(2, '0')
}`

/**
 * Returns all customers of a certain types (advertisers vs. agency) given some filters
 * @param {number[]|-1} [whitelabelIDs=-1] Whitelabel filter. -1 means all.
 * @param {number[]|-1} [agencyIDs=-1] Agency filter. -1 means all.
 * @param {number} [type=1] Customer type. See customer enum values.
 * @returns {Promise<{ customerID: number, customerName: string }[]>} Array of customers
 */
const getCustomers = async (whitelabelIDs = -1, agencyIDs = -1, type = CU_AGENCY) => {
  const filters = ['isactive', 'whitelabelid <> 0']
  const filterValues = []
  filters.push(type === CU_AGENCY ? 'agencyid = 0' : 'agencyid <> 0')
  if (Array.isArray(agencyIDs)) {
    filterValues.push(agencyIDs)
    filters.push(`${type === CU_AGENCY ? 'customerid' : 'agencyid'} = ANY($${filterValues.length})`)
  }
  if (Array.isArray(whitelabelIDs)) {
    filterValues.push(whitelabelIDs)
    filters.push(`whitelabelid = ANY($${filterValues.length})`)
  }

  const { rows } = await pgWithCache(`
    SELECT
      customerid AS "customerID",
      companyname AS "customerName"
    FROM public.customers
    WHERE
      ${filters.join(' AND ')}
  `, filterValues, atomPool, { ttl: 600, gzip: false }) // 10 minutes
  return rows
}

/**
 * Returns a unique hash per unordered collection of columns
 * @param {string[]} cols The view's columns
 * @returns {string} The view's hash
 */
const getViewHash = (cols) => {
  const hash = createHash('sha256')
  cols.sort().forEach((col) => {
    if (['view_hash', 'date', 'hour'].includes(col)) {
      return
    }
    hash.update(col)
  })
  return hash.digest('base64')
}

/**
 * Randomly assigns a partition number given view type (0-based)
 * Note: Partitions are not deterministically assigned so that the partition space
 * can be expanded at little cost (vs. for e.g. reallocation in case of rehashing)
 * @param {string} logType Log type
 * @returns {number} The partition number or undefined if supplied logType is not partitioned
 */
const assignCachePartition = (logType) => {
  if (!logTypes[logType].partitions) {
    return
  }
  return Math.floor(Math.random() * logTypes[logType].partitions)
}

/**
 * Returns the public representation of a log type's columns
 * @param {string} logType Log type
 * @param {number} [accessType=2] Access type. See access enum values.
 * @returns {object} Columns
 */
// eslint-disable-next-line arrow-body-style
const getReqViewColumns = (logType, accessType = ACCESS_CUSTOMER) => {
  return Object.entries(logTypes[logType].columns).reduce(
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
 * Returns a promise resolving after ms milliseconds
 * @param {number} ms Time (in ms) to wait for before the promise resolves
 * @returns {Promise<undefined>}
 */
const waitForMs = ms => new Promise(resolve => setTimeout(resolve, ms))

/**
 * Retries wrapped Athena method on throttling errors
 * @param {Function} callback Athena method to retry (use bind() to bind to Athena client)
 * @param {any[]} args Arguments to pass to the callback
 * @param {number} [maxAttempts=4] Maximum number of attempts
 * @param {number} [minWaitMS=1000] Minimum wait time (in ms) between each attempt
 * @returns {Promise<any>} Return value of the Athena method
 */
const retryAthenaOnThrottlingException = async (
  callback,
  args,
  maxAttempts = 4,
  minWaitMS = 1000,
) => {
  let attempt = 0
  while (attempt < maxAttempts) {
    try {
      // eslint-disable-next-line no-await-in-loop
      return await callback(...args).promise()
    } catch (err) {
      if (attempt === maxAttempts - 1 || ![
        'TooManyRequestsException',
        'ThrottlingException',
      ].includes(err.code)) {
        throw err
      }
      // exponential backoff
      // eslint-disable-next-line no-await-in-loop
      await waitForMs((2 ** attempt) * minWaitMS)
      attempt += 1
    }
  }
}

/**
 * Submits a query to Athena to retrieve a view's data
 * @param {number} customerID Customer
 * @param {string} logType Log type
 * @param {string} viewHash Hash representation of the view
 * @param {number} partition Cache partition
 * @param {string[]} viewColumns Columns which make up the view
 * @param {Date} start Start date (exclusive) from which to pull data
 * @param {Date} end End date (inclusive) up until which data will be considered
 * @returns {Promise<string>} Athena's 'Query Execution ID'
 */
const requestViewData = async (
  customerID, logType, viewHash, partition, viewColumns, start, end,
) => {
  try {
    const groupByColumns = [
      `'${viewHash}' AS view_hash`,
      '"date"',
      'hour',
    ]
    const aggColumns = []

    viewColumns.forEach((col) => {
      if (['view_hash', 'date', 'hour'].includes(col)) {
        return
      }
      const { expression, isAggregate } = logTypes[logType].columns[col]
      if (isAggregate) {
        aggColumns.push(expression || `SUM("${col}") AS "${col}"`)
        return
      }
      groupByColumns.push(expression || `"${col}"`)
    })

    const isoStart = toISODate(start)
    const isoEnd = toISODate(end)
    const startHour = start.getUTCHours()
    const endHour = end.getUTCHours()
    const dateFilter = []
    if (isoStart === isoEnd) {
      // eslint-disable-next-line max-len
      dateFilter.push(`"date" = date '${isoEnd}' AND hour BETWEEN ${startHour + 1} AND ${endHour}`)
    } else {
      dateFilter.push(`"date" > date '${isoStart}' AND "date" < date '${isoEnd}'`)
      // start is exclusive
      if (startHour !== 23) {
        dateFilter.push(`"date" = date '${isoStart}' AND hour > ${startHour}`)
      }
      // end is inclusive
      if (endHour !== 23) {
        dateFilter.push(`"date" = date '${isoEnd}' AND hour <= ${endHour}`)
      } else {
        dateFilter.push(`"date" = date '${isoEnd}'`)
      }
    }

    // token is used to cache queries at Athena's end
    // stable within current hour so queries can potentially be rerun in future
    const thisHour = new Date()
    thisHour.setUTCMinutes(0, 0, 0)
    const thisHourToken = `${toISODate(thisHour)}-${thisHour.getUTCHours()}`
    const token = `ml-${logType}-${customerID}-${viewHash}-${isoEnd}-${endHour}-${thisHourToken}`
    const { QueryExecutionId } = await retryAthenaOnThrottlingException(
      athena.startQueryExecution.bind(athena),
      [{
        QueryString: `
          SELECT
            ${[...groupByColumns, ...aggColumns].join(', ')}
          FROM ${logTypes[logType].table}
          WHERE
            customer_id = ${customerID}
            AND "date" IS NOT NULL
            AND hour IS NOT NULL
            AND (
              ${dateFilter.join(' OR ')}
            )
          GROUP BY ${groupByColumns.map((_, i) => i + 1).join(', ')}
        `,
        ClientRequestToken: token,
        // eslint-disable-next-line max-len
        ResultConfiguration: { OutputLocation: `s3://${ATHENA_OUTPUT_BUCKET}/${logType}/${customerID}${partition !== undefined ? `/${partition}` : ''}` },
        WorkGroup: ATHENA_WORKGROUP,
      }],
    )
    return QueryExecutionId
  } catch (err) {
    if (
      err.code === 'InvalidRequestException'
      && err.message === 'Idempotent parameters do not match'
    ) {
      return ''
    }
    throw err
  }
}

/**
 * Returns the latest end date for which an Athena query was sucessfully submitted along
 * with the partition index the view is stored under and the number of pending updates
 * @param {number} customerID Customer
 * @param {string} logType Log type
 * @param {string} viewHash Hash representation of the view
 * @returns {Promise<{ cacheUntil: Date, partition: number, pendingUpdates: number }>} undefined
 * if the view does not exist in cache
 */
const getViewCacheMeta = async (customerID, logType, viewHash) => {
  const { rows: [viewMeta] } = await knex.raw(`
    SELECT
      v.cached_until::timestamptz AS "cachedUntil",
      v.partition,
      COALESCE(COUNT(vu.*), 0)::int AS "pendingUpdates"
    FROM public.logs_views v
    LEFT JOIN public.logs_view_updates vu ON
      vu.log_type = v.log_type
      AND vu.customer_id = v.customer_id
      AND vu.view_hash = v.view_hash
    WHERE
      v.log_type = ?
      AND v.customer_id = ?
      AND v.view_hash = ?
    GROUP BY 1, 2
  `, [logType, customerID, viewHash])
  if (viewMeta && viewMeta.partition === null) {
    viewMeta.partition = undefined
  }
  return viewMeta
}

/**
 * Updates the view cache's end date and inserts pending update
 * @param {number} customerID Customer
 * @param {string} logType Log type
 * @param {string} viewHash Hash representation of the view
 * @param {number} partition Cache partition (0-based, undefined if the view is not partitioned)
 * @param {string} executionID Athena's query execution ID
 * @param {Date} start Update start date (exclusive)
 * @param {Date} end Update end date (inclusive)
 * @returns {Knex.Transaction}
 */
const updateViewCacheMeta = (
  customerID, logType, viewHash, partition, executionID, start, end,
) => knex
  .transaction(trx => Promise.all([
    // insert cache update request in logs_view_updates
    trx.raw(`
      INSERT INTO public.logs_view_updates
        (log_type, customer_id, view_hash, execution_uuid, start_date, end_date)
        VALUES (:logType, :customerID, :viewHash, :executionID, :start, :end)
      ON CONFLICT (log_type, customer_id, view_hash, execution_uuid) DO NOTHING
    `, {
      logType,
      customerID,
      viewHash,
      executionID,
      start: start.toISOString(),
      end: end.toISOString(),
    }),
    // update the cached_until date in logs_views
    trx.raw(`
      INSERT INTO public.logs_views
        (log_type, customer_id, view_hash, cached_until, partition)
        VALUES (
          :logType,
          :customerID,
          :viewHash,
          :end,
          :partition
        )
      ON CONFLICT (log_type, customer_id, view_hash) DO UPDATE
      SET cached_until = :end;
    `, {
      logType,
      customerID,
      viewHash,
      end: end.toISOString(),
      partition: partition !== undefined ? partition : null,
    }),
  ]))

/**
 * Spawns Athena queries to fill the view's PG cache with data for the last CACHE_DAYS days
 * @param {number} customerID Customer
 * @param {string} logType Log type
 * @param {string} viewHash Hash representation of the view
 * @param {Object} viewMeta View metadata
 * @param {string[]} viewColumns Columns which make up the view
 * @returns {Promise<boolean>} Whether or not the view's PG cache was updated
 */
const updateViewCache = async (customerID, logType, viewHash, viewMeta, viewColumns) => {
  try {
    const {
      cachedUntil,
      partition,
      pendingUpdates,
    } = viewMeta
    const thisHour = new Date()
    thisHour.setUTCMinutes(0, 0, 0)
    // start is exclusive
    let start = cachedUntil
      ? new Date(Math.max(
        new Date(cachedUntil).valueOf(),
        thisHour.valueOf() - (PG_CACHE_DAYS * ONE_DAY_MS),
      ))
      : new Date(thisHour.valueOf() - (PG_CACHE_DAYS * ONE_DAY_MS))
    let end
    const viewColumnsCount = viewColumns.reduce(
      (count, col) => (['view_hash', 'date', 'hour'].includes(col) ? count : count + 1),
      0,
    )
    let updated = pendingUpdates > 0
    // split time range into multiple queries
    while (start < thisHour) {
      // end is inclusive
      // except for the last day 'end' ends at 23h so that it queries full
      // partitions (logs partitioned by day)
      // the more fields the view includes the less dates the athena query spans
      end = new Date(Math.min(
        start.valueOf()
        + ((2 ** Math.max(4 - viewColumnsCount, 0)) * ONE_DAY_MS)
        + ((23 - start.getUTCHours()) * ONE_HOUR_MS),
        thisHour.valueOf(),
      ))
      updated = true
      // wait for query to succeed before subitting next one to ensure no gap in dataset
      // eslint-disable-next-line no-await-in-loop
      const executionId = await requestViewData(
        customerID, logType, viewHash, partition, viewColumns, start, end,
      )
      // eslint-disable-next-line no-await-in-loop
      await updateViewCacheMeta(customerID, logType, viewHash, partition, executionId, start, end)
      start = end
    }

    return updated
  } catch (err) {
    throw apiError('Error updating the view cache', 500)
  }
}

const getView = async (access, reqViews, reqViewColumns, { logType, query, agencyID }) => {
  if (!(logType in logTypes)) {
    throw apiError(`Invalid log type: ${logType}`, 403)
  }
  // check access
  const { whitelabel, customers, prefix } = access
  if (!(Array.isArray(customers) && customers.includes(agencyID)) && customers !== -1) {
    throw apiError('Invalid access permissions', 403)
  }
  const [{ customerID } = {}] = await getCustomers(whitelabel, [agencyID], logTypes[logType].owner)
  if (!customerID) {
    throw apiError('Invalid access permissions', 403)
  }

  const viewID = `logs_${logType}_${agencyID}`
  const [cacheColumns, queryColumns] = getQueryColumns(
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
  let viewHash
  let viewMeta
  if (!fastView) {
    viewHash = getViewHash(cacheColumns)
    viewMeta = await getViewCacheMeta(customerID, logType, viewHash)
    viewMeta = viewMeta || { partition: assignCachePartition(logType) }
    const isUpdating = await updateViewCache(customerID, logType, viewHash, viewMeta, cacheColumns)
    if (isUpdating) {
      // async query
      return false
    }
  }

  const groupByColumns = []
  const aggColumns = []
  const joinViews = {}
  const fdwConnections = new Set()

  queryColumns.forEach((col) => {
    const { aliasFor } = logTypes[logType].columns[col]
    const { viewExpression, joins, isAggregate } = logTypes[logType].columns[aliasFor || col]
    if (joins) {
      joins.forEach((join) => {
        joinViews[join.view] = join
      })
    }
    if (isAggregate) {
      aggColumns.push(`${viewExpression || `SUM(log."${aliasFor || col}")`} AS "${col}"`)
      return
    }
    groupByColumns.push(`${viewExpression || `log."${aliasFor || col}"`} AS "${col}"`)
  })

  reqViews[viewID] = knex
    .select(knex.raw([...groupByColumns, ...aggColumns].join(', ')))
    .as(viewID)

  if (fastView) {
    const { view, fdwConnection } = getPgView(fastView, customerID)
    if (fdwConnection) {
      fdwConnections.add(fdwConnection)
    }
    reqViews[viewID].from({ log: knex.select().from(view) })
  } else {
    const hasPartition = viewMeta && (viewMeta.partition !== undefined)
    reqViews[viewID]
      .from(
        { log: `public.logs_${logType}${hasPartition ? `_${viewMeta.partition}` : ''}` },
        { only: true },
      )
      .where({
        'log.customer_id': customerID,
        'log.view_hash': viewHash,
      })
  }

  if (groupByColumns.length) {
    reqViews[viewID].groupByRaw(groupByColumns.map((_, i) => i + 1).join(', '))
  }

  // join enrich views
  Object.values(joinViews).forEach(({ type, view, condition }) => {
    const { view: knexView, fdwConnection } = getPgView(view, customerID)
    if (fdwConnection) {
      fdwConnections.add(fdwConnection)
    }
    reqViews[viewID][`${type}Join`](knexView, condition)
  })

  // init connections to foreign db's
  await Promise.all([...fdwConnections].map(connectionName => fdwConnect({ connectionName })))

  reqViewColumns[viewID] = getReqViewColumns(logType, accessMap[prefix])
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
          view.columns = getReqViewColumns(type, accessMap[prefix])
        }
        return view
      }),
    ),
    [],
  )
}

const listView = async (access, viewID) => {
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
    columns: getReqViewColumns(logType, accessMap[prefix]),
  }
}


module.exports = {
  getView,
  listViews,
  listView,
}
