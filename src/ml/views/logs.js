/* eslint-disable function-paren-newline */
/* eslint-disable no-continue */
const { createHash } = require('crypto')

const { knex, atomPool } = require('../../util/db')
const { CAT_STRING, CAT_NUMERIC, CAT_JSON } = require('../type')
const apiError = require('../../util/api-error')
const { athena } = require('../../util/aws')
const { knexWithCache, pgWithCache } = require('../cache')


// constants
const CACHE_DAYS = 90 // days of logs to import into cache
const ATHENA_OUTPUT_BUCKET = 'ml-fusion-cache'
const ATHENA_WORKGROUP = 'locus_ml' // use to segregate billing and history
const ONE_HOUR_MS = 60 * 60 * 1000
const ONE_DAY_MS = 24 * ONE_HOUR_MS
const CU_AGENCY = 1 // customer type enum
const CU_ADVERTISER = 2 // customer type enum
const ACCESS_INTERNAL = 1 // access type enum
const ACCESS_CUSTOMER = 2 // access type enum
const LOG_TYPES = {
  imp: {
    name: 'ATOM Impressions',
    table: 'fusion_logs.impression_logs',
    owner: CU_ADVERTISER,
    columns: {
      camp_code: { category: CAT_NUMERIC },
      fsa: {
        category: CAT_STRING,
        geo_type: 'ca-fsa',
        expression: 'postal_code AS fsa',
      },
      impressions: {
        category: CAT_NUMERIC,
        expression: 'count(*) AS impressions',
        isAggregate: true,
      },
      clicks: {
        category: CAT_NUMERIC,
        expression: 'count_if(click) AS clicks',
        isAggregate: true,
      },
      user_ip: {
        category: CAT_STRING,
        expression: 'substr(to_hex(sha256(cast(ip AS varbinary))), 1, 20) AS user_ip',
      },
      user_id: {
        category: CAT_STRING,
        expression: 'substr(to_hex(sha256(cast(user_guid AS varbinary))), 1, 20) AS user_id',
      },
      hh_id: {
        category: CAT_STRING,
        expression: 'substr(to_hex(sha256(cast(hh_id AS varbinary))), 1, 20) AS hh_id',
      },
      hh_fsa: {
        category: CAT_STRING,
        geo_type: 'ca-fsa',
      },
      os_id: { category: CAT_NUMERIC },
      browser_id: { category: CAT_NUMERIC },
      city: { category: CAT_STRING },
      banner_code: { category: CAT_NUMERIC },
      app_platform_id: { category: CAT_NUMERIC },
      revenue: {
        category: CAT_NUMERIC,
        access: ACCESS_INTERNAL,
      },
      revenue_in_currency: {
        category: CAT_NUMERIC,
        access: ACCESS_INTERNAL,
      },
      spend: {
        aliasFor: 'revenue',
        access: ACCESS_CUSTOMER,
      },
      spend_in_currency: {
        aliasFor: 'revenue_in_currency',
        access: ACCESS_CUSTOMER,
      },
      cost: {
        category: CAT_NUMERIC,
        access: ACCESS_INTERNAL,
      },
      cost_in_currency: {
        category: CAT_NUMERIC,
        access: ACCESS_INTERNAL,
      },
    },
  },
  bcn: {
    name: 'LOCUS Beacons',
    table: 'fusion_logs.beacon_logs',
    owner: CU_AGENCY,
    columns: {
      camp_code: { category: CAT_NUMERIC },
      fsa: {
        category: CAT_STRING,
        geo_type: 'ca-fsa',
        expression: 'postal_code AS fsa',
      },
      beacon_id: { category: CAT_NUMERIC },
      impressions: {
        category: CAT_NUMERIC,
        expression: 'count(*) AS impressions',
        isAggregate: true,
      },
      user_ip: {
        category: CAT_STRING,
        expression: 'substr(to_hex(sha256(cast(ip AS varbinary))), 1, 20) AS user_ip',
      },
      user_id: {
        category: CAT_STRING,
        expression: 'substr(to_hex(sha256(cast(user_guid AS varbinary))), 1, 20) AS user_id',
      },
      hh_id: {
        category: CAT_STRING,
        expression: 'substr(to_hex(sha256(cast(hh_id AS varbinary))), 1, 20) AS hh_id',
      },
      hh_fsa: {
        category: CAT_STRING,
        geo_type: 'ca-fsa',
      },
      os_id: { category: CAT_NUMERIC },
      browser_id: { category: CAT_NUMERIC },
      city: { category: CAT_STRING },
      vendor: { category: CAT_STRING },
      type: { category: CAT_STRING },
      content: { category: CAT_JSON },
    },
  },
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
  const cacheColumns = new Set() // aliases are substituted with the columns they reference
  const queryColumns = new Set() // aliases not substituted
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
      const { access, aliasFor } = viewColumns[item.column]
      if (!access || access === accessType) {
        queryColumns.add(item.column)
        cacheColumns.add(aliasFor || item.column)
      }
      continue
    }
    if (
      Array.isArray(item) && item.length === 2 && typeof item[0] === 'string' && item[1] === viewID
    ) {
      if (item[0] === '*') {
        Object.entries(viewColumns).forEach(([col, { access, aliasFor }]) => {
          if (!access || access === accessType) {
            queryColumns.add(col)
            cacheColumns.add(aliasFor || col)
          }
        })
        continue
      }
      if (item[0] in viewColumns) {
        const { access, aliasFor } = viewColumns[item[0]]
        if (!access || access === accessType) {
          queryColumns.add(item[0])
          cacheColumns.add(aliasFor || item[0])
        }
        continue
      }
    }
    queue.push(...Object.values(item))
  }
  return [[...cacheColumns], [...queryColumns]]
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
  `, filterValues, atomPool, { ttl: 600 }) // 10 minutes
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
 * Returns the public representation of a log type's columns
 * @param {string} logType Log type
 * @param {number} [accessType=2] Access type. See access enum values.
 * @returns {object} Columns
 */
// eslint-disable-next-line arrow-body-style
const getReqViewColumns = (logType, accessType = ACCESS_CUSTOMER) => {
  return Object.entries(LOG_TYPES[logType].columns).reduce(
    (cols, [key, { category, geo_type, access, aliasFor }]) => {
      if (!access || access === accessType) {
        cols[key] = {
          category: aliasFor ? LOG_TYPES[logType].columns[aliasFor].category : category,
          geo_type: aliasFor ? LOG_TYPES[logType].columns[aliasFor].geo_type : geo_type,
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
 * @param {string[]} viewColumns Columns which make up the view
 * @param {Date} start Start date (exclusive) from which to pull data
 * @param {Date} end End date (inclusive) up until which data will be considered
 * @returns {Promise<string>} Athena's 'Query Execution ID'
 */
const requestViewData = async (customerID, logType, viewHash, viewColumns, start, end) => {
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
      const { expression, isAggregate } = LOG_TYPES[logType].columns[col]
      if (isAggregate) {
        aggColumns.push(expression || `"${col}"`)
        return
      }
      groupByColumns.push(expression || `"${col}"`)
    })

    const isoStart = toISODate(start)
    const isoEnd = toISODate(end)
    const dateFilter = []
    if (isoStart === isoEnd) {
      // eslint-disable-next-line max-len
      dateFilter.push(`"date" = date '${isoEnd}' AND hour BETWEEN ${start.getUTCHours() + 1} AND ${end.getUTCHours()}`)
    } else {
      dateFilter.push(`"date" > date '${isoStart}' AND "date" < date '${isoEnd}'`)
      // start is exclusive
      if (start.getUTCHours() !== 23) {
        dateFilter.push(`"date" = date '${isoStart}' AND hour > ${start.getUTCHours()}`)
      }
      // end is inclusive
      if (end.getUTCHours() !== 23) {
        dateFilter.push(`"date" = date '${isoEnd}' AND hour <= ${end.getUTCHours()}`)
      } else {
        dateFilter.push(`"date" = date '${isoEnd}'`)
      }
    }

    // token is used to cache queries at Athena's end
    const token = `ml-${logType}-${customerID}-${viewHash}-${isoEnd}-${end.getUTCHours()}`
    const { QueryExecutionId } = await retryAthenaOnThrottlingException(
      athena.startQueryExecution.bind(athena),
      [{
        QueryString: `
          SELECT
            ${[...groupByColumns, ...aggColumns].join(', ')}
          FROM ${LOG_TYPES[logType].table}
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
        ResultConfiguration: { OutputLocation: `s3://${ATHENA_OUTPUT_BUCKET}/${logType}/${customerID}` },
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
 * Returns the latest end date for which an Athena query was sucessfully submitted
 * @param {number} customerID Customer
 * @param {string} logType Log type
 * @param {string} viewHash Hash representation of the view
 * @returns {Promise<Date>} undefined if the view does not exist in cache
 */
const getViewCacheDate = async (customerID, logType, viewHash) => {
  const { rows: [{ cachedUntil } = {}] } = await knexWithCache(
    knex.raw(`
      SELECT
        cached_until::timestamptz as "cachedUntil"
      FROM public.logs_views
      WHERE
        log_type = ?
        AND customer_id = ?
        AND view_hash = ?
    `, [logType, customerID, viewHash]),
    { ttl: 1800 }, // 30 minutes
  )
  return cachedUntil
}

/**
 * Updates the view cache's end date
 * @param {number} customerID Customer
 * @param {string} logType Log type
 * @param {string} viewHash Hash representation of the view
 * @param {Date} end New cache end date
 */
const updateViewCacheDate = (customerID, logType, viewHash, end) => knex.raw(`
  INSERT INTO public.logs_views
    (log_type, customer_id, view_hash, cached_until)
    VALUES (:logType, :customerID, :viewHash, :end)
  ON CONFLICT (log_type, customer_id, view_hash) DO UPDATE
  SET cached_until = :end;
`, { logType, customerID, viewHash, end: end.toISOString() })

/**
 * Spawns Athena queries to fill the view's PG cache with data for the last CACHE_DAYS days
 * @param {number} customerID Customer
 * @param {string} logType Log type
 * @param {string} viewHash Hash representation of the view
 * @param {string[]} viewColumns Columns which make up the view
 * @returns {Promise<boolean>} Whether or not the view's PG cache was updated
 */
const updateViewCache = async (customerID, logType, viewHash, viewColumns) => {
  try {
    // get latest cache date
    const cacheDate = await getViewCacheDate(customerID, logType, viewHash)
    const thisHour = new Date()
    thisHour.setUTCMinutes(0, 0, 0)
    // start is exclusive
    let start = cacheDate
      ? new Date(
        Math.max(new Date(cacheDate).valueOf(), thisHour.valueOf() - (CACHE_DAYS * ONE_DAY_MS)),
      )
      : new Date(thisHour.valueOf() - (CACHE_DAYS * ONE_DAY_MS))
    let end
    const viewColumnsCount = viewColumns.reduce(
      (count, col) => (['view_hash', 'date', 'hour'].includes(col) ? count : count + 1),
      0,
    )
    let updated = false
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
      await requestViewData(customerID, logType, viewHash, viewColumns, start, end)
      // eslint-disable-next-line no-await-in-loop
      await updateViewCacheDate(customerID, logType, viewHash, end)
      start = end
    }

    return updated
  } catch (err) {
    throw apiError('Error updating the view cache', 500)
  }
}

const getView = async (access, reqViews, reqViewColumns, { logType, query, agencyID }) => {
  if (!(logType in LOG_TYPES)) {
    throw apiError(`Invalid log type: ${logType}`, 403)
  }
  // check access
  const { whitelabel, customers, prefix } = access
  if (!(Array.isArray(customers) && customers.includes(agencyID)) && customers !== -1) {
    throw apiError('Invalid access permissions', 403)
  }
  const [{ customerID } = {}] = await getCustomers(whitelabel, [agencyID], LOG_TYPES[logType].owner)
  if (!customerID) {
    throw apiError('Invalid access permissions', 403)
  }

  const viewID = `logs_${logType}_${agencyID}`
  const [cacheColumns, queryColumns] = getQueryColumns(
    viewID,
    LOG_TYPES[logType].columns,
    query,
    accessMap[prefix],
  )
  if (!cacheColumns.length || cacheColumns.length > 10) {
    throw apiError('Log views are restricted to queries pulling between 1 and 10 columns', 403)
  }
  if (cacheColumns.some(col => !(col in LOG_TYPES[logType].columns))) {
    throw apiError(`Unknow column for log type: ${logType}`, 403)
  }
  const viewHash = getViewHash(cacheColumns)

  const cacheIsUpdating = await updateViewCache(customerID, logType, viewHash, cacheColumns)
  if (cacheIsUpdating) {
    throw Error('We need a moment to load your query\'s data, please try again in 30s')
  }

  const groupByColumns = []
  const aggColumns = []
  queryColumns.forEach((col) => {
    const { aliasFor } = LOG_TYPES[logType].columns[col]
    if (LOG_TYPES[logType].columns[aliasFor || col].isAggregate) {
      aggColumns.push(`SUM("${aliasFor || col}") AS "${col}"`)
      return
    }
    groupByColumns.push(`"${aliasFor || col}" AS "${col}"`)
  })

  reqViews[viewID] = knex.raw(`
  (
    SELECT
      ${[...groupByColumns, ...aggColumns].join(', ')}
    FROM public.logs_${logType}
    WHERE
      customer_id = ?
      AND view_hash = ?
    ${groupByColumns.length ? `GROUP BY ${groupByColumns.map((_, i) => i + 1).join(', ')}` : ''}
  ) as ${viewID}
`, [customerID, viewHash])

  reqViewColumns[viewID] = getReqViewColumns(logType, accessMap[prefix])
}

const listViews = async ({ access, inclMeta = true }) => {
  const { whitelabel, customers, prefix } = access
  const agencies = await getCustomers(whitelabel, customers, CU_AGENCY)
  return agencies.reduce(
    (views, { customerID, customerName }) => views.concat(
      Object.entries(LOG_TYPES).map(([type, { name }]) => {
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
  if (!(logType in LOG_TYPES)) {
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
    name: `${LOG_TYPES[logType].name} - ${customerName} (${customerID})`,
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
