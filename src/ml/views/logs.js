/* eslint-disable function-paren-newline */
/* eslint-disable no-continue */
const { createHash } = require('crypto')

const AWS = require('aws-sdk')

const { knex, atomPool } = require('../../util/db')
const { CAT_STRING, CAT_NUMERIC } = require('../type')
const apiError = require('../../util/api-error')


const athena = new AWS.Athena({ region: 'us-east-1' })

// constants
const CACHE_DAYS = 90
const ATHENA_OUTPUT_BUCKET = 'ml-fusion-cache'
const ATHENA_WORKGROUP = 'locus_ml'
const ONE_HOUR = 60 * 60 * 1000
const ONE_DAY = 24 * ONE_HOUR
const CU_ADVERTISER = 0
const CU_AGENCY = 1
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
      revenue: { category: CAT_NUMERIC },
      revenue_in_currency: { category: CAT_NUMERIC },
      cost: { category: CAT_NUMERIC },
      cost_in_currency: { category: CAT_NUMERIC },
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
    },
  },
}

const getQueryColumns = (viewID, viewColumns, query) => {
  const columns = new Set()
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
      columns.add(item.column)
      continue
    }
    if (
      Array.isArray(item) && item.length === 2 && typeof item[0] === 'string' && item[1] === viewID
    ) {
      if (item[0] === '*') {
        Object.keys(viewColumns).forEach(col => columns.add(col))
        continue
      }
      if (item[0] in viewColumns) {
        columns.add(item[0])
        continue
      }
    }
    queue.push(...Object.values(item))
  }
  return [...columns]
}

const toISODate = date => `${date.getUTCFullYear()}-${
  (date.getUTCMonth() + 1).toString().padStart(2, '0')
}-${date.getUTCDate().toString().padStart(2, '0')
}`

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
  const { rows } = await atomPool.query(`
    SELECT
      customerid AS "customerID",
      companyname AS "customerName"
    FROM public.customers
    WHERE
      ${filters.join(' AND ')}
  `, filterValues)
  return rows
}

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

const getReqViewColumns = logType => Object.entries(LOG_TYPES[logType].columns).reduce(
  (cols, [key, { category, geo_type }]) => {
    cols[key] = { category, geo_type, key }
    return cols
  },
  {},
)

const waitForMs = ms => new Promise(resolve => setTimeout(resolve, ms))

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
      return await callback(args).promise()
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
      {
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
      },
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

const updateViewCache = async (customerID, logType, viewHash, viewColumns) => {
  // get latest cache date
  const { rows: [{ max_date: maxDate }] } = await knex.raw(`
    SELECT
      max("date" + "hour" * interval '1 hour')::timestamptz as max_date
    FROM public.logs_${logType}
    WHERE
      customer_id = ?
      AND view_hash = ?
  `, [customerID, viewHash])
  const thisHour = new Date()
  thisHour.setUTCMinutes(0, 0, 0)
  // start is exclusive
  let start = maxDate
    ? new Date(Math.max(new Date(maxDate).valueOf(), thisHour.valueOf() - (CACHE_DAYS * ONE_DAY)))
    : new Date(thisHour.valueOf() - (CACHE_DAYS * ONE_DAY))
  let end
  const viewColumnsCount = viewColumns.reduce(
    (count, col) => (['view_hash', 'date', 'hour'].includes(col) ? count : count + 1),
    0,
  )
  const queries = []
  // split time range into multiple queries
  while (start < thisHour) {
    // end is inclusive
    // except for the last day 'end' ends at 23h so that it queries full
    // partitions (logs partitioned by day)
    // the more fields the view includes the less dates the athena query spans
    end = new Date(Math.min(
      start.valueOf()
      + ((2 ** Math.max(4 - viewColumnsCount, 0)) * ONE_DAY)
      + ((23 - start.getUTCHours()) * ONE_HOUR),
      thisHour.valueOf(),
    ))
    queries.push(requestViewData(customerID, logType, viewHash, viewColumns, start, end))
    start = end
  }

  const executionIds = await Promise.all(queries)
  if (executionIds.length === 1) {
    // Idempotent queries
    if (executionIds[0] === '') {
      return true
    }
    const res = await retryAthenaOnThrottlingException(
      athena.getQueryExecution.bind(athena),
      { QueryExecutionId: executionIds[0] },
    )
    const { State, CompletionDateTime } = res.QueryExecution.Status
    if (State === 'SUCCEEDED' && CompletionDateTime.valueOf() < Date.now() - (30 * 1000)) {
      return true
    }
  }
  return false
}

const getView = async (access, reqViews, reqViewColumns, { logType, query, agencyID }) => {
  if (!(logType in LOG_TYPES)) {
    throw apiError(`Invalid log type: ${logType}`, 403)
  }
  // check access
  const { whitelabel, customers } = access
  if (!(Array.isArray(customers) && customers.includes(agencyID)) && customers !== -1) {
    throw apiError('Invalid access permissions', 403)
  }
  const [{ customerID } = {}] = await getCustomers(whitelabel, [agencyID], LOG_TYPES[logType].owner)
  if (!customerID) {
    throw apiError('Invalid access permissions', 403)
  }

  const viewID = `logs_${logType}_${agencyID}`
  const viewColumns = getReqViewColumns(logType)
  const queryColumns = getQueryColumns(viewID, viewColumns, query)
  if (!queryColumns.length || queryColumns.length > 10) {
    throw apiError('Log views are restricted to queries pulling between 1 and 10 columns', 403)
  }
  if (queryColumns.some(col => !(col in LOG_TYPES[logType].columns))) {
    throw apiError(`Unknow column for log type: ${logType}`, 403)
  }
  const viewHash = getViewHash(queryColumns)

  const cacheReady = await updateViewCache(customerID, logType, viewHash, queryColumns)
  if (!cacheReady) {
    throw Error('We need a moment to load your query\'s data, please try again in 30s')
  }

  const groupByColumns = []
  const aggColumns = []
  queryColumns.forEach((col) => {
    if (LOG_TYPES[logType].columns[col].isAggregate) {
      aggColumns.push(`SUM("${col}") AS "${col}"`)
      return
    }
    groupByColumns.push(`"${col}"`)
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

  reqViewColumns[viewID] = viewColumns
}

const listViews = async (access) => {
  const { whitelabel, customers } = access
  const agencies = await getCustomers(whitelabel, customers, CU_AGENCY)
  return agencies.reduce(
    (views, { customerID, customerName }) => views.concat(
      Object.entries(LOG_TYPES).map(
        ([type, { name }]) => ({
          name: `${name} - ${customerName} (${customerID})`,
          view: {
            type: 'logs',
            id: `logs_${type}_${customerID}`,
            logType: type,
            agencyID: customerID,
          },
          // TODO: remove 'columns' -> use listView() to get full view
          columns: getReqViewColumns(type),
        }),
      ),
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
  const { whitelabel, customers } = access
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
    columns: getReqViewColumns(logType),
  }
}


module.exports = {
  getView,
  listViews,
  listView,
}
