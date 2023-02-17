const { Transform, pipeline } = require('stream')
const { promisify } = require('util')
const {
  CAT_STRING,
  CAT_NUMERIC,
  CAT_DATE,
} = require('../type')
const { filterViewColumns } = require('./utils')
const trino = require('../../util/trino')
const { useAPIErrorOptions } = require('../../util/api-error')
const { viewTypes, viewCategories } = require('./taxonomies')


const { apiError } = useAPIErrorOptions({ tags: { service: 'ql' } })
const pipelineAsync = promisify(pipeline)

const columns = {
  account_type: { key: 'account_type', category: CAT_STRING },
  consumer_profile_province: { key: 'consumer_profile_province', category: CAT_STRING },
  consumer_profile_latitude: { key: 'consumer_profile_latitude', category: CAT_NUMERIC },
  consumer_profile_longitude: { key: 'consumer_profile_longitude', category: CAT_NUMERIC },
  encrypted_consumer_id: { key: 'encrypted_consumer_id', category: CAT_STRING },
  transaction_identifier: { key: 'transaction_identifier', category: CAT_STRING },
  transaction_type: { key: 'transaction_type', category: CAT_STRING },
  account_number: { key: 'account_number', category: CAT_STRING },
  amount_cents: { key: 'amount_cents', category: CAT_NUMERIC },
  amount_currency: { key: 'amount_currency', category: CAT_STRING },
  consumer_birth_year: { key: 'consumer_birth_year', category: CAT_NUMERIC },
  consumer_gender: { key: 'consumer_gender', category: CAT_STRING },
  consumer_profile_country: { key: 'consumer_profile_country', category: CAT_STRING },
  institution_name: { key: 'institution_name', category: CAT_STRING },
  consumer_profile_region: { key: 'consumer_profile_region', category: CAT_STRING },
  date: { key: 'date', category: CAT_DATE },
  date_posted: { key: 'date_posted', category: CAT_DATE },
  digest: { key: 'digest', category: CAT_STRING },
  consumer_profile_postal_code: { key: 'consumer_profile_postal_code', category: CAT_STRING },
  account_name: { key: 'account_name', category: CAT_STRING },
  retailer_name: { key: 'retailer_name', category: CAT_STRING },
  category_name: { key: 'category_name', category: CAT_STRING },
  transaction_description: { key: 'transaction_description', category: CAT_STRING },
  unique_id: { key: 'unique_id', category: CAT_STRING },
  year: { key: 'year', category: CAT_STRING },
  month: { key: 'month', category: CAT_STRING },
  day: { key: 'day', category: CAT_STRING },
}

const parseViewID = (access, viewID) => {
  const [, type, , yearStr, monthStr, dayStr,
  ] = viewID.match(/^paymi_(production)(_(\d{4})(\d{2})?(\d{2})?)?$/) || []
  const year = parseInt(yearStr)
  const month = parseInt(monthStr)
  const day = parseInt(dayStr)
  // user can only get one month of data at a time. Except for dev
  if (access.prefix !== 'dev' && (Number.isNaN(year) || Number.isNaN(month))) {
    throw apiError(`Invalid view: ${viewID}`, 400)
  }
  return { type, year, month, day }
}

// sync between the metadata and the storage
const syncMetadata = () => {
  trino.query({
    query: `
    CALL system.sync_partition_metadata(
      schema_name=>'paymi_production',
      table_name=>'normalized_gbq',
      mode=>'FULL'
      )
    `,
    catalog: 'paymi_hive',
    error(error) { throw apiError(error.message, 400) },
  })
}

const getQueryView = async (access, viewID, queryColumns) => {
  const { year, month, day } = parseViewID(access, viewID)
  const filteredColumns = filterViewColumns(columns, queryColumns)
  if (!Object.keys(filteredColumns).length) {
    throw apiError(`No column selected from view: ${viewID}`, 400)
  }
  let whereConditions = ''
  if (!Number.isNaN(year)) {
    whereConditions += `WHERE year = '${year}'`
    if (!Number.isNaN(month)) {
      whereConditions += ` AND month = '${String(month).padStart(2, '0')}'`
      if (!Number.isNaN(day)) {
        whereConditions += ` AND day = '${String(day).padStart(2, '0')}'`
      }
    }
  }

  const query = `
    SELECT ${Object.keys(filteredColumns).map(col => `gbq.${col}`).join(',')}
    FROM paymi_hive.paymi_production.normalized_gbq gbq 
    ${whereConditions}
    `
  return { viewID, query, columns: filteredColumns, engine: 'trino' }
}

const listViews = async ({ inclMeta = true }) => {
  syncMetadata()
  const query = `
    SELECT DISTINCT year, month
    FROM paymi_hive.paymi_production."normalized_gbq$partitions"
    ORDER BY 1, 2
    `
  const queryStream = trino.query({
    query,
    error(error) { throw apiError(error.message, 400) },
  })
  const views = []
  const viewCategory = viewCategories.PAYMI_PRODUCTION
  const toViewObject = new Transform({
    objectMode: true,
    transform(row, _, cb) {
      const { year, month } = row
      const viewObject = {
        name: `${viewCategory} - ${year}-${month}`,
        view: {
          id: `${viewCategory}_${year}${month}`,
          type: viewTypes.PAYMI,
          category: viewCategory,
          year,
          month,
        },
      }
      if (inclMeta) {
        viewObject.columns = columns
      }
      views.push(viewObject)
      cb(null)
    },
  })

  await pipelineAsync(queryStream, toViewObject)
  return views
}

const getView = async (access, viewID) => {
  const { year, month, day } = parseViewID(access, viewID)
  const viewCategory = viewCategories.PAYMI_PRODUCTION
  let whereConditions = ''
  let name = `${viewCategory}`
  if (!Number.isNaN(year)) {
    whereConditions += `WHERE year = '${year}'`
    name += ` - ${year}`
    if (!Number.isNaN(month)) {
      whereConditions += ` AND month = '${String(month).padStart(2, '0')}'`
      name += `-${month}`
      if (!Number.isNaN(day)) {
        whereConditions += ` AND day = '${String(day).padStart(2, '0')}'`
        name += `-${day}`
      }
    }
  }
  syncMetadata()
  const query = `
      SELECT *
      FROM paymi_hive.paymi_production."normalized_gbq$partitions"
      ${whereConditions}
      LIMIT 1
      `
  const queryStream = trino.query({
    query,
    error(error) { throw apiError(error.message, 400) },
  })
  const views = []
  const toViewObject = new Transform({
    objectMode: true,
    transform(row, _, cb) {
      const viewObject = {
        name,
        view: {
          id: viewID,
          type: viewTypes.PAYMI,
          category: viewCategory,
          year,
          month,
          day,
        },
        columns,
      }
      views.push(viewObject)
      cb(null)
    },
  })

  await pipelineAsync(queryStream, toViewObject)
  if (views.length === 0) {
    throw apiError(`Invalid view: ${viewID}`, 400)
  }
  return views[0]
}

module.exports = {
  getQueryView,
  listViews,
  getView,
}

