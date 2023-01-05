const {
  CAT_STRING,
  CAT_NUMERIC,
  CAT_DATE,
  CAT_JSON,
} = require('../type')
const { filterViewColumns } = require('./utils')
const { geometryTypes } = require('../parser/src')
const { useAPIErrorOptions } = require('../../util/api-error')
const { viewTypes, viewCategories } = require('./taxonomies')


const { apiError } = useAPIErrorOptions({ tags: { service: 'ql' } })

const FREQ_DAILY = 'daily'
const FREQ_HOURLY = 'hourly'

const VIEWS = {
  weather_daily: {
    name: 'weather daily',
    view: {
      id: `${viewTypes.WEATHER}_daily`,
      type: viewTypes.WEATHER,
      category: viewCategories.WEATHER,
      frequency: FREQ_DAILY,
    },
  },
  weather_hourly: {
    name: 'weather hourly',
    view: {
      id: `${viewTypes.WEATHER}_hourly`,
      type: viewTypes.WEATHER,
      category: viewCategories.WEATHER,
      frequency: FREQ_HOURLY,
    },
  },
}

const columns = {
  geo_ca_csd: { key: 'geo_ca_csd', geo_type: geometryTypes.CA_CSD, category: CAT_NUMERIC },
  local_ts: { key: 'local_ts', category: CAT_DATE },
  timezone: { key: 'timezone', category: CAT_STRING },
  data: { key: 'data', category: CAT_JSON },
}

const parseViewID = (viewID) => {
  if (!(viewID in VIEWS)) {
    throw apiError(`Invalid view: ${viewID}`, 400)
  }
  const { frequency } = VIEWS[viewID]
  return { frequency }
}

const getQueryView = async (_, viewID, queryColumns, engine) => {
  const { frequency } = parseViewID(viewID)
  const filteredColumns = filterViewColumns(columns, queryColumns)
  if (!Object.keys(filteredColumns).length) {
    throw apiError(`No column selected from view: ${viewID}`, 400)
  }
  const catalog = engine === 'trino' ? 'locus_place.' : ''
  const query = `
    SELECT
      csd_gid AS geo_ca_csd,
      local_ts AT TIME ZONE 'UTC' AS local_ts,
      timezone,
      data
    FROM ${catalog}public.dark_sky_${frequency}_stats
  `
  return { viewID, query, columns: filteredColumns }
}

const listViews = async ({ inclMeta = true }) => Object.values(VIEWS)
  .map(view => (inclMeta ? { view, columns } : view))

const getView = async (_, viewID) => {
  if (!(viewID in VIEWS)) {
    throw apiError(`Invalid view: ${viewID}`, 400)
  }
  return { ...VIEWS[viewID], columns }
}

module.exports = {
  getQueryView,
  listViews,
  getView,
}
