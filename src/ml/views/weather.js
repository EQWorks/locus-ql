/* eslint-disable no-use-before-define */
const { knex } = require('../../util/db')
const {
  CAT_STRING,
  CAT_NUMERIC,
  CAT_DATE,
  CAT_JSON,
} = require('../type')
const { geoTypes } = require('../geo')
const { apiError } = require('../../util/api-error')
const { viewTypes, viewCategories } = require('./taxonomies')


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
  geo_ca_csd: { key: 'geo_ca_csd', geo_type: geoTypes.CA_CSD, category: CAT_NUMERIC },
  local_ts: { key: 'local_ts', category: CAT_DATE },
  timezone: { key: 'timezone', category: CAT_STRING },
  data: { key: 'data', category: CAT_JSON },
}

const getQueryView = async (_, { frequency }) => {
  const viewID = `${viewTypes.WEATHER}_${frequency}`

  if (![FREQ_DAILY, FREQ_HOURLY].includes(frequency)) {
    throw apiError(`Invalid frequency: ${frequency}`, 400)
  }

  // view columns, skipping listViews here because columns is fixed
  const mlViewColumns = columns

  // view
  const mlView = knex.raw(`
    SELECT
      csd_gid AS geo_ca_csd,
      local_ts at time zone 'UTC' as local_ts,
      timezone,
      data
    FROM public.dark_sky_${frequency}_stats
  `)

  return { viewID, mlView, mlViewColumns }
}

const listViews = async ({ inclMeta = true }) => Object.values(VIEWS)
  .map((view) => {
    if (!inclMeta) {
      return view
    }
    return {
      view,
      columns,
    }
  })

const getView = async (_, viewID) => {
  if (!(viewID in VIEWS)) {
    throw apiError(`Invalid view: ${viewID}`, 403)
  }
  return {
    ...VIEWS[viewID],
    columns,
  }
}

module.exports = {
  getQueryView,
  listViews,
  getView,
}
