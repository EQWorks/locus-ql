/* eslint-disable no-use-before-define */

const { knex } = require('../../util/db')
const {
  CAT_STRING,
  CAT_NUMERIC,
  CAT_DATE,
  CAT_JSON,
} = require('../type')
const { apiError } = require('../../util/api-error')

// common constants
const QUERY_BY_POINT = 'point'
const QUERY_BY_POI_LIST = 'poi_list'
const FREQ_DAILY = 'daily'
const FREQ_HOURLY = 'hourly'


const getQueryView = async (access, { filters, frequency, queryBy }) => {
  const viewID = `weather_${frequency}_${queryBy}`

  if (![QUERY_BY_POINT, QUERY_BY_POI_LIST].includes(queryBy)) {
    throw apiError(`Invalid queryBy: ${queryBy}`, 403)
  }
  if (![FREQ_DAILY, FREQ_HOURLY].includes(frequency)) {
    throw apiError(`Invalid frequency: ${frequency}`, 403)
  }

  if (queryBy === QUERY_BY_POINT) {
    // inject view columns, skipping listViews here because columns is fixed
    const mlViewColumns = columns

    // filter validation and normalization
    if (!filters || !filters.lat || !filters.lon) {
      throw apiError('Missing view filter: lat or lon', 403)
    }
    let { lat, lon } = filters
    if (Number.isNaN(lat) || Number.isNaN(lon)) {
      throw apiError('Invalid filter: lat or lon', 403)
    } else {
      lat = parseFloat(lat)
      lon = parseFloat(lon)
    }

    // inject view
    const mlView = knex.raw(`
      (
        SELECT DS.csd_gid, DS.local_ts at time zone 'UTC' as local_ts,
          DS.timezone, DS.data
        FROM canada_geo.csd CSD
        LEFT JOIN dark_sky_${frequency}_stats DS ON DS.csd_gid = CSD.gid
        WHERE ST_Contains(CSD.geom, ST_SetSRID(ST_MakePoint(?, ?), 4326))
      ) as ${viewID}
    `, [lon, lat])

    return { viewID, mlView, mlViewColumns }
  } else if (queryBy === QUERY_BY_POI_LIST) {
    // filter validation and normalization
    if (!filters || !filters['poi-list-id']) {
      throw apiError('Missing view filter: poi-list-id', 403)
    }
    let poiListID = filters['poi-list-id']
    if (Number.isInteger(poiListID)) {
      poiListID = parseInt(poiListID)
    } else {
      throw apiError('Invalid filter: poi-list-id', 403)
    }

    // inject view columns, skipping listViews here because columns is fixed
    const mlViewColumns = poiListColumns

    // inject view
    const { whitelabel, customers } = access
    if (whitelabel !== -1 && (!whitelabel.length || (customers !== -1 && !customers.length))) {
      throw apiError('Invalid access permissions', 403)
    }

    const mlView = knex.raw(`
      (
        SELECT poi.poi_id, DS.csd_gid, DS.local_ts at time zone 'UTC' as local_ts,
          DS.timezone, DS.data
        FROM poi_list
        LEFT JOIN poi_list_map ON poi_list_map.poi_list_id = poi_list.poi_list_id
        LEFT JOIN poi ON poi.poi_id = poi_list_map.poi_id
        LEFT JOIN canada_geo.csd CSD ON
          ST_Contains(CSD.geom, ST_SetSRID(ST_MakePoint(poi.lon, poi.lat), 4326))
        LEFT JOIN dark_sky_${frequency}_stats DS ON DS.csd_gid = CSD.gid
        WHERE
          poi_list.poi_list_id = :poiListID
          ${whitelabel !== -1 ? `AND poi_list.whitelabelid = ANY (:whitelabel)
            ${customers !== -1 ? 'AND poi_list.customerid = ANY (:customers)' : ''}
          ` : ''}
      ) as ${viewID}
    `, { poiListID, whitelabel, customers })

    return { viewID, mlView, mlViewColumns }
  }
}

const listViews = async ({ inclMeta = true }) => Object.values(VIEWS)
  .map((view) => {
    if (inclMeta) {
      return view
    }
    const { columns, ...rest } = view
    return rest
  })

const getView = async (_, viewID) => {
  if (!(viewID in VIEWS)) {
    throw apiError(`Invalid view: ${viewID}`, 403)
  }
  return VIEWS[viewID]
}

const columns = {
  csd_gid: { key: 'csd_gid', category: CAT_NUMERIC },
  local_ts: { key: 'local_ts', category: CAT_DATE },
  timezone: { key: 'timezone', category: CAT_STRING },
  data: { key: 'data', category: CAT_JSON },
}

const poiListColumns = {
  ...columns,
  poi_id: { key: 'poi_id', category: CAT_NUMERIC },
}

const VIEWS = {
  weather_daily_point: {
    name: 'weather daily point',
    view: {
      type: 'weather',
      id: 'weather_daily_point',
      filters: ['lat', 'lon'],
      frequency: FREQ_DAILY,
      queryBy: QUERY_BY_POINT,
    },
    columns,
  },
  weather_daily_poi_list: {
    name: 'weather daily poi list',
    view: {
      type: 'weather',
      id: 'weather_daily_poi_list',
      filters: ['poi-list-id'],
      frequency: FREQ_DAILY,
      queryBy: QUERY_BY_POI_LIST,
    },
    columns: poiListColumns,
  },
  weather_hourly_point: {
    name: 'weather hourly point',
    view: {
      type: 'weather',
      id: 'weather_hourly_point',
      filters: ['lat', 'lon'],
      frequency: FREQ_HOURLY,
      queryBy: QUERY_BY_POINT,
    },
    columns,
  },
  weather_hourly_poi_list: {
    name: 'weather hourly poi list',
    view: {
      type: 'weather',
      id: 'weather_hourly_poi_list',
      filters: ['poi-list-id'],
      frequency: FREQ_HOURLY,
      queryBy: QUERY_BY_POI_LIST,
    },
    columns: poiListColumns,
  },
}

module.exports = {
  getQueryView,
  listViews,
  getView,
}
