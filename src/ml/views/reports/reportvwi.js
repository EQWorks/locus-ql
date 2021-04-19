const { knex } = require('../../../util/db')
const { listLayers } = require('../../../routes/layer/interface')
const {
  CAT_STRING,
  CAT_NUMERIC,
  CAT_DATE,
  CAT_JSON,
  CAT_BOOL,
} = require('../../type')
const { geoTypes } = require('../../geo')
const { apiError } = require('../../../util/api-error')
const { knexWithCache } = require('../../cache')
const { viewTypes, viewCategories } = require('../taxonomies')


const options = {
  columns: {
    time_zone: { category: CAT_STRING },
    poi_id: {
      category: CAT_NUMERIC,
      geo_type: geoTypes.POI,
    },
    poi_name: { category: CAT_STRING },
    chain_id: { category: CAT_NUMERIC },
    type: { category: CAT_NUMERIC },
    category: { category: CAT_NUMERIC },
    lat: { category: CAT_NUMERIC },
    lon: { category: CAT_NUMERIC },
    address_label: { category: CAT_STRING },
    address_line1: { category: CAT_STRING },
    address_line2: { category: CAT_STRING },
    address_unit: { category: CAT_STRING },
    address_city: { category: CAT_STRING },
    address_region: { category: CAT_STRING },
    address_postalcode: { category: CAT_STRING },
    address_country: { category: CAT_STRING },
    geo_ca_fsa: {
      category: CAT_STRING,
      geo_type: geoTypes.CA_FSA,
    },
    geo_us_postalcode: {
      category: CAT_NUMERIC,
      // geo_type: 'us-postalcode',
    },

    vwi_factor: { category: CAT_NUMERIC },
    name: { category: CAT_STRING },

    report_id: { category: CAT_NUMERIC },
    beacon_id: { category: CAT_NUMERIC },
    beacon_name: { category: CAT_STRING },
    date_type: { category: CAT_NUMERIC },
    start_date: { category: CAT_DATE },
    end_date: { category: CAT_DATE },
    repeat_type: { category: CAT_NUMERIC },
    outlier: { category: CAT_BOOL },

    beacon: { category: CAT_NUMERIC },
    beacon_unique_user: { category: CAT_NUMERIC },
    beacon_xdevice: { category: CAT_NUMERIC },
    beacon_unique_hh: { category: CAT_NUMERIC },
    converted_beacon: { category: CAT_NUMERIC },
    converted_beacon_unique_user: { category: CAT_NUMERIC },
    beacon_conversion_rate: { category: CAT_NUMERIC },
    converted_beacon_xdevice: { category: CAT_NUMERIC },
    converted_beacon_unique_hh: { category: CAT_NUMERIC },
    converted_visits: { category: CAT_NUMERIC },
    converted_unique_visitors: { category: CAT_NUMERIC },
    converted_repeat_visits: { category: CAT_NUMERIC },
    converted_repeat_visitors: { category: CAT_NUMERIC },
    repeat_conversion_rate: { category: CAT_NUMERIC },
    converted_visits_hod: { category: CAT_JSON },
    converted_visits_dow: { category: CAT_JSON },
    converted_unique_visitors_hod: { category: CAT_JSON },
    converted_unique_visitors_dow: { category: CAT_JSON },
    timeto_conversion: { category: CAT_NUMERIC },
    converted_unique_visitors_single_visit: { category: CAT_NUMERIC },
    converted_unique_visitors_multi_visit: { category: CAT_NUMERIC },
    converted_unique_xdevice: { category: CAT_NUMERIC },
    converted_unique_hh: { category: CAT_NUMERIC },
    vendor: { category: CAT_STRING },
    campaign: { category: CAT_NUMERIC },
  },
  aoi: {
    aoi_type: { category: CAT_STRING },
    aoi_id: { category: CAT_STRING },
    aoi_category: { category: CAT_STRING },
    inflator: { category: CAT_NUMERIC },
  },
}

const getReportLayers = (wl, cu, { layerID, reportID }) => {
  const whereFilters = { 'report.type': 2 } // vwi
  if (reportID) {
    whereFilters['report_vwi.report_id'] = reportID
  }
  if (layerID) {
    whereFilters.layer_id = layerID
  }
  const layerQuery = knex('layer')
  layerQuery.column(['layer.name', 'layer.layer_id', 'layer.report_id', 'report.type'])
  layerQuery.select(knex.raw(`
    COALESCE(
      ARRAY_AGG(DISTINCT ARRAY[
        report_vwi.start_date::varchar,
        report_vwi.end_date::varchar,
        report_vwi.date_type::varchar
      ])
      FILTER (WHERE report_vwi.start_date IS NOT null),
      '{}'
    ) AS dates
  `))
  layerQuery.select(knex.raw(`
    COALESCE(
      ARRAY_AGG(DISTINCT report_vwi.vendor) FILTER (WHERE report_vwi.vendor != ''), '{}'
    ) AS vendors
  `))
  layerQuery.select(knex.raw(`
    COALESCE(
      ARRAY_AGG(DISTINCT ARRAY[report_vwi.campaign, camps.name])
      FILTER (WHERE report_vwi.campaign != ''),
      '{}'
    ) AS camps
  `))
  layerQuery.leftJoin('customers', 'layer.customer', 'customers.customerid')
  layerQuery.leftJoin('report', 'layer.report_id', 'report.report_id')
  layerQuery.innerJoin('report_vwi', 'report.report_id', 'report_vwi.report_id')
  layerQuery.joinRaw('LEFT JOIN camps on camps.camp_id::text = report_vwi.campaign')
  layerQuery.where(whereFilters)
  layerQuery.whereNull('layer.parent_layer')
  if (wl !== -1) {
    layerQuery.whereRaw('layer.whitelabel = ANY (?)', [wl])
    if (cu !== -1) {
      layerQuery.whereRaw('(layer.customer = ANY (?) OR customers.agencyid = ANY (?))', [cu, cu])
    }
  }
  layerQuery.groupBy(['layer.name', 'layer.layer_id', 'layer.report_id', 'report.type'])
  return knexWithCache(layerQuery, { ttl: 600 }) // 10 minutes
}

const getLayerIDs = (wl, cu, reportID) => {
  const layerIDQuery = knex('layer')
  layerIDQuery.column('layer_id')
  layerIDQuery.where({ report_id: reportID })
  layerIDQuery.whereNull('parent_layer')
  if (wl !== -1) {
    layerIDQuery.whereRaw('whitelabel = ANY (?)', [wl])
    if (cu !== -1) {
      layerIDQuery.whereRaw('customer = ANY (?)', [cu])
    }
  }
  return knexWithCache(layerIDQuery, { ttl: 600 }) // 10 minutes
}

// TODO: fetch aoi data using another endpoint
const hasAOIData = async (wl, layerID, reportID) => {
  const whereFilters = ['vwi_aoi.report_id = ?', 'l.layer_id = ?']
  const whereValues = [reportID, layerID]
  if (wl !== -1) {
    whereFilters.push('l.whitelabel = ANY (?)')
    whereValues.push(wl)
  }
  const [{ exists } = {}] = await knexWithCache(
    knex.raw(`
      SELECT EXISTS (SELECT
        l.layer_id,
        vwi_aoi.aoi_id
      FROM layer l
        JOIN report_vwi_aoi vwi_aoi ON vwi_aoi.report_id = l.report_id
      WHERE ${whereFilters.join(' AND ')}
      )
    `, whereValues),
    { ttl: 600 }, // 10 minutes
  )
  return exists
}

const listViews = async ({ access, filter, inclMeta = true }) => {
  const { whitelabel, customers } = access
  if (whitelabel !== -1 && (!whitelabel.length || (customers !== -1 && !customers.length))) {
    throw apiError('Invalid access permissions', 403)
  }
  const reportLayers = await getReportLayers(whitelabel, customers, filter)
  return Promise.all(reportLayers.map(async ({
    name,
    layer_id,
    report_id,
    type,
    dates,
    vendors,
    camps,
  }) => {
    const view = {
      name,
      view: {
        id: `${viewTypes.REPORT_VWI}_${layer_id}_${report_id}`,
        type: viewTypes.REPORT_VWI,
        category: viewCategories.REPORT_VWI,
        report_id,
        layer_id,
      },
    }
    if (inclMeta) {
      const hasAOI = await hasAOIData(whitelabel, layer_id, report_id)
      // const columns = hasAOI ? { ...options.columns, ...options.aoi } : options.columns
      const { columns } = options
      Object.entries(columns).forEach(([key, column]) => { column.key = key })
      Object.assign(view, {
        columns,
        report_type: type,
        dates: dates.map(([start, end, dType]) => ({ start, end, dType: parseInt(dType) })),
        vendors,
        camps: camps.map(([campID, name]) => ({ campID: parseInt(campID), name })),
        hasAOI,
      })
    }
    return view
  }))
}

const getView = async (access, viewID) => {
  const { whitelabel, customers } = access
  if (whitelabel !== -1 && (!whitelabel.length || (customers !== -1 && !customers.length))) {
    throw apiError('Invalid access permissions', 403)
  }
  const [, layerIDStr, reportIDStr] = viewID.match(/^reportvwi_(\d+|\w+)_(\d+)$/) || []
  let layerIDs = []

  // UNCOMMENT WHEN layer_id = 'any' is deprecated
  // eslint-disable-next-line radix
  // const layerID = parseInt(layerIDStr, 10)
  // if (!layerID) {
  //   throw apiError(`Invalid view: ${viewID}`, 403)
  // }

  // eslint-disable-next-line radix
  const reportID = parseInt(reportIDStr, 10)
  if (!reportID) {
    throw apiError(`Invalid view: ${viewID}`, 403)
  }
  // TO BE DEPRECATED - Filtering by report now available via `report` query param
  // optional layer_id param
  if (layerIDStr === 'any') {
    layerIDs = await getLayerIDs(whitelabel, customers, reportID)
  } else {
    // eslint-disable-next-line radix
    layerIDs = [{ layer_id: parseInt(layerIDStr, 10) }]
  }

  const viewLayers = await Promise.all(layerIDs.map(async ({ layer_id }) => {
    const [reportLayer] = await getReportLayers(
      whitelabel,
      customers,
      { layerID: layer_id, reportID },
    )
    if (!reportLayer) {
      return null
    }
    const { name, type, dates, vendors, camps } = reportLayer
    const hasAOI = await hasAOIData(whitelabel, layer_id, reportID)
    // const columns = hasAOI ? { ...options.columns, ...options.aoi } : options.columns
    const { columns } = options
    Object.entries(columns).forEach(([key, column]) => { column.key = key })
    return {
      name,
      view: {
        id: `${viewTypes.REPORT_VWI}_${layer_id}_${reportID}`,
        type: viewTypes.REPORT_VWI,
        category: viewCategories.REPORT_VWI,
        report_id: reportID,
        layer_id,
      },
      columns,
      report_type: type,
      dates: dates.map(([start, end, dateType]) => ({ start, end, dateType: parseInt(dateType) })),
      vendors,
      camps: camps.map(([campID, name]) => ({ campID: parseInt(campID), name })),
      hasAOI,
    }
  }))

  const views = viewLayers.filter(v => v)
  if (views.length === 0) {
    throw apiError('Access to layer(s) not allowed', 403)
  }
  if (layerIDStr === 'any') {
    return views
  }
  return views[0]
}

const getQueryView = async (access, { layer_id, report_id }) => {
  const { whitelabel, customers } = access
  if (whitelabel !== -1 && (!whitelabel.length || (customers !== -1 && !customers.length))) {
    throw apiError('Invalid access permissions', 403)
  }
  const viewID = `${viewTypes.REPORT_VWI}_${layer_id}_${report_id}`
  const [layer] = await listLayers(
    whitelabel,
    customers,
    { layer_id, report_id },
  )
  if (!layer) {
    throw apiError('Access to layer or report not allowed', 403)
  }

  // inject view columns
  const viewMeta = await listViews({
    access,
    filter: { layer_id, 'report_vwi.report_id': report_id },
  })
  const mlViewColumns = (viewMeta[0] || {}).columns

  const whereFilters = ['r.type = 2', 'r.report_id = ?', 'layer.layer_id = ?']
  const whereValues = [report_id, layer_id]
  if (whitelabel !== -1) {
    whereFilters.push('layer.whitelabel = ANY (?)')
    whereValues.push(whitelabel)
  }
  // inject view
  const mlView = knex.raw(`
    SELECT coalesce(tz.tzid, 'UTC'::TEXT) AS time_zone,
      poi.poi_id,
      poi.name AS poi_name,
      poi.chain_id,
      poi.type,
      poi.category,
      poi.lat,
      poi.lon,
      poi.address_label,
      poi.address_line1,
      poi.address_line2,
      poi.address_unit,
      poi.address_city,
      poi.address_region,
      poi.address_postalcode,
      poi.address_country,
      upper(substring(poi.address_postalcode from '^[A-Z]\\d[A-Z]')) AS geo_ca_fsa,
      substring(poi.address_postalcode from '^\\d{5}$')::int AS geo_us_postalcode,

      layer.vwi_factor,
      layer.name,

      vwi.report_id,
      b.id AS beacon_id,
      b.name AS beacon_name,
      vwi.date_type,
      vwi.start_date,
      vwi.end_date,
      vwi.repeat_type,
      vwi.outlier,

      vwi.beacon,
      vwi.beacon_unique_user,
      vwi.beacon_xdevice,
      vwi.beacon_unique_hh,
      vwi.converted_beacon * layer.vwi_factor as converted_beacon,
      vwi.converted_beacon_unique_user * layer.vwi_factor as converted_beacon_unique_user,
      CASE
        WHEN vwi.beacon_unique_user in (null, 0) THEN 0
        ELSE vwi.converted_beacon_unique_user / vwi.beacon_unique_user::float
      END as beacon_conversion_rate,
      vwi.converted_beacon_xdevice * layer.vwi_factor as converted_beacon_xdevice,
      vwi.converted_beacon_unique_hh * layer.vwi_factor as converted_beacon_unique_hh,
      vwi.converted_visits * layer.vwi_factor as converted_visits,
      vwi.converted_unique_visitors * layer.vwi_factor as converted_unique_visitors,
      vwi.converted_repeat_visits * layer.vwi_factor as converted_repeat_visits,
      vwi.converted_repeat_visitors * layer.vwi_factor as converted_repeat_visitors,
      CASE
        WHEN vwi.converted_unique_visitors in (null, 0) THEN 0
        ELSE vwi.converted_repeat_visitors / vwi.converted_unique_visitors::float
      END as repeat_conversion_rate,
      vwi.converted_visits_hod,
      vwi.converted_visits_dow,
      vwi.converted_unique_visitors_hod,
      vwi.converted_unique_visitors_dow,
      vwi.timeto_conversion,
      vwi.converted_unique_visitors_single_visit * layer.vwi_factor
        as converted_unique_visitors_single_visit,
      vwi.converted_unique_visitors_multi_visit * layer.vwi_factor
        as converted_unique_visitors_multi_visit,
      vwi.converted_unique_xdevice * layer.vwi_factor as converted_unique_xdevice,
      vwi.converted_unique_hh * layer.vwi_factor as converted_unique_hh,
      NULLIF(vwi.vendor, '') AS vendor,
      NULLIF(vwi.campaign,'')::int AS campaign
    FROM poi
    LEFT JOIN tz_world AS tz ON ST_Contains(
      tz.geom,
      ST_SetSRID(ST_MakePoint(poi.lon, poi.lat), 4326)
    )
    RIGHT JOIN poi_list_map ON poi.poi_id = poi_list_map.poi_id
    RIGHT JOIN layer ON layer.poi_list_id = poi_list_map.poi_list_id
    LEFT JOIN report AS r ON r.report_id = layer.report_id
    INNER JOIN report_vwi as vwi ON
      vwi.report_id = r.report_id
      AND vwi.poi_id = poi.poi_id
    LEFT JOIN beacons AS b ON
      b.camps = vwi.campaign
      AND b.vendors = vwi.vendor
    WHERE ${whereFilters.join(' AND ')}
  `, whereValues)

  return { viewID, mlView, mlViewColumns }
}

module.exports = {
  listViews,
  getView,
  getQueryView,
}
