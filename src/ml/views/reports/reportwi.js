const { knex } = require('../../../util/db')
const { listLayers } = require('../../../routes/layer/interface')
const {
  CAT_STRING,
  CAT_NUMERIC,
  CAT_DATE,
  CAT_JSON,
  CAT_BOOL,
} = require('../../type')
const { apiError } = require('../../../util/api-error')
const { knexWithCache } = require('../../cache')


const options = {
  columns: {
    time_zone: { category: CAT_STRING },
    poi_id: { category: CAT_NUMERIC },
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

    wi_factor: { category: CAT_NUMERIC },
    name: { category: CAT_STRING },

    report_id: { category: CAT_NUMERIC },
    date_type: { category: CAT_NUMERIC },
    start_date: { category: CAT_DATE },
    end_date: { category: CAT_DATE },
    repeat_type: { category: CAT_NUMERIC },
    visits: { category: CAT_NUMERIC },
    unique_visitors: { category: CAT_NUMERIC },
    repeat_visits: { category: CAT_NUMERIC },
    repeat_visitors: { category: CAT_NUMERIC },
    repeat_visitor_rate: { category: CAT_NUMERIC },
    visits_hod: { category: CAT_JSON },
    visits_dow: { category: CAT_JSON },
    unique_visitors_hod: { category: CAT_JSON },
    unique_visitors_dow: { category: CAT_JSON },
    unique_visitors_single_visit: { category: CAT_NUMERIC },
    unique_visitors_multi_visit: { category: CAT_NUMERIC },
    unique_xdevice: { category: CAT_NUMERIC },
    unique_hh: { category: CAT_NUMERIC },
    repeat_visitors_hh: { category: CAT_NUMERIC },
    outlier: { category: CAT_BOOL },
  },
  aoi: {
    aoi_type: { category: CAT_STRING },
    aoi_id: { category: CAT_STRING },
    aoi_category: { category: CAT_STRING },
    inflator: { category: CAT_NUMERIC },
  },
}

const getReportLayers = (wl, cu, { layerID, reportID }) => {
  const whereFilters = { 'report.type': 1 } // wi
  if (reportID) {
    whereFilters['report_wi.report_id'] = reportID
  }
  if (layerID) {
    whereFilters.layer_id = layerID
  }
  const layerQuery = knex('layer')
  layerQuery.column(['layer.name', 'layer.layer_id', 'layer.report_id', 'report.type'])
  layerQuery.select(knex.raw(`
    COALESCE(
      ARRAY_AGG(DISTINCT ARRAY[
        report_wi.start_date::varchar,
        report_wi.end_date::varchar,
        report_wi.date_type::varchar
      ])
      FILTER (WHERE report_wi.start_date IS NOT null),
      '{}'
    ) AS dates
  `))
  layerQuery.leftJoin('customers', 'layer.customer', 'customers.customerid')
  layerQuery.leftJoin('report', 'layer.report_id', 'report.report_id')
  layerQuery.innerJoin('report_wi', 'report.report_id', 'report_wi.report_id')
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
  const whereFilters = ['wi_aoi.report_id = ?', 'l.layer_id = ?']
  const whereValues = [reportID, layerID]
  if (wl !== -1) {
    whereFilters.push('l.whitelabel = ANY (?)')
    whereValues.push(wl)
  }
  const [{ exists } = {}] = await knexWithCache(
    knex.raw(`
      SELECT EXISTS (SELECT
        l.layer_id,
        wi_aoi.aoi_id
      FROM layer l
        JOIN report_wi_aoi wi_aoi ON wi_aoi.report_id = l.report_id
      WHERE ${whereFilters.join(' AND ')}
      )
    `, whereValues),
    { ttl: 600 }, // 10 minutes
  )
  return exists
}

const listViews = async ({ access, filter = {}, inclMeta = true }) => {
  const { whitelabel, customers } = access
  if (whitelabel !== -1 && (!whitelabel.length || (customers !== -1 && !customers.length))) {
    throw apiError('Invalid access permissions', 403)
  }
  const reportLayers = await getReportLayers(whitelabel, customers, filter)
  return Promise.all(reportLayers.map(async ({ name, layer_id, report_id, type, dates }) => {
    const view = {
      name,
      view: {
        type: 'reportwi', // TODO: dash error on name type
        id: `reportwi_${layer_id}_${report_id}`,
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
  const [, layerIDStr, reportIDStr] = viewID.match(/^reportwi_(\d+|\w+)_(\d+)$/) || []
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
    const { name, type, dates } = reportLayer
    const hasAOI = await hasAOIData(whitelabel, layer_id, reportID)
    // const columns = hasAOI ? { ...options.columns, ...options.aoi } : options.columns
    const { columns } = options
    Object.entries(columns).forEach(([key, column]) => { column.key = key })
    return {
      name,
      view: {
        type: 'reportwi',
        id: `reportwi_${layer_id}_${reportID}`,
        report_id: reportID,
        layer_id,
      },
      columns,
      report_type: type,
      dates: dates.map(([start, end, dateType]) => ({ start, end, dateType: parseInt(dateType) })),
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
  const viewID = `reportwi_${layer_id}_${report_id}`
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
    filter: { layer_id, 'report_wi.report_id': report_id },
  })
  const mlViewColumns = (viewMeta[0] || {}).columns

  const whereFilters = ['r.type = 1', 'r.report_id = ?', 'layer.layer_id = ?']
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

      layer.wi_factor,
      layer.name,

      wi.report_id,
      wi.date_type,
      wi.start_date,
      wi.end_date,
      wi.repeat_type,
      wi.visits * layer.wi_factor as visits,
      wi.unique_visitors * layer.wi_factor as unique_visitors,
      wi.repeat_visits * layer.wi_factor as repeat_visits,
      wi.repeat_visitors * layer.wi_factor as repeat_visitors,
      CASE
        WHEN wi.unique_visitors in (null, 0) THEN 0
        ELSE wi.repeat_visitors / wi.unique_visitors::float
      END as repeat_visitor_rate,
      wi.visits_hod,
      wi.visits_dow,
      wi.unique_visitors_hod,
      wi.unique_visitors_dow,
      wi.unique_visitors_single_visit * layer.wi_factor as unique_visitors_single_visit,
      wi.unique_visitors_multi_visit * layer.wi_factor as unique_visitors_multi_visit,
      wi.unique_xdevice * layer.wi_factor as unique_xdevice,
      wi.unique_hh * layer.wi_factor as unique_hh,
      wi.repeat_visitors_hh * layer.wi_factor as repeat_visitors_hh,
      wi.outlier
    FROM poi
    LEFT JOIN tz_world AS tz ON ST_Contains(
      tz.geom,
      ST_SetSRID(ST_MakePoint(poi.lon, poi.lat), 4326)
    )
    RIGHT JOIN poi_list_map ON poi.poi_id = poi_list_map.poi_id
    RIGHT JOIN layer ON layer.poi_list_id = poi_list_map.poi_list_id
    LEFT JOIN report AS r ON r.report_id = layer.report_id
    INNER JOIN report_wi AS wi ON 
      wi.report_id = r.report_id AND
      wi.poi_id = poi.poi_id
    WHERE ${whereFilters.join(' AND ')}
  `, whereValues)

  return { viewID, mlView, mlViewColumns }
}

module.exports = {
  listViews,
  getView,
  getQueryView,
}
