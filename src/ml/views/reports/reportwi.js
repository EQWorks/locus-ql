const { knex } = require('../../../util/db')
const { listLayers } = require('../../../routes/layer/interface')
const {
  CAT_STRING,
  CAT_NUMERIC,
  CAT_DATE,
  CAT_JSON,
  CAT_BOOL,
} = require('../../type')
const apiError = require('../../../util/api-error')


const options = {
  columns: {
    time_zone: { category: CAT_STRING },
    poi_id: { category: CAT_NUMERIC },
    name: { category: CAT_STRING },
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
}

const getReportLayers = (wl, cu, filter) => {
  const whereFilters = {
    ...filter,
    'report.type': 1, // wi
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
  layerQuery.whereNull('parent_layer')
  if (wl !== -1) {
    layerQuery.where({ whitelabel: wl[0] })
    if (cu !== -1) {
      layerQuery.where({ agencyid: cu[0] })
    }
  }
  layerQuery.groupBy(['layer.name', 'layer.layer_id', 'layer.report_id', 'report.type'])
  return layerQuery
}

const getLayerIDs = (wl, cu, reportID) => {
  const layerIDQuery = knex('layer')
  layerIDQuery.column('layer_id')
  layerIDQuery.where({ report_id: reportID })
  layerIDQuery.whereNull('parent_layer')
  if (wl !== -1) {
    layerIDQuery.where({ whitelabel: wl[0] })
    if (cu !== -1) {
      layerIDQuery.where({ agencyid: cu[0] })
    }
  }
  return layerIDQuery
}

const listViews = async (access, filter = {}) => {
  const { whitelabel, customers } = access
  const reportLayers = await getReportLayers(whitelabel, customers, filter)
  return reportLayers.map(({ name, layer_id, report_id, type, dates }) => {
    Object.entries(options.columns).forEach(([key, column]) => { column.key = key })
    return {
      name,
      view: {
        type: 'reportwi', // TODO: dash error on name type
        id: `reportwi_${layer_id}_${report_id}`,
        report_id,
        layer_id,
      },
      // TODO: remove 'columns' and meta fields -> use listView() to get full view
      columns: options.columns,
      report_type: type,
      dates: dates.map(([start, end, dateType]) => ({ start, end, dateType: parseInt(dateType) })),
    }
  })
}

const listView = async (access, viewID) => {
  const { whitelabel, customers } = access
  const [, layerIDStr, reportIDStr] = viewID.match(/^reportwi_(\d+|\w+)_(\d+)$/) || []
  let layerIDs = []

  // eslint-disable-next-line radix
  const reportID = parseInt(reportIDStr, 10)
  if (!reportID) {
    throw apiError(`Invalid view: ${viewID}`, 403)
  }
  // optional layer_id param
  if (layerIDStr === 'any') {
    layerIDs = await getLayerIDs(whitelabel, customers, reportID)
  } else {
    // eslint-disable-next-line radix
    layerIDs = [{ layer_id: parseInt(layerIDStr, 10) }]
  }

  const viewLayers = await Promise.all(layerIDs.map(async ({ layer_id }) => {
    const [reportLayer] = await getReportLayers(whitelabel, customers)
    if (!reportLayer) {
      return null
    }
    const { name, type, dates } = reportLayer
    Object.entries(options.columns).forEach(([key, column]) => { column.key = key })
    return {
      name,
      view: {
        type: 'reportwi',
        id: `reportwi_${layer_id}_${reportID}`,
        report_id: reportID,
        layer_id,
      },
      columns: options.columns,
      report_type: type,
      dates: dates.map(([start, end, dateType]) => ({ start, end, dateType: parseInt(dateType) })),
    }
  }))
  if (viewLayers.filter(v => v).length === 0) {
    throw apiError('Access to layer(s) not allowed', 403)
  }
  return viewLayers.filter(v => v)
}

const getView = async (access, reqViews, reqViewColumns, { layer_id, report_id }) => {
  const { whitelabel, customers } = access
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
  const viewMeta = await listViews(access, { layer_id, 'report_wi.report_id': report_id })
  reqViewColumns[viewID] = (viewMeta[0] || {}).columns

  // inject view
  reqViews[viewID] = knex.raw(`
    (SELECT coalesce(tz.tzid, 'UTC'::TEXT) AS time_zone,
      poi.poi_id,
      poi.name,
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

      report.report_id,
      report.date_type,
      report.start_date,
      report.end_date,
      report.repeat_type,
      report.visits * layer.wi_factor as visits,
      report.unique_visitors * layer.wi_factor as unique_visitors,
      report.repeat_visits * layer.wi_factor as repeat_visits,
      report.repeat_visitors * layer.wi_factor as repeat_visitors,
      CASE
        WHEN report.unique_visitors in (null, 0) THEN 0
        ELSE report.repeat_visitors / report.unique_visitors::float
      END as repeat_visitor_rate,
      report.visits_hod,
      report.visits_dow,
      report.unique_visitors_hod,
      report.unique_visitors_dow,
      report.unique_visitors_single_visit * layer.wi_factor as unique_visitors_single_visit,
      report.unique_visitors_multi_visit * layer.wi_factor as unique_visitors_multi_visit,
      report.unique_xdevice * layer.wi_factor as unique_xdevice,
      report.unique_hh * layer.wi_factor as unique_hh,
      report.repeat_visitors_hh * layer.wi_factor as repeat_visitors_hh,
      report.outlier
    FROM poi
    LEFT JOIN tz_world AS tz ON ST_Contains(
      tz.geom,
      ST_SetSRID(ST_MakePoint(poi.lon, poi.lat), 4326)
    )
    RIGHT JOIN poi_list_map ON poi.poi_id = poi_list_map.poi_id
    RIGHT JOIN layer ON layer.poi_list_id = poi_list_map.poi_list_id
    LEFT JOIN report AS r ON r.report_id = layer.report_id
    INNER JOIN report_wi AS report ON 
      report.report_id = r.report_id AND
      report.poi_id = poi.poi_id
    WHERE r.type = 1
      AND r.report_id = ?
      AND layer.layer_id = ?
    ) as ${viewID}
  `, [report_id, layer_id])
}

module.exports = {
  listViews,
  listView,
  getView,
}
