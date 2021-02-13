const { knex } = require('../../../util/db')
const { listLayers } = require('../../../routes/layer/interface')
const {
  CAT_STRING,
  CAT_NUMERIC,
  CAT_DATE,
  CAT_JSON,
} = require('../../type')
const apiError = require('../../../util/api-error')
const { knexWithCache } = require('../../cache')


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

    target_poi_list_id: { category: CAT_NUMERIC },
    target_poi_id: { category: CAT_NUMERIC },
    xvisit_visits: { category: CAT_NUMERIC },
    xvisit_unique_visitors: { category: CAT_NUMERIC },
    xvisit_repeat_visits: { category: CAT_NUMERIC },
    xvisit_repeat_visitors: { category: CAT_NUMERIC },
    xvisit_visits_hod: { category: CAT_JSON },
    xvisit_visits_dow: { category: CAT_JSON },
    xvisit_unique_visitors_hod: { category: CAT_JSON },
    xvisit_unique_visitors_dow: { category: CAT_JSON },
    timeto_xvisit: { category: CAT_NUMERIC },
    xvisit_unique_visitors_single_visit: { category: CAT_NUMERIC },
    xvisit_unique_visitors_multi_visit: { category: CAT_NUMERIC },
    xvisit_unique_xdevice: { category: CAT_NUMERIC },
    xvisit_unique_hh: { category: CAT_NUMERIC },
  },
}

const getReportLayers = (wl, cu, filter) => {
  const whereFilters = {
    ...filter,
    'report.type': 4, // xwi
  }
  const layerQuery = knex('layer')
  layerQuery.column(['layer.name', 'layer.layer_id', 'layer.report_id', 'report.type'])
  layerQuery.select(knex.raw(`
    COALESCE(
      ARRAY_AGG(DISTINCT ARRAY[
        report_xwi.start_date::varchar,
        report_xwi.end_date::varchar,
        report_xwi.date_type::varchar
      ])
      FILTER (WHERE report_xwi.start_date IS NOT null),
      '{}'
    ) AS dates
  `))
  layerQuery.leftJoin('customers', 'layer.customer', 'customers.customerid')
  layerQuery.leftJoin('report', 'layer.report_id', 'report.report_id')
  layerQuery.innerJoin('report_xwi', 'report.report_id', 'report_xwi.report_id')
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

const listViews = async ({ access, filter = {}, inclMeta = true }) => {
  const { whitelabel, customers } = access
  if (whitelabel !== -1 && (!whitelabel.length || (customers !== -1 && !customers.length))) {
    throw apiError('Invalid access permissions', 403)
  }
  const reportLayers = await getReportLayers(whitelabel, customers, filter)
  return reportLayers.map(({ name, layer_id, report_id, type, dates }) => {
    const view = {
      name,
      view: {
        type: 'reportxwi',
        id: `reportxwi_${layer_id}_${report_id}`,
        report_id,
        layer_id,
      },
    }
    if (inclMeta) {
      Object.entries(options.columns).forEach(([key, column]) => { column.key = key })
      Object.assign(view, {
        columns: options.columns,
        report_type: type,
        dates: dates.map(([start, end, dType]) => ({ start, end, dType: parseInt(dType) })),
      })
    }
    return view
  })
}

const getView = async (access, viewID) => {
  const { whitelabel, customers } = access
  if (whitelabel !== -1 && (!whitelabel.length || (customers !== -1 && !customers.length))) {
    throw apiError('Invalid access permissions', 403)
  }
  const [, layerIDStr, reportIDStr] = viewID.match(/^reportxwi_(\d+|\w+)_(\d+)$/) || []
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
    const [reportLayer] = await getReportLayers(
      whitelabel,
      customers,
      { layer_id, 'report_xwi.report_id': reportID },
    )
    if (!reportLayer) {
      return null
    }
    const { name, type, dates } = reportLayer
    Object.entries(options.columns).forEach(([key, column]) => { column.key = key })
    return {
      name,
      view: {
        type: 'reportxwi',
        id: `reportxwi_${layer_id}_${reportID}`,
        report_id: reportID,
        layer_id,
      },
      columns: options.columns,
      report_type: type,
      dates: dates.map(([start, end, dateType]) => ({ start, end, dateType: parseInt(dateType) })),
    }
  }))
  if (viewLayers.filter(v => v).length === 0) {
    throw apiError('Access to layer(s) not allowed.', 403)
  }
  return viewLayers.filter(v => v)
}

const getQueryView = async (access, { layer_id, report_id }) => {
  const { whitelabel, customers } = access
  if (whitelabel !== -1 && (!whitelabel.length || (customers !== -1 && !customers.length))) {
    throw apiError('Invalid access permissions', 403)
  }
  const viewID = `reportxwi_${layer_id}_${report_id}`

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
    filter: { layer_id, 'report_xwi.report_id': report_id },
  })
  const mlViewColumns = (viewMeta[0] || {}).columns

  // inject view
  const mlView = knex.raw(`
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

      xwi.report_id,
      xwi.date_type,
      xwi.start_date,
      xwi.end_date,
      xwi.repeat_type,

      xwi.target_poi_list_id,
      xwi.target_poi_id,
      xwi.xvisit_visits * layer.wi_factor as xvisit_visits,
      xwi.xvisit_unique_visitors * layer.wi_factor as xvisit_unique_visitors,
      xwi.xvisit_repeat_visits * layer.wi_factor as xvisit_repeat_visits,
      xwi.xvisit_repeat_visitors * layer.wi_factor as xvisit_repeat_visitors,
      xwi.xvisit_visits_hod,
      xwi.xvisit_visits_dow,
      xwi.xvisit_unique_visitors_hod,
      xwi.xvisit_unique_visitors_dow,
      xwi.timeto_xvisit,
      xwi.xvisit_unique_visitors_single_visit * layer.wi_factor
        as xvisit_unique_visitors_single_visit,
      xwi.xvisit_unique_visitors_multi_visit * layer.wi_factor
        as xvisit_unique_visitors_multi_visit,
      xwi.xvisit_unique_xdevice * layer.wi_factor as xvisit_unique_xdevice,
      xwi.xvisit_unique_hh * layer.wi_factor as xvisit_unique_hh
    FROM poi
    LEFT JOIN tz_world AS tz ON ST_Contains(
      tz.geom,
      ST_SetSRID(ST_MakePoint(poi.lon, poi.lat), 4326)
    )
    RIGHT JOIN poi_list_map ON poi.poi_id = poi_list_map.poi_id
    RIGHT JOIN LAYER ON layer.poi_list_id = poi_list_map.poi_list_id
    LEFT JOIN report AS r ON r.report_id = layer.report_id
    INNER JOIN report_xwi as xwi ON
      xwi.report_id = r.report_id AND
      xwi.poi_id = poi.poi_id
    WHERE r.type = 4
      AND r.report_id = ?
      AND layer.layer_id = ?
    ) as ${viewID}
  `, [report_id, layer_id])

  return { viewID, mlView, mlViewColumns }
}

module.exports = {
  listViews,
  getView,
  getQueryView,
}
