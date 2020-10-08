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

const listViews = async (access, filter = {}) => {
  const { whitelabel, customers } = access
  const reportLayerTypes = [16, 17] // xwi and xvwi

  const layerQuery = knex('layer')
  layerQuery.column(['name', 'layer_id', 'layer.report_id', 'layer_type_id'])
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
  layerQuery.leftJoin('report_wi', 'report_wi.report_id', 'layer.report_id')
  layerQuery.leftJoin('report_xwi', 'report_xwi.report_id', 'layer.report_id')
  layerQuery.where(filter)
  layerQuery.whereIn('layer_type_id', reportLayerTypes)
  layerQuery.whereNull('parent_layer')
  if (whitelabel !== -1) {
    layerQuery.where({ whitelabel: whitelabel[0] })
    if (customers !== -1) {
      layerQuery.where({ agencyid: customers[0] })
    }
  }
  layerQuery.groupBy(['name', 'layer_id', 'layer.report_id', 'layer_type_id'])

  const reportLayers = await layerQuery
  return reportLayers.map(({ name, layer_id, report_id, layer_type_id, dates }) => {
    // TODO: remove 'columns' -> use listView() to get full view
    Object.entries(options.columns).forEach(([key, column]) => { column.key = key })
    return {
      name,
      view: {
        type: 'reportxwi',
        id: `reportxwi_${layer_id}_${report_id}`,
        report_id,
        layer_id,
      },
      // TODO: remove 'columns' and meta fields -> use listView() to get full view
      columns: options.columns,
      layer_type_id,
      dates: dates.map(([start, end, dateType]) => ({ start, end, dateType: parseInt(dateType) })),
    }
  })
}

const listView = async (access, viewID) => {
  const { whitelabel, customers } = access
  const reportLayerTypes = [16, 17] // xwi and xvwi
  const [, layerIDStr, reportIDStr] = viewID.match(/^reportxwi_(\d+|\w+)_(\d+)$/) || []
  let layerIDs = []

  // eslint-disable-next-line radix
  const reportID = parseInt(reportIDStr, 10)
  if (!reportID) {
    throw apiError(`Invalid view: ${viewID}`, 403)
  }
  // optional layer_id param
  if (layerIDStr === 'any') {
    const layerIDQuery = knex('layer')
    layerIDQuery.column('layer_id')
    layerIDQuery.where({ report_id: reportID })
    layerIDQuery.whereNull('parent_layer')
    if (whitelabel !== -1) {
      layerIDQuery.where({ whitelabel: whitelabel[0] })
      if (customers !== -1) {
        layerIDQuery.where({ agencyid: customers[0] })
      }
    }

    layerIDs = await layerIDQuery
  } else {
    // eslint-disable-next-line radix
    layerIDs = [{ layer_id: parseInt(layerIDStr, 10) }]
  }

  const viewLayers = await Promise.all(layerIDs.map(async ({ layer_id }) => {
    const layerQuery = knex('layer')
    layerQuery.column(['name', 'layer_type_id'])
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
    layerQuery.leftJoin('report_wi', 'report_wi.report_id', 'layer.report_id')
    layerQuery.leftJoin('report_xwi', 'report_xwi.report_id', 'layer.report_id')
    layerQuery.where({ layer_id, 'layer.report_id': reportID })
    layerQuery.whereIn('layer_type_id', reportLayerTypes)
    layerQuery.whereNull('parent_layer')
    if (whitelabel !== -1) {
      layerQuery.where({ whitelabel: whitelabel[0] })
      if (customers !== -1) {
        layerQuery.where({ agencyid: customers[0] })
      }
    }
    layerQuery.groupBy(['name', 'layer_type_id'])

    const [reportLayer] = await layerQuery
    if (!reportLayer) {
      return null
    }
    const { name, layer_type_id, dates } = reportLayer
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
      layer_type_id,
      dates: dates.map(([start, end, dateType]) => ({ start, end, dateType: parseInt(dateType) })),
    }
  }))
  if (viewLayers.filter(v => v).length === 0) {
    throw apiError('Access to layer(s) not allowed.', 403)
  }
  return viewLayers.filter(v => v)
}

const getView = async (access, reqViews, reqViewColumns, { layer_id, report_id }) => {
  const { whitelabel, customers } = access
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
      wi.outlier,

      xwi.target_poi_list_id,
      xwi.target_poi_id,
      xwi.xvisit_visits * layer.wi_factor as xvisit_visits,
      xwi.xvisit_unique_visitors * layer.wi_factor as xvisit_unique_visitors,
      xwi.xvisit_repeat_visits * layer.wi_factor as xvisit_repeat_visits,
      xwi.xvisit_repeat_visitors * layer.wi_factor as xvisit_repeate_visitors,
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
    LEFT JOIN report_wi AS wi ON wi.poi_id = poi.poi_id
    LEFT JOIN report_xwi as xwi ON
      wi.report_id = xwi.report_id AND
      wi.date_type = xwi.date_type AND
      wi.start_date = xwi.start_date AND
      wi.end_date = xwi.end_date AND
      wi.repeat_type = xwi.repeat_type
    WHERE wi.report_id = ?
    AND layer.layer_id = ?
    ) as ${viewID}
  `, [report_id, layer_id])
}

module.exports = {
  listViews,
  listView,
  getView,
}
