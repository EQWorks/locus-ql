/* eslint-disable no-use-before-define */

const { knex } = require('../../util/db')
const { listLayers } = require('../../routes/layer/interface')
const {
  CAT_STRING,
  CAT_NUMERIC,
  CAT_DATE,
  CAT_JSON,
  CAT_BOOL,
} = require('../type')
const apiError = require('../../util/api-error')


const getView = async (access, reqViews, reqViewColumns, { layer_id, report_id }) => {
  const viewID = `report_vwi_${layer_id}_${report_id}`

  const [layer] = await listLayers(
    access.whitelabel,
    access.customers,
    { layer_id, report_id },
  )

  if (!layer) {
    throw apiError('Access to layer or report not allowed', 403)
  }

  // inject view columns
  const viewMeta = await listViews(access, { layer_id, 'layer.report_id': report_id })
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
      layer.vwi_factor,

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
      CASE
        WHEN wi.unique_visitors in (null, 0) THEN 0
        ELSE vwi.converted_unique_visitors / wi.unique_visitors::float
      END as visitor_conversion_rate,
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
      vwi.vendor,
      vwi.campaign
    FROM poi
    LEFT JOIN tz_world AS tz ON ST_Contains(
      tz.geom,
      ST_SetSRID(ST_MakePoint(poi.lon, poi.lat), 4326)
    )
    RIGHT JOIN poi_list_map ON poi.poi_id = poi_list_map.poi_id
    RIGHT JOIN LAYER ON layer.poi_list_id = poi_list_map.poi_list_id
    LEFT JOIN report_wi AS wi ON wi.poi_id = poi.poi_id
    LEFT JOIN report_vwi as vwi ON
      wi.report_id = vwi.report_id AND
      wi.poi_id = vwi.poi_id AND
      wi.date_type = vwi.date_type AND
      wi.start_date = vwi.start_date AND
      wi.end_date = vwi.end_date AND
      wi.repeat_type = vwi.repeat_type
    WHERE wi.report_id = ?
      AND layer.layer_id = ?
    ) as ${viewID}
  `, [report_id, layer_id])

  // TODO: missed applying factor to json columns, need to do on client side
}

const listViews = async (access, filter = {}) => {
  const { whitelabel, customers } = access
  const reportLayerTypes = [2]

  const layerQuery = knex('layer')
  layerQuery.column(['layer.name', 'layer_id', 'layer.report_id', 'layer_type_id'])
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
  layerQuery.leftJoin('report_vwi', 'report_vwi.report_id', 'layer.report_id')
  layerQuery.joinRaw('LEFT JOIN camps on camps.camp_id::text = report_vwi.campaign')
  layerQuery.where(filter)
  layerQuery.whereIn('layer_type_id', reportLayerTypes)
  layerQuery.whereNull('parent_layer')
  if (whitelabel !== -1) {
    layerQuery.where({ whitelabel: whitelabel[0] })
    if (customers !== -1) {
      layerQuery.where({ agencyid: customers[0] })
    }
  }
  layerQuery.groupBy(['layer.name', 'layer_id', 'layer.report_id', 'layer_type_id'])

  const reportLayers = await layerQuery

  return reportLayers.map(({ name, layer_id, report_id, layer_type_id, dates, vendors, camps }) => {
    Object.entries(options.columns).forEach(([key, column]) => {
      column.key = key
    })

    return {
      // required
      name,
      view: {
        type: 'report-vwi',
        id: `report_vwi_${layer_id}_${report_id}`,
        report_id,
        layer_id,
      },
      columns: options.columns,
      // meta
      layer_type_id,
      dates: dates.map(([start, end, dateType]) => ({ start, end, dateType: parseInt(dateType) })),
      vendors,
      camps: camps.map(([campID, name]) => ({ campID: parseInt(campID), name })),
    }
  })
}

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
    vwi_factor: { category: CAT_NUMERIC },

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
    visitor_conversion_rate: { category: CAT_NUMERIC },
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
    campaign: { category: CAT_STRING },
  },
}

module.exports = {
  getView,
  listViews,
}
