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
  const viewID = `report_${layer_id}_${report_id}`

  const [layer] = await listLayers(
    access.whitelabel,
    access.customers,
    { layer_id, report_id },
  )

  if (!layer) {
    throw apiError('Access to layer or report not allowed', 403)
  }

  // inject view columns
  const viewMeta = await listViews(access, { layer_id, report_id })
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
    LEFT JOIN report_wi AS report ON report.poi_id = poi.poi_id
    WHERE report.report_id = ?
      AND layer.layer_id = ?
    ) as ${viewID}
  `, [report_id, layer_id])

  // TODO: missed applying factor to json columns, need to do on client side
}

const listViews = async (access, filter = {}) => {
  const { whitelabel, customers } = access
  const reportLayerTypes = [1, 2, 16, 17]

  const layerQuery = knex('layer')
  layerQuery.column(['name', 'layer_id', 'report_id', 'layer_type_id'])
  layerQuery.leftJoin('customers', 'layer.customer', 'customers.customerid')
  layerQuery.where(filter)
  layerQuery.whereIn('layer_type_id', reportLayerTypes)
  layerQuery.whereNull('parent_layer')
  if (whitelabel !== -1) {
    layerQuery.where({ whitelabel: whitelabel[0] })
    if (customers !== -1) {
      layerQuery.where({ agencyid: customers[0] })
    }
  }

  const reportLayers = await layerQuery

  return reportLayers.map(({ name, layer_id, report_id, layer_type_id }) => {
    Object.entries(options.columns).forEach(([key, column]) => {
      column.key = key
    })

    return {
      name,
      layer_type_id,
      view: {
        type: 'report',
        id: `report_${layer_id}_${report_id}`,
        report_id,
        layer_id,
      },
      columns: options.columns,
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

    report_id: { category: CAT_NUMERIC },
    date_type: { category: CAT_NUMERIC },
    start_date: { category: CAT_DATE },
    end_date: { category: CAT_DATE },
    repeat_type: { category: CAT_NUMERIC },
    visits: { category: CAT_NUMERIC },
    unique_visitors: { category: CAT_NUMERIC },
    repeat_visits: { category: CAT_NUMERIC },
    repeat_visitors: { category: CAT_NUMERIC },
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
  filters: [
    'time_zone',
    'poi_id',
    'name',
    'chain_id',
    'type',
    'category',
    'lat',
    'lon',
    'address_label',
    'address_line1',
    'address_line2',
    'address_unit',
    'address_city',
    'address_region',
    'address_postalcode',
    'address_country',

    'report_id',
    'poi_id',
    'date_type',
    'start_date',
    'end_date',
    'repeat_type',
    'visits',
    'unique_visitors',
    'repeat_visits',
    'repeat_visitors',
    'visits_hod',
    'visits_dow',
    'unique_visitors_hod',
    'unique_visitors_dow',
    'unique_visitors_single_visit',
    'unique_visitors_multi_visit',
    'unique_xdevice',
    'unique_hh',
    'repeat_visitors_hh',
    'outlier',
  ],

}

module.exports = {
  getView,
  listViews,
}
