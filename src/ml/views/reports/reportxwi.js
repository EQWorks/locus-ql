const {
  CAT_STRING,
  CAT_NUMERIC,
  CAT_DATE,
  CAT_JSON,
} = require('../../type')
const { geometryTypes } = require('../../parser/src')


// optional joins
const queryJoins = {
  tz: {
    pg: `
      LEFT JOIN public.tz_world AS tz ON ST_Contains(
        tz.geom,
        ST_SetSRID(ST_MakePoint(poi.lon, poi.lat), 4326)
      )
    `,
    trino: `
      LEFT JOIN public.tz_world AS tz ON ST_Contains(
        tz.geom,
        ST_Point(poi.lon, poi.lat)
      )
    `,
  },
}

const columns = {
  time_zone: {
    category: CAT_STRING,
    expression: "coalesce(tz.tzid, 'UTC')",
    join: queryJoins.tz,
  },
  poi_id: {
    category: CAT_NUMERIC,
    geo_type: geometryTypes.POI,
    expression: 'poi.poi_id',
  },
  poi_name: {
    category: CAT_STRING,
    expression: 'poi.name',
  },
  chain_id: {
    category: CAT_NUMERIC,
    expression: 'poi.chain_id',
  },
  type: {
    category: CAT_NUMERIC,
    expression: 'poi.type',
  },
  category: {
    category: CAT_NUMERIC,
    expression: 'poi.category',
  },
  lat: {
    category: CAT_NUMERIC,
    expression: 'poi.lat',
  },
  lon: {
    category: CAT_NUMERIC,
    expression: 'poi.lon',
  },
  address_label: {
    category: CAT_STRING,
    expression: 'poi.address_label',
  },
  address_line1: {
    category: CAT_STRING,
    expression: 'poi.address_line1',
  },
  address_line2: {
    category: CAT_STRING,
    expression: 'poi.address_line2',
  },
  address_unit: {
    category: CAT_STRING,
    expression: 'poi.address_unit',
  },
  address_city: {
    category: CAT_STRING,
    expression: 'poi.address_city',
  },
  address_region: {
    category: CAT_STRING,
    expression: 'poi.address_region',
  },
  address_postalcode: {
    category: CAT_STRING,
    expression: 'poi.address_postalcode',
  },
  address_country: {
    category: CAT_STRING,
    expression: 'poi.address_country',
  },
  geo_ca_fsa: {
    category: CAT_STRING,
    geo_type: geometryTypes.CA_FSA,
    pg: "upper(substring(poi.address_postalcode from '^[A-Z]\\d[A-Z]'))",
    trino: "upper(regexp_extract(poi.address_postalcode, '^[A-Z]\\d[A-Z]'))",
  },
  geo_us_postalcode: {
    category: CAT_STRING,
    // geo_type: 'us-postalcode',
    pg: "substring(poi.address_postalcode from '^\\d{5}$')",
    trino: "regexp_extract(poi.address_postalcode, '^\\d{5}$')",
  },
  wi_factor: {
    category: CAT_NUMERIC,
    expression: 'layer.wi_factor',
  },
  name: {
    category: CAT_STRING,
    expression: 'layer.name',
  },
  report_id: {
    category: CAT_NUMERIC,
    expression: 'wi.report_id',
  },
  date_type: {
    category: CAT_NUMERIC,
    expression: 'wi.date_type',
  },
  start_date: {
    category: CAT_DATE,
    expression: 'wi.start_date',
  },
  end_date: {
    category: CAT_DATE,
    expression: 'wi.end_date',
  },
  repeat_type: {
    category: CAT_NUMERIC,
    expression: 'wi.repeat_type',
  },
  target_poi_list_id: {
    category: CAT_NUMERIC,
    expression: 'wi.target_poi_list_id',
  },
  target_poi_id: {
    category: CAT_NUMERIC,
    geo_type: geometryTypes.POI,
    expression: 'wi.target_poi_id',
  },
  xvisit_visits: {
    category: CAT_NUMERIC,
    expression: 'wi.xvisit_visits * layer.wi_factor',
  },
  xvisit_unique_visitors: {
    category: CAT_NUMERIC,
    expression: 'wi.xvisit_unique_visitors * layer.wi_factor',
  },
  xvisit_repeat_visits: {
    category: CAT_NUMERIC,
    expression: 'wi.xvisit_repeat_visits * layer.wi_factor',
  },
  xvisit_repeat_visitors: {
    category: CAT_NUMERIC,
    expression: 'wi.xvisit_repeat_visitors * layer.wi_factor',
  },
  xvisit_visits_hod: {
    category: CAT_JSON,
    expression: 'wi.xvisit_visits_hod',
  },
  xvisit_visits_dow: {
    category: CAT_JSON,
    expression: 'wi.xvisit_visits_dow',
  },
  xvisit_unique_visitors_hod: {
    category: CAT_JSON,
    expression: 'wi.xvisit_unique_visitors_hod',
  },
  xvisit_unique_visitors_dow: {
    category: CAT_JSON,
    expression: 'wi.xvisit_unique_visitors_dow',
  },
  timeto_xvisit: {
    category: CAT_NUMERIC,
    expression: 'wi.timeto_xvisit',
  },
  xvisit_unique_visitors_single_visit: {
    category: CAT_NUMERIC,
    expression: 'wi.xvisit_unique_visitors_single_visit * layer.wi_factor',
  },
  xvisit_unique_visitors_multi_visit: {
    category: CAT_NUMERIC,
    expression: 'wi.xvisit_unique_visitors_multi_visit * layer.wi_factor',
  },
  xvisit_unique_xdevice: {
    category: CAT_NUMERIC,
    expression: 'wi.xvisit_unique_xdevice * layer.wi_factor',
  },
  xvisit_unique_hh: {
    category: CAT_NUMERIC,
    expression: 'wi.xvisit_unique_hh * layer.wi_factor',
  },
}
Object.entries(columns).forEach(([key, column]) => { column.key = key })

module.exports = { columns }
