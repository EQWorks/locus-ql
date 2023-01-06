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
      LEFT JOIN locus_place.public.tz_world AS tz ON ST_Contains(
        tz.geom,
        ST_Point(poi.lon, poi.lat)
      )
    `,
  },
  targetPOI: {
    pg: 'INNER JOIN public.poi AS tpoi ON tpoi.poi_id = wi.target_poi_id',
    trino: 'INNER JOIN locus_place.public.poi AS tpoi ON tpoi.poi_id = wi.target_poi_id',
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
  target_poi_name: {
    category: CAT_STRING,
    expression: 'tpoi.name',
    join: queryJoins.targetPOI,
  },
  target_poi_chain_id: {
    category: CAT_NUMERIC,
    expression: 'tpoi.chain_id',
    join: queryJoins.targetPOI,
  },
  target_poi_type: {
    category: CAT_NUMERIC,
    expression: 'tpoi.type',
    join: queryJoins.targetPOI,
  },
  target_poi_category: {
    category: CAT_NUMERIC,
    expression: 'tpoi.category',
    join: queryJoins.targetPOI,
  },
  target_poi_lat: {
    category: CAT_NUMERIC,
    expression: 'tpoi.lat',
    join: queryJoins.targetPOI,
  },
  target_poi_lon: {
    category: CAT_NUMERIC,
    expression: 'tpoi.lon',
    join: queryJoins.targetPOI,
  },
  target_poi_address_label: {
    category: CAT_STRING,
    expression: 'tpoi.address_label',
    join: queryJoins.targetPOI,
  },
  target_poi_address_line1: {
    category: CAT_STRING,
    expression: 'tpoi.address_line1',
    join: queryJoins.targetPOI,
  },
  target_poi_address_line2: {
    category: CAT_STRING,
    expression: 'tpoi.address_line2',
    join: queryJoins.targetPOI,
  },
  target_poi_address_unit: {
    category: CAT_STRING,
    expression: 'tpoi.address_unit',
    join: queryJoins.targetPOI,
  },
  target_poi_address_city: {
    category: CAT_STRING,
    expression: 'tpoi.address_city',
    join: queryJoins.targetPOI,
  },
  target_poi_address_region: {
    category: CAT_STRING,
    expression: 'tpoi.address_region',
    join: queryJoins.targetPOI,
  },
  target_poi_address_postalcode: {
    category: CAT_STRING,
    expression: 'tpoi.address_postalcode',
    join: queryJoins.targetPOI,
  },
  target_poi_address_country: {
    category: CAT_STRING,
    expression: 'tpoi.address_country',
    join: queryJoins.targetPOI,
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
    pg: 'wi.xvisit_visits_hod::jsonb',
    trino: 'wi.xvisit_visits_hod',
  },
  xvisit_visits_dow: {
    category: CAT_JSON,
    pg: 'wi.xvisit_visits_dow::jsonb',
    trino: 'wi.xvisit_visits_dow',
  },
  xvisit_unique_visitors_hod: {
    category: CAT_JSON,
    pg: 'wi.xvisit_unique_visitors_hod::jsonb',
    trino: 'wi.xvisit_unique_visitors_hod',
  },
  xvisit_unique_visitors_dow: {
    category: CAT_JSON,
    pg: 'wi.xvisit_unique_visitors_dow::jsonb',
    trino: 'wi.xvisit_unique_visitors_dow',
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
