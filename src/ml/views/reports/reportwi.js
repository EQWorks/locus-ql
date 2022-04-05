const {
  CAT_STRING,
  CAT_NUMERIC,
  CAT_DATE,
  CAT_BOOL,
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
  visits: {
    category: CAT_NUMERIC,
    expression: 'wi.visits * layer.wi_factor',
  },
  unique_visitors: {
    category: CAT_NUMERIC,
    expression: 'wi.unique_visitors * layer.wi_factor',
  },
  repeat_visits: {
    category: CAT_NUMERIC,
    expression: 'wi.repeat_visits * layer.wi_factor',
  },
  repeat_visitors: {
    category: CAT_NUMERIC,
    expression: 'wi.repeat_visitors * layer.wi_factor',
  },
  repeat_visitor_rate: {
    category: CAT_NUMERIC,
    expression: `
    CASE
      WHEN wi.unique_visitors in (null, 0) THEN 0
      ELSE wi.repeat_visitors / CAST(wi.unique_visitors AS double precision)
    END
    `,
  },
  visits_hod: {
    category: CAT_JSON,
    expression: 'wi.visits_hod',
  },
  visits_dow: {
    category: CAT_JSON,
    expression: 'wi.visits_dow',
  },
  unique_visitors_hod: {
    category: CAT_JSON,
    expression: 'wi.unique_visitors_hod',
  },
  unique_visitors_dow: {
    category: CAT_JSON,
    expression: 'wi.unique_visitors_dow',
  },
  unique_visitors_single_visit: {
    category: CAT_NUMERIC,
    expression: 'wi.unique_visitors_single_visit * layer.wi_factor',
  },
  unique_visitors_multi_visit: {
    category: CAT_NUMERIC,
    expression: 'wi.unique_visitors_multi_visit * layer.wi_factor',
  },
  unique_xdevice: {
    category: CAT_NUMERIC,
    expression: 'wi.unique_xdevice * layer.wi_factor',
  },
  unique_hh: {
    category: CAT_NUMERIC,
    expression: 'wi.unique_hh * layer.wi_factor',
  },
  repeat_visitors_hh: {
    category: CAT_NUMERIC,
    expression: 'wi.repeat_visitors_hh * layer.wi_factor',
  },
  outlier: {
    category: CAT_BOOL,
    expression: 'wi.outlier',
  },
}
Object.entries(columns).forEach(([key, column]) => { column.key = key })

const aoiColumns = {
  aoi_type: { category: CAT_STRING, expression: 'wi.aoi_type' },
  aoi_id: { category: CAT_STRING, expression: 'wi.aoi_id' },
  aoi_category: { category: CAT_STRING, expression: 'wi.aoi_category' },
  inflator: { category: CAT_NUMERIC, expression: 'wi.inflator' },
}
Object.entries(aoiColumns).forEach(([key, column]) => { column.key = key })

module.exports = {
  columns,
  aoiColumns,
}
