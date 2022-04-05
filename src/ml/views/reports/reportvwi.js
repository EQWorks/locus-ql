const {
  CAT_STRING,
  CAT_NUMERIC,
  CAT_DATE,
  CAT_JSON,
  CAT_BOOL,
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
  beacons: {
    expression: `
      LEFT JOIN public.beacons AS b ON
        b.camps = wi.campaign
        AND b.vendors = wi.vendor
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
  vwi_factor: {
    category: CAT_NUMERIC,
    expression: 'layer.vwi_factor',
  },
  name: {
    category: CAT_STRING,
    expression: 'layer.name',
  },
  report_id: {
    category: CAT_NUMERIC,
    expression: 'wi.report_id',
  },
  beacon_id: {
    category: CAT_NUMERIC,
    expression: 'b.id',
    join: queryJoins.beacons,
  },
  beacon_name: {
    category: CAT_STRING,
    expression: 'b.name',
    join: queryJoins.beacons,
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
  outlier: {
    category: CAT_BOOL,
    expression: 'wi.outlier',
  },
  beacon: {
    category: CAT_NUMERIC,
    expression: 'wi.beacon',
  },
  beacon_unique_user: {
    category: CAT_NUMERIC,
    expression: 'wi.beacon_unique_user',
  },
  beacon_xdevice: {
    category: CAT_NUMERIC,
    expression: 'wi.beacon_xdevice',
  },
  beacon_unique_hh: {
    category: CAT_NUMERIC,
    expression: 'wi.beacon_unique_hh',
  },
  converted_beacon: {
    category: CAT_NUMERIC,
    expression: 'wi.converted_beacon * layer.vwi_factor',
  },
  converted_beacon_unique_user: {
    category: CAT_NUMERIC,
    expression: 'wi.converted_beacon_unique_user * layer.vwi_factor',
  },
  beacon_conversion_rate: {
    category: CAT_NUMERIC,
    expression: `
      CASE
        WHEN wi.beacon_unique_user in (null, 0) THEN 0
        ELSE wi.converted_beacon_unique_user / CAST(wi.beacon_unique_user AS double precision)
      END
    `,
  },
  converted_beacon_xdevice: {
    category: CAT_NUMERIC,
    expression: 'wi.converted_beacon_xdevice * layer.vwi_factor',
  },
  converted_beacon_unique_hh: {
    category: CAT_NUMERIC,
    expression: 'wi.converted_beacon_unique_hh * layer.vwi_factor',
  },
  converted_visits: {
    category: CAT_NUMERIC,
    expression: 'wi.converted_visits * layer.vwi_factor',
  },
  converted_unique_visitors: {
    category: CAT_NUMERIC,
    expression: 'wi.converted_unique_visitors * layer.vwi_factor',
  },
  converted_repeat_visits: {
    category: CAT_NUMERIC,
    expression: 'wi.converted_repeat_visits * layer.vwi_factor',
  },
  converted_repeat_visitors: {
    category: CAT_NUMERIC,
    expression: 'wi.converted_repeat_visitors * layer.vwi_factor',
  },
  repeat_conversion_rate: {
    category: CAT_NUMERIC,
    expression: `
      CASE
        WHEN wi.converted_unique_visitors in (null, 0) THEN 0
        ELSE wi.converted_repeat_visitors / CAST(wi.converted_unique_visitors AS double precision)
      END
    `,
  },
  converted_visits_hod: {
    category: CAT_JSON,
    expression: 'wi.converted_visits_hod',
  },
  converted_visits_dow: {
    category: CAT_JSON,
    expression: 'wi.converted_visits_dow',
  },
  converted_unique_visitors_hod: {
    category: CAT_JSON,
    expression: 'wi.converted_unique_visitors_hod',
  },
  converted_unique_visitors_dow: {
    category: CAT_JSON,
    expression: 'wi.converted_unique_visitors_dow',
  },
  timeto_conversion: {
    category: CAT_NUMERIC,
    expression: 'wi.timeto_conversion',
  },
  converted_unique_visitors_single_visit: {
    category: CAT_NUMERIC,
    expression: 'wi.converted_unique_visitors_single_visit * layer.vwi_factor',
  },
  converted_unique_visitors_multi_visit: {
    category: CAT_NUMERIC,
    expression: 'wi.converted_unique_visitors_multi_visit * layer.vwi_factor',
  },
  converted_unique_xdevice: {
    category: CAT_NUMERIC,
    expression: 'wi.converted_unique_xdevice * layer.vwi_factor',
  },
  converted_unique_hh: {
    category: CAT_NUMERIC,
    expression: 'wi.converted_unique_hh * layer.vwi_factor',
  },
  vendor: {
    category: CAT_STRING,
    expression: "NULLIF(wi.vendor, '')",
  },
  campaign: {
    category: CAT_NUMERIC,
    expression: "CAST(NULLIF(wi.campaign,'') AS int)",
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
