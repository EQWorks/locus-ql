const { knex } = require('../../../../util/db')
const { PG_CACHE_DAYS } = require('../constants')


const getCampView = ({ agencyID }) => ({
  view: knex.raw(`
    (
      SELECT
        camp_id AS camp_code,
        name AS camp_name
      FROM public.camps
      ${agencyID ? 'WHERE advertiser_id = :agencyID' : ''}
    ) AS locus_camps
  `, { agencyID }),
})

const getBeaconView = ({ agencyID }) => ({
  view: knex.raw(`
    (
      SELECT
        id AS beacon_id,
        name AS beacon_name,
        camps AS camp_code,
        vendors AS vendor,
        type
      FROM public.beacons
      ${agencyID ? 'WHERE advertiserid = :agencyID' : ''}
    ) AS locus_beacons
  `, { agencyID }),
})

const getBeaconHistoryView = ({ agencyID }) => ({
  view: knex.raw(`
    (
      SELECT
        bh.camp AS camp_code,
        timezone(
          c.timezone,
          timezone('UTC', bh.date + bh.hour * INTERVAL '1 hour')
        )::timestamptz AS time_tz,
        bh.vendor,
        SUM(COALESCE(bh.impressions, 0)) AS impressions
      FROM public.beaconhistory AS bh
      JOIN public.customers AS c ON c.customerid = bh.advertiserid
      WHERE
        bh.date >= (NOW() - ${PG_CACHE_DAYS} * INTERVAL '1 day')::date
        ${agencyID ? 'AND bh.advertiserid = :agencyID' : ''}
      GROUP BY 1, 2, 3
    ) AS locus_beacon_history
  `, { agencyID }),
})

const getSegmentView = ({ agencyID }) => ({
  view: knex.raw(`
    (
      SELECT
        s.id AS user_segment_id,
        COALESCE(sn.name, s.name, s.id::text) AS user_segment_name,
        s.unique_user AS user_segment_size
      FROM public.segment AS s
      LEFT JOIN public.segment_name AS sn ON sn.segment_id = s.id
      ${agencyID ? 'WHERE s.customer = :agencyID' : ''}
    ) AS locus_segments
  `, { agencyID }),
})

const getPOIView = ({ agencyID }) => ({
  view: knex.raw(`
    (
      SELECT
        p.poi_id AS locus_poi_id,
        p.name AS locus_poi_name,
        p.lat AS locus_poi_lat,
        p.lon AS locus_poi_lon,
        p.address_line1 AS locus_poi_address_line1,
        p.address_line2 AS locus_poi_address_line2,
        p.address_unit AS locus_poi_address_unit,
        p.address_city AS locus_poi_address_city,
        p.address_region AS locus_poi_address_region,
        p.address_postalcode AS locus_poi_address_postalcode,
        p.address_country AS locus_poi_address_country,
        pc.name AS locus_poi_category_name
      FROM public.poi AS p
      LEFT JOIN public.poi_category AS pc ON pc.poi_category_id = p.category
      WHERE
        p.public
        ${agencyID ? 'OR p.customerid = :agencyID' : ''}
    ) AS locus_poi
  `, { agencyID }),
})

const getPOIListView = ({ agencyID }) => ({
  view: knex.raw(`
    (
      SELECT
        poi_list_id AS locus_poi_list_id,
        name AS locus_poi_list_name
      FROM public.poi_list
      WHERE
        public
        ${agencyID ? 'OR customerid = :agencyID' : ''}
    ) AS locus_poi_lists
  `, { agencyID }),
})


const getGeoCohortView = ({ agencyID }) => ({
  view: knex.raw(`
    (
      SELECT
        id AS geo_cohort_id,
        name AS geo_cohort_name
      FROM public.geo_cohort
      WHERE
        enabled
        ${agencyID ? 'AND (cu = :agencyID OR (wl IS NULL AND cu IS NULL))' : ''}
    ) AS locus_geo_cohorts
  `, { agencyID }),
})

const getGeoCohortItemView = ({ agencyID }) => ({
  view: knex.raw(`
    (
      SELECT
        gcl.geo_cohort_id,
        gcl.code AS _geo_cohort_item
      FROM public.geo_cohort_list gcl
      JOIN public.geo_cohort gc ON gc.id = gcl.geo_cohort_id
      WHERE
        gc.enabled
        ${agencyID ? 'AND (gc.cu = :agencyID OR (gc.wl IS NULL AND gc.cu IS NULL))' : ''}
    ) AS locus_geo_cohort_items
  `, { agencyID }),
})

module.exports = {
  getCampView,
  getBeaconView,
  getBeaconHistoryView,
  getPOIView,
  getPOIListView,
  getSegmentView,
  getGeoCohortView,
  getGeoCohortItemView,
}
