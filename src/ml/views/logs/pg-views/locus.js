const { knex } = require('../../../../util/db')
const { PG_CACHE_DAYS } = require('../constants')


const getCampView = customerID => ({
  view: knex.raw(`
    (
      SELECT
        camp_id AS camp_code,
        name AS camp_name
      FROM public.camps
      ${customerID ? 'WHERE advertiser_id = :customerID' : ''}
    ) AS locus_camps
  `, { customerID }),
})

const getBeaconView = customerID => ({
  view: knex.raw(`
    (
      SELECT
        id AS beacon_id,
        name AS beacon_name,
        camps AS camp_code,
        vendors AS vendor,
        type
      FROM public.beacons
      ${customerID ? 'WHERE advertiserid = :customerID' : ''}
    ) AS locus_beacons
  `, { customerID }),
})

const getBeaconHistoryView = customerID => ({
  view: knex.raw(`
    (
      SELECT
        camp AS camp_code,
        date,
        vendor,
        SUM(COALESCE(impressions, 0)) AS impressions
      FROM public.beaconhistory
      WHERE
        date >= (NOW() - ${PG_CACHE_DAYS} * INTERVAL '1 day')::date
        ${customerID ? 'AND advertiserid = :customerID' : ''}
      GROUP BY 1, 2, 3
    ) AS locus_beacon_history
  `, { customerID }),
})

const getSegmentView = customerID => ({
  view: knex.raw(`
    (
      SELECT
        s.id AS user_segment_id,
        COALESCE(sn.name, s.name, s.id) AS user_segment_name,
        s.unique_user AS user_segment_users
      FROM public.segment AS s
      LEFT JOIN public.segment_name AS sn ON sn.segment_id = s.id
      ${customerID ? 'WHERE s.customer = :customerID' : ''}
    ) AS locus_segments
  `, { customerID }),
})

const getPOIView = customerID => ({
  view: knex.raw(`
    (
      SELECT
        p.poi_id,
        p.name AS poi_name,
        p.lat AS poi_lat,
        p.lon AS poi_lon,
        p.address_line1 AS poi_address_line1,
        p.address_line2 AS poi_address_line2,
        p.address_unit AS poi_address_unit,
        p.address_city AS poi_address_city,
        p.address_region AS poi_address_region,
        p.address_postalcode AS poi_address_postalcode,
        p.address_country AS poi_address_country,
        pc.name AS poi_category_name
      FROM public.poi AS p
      LEFT JOIN public.poi_category AS pc ON pc.poi_category_id = p.category
      WHERE
        p.public
        ${customerID ? 'OR p.customerid = :customerID' : ''}
    ) AS locus_poi
  `, { customerID }),
})

const getPOIListView = customerID => ({
  view: knex.raw(`
    (
      SELECT
        poi_list_id,
        name AS poi_list_name
      FROM public.poi_list
      WHERE
        public
        ${customerID ? 'OR customerid = :customerID' : ''}
    ) AS locus_poi_lists
  `, { customerID }),
})

module.exports = {
  getCampView,
  getBeaconView,
  getBeaconHistoryView,
  getPOIView,
  getPOIListView,
  getSegmentView,
}
