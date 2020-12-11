const { knex } = require('../../../../util/db')
const { ATOM_CONNECTION_NAME, PG_CACHE_DAYS } = require('../constants')


const getCampView = customerID => ({
  view: knex.raw(`
    (
      SELECT * FROM dblink(:fdwConnection,'
        SELECT
          mb.campcode AS camp_code,
          mb.name AS camp_name,
          f.campcode AS flight_code,
          f.name AS flight_name,
          o.campcode AS offer_code,
          o.name AS offer_name
        FROM public.campaigns AS mb
        JOIN public.campaigns AS f ON f.campcode = mb.flightid
        JOIN public.campaigns AS o ON o.campcode = mb.offerid
        WHERE
          mb.flightid <> 0
          AND mb.offerid <> 0
          ${customerID ? "AND mb.advertiserid = ' || :customerID || '" : ''}
      ') AS t(
        camp_code int,
        camp_name text,
        flight_code int,
        flight_name text,
        offer_code int,
        offer_name text
      )
    ) AS atom_camps
  `, {
    fdwConnection: ATOM_CONNECTION_NAME,
    customerID,
  }),
  fdwConnection: ATOM_CONNECTION_NAME,
})

const getOsView = () => ({
  view: knex.raw(`
    (
      SELECT * FROM dblink(?,'
        SELECT
          osid,
          osname
        FROM public.os
      ') AS t(
        os_id int,
        os_name text
      )
    ) AS atom_os
  `, [ATOM_CONNECTION_NAME]),
  fdwConnection: ATOM_CONNECTION_NAME,
})

const getBrowserView = () => ({
  view: knex.raw(`
    (
      SELECT * FROM dblink(?,'
        SELECT
          browsersid,
          browsername
        FROM public.browsers
      ') AS t(
        browser_id int,
        browser_name text
      )
    ) AS atom_browsers
  `, [ATOM_CONNECTION_NAME]),
  fdwConnection: ATOM_CONNECTION_NAME,
})

const getBannerView = customerID => ({
  view: knex.raw(`
    (
      SELECT * FROM dblink(:fdwConnection,'
        SELECT
          bannercode,
          bannername,
          imgwidth || ''x'' || imgheight
        FROM public.banners
        ${customerID ? "WHERE advertiserid = ' || :customerID || '" : ''}
      ') AS t(
        banner_code int,
        banner_name text,
        banner_size text
      )
    ) AS atom_banners
  `, {
    fdwConnection: ATOM_CONNECTION_NAME,
    customerID,
  }),
  fdwConnection: ATOM_CONNECTION_NAME,
})

const getAdPositionView = () => ({
  view: knex.raw(`
    (
      SELECT * FROM dblink(?,'
        SELECT
          id,
          name
        FROM public.bannerpositions
      ') AS t(
        ad_position smallint,
        ad_position_name text
      )
    ) AS atom_ad_positions
  `, [ATOM_CONNECTION_NAME]),
  fdwConnection: ATOM_CONNECTION_NAME,
})

const getDomainView = () => ({
  view: knex.raw(`
    (
      SELECT * FROM dblink(?,'
        SELECT
          d.domainid,
          COALESCE(a.name, d.tld)
        FROM public.domains AS d
        LEFT JOIN public.apps AS a ON a.domainid = d.domainid AND a.pkgid = d.tld
      ') AS t(
        domain_id bigint,
        domain_name text
      )
    ) AS atom_domains
  `, [ATOM_CONNECTION_NAME]),
  fdwConnection: ATOM_CONNECTION_NAME,
})

const makeChView = (chFeature, viewColumn, viewPgType, customerID) => ({
  view: knex.raw(`
    (
      SELECT * FROM dblink(:fdwConnection, '
        WITH hourly AS (
          SELECT
            mb.advertiserid,
            ch.campcode,
            timezone(
              mb.timezone,
              timezone(''UTC'', ch.date + ch.hour * INTERVAL ''1 hour'')
            )::date AS date,
            ch."${chFeature}",
            ch.impressions,
            ch.clicks,
            ch.revenue,
            ch.revenueincurrency,
            ch.cost,
            ch.costincurrency
          FROM public.camphistory_${chFeature} AS ch
          JOIN public.campaigns AS mb ON mb.campcode = ch.campcode
          WHERE
            ch.date >= (NOW() - ${PG_CACHE_DAYS} * INTERVAL ''1 day'')::date
            ${customerID ? "AND mb.advertiserid = ' || :customerID || '" : ''}

          UNION

          SELECT
            mb.advertiserid,
            ch_tz.campcode,
            ch_tz.date,
            ch_tz."${chFeature}",
            ch_tz.impressions,
            ch_tz.clicks,
            ch_tz.revenue,
            ch_tz.revenueincurrency,
            ch_tz.cost,
            ch_tz.costincurrency
          FROM public.ch_tz_${chFeature} AS ch_tz
          JOIN public.campaigns AS mb ON mb.campcode = ch_tz.campcode
          WHERE
            ch_tz.date >= (NOW() - ${PG_CACHE_DAYS} * INTERVAL ''1 day'')::date
            ${customerID ? "AND mb.advertiserid = ' || :customerID || '" : ''}
        )
        SELECT
          advertiserid,
          campcode,
          date,
          "${chFeature}",
          SUM(COALESCE(impressions, 0))::int,
          SUM(COALESCE(clicks, 0))::int,
          SUM(COALESCE(revenue, 0)),
          SUM(COALESCE(revenueincurrency, 0)),
          SUM(COALESCE(cost, 0)),
          SUM(COALESCE(costincurrency, 0))
        FROM hourly
        GROUP BY 1, 2, 3, 4
      ') AS t(
        customer_id int,
        camp_code int,
        date date,
        ${viewColumn} ${viewPgType},
        impressions int,
        clicks int,
        revenue real,
        revenue_in_currency real,
        cost real,
        cost_in_currency real
      )
    ) AS atom_ch_${viewColumn}
  `, {
    fdwConnection: ATOM_CONNECTION_NAME,
    customerID,
  }),
  fdwConnection: ATOM_CONNECTION_NAME,
})

module.exports = {
  getCampView,
  getOsView,
  getBrowserView,
  getBannerView,
  getAdPositionView,
  getDomainView,
  makeChView,
}
