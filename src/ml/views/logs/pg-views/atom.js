const { knex, ATOM_READ_FDW_CONNECTION } = require('../../../../util/db')
const { PG_CACHE_DAYS } = require('../constants')


const getCampView = (_, advertiserID) => ({
  view: knex.raw(`
    (
      SELECT * FROM dblink(:fdwConnection, '
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
          ${advertiserID ? "AND mb.advertiserid = ' || :advertiserID || '" : ''}
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
    fdwConnection: ATOM_READ_FDW_CONNECTION,
    advertiserID,
  }),
  fdwConnection: ATOM_READ_FDW_CONNECTION,
})

const getOsView = () => ({
  view: knex.raw(`
    (
      SELECT * FROM dblink(?, '
        SELECT
          osid,
          osname
        FROM public.os
      ') AS t(
        os_id int,
        os_name text
      )
    ) AS atom_os
  `, [ATOM_READ_FDW_CONNECTION]),
  fdwConnection: ATOM_READ_FDW_CONNECTION,
})

const getBrowserView = () => ({
  view: knex.raw(`
    (
      SELECT * FROM dblink(?, '
        SELECT
          browsersid,
          browsername
        FROM public.browsers
      ') AS t(
        browser_id int,
        browser_name text
      )
    ) AS atom_browsers
  `, [ATOM_READ_FDW_CONNECTION]),
  fdwConnection: ATOM_READ_FDW_CONNECTION,
})

const getBannerView = (_, advertiserID) => ({
  view: knex.raw(`
    (
      SELECT * FROM dblink(:fdwConnection, '
        SELECT
          bannercode,
          bannername,
          imgwidth || ''x'' || imgheight
        FROM public.banners
        ${advertiserID ? "WHERE advertiserid = ' || :advertiserID || '" : ''}
      ') AS t(
        banner_code int,
        banner_name text,
        banner_size text
      )
    ) AS atom_banners
  `, {
    fdwConnection: ATOM_READ_FDW_CONNECTION,
    advertiserID,
  }),
  fdwConnection: ATOM_READ_FDW_CONNECTION,
})

const getAdPositionView = () => ({
  view: knex.raw(`
    (
      SELECT * FROM dblink(?, '
        SELECT
          id,
          name
        FROM public.bannerpositions
      ') AS t(
        ad_position smallint,
        ad_position_name text
      )
    ) AS atom_ad_positions
  `, [ATOM_READ_FDW_CONNECTION]),
  fdwConnection: ATOM_READ_FDW_CONNECTION,
})

const getDomainView = () => ({
  view: knex.raw(`
    (
      SELECT * FROM dblink(?, '
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
  `, [ATOM_READ_FDW_CONNECTION]),
  fdwConnection: ATOM_READ_FDW_CONNECTION,
})

const getSegmentView = agencyID => ({
  view: knex.raw(`
    (
      SELECT * FROM dblink(:fdwConnection, '
        SELECT
          s.id,
          COALESCE(sn.name, s.name, s.id::text),
          s.unique_user
        FROM public.segment AS s
        LEFT JOIN public.segment_name AS sn ON sn.segment_id = s.id
        ${agencyID ? "WHERE s.customer = ' || :agencyID || '" : ''}
      ') AS t(
        user_segment_id int,
        user_segment_name text,
        user_segment_size int
      )
    ) AS atom_segments
  `, {
    fdwConnection: ATOM_READ_FDW_CONNECTION,
    agencyID,
  }),
  fdwConnection: ATOM_READ_FDW_CONNECTION,
})

const getLanguageView = () => ({
  view: knex.raw(`
    (
      SELECT * FROM dblink(?, '
        SELECT
          lang,
          langname
        FROM public.lang
      ') AS t(
        language_code text,
        language_name text
      )
    ) AS atom_languages
  `, [ATOM_READ_FDW_CONNECTION]),
  fdwConnection: ATOM_READ_FDW_CONNECTION,
})

const getNetworkView = () => ({
  view: knex.raw(`
    (
      SELECT * FROM dblink(?, '
        SELECT
          networkid,
          networkname
        FROM public.networks
      ') AS t(
        network_id int,
        network_name text
      )
    ) AS atom_networks
  `, [ATOM_READ_FDW_CONNECTION]),
  fdwConnection: ATOM_READ_FDW_CONNECTION,
})

const getChViewabilityView = (_, advertiserID) => ({
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
            ch.bannercode,
            ch.measurable,
            ch.inview,
            ch.fullyinview,
            ch.timeuntilinview,
            ch.totalexposuretime,
            ch.universalinteraction,
            ch.belowthefold,
            ch.didhover,
            ch.timeuntilhover,
            ch.didscroll,
            ch.timeuntilscroll
          FROM public.camphistory_viewability AS ch
          JOIN public.campaigns AS mb ON mb.campcode = ch.campcode
          WHERE
            ch.date >= (NOW() - ${PG_CACHE_DAYS} * INTERVAL ''1 day'')::date
            ${advertiserID ? "AND mb.advertiserid = ' || :advertiserID || '" : ''}

          UNION

          SELECT
            mb.advertiserid,
            ch_tz.campcode,
            ch_tz.date,
            ch_tz.bannercode,
            ch_tz.measurable,
            ch_tz.inview,
            ch_tz.fullyinview,
            ch_tz.timeuntilinview,
            ch_tz.totalexposuretime,
            ch_tz.universalinteraction,
            ch_tz.belowthefold,
            ch_tz.didhover,
            ch_tz.timeuntilhover,
            ch_tz.didscroll,
            ch_tz.timeuntilscroll
          FROM public.ch_tz_viewability AS ch_tz
          JOIN public.campaigns AS mb ON mb.campcode = ch_tz.campcode
          WHERE
            ch_tz.date >= (NOW() - ${PG_CACHE_DAYS} * INTERVAL ''1 day'')::date
            ${advertiserID ? "AND mb.advertiserid = ' || :advertiserID || '" : ''}
        )
        SELECT
          advertiserid,
          campcode,
          date,
          bannercode,
          SUM(COALESCE(measurable, 0)),
          SUM(COALESCE(inview, 0)),
          SUM(COALESCE(fullyinview, 0)),
          SUM(COALESCE(timeuntilinview, 0)),
          SUM(COALESCE(totalexposuretime, 0)),
          SUM(COALESCE(universalinteraction, 0)),
          SUM(COALESCE(belowthefold, 0)),
          SUM(COALESCE(didhover, 0)),
          SUM(COALESCE(timeuntilhover, 0)),
          SUM(COALESCE(didscroll, 0)),
          SUM(COALESCE(timeuntilscroll, 0))
        FROM hourly
        GROUP BY 1, 2, 3, 4
      ') AS t(
        customer_id int,
        camp_code int,
        time_tz timestamptz,
        banner_code int,
        viewability_measurable int,
        viewability_in_view int,
        viewability_fully_in_view int,
        viewability_time_until_in_view int,
        viewability_total_exposure_time int,
        viewability_universal_interaction int,
        viewability_below_the_fold int,
        viewability_did_hover int,
        viewability_time_until_hover int,
        viewability_did_scroll int,
        viewability_time_until_scroll int
      )
    ) AS atom_ch_viewability
  `, {
    fdwConnection: ATOM_READ_FDW_CONNECTION,
    advertiserID,
  }),
  fdwConnection: ATOM_READ_FDW_CONNECTION,
})

// features = [feature, viewColumn, viewPgType][]
const makeChView = (features, advertiserID) => ({
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
            ${features.map(([ft]) => `ch."${ft}"`).join(', ')},
            ch.impressions,
            ch.clicks,
            ch.revenue,
            ch.revenueincurrency,
            ch.cost,
            ch.costincurrency
          FROM public.camphistory_${features.map(([ft]) => ft).join('_')} AS ch
          JOIN public.campaigns AS mb ON mb.campcode = ch.campcode
          WHERE
            ch.date >= (NOW() - ${PG_CACHE_DAYS} * INTERVAL ''1 day'')::date
            ${advertiserID ? "AND mb.advertiserid = ' || :advertiserID || '" : ''}

          UNION

          SELECT
            mb.advertiserid,
            ch_tz.campcode,
            ch_tz.date,
            ${features.map(([ft]) => `ch_tz."${ft}"`).join(', ')},
            ch_tz.impressions,
            ch_tz.clicks,
            ch_tz.revenue,
            ch_tz.revenueincurrency,
            ch_tz.cost,
            ch_tz.costincurrency
          FROM public.ch_tz_${features.map(([ft]) => ft).join('_')} AS ch_tz
          JOIN public.campaigns AS mb ON mb.campcode = ch_tz.campcode
          WHERE
            ch_tz.date >= (NOW() - ${PG_CACHE_DAYS} * INTERVAL ''1 day'')::date
            ${advertiserID ? "AND mb.advertiserid = ' || :advertiserID || '" : ''}
        )
        SELECT
          advertiserid,
          campcode,
          date,
          ${features.map(([ft]) => `"${ft}"`).join(', ')},
          SUM(COALESCE(impressions, 0))::int,
          SUM(COALESCE(clicks, 0))::int,
          SUM(COALESCE(revenue, 0)),
          SUM(COALESCE(revenueincurrency, 0)),
          SUM(COALESCE(cost, 0)),
          SUM(COALESCE(costincurrency, 0))
        FROM hourly
        GROUP BY 1, 2, 3, ${features.map((_, i) => i + 4).join(', ')}
      ') AS t(
        customer_id int,
        camp_code int,
        time_tz timestamptz,
        ${features.map(([, col, type]) => `${col} ${type}`).join(',')},
        impressions int,
        clicks int,
        _revenue real,
        _revenue_in_currency real,
        _cost real,
        _cost_in_currency real
      )
    ) AS atom_ch_${features.map(([, col]) => col).join('_')}
  `, {
    fdwConnection: ATOM_READ_FDW_CONNECTION,
    advertiserID,
  }),
  fdwConnection: ATOM_READ_FDW_CONNECTION,
})

module.exports = {
  getCampView,
  getOsView,
  getBrowserView,
  getBannerView,
  getAdPositionView,
  getDomainView,
  getSegmentView,
  getLanguageView,
  getNetworkView,
  getChViewabilityView,
  makeChView,
}
