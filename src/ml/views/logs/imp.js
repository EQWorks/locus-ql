const { CAT_STRING, CAT_NUMERIC, CAT_DATE } = require('../../type')
const { geoTypes } = require('../../geo')
const { CU_ADVERTISER, ACCESS_INTERNAL, ACCESS_CUSTOMER, ACCESS_PRIVATE } = require('./constants')
const { pgViews } = require('./pg-views')
const { viewCategories } = require('../taxonomies')


const campJoin = {
  type: 'left',
  view: pgViews.ATOM_CAMPS,
  condition() {
    this.on('log.camp_code', '=', 'atom_camps.camp_code')
  },
}

const bannerJoin = {
  type: 'left',
  view: pgViews.ATOM_BANNERS,
  condition() {
    this.on('log.banner_code', '=', 'atom_banners.banner_code')
  },
}

const locusPoiJoin = {
  type: 'left',
  view: pgViews.LOCUS_POI,
  condition() {
    this.on('log.locus_poi_id', '=', 'locus_poi.locus_poi_id')
  },
}

const locusPoiListJoin = {
  type: 'left',
  view: pgViews.LOCUS_POI_LISTS,
  condition() {
    this.on('log.locus_poi_list_id', '=', 'locus_poi_lists.locus_poi_list_id')
  },
}

const iabJoin = {
  type: 'left',
  view: pgViews.IAB_CATS,
  condition() {
    this.on('log.iab_cat', '=', 'iab_cats.iab_cat')
  },
}

const segmentJoin = {
  type: 'left',
  view: pgViews.ATOM_SEGMENTS,
  condition() {
    this.on('log.user_segment_id', '=', 'atom_segments.user_segment_id')
  },
}

// sorted from lower to higher cardinality (observed)
const allStandardChViews = [
  pgViews.ATOM_CH_AD_POSITION,
  pgViews.ATOM_CH_BROWSER_ID,
  pgViews.ATOM_CH_OS_ID,
  pgViews.ATOM_CH_LANGUAGE,
  pgViews.ATOM_CH_SESS_DEPTH,
  pgViews.ATOM_CH_BANNER_CODE,
  pgViews.ATOM_CH_NETWORK_ID,
  pgViews.ATOM_CH_USER_SEG,
  pgViews.ATOM_CH_IAB_CAT,
  pgViews.ATOM_CH_CITY,
  pgViews.ATOM_CH_DOMAIN_ID,
  pgViews.ATOM_CH_GEOCOHORT_ID_ITEM,
]

module.exports = {
  name: 'ATOM Impressions',
  category: viewCategories.ATOM_CAMPAIGNS,
  table: 'fusion_logs.impression_logs',
  owner: CU_ADVERTISER,
  columns: {
    camp_code: {
      category: CAT_NUMERIC,
      pgType: 'int',
      inFastViews: [pgViews.ATOM_CAMPS, ...allStandardChViews, pgViews.ATOM_CH_VIEWABILITY],
    },
    camp_name: {
      category: CAT_STRING,
      dependsOn: ['camp_code'],
      viewExpression: 'atom_camps.camp_name',
      viewJoins: [campJoin],
    },
    flight_code: {
      category: CAT_NUMERIC,
      dependsOn: ['camp_code'],
      viewExpression: 'atom_camps.flight_code',
      viewJoins: [campJoin],
    },
    flight_name: {
      category: CAT_STRING,
      dependsOn: ['camp_code'],
      viewExpression: 'atom_camps.flight_name',
      viewJoins: [campJoin],
    },
    offer_code: {
      category: CAT_NUMERIC,
      dependsOn: ['camp_code'],
      viewExpression: 'atom_camps.offer_code',
      viewJoins: [campJoin],
    },
    offer_name: {
      category: CAT_STRING,
      dependsOn: ['camp_code'],
      viewExpression: 'atom_camps.offer_name',
      viewJoins: [campJoin],
    },
    _date: {
      category: CAT_DATE,
      access: ACCESS_PRIVATE,
      inFastViews: [...allStandardChViews, pgViews.ATOM_CH_VIEWABILITY],
    },
    _hour: {
      category: CAT_NUMERIC,
      access: ACCESS_PRIVATE,
    },
    date: {
      category: CAT_DATE,
      dependsOn: ['_date'],
      viewExpression: 'log.time_tz::date',
    },
    datetime: {
      category: CAT_DATE,
      dependsOn: ['_date', '_hour'],
      viewExpression: 'log.time_tz',
    },
    _postal_code: {
      category: CAT_STRING,
      pgType: 'varchar(10)',
      access: ACCESS_PRIVATE,
      expression: 'postal_code',
    },
    geo_ca_fsa: {
      category: CAT_STRING,
      dependsOn: ['_postal_code'],
      geo_type: geoTypes.CA_FSA,
      viewExpression: "substring(log._postal_code from '^[A-Z]\\d[A-Z]$')",
    },
    geo_us_postalcode: {
      category: CAT_NUMERIC,
      dependsOn: ['_postal_code'],
      // geo_type: 'us-postalcode',
      viewExpression: "substring(log._postal_code from '^\\d{5}$')::int",
    },
    impressions: {
      category: CAT_NUMERIC,
      pgType: 'int',
      expression: 'count(*)',
      viewExpression: 'SUM(COALESCE(log.impressions, 0))',
      isAggregate: true,
      inFastViews: allStandardChViews,
    },
    clicks: {
      category: CAT_NUMERIC,
      pgType: 'int',
      expression: 'count_if(click)',
      viewExpression: 'SUM(COALESCE(log.clicks, 0))',
      isAggregate: true,
      inFastViews: allStandardChViews,
    },
    click_through_rate: {
      category: CAT_NUMERIC,
      dependsOn: ['impressions', 'clicks'],
      viewExpression: `
        CASE
          WHEN SUM(COALESCE(log.impressions, 0)) > 0 THEN
            (SUM(COALESCE(log.clicks, 0))::real / SUM(COALESCE(log.impressions, 0)))::numeric(5, 4)
          ELSE 0
        END
      `,
      isAggregate: true,
    },
    user_ip: {
      category: CAT_STRING,
      pgType: 'varchar(20)',
      expression: 'substr(to_hex(sha256(cast(ip AS varbinary))), 1, 20)',
    },
    user_id: {
      category: CAT_STRING,
      pgType: 'varchar(20)',
      expression: 'substr(to_hex(sha256(cast(user_guid AS varbinary))), 1, 20)',
    },
    household_id: {
      category: CAT_STRING,
      pgType: 'varchar(20)',
      expression: 'substr(to_hex(sha256(cast(hh_id AS varbinary))), 1, 20)',
    },
    household_fsa: {
      category: CAT_STRING,
      pgType: 'varchar(10)',
      geo_type: geoTypes.CA_FSA,
      expression: 'hh_fsa',
    },
    os_id: {
      category: CAT_NUMERIC,
      pgType: 'smallint',
      expression: 'COALESCE(os_id, 1092)',
      inFastViews: [pgViews.ATOM_OS, pgViews.ATOM_CH_OS_ID],
    },
    os_name: {
      category: CAT_STRING,
      dependsOn: ['os_id'],
      viewExpression: 'atom_os.os_name',
      viewJoins: [
        {
          type: 'left',
          view: pgViews.ATOM_OS,
          condition() {
            this.on('log.os_id', '=', 'atom_os.os_id')
          },
        },
      ],
    },
    browser_id: {
      category: CAT_NUMERIC,
      pgType: 'smallint',
      expression: 'COALESCE(browser_id, 63)',
      inFastViews: [pgViews.ATOM_BROWSERS, pgViews.ATOM_CH_BROWSER_ID],
    },
    browser_name: {
      category: CAT_STRING,
      dependsOn: ['browser_id'],
      viewExpression: 'atom_browsers.browser_name',
      viewJoins: [
        {
          type: 'left',
          view: pgViews.ATOM_BROWSERS,
          condition() {
            this.on('log.browser_id', '=', 'atom_browsers.browser_id')
          },
        },
      ],
    },
    city: {
      category: CAT_STRING,
      pgType: 'varchar(500)',
      inFastViews: [pgViews.ATOM_CH_CITY],
    },
    country: {
      category: CAT_STRING,
      dependsOn: ['city'],
      viewExpression: 'substring(log.city for 2)',
    },
    geo_ca_province: {
      category: CAT_STRING,
      geo_type: geoTypes.CA_PROVINCE,
      dependsOn: ['city'],
      viewExpression: "substring(log.city from '^CA\\$([A-Z]{2})')",
    },
    geo_us_state: {
      category: CAT_STRING,
      // geo_type: 'us-state',
      dependsOn: ['city'],
      viewExpression: "substring(log.city from '^US\\$([A-Z]{2})')",
    },
    geo_ca_city: {
      category: CAT_STRING,
      geo_type: geoTypes.CA_CITY,
      dependsOn: ['city'],
      viewExpression: "upper(substring(log.city from '^CA\\$[A-Z]{2}\\$(.*)$'))",
    },
    geo_us_city: {
      category: CAT_STRING,
      // geo_type: 'us-city',
      dependsOn: ['city'],
      viewExpression: "upper(substring(log.city from '^US\\$[A-Z]{2}\\$(.*)$'))",
    },
    banner_code: {
      category: CAT_NUMERIC,
      pgType: 'int',
      inFastViews: [pgViews.ATOM_BANNERS, pgViews.ATOM_CH_BANNER_CODE, pgViews.ATOM_CH_VIEWABILITY],
    },
    banner_name: {
      category: CAT_STRING,
      dependsOn: ['banner_code'],
      viewExpression: 'atom_banners.banner_name',
      viewJoins: [bannerJoin],
    },
    banner_size: {
      category: CAT_STRING,
      dependsOn: ['banner_code'],
      viewExpression: 'atom_banners.banner_size',
      viewJoins: [bannerJoin],
    },
    app_platform_id: {
      category: CAT_NUMERIC,
      pgType: 'smallint',
      expression: 'COALESCE(app_platform_id, 0)',
      inFastViews: [pgViews.APP_PLATFORMS],
    },
    app_platform_name: {
      category: CAT_STRING,
      dependsOn: ['app_platform_id'],
      viewExpression: 'app_platforms.app_platform_name',
      viewJoins: [
        {
          type: 'left',
          view: pgViews.APP_PLATFORMS,
          condition() {
            this.on('log.app_platform_id', '=', 'app_platforms.app_platform_id')
          },
        },
      ],
    },
    _revenue: {
      category: CAT_NUMERIC,
      pgType: 'numeric(19, 4)',
      access: ACCESS_PRIVATE,
      expression: 'SUM(revenue)',
      // viewExpression: 'SUM(COALESCE(log._revenue, 0))',
      isAggregate: true,
      inFastViews: allStandardChViews,
    },
    _revenue_in_currency: {
      category: CAT_NUMERIC,
      pgType: 'numeric(19, 4)',
      access: ACCESS_PRIVATE,
      expression: 'SUM(revenue_in_currency)',
      // viewExpression: 'SUM(COALESCE(log._revenue_in_currency, 0))',
      isAggregate: true,
      inFastViews: allStandardChViews,
    },
    revenue: {
      category: CAT_NUMERIC,
      access: ACCESS_INTERNAL,
      dependsOn: ['_revenue', '_revenue_in_currency'],
      viewExpression: 'SUM(COALESCE(log._revenue_in_currency, log._revenue, 0))',
      isAggregate: true,
    },
    spend: {
      aliasFor: 'revenue',
      access: ACCESS_CUSTOMER,
    },
    _cost: {
      category: CAT_NUMERIC,
      pgType: 'numeric(19, 4)',
      access: ACCESS_PRIVATE,
      expression: 'SUM(cost)',
      // viewExpression: 'SUM(COALESCE(log._cost, 0))',
      isAggregate: true,
      inFastViews: allStandardChViews,
    },
    _cost_in_currency: {
      category: CAT_NUMERIC,
      pgType: 'numeric(19, 4)',
      access: ACCESS_PRIVATE,
      expression: 'SUM(cost_in_currency)',
      // viewExpression: 'SUM(COALESCE(log._cost_in_currency, 0))',
      isAggregate: true,
      inFastViews: allStandardChViews,
    },
    cost: {
      category: CAT_NUMERIC,
      access: ACCESS_INTERNAL,
      dependsOn: ['_cost', '_cost_in_currency'],
      viewExpression: 'SUM(COALESCE(log._cost_in_currency, log._cost, 0))',
      isAggregate: true,
    },
    cost_per_mile: {
      category: CAT_NUMERIC,
      dependsOn: ['impressions', '_revenue', '_revenue_in_currency'],
      viewExpression: `
        CASE
          WHEN SUM(COALESCE(log.impressions, 0)) > 0 THEN
            (1000 * SUM(
              COALESCE(log._revenue_in_currency, log._revenue, 0)
            )::real / SUM(
              COALESCE(log.impressions, 0))
            )::numeric(5, 4)
          ELSE 0
        END
      `,
      isAggregate: true,
    },
    cost_per_mile_internal: {
      category: CAT_NUMERIC,
      access: ACCESS_INTERNAL,
      dependsOn: ['impressions', '_cost', '_cost_in_currency'],
      viewExpression: `
        CASE
          WHEN SUM(COALESCE(log.impressions, 0)) > 0 THEN
            (1000 * SUM(
              COALESCE(log._cost_in_currency, log._cost, 0)
            )::real / SUM(
              COALESCE(log.impressions, 0))
            )::numeric(5, 4)
          ELSE 0
        END
      `,
      isAggregate: true,
    },
    locus_poi_id: {
      category: CAT_NUMERIC,
      pgType: 'int',
      geo_type: geoTypes.POI,
      inFastViews: [pgViews.LOCUS_POI],
    },
    locus_poi_name: {
      category: CAT_STRING,
      dependsOn: ['locus_poi_id'],
      viewExpression: 'locus_poi.locus_poi_name',
      viewJoins: [locusPoiJoin],
    },
    locus_poi_lat: {
      category: CAT_NUMERIC,
      dependsOn: ['locus_poi_id'],
      viewExpression: 'locus_poi.locus_poi_lat',
      viewJoins: [locusPoiJoin],
    },
    locus_poi_long: {
      category: CAT_NUMERIC,
      dependsOn: ['locus_poi_id'],
      viewExpression: 'locus_poi.locus_poi_long',
      viewJoins: [locusPoiJoin],
    },
    locus_poi_list_id: {
      category: CAT_NUMERIC,
      pgType: 'int',
      inFastViews: [pgViews.LOCUS_POI_LISTS],
    },
    locus_poi_list_name: {
      category: CAT_STRING,
      dependsOn: ['locus_poi_list_id'],
      viewExpression: 'locus_poi_lists.locus_poi_list_name',
      viewJoins: [locusPoiListJoin],
    },
    ad_position: {
      category: CAT_NUMERIC,
      pgType: 'smallint',
      expression: 'COALESCE(ad_position, 1)',
      inFastViews: [pgViews.ATOM_AD_POSITIONS, pgViews.ATOM_CH_AD_POSITION],
    },
    ad_position_name: {
      category: CAT_STRING,
      dependsOn: ['ad_position'],
      viewExpression: 'atom_ad_positions.ad_position_name',
      viewJoins: [
        {
          type: 'left',
          view: pgViews.ATOM_AD_POSITIONS,
          condition() {
            this.on('log.ad_position', '=', 'atom_ad_positions.ad_position')
          },
        },
      ],
    },
    domain_id: {
      category: CAT_NUMERIC,
      pgType: 'bigint',
      inFastViews: [pgViews.ATOM_DOMAINS, pgViews.ATOM_CH_DOMAIN_ID],
    },
    domain_name: {
      category: CAT_STRING,
      dependsOn: ['domain_id'],
      viewExpression: 'atom_domains.domain_name',
      viewJoins: [
        {
          type: 'left',
          view: pgViews.ATOM_DOMAINS,
          condition() {
            this.on('log.domain_id', '=', 'atom_domains.domain_id')
          },
        },
      ],
    },
    user_segment_id: {
      category: CAT_NUMERIC,
      pgType: 'int',
      expression: 'u_segments.user_segment_id',
      crossJoin: 'CROSS JOIN UNNEST(user_segments) AS u_segments(user_segment_id)',
      inFastViews: [pgViews.ATOM_SEGMENTS, pgViews.ATOM_CH_USER_SEG],
    },
    user_segment_name: {
      category: CAT_STRING,
      dependsOn: ['user_segment_id'],
      viewExpression: 'atom_segments.user_segment_name',
      viewJoins: [segmentJoin],
    },
    user_segment_size: {
      category: CAT_NUMERIC,
      dependsOn: ['user_segment_id'],
      viewExpression: 'atom_segments.user_segment_size',
      viewJoins: [segmentJoin],
    },
    connection_type: {
      category: CAT_NUMERIC,
      pgType: 'smallint',
      inFastViews: [pgViews.MAXMIND_CONNECTION_TYPES],
    },
    connection_type_name: {
      category: CAT_STRING,
      dependsOn: ['connection_type'],
      viewExpression: 'maxmind_connection_types.connection_type_name',
      viewJoins: [
        {
          type: 'left',
          view: pgViews.MAXMIND_CONNECTION_TYPES,
          condition() {
            this.on('log.connection_type', '=', 'maxmind_connection_types.connection_type')
          },
        },
      ],
    },
    language_code: {
      category: CAT_STRING,
      pgType: 'varchar(2)',
      expression: 'language',
      inFastViews: [pgViews.ATOM_LANGUAGES, pgViews.ATOM_CH_LANGUAGE],
    },
    language_name: {
      category: CAT_STRING,
      dependsOn: ['language_code'],
      viewExpression: 'atom_languages.language_name',
      viewJoins: [
        {
          type: 'left',
          view: pgViews.ATOM_LANGUAGES,
          condition() {
            this.on('log.language_code', '=', 'atom_languages.language_code')
          },
        },
      ],
    },
    network_id: {
      category: CAT_NUMERIC,
      pgType: 'int',
      expression: 'ntwrk_id',
      inFastViews: [pgViews.ATOM_NETWORKS, pgViews.ATOM_CH_NETWORK_ID],
    },
    network_name: {
      category: CAT_STRING,
      dependsOn: ['network_id'],
      viewExpression: 'atom_networks.network_name',
      viewJoins: [
        {
          type: 'left',
          view: pgViews.ATOM_NETWORKS,
          condition() {
            this.on('log.network_id', '=', 'atom_networks.network_id')
          },
        },
      ],
    },
    geo_cohort_id: {
      category: CAT_NUMERIC,
      pgType: 'int',
      expression: 'geo_cohort_id',
      inFastViews: [pgViews.LOCUS_GEO_COHORTS, pgViews.ATOM_CH_GEOCOHORT_ID_ITEM],
    },
    geo_cohort_name: {
      category: CAT_STRING,
      dependsOn: ['geo_cohort_id'],
      viewExpression: 'locus_geo_cohorts.geo_cohort_name',
      viewJoins: [
        {
          type: 'left',
          view: pgViews.LOCUS_GEO_COHORTS,
          condition() {
            this.on('log.geo_cohort_id', '=', 'locus_geo_cohorts.geo_cohort_id')
          },
        },
      ],
    },
    _geo_cohort_item: {
      category: CAT_STRING,
      access: ACCESS_PRIVATE,
      pgType: 'varchar(10)', // to support US DDDDD-DDDD
      expression: 'geo_cohort_item',
      inFastViews: [pgViews.LOCUS_GEO_COHORT_ITEMS, pgViews.ATOM_CH_GEOCOHORT_ID_ITEM],
    },
    geo_cohort_fsa: {
      category: CAT_STRING,
      dependsOn: ['_geo_cohort_item'],
      geo_type: geoTypes.CA_FSA,
      viewExpression: "substring(log._geo_cohort_item from '^[A-Z]\\d[A-Z]')",
    },
    geo_cohort_postalcode: {
      category: CAT_STRING,
      dependsOn: ['_geo_cohort_item'],
      geo_type: geoTypes.CA_POSTALCODE,
      viewExpression: "substring(log._geo_cohort_item from '^[A-Z]\\d[A-Z]\\d[A-Z]\\d')",
    },
    iab_cat: {
      category: CAT_STRING,
      pgType: 'varchar(10)',
      expression: 'u_iab_cats.iab_cat',
      crossJoin: 'CROSS JOIN UNNEST(iab_cat) AS u_iab_cats(iab_cat)',
      inFastViews: [pgViews.IAB_CATS, pgViews.ATOM_CH_IAB_CAT],
    },
    iab_cat_name: {
      category: CAT_STRING,
      dependsOn: ['iab_cat'],
      viewExpression: 'iab_cats.iab_cat_name',
      viewJoins: [iabJoin],
    },
    iab_parent_cat: {
      category: CAT_STRING,
      dependsOn: ['iab_cat'],
      viewExpression: 'iab_cats.iab_parent_cat',
      viewJoins: [iabJoin],
    },
    iab_parent_cat_name: {
      category: CAT_STRING,
      dependsOn: ['iab_cat'],
      viewExpression: 'iab_cats.iab_parent_cat_name',
      viewJoins: [iabJoin],
    },
    viewability_measurable: {
      category: CAT_NUMERIC,
      pgType: 'int',
      expression: 'count_if(view_measurable)',
      viewExpression: 'SUM(COALESCE(log.viewability_measurable, 0))',
      isAggregate: true,
      inFastViews: [pgViews.ATOM_CH_VIEWABILITY],
    },
    viewability_in_view: {
      category: CAT_NUMERIC,
      pgType: 'int',
      expression: 'count_if(view_in_view)',
      viewExpression: 'SUM(COALESCE(log.viewability_in_view, 0))',
      isAggregate: true,
      inFastViews: [pgViews.ATOM_CH_VIEWABILITY],
    },
    viewability_fully_in_view: {
      category: CAT_NUMERIC,
      pgType: 'int',
      expression: 'count_if(view_fully_in_view)',
      viewExpression: 'SUM(COALESCE(log.viewability_fully_in_view, 0))',
      isAggregate: true,
      inFastViews: [pgViews.ATOM_CH_VIEWABILITY],
    },
    viewability_time_until_in_view: {
      category: CAT_NUMERIC,
      pgType: 'int',
      expression: 'SUM(view_time_until_in_view)',
      viewExpression: 'SUM(COALESCE(log.viewability_time_until_in_view, 0))',
      isAggregate: true,
      inFastViews: [pgViews.ATOM_CH_VIEWABILITY],
    },
    viewability_average_time_until_in_view: {
      category: CAT_NUMERIC,
      dependsOn: ['viewability_in_view', 'viewability_time_until_in_view'],
      viewExpression: `
        CASE
          WHEN SUM(COALESCE(log.viewability_in_view, 0)) > 0 THEN
            CEILING(SUM(
              COALESCE(log.viewability_time_until_in_view, 0)
            )::real / SUM(
              COALESCE(log.viewability_in_view, 0)
            ))
          ELSE 0
        END
      `,
      isAggregate: true,
    },
    viewability_total_exposure_time: {
      category: CAT_NUMERIC,
      pgType: 'int',
      expression: 'SUM(view_total_exposure_time)',
      viewExpression: 'SUM(COALESCE(log.viewability_total_exposure_time, 0))',
      isAggregate: true,
      inFastViews: [pgViews.ATOM_CH_VIEWABILITY],
    },
    viewability_universal_interaction: {
      category: CAT_NUMERIC,
      pgType: 'int',
      expression: 'count_if(view_universal_interaction)',
      viewExpression: 'SUM(COALESCE(log.viewability_universal_interaction, 0))',
      isAggregate: true,
      inFastViews: [pgViews.ATOM_CH_VIEWABILITY],
    },
    viewability_below_the_fold: {
      category: CAT_NUMERIC,
      pgType: 'int',
      expression: 'count_if(view_below_the_fold)',
      viewExpression: 'SUM(COALESCE(log.viewability_below_the_fold, 0))',
      isAggregate: true,
      inFastViews: [pgViews.ATOM_CH_VIEWABILITY],
    },
    viewability_above_the_fold: {
      category: CAT_NUMERIC,
      dependsOn: ['viewability_measurable', 'viewability_below_the_fold'],
      // eslint-disable-next-line max-len
      viewExpression: 'SUM(COALESCE(log.viewability_measurable - log.viewability_below_the_fold, 0))',
      isAggregate: true,
    },
    viewability_did_hover: {
      category: CAT_NUMERIC,
      pgType: 'int',
      expression: 'count_if(view_did_hover)',
      viewExpression: 'SUM(COALESCE(log.viewability_did_hover, 0))',
      isAggregate: true,
      inFastViews: [pgViews.ATOM_CH_VIEWABILITY],
    },
    viewability_did_hover_rate: {
      category: CAT_NUMERIC,
      dependsOn: ['viewability_measurable', 'viewability_did_hover'],
      viewExpression: `
        CASE
          WHEN SUM(COALESCE(log.viewability_measurable, 0)) > 0 THEN
            SUM(
              COALESCE(log.viewability_did_hover, 0)
            )::real / SUM(
              COALESCE(log.viewability_measurable, 0)
            )::numeric(5, 4)
          ELSE 0
        END
      `,
      isAggregate: true,
    },
    viewability_time_until_hover: {
      category: CAT_NUMERIC,
      pgType: 'int',
      expression: 'SUM(view_time_until_hover)',
      viewExpression: 'SUM(COALESCE(log.viewability_time_until_hover, 0))',
      isAggregate: true,
      inFastViews: [pgViews.ATOM_CH_VIEWABILITY],
    },
    viewability_average_time_until_hover: {
      category: CAT_NUMERIC,
      dependsOn: ['viewability_did_hover', 'viewability_time_until_hover'],
      viewExpression: `
        CASE
          WHEN SUM(COALESCE(log.viewability_did_hover, 0)) > 0 THEN
            CEILING(SUM(
              COALESCE(log.viewability_time_until_hover, 0)
            )::real / SUM(
              COALESCE(log.viewability_did_hover, 0)
            ))
          ELSE 0
        END
      `,
      isAggregate: true,
    },
    viewability_did_scroll: {
      category: CAT_NUMERIC,
      pgType: 'int',
      expression: 'count_if(view_did_scroll)',
      viewExpression: 'SUM(COALESCE(log.view_did_scroll, 0))',
      isAggregate: true,
      inFastViews: [pgViews.ATOM_CH_VIEWABILITY],
    },
    viewability_did_scroll_rate: {
      category: CAT_NUMERIC,
      dependsOn: ['viewability_measurable', 'viewability_did_scroll'],
      viewExpression: `
        CASE
          WHEN SUM(COALESCE(log.viewability_measurable, 0)) > 0 THEN
            SUM(
              COALESCE(log.viewability_did_scroll, 0)
            )::real / SUM(
              COALESCE(log.viewability_measurable, 0)
            )::numeric(5, 4)
          ELSE 0
        END
      `,
      isAggregate: true,
    },
    viewability_time_until_scroll: {
      category: CAT_NUMERIC,
      pgType: 'int',
      expression: 'SUM(view_time_until_scroll)',
      viewExpression: 'SUM(COALESCE(log.viewability_time_until_scroll, 0))',
      isAggregate: true,
      inFastViews: [pgViews.ATOM_CH_VIEWABILITY],
    },
    viewability_average_time_until_scroll: {
      category: CAT_NUMERIC,
      dependsOn: ['viewability_did_scroll', 'viewability_time_until_scroll'],
      viewExpression: `
        CASE
          WHEN SUM(COALESCE(log.viewability_did_scroll, 0)) > 0 THEN
            CEILING(SUM(
              COALESCE(log.viewability_time_until_scroll, 0)
            )::real / SUM(
              COALESCE(log.viewability_did_scroll, 0)
            ))
          ELSE 0
        END
      `,
      isAggregate: true,
    },
  },
}
