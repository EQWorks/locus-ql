const { CAT_STRING, CAT_NUMERIC, CAT_DATE } = require('../../type')
const { CU_ADVERTISER, ACCESS_INTERNAL, ACCESS_CUSTOMER } = require('./constants')
const { pgViews } = require('./pg-views')


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
]

module.exports = {
  name: 'ATOM Impressions',
  table: 'fusion_logs.impression_logs',
  owner: CU_ADVERTISER,
  columns: {
    camp_code: {
      category: CAT_NUMERIC,
      inFastViews: [pgViews.ATOM_CAMPS, ...allStandardChViews],
    },
    camp_name: {
      category: CAT_STRING,
      dependsOn: ['camp_code'],
      viewExpression: 'atom_camps.camp_name',
      joins: [campJoin],
    },
    flight_code: {
      category: CAT_NUMERIC,
      dependsOn: ['camp_code'],
      viewExpression: 'atom_camps.flight_code',
      joins: [campJoin],
    },
    flight_name: {
      category: CAT_STRING,
      dependsOn: ['camp_code'],
      viewExpression: 'atom_camps.flight_name',
      joins: [campJoin],
    },
    offer_code: {
      category: CAT_NUMERIC,
      dependsOn: ['camp_code'],
      viewExpression: 'atom_camps.offer_code',
      joins: [campJoin],
    },
    offer_name: {
      category: CAT_STRING,
      dependsOn: ['camp_code'],
      viewExpression: 'atom_camps.offer_name',
      joins: [campJoin],
    },
    date: {
      category: CAT_DATE,
      inFastViews: allStandardChViews,
    },
    fsa: {
      category: CAT_STRING,
      geo_type: 'ca-fsa',
      expression: 'postal_code AS fsa',
    },
    impressions: {
      category: CAT_NUMERIC,
      expression: 'count(*) AS impressions',
      isAggregate: true,
      inFastViews: allStandardChViews,
    },
    clicks: {
      category: CAT_NUMERIC,
      expression: 'count_if(click) AS clicks',
      isAggregate: true,
      inFastViews: allStandardChViews,
    },
    user_ip: {
      category: CAT_STRING,
      expression: 'substr(to_hex(sha256(cast(ip AS varbinary))), 1, 20) AS user_ip',
    },
    user_id: {
      category: CAT_STRING,
      expression: 'substr(to_hex(sha256(cast(user_guid AS varbinary))), 1, 20) AS user_id',
    },
    household_id: {
      category: CAT_STRING,
      expression: 'substr(to_hex(sha256(cast(hh_id AS varbinary))), 1, 20) AS hh_id',
      viewExpression: 'log.hh_id AS household_id',
    },
    household_fsa: {
      category: CAT_STRING,
      geo_type: 'ca-fsa',
      expression: 'hh_fsa',
      viewExpression: 'hh_fsa AS household_fsa',
    },
    os_id: {
      category: CAT_NUMERIC,
      inFastViews: [pgViews.ATOM_OS, pgViews.ATOM_CH_OS_ID],
    },
    os_name: {
      category: CAT_STRING,
      dependsOn: ['os_id'],
      viewExpression: 'atom_os.os_name',
      joins: [
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
      inFastViews: [pgViews.ATOM_BROWSERS, pgViews.ATOM_CH_BROWSER_ID],
    },
    browser_name: {
      category: CAT_STRING,
      dependsOn: ['browser_id'],
      viewExpression: 'atom_browsers.browser_name',
      joins: [
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
      inFastViews: [pgViews.ATOM_CH_CITY],
    },
    banner_code: {
      category: CAT_NUMERIC,
      inFastViews: [pgViews.ATOM_BANNERS, pgViews.ATOM_CH_BANNER_CODE],
    },
    banner_name: {
      category: CAT_STRING,
      dependsOn: ['banner_code'],
      viewExpression: 'atom_banners.banner_name',
      joins: [bannerJoin],
    },
    banner_size: {
      category: CAT_STRING,
      dependsOn: ['banner_code'],
      viewExpression: 'atom_banners.banner_size',
      joins: [bannerJoin],
    },
    app_platform_id: {
      category: CAT_NUMERIC,
      expression: 'COALESCE(app_platform_id, 0) AS app_platform_id',
    },
    app_platform_name: {
      category: CAT_STRING,
      dependsOn: ['app_platform_id'],
      viewExpression: `
        CASE
          WHEN log.app_platform_id = 1 THEN 'Android'
          WHEN log.app_platform_id = 2 THEN 'iOS'
          ELSE 'Unknown/Browser'
        END
      `,
    },
    revenue: {
      category: CAT_NUMERIC,
      access: ACCESS_INTERNAL,
      isAggregate: true,
      inFastViews: allStandardChViews,
    },
    revenue_in_currency: {
      category: CAT_NUMERIC,
      access: ACCESS_INTERNAL,
      isAggregate: true,
      inFastViews: allStandardChViews,
    },
    spend: {
      aliasFor: 'revenue',
      access: ACCESS_CUSTOMER,
    },
    spend_in_currency: {
      aliasFor: 'revenue_in_currency',
      access: ACCESS_CUSTOMER,
    },
    cost: {
      category: CAT_NUMERIC,
      access: ACCESS_INTERNAL,
      isAggregate: true,
      inFastViews: allStandardChViews,
    },
    cost_in_currency: {
      category: CAT_NUMERIC,
      access: ACCESS_INTERNAL,
      isAggregate: true,
      inFastViews: allStandardChViews,
    },
  },
}
