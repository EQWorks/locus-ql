const { CAT_STRING, CAT_NUMERIC, CAT_DATE } = require('../../type')
const { CU_ADVERTISER, ACCESS_INTERNAL, ACCESS_CUSTOMER } = require('./constants')
const { campView, osView, browserView, bannerView } = require('./atom')


const campJoin = {
  type: 'left',
  view: campView,
  condition() {
    this.on('log.camp_code', '=', 'atom_camps.camp_code')
  },
}

const bannerJoin = {
  type: 'left',
  view: bannerView,
  condition() {
    this.on('log.banner_code', '=', 'atom_banners.banner_code')
  },
}

module.exports = {
  name: 'ATOM Impressions',
  table: 'fusion_logs.impression_logs',
  owner: CU_ADVERTISER,
  columns: {
    camp_code: { category: CAT_NUMERIC },
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
    date: { category: CAT_DATE },
    fsa: {
      category: CAT_STRING,
      geo_type: 'ca-fsa',
      expression: 'postal_code AS fsa',
    },
    impressions: {
      category: CAT_NUMERIC,
      expression: 'count(*) AS impressions',
      isAggregate: true,
    },
    clicks: {
      category: CAT_NUMERIC,
      expression: 'count_if(click) AS clicks',
      isAggregate: true,
    },
    user_ip: {
      category: CAT_STRING,
      expression: 'substr(to_hex(sha256(cast(ip AS varbinary))), 1, 20) AS user_ip',
    },
    user_id: {
      category: CAT_STRING,
      expression: 'substr(to_hex(sha256(cast(user_guid AS varbinary))), 1, 20) AS user_id',
    },
    hh_id: {
      category: CAT_STRING,
      expression: 'substr(to_hex(sha256(cast(hh_id AS varbinary))), 1, 20) AS hh_id',
    },
    hh_fsa: {
      category: CAT_STRING,
      geo_type: 'ca-fsa',
    },
    os_id: { category: CAT_NUMERIC },
    os_name: {
      category: CAT_STRING,
      dependsOn: ['os_id'],
      viewExpression: 'atom_os.os_name',
      joins: [
        {
          type: 'left',
          view: osView,
          condition() {
            this.on('log.os_id', '=', 'atom_os.os_id')
          },
        },
      ],
    },
    browser_id: { category: CAT_NUMERIC },
    browser_name: {
      category: CAT_STRING,
      dependsOn: ['browser_id'],
      viewExpression: 'atom_browsers.browser_name',
      joins: [
        {
          type: 'left',
          view: browserView,
          condition() {
            this.on('log.browser_id', '=', 'atom_browsers.browser_id')
          },
        },
      ],
    },
    city: { category: CAT_STRING },
    banner_code: { category: CAT_NUMERIC },
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
          WHEN log.app_platform_id = 2 THEN 'iOs'
          ELSE 'Unknown/Browser'
        END
      `,
    },
    revenue: {
      category: CAT_NUMERIC,
      access: ACCESS_INTERNAL,
    },
    revenue_in_currency: {
      category: CAT_NUMERIC,
      access: ACCESS_INTERNAL,
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
    },
    cost_in_currency: {
      category: CAT_NUMERIC,
      access: ACCESS_INTERNAL,
    },
  },
}
