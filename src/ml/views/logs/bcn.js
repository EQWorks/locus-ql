const { CAT_STRING, CAT_NUMERIC, CAT_JSON, CAT_DATE } = require('../../type')
const { geometryTypes } = require('../../parser/src')
const { CU_AGENCY, ACCESS_PRIVATE } = require('./constants')
const { pgViews } = require('./pg-views')
const { viewCategories } = require('../taxonomies')


module.exports = {
  name: 'LOCUS Beacons',
  table: 'fusion_logs.beacon_logs',
  category: viewCategories.LOCUS_BEACONS,
  owner: CU_AGENCY,
  columns: {
    camp_code: {
      category: CAT_NUMERIC,
      pgType: 'int',
      inFastViews: [pgViews.LOCUS_CAMPS, pgViews.LOCUS_BEACON_HISTORY],
    },
    camp_name: {
      category: CAT_STRING,
      dependsOn: ['camp_code'],
      viewExpression: 'locus_camps.camp_name',
      viewJoins: [
        {
          type: 'left',
          view: pgViews.LOCUS_CAMPS,
          condition() {
            this.on('log.camp_code', '=', 'locus_camps.camp_code')
          },
        },
      ],
    },
    _date: {
      category: CAT_DATE,
      access: ACCESS_PRIVATE,
      inFastViews: [pgViews.LOCUS_BEACON_HISTORY],
    },
    _hour: {
      category: CAT_NUMERIC,
      access: ACCESS_PRIVATE,
      inFastViews: [pgViews.LOCUS_BEACON_HISTORY],
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
      geo_type: geometryTypes.CA_FSA,
      viewExpression: "substring(log._postal_code from '^[A-Z]\\d[A-Z]$')",
    },
    geo_us_postalcode: {
      category: CAT_NUMERIC,
      dependsOn: ['_postal_code'],
      // geo_type: 'us-postalcode',
      viewExpression: "substring(log._postal_code from '^\\d{5}$')::int",
    },
    beacon_id: {
      category: CAT_NUMERIC,
      pgType: 'int',
      inFastViews: [pgViews.LOCUS_BEACONS],
    },
    beacon_name: {
      category: CAT_STRING,
      dependsOn: ['beacon_id'],
      viewExpression: 'locus_beacons.beacon_name',
      viewJoins: [
        {
          type: 'left',
          view: pgViews.LOCUS_BEACONS,
          condition() {
            this.on('log.beacon_id', '=', 'locus_beacons.beacon_id')
          },
        },
      ],
    },
    impressions: {
      category: CAT_NUMERIC,
      pgType: 'int',
      expression: 'count(*)',
      viewExpression: 'SUM(COALESCE(log.impressions, 0))',
      isAggregate: true,
      inFastViews: [pgViews.LOCUS_BEACON_HISTORY],
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
      geo_type: geometryTypes.CA_FSA,
      expression: 'hh_fsa',
    },
    os_id: {
      category: CAT_NUMERIC,
      pgType: 'smallint',
      expression: 'COALESCE(os_id, 1092)',
      inFastViews: [pgViews.ATOM_OS],
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
      inFastViews: [pgViews.ATOM_BROWSERS],
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
    },
    country: {
      category: CAT_STRING,
      dependsOn: ['city'],
      viewExpression: 'substring(log.city for 2)',
    },
    geo_ca_province: {
      category: CAT_STRING,
      geo_type: geometryTypes.CA_PROVINCE,
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
      geo_type: geometryTypes.CA_CITY,
      dependsOn: ['city'],
      viewExpression: "upper(substring(log.city from '^CA\\$[A-Z]{2}\\$(.*)$'))",
    },
    geo_us_city: {
      category: CAT_STRING,
      // geo_type: 'us-city',
      dependsOn: ['city'],
      viewExpression: "upper(substring(log.city from '^US\\$[A-Z]{2}\\$(.*)$'))",
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
    vendor: {
      category: CAT_STRING,
      pgType: 'varchar(255)',
      inFastViews: [pgViews.LOCUS_BEACONS, pgViews.LOCUS_BEACON_HISTORY],
    },
    type: {
      category: CAT_STRING,
      pgType: 'varchar(20)',
      inFastViews: [pgViews.LOCUS_BEACONS],
    },
    referrer: {
      category: CAT_STRING,
      pgType: 'text',
    },
    content: {
      category: CAT_JSON,
      pgType: 'jsonb',
    },
    // TODO: expose default fields once convention agreed upon
    // example:
    // content_ga_id: {
    //   category: CAT_STRING,
    //   dependsOn: ['content'],
    //   viewExpression: "log.content->'ga_id'",
    // },
  },
}
