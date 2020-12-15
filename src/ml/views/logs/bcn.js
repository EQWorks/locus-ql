const { CAT_STRING, CAT_NUMERIC, CAT_JSON, CAT_DATE } = require('../../type')
const { CU_AGENCY, ACCESS_PRIVATE } = require('./constants')
const { pgViews } = require('./pg-views')


module.exports = {
  name: 'LOCUS Beacons',
  table: 'fusion_logs.beacon_logs',
  partitions: 4,
  owner: CU_AGENCY,
  columns: {
    camp_code: {
      category: CAT_NUMERIC,
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
    fsa: {
      category: CAT_STRING,
      geo_type: 'ca-fsa',
      expression: 'postal_code AS fsa',
    },
    beacon_id: {
      category: CAT_NUMERIC,
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
      expression: 'count(*) AS impressions',
      viewExpression: 'SUM(COALESCE(log.impressions, 0))',
      isAggregate: true,
      inFastViews: [pgViews.LOCUS_BEACON_HISTORY],
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
      viewExpression: 'log.hh_id',
    },
    household_fsa: {
      category: CAT_STRING,
      geo_type: 'ca-fsa',
      expression: 'hh_fsa',
      viewExpression: 'hh_fsa AS',
    },
    os_id: {
      category: CAT_NUMERIC,
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
    city: { category: CAT_STRING },
    connection_type: {
      category: CAT_NUMERIC,
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
      inFastViews: [pgViews.LOCUS_BEACONS, pgViews.LOCUS_BEACON_HISTORY],
    },
    type: {
      category: CAT_STRING,
      inFastViews: [pgViews.LOCUS_BEACONS],
    },
    referrer: { category: CAT_STRING },
    content: { category: CAT_JSON },
    // TODO: expose default fields once convention agreed upon
    // example:
    // content_ga_id: {
    //   category: CAT_STRING,
    //   dependsOn: ['content'],
    //   viewExpression: "log.content->'ga_id'",
    // },
  },
}
