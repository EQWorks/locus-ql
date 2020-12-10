const { CAT_STRING, CAT_NUMERIC, CAT_JSON, CAT_DATE } = require('../../type')
const { CU_AGENCY } = require('./constants')
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
      joins: [
        {
          type: 'left',
          view: pgViews.LOCUS_CAMPS,
          condition() {
            this.on('log.camp_code', '=', 'locus_camps.camp_code')
          },
        },
      ],
    },
    date: {
      category: CAT_DATE,
      inFastViews: [pgViews.LOCUS_BEACON_HISTORY],
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
      joins: [
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
      inFastViews: [pgViews.ATOM_OS],
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
      inFastViews: [pgViews.ATOM_BROWSERS],
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
    city: { category: CAT_STRING },
    vendor: {
      category: CAT_STRING,
      inFastViews: [pgViews.LOCUS_BEACONS, pgViews.LOCUS_BEACON_HISTORY],
    },
    type: {
      category: CAT_STRING,
      inFastViews: [pgViews.LOCUS_BEACONS],
    },
    content: { category: CAT_JSON },
    // TODO: expose default fields once convention agreed upon
    // example:
    // content_ga_id: {
    //   category: CAT_STRING,
    //   dependsOn: ['content'],
    //   viewExpression: "log.content->'ga_id' AS content_ga_id",
    // },
  },
}
