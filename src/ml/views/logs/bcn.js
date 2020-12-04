const { CAT_STRING, CAT_NUMERIC, CAT_JSON, CAT_DATE } = require('../../type')
const { CU_AGENCY } = require('./constants')
const { osView, browserView } = require('./atom')
const { campView } = require('./locus')


module.exports = {
  name: 'LOCUS Beacons',
  table: 'fusion_logs.beacon_logs',
  owner: CU_AGENCY,
  columns: {
    camp_code: { category: CAT_NUMERIC },
    camp_name: {
      category: CAT_STRING,
      dependsOn: ['camp_code'],
      viewExpression: 'locus_camps.camp_name',
      joins: [
        {
          type: 'left',
          view: campView,
          condition() {
            this.on('log.camp_code', '=', 'locus_camps.camp_code')
          },
        },
      ],
    },
    date: { category: CAT_DATE },
    fsa: {
      category: CAT_STRING,
      geo_type: 'ca-fsa',
      expression: 'postal_code AS fsa',
    },
    beacon_id: { category: CAT_NUMERIC },
    impressions: {
      category: CAT_NUMERIC,
      expression: 'count(*) AS impressions',
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
    vendor: { category: CAT_STRING },
    type: { category: CAT_STRING },
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
