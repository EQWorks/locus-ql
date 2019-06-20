const { knex } = require('../../util/db')
const { typeToCatMap } = require('../type')
const apiError = require('../../util/api-error')


const CONNECTION_TABLE = 'ext_conn.connections'
const SETS_TABLE = 'ext_conn.sets'

module.exports.getView = async (access, reqViews, { conn_id }) => {
  const viewID = `ext_${conn_id}`

  let { whitelabel, customers } = access
  if (whitelabel !== -1) {
    whitelabel = whitelabel[0]
  }
  if (customers !== -1) {
    customers = customers[0]
  }

  // check access to ext connection table and get table name
  const connections = await knex(CONNECTION_TABLE)
    .where({ id: conn_id })
    .whereRaw('? in (whitelabel, -1)', whitelabel)
    .whereRaw('? in (customer, -1)', customers)

  if (connections.length === 0) {
    throw apiError('Connection not found ', 403)
  }

  const [{ dest: { table, schema } }] = connections
  reqViews[viewID] = knex.raw(`
    (
      SELECT *
      FROM ${schema}.${table}
    ) as ${viewID}
  `)
}

module.exports.listViews = async (access) => {
  const { whitelabel, customers } = access

  const connQuery = knex(CONNECTION_TABLE)
  connQuery.column([
    `${CONNECTION_TABLE}.id`,
    'set_id',
    'type',
    `${CONNECTION_TABLE}.name`,
    'columns',
  ])
  connQuery.innerJoin(SETS_TABLE, `${SETS_TABLE}.id`, 'set_id')
  if (whitelabel !== -1) {
    connQuery.where({ whitelabel: whitelabel[0] })
    if (customers !== -1) {
      connQuery.where({ agencyid: customers[0] })
    }
  }

  const connections = await connQuery

  return connections.map(({ id, set_id, type, name, columns }) => {
    // insert column type category
    Object.entries(columns).forEach(([key, column]) => {
      column.key = key
      column.category = typeToCatMap.get(column.type)
    })

    return {
      name,
      set_id,
      type,
      view: {
        type: 'ext',
        id: `ext_${id}`,
        conn_id: id,
      },
      columns,
    }
  })
}
