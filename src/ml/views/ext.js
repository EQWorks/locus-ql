/* eslint-disable no-use-before-define */

const { knex, pool } = require('../../util/db')
const { typeToCatMap } = require('../type')
const apiError = require('../../util/api-error')
const { knexWithCache, pgWithCache } = require('../cache')


const CONNECTION_TABLE = 'ext_conn.connections'
const SETS_TABLE = 'ext_conn.sets'

const getView = async (access, reqViews, reqViewColumns, { conn_id }) => {
  const viewID = `ext_${conn_id}`
  const { whitelabel, customers } = access

  // check access to ext connection table and get table name
  const connections = await knexWithCache(
    knex(CONNECTION_TABLE)
      .where({ id: conn_id })
      .where((builder) => {
        if (whitelabel === -1) {
          return
        }
        builder.whereRaw('connections.whitelabel = ANY (?)', [whitelabel])
        if (customers !== -1) {
          builder.whereRaw('connections.customer = ANY (?)', [customers])
        }
      }),
    { ttl: 600 }, // 10 minutes
  )

  if (connections.length === 0) {
    throw apiError('Connection not found', 403)
  }

  // inject view columns
  const viewMeta = await listViews({ access, filter: { conn_id } })
  reqViewColumns[viewID] = (viewMeta[0] || {}).columns

  // inject view
  const [{ dest: { table, schema } }] = connections
  reqViews[viewID] = knex.raw(`
    (
      SELECT *
      FROM ${schema}."${table}"
    ) as ${viewID}
  `)
}

const listViews = async ({ access, filter: { conn_id } = {}, inclMeta = true }) => {
  const { whitelabel, customers } = access

  const query = {
    text: `
      SELECT
        c.id,
        c.set_id,
        c.type,
        c.name,
        s.columns,
        pc.reltuples AS records
      FROM ${CONNECTION_TABLE} AS c
      INNER JOIN ${SETS_TABLE} AS s ON s.id = c.set_id
      INNER JOIN pg_class AS pc
        ON pc.relname = c.dest->>'table'
      -- this is to ensure filter on schema name as well
      INNER JOIN pg_catalog.pg_namespace AS n
        ON n.oid = pc.relnamespace
        AND n.nspname = c.dest->>'schema'
        AND pc.relkind = 'r'
        AND pc.reltuples > 0
      WHERE c.last_sync IS NOT NULL
        AND c.is_syncing = '0'
    `,
    values: [],
  }

  if (conn_id) {
    query.values.push(conn_id)
    query.text = `
      ${query.text}
      AND c.id = $${query.values.length}
    `
  }

  if (whitelabel !== -1) {
    query.values.push(whitelabel)
    query.text = `
      ${query.text}
      AND c.whitelabel = ANY ($${query.values.length})
    `
    if (customers !== -1) {
      query.values.push(customers)
      query.text = `
        ${query.text}
        AND c.customer = ANY ($${query.values.length})
      `
    }
  }

  const { rows: connections } = await pgWithCache(
    query.text,
    query.values,
    pool,
    { maxAge: 600 }, // 10 minutes
  )

  return connections.map(({ id, set_id, type, name, columns }) => {
    // TODO: remove 'columns' -> use listView() to get full view
    // insert column type category
    Object.entries(columns).forEach(([key, column]) => {
      column.key = key
      column.category = typeToCatMap.get(column.type)
    })

    const view = {
      name,
      set_id,
      type,
      view: {
        type: 'ext',
        id: `ext_${id}`,
        conn_id: id,
      },
    }
    if (inclMeta) {
      view.columns = columns
    }
    return view
  })
}

const listView = async (access, viewID) => {
  const [, idStr] = viewID.match(/^ext_(\d+)$/) || []
  // eslint-disable-next-line radix
  const conn_id = parseInt(idStr, 10)
  if (!conn_id) {
    throw apiError(`Invalid view: ${viewID}`, 403)
  }
  const { whitelabel, customers } = access

  const query = {
    text: `
      SELECT
        c.id,
        c.set_id,
        c.type,
        c.name,
        s.columns,
        pc.reltuples AS records
      FROM ${CONNECTION_TABLE} AS c
      INNER JOIN ${SETS_TABLE} AS s ON s.id = c.set_id
      INNER JOIN pg_class AS pc
        ON pc.relname = c.dest->>'table'
      -- this is to ensure filter on schema name as well
      INNER JOIN pg_catalog.pg_namespace AS n
        ON n.oid = pc.relnamespace
        AND n.nspname = c.dest->>'schema'
        AND pc.relkind = 'r'
        AND pc.reltuples > 0
      WHERE c.last_sync IS NOT NULL
        AND c.is_syncing = '0'
        AND c.id = $1
    `,
    values: [conn_id],
  }

  if (whitelabel !== -1) {
    query.values.push(whitelabel)
    query.text = `
      ${query.text}
      AND c.whitelabel = ANY ($${query.values.length})
    `
    if (customers !== -1) {
      query.values.push(customers)
      query.text = `
        ${query.text}
        AND c.customer = ANY ($${query.values.length})
      `
    }
  }

  const { rows: [connection = {}] } = await pgWithCache(
    query.text,
    query.values,
    pool,
    { ttl: 600 }, // 10 minutes
  )
  const { set_id, type, name, columns } = connection

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
      id: `ext_${conn_id}`,
      conn_id,
    },
    columns,
  }
}

module.exports = {
  getView,
  listViews,
  listView,
}
