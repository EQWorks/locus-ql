const { Pool, types } = require('pg')
const Knex = require('knex')

const config = require('../../config')


const applicationName = process.env.PGAPPNAME || `firstorder-${process.env.STAGE || 'dev'}`
const pool = new Pool({ ...config.pg, max: 1, application_name: applicationName })
const mlPool = new Pool({ ...config.pgML, max: 1, application_name: applicationName })
const mapPool = new Pool({ ...config.mappingPg, max: 1, application_name: applicationName })
const atomPool = new Pool({ ...config.pgAtom, max: 1, application_name: applicationName })
const knex = Knex({
  client: 'pg',
  connection: { ...config.pg, application_name: applicationName },
  pool: { min: 1, max: 1 },
  debug: ['1', 'true'].includes((process.env.DEBUG || '').toLowerCase()),
})
const mlKnex = Knex({
  client: 'pg',
  connection: { ...config.pgML, application_name: applicationName },
  pool: { min: 1, max: 1 },
  debug: ['1', 'true'].includes((process.env.DEBUG || '').toLowerCase()),
})
const mapKnex = Knex({
  client: 'pg',
  connection: { ...config.mappingPg, application_name: applicationName },
  pool: { min: 1, max: 1 },
  debug: ['1', 'true'].includes((process.env.DEBUG || '').toLowerCase()),
})

// register pg parsers
types.setTypeParser(types.builtins.NUMERIC, val => Number(val))

// dblink connect functions (foreign-data wrapper)
// https://www.postgresql.org/docs/9.6/dblink.html
const ATOM_READ_FDW_CONNECTION = 'locus_atom_fdw'
const MAPS_FDW_CONNECTION = 'locus_maps_fdw'

const fdwConnect = async (
  pgConnection,
  {
    connectionName = ATOM_READ_FDW_CONNECTION,
    creds = config.pgAtomRead, // use read replica
    timeout = 30, // Maximum wait for connection, in seconds (write as a decimal integer string).
    // Zero or not specified means wait indefinitely. It is not recommended to
    // use a timeout of less than 2 seconds.
  } = {},
) => {
  try {
    const { user, password, host, port, database } = creds
    const { rows: [{ dblink_connect }] } = await knex
      .raw(
        `
          SELECT dblink_connect(
            ?,
              'postgresql://' || ? || ':' || ? || '@' || ? || ':' || ? || '/' || ?
              || '\\?connect_timeout=' || ?
              || '&application_name=' || ?
              || '&options=-csearch_path%3D'
          )
        `,
        [connectionName, user, password, host, port, database, timeout, applicationName],
      )
      .connection(pgConnection)
    if (dblink_connect !== 'OK') {
      throw new Error(`Connection error for ${connectionName}`)
    }
  } catch (err) {
    if (err.code && err.code === '42710') {
      // already connected
      return
    }
    // don't return actual err as it may contain login credentials as bindings
    throw new Error(`Connection error for ${connectionName}`)
  }
}

const fdwDisconnect = async (pgConnection, connectionName = ATOM_READ_FDW_CONNECTION) => {
  try {
    const { rows: [{ dblink_disconnect }] } = await knex
      .raw('SELECT dblink_disconnect(?)', [connectionName])
      .connection(pgConnection)
    if (dblink_disconnect !== 'OK') {
      throw new Error(`Disconnection error for ${connectionName}`)
    }
  } catch (err) {
    console.log(`Disconnection error for ${connectionName}`, err)
    if (err.code && err.code === '08003') {
      // connection does not exist (e.g. already disconnected)
      return
    }
    throw err
  }
}

const fdwConnectByName = (pgConnection, { connectionName, timeout }) => {
  let creds
  switch (connectionName) {
    case ATOM_READ_FDW_CONNECTION:
      creds = config.pgAtom
      break
    case MAPS_FDW_CONNECTION:
      creds = config.mappingPg
      break
    default:
      creds = config.pgAtom
  }
  return fdwConnect(pgConnection, { connectionName, timeout, creds })
}

// converts a knex.QueryBuilder into a knex.Raw
// useful to expose the underlying pool's response
const knexBuilderToRaw = (builder) => {
  const { client, _connection } = builder
  const { sql, bindings } = builder.toSQL()
  const raw = client.raw(sql, bindings)
  if (_connection) {
    raw.connection(_connection)
  }
  return raw
}

module.exports = {
  pool,
  mlPool,
  mapPool,
  atomPool,
  knex,
  mlKnex,
  mapKnex,
  fdwConnect,
  fdwDisconnect,
  ATOM_READ_FDW_CONNECTION,
  MAPS_FDW_CONNECTION,
  fdwConnectByName,
  knexBuilderToRaw,
}
