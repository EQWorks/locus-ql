const config = require('../../config')

const { Pool } = require('pg')
const Knex = require('knex')


const pool = new Pool({ ...config.pg, max: 1 })
const mapPool = new Pool({ ...config.mappingPg, max: 1 })
const atomPool = new Pool({ ...config.pgAtom, max: 1 })
const knex = Knex({
  client: 'pg',
  connection: config.pg,
  debug: ['1', 'true'].includes((process.env.DEBUG || '').toLowerCase()),
})
const mapKnex = Knex({
  client: 'pg',
  connection: config.mappingPg,
  debug: ['1', 'true'].includes((process.env.DEBUG || '').toLowerCase()),
})

// dblink connect functions (foreign-data wrapper)
// https://www.postgresql.org/docs/9.6/dblink.html
const fdwConnect = async ({
  connectionName = 'locus_atom_fdw',
  creds = config.pgAtomRead, // use read replica
  timeout = 30, // Maximum wait for connection, in seconds (write as a decimal integer string).
  // Zero or not specified means wait indefinitely. It is not recommended to
  // use a timeout of less than 2 seconds.
} = {}) => {
  try {
    const { user, password, host, port, database } = creds
    const applicationName = process.env.PGAPPNAME || `firstorder-${process.env.STAGE || 'dev'}`
    const { rows: [{ dblink_connect }] } = await knex.raw(`
      SELECT dblink_connect(
        ?,
          'postgresql://' || ? || ':' || ? || '@' || ? || ':' || ? || '/' || ?
          || '\\?connect_timeout=' || ?
          || '&application_name=' || ?
          || '&options=-csearch_path%3D'
      )
    `, [connectionName, user, password, host, port, database, timeout, applicationName])
    if (dblink_connect !== 'OK') {
      throw new Error('Connection error')
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

const fdwDisconnect = async (connectionName = 'locus_atom_fdw') => {
  try {
    const { rows: [{ dblink_disconnect }] } = await knex.raw(`
      SELECT dblink_disconnect(?)
    `, [connectionName])
    if (dblink_disconnect !== 'OK') {
      throw new Error('Disconnection error')
    }
  } catch (err) {
    console.log('disconnect error', err)
    if (err.code && err.code === '08003') {
      // connection does not exist (e.g. already disconnected)
      return
    }
    throw err
  }
}

module.exports = { pool, mapPool, atomPool, knex, mapKnex, fdwConnect, fdwDisconnect }
