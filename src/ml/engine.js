/* eslint-disable no-await-in-loop */
const { gzip } = require('zlib')

const Cursor = require('pg-cursor')
const fs = require('fs')
const parquet = require('parquetjs')

const { mlPool, fdwConnectByName, newPGClientFromPoolConfig } = require('../util/db')
const { parseQueryTreeToEngine } = require('./parser')
const { QUERY_BUCKET, RESULTS_PART_SIZE_MB } = require('./constants')
const { useAPIErrorOptions } = require('../util/api-error')
const {
  queryWithCache,
  cacheTypes,
  pgWithCache,
  getFromS3Cache,
  putToS3Cache,
} = require('../util/cache')
const { getObjectHash } = require('./utils')
const { typeToPrqMap, PRQ_STRING } = require('./type')


const { apiError, getSetAPIError } = useAPIErrorOptions({ tags: { service: 'ql' } })

/**
 * Establishes connections with foreign databases
 * @param {PG.PoolClient} pgConnection PG connection to use to connect to the foreign db's
 * @param {Object.<string, string[]>} fdwConnections Map of view ID's and array of connection names
 * @param {number} [timeout] Connection timeout in seconds
 * @returns {Promise<undefined>}
 */
const establishFdwConnections = (pgConnection, fdwConnections, timeout) => {
  // remove duplicates
  const uniqueConnections = [...Object.values(fdwConnections).reduce((acc, connections) => {
    if (connections) {
      connections.forEach(c => acc.add(c))
    }
    return acc
  }, new Set())]
  return Promise.all(uniqueConnections.map(connectionName => fdwConnectByName(
    pgConnection,
    { connectionName, timeout },
  )))
}

// runs query with cache
// returns entire results
const executeQuery = async (
  whitelabelID, customerID, views, tree,
  { engine = 'pg', executionID, maxAge },
) => {
  if (engine !== 'pg') {
    throw apiError('Failed to execute the query', 500)
  }
  // get view queries
  const { viewQueries, fdwConnections } = Object.entries(views)
    .reduce((acc, [id, { query, fdwConnections }]) => {
      acc.viewQueries[id] = query
      acc.fdwConnections[id] = fdwConnections
      return acc
    }, { viewQueries: {}, fdwConnections: {} })
  const query = parseQueryTreeToEngine(tree, { engine, viewQueries, whitelabelID, customerID })
  // 30 minutes (subject to maxAge)
  const cacheOptions = { ttl: 1800, maxAge, type: cacheTypes.S3, bucket: QUERY_BUCKET }
  // instantiate PG connection to use for fdw + query (must be same)
  // client's application name must be specific to this execution so the pg pid can be
  // readily identified
  const pgClient = newPGClientFromPoolConfig(mlPool, {
    application_name: `ql-executor-${process.env.STAGE}${executionID ? `-${executionID}` : ''}`,
  // eslint-disable-next-line object-curly-newline
  })
  await pgClient.connect()
  try {
    // establish fdw connections
    await establishFdwConnections(pgClient, fdwConnections)
    // run query
    return await pgWithCache(query, [], pgClient, cacheOptions)
  } finally {
    pgClient.end()
  }
}

const readRowsFromCursor = (cursor, rowCount) => new Promise((resolve, reject) =>
  cursor.read(rowCount, (err, rows) => (err ? reject(err) : resolve(rows))))

const getGzipCompressionRatio = (json) => {
  const raw = Buffer.from(json, 'utf8')
  return new Promise((resolve, reject) => gzip(raw, (err, compressed) => {
    if (err) {
      return reject(err)
    }
    resolve(raw.length / compressed.length)
  }))
}

const makeResultsPartIter = (cursor, { partSizeBytes = 10000, cursorSizeRows = 50000 } = {}) => {
  let rawPartSizeBytes = 0 // based on compression ratio of first `cursorSizeRows` rows
  let buffer = []
  // know buffer is the part of the overall buffer for which we know the byte size
  let knownBufferSize = 0 // 1 char = 1 byte - assumes all ASCII chars
  let knownBufferNextIndex = 0
  let done = false
  return async () => {
    if (done) {
      return { done }
    }
    // init
    if (rawPartSizeBytes === 0) {
      buffer = await readRowsFromCursor(cursor, cursorSizeRows)
      const bufferJSON = JSON.stringify(buffer)
      const compressionRatio = await getGzipCompressionRatio(bufferJSON)
      rawPartSizeBytes = compressionRatio * partSizeBytes
      knownBufferSize = bufferJSON.length
      knownBufferNextIndex = buffer.length
    }
    // eslint-disable-next-line no-constant-condition
    while (true) {
      let currentSize = 0
      // skip known buffer if too small to make part
      if (knownBufferSize > rawPartSizeBytes) {
        for (let i = 0; i < knownBufferNextIndex; i++) {
          currentSize += JSON.stringify(buffer[i]).length + 1
          if (currentSize >= rawPartSizeBytes) {
            const rows = buffer.slice(0, i + 1)
            buffer = buffer.slice(i + 1)
            knownBufferSize -= currentSize
            knownBufferNextIndex -= i + 1
            return { rows, done }
          }
        }
      } else {
        currentSize = knownBufferSize
      }
      // explore unknown part of the buffer
      for (let i = knownBufferNextIndex; i < buffer.length; i++) {
        currentSize += JSON.stringify(buffer[i]).length + 1
        if (currentSize >= rawPartSizeBytes) {
          const rows = buffer.slice(0, i + 1)
          buffer = buffer.slice(i + 1)
          knownBufferSize = 0
          knownBufferNextIndex = 0
          return { rows, done }
        }
      }
      // buffer is too small, read more rows from the pg cursor
      const cursorRows = await readRowsFromCursor(cursor, cursorSizeRows)
      for (let i = 0; i < cursorRows.length; i++) {
        currentSize += JSON.stringify(cursorRows[i]).length + 1
        if (currentSize >= rawPartSizeBytes) {
          const rows = buffer.concat(cursorRows.slice(0, i + 1))
          buffer = cursorRows.slice(i + 1)
          knownBufferSize = 0
          knownBufferNextIndex = 0
          return { rows, done }
        }
      }
      // no more rows
      if (!cursorRows.length) {
        done = true
        return { rows: buffer, done: false }
      }
      // buffer + cursor rows too small -> becomes known buffer
      buffer = buffer.concat(cursorRows)
      knownBufferSize = currentSize
      knownBufferNextIndex = buffer.length
    }
  }
}

const convertToParquet = async (rows, schema) => {
  const writer = await parquet.ParquetWriter.openFile(schema, '/tmp/results.parquet')
  rows.forEach(row => writer.appendRow(row))
  writer.close()
  return fs.createReadStream('/tmp/results.parquet')
}

// runs query with cache
// calls cb with each results part
const executeQueryInStreamMode = async (
  whitelabelID, customerID, views, tree, columns, callback,
  { engine = 'pg', executionID, maxAge, toParquet = false },
) => {
  if (engine !== 'pg') {
    throw apiError('Failed to execute the query', 500)
  }
  // get view queries
  const { viewQueries, fdwConnections } = Object.entries(views)
    .reduce((acc, [id, { query, fdwConnections }]) => {
      acc.viewQueries[id] = query
      acc.fdwConnections[id] = fdwConnections
      return acc
    }, { viewQueries: {}, fdwConnections: {} })
  const query = parseQueryTreeToEngine(tree, { engine, viewQueries, whitelabelID, customerID })
  const promises = []
  let isCached = true
  // instantiate PG connection to use for fdw + query (must be same)
  // client's application name must be specific to this execution so the pg pid can be
  // readily identified
  const pgClientName = `ql-executor-${process.env.STAGE}${executionID ? `-${executionID}` : ''}`
  const pgClient = newPGClientFromPoolConfig(mlPool, { application_name: pgClientName })
  let cursor
  let getNextPart
  let schema
  if (toParquet) {
    const parquetSchema = columns.reduce((obj, [name, pgType]) => ({
      ...obj,
      [name]: { type: typeToPrqMap.get(pgType) } || PRQ_STRING,
    }), {})
    schema = new parquet.ParquetSchema(parquetSchema)
  }
  try {
    for (let i = 0; ; i += 1) {
      let rows
      if (isCached) {
        rows = await getFromS3Cache([query, i], { maxAge, bucket: QUERY_BUCKET })
        if (!rows) {
          if (i) {
            // no more rows
            break
          }
          // first part is undefined -> nothing in cache
          isCached = false
        }
      }
      if (!isCached) {
        // init
        if (!cursor) {
          await pgClient.connect()
          // establish fdw connections
          await establishFdwConnections(pgClient, fdwConnections)
          // init cursor
          cursor = pgClient.query(new Cursor(query))
          getNextPart = makeResultsPartIter(cursor, {
            partSizeBytes: RESULTS_PART_SIZE_MB * (2 ** 20),
            // to minimize memory footprint (buffer size), # rows fetched by cursor is
            // inversely proportional to # columns
            cursorSizeRows: Math.max(Math.ceil(50000 / (0.5 + (0.5 * columns.length))), 5000),
          })
        }
        // fetch next part
        const part = await getNextPart()
        if (part.done) {
          break
        }
        rows = part.rows
        // push to cache
        promises.push(putToS3Cache([query, i], rows, { bucket: QUERY_BUCKET }))
        if (!rows.length) {
          break
        }
      }
      if (toParquet && !Buffer.isBuffer(rows)) {
        rows = await convertToParquet(rows, schema)
      }
      // send rows to cb
      const res = callback(rows, i)
      if (res instanceof Promise) {
        promises.push(res)
      }
    }
  } finally {
    try {
      if (cursor) {
        await cursor.close()
      }
    } finally {
      pgClient.end()
    }
  }
  if (promises.length) {
    await Promise.all(promises)
  }
}

/**
 * Parses and validates query by running it with a limit of 0
 * @param {number} whitelabelID Whitelabel ID
 * @param {number} customerID Customer ID (agency ID)
 * @param {Object.<string, Object>} views Map of view ID's to query, columns...
 * @param {Object} tree Query tree
 * @param {string} [engine='pg'] One of 'pg' or 'trino'
 * @returns {Promise<Object>} Query and result column hashes along with result schema
 */
const validateQuery = async (whitelabelID, customerID, views, tree, engine = 'pg') => {
  if (engine !== 'pg') {
    throw apiError('Failed to validate the query', 500)
  }
  // get view queries
  const { viewQueries, fdwConnections } = Object.entries(views)
    .reduce((acc, [id, { query, fdwConnections }]) => {
      acc.viewQueries[id] = query
      acc.fdwConnections[id] = fdwConnections
      return acc
    }, { viewQueries: {}, fdwConnections: {} })
  const query = parseQueryTreeToEngine(
    tree,
    { engine, viewQueries, whitelabelID, customerID, limit: 0 },
  )

  const pgClient = await mlPool.connect()
  let fields
  try {
    // establish fdw connections
    await establishFdwConnections(pgClient, fdwConnections)
    // run query
    fields = await queryWithCache(
      [query, 'fields'],
      () => pgClient.query(query).then(({ fields }) => fields), // only cache fields
      { ttl: 86400, type: cacheTypes.REDIS }, // 1 day
    )
  } finally {
    pgClient.release()
  }

  // pg specific
  const columns = fields.map(({ name, dataTypeID }) => [name, dataTypeID])

  return {
    mlQueryHash: getObjectHash(tree.toQL({ keepParamRefs: false })),
    mlQueryColumnHash: getObjectHash(columns),
    mlQueryColumns: columns,
  }
}

// throws an error if the query cannot be parsed
// attaches queryHash, columnHash and columns to req
const validateQueryMW = async (req, _, next) => {
  try {
    // access has single customer
    const { whitelabel: [whitelabelID], customers: [customerID] } = req.access
    const { tree, views, engine } = req.ql

    // get query and column hashes + results schema and attach to req
    const values = await validateQuery(whitelabelID, customerID, views, tree, engine)
    Object.assign(req, values)
    next()
  } catch (err) {
    next(getSetAPIError(err, 'Failed to validate the query', 500))
  }
}

module.exports = {
  executeQuery,
  executeQueryInStreamMode,
  validateQuery,
  validateQueryMW,
  establishFdwConnections,
}

