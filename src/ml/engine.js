const { mlPool, fdwConnectByName, newPGClientFromPoolConfig } = require('../util/db')
const { parseQueryTreeToEngine } = require('./parser')
const { QUERY_BUCKET } = require('./constants')
const { useAPIErrorOptions } = require('../util/api-error')
const { queryWithCache, cacheTypes, pgWithCache } = require('../util/cache')
const { getObjectHash } = require('./utils')


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
    application_name: `ql-executor-${process.env.STAGE}-${executionID ? `-${executionID}` : ''}`,
  // eslint-disable-next-line object-curly-newline
  })
  await pgClient.connect()
  try {
    // establish fdw connections
    await establishFdwConnections(pgClient, fdwConnections)
    // run query
    return pgWithCache(query, [], pgClient, cacheOptions)
  } finally {
    pgClient.release()
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
  console.log(query)

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
    mlQueryHash: getObjectHash(tree.toQL({ keepParamRefs: !tree.hasParameterValues() })),
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
  validateQuery,
  validateQueryMW,
  establishFdwConnections,
}

