const { createHash } = require('crypto')
const { gzip, gunzip } = require('zlib')
const { promisify } = require('util')

const { apiError } = require('../util/api-error')
const { s3 } = require('../util/aws')
const { client: redis } = require('../util/redis')


const S3_CACHE_BUCKET = 'ml-query-cache'
const gzipAsync = promisify(gzip)
const gunzipAsync = promisify(gunzip)
const getRedisAsync = promisify(redis.get).bind(redis)
const evalRedisAsync = promisify(redis.eval).bind(redis)

/**
 * Retrieves query results from redis cache
 * @param {string} hash Query hash
 * @param {number} maxAge Max age of query results in seconds. 0 means don't pull from the cache
 * while a negative value means return whatever is in the cache irrespective of age
 * @returns {Promise<string|integer|undefined>} Query results or undefined if cache miss
 */
const getFromRedisCache = async (hash, maxAge) => {
  if (maxAge === 0) {
    // invalidate cache
    return
  }
  const queryKey = `query:${hash}`
  if (maxAge < 0) {
    // disregard maxAge
    const res = getRedisAsync(queryKey)
    return res !== null ? res : undefined
  }
  const createdKey = `query:created:${hash}`
  const earliestCreated = Math.floor(Date.now() / 1000) - maxAge
  const res = await evalRedisAsync(`
    local created = tonumber(redis.call('GET', ARGV[2]))
    if not created or (created < tonumber(ARGV[3])) then
        return nil
    end
    return redis.call('GET', ARGV[1])
  `, 0, queryKey, createdKey, earliestCreated)
  return res !== null ? res : undefined
}

/**
 * Persists query results to redis cache
 * @param {string} hash Query hash
 * @param {string|integer} value Value to cache (objects must be serialized prior to invocation)
 * @param {number} ttl Cache TTL in seconds
 * @returns {Promise<boolean>} True if the value was cached, false otherwise
 */
const putToRedisCache = (hash, value, ttl) => new Promise((resolve, reject) => {
  if (ttl <= 0) {
    return resolve(false)
  }
  const queryKey = `query:${hash}`
  const createdKey = `query:created:${hash}`
  redis
    .multi()
    .set(queryKey, value, 'EX', ttl)
    .set(createdKey, Math.floor(Date.now() / 1000), 'EX', ttl)
    .exec((err, res) => {
      if (err) {
        return reject(err)
      }
      const commandErr = res.find(commandRes => commandRes instanceof Error)
      if (commandErr) {
        return reject(commandErr)
      }
      resolve(true)
    })
})

/**
 * Pulls query results from cache if available, otherwise, executes query and persists
 * results to cache
 * @param {string} sql Query string
 * @param {Object|Array} bindings Query variable bindings
 * @param {function} runQuery Callback to invoke in order to fetch SQL results (must
 * resolve to an object)
 * @param {Object} options
 * @param {number} [options.maxAge=600] Max age (in seconds) of SQL results when pulling from the
 * cache (typically same as ttl)
 * @param {number} [options.ttl=600] TTL (in seconds) of the cached results
 * @returns {Promise<Object>} Query results
 */
const queryWithCache = async (sql, bindings, runQuery, { maxAge = 600, ttl = 600 } = {}) => {
  // compute cache hash
  const hash = createHash('sha256')
    .update(JSON.stringify({ sql, bindings }))
    .digest('hex')

  // get from cache
  try {
    const cachedValue = await getFromRedisCache(hash, maxAge)
    if (cachedValue) {
      return JSON.parse(cachedValue)
    }
  } catch (err) {
    // don't throw - will execute sql query if cache error
    console.log('Error retrieving query results from cache', err.message)
  }

  // run query
  const queryValue = await runQuery()

  // set cache
  try {
    await putToRedisCache(hash, JSON.stringify(queryValue), ttl)
  } catch (err) {
    // don't throw
    console.log('Error persisting query results to cache', err.message)
  }

  return queryValue
}

/**
 * Pulls query results from cache if available, otherwise, executes query and persists
 * results to cache
 * @param {Knex.QueryBuilder} knexQuery Knex QueryBuilder object
 * @param {Object} options
 * @param {number} [options.maxAge=600] Max age (in seconds) of SQL results when pulling from the
 * cache (typically same as ttl)
 * @param {number} [options.ttl=600] TTL (in seconds) of the cached results
 * @returns {Promise<Object>} Query results
 */
const knexWithCache = async (knexQuery, options) => {
  const { sql, bindings } = knexQuery.toSQL()
  const runQuery = () => knexQuery
  return queryWithCache(sql, bindings, runQuery, options)
}

/**
 * Pulls query results from cache if available, otherwise, executes query and persists
 * results to cache
 * @param {string} sql Query string
 * @param {Object|Array} bindings Query variable bindings
 * @param {pg.Pool|pg.Client} pool Node-pg pool or client
 * @param {Object} options
 * @param {number} [options.maxAge=600] Max age (in seconds) of SQL results when pulling from the
 * cache (typically same as ttl)
 * @param {number} [options.ttl=600] TTL (in seconds) of the cached results
 * @returns {Promise<Object>} Query results
 */
const pgWithCache = (sql, bindings, pool, options) => {
  const runQuery = () => pool.query(sql, bindings)
  return queryWithCache(sql, bindings, runQuery, options)
}

/**
 * Stores key value pair in S3 cache
 * @param {string} key Cache key
 * @param {string} value Cache value. Objects need be stringified before being
 * passed to the function
 */
const putToS3Cache = async (key, value) => {
  // const compressedBody = await gzipAsync(JSON.stringify(body))
  const compressedBody = await gzipAsync(value)
  await s3.putObject({
    Bucket: S3_CACHE_BUCKET,
    Key: `${key}.json.gz`,
    Body: compressedBody,
    ContentType: 'application/json',
    ContentEncoding: 'gzip',
  }).promise()
}

/**
 * Retrieves value from S3 cache given a key
 * @param {string} key Cache key
 * @param {number} [maxAge=0] If greater than 0, will return undefined if the cache was
 * last updated more than maxAge seconds ago
 * @returns {string|undefined} Value in cache or undefined if not found
 */
const getFromS3Cache = async (key, maxAge = 0) => {
  const params = {
    Bucket: S3_CACHE_BUCKET,
    Key: `${key}.json.gz`,
  }
  if (maxAge > 0) {
    params.IfModifiedSince = Math.floor(Date.now() / 1000) - maxAge
  }
  try {
    const { Body } = await s3.getObject(params).promise()
    const uncompressedValue = await gunzipAsync(Body)
    return uncompressedValue.toString('utf8')
  } catch (err) {
    if (['NoSuchKey', 'NotModified'].includes(err.code)) {
      return undefined
    }
    throw err
  }
}

// Express middleware
const getResFromS3Cache = async (req, res, next) => {
  try {
    // default cache is 10 minutes
    const { access, body: { query }, query: { cache = 600 } } = req
    if (typeof cache !== 'number') {
      throw apiError('query parameter "cache" must be of type number', 400)
    }
    if (cache === -2) {
      // do not cache
      return next()
    }

    // cache key is a blend of the user's access and the query
    // i.e. users with different access permissions but submitting the same query
    // will not be sharing the same cache (for now)
    req.mlCacheKey = createHash('sha256').update(JSON.stringify({ access, query })).digest('hex')
    if (cache === 0) {
      // refresh, do not pull from cache
      return next()
    }

    const cachedRes = await getFromS3Cache(req.mlCacheKey, cache)
    if (cachedRes) {
      // cachedRes is a JSON string
      return res.status(200).type('application/json').send(cachedRes)
    }

    next()
  } catch (err) {
    next(err)
  }
}

module.exports = {
  putToS3Cache,
  getResFromS3Cache,
  queryWithCache,
  knexWithCache,
  pgWithCache,
}
