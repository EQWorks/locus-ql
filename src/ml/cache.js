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

const cacheTypes = {
  REDIS: 1,
  S3: 2,
}

/**
 * Retrieves query results from redis cache
 * @param {string} key Cache key
 * @param {number} maxAge Max age of query results in seconds. 0 means don't pull from the cache
 * while a negative value means return whatever is in the cache irrespective of age
 * @param {boolean} [gzip=true] Whether or not the value in cache is compressed
 * @returns {Promise<string|integer|undefined>} Query results or undefined if cache miss
 */
const getFromRedisCache = async (key, maxAge, gzip = true) => {
  if (maxAge === 0) {
    // invalidate cache
    return
  }
  const prefix = gzip ? 'qc' : 'q'
  const valueKey = gzip ? Buffer.from(`${prefix}:${key}`, 'utf8') : `${prefix}:${key}`
  let res
  if (maxAge < 0) {
    // disregard maxAge
    res = await getRedisAsync(valueKey)
  } else {
    const createdKey = `${prefix}:created:${key}`
    const earliestCreated = Math.floor(Date.now() / 1000) - maxAge
    res = await evalRedisAsync(`
      local created = tonumber(redis.call('GET', ARGV[2]))
      if not created or (created < tonumber(ARGV[3])) then
          return nil
      end
      return redis.call('GET', ARGV[1])
    `, 0, valueKey, createdKey, earliestCreated)
  }
  if (res === null) {
    return
  }
  if (gzip) {
    const uncompressedValue = await gunzipAsync(res)
    return uncompressedValue.toString('utf8')
  }
  return res
}

/**
 * Persists query results to redis cache
 * @param {string} key Cache key
 * @param {string|integer} value Value to cache (objects must be serialized prior to invocation)
 * @param {number} ttl Cache TTL in seconds
 * @param {boolean} [gzip=true] Whether or not the value in cache is compressed
 * @returns {Promise<boolean>} True if the value was cached, false otherwise
 */
const putToRedisCache = async (key, value, ttl, gzip = true) => {
  if (ttl <= 0) {
    return false
  }
  const prefix = gzip ? 'qc' : 'q'
  const valueKey = `${prefix}:${key}`
  const createdKey = `${prefix}:created:${key}`
  const compressedValue = gzip ? await gzipAsync(value) : value
  return new Promise((resolve, reject) => {
    redis
      .multi()
      .set(valueKey, compressedValue, 'EX', ttl)
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
}

/**
 * Retrieves value from S3 cache given a key
 * @param {string} key Cache key
 * @param {number} maxAge If greater than 0, will return undefined if the cache was
 * last updated more than maxAge seconds ago
 * @returns {string|undefined} Value in cache or undefined if not found
 */
const getFromS3Cache = async (key, maxAge) => {
  if (maxAge === 0) {
    // invalidate cache
    return
  }
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
      return
    }
    throw err
  }
}

/**
 * Stores key value pair in S3 cache
 * @param {string} key Cache key
 * @param {string} value Cache value. Objects need be stringified before being
 * passed to the function
 */
const putToS3Cache = async (key, value) => {
  const compressedValue = await gzipAsync(value)
  await s3.putObject({
    Bucket: S3_CACHE_BUCKET,
    Key: `${key}.json.gz`,
    Body: compressedValue,
    ContentType: 'application/json',
    ContentEncoding: 'gzip',
  }).promise()
}

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
 * @param {number} [options.type=type.REDIS] Storage type
 * @param {boolean} [options.gzip=true] Whether or not the value in cache is compressed
 * @returns {Promise<Object>} Query results
 */
const queryWithCache = async (
  sql,
  bindings,
  runQuery,
  { maxAge = 600, ttl = 600, type = cacheTypes.REDIS, gzip = true } = {},
) => {
  // compute cache hash
  const hash = createHash('sha256')
    .update(JSON.stringify({ sql, bindings }))
    .digest('hex')

  // get from cache
  try {
    let cachedValue
    switch (type) {
      case cacheTypes.REDIS:
        cachedValue = await getFromRedisCache(hash, Math.min(maxAge, ttl), gzip)
        break
      case cacheTypes.S3:
        cachedValue = await getFromS3Cache(hash, Math.min(maxAge, ttl))
        break
      default:
        throw new Error('Cache type not supported')
    }
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
    const cacheValue = JSON.stringify(queryValue)
    switch (type) {
      case cacheTypes.REDIS:
        await putToRedisCache(hash, cacheValue, ttl, gzip)
        break
      case cacheTypes.S3:
        await putToS3Cache(hash, cacheValue)
        break
      default:
        throw new Error('Cache type not supported')
    }
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
 * @param {number} [options.type=type.REDIS] Storage type
 * @param {boolean} [options.gzip=true] Whether or not the value in cache is compressed
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
 * @param {number} [options.type=type.REDIS] Storage type
 * @param {boolean} [options.gzip=true] Whether or not the value in cache is compressed
 * @returns {Promise<Object>} Query results
 */
const pgWithCache = (sql, bindings, pool, options) => {
  const runQuery = () => pool.query(sql, bindings)
  return queryWithCache(sql, bindings, runQuery, options)
}

// Express middleware
const getResFromS3Cache = async (req, res, next) => {
  try {
    // default cache is 10 minutes
    const { access, body: { query }, query: { cache: maxAge = 600 } } = req
    if (typeof maxAge !== 'number') {
      throw apiError('query parameter "cache" must be of type number', 400)
    }
    if (maxAge === -2) {
      // do not cache
      return next()
    }

    // cache key is a blend of the user's access and the query
    // i.e. users with different access permissions but submitting the same query
    // will not be sharing the same cache (for now)
    req.mlCacheKey = createHash('sha256').update(JSON.stringify({ access, query })).digest('hex')
    if (maxAge === 0) {
      // refresh, do not pull from cache
      return next()
    }

    const cachedRes = await getFromS3Cache(req.mlCacheKey, maxAge)
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
  cacheTypes,
}
