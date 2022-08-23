const { createHash } = require('crypto')
const { gzip, gunzip } = require('zlib')
const { promisify } = require('util')
const stream = require('stream')

const { s3 } = require('./aws')
const { client: redis } = require('./redis')


const gzipAsync = promisify(gzip)
const gunzipAsync = promisify(gunzip)
const evalRedisAsync = promisify(redis.eval).bind(redis)

const cacheTypes = {
  REDIS: 1,
  S3: 2,
}
const API_CACHE_BUCKET = `locus-api-cache-${process.env.STAGE || 'dev'}`

/**
 * Reduces keys to a single key
 * @param {string|any[]]} keys Single key (string) or array of keys which must be serializable
 * into JSON
 * @returns {string} Generated key
 */
const getCacheKey = (keys) => {
  if (typeof keys === 'string') {
    return keys
  }
  if (keys.length === 1 && typeof keys[0] === 'string') {
    return keys[0]
  }
  return createHash('sha256').update(JSON.stringify(keys)).digest('hex')
}

/**
 * Retrieves value from Redis cache given a key
 * @param {string|any[]]} keys Single key (string) or array of keys which must be serializable
 * into JSON
 * @param {Object} options
 * @param {number} [options.maxAge] Max age of the cache in seconds. 0 means don't pull from the
 * cache while a negative value (or undefined) means return whatever is in the cache
 * irrespective of age
 * @param {number} [options.maxSize] Max size in bytes of the value in cache. 0 means don't pull
 * from the cache while a negative value (or undefined) means return whatever is in the cache
 * irrespective of size
 * @param {boolean} [options.parseFromJson=true] Whether or not the value should be parsed
 * in the event the value is a JSON string
 * @returns {Promise<string|integer|Object|undefined>} Value in cache or undefined if cache miss
 */
const getFromRedisCache = async (keys, { maxAge, maxSize, parseFromJson = true } = {}) => {
  if (maxAge === 0 || maxSize === 0) {
    // invalidate cache
    return
  }
  const key = getCacheKey(keys)
  const valueKey = Buffer.from(`q:${key}`, 'utf8')
  const metaKey = `q:meta:${key}`
  const earliestCreated = maxAge > 0 ? Math.floor(Date.now() / 1000) - maxAge : 0
  const res = await evalRedisAsync(`
    local meta = redis.call('GET', ARGV[2])
    if not meta then
        return nil
    end
    local created, json, gzip, size = string.match(meta, '(.*):(.*):(.*):(.*)')
    if (
      (tonumber(ARGV[3]) or 0) > 0 and tonumber(created) < tonumber(ARGV[3])
      or (tonumber(ARGV[4]) or 0) > 0 and tonumber(size) > tonumber(ARGV[4])
    ) then
        return nil
    end
    return {
      redis.call('GET', ARGV[1]),
      tonumber(json),
      tonumber(gzip),
    }
  `, 0, valueKey, metaKey, earliestCreated, maxSize || 0)

  if (res === null) {
    return
  }

  // eslint-disable-next-line prefer-const
  let [value, json, gzip] = res
  if (gzip) {
    value = await gunzipAsync(value)
  }
  value = value.toString('utf8')
  return json && parseFromJson ? JSON.parse(value) : value
}

/**
 * Stores key value pair in Redis cache
 * @param {string} key Cache key
 * @param {string|integer} value Value to cache (objects must be serialized prior to invocation)
 * @param {Object} options
 * @param {number} [options.ttl=600] Cache TTL in seconds
 * @param {boolean} [options.gzip=true] Whether or not the value in cache should be compressed
 * @param {boolean} [options.json=true] Whether or not the value should be cached as type JSON. When
 * an object is passed as value, this parameter is ignored and the content type set to JSON.
 */
const putToRedisCache = async (keys, value, { ttl = 600, gzip = true, json = true } = {}) => {
  if (ttl <= 0) {
    return false
  }
  const key = getCacheKey(keys)
  const valueKey = `q:${key}`
  const metaKey = `q:meta:${key}`

  let cacheValue = value
  if (typeof value === 'object') {
    // eslint-disable-next-line no-param-reassign
    json = true
    cacheValue = JSON.stringify(value)
  }
  if (gzip) {
    cacheValue = await gzipAsync(cacheValue)
  }
  if (!(cacheValue instanceof Buffer)) {
    cacheValue = Buffer.from(cacheValue, 'utf8')
  }
  return new Promise((resolve, reject) => {
    redis
      .multi()
      .set(valueKey, cacheValue, 'EX', ttl)
      .set(
        metaKey,
        // meta: unix:json:gzip:size
        `${Math.floor(Date.now() / 1000)}:${Number(json)}:${Number(gzip)}:${cacheValue.length}`,
        'EX',
        ttl,
      )
      .exec((err, res) => {
        if (err) {
          return reject(err)
        }
        const commandErr = res.find(commandRes => commandRes instanceof Error)
        if (commandErr) {
          return reject(commandErr)
        }
        resolve(undefined)
      })
  })
}

/**
 * Retrieves value from S3 cache given a key
 * @param {string|any[]]} keys Single key (string) or array of keys which must be serializable
 * into JSON
 * @param {Object} options
 * @param {number} [options.maxAge] Max age of the cache in seconds. 0 means don't pull from the
 * cache while a negative value (or undefined) means return whatever is in the cache
 * irrespective of age
 * @param {number} [options.maxSize] Max size in bytes of the value in cache. 0 means don't pull
 * from the cache while a negative value (or undefined) means return whatever is in the cache
 * irrespective of size
 * @param {boolean} [options.parseFromJson=true] Whether or not the value should be parsed
 * in the event the value is a JSON string
 * @param {string} [options.bucket=QUERY_BUCKET] Cache bucket
 * @returns {Promise<string|Object|undefined>} Value in cache or undefined if not found
 */
const getFromS3Cache = async (
  keys,
  { maxAge, maxSize, parseFromJson = true, bucket = API_CACHE_BUCKET } = {},
) => {
  if (maxAge === 0 || maxSize === 0) {
    // invalidate cache
    return
  }
  const params = {
    Bucket: bucket,
    Key: getCacheKey(keys),
  }
  if (maxAge > 0) {
    params.IfModifiedSince = Math.floor(Date.now() / 1000) - maxAge
  }

  try {
    if (maxSize > 0) {
      const { ContentLength } = await s3.headObject(params).promise()
      if (ContentLength > maxSize) {
        // value in cache is too large
        return
      }
    }
    const { Body, ContentEncoding, ContentType } = await s3.getObject(params).promise()
    let value = Body
    if (ContentEncoding === 'gzip') {
      value = await gunzipAsync(value)
    }
    if (ContentType !== 'application/octet-stream') {
      value = value.toString('utf8')
    }
    if (ContentType === 'application/json' && parseFromJson) {
      return JSON.parse(value)
    }
    return value
  } catch (err) {
    if (['NoSuchKey', 'NotModified'].includes(err.code)) {
      return
    }
    console.log('error:', err)
    throw err
  }
}

/**
 * Stores key value pair in S3 cache
 * @param {string|any[]]} keys Single key (string) or array of keys which must be serializable
 * into JSON
 * @param {string|Object} value Cache value (in the case of an object, it must be must be
 * serializable into JSON and options.json must be set to true)
 * @param {Object} options
 * @param {boolean} [options.gzip=true] Whether or not the value in cache should be compressed
 * @param {boolean} [options.json=true] Whether or not the value should be cached as type JSON. When
 * an object is passed as value, this parameter is ignored and the content type set to JSON.
 * @param {string} [options.bucket=QUERY_BUCKET] Cache bucket
 */
const putToS3Cache = async (
  keys,
  value,
  { gzip = true, json = true, bucket = API_CACHE_BUCKET, metadata = {} } = {},
) => {
  let cacheValue = value
  const key = getCacheKey(keys)
  if (Buffer.isBuffer(value) || value instanceof stream.Stream) {
    await s3.upload({
      Bucket: bucket,
      Key: `${key}.parquet`,
      Body: cacheValue,
      ContentType: 'application/octet-stream',
      Metadata: metadata,
    }).promise()
  } else {
    if (typeof value === 'object') {
    // eslint-disable-next-line no-param-reassign
      json = true
      cacheValue = JSON.stringify(value)
    }
    if (gzip) {
      cacheValue = await gzipAsync(cacheValue)
    }
    await s3.putObject({
      Bucket: bucket,
      Key: key,
      Body: cacheValue,
      ContentType: json ? 'application/json' : 'text/plain',
      ContentEncoding: gzip ? 'gzip' : 'identity',
      Metadata: metadata,
    }).promise()
  }
}

/**
 * Generates a pre-signed URL to the S3 cache given a key
 * @param {string|any[]]} keys Single key (string) or array of keys which must be serializable
 * into JSON
 * @param {Object} options
 * @param {number} [options.maxAge] Max age of the cache in seconds. 0 means don't pull from the
 * cache while a negative value (or undefined) means return whatever is in the cache
 * irrespective of age
 * @param {number} [options.ttl=600] URL validity in seconds. Defaults to 600 (10 minutes)
 * @param {string} [options.bucket=QUERY_BUCKET] Cache bucket
 * @returns {Promise<string|undefined>} URL of the object in cache or undefined if not found
 */
const getS3CacheURL = async (
  keys,
  { maxAge, ttl = 600, bucket = API_CACHE_BUCKET } = {},
) => {
  if (maxAge === 0) {
    // invalidate cache
    return
  }
  const params = {
    Bucket: bucket,
    Key: getCacheKey(keys),
    Expires: ttl,
  }
  if (maxAge > 0) {
    params.IfModifiedSince = Math.floor(Date.now() / 1000) - maxAge
  }
  try {
    const url = await s3.getSignedUrlPromise('getObject', params)
    return url
  } catch (err) {
    if (['NoSuchKey', 'NotModified'].includes(err.code)) {
      return
    }
    throw err
  }
}

/**
 * Pulls query results from cache if available, otherwise, executes query and persists
 * results to cache
 * @param {string|any[]]} keys Single key (string) or array of keys which must be serializable
 * into JSON
 * @param {function} queryFn Callback to invoke in order to fetch query results (must
 * resolve to an object)
 * @param {Object} options
 * @param {number} [options.maxAge=600] Max age (in seconds) of query results when pulling from the
 * cache (typically same as ttl)
 * @param {number} [options.ttl=600] TTL (in seconds) of the cached results
 * @param {number} [options.maxSize] Max size (in bytes) of query results when pulling from the
 * cache
 * @param {number} [options.type=type.REDIS] Storage type
 * @param {boolean} [options.gzip=true] Whether or not the value in cache is compressed
 * @param {boolean} [options.json=true] Whether or not the value in cache is of type JSON. When
 * queryFn resolves to an object, this parameter is ignored and the content type set to JSON.
 * @param {string} [options.bucket=QUERY_CACHE_BUCKET] Cache bucket (when the storage type is S3)
 * @returns {Promise<Object>} Query results
 */
const queryWithCache = async (
  keys,
  queryFn,
  {
    maxAge = 600,
    ttl = 600,
    maxSize,
    type = cacheTypes.REDIS,
    gzip = true,
    json = true,
    bucket = API_CACHE_BUCKET,
  } = {},
) => {
  // compute cache key
  const key = getCacheKey(keys)

  // get from cache
  try {
    const getOptions = {
      maxAge: Math.min(maxAge, ttl),
      maxSize,
      parseFromJson: true,
      bucket,
    }
    let cachedValue
    switch (type) {
      case cacheTypes.REDIS:
        cachedValue = await getFromRedisCache(key, getOptions)
        break
      case cacheTypes.S3:
        cachedValue = await getFromS3Cache(key, getOptions)
        break
      default:
        throw new Error('Cache type not supported')
    }
    if (cachedValue) {
      return cachedValue
    }
  } catch (err) {
    // don't throw - will execute sql query if cache error
    console.log('Error retrieving query results from cache', err.message)
  }

  // run query
  const queryValue = await queryFn()

  // set cache
  try {
    const putOptions = {
      ttl,
      gzip,
      json,
      bucket,
    }
    switch (type) {
      case cacheTypes.REDIS:
        await putToRedisCache(key, queryValue, putOptions)
        break
      case cacheTypes.S3:
        await putToS3Cache(key, queryValue, putOptions)
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
 * By defaults only the results' rows are cached and returned
 * @param {Knex.QueryBuilder} knexQuery Knex QueryBuilder object
 * @param {Object} options
 * @param {boolean} [options.rows=true] Whether or not to limit caching to the results' rows
 * @param {number} [options.maxAge=600] Max age (in seconds) of SQL results when pulling from the
 * cache (typically same as ttl)
 * @param {number} [options.ttl=600] TTL (in seconds) of the cached results
 * @param {number} [options.type=type.REDIS] Storage type
 * @param {boolean} [options.gzip=true] Whether or not the value in cache is compressed
 * @returns {Promise<Object>} Query results
 */
const knexWithCache = async (knexQuery, { rows = true, ...options } = {}) => {
  const { sql, bindings } = knexQuery.toSQL()
  const queryFn = () => knexQuery
    .then(res => (rows && res && !Array.isArray(res) && res.rows ? res.rows : res))
  return queryWithCache([sql, bindings], queryFn, options)
}

/**
 * Pulls query results from cache if available, otherwise, executes query and persists
 * results to cache
 * By defaults only the results' rows are cached and returned
 * @param {string} sql Query string
 * @param {Object|Array} bindings Query variable bindings
 * @param {pg.Pool|pg.Client} pool Node-pg pool or client
 * @param {Object} options
 * @param {boolean} [options.rows=true] Whether or not to limit caching to the results' rows
 * @param {number} [options.maxAge=600] Max age (in seconds) of SQL results when pulling from the
 * cache (typically same as ttl)
 * @param {number} [options.ttl=600] TTL (in seconds) of the cached results
 * @param {number} [options.type=type.REDIS] Storage type
 * @param {boolean} [options.gzip=true] Whether or not the value in cache is compressed
 * @returns {Promise<Object>} Query results
 */
const pgWithCache = (sql, bindings, pool, { rows = true, ...options } = {}) => {
  const queryFn = () => pool
    .query(sql, bindings)
    .then(res => (rows && res && !Array.isArray(res) && res.rows ? res.rows : res))
  return queryWithCache([sql, bindings], queryFn, options)
}

/**
 * Set default options for caching utility
 * @param {Object} defaultOptions Default options
 * @param {number} defaultOptions.maxAge Max age of the cache in seconds. 0 means don't pull
 * from the cache while a negative value (or undefined) means return whatever is in the cache
 * irrespective of age
 * @param {number} defaultOptions.maxSize Max size in bytes of the value in cache. 0 means don't
 * pull from the cache while a negative value (or undefined) means return whatever is in the cache
 * irrespective of size
 * @param {number} defaultOptions.ttl Cache TTL in seconds
 * @param {boolean} defaultOptions.parseFromJson Whether or not the value should be parsed
 * in the event the value is a JSON string
 * @param {boolean} defaultOptions.gzip Whether or not the value in cache should be compressed
 * @param {boolean} defaultOptions.json Whether or not the value should be cached as type JSON. When
 * an object is passed as value, this parameter is ignored and the content type set to JSON
 * @param {string} defaultOptions.bucket Cache bucket (when the storage type is S3)
 * @param {number} defaultOptions.type Storage type
 * @param {boolean} defaultOptions.rows Whether or not to limit caching to the queryFn's results'
 * 'rows' object (when not undefined)
 * @returns {Object} Cache functions
 */
const useCacheOptions = defaultOptions =>
  [
    getFromRedisCache,
    putToRedisCache,
    getFromS3Cache,
    putToS3Cache,
    getS3CacheURL,
    queryWithCache,
    knexWithCache,
    pgWithCache,
  ].reduce((allFn, fn) => {
    allFn[fn.name] = (...args) => {
      if (args.length < fn.length || args.length > fn.length + 1) {
        // missing required args or too many args - let fn handle it
        return fn(...args)
      }
      const options = { ...defaultOptions }
      // options arg passed
      if (args.length > fn.length) {
        Object.assign(options, args.pop())
      }
      return fn(...args, options)
    }
    return allFn
  }, {})

module.exports = {
  putToS3Cache,
  getFromS3Cache,
  getS3CacheURL,
  queryWithCache,
  knexWithCache,
  pgWithCache,
  useCacheOptions,
  cacheTypes,
  API_CACHE_BUCKET,
}
