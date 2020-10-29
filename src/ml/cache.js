const { createHash } = require('crypto')
const { gzip, gunzip } = require('zlib')
const { promisify } = require('util')

const { apiError } = require('../util/api-error')
const { s3 } = require('../util/aws')


const CACHE_BUCKET = 'ml-query-cache'
const gzipAsync = promisify(gzip)
const gunzipAsync = promisify(gunzip)

/**
 * Stores key value pair in S3 cache
 * @param {string} key Cache key
 * @param {string} value Cache value. Objects need be stringified before being
 * passed to the function
 */
const putToCache = async (key, value) => {
  // const compressedBody = await gzipAsync(JSON.stringify(body))
  const compressedBody = await gzipAsync(value)
  await s3.putObject({
    Bucket: CACHE_BUCKET,
    Key: `${key}.json.gz`,
    Body: compressedBody,
    ContentType: 'application/json',
    ContentEncoding: 'gzip',
  }).promise()
}

/**
 * Retrieves value from S3 cache given a key
 * @param {string} key Cache key
 * @param {number} [staleSeconds=0] If greater than 0, will return undefined if the cache was
 * last updated more than stateSeconds seconds ago
 * @returns {string|undefined} Value in cache or undefined if not found
 */
const getFromCache = async (key, staleSeconds = 0) => {
  const params = {
    Bucket: CACHE_BUCKET,
    Key: `${key}.json.gz`,
  }
  if (staleSeconds > 0) {
    params.IfModifiedSince = Math.floor(Date.now() / 1000) - staleSeconds
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
const getResFromCache = async (req, res, next) => {
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
    req.cacheKey = createHash('sha256').update(JSON.stringify({ access, query })).digest('hex')
    if (cache === 0) {
      // refresh, do not pull from cache
      return next()
    }

    const cachedRes = await getFromCache(req.cacheKey, cache)
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
  putToCache,
  getResFromCache,
}
