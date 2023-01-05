const AWS = require('aws-sdk')


const lambda = new AWS.Lambda({
  apiVersion: '2015-03-31',
  region: 'us-east-1',
})

const s3 = new AWS.S3({ region: 'us-east-1' })

const firehose = new AWS.Firehose({
  apiVersion: '2015-08-04',
  region: 'us-east-1',
})

async function invokeLambda(params) {
  const response = { statusCode: 200 }
  // TODO: this function does not support `Event` invoke type
  try {
    const result = await lambda.invoke(params).promise()
    const payload = JSON.parse(result.Payload)
    if ('statusCode' in payload) {
      response.statusCode = payload.statusCode
    }

    if ('body' in payload) {
      response.body = payload.body
    } else {
      response.body = payload
    }
  } catch (e) {
    console.log('invoke lambda failed')
    throw e
  }

  return response
}

async function listS3Objects(bucket, prefix) {
  const params = { Bucket: bucket, Prefix: prefix }

  let keys = []
  // eslint-disable-next-line no-constant-condition
  while (true) {
    // eslint-disable-next-line no-await-in-loop
    const data = await s3.listObjectsV2(params).promise()

    // eslint-disable-next-line no-loop-func
    data.Contents.forEach((elem) => {
      keys = keys.concat(elem.Key)
    })

    if (!data.IsTruncated) {
      break
    }

    if (data.NextContinuationToken) {
      params.ContinuationToken = data.NextContinuationToken
    }
  }

  return keys
}

async function getS3PresignedURL(bucket, key, expires = 900) {
  const params = { Bucket: bucket, Key: key, Expires: expires }
  return s3.getSignedUrlPromise('getObject', params)
}

async function checkS3ObjectExist(bucket, key) {
  const params = { Bucket: bucket, Key: key }
  try {
    const res = await s3.headObject(params).promise()
    return res.Metadata.status === 'completed'
  } catch (err) {
    return false
  }
}

module.exports = {
  invokeLambda,
  s3,
  lambda,
  listS3Objects,
  getS3PresignedURL,
  firehose,
  checkS3ObjectExist,
}
