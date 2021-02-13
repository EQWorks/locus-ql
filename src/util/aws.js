const AWS = require('aws-sdk')


const lambda = new AWS.Lambda({
  apiVersion: '2015-03-31',
  region: 'us-east-1',
})

const s3 = new AWS.S3({ region: 'us-east-1' })

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

module.exports = { invokeLambda, s3, lambda, listS3Objects }
