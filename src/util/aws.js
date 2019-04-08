const AWS = require('aws-sdk')


const lambda = new AWS.Lambda({
  apiVersion: '2015-03-31',
  region: 'us-east-1',
})

const s3 = new AWS.S3({ region: 'us-east-1' })

async function invokeLambda(params) {
  const response = { statusCode: 200 }

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

module.exports = { invokeLambda, s3 }
