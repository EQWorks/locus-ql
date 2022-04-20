/* QL CONSTANTS */
module.exports = {
  // statuses
  STATUS_QUEUED: 'QUEUED',
  STATUS_SOURCING: 'SOURCING',
  STATUS_RUNNING: 'RUNNING',
  STATUS_RETRYING: 'RETRYING',
  STATUS_SUCCEEDED: 'SUCCEEDED',
  STATUS_CANCELLED: 'CANCELLED',
  STATUS_FAILED: 'FAILED',

  // pg schema (depends on deploy stage)
  QL_SCHEMA: process.env.ML_SCHEMA,

  // s3 buckets
  QUERY_BUCKET: process.env.ML_QUERY_BUCKET,
  EXECUTION_BUCKET: process.env.ML_EXECUTION_BUCKET,

  // size of execution result parts
  RESULTS_PART_SIZE_MB: 10, // 10MB gzip compressed

  // query executor
  LAMBDA_EXECUTOR_ARN: process.env.ML_LAMBDA_EXECUTOR_ARN,

  // max input sizes
  MAX_LENGTH_QUERY_NAME: 42, // name + optional ' - <seq{5}>'
  MAX_LENGTH_QUERY_DESCRIPTION: 255,
  MAX_LENGTH_EXECUTION_TOKEN: 255,
  MAX_LENGTH_STATUS_REASON: 255,
}
