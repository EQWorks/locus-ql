const cronParser = require('cron-parser')

const { knex } = require('../util/db')
const { apiError } = require('../util/api-error')
const { queueQueryExecution } = require('./queries')


const { ML_SCHEMA } = process.env
const STATUS_RUNNING = 'RUNNING'
const STATUS_RETRYING = 'RETRYING'
const STATUS_SUCCEEDED = 'SUCCEEDED'


/**
 * Returns an array of execution metas based on the supplied filters
 * @param {number} jobID Job ID
 * @returns {Promise<Object>}
 */
const getScheduleJob = async (jobID) => {
  const { rows: [job] } = await knex.raw(`
    SELECT
      j.job_id AS "jobID",
      c.whitelabelid AS "whitelabelID",
      c.customerid AS "customerID",
      j.schedule_id AS "scheduleID",
      j.job_ts AS "jobTS",
      j.status,
      j.status_ts AS "statusTS"
    FROM ${ML_SCHEMA}.schedule_jobs j
    JOIN ${ML_SCHEMA}.schedules s ON s.schedule_id = j.schedule_id
    JOIN public.customers c ON c.customerid = s.customer_id
    WHERE j.job_id = :jobID
  `, { jobID })
  return job
}

/**
 * Updates a schedule job based on its ID
 * @param {number} jobID Schedule job ID
 * @param {Object} updates
 * @param {string} [updates.status] New status
 */
const updateScheduleJob = async (
  jobID,
  { status },
) => {
  const columns = []
  const values = []
  const expressions = []
  if (status) {
    columns.push('status')
    values.push(status)
    expressions.push('status_ts = now()')
  }
  if (!columns.length && !expressions.length) {
    // nothing to update
    return
  }
  await knex.raw(`
    UPDATE ${ML_SCHEMA}.schedule_jobs
    SET ${columns.map(col => `${col} = ?`).concat(expressions).join(', ')}
    WHERE job_id = ?
  `, [...values, jobID])
}

// fetch all schedules for customer with inferior id which share the same queries
// compare cron expression -> if overlap, yield to schedule with lowest id
// insert entry in execution table with ref to job id (schedule id + ts)
// IDEMPOTENT: lambda might be called multiple times by airflow in case of failure
// - check if execution already attached to query or not
/**
 * Returns list of queries that the schedule job has responsibiity for, excluding those which have
 * already run for the job's timestamp
 * @param {number} jobID Schedule job ID
 * @returns {number[]} Filtered array of query ID's
 */
const getScheduleJobQueryIDs = async (jobID) => {
  const { rows: queries } = await knex.raw(`
    SELECT
      all_sq.query_id AS "queryID",
      sq.schedule_id AS "jobScheduleID",
      j.job_ts AS "jobTS",
      array_agg(
        json_build_object(
          'scheduleID', all_sq.schedule_id,
          'cron', s.cron
        ) ORDER BY all_sq.schedule_id
      ) AS "schedules"
    FROM ${ML_SCHEMA}.schedule_jobs j
    JOIN ${ML_SCHEMA}.schedule_queries sq ON sq.schedule_id = j.schedule_id
    JOIN ${ML_SCHEMA}.schedule_queries all_sq ON all_sq.query_id = sq.query_id
    JOIN ${ML_SCHEMA}.schedules s ON s.schedule_id = all_sq.schedule_id
    LEFT JOIN ${ML_SCHEMA}.schedule_jobs all_j ON
      all_j.schedule_id = all_sq.schedule_id
      AND all_j.job_ts = j.job_ts
    LEFT JOIN ${ML_SCHEMA}.executions e ON
      e.query_id = sq.query_id
      AND e.schedule_job_id = j.job_id
    WHERE
      j.job_id = :jobID
      AND j.status <> '${STATUS_SUCCEEDED}'
      AND e.execution_id IS NULL
      AND all_sq.schedule_id <= sq.schedule_id
      AND (all_sq.start_date IS NULL OR all_sq.start_date <= j.job_ts)
      AND (all_sq.end_date IS NULL OR all_sq.end_date + 1 > j.job_ts)
      AND all_sq.is_paused = FALSE
    GROUP BY 1, 2, 3
    HAVING every(all_j.job_id IS NULL OR all_j.job_id = j.job_id) -- exclude if competing job
    ORDER BY 1
  `, { jobID })

  const cronValueMemo = {}
  let cronParserOptions
  let jobTSValue

  return queries.reduce((filteredQueries, { queryID, jobScheduleID, jobTS, schedules }) => {
    cronParserOptions = cronParserOptions || {
      currentDate: jobTS.valueOf() - 1, // exclusive -> remove one ms
      endDate: jobTS,
      utc: true,
    }
    jobTSValue = jobTSValue || jobTS.valueOf()
    for (const { scheduleID, cron } of schedules) {
      if (scheduleID === jobScheduleID) {
        filteredQueries.push(queryID)
        break
      }
      // parse cron and add to memo
      if (!(cron in cronValueMemo)) {
        const parsed = cronParser.parseExpression(cron, cronParserOptions)
        cronValueMemo[cron] = new Date(parsed.next().toString()).valueOf()
      }
      // if competing schedule, exclude query
      if (cronValueMemo[cron] === jobTSValue) {
        break
      }
    }
    return filteredQueries
  }, [])
}

// let errors bubble up so the query can be retried
// timestamp is the airflow execution date
const runScheduleJob = async (jobID) => {
  try {
    const job = await getScheduleJob(jobID)
    if (!job) {
      throw apiError('Invalid schedule job ID')
    }
    const { status } = job
    if (status !== STATUS_RUNNING) {
      // don't run unless the status was set to running beforehand
      return
    }

    const queryIDs = await getScheduleJobQueryIDs(jobID)
    // allSettled and then check for error
    const results = await Promise.allSettled(queryIDs.map(id => queueQueryExecution(id, jobID)))
    const { reason } = results.find(({ status }) => status === 'rejected') || {}
    if (reason) {
      throw reason
    }

    // update status to succeeded
    await updateScheduleJob(
      jobID,
      { status: STATUS_SUCCEEDED },
    )
  } catch (err) {
    // let the listeners know that the function might be retried
    await updateScheduleJob(
      jobID,
      { status: STATUS_RETRYING },
    )
    throw err
  }
}

// lambda handler
const scheduleJobHandler = ({ job_id }) => {
  // eslint-disable-next-line radix
  const id = parseInt(job_id, 10)
  if (Number.isNaN(id)) {
    throw apiError(`Invalid schedule job ID: ${job_id}`)
  }
  console.log('schedule job id', id)
  return runScheduleJob(id)
}

const test = () => {
  getScheduleJobQueryIDs(1).then(console.log)
  getScheduleJobQueryIDs(2).then(console.log)
  // queueQueryExecution(230, 2).then(console.log)
  // runScheduleJob(2).then(console.log)
}

test()

module.exports = {
  scheduleJobHandler,
}
