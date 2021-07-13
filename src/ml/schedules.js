const cronParser = require('cron-parser')

const { knex } = require('../util/db')
const { apiError, APIError } = require('../util/api-error')
const { queueQueryExecution } = require('./queries')


const { ML_SCHEMA } = process.env
const STATUS_RUNNING = 'RUNNING'
const STATUS_RETRYING = 'RETRYING'
const STATUS_SUCCEEDED = 'SUCCEEDED'

/**
 * Returns the schedule ID corresponding to the customer ID + CRON combination.
 * Creates said schedule if it does not already exists.
 * @param {number} customerID Customer ID
 * @param {string} cron CRON expression
 * @returns {Promise<number>} Schedule ID
 */
const getSetSchedule = async (customerID, cron) => {
  const { rows: [{ scheduleID } = {}] } = await knex.raw(`
    WITH existing AS (
      SELECT schedule_id AS "scheduleID"
      FROM ${ML_SCHEMA}.schedules
      WHERE
        customer_id = :customerID
        AND cron = :cron
    ),
    new AS (
      INSERT INTO ${ML_SCHEMA}.schedules
        (customer_id, cron)
      SELECT :customerID, :cron
      WHERE NOT EXISTS (SELECT * FROM existing)
      RETURNING schedule_id AS "scheduleID"
    )
    SELECT * FROM existing
    UNION
    SELECT * FROM new
  `, { customerID, cron })
  return scheduleID
}

/**
 * Returns the schedule ID corresponding to the customer ID + CRON combination.
 * @param {number} customerID Customer ID
 * @param {string} cron CRON expression
 * @returns {Promise<number>} Schedule ID or undefined if not found
 */
const getScheduleID = async (customerID, cron) => {
  const { rows: [{ scheduleID } = {}] } = await knex.raw(`
    SELECT
      schedule_id AS "scheduleID"
    FROM ${ML_SCHEMA}.schedules
    WHERE
      customer_id = ?
      AND cron = ?
  `, [customerID, cron])
  return scheduleID
}

/**
 * Returns all the schedules attached to a given query
 * @param {number} queryID Query ID
 * @returns {Promise<Object[]>} Array of queru schedule objects
 */
const getQuerySchedules = async (queryID) => {
  const { rows } = await knex.raw(`
    SELECT
      -- s.schedule_id AS "scheduleID",
      sq.query_id AS "queryID",
      s.cron,
      sq.start_date AS "startDate",
      sq.end_date AS "endDate",
      sq.is_paused AS "isPaused"
    FROM ${ML_SCHEMA}.schedule_queries sq
    JOIN ${ML_SCHEMA}.schedules s USING (schedule_id)
    WHERE sq.query_id = ?
    ORDER BY sq.is_paused, sq.schedule_id
  `, [queryID])
  return rows
}

/**
 * Upserts query schedule
 * @param {number} scheduleID Schedule ID
 * @param {number} queryID Query ID
 * @param {Object} [options] Query schedule options
 * @param {Date|null} [options.startDate] Effective date of query schedule
 * @param {Date|null} [options.endDate] End date (inclusive) of query schedule
 * @param {boolean} [options.isPaused]
 * @returns {Promise<{scheduleID: number, queryID: number}>}
 */
const upsertQuerySchedule = async (scheduleID, queryID, { startDate, endDate, isPaused } = {}) => {
  const cols = ['schedule_id', 'query_id']
  const values = [scheduleID, queryID]
  const updateCols = []

  if (startDate !== undefined) {
    cols.push('start_date')
    updateCols.push('start_date')
    values.push(startDate)
  }

  if (endDate !== undefined) {
    cols.push('end_date')
    updateCols.push('end_date')
    values.push(endDate)
  }

  if (isPaused !== undefined) {
    cols.push('is_paused')
    updateCols.push('is_paused')
    values.push(isPaused)
  }

  await knex.raw(`
    INSERT INTO ${ML_SCHEMA}.schedule_queries
      (${cols.join(', ')})
    VALUES
      (${cols.map(() => '?').join(', ')})
    ON CONFLICT (schedule_id, query_id) DO ${updateCols.length ? 'UPDATE SET' : 'NOTHING'}
      ${updateCols.map(col => `${col} = EXCLUDED.${col}`).join(', ')}
  `, values)
  return { scheduleID, queryID }
}

/**
 * Deletes query schedule
 * Removes the underlying schedule should no further reference to it be required.
 * @param {number} scheduleID Schedule ID
 * @param {number} queryID Query ID
 * @returns {Promise<{scheduleID: number, queryID: number}>}
 */
const deleteQuerySchedule = async (scheduleID, queryID) => {
  await knex.raw(`
    WITH jobs AS (
      SELECT job_id
      FROM ${ML_SCHEMA}.schedule_jobs
      WHERE
        schedule_id = :scheduleID
    ),
    other_queries AS (
      SELECT query_id
      FROM ${ML_SCHEMA}.schedule_queries
      WHERE
        schedule_id = :scheduleID
        AND query_id <> :queryID
    ),
    delete AS (
      DELETE FROM ${ML_SCHEMA}.schedule_queries
      WHERE
        schedule_id = :scheduleID
        AND query_id = :queryID
    )
    DELETE FROM ${ML_SCHEMA}.schedules
    WHERE
      schedule_id = :scheduleID
      AND NOT EXISTS (SELECT * FROM jobs)
      AND NOT EXISTS (SELECT * FROM other_queries)
  `, { scheduleID, queryID })
  return { scheduleID, queryID }
}

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
 * @param {Object} updates Updates to be posted
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

/**
 * Returns list of queries that the schedule job has responsibiity for, excluding those which have
 * already run for the job's timestamp
 * @param {number} jobID Schedule job ID
 * @returns {number[]} Filtered array of query ID's
 */
const getScheduleJobQueryIDs = async (jobID) => {
  // Job to query resolution logic:
  // - fetch list of queries associated with job's schedule
  // - for each of these queries, get list of associated schedules with lower id than job's schedule
  // - when two schedules intersect at the job's timestamp, yield to the schedule with the lower id
  // - exclude queries with executions referencing this job's id (i.e. already queued)
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
  return runScheduleJob(id)
}


const putQuerySchedule = async (req, res, next) => {
  try {
    const { queryID } = req.mlQuery
    const { cron, startDate, endDate, isPaused } = req.body

    // input validation
    if (!cron) {
      throw apiError('Missing cron expression')
    }
    let safeCron
    try {
      safeCron = cronParser.parseExpression(cron).stringify()
    } catch (err) {
      throw apiError(`Invalid cron expression: ${cron}`)
    }
    let safeStartDate
    if (startDate) {
      safeStartDate = new Date(typeof startDate === 'number' ? startDate * 1000 : startDate)
      if (safeStartDate.toString() === 'Invalid Date') {
        throw apiError(`Invalid start date: ${startDate}`)
      }
    } else if (startDate === null) {
      safeStartDate = null
    }
    let safeEndDate
    if (endDate) {
      safeEndDate = new Date(typeof endDate === 'number' ? endDate * 1000 : endDate)
      if (safeEndDate.toString() === 'Invalid Date') {
        throw apiError(`Invalid end date: ${endDate}`)
      }
    } else if (endDate === null) {
      safeEndDate = null
    }
    if (isPaused !== undefined && typeof isPaused !== 'boolean') {
      throw apiError(`Invalid isPaused flag: ${isPaused}`)
    }

    // get/set schedule for cron expression
    const scheduleID = await getSetSchedule(req.access.customers[0], safeCron)
    // create/update query schedule
    await upsertQuerySchedule(scheduleID, queryID, {
      startDate: safeStartDate,
      endDate: safeEndDate,
      isPaused,
    })

    res.json({ queryID, cron: safeCron })
  } catch (err) {
    if (err instanceof APIError) {
      return next(err)
    }
    next(apiError('Failed to create or update the query schedule', 500))
  }
}

const deleteQueryScheduleMW = async (req, res, next) => {
  try {
    const { queryID } = req.mlQuery
    const { cron } = req.body

    // input validation
    if (cron === '') {
      throw apiError('Missing cron expression')
    }
    let safeCron
    try {
      safeCron = cronParser.parseExpression(cron).stringify()
    } catch (err) {
      throw apiError(`Invalid cron expression: ${cron}`)
    }

    // retrieve schedule ID
    const scheduleID = await getScheduleID(req.access.customers[0], safeCron)
    // delete
    await deleteQuerySchedule(scheduleID, queryID)
    res.json({ queryID, cron: safeCron })
  } catch (err) {
    if (err instanceof APIError) {
      return next(err)
    }
    next(apiError('Failed to delete the query schedule', 500))
  }
}

const listQuerySchedules = async (req, res, next) => {
  try {
    const { queryID } = req.mlQuery

    const schedules = await getQuerySchedules(queryID)
    res.json(schedules)
  } catch (err) {
    if (err instanceof APIError) {
      return next(err)
    }
    next(apiError('Failed to delete the query schedule', 500))
  }
}

const test = () => {
  // getScheduleJobQueryIDs(1).then(console.log)
  // getScheduleJobQueryIDs(2).then(console.log)
  // queueQueryExecution(230, 2).then(console.log)
  // runScheduleJob(2).then(console.log)
  // getSetSchedule(9593, '0 0 8 * *')
  //   .then(id => upsertQuerySchedule(id, 230, { startDate: new Date(2021, 7, 9) }))
  //   // .then(id => upsertQuerySchedule(id, 230, { startDate: null }))
  //   .then(console.log)

}

test()

module.exports = {
  scheduleJobHandler,
  putQuerySchedule,
  deleteQueryScheduleMW,
  listQuerySchedules,
}
