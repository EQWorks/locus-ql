const cronParser = require('cron-parser')

const { knex } = require('../../util/db')
const { useAPIErrorOptions } = require('../../util/api-error')
const { QL_SCHEMA } = require('../constants')
const { getSetSchedule, getScheduleID } = require('./schedules')


const { apiError, getSetAPIError } = useAPIErrorOptions({ tags: { service: 'ql' } })

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
    FROM ${QL_SCHEMA}.schedule_queries sq
    JOIN ${QL_SCHEMA}.schedules s USING (schedule_id)
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
    values.push(startDate.toISOString())
  }

  if (endDate !== undefined) {
    cols.push('end_date')
    updateCols.push('end_date')
    values.push(endDate.toISOString())
  }

  if (isPaused !== undefined) {
    cols.push('is_paused')
    updateCols.push('is_paused')
    values.push(isPaused)
  }

  await knex.raw(`
    INSERT INTO ${QL_SCHEMA}.schedule_queries
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
      FROM ${QL_SCHEMA}.schedule_jobs
      WHERE
        schedule_id = :scheduleID
    ),
    other_queries AS (
      SELECT query_id
      FROM ${QL_SCHEMA}.schedule_queries
      WHERE
        schedule_id = :scheduleID
        AND query_id <> :queryID
    ),
    delete AS (
      DELETE FROM ${QL_SCHEMA}.schedule_queries
      WHERE
        schedule_id = :scheduleID
        AND query_id = :queryID
    )
    DELETE FROM ${QL_SCHEMA}.schedules
    WHERE
      schedule_id = :scheduleID
      AND NOT EXISTS (SELECT * FROM jobs)
      AND NOT EXISTS (SELECT * FROM other_queries)
  `, { scheduleID, queryID })
  return { scheduleID, queryID }
}

const putQuerySchedule = async (req, res, next) => {
  try {
    const { queryID } = req.mlQuery
    const { cron, startDate, endDate, isPaused } = req.body
    const { whitelabel, customers } = req.access

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
    const scheduleID = await getSetSchedule(whitelabel[0], customers[0], safeCron)
    if (!scheduleID) {
      throw apiError('Invalid access permissions', 403)
    }
    // create/update query schedule
    await upsertQuerySchedule(scheduleID, queryID, {
      startDate: safeStartDate,
      endDate: safeEndDate,
      isPaused,
    })

    res.json({ queryID, cron: safeCron })
  } catch (err) {
    next(getSetAPIError(err, 'Failed to create or update the query schedule', 500))
  }
}

const deleteQueryScheduleMW = async (req, res, next) => {
  try {
    const { queryID } = req.mlQuery
    const { cron } = req.body
    const { whitelabel, customers } = req.access

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

    // retrieve schedule ID
    const scheduleID = await getScheduleID(whitelabel[0], customers[0], safeCron)
    if (!scheduleID) {
      throw apiError('Query schedule not found', 404)
    }
    // delete
    await deleteQuerySchedule(scheduleID, queryID)
    res.json({ queryID, cron: safeCron })
  } catch (err) {
    next(getSetAPIError(err, 'Failed to delete the query schedule', 500))
  }
}

const listQuerySchedules = async (req, res, next) => {
  try {
    const { queryID } = req.mlQuery

    const schedules = await getQuerySchedules(queryID)
    res.json(schedules)
  } catch (err) {
    next(getSetAPIError(err, 'Failed to retrieve the query schedules', 500))
  }
}

module.exports = {
  putQuerySchedule,
  deleteQueryScheduleMW,
  listQuerySchedules,
}
