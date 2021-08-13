const { knex } = require('../../util/db')
const { QL_SCHEMA } = require('../constants')


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
      FROM ${QL_SCHEMA}.schedules
      WHERE
        customer_id = :customerID
        AND cron = :cron
    ),
    new AS (
      INSERT INTO ${QL_SCHEMA}.schedules
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
    FROM ${QL_SCHEMA}.schedules
    WHERE
      customer_id = ?
      AND cron = ?
  `, [customerID, cron])
  return scheduleID
}


module.exports = {
  getSetSchedule,
  getScheduleID,
}
