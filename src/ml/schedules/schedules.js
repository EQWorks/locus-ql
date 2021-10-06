const { knex } = require('../../util/db')
const { QL_SCHEMA } = require('../constants')


/**
 * Returns the schedule ID corresponding to the customer ID + CRON combination.
 * Creates said schedule if it does not already exists.
 * @param {number} whitelabelID Whitelabel ID
 * @param {number} customerID Customer ID
 * @param {string} cron CRON expression
 * @returns {Promise<number>} Schedule ID
 */
const getSetSchedule = async (whitelabelID, customerID, cron) => {
  const { rows: [{ scheduleID } = {}] } = await knex.raw(`
    WITH access AS (
      SELECT customerid FROM public.customers
      WHERE
        whitelabelid = :whitelabelID
        AND customerid = :customerID
    ),
    existing AS (
      SELECT schedule_id AS "scheduleID"
      FROM ${QL_SCHEMA}.schedules
      WHERE
        EXISTS (SELECT * FROM access)
        AND customer_id = :customerID
        AND cron = :cron
    ),
    new AS (
      INSERT INTO ${QL_SCHEMA}.schedules
        (customer_id, cron)
        SELECT :customerID, :cron
        WHERE
          EXISTS (SELECT * FROM access)
          AND NOT EXISTS (SELECT * FROM existing)
      RETURNING schedule_id AS "scheduleID"
    )
    SELECT * FROM existing
    UNION
    SELECT * FROM new
  `, { whitelabelID, customerID, cron })
  return scheduleID
}

/**
 * Returns the schedule ID corresponding to the customer ID + CRON combination.
 * @param {number} whitelabelID Whitelabel ID
 * @param {number} customerID Customer ID
 * @param {string} cron CRON expression
 * @returns {Promise<number>} Schedule ID or undefined if not found
 */
const getScheduleID = async (whitelabelID, customerID, cron) => {
  const { rows: [{ scheduleID } = {}] } = await knex.raw(`
    WITH access AS (
      SELECT customerid FROM public.customers
      WHERE
        whitelabelid = :whitelabelID
        AND customerid = :customerID
    )
    SELECT
      schedule_id AS "scheduleID"
    FROM ${QL_SCHEMA}.schedules
    WHERE
      EXISTS (SELECT * FROM access)
      AND customer_id = :customerID
      AND cron = :cron
  `, { whitelabelID, customerID, cron })
  return scheduleID
}


module.exports = {
  getSetSchedule,
  getScheduleID,
}
