const { knex } = require('../../../util/db')


const campView = {
  view: knex.raw(`
    (
      SELECT
        camp_id AS camp_code,
        name AS camp_name
      FROM public.camps
    ) AS locus_camps
  `),
}

module.exports = { campView }
