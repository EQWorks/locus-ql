const { knex } = require('../../../util/db')

const { ATOM_CONNECTION_NAME } = require('./constants')


const campView = {
  view: knex.raw(`
    (
      SELECT * FROM dblink(?,'
        SELECT
          mb.campcode AS camp_code,
          mb.name AS camp_name,
          f.campcode AS flight_code,
          f.name AS flight_name,
          o.campcode AS offer_code,
          o.name AS offer_name
        FROM public.campaigns AS mb
        JOIN public.campaigns AS f ON f.campcode = mb.flightid
        JOIN public.campaigns AS o ON o.campcode = mb.offerid
        WHERE
          mb.flightid <> 0
          AND mb.offerid <> 0
      ') AS t(
        camp_code int,
        camp_name text,
        flight_code int,
        flight_name text,
        offer_code int,
        offer_name text
      )
    ) AS atom_camps
  `, [ATOM_CONNECTION_NAME]),
  fdwConnection: ATOM_CONNECTION_NAME,
}

const osView = {
  view: knex.raw(`
    (
      SELECT * FROM dblink(?,'
        SELECT
          osid,
          osname
        FROM public.os
      ') AS t(
        os_id int,
        os_name text
      )
    ) AS atom_os
  `, [ATOM_CONNECTION_NAME]),
  fdwConnection: ATOM_CONNECTION_NAME,
}

const browserView = {
  view: knex.raw(`
    (
      SELECT * FROM dblink(?,'
        SELECT
          browserid,
          browsername
        FROM public.browsers
      ') AS t(
        browser_id int,
        browser_name text
      )
    ) AS atom_browsers
  `, [ATOM_CONNECTION_NAME]),
  fdwConnection: ATOM_CONNECTION_NAME,
}

const bannerView = {
  view: knex.raw(`
    (
      SELECT * FROM dblink(?,'
        SELECT
          bannercode,
          bannername,
          imgwidth || ''x'' || imgheight
        FROM public.banners
      ') AS t(
        banner_code int,
        banner_name text,
        banner_size text
      )
    ) AS atom_banners
  `, [ATOM_CONNECTION_NAME]),
  fdwConnection: ATOM_CONNECTION_NAME,
}


module.exports = {
  ATOM_CONNECTION_NAME,
  campView,
  osView,
  browserView,
  bannerView,
}
