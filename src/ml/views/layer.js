/* eslint-disable indent */
/* eslint-disable no-use-before-define */

const { knex, mapKnex, MAPS_FDW_CONNECTION } = require('../../util/db')
const {
  CAT_STRING,
  CAT_NUMERIC,
} = require('../type')
const { apiError } = require('../../util/api-error')
const { knexWithCache } = require('../cache')
const { geoMapping } = require('../geo')


const getKnexLayerQuery = async (access, filter = {}) => {
  const { whitelabel, customers, email = '' } = access
  // for all layers that user has access to
  // get all categories and expand it out as a single row
  // each view should be a layer + category

  const layerTypes = [18, 19, 20] // demo, prop and persona

  const layerQuery = knex('layer')
  layerQuery.column([
    'layer.name',
    'layer.layer_id',
    'layer.layer_type_id',
    'layer.layer_categories',
  ])
  layerQuery.where(filter)
  layerQuery.whereIn('layer.layer_type_id', layerTypes)
  layerQuery.whereNotNull('layer.layer_categories')
  layerQuery.whereNull('layer.parent_layer')
  // subscription logic
  if (whitelabel !== -1) {
    if (!whitelabel.length || (customers !== -1 && !customers.length)) {
      // if no WL and/or no customers, can only see WL= -1
      layerQuery.where('layer.whitelabel', -1)
    } else {
      // else can see WL = -1 + WL = whitelabel (subject to customers value) + subscribed layers

      // fetch subscribed layers
      const rows = await knexWithCache(
        knex.raw(`
          SELECT type_id
          FROM market_ownership_flat MO
          LEFT JOIN customers as CU ON CU.customerid = MO.customer
          WHERE
            MO.type = 'layer'
            AND MO.whitelabel = ANY (:whitelabel)
            ${customers !== -1
              ? 'AND (MO.customer = ANY (:customers) OR CU.agencyid = ANY (:customers))'
              : ''
            }
        `, { whitelabel, customers }),
        { ttl: 60 }, // 1 minute (to reflect new subscriptions)
      )
      const subscribedLayerIDs = rows.map(layer => layer.type_id)

      // join with customers to expose the agency ID
      if (customers !== -1) {
        layerQuery.joinRaw('LEFT JOIN customers as CU ON CU.customerid = layer.customer')
      }

      layerQuery.whereRaw(`
        (
          layer.whitelabel = -1
          OR
          (
            layer.whitelabel = ANY (:whitelabel)
            AND layer.account in ('0', '-1', :email)
            ${customers !== -1
              ? 'AND (layer.customer = ANY (:customers) OR CU.agencyid = ANY (:customers))'
              : ''
            }
          )
          OR
          layer.layer_id = ANY (:subscribedLayerIDs)
        )
      `, { whitelabel, customers, email, subscribedLayerIDs })
    }
  }
  return knexWithCache(layerQuery, { ttl: 600 }) // 10 minutes
}

const getQueryView = async (access, { layer_id, categoryKey }) => {
  const viewID = `layer_${layer_id}_${categoryKey}`
  const [layer] = await getKnexLayerQuery(access, { layer_id })
  if (!layer) {
    throw apiError('Access to layer not allowed', 403)
  }

  const category = layer.layer_categories[categoryKey]
  if (!category) {
    throw apiError('Invalid layer category', 403)
  }

  // inject view columns
  const viewMeta = await listViews({ access, filter: { layer_id } })
  const mlViewColumns = (viewMeta[0] || {}).columns

  const { table, slug, resolution } = category
  const geo = geoMapping[`ca-${resolution}`]

  let schema = ''
  // retrieve schema if missing (usually 'static_layers')
  if ((table || slug).indexOf('.') === -1) {
    // [{ schema = 'public' } = {}] = await knexWithCache(
    //   mapKnex.raw(`
    //     SELECT table_schema AS schema
    //     FROM information_schema.tables
    //     WHERE table_name = ?
    //     LIMIT 1
    //   `, [table || slug]),
    //   { ttl: 3600 }, // 1 hour
    // )
    // schema = `${schema}.`
    schema = 'static_layers.'
  }

  const mlView = mapKnex.raw(`
    SELECT * FROM dblink(:fdwConnection, '
      SELECT
        GM.ggid AS id,
        L.geo_id,
        L.geo_id AS geo_ca_${resolution},
        L.total,
        L.summary_data::json #>> ''{main_number_pcnt, title}'' AS title,
        L.summary_data::json #>> ''{main_number_pcnt, value}'' AS value,
        L.summary_data::json #>> ''{main_number_pcnt, percent}'' AS percent,
        L.summary_data::json #>> ''{main_number_pcnt, units}'' AS units
      FROM ${schema}${table || slug} as L
      INNER JOIN config.ggid_map as GM ON GM.type = ''${resolution}'' AND GM.local_id = L.geo_id
      INNER JOIN ${geo.schema}.${geo.table} as GT ON GT.${geo.idColumn} = L.geo_id
      WHERE GT.wkb_geometry IS NOT NULL
    ') AS t(
      id int,
      geo_id text,
      geo_ca_${resolution} text,
      total int,
      title text,
      value real,
      percent real,
      units text
    )
  `, { fdwConnection: MAPS_FDW_CONNECTION })

  return { viewID, mlView, mlViewColumns, mlViewFdwConnections: [MAPS_FDW_CONNECTION] }
}

const listViews = async ({ access, filter = {}, inclMeta = true }) => {
  const layers = await getKnexLayerQuery(access, filter)
  return layers.map(({ name, layer_categories, layer_id, layer_type_id }) => {
    // TODO: remove 'columns' -> use listView() to get full view
    Object.entries(options.columns).forEach(([key, column]) => {
      column.key = key
    })
    return Object.entries(layer_categories)
      .map(([categoryKey, { table, name: catName, resolution }]) => {
        const geoType = `ca-${resolution}`
        const geo = geoMapping[geoType]
        const view = {
          // required
          name: `${name} // ${catName}`,
          view: {
            type: 'layer',
            id: `layer_${layer_id}_${categoryKey}`,
            layer_id,
            layer_type_id,
            resolution,
            table,
            categoryKey,
          },
        }
        if (inclMeta) {
          view.columns = {
            ...options.columns,
            geo_id: {
              key: 'geo_id',
              category: geo.idType,
              geo_type: geoType,
            },
            [`geo_ca_${resolution}`]: {
              key: `geo_ca_${resolution}`,
              category: geo.idType,
              geo_type: geoType,
            },
          }
        }
        return view
      })
  }).reduce((agg, view) => [...agg, ...view], [])
}

const getView = async (access, viewID) => {
  const [, layerIDStr, categoryKeyStr] = viewID.match(/^layer_(\d+)_(\d+)$/) || []
  // eslint-disable-next-line radix
  const layer_id = parseInt(layerIDStr, 10)
  // eslint-disable-next-line radix
  const categoryKey = parseInt(categoryKeyStr, 10)
  if (!layer_id || Number.isNaN(categoryKey)) {
    throw apiError(`Invalid view: ${viewID}`, 403)
  }

  const [layer] = await getKnexLayerQuery(access, { layer_id })
  if (!layer || !(categoryKey in layer.layer_categories)) {
    throw apiError('Access to layer not allowed', 403)
  }

  Object.entries(options.columns).forEach(([key, column]) => {
    column.key = key
  })

  const { name, layer_categories, layer_type_id } = layer
  const { table, name: catName, resolution } = layer_categories[categoryKey]
  const geoType = `ca-${resolution}`
  const geo = geoMapping[geoType]

  return {
    // required
    name: `${name} // ${catName}`,
    view: {
      type: 'layer',
      id: `layer_${layer_id}_${categoryKey}`,
      layer_id,
      layer_type_id,
      resolution,
      table,
      categoryKey,
    },
    columns: {
      ...options.columns,
      geo_id: {
        key: 'geo_id',
        category: geo.idType,
        geo_type: geoType,
      },
      [`geo_ca_${resolution}`]: {
        key: `geo_ca_${resolution}`,
        category: geo.idType,
        geo_type: geoType,
      },
    },
    // meta
  }
}

const options = {
  columns: {
    total: { category: CAT_NUMERIC },
    id: { category: CAT_NUMERIC }, // geo ggid
    title: { category: CAT_STRING },
    value: { category: CAT_NUMERIC },
    percent: { category: CAT_NUMERIC },
    units: { category: CAT_STRING },
  },
}

module.exports = {
  getQueryView,
  listViews,
  getView,
}
