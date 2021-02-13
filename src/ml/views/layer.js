/* eslint-disable no-use-before-define */

const { knex, mapKnex } = require('../../util/db')
const {
  CAT_STRING,
  CAT_NUMERIC,
} = require('../type')
const apiError = require('../../util/api-error')
const { knexWithCache } = require('../cache')


const RESOLUTION_TABLE_MAP = Object.freeze({
  fsa: {
    table: 'canada_geo.fsa',
    geoIDColumn: 'fsa',
  },
  postalcode: {
    table: 'canada_geo.postalcode_2018',
    geoIDColumn: 'postalcode',
  },
  ct: {
    table: 'canada_geo.ct',
    geoIDColumn: 'ctuid',
  },
  da: {
    table: 'canada_geo.da',
    geoIDColumn: 'dauid',
  },
})


const getKnexLayerQuery = async (access, filter = {}) => {
  const { whitelabel, customers, email } = access
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
      layerQuery.where('layer.whitelabel', -1)
    } else if (customers === -1) {
      layerQuery.whereRaw(`(
        layer.whitelabel = -1
        OR (layer.whitelabel = ANY (?) AND layer.account in ('0', '-1')))
      `, [whitelabel])
    } else {
      // get subscribe layers
      const { rows } = await knexWithCache(
        knex.raw(`
          SELECT type_id
          FROM market_ownership_flat MO
          WHERE MO.type = 'layer' AND MO.whitelabel = ANY (?) AND MO.customer = ANY (?)
        `, [whitelabel, customers]),
        { ttl: 1800 }, // 30 minutes
      )
      const subscribeLayerIDs = rows.map(layer => layer.type_id)
      layerQuery.joinRaw('LEFT JOIN customers as CU ON CU.customerid = layer.customer')
      layerQuery.whereRaw(`
        (
          layer.whitelabel = -1
          OR
          (
            layer.whitelabel = ANY (?)
            AND (layer.customer = ANY (?) OR CU.agencyid = ANY (?))
            AND layer.account in ('0', '-1', ?)
          )
          OR
          layer.layer_id = ANY (?)
        )
      `, [whitelabel, customers, customers, email, subscribeLayerIDs])
    }
  }
  return knexWithCache(layerQuery, { ttl: 600 }) // 30 minutes
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
  const { table: geoTable, geoIDColumn } = RESOLUTION_TABLE_MAP[resolution]

  const mlView = mapKnex.raw(`
    (
      SELECT
        GM.ggid as id,
        L.geo_id,
        L.total,
        L.summary_data::json #>> '{main_number_pcnt, title}' AS title,
        L.summary_data::json #>> '{main_number_pcnt, value}' AS value,
        L.summary_data::json #>> '{main_number_pcnt, percent}' AS percent,
        L.summary_data::json #>> '{main_number_pcnt, units}' AS units
      FROM ${table || slug} as L
      INNER JOIN config.ggid_map as GM ON GM.type = '${resolution}' AND GM.local_id = L.geo_id
      INNER JOIN ${geoTable} as GT ON GT.${geoIDColumn} = L.geo_id
      WHERE GT.wkb_geometry IS NOT NULL
    ) as ${viewID}
  `)

  return { viewID, mlView, mlViewColumns }
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
          view.columns = options.columns
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
    columns: options.columns,
    // meta
  }
}

const options = {
  columns: {
    total: { category: CAT_NUMERIC },
    id: { category: CAT_NUMERIC },
    geo_id: { category: CAT_NUMERIC },
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
