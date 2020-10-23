/* eslint-disable no-use-before-define */

const { knex, mapKnex } = require('../../util/db')
const {
  CAT_STRING,
  CAT_NUMERIC,
} = require('../type')
const apiError = require('../../util/api-error')


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
  layerQuery.column(['name', 'layer_id', 'layer_type_id', 'layer_categories'])
  layerQuery.where(filter)
  layerQuery.whereIn('layer_type_id', layerTypes)
  layerQuery.whereNotNull('layer_categories')
  layerQuery.whereNull('parent_layer')
  // subscription logic
  if (Array.isArray(whitelabel)
    && whitelabel.length > 0
    && customers === -1) {
    layerQuery.whereRaw(`(
      whitelabel = -1
      OR (whitelabel = ANY (?) AND account in ('0', '-1')))
    `, [whitelabel])
  } else if (Array.isArray(whitelabel)
    && whitelabel.length > 0
    && Array.isArray(customers)
    && customers.length > 0) {
    // get subscribe layers
    const { rows } = await knex.raw(`
      SELECT type_id
      FROM market_ownership_flat MO
      WHERE MO.type = 'layer' AND MO.whitelabel = ? AND MO.customer = ?
    `, whitelabel[0], customers[0])
    const subscribeLayerIDs = rows.map(layer => layer.type_id)
    layerQuery.joinRaw('LEFT JOIN customers as CU ON CU.customerid = layer.customer')
    layerQuery.whereRaw(`
      (
        whitelabel = -1
        OR
        (
          whitelabel = ANY (?)
          AND (customer = ANY (?) OR agencyid = ANY (?))
          AND account in ('0', '-1', ?)
        )
        OR
        layer_id = ANY (?)
      )
    `, [whitelabel], [customers], [customers], email, subscribeLayerIDs)
  }
  return layerQuery
}

const getView = async (access, reqViews, reqViewColumns, { layer_id, categoryKey }) => {
  const viewID = `layer_${layer_id}`
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
  reqViewColumns[viewID] = (viewMeta[0] || {}).columns

  const { table, slug, resolution } = category
  const { table: geoTable, geoIDColumn } = RESOLUTION_TABLE_MAP[resolution]

  reqViews[viewID] = mapKnex.raw(`
    (
      SELECT
        GM.ggid as id,
        geo_id,
        total,
        summary_data::json #>> '{main_number_pcnt, title}' AS title,
        summary_data::json #>> '{main_number_pcnt, value}' AS value,
        summary_data::json #>> '{main_number_pcnt, percent}' AS percent,
        summary_data::json #>> '{main_number_pcnt, units}' AS units
      FROM ${table || slug}
      INNER JOIN config.ggid_map as GM ON GM.type = '${resolution}' AND GM.local_id = geo_id
      INNER JOIN ${geoTable} as GT ON GT.${geoIDColumn} = geo_id
      WHERE GT.wkb_geometry IS NOT NULL
    ) as ${viewID}
  `)
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

const listView = async (access, viewID) => {
  const [, layerIDStr, categoryKeyStr] = viewID.match(/^layer_(\d+)_(\d+)$/) || []
  // eslint-disable-next-line radix
  const layer_id = parseInt(layerIDStr, 10)
  // eslint-disable-next-line radix
  const categoryKey = parseInt(categoryKeyStr, 10)
  if (!layer_id || !categoryKey) {
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
  getView,
  listViews,
  listView,
}
