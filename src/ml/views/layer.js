/* eslint-disable indent */
/* eslint-disable no-use-before-define */
const { knex } = require('../../util/db')
const { CAT_STRING, CAT_NUMERIC } = require('../type')
const { apiError } = require('../../util/api-error')
const { knexWithCache } = require('../cache')
const { geoMapping } = require('../geo')
const { viewTypes, viewCategories } = require('./taxonomies')


const getLayerColumns = (table, resolution) => {
  const geoType = `ca-${resolution}`
  const { idType: geoCategory } = geoMapping[geoType]

  // init with geo columns
  const columns = {
    geo_id: { category: geoCategory, geo_type: geoType },
    [`geo_ca_${resolution}`]: { category: geoCategory, geo_type: geoType },
  }

  // append columns based on layer table
  switch (true) {
    case table.startsWith('persona'):
      Object.assign(columns, {
        id: { category: CAT_NUMERIC }, // geo ggid
        title: { category: CAT_STRING },
        has_persona: { category: CAT_NUMERIC },
      })
      break

    default:
      Object.assign(columns, {
        id: { category: CAT_NUMERIC }, // geo ggid
        title: { category: CAT_STRING },
        // total: { category: CAT_NUMERIC },
        value: { category: CAT_NUMERIC },
        percent: { category: CAT_NUMERIC },
        units: { category: CAT_STRING },
      })
  }

  // append column key to column object
  Object.entries(columns).forEach(([key, column]) => {
    column.key = key
  })

  return columns
}

const viewCategoryToLayerType = {
  [viewCategories.LAYER_DEMOGRAPHIC]: 18,
  [viewCategories.LAYER_DEMOGRAPHIC]: 19,
  [viewCategories.LAYER_PROPENSITY]: 20,
}
const layerTypeToViewCategory = Object.entries(viewCategoryToLayerType).reduce((acc, [k, v]) => {
  acc[v] = k
  return acc
}, {})

const getKnexLayerQuery = async (access, { categories, ...filter } = {}) => {
  const { whitelabel, customers, email = '' } = access
  // for all layers that user has access to
  // get all categories and expand it out as a single row
  // each view should be a layer + category

  // const layerTypes = [18, 19, 20] // demo, prop and persona
  const layerTypes = categories
    ? categories.map(cat => viewCategoryToLayerType[cat])
    : [18, 19, 20] // demo, prop and persona

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
  const viewID = `${viewTypes.LAYER}_${layer_id}_${categoryKey}`
  const [layer] = await getKnexLayerQuery(access, { layer_id })
  if (!layer) {
    throw apiError('Access to layer not allowed', 403)
  }

  const category = layer.layer_categories[categoryKey]
  if (!category) {
    throw apiError('Invalid layer category', 403)
  }

  const { table, slug, resolution } = category
  // inject view columns
  const mlViewColumns = getLayerColumns(table || slug, resolution)

  const geo = geoMapping[`ca-${resolution}`]
  // add schema if missing
  const schema = (table || slug).indexOf('.') === -1 ? 'public.' : ''

  const columnExpressions = (table || slug).startsWith('persona')
    ? '1 AS has_persona'
    : `
      L.summary_data::json #>> '{main_number_pcnt, value}' AS value,
      L.summary_data::json #>> '{main_number_pcnt, percent}' AS percent,
      L.summary_data::json #>> '{main_number_pcnt, units}' AS units
    `

  const mlView = knex.raw(`
    SELECT
      GM.ggid AS id,
      L.geo_id,
      L.geo_id AS geo_ca_${resolution},
      L.summary_data::json #>> '{main_number_pcnt, title}' AS title,
      ${columnExpressions}
    FROM ${schema}${table || slug} as L
    INNER JOIN config.ggid_map as GM ON GM.type = '${resolution}' AND GM.local_id = L.geo_id
    INNER JOIN ${geo.schema}.${geo.table} as GT ON GT.${geo.idColumn} = L.geo_id
    WHERE GT.wkb_geometry IS NOT NULL
  `)

  return { viewID, mlView, mlViewColumns }
}

const listViews = async ({ access, filter = {}, inclMeta = true }) => {
  const layers = await getKnexLayerQuery(access, filter)
  return layers.map(({ name, layer_categories, layer_id, layer_type_id }) => Object
    // TODO: remove 'columns' -> use listView() to get full view
    .entries(layer_categories)
    .map(([categoryKey, { table, slug, name: catName, resolution }]) => {
      const view = {
        // required
        name: `${name} // ${catName}`,
        view: {
          id: `${viewTypes.LAYER}_${layer_id}_${categoryKey}`,
          type: viewTypes.LAYER,
          category: layerTypeToViewCategory[layer_type_id],
          layer_id,
          layer_type_id,
          resolution,
          // table,
          categoryKey,
        },
      }
      if (inclMeta) {
        view.columns = getLayerColumns(table || slug, resolution)
      }
      return view
    })).reduce((agg, view) => [...agg, ...view], [])
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

  const { name, layer_categories, layer_type_id } = layer
  const { table, slug, name: catName, resolution } = layer_categories[categoryKey]

  return {
    // required
    name: `${name} // ${catName}`,
    view: {
      id: `${viewTypes.LAYER}_${layer_id}_${categoryKey}`,
      type: viewTypes.LAYER,
      category: layerTypeToViewCategory[layer_type_id],
      layer_id,
      layer_type_id,
      resolution,
      // table,
      categoryKey,
    },
    columns: getLayerColumns(table || slug, resolution),
    // meta
  }
}

module.exports = {
  getQueryView,
  listViews,
  getView,
}
