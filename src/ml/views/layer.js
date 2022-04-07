/* eslint-disable indent */
const { knex } = require('../../util/db')
const { CAT_STRING, CAT_NUMERIC } = require('../type')
const { filterViewColumns } = require('./utils')
const { useAPIErrorOptions } = require('../../util/api-error')
const { knexWithCache } = require('../../util/cache')
const geoTables = require('../geo-tables')
const { viewTypes, viewCategories } = require('./taxonomies')


const { apiError } = useAPIErrorOptions({ tags: { service: 'ql' } })


const viewCategoryToLayerType = {
  [viewCategories.LAYER_DEMOGRAPHIC]: 18,
  [viewCategories.LAYER_DEMOGRAPHIC]: 19,
  [viewCategories.LAYER_PROPENSITY]: 20,
}
const layerTypeToViewCategory = Object.entries(viewCategoryToLayerType).reduce((acc, [k, v]) => {
  acc[v] = k
  return acc
}, {})

const getLayerColumns = (table, resolution) => {
  const geoType = `ca-${resolution}`
  const { idType: geoCategory } = geoTables[geoType]

  // init with geo columns
  const columns = {
    id: { category: CAT_NUMERIC }, // geo ggid
    geo_id: { category: geoCategory, geo_type: geoType },
    [`geo_ca_${resolution}`]: { category: geoCategory, geo_type: geoType },
    title: { category: CAT_STRING },
  }

  // append columns based on layer table
  switch (true) {
    case table.startsWith('persona'):
      Object.assign(columns, { has_persona: { category: CAT_NUMERIC } })
      break

    default:
      Object.assign(columns, {
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

const parseViewID = (viewID) => {
  const [, layerIDStr, categoryKeyStr] = viewID.match(/^layer_(\d+)_(\d+)$/) || []
  // eslint-disable-next-line radix
  const layerID = parseInt(layerIDStr, 10)
  // eslint-disable-next-line radix
  const categoryKey = parseInt(categoryKeyStr, 10)
  if (Number.isNaN(layerID) || Number.isNaN(categoryKey)) {
    throw apiError(`Invalid view: ${viewID}`, 400)
  }
  return { layerID, categoryKey }
}

const getLayers = async (access, { categories, layerID } = {}) => {
  const { whitelabel, customers, email = '' } = access
  // for all layers that user has access to
  // get all categories and expand it out as a single row
  // each view should be a layer + category

  const filters = {}
  if (layerID) {
    filters['layer.layer_id'] = layerID
  }

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
  layerQuery.where(filters)
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

      // subscribed layers
      layerQuery.with('subscribed_layers', knex.raw(`
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
      `, { whitelabel, customers }))

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
          layer.layer_id = ANY (SELECT * FROM subscribed_layers)
        )
      `, { whitelabel, customers, email })
    }
  }
  return knexWithCache(layerQuery, { ttl: 600 }) // 10 minutes
}

const getQueryView = async (access, viewID, queryColumns, engine) => {
  const { layerID, categoryKey } = parseViewID(viewID)
  const [layer] = await getLayers(access, { layerID })
  if (!layer) {
    throw apiError('Access to layer not allowed', 403)
  }

  const category = layer.layer_categories[categoryKey]
  if (!category) {
    throw apiError('Invalid layer category', 400)
  }

  const { table, slug, resolution } = category
  // inject view columns
  const layerColumns = getLayerColumns(table || slug, resolution)
  const columns = filterViewColumns(layerColumns, queryColumns)
  if (!Object.keys(columns).length) {
    throw apiError(`No column selected from view: ${viewID}`, 400)
  }

  const geo = geoTables[`ca-${resolution}`]
  // add schema if missing
  const schema = (table || slug).indexOf('.') === -1 ? '"public".' : ''

  let columnExpressions = engine === 'trino'
      ? "json_extract(L.summary_data, '$.main_number_pcnt.title}') AS title,"
      : "L.summary_data::json #>> '{main_number_pcnt, title}' AS title,"

  if ((table || slug).startsWith('persona')) {
    columnExpressions = `
      ${columnExpressions}
      1 AS has_persona'
    `
  } else {
    columnExpressions = engine === 'trino'
      ? `
        ${columnExpressions}
        CAST(
          json_extract(L.summary_data, '$.main_number_pcnt.value}') AS double precision
        ) AS value,
        CAST(
          json_extract(L.summary_data, '$.main_number_pcnt.percent}') AS double precision
        ) AS percent,
        json_extract(L.summary_data, '$.main_number_pcnt.units}') AS units
      `
      : `
        ${columnExpressions}
        (L.summary_data::json #>> '{main_number_pcnt, value}')::real AS value,
        (L.summary_data::json #>> '{main_number_pcnt, percent}')::real AS percent,
        L.summary_data::json #>> '{main_number_pcnt, units}' AS units
      `
  }

  const catalog = engine === 'trino' ? 'locus_place.' : ''

  const query = `
    SELECT
      GM.ggid AS id,
      L.geo_id,
      L.geo_id AS geo_ca_${resolution},
      ${columnExpressions}
    FROM ${catalog}${schema}${table || slug} as L
    INNER JOIN ${catalog}config.ggid_map as GM ON
      GM.type = '${resolution}' AND GM.local_id = L.geo_id
    INNER JOIN ${catalog}"${geo.schema}"."${geo.table}" as GT ON GT."${geo.idColumn}" = L.geo_id
    WHERE GT.wkb_geometry IS NOT NULL
  `

  return { viewID, query, columns }
}

const listViews = async ({ access, filter: { categories } = {}, inclMeta = true }) => {
  const layers = await getLayers(access, { categories })
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
  const { layerID, categoryKey } = parseViewID(viewID)

  const [layer] = await getLayers(access, { layerID })
  if (!layer || !(categoryKey in layer.layer_categories)) {
    throw apiError('Access to layer not allowed', 403)
  }

  const { name, layer_categories, layer_type_id } = layer
  const { table, slug, name: catName, resolution } = layer_categories[categoryKey]

  return {
    // required
    name: `${name} // ${catName}`,
    view: {
      id: `${viewTypes.LAYER}_${layerID}_${categoryKey}`,
      type: viewTypes.LAYER,
      category: layerTypeToViewCategory[layer_type_id],
      layer_id: layerID,
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
