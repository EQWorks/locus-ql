const { knex } = require('../../../util/db')
const { useAPIErrorOptions } = require('../../../util/api-error')
const { useCacheOptions } = require('../../../util/cache')
const { viewTypes, viewCategories } = require('../taxonomies')
const { filterViewColumns } = require('../utils')

const wi = require('./reportwi')
const vwi = require('./reportvwi')
const xwi = require('./reportxwi')


const { apiError } = useAPIErrorOptions({ tags: { service: 'ql' } })
const { knexWithCache } = useCacheOptions({ ttl: 600 }) // 10 minutes

const reportTypes = {
  WI: 1,
  VWI: 2,
  XWI: 4,
}

const reportModules = {
  [reportTypes.WI]: wi,
  [reportTypes.VWI]: vwi,
  [reportTypes.XWI]: xwi,
}

const reportTables = {
  [reportTypes.WI]: 'report_wi',
  [reportTypes.VWI]: 'report_vwi',
  [reportTypes.XWI]: 'report_xwi',
}

const AOITables = {
  [reportTypes.WI]: 'report_wi_aoi',
  [reportTypes.VWI]: 'report_vwi_aoi',
}

const reportViewTypes = {
  [reportTypes.WI]: viewTypes.REPORT_WI,
  [reportTypes.VWI]: viewTypes.REPORT_VWI,
  [reportTypes.XWI]: viewTypes.REPORT_XWI,
}

const reportViewCategories = {
  [reportTypes.WI]: viewCategories.REPORT_WI,
  [reportTypes.VWI]: viewCategories.REPORT_VWI,
  [reportTypes.XWI]: viewCategories.REPORT_XWI,
}

const parseViewID = (viewID) => {
  const [, type, layerIDStr, reportIDStr] = viewID.match(/^report(wi|vwi|xwi)_(\d+)_(\d+)$/) || []
  // eslint-disable-next-line radix
  const layerID = parseInt(layerIDStr, 10)
  // eslint-disable-next-line radix
  const reportID = parseInt(reportIDStr, 10)
  if (!layerID || !reportID) {
    throw apiError(`Invalid view: ${viewID}`, 400)
  }
  return { layerID, reportID, reportType: reportTypes[type.toUpperCase()] }
}

const getReportLayers = (wl, cu, { layerID, reportID, reportType = reportTypes.WI }) => {
  const whereFilters = { 'report.type': reportType }
  if (reportID) {
    whereFilters['rt.report_id'] = reportID
  }
  if (layerID) {
    whereFilters.layer_id = layerID
  }
  const groupByCols = [
    'layer.name',
    'layer.layer_id',
    'layer.customer',
    'layer.whitelabel',
    { layer_type_name: 'layer_type.name' },
    'layer.report_id',
    'report.type',
    { report_name: 'report.name' },
    { report_created: 'report.created' },
    { report_updated: 'report.updated' },
    { report_description: 'report.description' },
    'report.tld',
    'report.poi_list_id',
  ]
  const layerQuery = knex('public.layer')
  layerQuery.column(groupByCols)
  layerQuery.select(knex.raw(`
    COALESCE(
      ARRAY_AGG(DISTINCT ARRAY[
        rt.start_date::varchar,
        rt.end_date::varchar,
        rt.date_type::varchar
      ])
      FILTER (WHERE rt.start_date IS NOT null),
      '{}'
    ) AS dates
  `))

  if (reportType === reportTypes.VWI) {
    layerQuery.select(knex.raw(`
      COALESCE(
        ARRAY_AGG(DISTINCT rt.vendor) FILTER (WHERE rt.vendor != ''), '{}'
      ) AS vendors
    `))
    layerQuery.select(knex.raw(`
      COALESCE(
        ARRAY_AGG(DISTINCT ARRAY[rt.campaign, camps.name])
        FILTER (WHERE rt.campaign != ''),
        '{}'
      ) AS camps
    `))
  }

  layerQuery.leftJoin('public.customers', 'layer.customer', 'customers.customerid')
  layerQuery.leftJoin('public.report', 'layer.report_id', 'report.report_id')
  layerQuery.leftJoin('public.layer_type', 'layer.layer_type_id', 'layer_type.layer_type_id')
  layerQuery.innerJoin(
    { rt: `public.${reportTables[reportType]}` },
    'report.report_id',
    'rt.report_id',
  )

  if (reportType === reportTypes.VWI) {
    // layerQuery.joinRaw(`LEFT JOIN camps on camps.camp_id::text = rt.campaign`)
    layerQuery.leftJoin('public.camps', knex.raw('camps.camp_id::text'), 'rt.campaign')
  }

  layerQuery.where(whereFilters)
  layerQuery.whereNull('layer.parent_layer')
  if (wl !== -1) {
    layerQuery.whereRaw('layer.whitelabel = ANY (?)', [wl])
    if (cu !== -1) {
      layerQuery.whereRaw('(layer.customer = ANY (?) OR customers.agencyid = ANY (?))', [cu, cu])
    }
  }
  layerQuery.groupByRaw(groupByCols.map((_, i) => i + 1).join(', '))
  return knexWithCache(layerQuery)
}

// TODO: fetch aoi data using another endpoint
const hasAOIData = async (wl, cu, layerID, reportID, reportType) => {
  if (!(reportType in AOITables)) {
    // not supported
    return false
  }
  const whereFilters = ['aoi.report_id = ?', 'l.layer_id = ?']
  const whereValues = [reportID, layerID]
  if (wl !== -1) {
    whereValues.push(wl)
    whereFilters.push('l.whitelabel = ANY (?)')
    // if (cu !== -1) {
    //   whereValues.push(cu)
    //   whereFilters.push('l.customer = ANY (?)')
    // }
  }
  const [{ exists } = {}] = await knexWithCache(knex.raw(`
    SELECT EXISTS (
      SELECT
        l.layer_id,
        aoi.aoi_id
      FROM public.layer l
      JOIN public.${AOITables[reportType]} aoi ON aoi.report_id = l.report_id
      WHERE ${whereFilters.join(' AND ')}
    )
  `, whereValues))
  return exists
}

const listViews = async ({ access, filter, inclMeta = true }) => {
  const { whitelabel, customers } = access
  if (whitelabel !== -1 && (!whitelabel.length || (customers !== -1 && !customers.length))) {
    throw apiError('Invalid access permissions', 403)
  }
  const reportLayers = await getReportLayers(whitelabel, customers, filter)
  return Promise.all(reportLayers.map(async ({
    name,
    layer_id,
    report_id,
    type: report_type,
    dates,
    vendors,
    camps,
    layer_type_name,
    report_name,
    whitelabel: wl,
    customer,
    report_created,
    report_updated,
    report_description,
    tld,
    poi_list_id,
  }) => {
    const view = {
      name: `${name} (${layer_type_name})`,
      view: {
        id: `${reportViewTypes[report_type]}_${layer_id}_${report_id}`,
        type: reportViewTypes[report_type],
        category: reportViewCategories[report_type],
        report_id,
        layer_id,
      },
    }
    if (inclMeta) {
      const hasAOI = await hasAOIData([wl], [customer], layer_id, report_id, report_type)
      // const columns = hasAOI ? { ...options.columns, ...options.aoi } : options.columns
      const { columns } = reportModules[report_type]
      Object.assign(view, {
        columns,
        report_type,
        report_name,
        whitelabel: wl,
        customer,
        report_created,
        report_updated,
        report_description,
        tld,
        poi_list_id,
        dates: dates.map(([start, end, dType]) => ({ start, end, dType: parseInt(dType) })),
        hasAOI,
      })
      if (report_type === reportTypes.VWI) {
        Object.assign(view, {
          vendors,
          camps: camps.map(([campID, name]) => ({ campID: parseInt(campID), name })),
        })
      }
    }
    return view
  }))
}

const getView = async (access, viewID) => {
  const { whitelabel, customers } = access
  if (whitelabel !== -1 && (!whitelabel.length || (customers !== -1 && !customers.length))) {
    throw apiError('Invalid access permissions', 403)
  }
  const { layerID, reportID, reportType } = parseViewID(viewID)

  const [layer] = await getReportLayers(whitelabel, customers, { layerID, reportID, reportType })
  if (!layer) {
    throw apiError('Access to layer not allowed', 403)
  }
  const {
    name,
    dates,
    vendors,
    camps,
    layer_type_name,
    report_name,
    whitelabel: wl,
    customer,
    report_created,
    report_updated,
    report_description,
    tld,
    poi_list_id,
  } = layer
  const hasAOI = await hasAOIData([wl], [customer], layerID, reportID, reportType)
  // const columns = hasAOI ? { ...options.columns, ...options.aoi } : options.columns
  const { columns } = reportModules[reportType]

  const view = {
    name: `${name} (${layer_type_name})`,
    view: {
      id: `${reportViewTypes[reportType]}_${layerID}_${reportID}`,
      type: reportViewTypes[reportType],
      category: reportViewCategories[reportType],
      report_id: reportID,
      layer_id: layerID,
    },
    columns,
    report_type: reportType,
    report_name,
    whitelabel: wl,
    customer,
    report_created,
    report_updated,
    report_description,
    tld,
    poi_list_id,
    dates: dates.map(([start, end, dateType]) => ({ start, end, dateType: parseInt(dateType) })),
    hasAOI,
  }

  if (reportType === reportTypes.VWI) {
    Object.assign(view, {
      vendors,
      camps: camps.map(([campID, name]) => ({ campID: parseInt(campID), name })),
    })
  }
  return view
}

const getQueryView = async (access, viewID, queryColumns, engine) => {
  const { whitelabel, customers } = access
  if (whitelabel !== -1 && (!whitelabel.length || (customers !== -1 && !customers.length))) {
    throw apiError('Invalid access permissions', 403)
  }
  const { layerID, reportID, reportType } = parseViewID(viewID)
  const [layer] = await getReportLayers(whitelabel, customers, { layerID, reportID, reportType })
  if (!layer) {
    throw apiError('Access to layer not allowed', 403)
  }
  const { columns: reportColumns } = reportModules[reportType]
  const columns = filterViewColumns(reportColumns, queryColumns)
  if (!Object.keys(columns).length) {
    throw apiError(`No column selected from view: ${viewID}`, 400)
  }
  // all values are safe
  const where = [
    `r.type = '${reportType}'`,
    `r.report_id = ${reportID}`,
    `layer.layer_id = ${layerID}`]
  // const whereValues = [reportType, reportID, layerID]
  if (whitelabel !== -1) {
    // whereValues.push(whitelabel)
    where.push(`layer.whitelabel = ANY (ARRAY[${whitelabel.join(', ')}])`)
    if (customers !== -1) {
      // whereValues.push(customers)
      where.push(`layer.customer = ANY (ARRAY[${customers.join(', ')}])`)
    }
  }

  const [exps, joins] = Object.entries(columns).reduce(
    (acc, [alias, { [engine]: engineExp, expression, join }]) => {
      acc[0].add(`${engineExp || expression} AS ${alias}`)
      if (join) {
        acc[1].add(join[engine] || join.expression)
      }
      return acc
    },
    [new Set(), new Set()],
  )

  const catalog = engine === 'trino' ? 'locus_place.' : ''

  const query = `
    SELECT
      ${[...exps].join(',\n ')}
    FROM ${catalog}public.poi
    INNER JOIN ${catalog}public.poi_list_map ON poi.poi_id = poi_list_map.poi_id
    INNER JOIN ${catalog}public.layer ON layer.poi_list_id = poi_list_map.poi_list_id
    INNER JOIN ${catalog}public.report AS r ON r.report_id = layer.report_id
    INNER JOIN ${catalog}public.${reportTables[reportType]} AS wi ON
      wi.report_id = r.report_id
      AND wi.poi_id = poi.poi_id
    ${[...joins].join(',\n ')}
    WHERE ${where.join(' AND ')}
  `

  return { viewID, query, columns }
}

module.exports = {
  listViews,
  getView,
  getQueryView,
}
