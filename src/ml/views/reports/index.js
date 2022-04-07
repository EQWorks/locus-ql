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
// reverse lookup
const reportViewTypeValues = Object.entries(reportViewTypes).reduce((acc, [k, v]) => {
  acc[v] = k
  return acc
}, {})

const AOIViewTypes = {
  [reportTypes.WI]: viewTypes.REPORT_WI_AOI,
  [reportTypes.VWI]: viewTypes.REPORT_VWI_AOI,
}
// reverse lookup
const AOIViewTypeValues = Object.entries(AOIViewTypes).reduce((acc, [k, v]) => {
  acc[v] = k
  return acc
}, {})

const reportViewCategories = {
  [reportTypes.WI]: viewCategories.REPORT_WI,
  [reportTypes.VWI]: viewCategories.REPORT_VWI,
  [reportTypes.XWI]: viewCategories.REPORT_XWI,
}

const parseViewID = (viewID) => {
  const [, type, aoi, layerIDStr, reportIDStr] =
    viewID.match(/^report(wi|vwi|xwi)(aoi)?_(\d+)_(\d+)$/) || []
  // eslint-disable-next-line radix
  const layerID = parseInt(layerIDStr, 10)
  // eslint-disable-next-line radix
  const reportID = parseInt(reportIDStr, 10)
  const reportType = reportTypes[type.toUpperCase()]
  const isAOI = aoi === 'aoi'
  if (!layerID || !reportID || (isAOI && !(reportType in AOITables))) {
    throw apiError(`Invalid view: ${viewID}`, 400)
  }
  return { layerID, reportID, reportType, isAOI }
}

const getReports = (wl, cu, { layerID, reportID, reportType = reportTypes.WI, hasAOI }) => {
  const whereFilters = { 'report.type': reportType }
  if (reportID) {
    whereFilters['rt.report_id'] = reportID
  }
  if (layerID) {
    whereFilters.layer_id = layerID
  }
  if (hasAOI && !(reportType in AOITables)) {
    return
  }
  const groupByCols = [
    'layer.name',
    'layer.customer',
    'layer.whitelabel',
    'layer.layer_id',
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
  const reportQuery = knex('public.layer')
  reportQuery.column(groupByCols)
  reportQuery.select(knex.raw(`
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
    reportQuery.select(knex.raw(`
      COALESCE(
        ARRAY_AGG(DISTINCT rt.vendor) FILTER (WHERE rt.vendor != ''), '{}'
      ) AS vendors
    `))
    reportQuery.select(knex.raw(`
      COALESCE(
        ARRAY_AGG(DISTINCT ARRAY[rt.campaign, camps.name])
        FILTER (WHERE rt.campaign != ''),
        '{}'
      ) AS camps
    `))
  }

  reportQuery.leftJoin('public.customers', 'layer.customer', 'customers.customerid')
  reportQuery.leftJoin('public.report', 'layer.report_id', 'report.report_id')
  reportQuery.leftJoin('public.layer_type', 'layer.layer_type_id', 'layer_type.layer_type_id')
  reportQuery.innerJoin(
    { rt: `public.${reportTables[reportType]}` },
    'report.report_id',
    'rt.report_id',
  )

  if (reportType === reportTypes.VWI) {
    reportQuery.leftJoin('public.camps', knex.raw('camps.camp_id::text'), 'rt.campaign')
  }

  reportQuery.where(whereFilters)
  reportQuery.whereNull('layer.parent_layer')
  if (wl !== -1) {
    reportQuery.whereRaw('layer.whitelabel = ANY (?)', [wl])
    if (cu !== -1) {
      reportQuery.whereRaw('(layer.customer = ANY (?) OR customers.agencyid = ANY (?))', [cu, cu])
    }
  }
  reportQuery.groupByRaw(groupByCols.map((_, i) => i + 1).join(', '))

  // hasAOI
  if (reportType in AOITables) {
    const aoiQuery = knex.raw(`
      EXISTS (
        SELECT 1
        FROM public.${AOITables[reportType]} aoi
        WHERE
          aoi.report_id = layer.report_id
          AND aoi.aoi_id IS NOT NULL
        LIMIT 1
      )
    `)
    if (hasAOI) {
      reportQuery.having(aoiQuery)
      reportQuery.select(knex.raw('TRUE AS has_aoi'))
    } else {
      reportQuery.select({ has_aoi: aoiQuery })
    }
  } else {
    reportQuery.select(knex.raw('FALSE AS has_aoi'))
  }
  return knexWithCache(reportQuery)
}

const getViewObject = ({
  name,
  customer,
  whitelabel,
  layer_id,
  layer_type_name,
  report_id,
  type: report_type,
  dates,
  vendors,
  camps,
  report_name,
  report_created,
  report_updated,
  report_description,
  tld,
  poi_list_id,
  has_aoi: hasAOI,
}, { inclMeta = true, isAOI = false }) => {
  const type = (isAOI ? AOIViewTypes : reportViewTypes)[report_type]
  const view = {
    name: `${name} (${layer_type_name}${isAOI ? ' AOI' : ''})`,
    view: {
      id: `${type}_${layer_id}_${report_id}`,
      type,
      category: reportViewCategories[report_type],
      report_id,
      layer_id,
    },
  }
  if (inclMeta) {
    const columns = Object
      .entries(isAOI
        ? { ...reportModules[report_type].columns, ...reportModules[report_type].aoiColumns }
        : reportModules[report_type].columns)
      .reduce((acc, [key, { category, geo_type }]) => {
        acc[key] = { key, category, geo_type }
        return acc
      }, {})
    Object.assign(view, {
      columns,
      report_type,
      report_name,
      whitelabel,
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
}

const listViews = async ({ access, filter: { type, ...filter }, inclMeta = true }) => {
  const { whitelabel, customers } = access
  const isAOI = type in AOIViewTypeValues
  const reportType = (isAOI ? AOIViewTypeValues : reportViewTypeValues)[type]
  const reports = await getReports(whitelabel, customers, { reportType, hasAOI: isAOI, ...filter })
  return reports.reduce((acc, report) => {
    acc.push(getViewObject(report, { inclMeta, isAOI }))
    return acc
  }, [])
}

const getView = async (access, viewID) => {
  const { whitelabel, customers } = access
  if (whitelabel !== -1 && (!whitelabel.length || (customers !== -1 && !customers.length))) {
    throw apiError('Invalid access permissions', 403)
  }
  const { layerID, reportID, reportType, isAOI } = parseViewID(viewID)
  const [report] = await getReports(
    whitelabel,
    customers,
    { layerID, reportID, reportType, hasAOI: isAOI },
  )
  if (!report) {
    throw apiError('Report not found', 404)
  }
  return getViewObject(report, { inclMeta: true, isAOI })
}

const getQueryView = async (access, viewID, queryColumns, engine) => {
  const { whitelabel, customers } = access
  if (whitelabel !== -1 && (!whitelabel.length || (customers !== -1 && !customers.length))) {
    throw apiError('Invalid access permissions', 403)
  }
  const { layerID, reportID, reportType, isAOI } = parseViewID(viewID)
  const [report] = await getReports(
    whitelabel,
    customers,
    { layerID, reportID, reportType, hasAOI: isAOI },
  )
  if (!report) {
    throw apiError('Report not found', 404)
  }
  const reportColumns = isAOI
    ? { ...reportModules[reportType].columns, ...reportModules[reportType].aoiColumns }
    : reportModules[reportType].columns
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
    INNER JOIN ${catalog}public.${(isAOI ? AOITables : reportTables)[reportType]} AS wi ON
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
