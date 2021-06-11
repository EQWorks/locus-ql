const { apiError, APIError } = require('../../util/api-error')


// one module = one type
/**
 * @enum
 */
const viewTypes = {
  EXT: 'ext',
  GEO: 'geo',
  LAYER: 'layer',
  LOGS: 'logs',
  REPORT_VWI: 'reportvwi',
  REPORT_WI: 'reportwi',
  REPORT_XWI: 'reportxwi',
  WEATHER: 'weather',
}

// reverse lookup
const viewTypeValues = Object.entries(viewTypes).reduce((acc, [k, v]) => {
  acc[v] = k
  return acc
}, {})

/**
 * @enum
 */
const viewCategories = {
  ATOM_CAMPAIGNS: 'atom_campaigns',
  ATOM_VAST_EVENTS: 'atom_vast_events',
  CUSTOMER_DATA: 'customer_data',
  EQ_DATA: 'eq_data',
  EXT: 'external_data',
  GEO: 'geographic',
  LAYERS: 'layers',
  LOCUS_BEACONS: 'locus_beacons',
  MARKETING_DATA: 'marketing_data',
  MARKETPLACE_DATA: 'marketplace_data',
  LAYER_DEMOGRAPHIC: 'layer_demographic',
  LAYER_PERSONA: 'layer_persona',
  LAYER_PROPENSITY: 'layer_propensity',
  REPORT: 'report',
  REPORT_VWI: 'report_vwi',
  REPORT_WI: 'report_wi',
  REPORT_XWI: 'report_xwi',
  WEATHER: 'weather',
}

// reverse lookup
const viewCategoryValues = Object.entries(viewCategories).reduce((acc, [k, v]) => {
  acc[v] = k
  return acc
}, {})

const rootViewCategories = [
  viewCategories.EQ_DATA,
  viewCategories.MARKETPLACE_DATA,
  viewCategories.CUSTOMER_DATA,
]

const viewCategoryDesc = {
  [viewCategories.EQ_DATA]: {
    name: 'EQ Data',
    children: [viewCategories.REPORT, viewCategories.MARKETING_DATA],
  },
  [viewCategories.REPORT]: {
    name: 'Reports',
    children: [
      viewCategories.REPORT_WI,
      viewCategories.REPORT_VWI,
      viewCategories.REPORT_XWI,
    ],
  },
  [viewCategories.REPORT_WI]: {
    name: 'Walk-in Reports',
    type: viewTypes.REPORT_WI,
  },
  [viewCategories.REPORT_VWI]: {
    name: 'Verified Walk-in Reports',
    type: viewTypes.REPORT_VWI,
  },
  [viewCategories.REPORT_XWI]: {
    name: 'Cross Walk-in Reports',
    type: viewTypes.REPORT_XWI,
  },
  [viewCategories.MARKETING_DATA]: {
    name: 'Marketing Data',
    children: [
      viewCategories.ATOM_CAMPAIGNS,
      viewCategories.ATOM_VAST_EVENTS,
      viewCategories.LOCUS_BEACONS,
    ],
  },
  [viewCategories.ATOM_CAMPAIGNS]: {
    name: 'Atom Campaigns',
    type: viewTypes.LOGS,
  },
  [viewCategories.ATOM_VAST_EVENTS]: {
    name: 'Atom Vast Events',
    type: viewTypes.LOGS,
  },
  [viewCategories.LOCUS_BEACONS]: {
    name: 'Locus Beacons',
    type: viewTypes.LOGS,
  },
  [viewCategories.MARKETPLACE_DATA]: {
    name: 'Marketplace Data',
    children: [viewCategories.LAYERS],
  },
  [viewCategories.LAYERS]: {
    name: 'Layers',
    children: [
      viewCategories.LAYER_DEMOGRAPHIC,
      viewCategories.LAYER_PROPENSITY,
      viewCategories.WEATHER,
      viewCategories.GEO,
    ],
  },
  [viewCategories.LAYER_DEMOGRAPHIC]: {
    name: 'Demographic',
    type: viewTypes.LAYER,
  },
  [viewCategories.LAYER_PROPENSITY]: {
    name: 'Propensity',
    type: viewTypes.LAYER,
  },
  [viewCategories.WEATHER]: {
    name: 'Weather',
    type: viewTypes.WEATHER,
  },
  [viewCategories.GEO]: {
    name: 'Geographic',
    type: viewTypes.GEO,
  },
  [viewCategories.CUSTOMER_DATA]: {
    name: 'Customer Data',
    children: [viewCategories.EXT],
  },
  [viewCategories.EXT]: {
    name: 'External Data',
    type: viewTypes.EXT,
  },
}

// walk tree with callback using accumulator
// callback: (nodeKey, stackFrameAcc) => [stackFrameAcc, nextStackFrameAcc]
const reduceViewCategoryTree = (cb, initAcc, root) => {
  // stack = [[[child1, child2...], array to append child nodes to], ]
  // visit last child first so can pop entry from children in bubble-up phase instead
  // of shifting (reallocation cost)
  const stack = []

  // root node
  const isRoot = !(root in viewCategoryDesc)
  const [initAccOut, nextAcc] = cb(isRoot ? 'root' : root, initAcc)
  stack.push([
    isRoot ? [...rootViewCategories] : [...(viewCategoryDesc[root].children || [])],
    nextAcc,
  ])

  // append children
  while (stack.length) {
    // get nodes to visit from current stack frame
    const [nodesToVisit, currentAccIn] = stack.slice(-1)[0]
    // no more nodes to visit
    if (!nodesToVisit.length) {
      stack.pop()
      // remove parent from stack
      if (stack.length) {
        stack.slice(-1)[0][0].pop()
      }
      // eslint-disable-next-line no-continue
      continue
    }
    const currentNode = nodesToVisit.slice(-1)[0]
    const [currentAccOut, nextAcc] = cb(currentNode, currentAccIn)
    // replace current stack frame acc
    stack.slice(-1)[0][1] = currentAccOut
    // push node's children to stack
    stack.push([[...(viewCategoryDesc[currentNode].children || [])], nextAcc])
  }

  return initAccOut
}

const getViewCategoryTree = root => reduceViewCategoryTree(
  (nodeKey, currentAcc) => {
    const { name, type } = viewCategoryDesc[nodeKey] || {}
    const node = {
      id: nodeKey,
      name,
      type,
      children: [],
    }
    // root case
    if (!currentAcc) {
      return [node, node.children]
    }
    currentAcc.push(node)
    return [currentAcc, node.children]
  },
  undefined,
  root,
)

const listViewCategories = root => reduceViewCategoryTree(
  (nodeKey, currentAcc) => {
    if (nodeKey !== 'root') {
      currentAcc.push(nodeKey)
    }
    return [currentAcc, currentAcc]
  },
  [],
  root,
)

// to use in SQL filter as 'WHERE type = ANY(<array>)
const listViewCategoriesByViewType = root => reduceViewCategoryTree(
  (nodeKey, currentAcc) => {
    if (nodeKey !== 'root' && viewCategoryDesc[nodeKey].type) {
      const { type } = viewCategoryDesc[nodeKey]
      currentAcc[type] = currentAcc[type] || []
      currentAcc[type].push(nodeKey)
    }
    return [currentAcc, currentAcc]
  },
  {},
  root,
)

const getViewCategoryTreeMW = (req, res, next) => {
  try {
    const { root } = req.query
    const tree = getViewCategoryTree(root)
    res.status(200).json(tree)
  } catch (err) {
    if (err instanceof APIError) {
      return next(err)
    }
    next(apiError('Failed to retrieve view categories', 500))
  }
}

module.exports = {
  viewTypes,
  viewTypeValues,
  viewCategories,
  viewCategoryValues,
  getViewCategoryTree,
  getViewCategoryTreeMW,
  listViewCategories,
  listViewCategoriesByViewType,
}
