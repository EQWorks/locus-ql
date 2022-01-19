const { useAPIErrorOptions } = require('../../util/api-error')


const { apiError } = useAPIErrorOptions({ tags: { service: 'ql', module: 'parser' } })

const functions = {}

functions.json_extract_path = {
  trino: (node, options) => {
    const [json, ...path] = node.args.map(e => e.to('trino', options))
    return `json_extract(${json}, '$.${path.join('.')}')`
  },
}

const geoIntersectParser = engine => (node, options) => {
  const [idA, typeA, idB, typeB] = node.args.map(e => e.to(engine, options))
  const safeTypeA = typeA.toLowerCase()
  const safeTypeB = typeB.toLowerCase()
  if (
    [safeTypeA, safeTypeB].some(t =>
      !['postalcode', 'fsa', 'ct', 'da'].includes(t.slice(1, -1)))
    || [safeTypeA, safeTypeB].every(t => ['ct', 'da'].includes(t.slice(1, -1)))
  ) {
    throw apiError('Intersection not supported', 500)
  }
  if (safeTypeA === safeTypeB) {
    return `${idA} = ${idB}`
  }
  let prefixA = 'source'
  let prefixB = 'query'
  if (safeTypeA === "'postalcode'" || safeTypeA === "'fsa'") {
    prefixA = 'query'
    prefixB = 'source'
  }
  return `
    EXISTS(
      SELECT *
      FROM canada_geo.intersection
      WHERE
        ${prefixA}_geo_type = ${safeTypeA}
        AND ${prefixA}_geo_id = ${idA}
        AND ${prefixB}_geo_type = ${safeTypeB}
        AND ${prefixB}_geo_id = ${idB}
    )
  `
}

functions.geo_intersect = {
  pg: geoIntersectParser('pg'),
  trino: geoIntersectParser('trino'),
}

module.exports = functions
