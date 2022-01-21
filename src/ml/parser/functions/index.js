const {
  geoIntersectsParser,
  geoIntersectionAreaParser,
  geoIntersectionAreaRatioParser,
  geoAreaParser,
} = require('./geo')


const functions = {}

functions.json_extract_path = {
  trino: (node, options) => {
    const [json, ...path] = node.args.map(e => e.to('trino', options))
    return `json_extract(${json}, '$.${path.join('.')}')`
  },
}

functions.geo_intersects = {
  pg: geoIntersectsParser('pg'),
  trino: geoIntersectsParser('trino'),
}

functions.geo_intersection_area = {
  pg: geoIntersectionAreaParser('pg'),
  trino: geoIntersectionAreaParser('trino'),
}

functions.geo_intersection_area_ratio = {
  pg: geoIntersectionAreaRatioParser('pg'),
  trino: geoIntersectionAreaRatioParser('trino'),
}

functions.geo_area = {
  pg: geoAreaParser('pg'),
  trino: geoAreaParser('trino'),
}

module.exports = functions
