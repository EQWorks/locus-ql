const {
  geoParser,
  geoIntersectsParser,
  geoIntersectionAreaParser,
  geoIntersectionAreaRatioParser,
  geoAreaParser,
  geoDistanceParser,
} = require('./geo')


const functions = {}

functions.json_extract_path = {
  trino: (node, options) => {
    const [json, ...path] = node.args.map(e => e.to('trino', options))
    return `json_extract(${json}, '$.${path.join('.')}')`
  },
}

functions.geometry = {
  pg: geoParser('pg'),
  trino: geoParser('trino'),
}

functions.geo = functions.geometry

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

functions.geo_distance = {
  pg: geoDistanceParser('pg'),
  trino: geoDistanceParser('trino'),
}

module.exports = functions
