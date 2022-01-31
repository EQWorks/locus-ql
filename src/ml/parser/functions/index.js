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

// date/time functions
functions.datetime = {
  pg: 'timestamptz',
  trino: (node, options) => {
    const ts = node.args[0].to('trino', options)
    return `
      COALESCE(
        TRY_CAST(${ts} AS TIMESTAMP WITH TIME ZONE),
        from_iso8601_timestamp(${ts})
      )
    `
  },
}
functions.timestamptz = functions.datetime
functions.timedelta = {
  pg: (node, options) => {
    const [unit, quantity] = node.args.map(e => e.to('pg', options))
    const sql = `${quantity} * INTERVAL '1 ${unit.slice(1, -1)}'`
    return node.isRoot() && !node.as && !node.cast ? sql : `(${sql})`
  },
  trino: (node, options) => {
    const [unit, quantity] = node.args.map(e => e.to('trino', options))
    const sql = `${quantity} * INTERVAL '1' ${unit.slice(1, -1)}`
    return node.isRoot() && !node.as && !node.cast ? sql : `(${sql})`
  },
}


// geo functions
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
