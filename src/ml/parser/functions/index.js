const {
  geoParser,
  geoIntersectsParser,
  geoIntersectionAreaParser,
  geoAreaParser,
  geoJSONParser,
  geoDistanceParser,
} = require('./geo')
const castMapping = require('../cast')


const functions = {}

// json functions
functions.json_extract_path = {
  pg: 'jsonb_extract_path',
  trino: (node, options) => {
    const [json, ...path] = node.args.map(e => e.to('trino', options))
    return `(
      SELECT json_extract(${json}, '$.' || ${path.join(" || '.' || ")}) AS json_extract_path
    )`
  },
}
functions.json_extract_path_text = {
  pg: 'jsonb_extract_path_text',
  trino: (node, options) => {
    const [json, ...path] = node.args.map(e => e.to('trino', options))
    return `(
      SELECT json_format(
        json_extract(${json}, '$.' || ${path.join(" || '.' || ")})
      ) AS json_extract_path_text
    )`
  },
};

// type cast functions
['pg', 'trino'].forEach(engine => Object.entries(castMapping[engine]).forEach(([type, cast]) => {
  const name = `cast_${type}`
  functions[name] = functions[name] || {}
  functions[name][engine] = (node, options) =>
    `(SELECT CAST(${node.args[0].to(engine, options)} AS ${cast}) AS ${name})`
}))
// trino does not parse json when casting to type json (unlike pg)
functions.cast_json.trino = (node, options) => `(
  SELECT json_parse(CAST(${node.args[0].to('trino', options)} AS VARCHAR)) AS cast_json
)`

// date/time functions
functions.date = {
  trino: (node, options) => {
    const date = node.args[0].to('trino', options)
    return `(
      SELECT COALESCE(
        TRY_CAST(${date} AS DATE),
        from_iso8601_date(${date})
      ) AS date
    `
  },
}
functions.time = {
  pg: 'timetz',
  trino: (node, options) => {
    const time = node.args[0].to('trino', options)
    return `(SELECT CAST(${time} AS TIME WITH TIME ZONE) AS time)`
  },
}
functions.datetime = {
  pg: 'timestamptz',
  trino: (node, options) => {
    const ts = node.args[0].to('trino', options)
    return `(
      SELECT COALESCE(
        TRY_CAST(${ts} AS TIMESTAMP WITH TIME ZONE),
        from_iso8601_timestamp(${ts})
      ) AS datetime
    )`
  },
}
functions.timestamptz = functions.datetime
functions.timedelta = {
  pg: (node, options) => {
    const [unit, quantity] = node.args.map(e => e.to('pg', options))
    const sql = `
      SELECT ${quantity} * (
        CASE lower(trim(${unit}))
          WHEN 'millisecond' THEN INTERVAL '1 millisecond'
          WHEN 'second' THEN INTERVAL '1 second'
          WHEN 'minute' THEN INTERVAL '1 minute'
          WHEN 'hour' THEN INTERVAL '1 hour'
          WHEN 'day' THEN INTERVAL '1 day'
          WHEN 'week' THEN INTERVAL '1 week'
          WHEN 'month' THEN INTERVAL '1 month'
          WHEN 'year' THEN INTERVAL '1 year'
        END
      ) AS timedelta
    `
    return node.isRoot() && !node.as && !node.cast ? sql : `(${sql})`
  },
  trino: (node, options) => {
    const [unit, quantity] = node.args.map(e => e.to('trino', options))
    const sql = `
      SELECT ${quantity} * (
        CASE lower(trim(${unit}))
          WHEN 'millisecond' THEN INTERVAL '1' millisecond
          WHEN 'second' THEN INTERVAL '1' second
          WHEN 'minute' THEN INTERVAL '1' minute
          WHEN 'hour' THEN INTERVAL '1' hour
          WHEN 'day' THEN INTERVAL '1' day
          WHEN 'week' THEN INTERVAL '1' week
          WHEN 'month' THEN INTERVAL '1' month
          WHEN 'year' THEN INTERVAL '1' year
        END
      ) AS timedelta
    `
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

functions.geo_area = {
  pg: geoAreaParser('pg'),
  trino: geoAreaParser('trino'),
}

functions.geo_json = {
  pg: geoJSONParser('pg'),
  trino: geoJSONParser('trino'),
}

functions.geo_distance = {
  pg: geoDistanceParser('pg'),
  trino: geoDistanceParser('trino'),
}

module.exports = functions
