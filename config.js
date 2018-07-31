const pgConfig = {
  host: process.env.PG_HOST,
  port: process.env.PG_PORT,
  database: process.env.PG_DB,
  user: process.env.PG_USER,
  password: process.env.PG_PW,
}

const mapZenConfig = {
  baseURL: process.env.MAP_ZEN_BASE_URL,
  apikey: process.env.API_KEY,
}

const mapboxConfig = {
  geocodeURL: process.env.MAP_BOX_GEOCODE_URL,
  appId: process.env.MAP_BOX_APP_ID,
  appCode: process.env.MAP_BOX_APP_CODE,
  token: process.env.MAP_BOX_TOKEN,
}

const graphhopperConfig = {
  isochroneURL: process.env.GRAPHHOPPER_ISOCHRONE_URL,
  apiKey: process.env.GRAPHHOPPER_API_KEY,
}

const keyWardenConfig = {
  host: process.env.KEY_WARDEN_HOST,
  stage: process.env.KEY_WARDEN_STAGE,
}

const basePath = process.env.API_GATEWAY_BASE_PATH || ''

const commitHash = process.env.COMMIT_SHORT_HASH || 'unknown'

const googleMapConfig = {
  apiKey: process.env.GOOGLE_MAP_API_KEY,
}

module.exports = {
  pg: pgConfig,
  mapzen: mapZenConfig,
  mapbox: mapboxConfig,
  graphhopper: graphhopperConfig,
  keyWarden: keyWardenConfig,
  basePath,
  commitHash,
  googleMap: googleMapConfig,
}
