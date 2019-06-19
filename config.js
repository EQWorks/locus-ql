const pgConfig = {
  host: process.env.PG_HOST,
  port: process.env.PG_PORT,
  database: process.env.PG_DB,
  user: process.env.PG_USER,
  password: process.env.PG_PW,
}

const pgAtomConfig = {
  host: process.env.PG_ATOM_HOST,
  port: process.env.PG_ATOM_PORT,
  database: process.env.PG_ATOM_DB,
  user: process.env.PG_ATOM_USER,
  password: process.env.PG_ATOM_PW,
}

const mappingPgConfig = {
  host: process.env.MAPPING_PG_HOST,
  port: process.env.MAPPING_PG_PORT,
  database: process.env.MAPPING_PG_DB,
  user: process.env.MAPPING_PG_USER,
  password: process.env.MAPPING_PG_PW,
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
  mblimit: process.env.MAP_BOX_LIMIT_PER_BATCH,
  folimit: process.env.MAX_POI_PER_POI_LIST,
  breaktime: process.env.MAP_BOX_BREAK_BETWEEN_BATCH,
}

const graphhopperConfig = {
  isochroneURL: process.env.GRAPHHOPPER_ISOCHRONE_URL,
  apiKey: process.env.GRAPHHOPPER_API_KEY,
}

const keyWardenConfig = {
  host: process.env.KEY_WARDEN_HOST,
  stage: process.env.KEY_WARDEN_STAGE,
}

const lotameConfig = {
  auth_url: process.env.LOTAME_AUTH_URL,
  api_url: process.env.LOTAME_API_URL,
  username: process.env.LOTAME_USERNAME,
  password: process.env.LOTAME_PASSWORD,
  client_id: process.env.LOTAME_CLIENT_ID,
}


const dataPipeLineTasksConfig = {
  prefix: 'datapipeline-tasks',
  stage: process.env.KEY_WARDEN_STAGE,
  approvalUsers: {
    dataops: [
      'stevenl@eqworks.com',
      'qinglei.zhang@eqworks.com',
      'riyad.parvez@eqworks.com',
      'hao.zhang@eqworks.com',
      'dhruv.thapar@eqworks.com',
      'tom.wainas@eqworks.com',
      'dilshank@eqworks.com',
    ],
    financial: [
      'dilshank@eqworks.com',
      'peter.kanniah@eqworks.com',
      'benjamun.ng@eqworks.com',
      'leo.li@eqworks.com',
    ],
  },
}

const algolia = {
  appID: process.env.ALGOLIA_APP_ID,
  adminKey: process.env.ALGOLIA_ADMIN_KEY,
  searchAPIKey: process.env.ALGOLIA_SEARCH_API_KEY,
}

const basePath = process.env.API_GATEWAY_BASE_PATH || ''

const commitHash = process.env.COMMIT_SHORT_HASH || 'unknown'

const redisUrl = process.env.REDIS_URL || 'localhost'

module.exports = {
  pg: pgConfig,
  pgAtom: pgAtomConfig,
  mappingPg: mappingPgConfig,
  mapzen: mapZenConfig,
  mapbox: mapboxConfig,
  graphhopper: graphhopperConfig,
  keyWarden: keyWardenConfig,
  lotame: lotameConfig,
  dataPipeLine: dataPipeLineTasksConfig,
  basePath,
  commitHash,
  algolia,
  redisUrl,
}
