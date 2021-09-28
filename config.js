module.exports.pg = {
  host: process.env.PG_HOST,
  port: process.env.PG_PORT,
  database: process.env.PG_DB,
  user: process.env.PG_USER,
  password: process.env.PG_PW,
}

// ML replica of locus_place
module.exports.pgML = {
  host: process.env.PG_HOST_ML,
  port: process.env.PG_PORT,
  database: process.env.PG_DB,
  user: process.env.PG_USER,
  password: process.env.PG_PW,
}

module.exports.pgAtom = {
  host: process.env.PG_ATOM_HOST,
  port: process.env.PG_ATOM_PORT,
  database: process.env.PG_ATOM_DB,
  user: process.env.PG_ATOM_USER,
  password: process.env.PG_ATOM_PW,
}

module.exports.pgAtomRead = {
  host: process.env.PG_ATOM_HOST_READ,
  port: process.env.PG_ATOM_PORT,
  database: process.env.PG_ATOM_DB,
  user: process.env.PG_ATOM_USER,
  password: process.env.PG_ATOM_PW,
}

module.exports.mapbox = {
  geocodeURL: process.env.MAP_BOX_GEOCODE_URL,
  appId: process.env.MAP_BOX_APP_ID,
  appCode: process.env.MAP_BOX_APP_CODE,
  token: process.env.MAP_BOX_TOKEN,
  mblimit: process.env.MAP_BOX_LIMIT_PER_BATCH,
  folimit: process.env.MAX_POI_PER_POI_LIST,
  breaktime: process.env.MAP_BOX_BREAK_BETWEEN_BATCH,
}

module.exports.lotame = {
  api_url: process.env.LOTAME_API_URL,
  api_key: process.env.LOTAME_API_KEY,
  api_token: process.env.LOTAME_API_TOKEN,
  client_id: process.env.LOTAME_CLIENT_ID,
}

module.exports.dataPipeLine = {
  prefix: 'datapipeline-tasks',
  stage: process.env.KEY_WARDEN_STAGE,
}

module.exports.algolia = {
  appID: process.env.ALGOLIA_APP_ID,
  adminKey: process.env.ALGOLIA_ADMIN_KEY,
  searchAPIKey: process.env.ALGOLIA_SEARCH_API_KEY,
}
