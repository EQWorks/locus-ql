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
