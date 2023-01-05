module.exports = {
  /* POLICIES CONSTANTS */
  // cox
  POLICY_COX: 'cox:*:*',

  // connection hub
  POLICY_HUB_READ: 'hub:read',
  POLICY_HUB_WRITE: 'hub:read:write',

  // ql
  POLICY_QL_READ: 'ql:read',
  POLICY_QL_WRITE: 'ql:read:write',
  // ql beta stage
  POLICY_QL_BETA_READ: 'ql:beta:read',
  POLICY_QL_BETA_WRITE: 'ql:beta:read:write',
  // executions
  POLICY_QL_EXECUTIONS_READ: 'ql:executions:read',
  POLICY_QL_EXECUTIONS_WRITE: 'ql:executions:read:write',
  // queries
  POLICY_QL_QUERIES_READ: 'ql:queries:read',
  POLICY_QL_QUERIES_WRITE: 'ql:queries:read:write',

  QL_SCHEMA: process.env.ML_SCHEMA,
  COX_WL: 2456,
  COX_CU: 27848,

  /* WIDGET CONSTANTS */
  WIDGET_TYPE: { map: 'map' },

  /* GEO CONSTANTS */
  GEO_LEVEL: {
    fsa: ['fsa'],
    pc: ['postalcode', 'pc'],
  },
}
