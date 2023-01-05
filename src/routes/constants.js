module.exports = {
  /* POLICIES CONSTANTS */
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
}
