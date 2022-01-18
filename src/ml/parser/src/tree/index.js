const { objectParsers, parseExpression } = require('./expression')
const { parserError, isArray, expressionTypes: expTypes } = require('../utils')
const ArrayNode = require('./array')
const CaseNode = require('./case')
const CastNode = require('./cast')
const ColumnReferenceNode = require('./column')
const FunctionNode = require('./function')
const JoinNode = require('./join')
const ListNode = require('./list')
const OperatorNode = require('./operator')
const ParameterReferenceNode = require('./parameter')
const PrimitiveNode = require('./primitive')
const { SelectNode, CTESelectNode, RangeSelectNode } = require('./select')
const ShortNode = require('./short')
const SortNode = require('./sort')
const SQLNode = require('./sql')
const ViewReferenceNode = require('./view')


const nodes = {
  ArrayNode,
  CaseNode,
  CastNode,
  ColumnReferenceNode,
  FunctionNode,
  JoinNode,
  ListNode,
  OperatorNode,
  ParameterReferenceNode,
  PrimitiveNode,
  SelectNode,
  CTESelectNode,
  RangeSelectNode,
  ShortNode,
  SortNode,
  SQLNode,
  ViewReferenceNode,
}

objectParsers[expTypes.SELECT] = (exp, context) => new SelectNode(exp, context)
objectParsers[expTypes.SELECT_RANGE] = (exp, context) => new RangeSelectNode(exp, context)
objectParsers[expTypes.SELECT_CTE] = (exp, context) => new CTESelectNode(exp, context)
objectParsers[expTypes.JOIN] = (exp, context) => new JoinNode(exp, context)
objectParsers[expTypes.VIEW] = (exp, context) => new ViewReferenceNode(exp, context)
objectParsers[expTypes.COLUMN] = (exp, context) => new ColumnReferenceNode(exp, context)
objectParsers[expTypes.PARAMETER] = (exp, context) => new ParameterReferenceNode(exp, context)
objectParsers[expTypes.SHORT] = (exp, context) => new ShortNode(exp, context)
objectParsers[expTypes.SQL] = (exp, context) => new SQLNode(exp, context)
objectParsers[expTypes.CAST] = (exp, context) => new CastNode(exp, context)
objectParsers[expTypes.PRIMITIVE] = (exp, context) => new PrimitiveNode(exp, context)
objectParsers[expTypes.CASE] = (exp, context) => new CaseNode(exp, context)
objectParsers[expTypes.ARRAY] = (exp, context) => new ArrayNode(exp, context)
objectParsers[expTypes.LIST] = (exp, context) => new ListNode(exp, context)
objectParsers[expTypes.FUNCTION] = (exp, context) => new FunctionNode(exp, context)
objectParsers[expTypes.SORT] = (exp, context) => new SortNode(exp, context)
objectParsers[expTypes.OPERATOR] = (exp, context) => new OperatorNode(exp, context)
objectParsers[expTypes.AND] = (exp, context) => {
  const { values, as, cast } = exp
  if (!isArray(values)) {
    throw parserError(`Invalid and syntax: ${JSON.stringify(exp)}`)
  }
  return new OperatorNode({ values: ['and', ...values], as, cast }, context)
}
objectParsers[expTypes.OR] = (exp, context) => {
  const { values, as, cast } = exp
  if (!isArray(values)) {
    throw parserError(`Invalid or syntax: ${JSON.stringify(exp)}`)
  }
  return new OperatorNode({ values: ['or', ...values], as, cast }, context)
}

module.exports = {
  nodes,
  parseExpression,
}
