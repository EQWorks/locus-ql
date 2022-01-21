const {
  arrayParser,
  caseParser,
  castParser,
  columnParser,
  functionParser,
  geometryParser,
  joinParser,
  listParser,
  operatorParser,
  parameterParser,
  primitiveParser,
  selectParser,
  cteSelectParser,
  rangeSelectParser,
  shortParser,
  sortParser,
  sqlParser,
  viewParser,
} = require('./nodes')
const {
  nodes: {
    ArrayNode,
    CaseNode,
    CastNode,
    ColumnReferenceNode,
    FunctionNode,
    GeometryNode,
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
  },
} = require('./src')


const TRINO = 'trino'

ArrayNode.registerParser(TRINO, arrayParser(TRINO))
CaseNode.registerParser(TRINO, caseParser(TRINO))
CastNode.registerParser(TRINO, castParser(TRINO))
ColumnReferenceNode.registerParser(TRINO, columnParser)
FunctionNode.registerParser(TRINO, functionParser(TRINO))
GeometryNode.registerParser(TRINO, geometryParser(TRINO))
JoinNode.registerParser(TRINO, joinParser(TRINO))
ListNode.registerParser(TRINO, listParser(TRINO))
OperatorNode.registerParser(TRINO, operatorParser(TRINO))
ParameterReferenceNode.registerParser(TRINO, parameterParser(TRINO))
PrimitiveNode.registerParser(TRINO, primitiveParser)
SelectNode.registerParser(TRINO, selectParser(TRINO))
CTESelectNode.registerParser(TRINO, cteSelectParser(TRINO))
RangeSelectNode.registerParser(TRINO, rangeSelectParser(TRINO))
ShortNode.registerParser(TRINO, shortParser(TRINO))
SortNode.registerParser(TRINO, sortParser(TRINO))
SQLNode.registerParser(TRINO, sqlParser(TRINO))
ViewReferenceNode.registerParser(TRINO, viewParser)
