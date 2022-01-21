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


const PG = 'pg'

ArrayNode.registerParser(PG, arrayParser(PG))
CaseNode.registerParser(PG, caseParser(PG))
CastNode.registerParser(PG, castParser(PG))
ColumnReferenceNode.registerParser(PG, columnParser)
FunctionNode.registerParser(PG, functionParser(PG))
GeometryNode.registerParser(PG, geometryParser(PG))
JoinNode.registerParser(PG, joinParser(PG))
ListNode.registerParser(PG, listParser(PG))
OperatorNode.registerParser(PG, operatorParser(PG))
ParameterReferenceNode.registerParser(PG, parameterParser(PG))
PrimitiveNode.registerParser(PG, primitiveParser)
SelectNode.registerParser(PG, selectParser(PG))
CTESelectNode.registerParser(PG, cteSelectParser(PG))
RangeSelectNode.registerParser(PG, rangeSelectParser(PG))
ShortNode.registerParser(PG, shortParser(PG))
SortNode.registerParser(PG, sortParser(PG))
SQLNode.registerParser(PG, sqlParser(PG))
ViewReferenceNode.registerParser(PG, viewParser)
