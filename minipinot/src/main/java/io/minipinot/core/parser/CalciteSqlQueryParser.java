package io.minipinot.core.parser;

import io.minipinot.core.request.ExpressionContext;
import io.minipinot.core.request.FilterContext;
import io.minipinot.core.request.FunctionContext;
import io.minipinot.core.request.LiteralContext;
import io.minipinot.core.request.OrderByExpressionContext;
import io.minipinot.core.request.QueryContext;
import io.minipinot.core.request.predicate.EqPredicate;
import io.minipinot.core.request.predicate.InPredicate;
import io.minipinot.core.request.predicate.NotEqPredicate;
import io.minipinot.core.request.predicate.Predicate;
import io.minipinot.core.request.predicate.RangePredicate;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

/**
 * The production SQL parser used everywhere in MiniPinot's query path. It parses the query with
 * Apache Calcite (the very same library Pinot uses) into a Calcite {@link SqlNode}, then converts
 * that tree into MiniPinot's {@link QueryContext} request/expression model.
 *
 * <p>This mirrors Pinot's {@code CalciteSqlParser} + {@code QueryContextConverterUtils} pipeline
 * (Calcite {@code SqlNode} -> internal request model). The parser is configured like Pinot's:
 * BABEL conformance and unchanged identifier casing so column names survive verbatim.
 *
 * <p>The hand-rolled {@link SqlQueryParser} is kept separately as a from-scratch learning reference;
 * this Calcite-based parser is what the engine and tests actually use.
 */
public final class CalciteSqlQueryParser {
  private static final Set<String> AGGREGATIONS = Set.of("count", "sum", "min", "max", "avg");

  private CalciteSqlQueryParser() {
  }

  public static QueryContext compile(String sql) {
    SqlNode sqlNode;
    try {
      sqlNode = SqlParser.create(sql, parserConfig()).parseQuery();
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to parse query: " + sql, e);
    }
    return convert(sqlNode);
  }

  private static SqlParser.Config parserConfig() {
    // Match Pinot's parser behaviour: lenient (BABEL) conformance and verbatim identifier casing.
    return SqlParser.config()
        .withCaseSensitive(false)
        .withUnquotedCasing(Casing.UNCHANGED)
        .withQuotedCasing(Casing.UNCHANGED)
        .withConformance(SqlConformanceEnum.BABEL);
  }

  // ----------------------------------------------------------------------------------------------
  // SqlNode -> QueryContext
  // ----------------------------------------------------------------------------------------------

  private static QueryContext convert(SqlNode sqlNode) {
    SqlSelect select;
    SqlNodeList orderList = null;
    SqlNode fetch = null;
    SqlNode offset = null;
    if (sqlNode instanceof SqlOrderBy) {
      // Calcite wraps ORDER BY / LIMIT / OFFSET around the SELECT.
      SqlOrderBy orderBy = (SqlOrderBy) sqlNode;
      select = (SqlSelect) orderBy.query;
      orderList = orderBy.orderList;
      fetch = orderBy.fetch;
      offset = orderBy.offset;
    } else if (sqlNode instanceof SqlSelect) {
      select = (SqlSelect) sqlNode;
    } else {
      throw new IllegalArgumentException("Unsupported query type: " + sqlNode.getKind());
    }

    QueryContext.Builder builder = new QueryContext.Builder();
    builder.setSelectExpressions(convertSelectList(select.getSelectList()));

    if (select.getWhere() != null) {
      builder.setFilter(toFilter(select.getWhere()));
    }
    SqlNodeList groupBy = select.getGroup();
    if (groupBy != null && !groupBy.isEmpty()) {
      builder.setGroupByExpressions(convertExpressionList(groupBy));
    }
    if (select.getHaving() != null) {
      builder.setHavingFilter(toFilter(select.getHaving()));
    }
    if (orderList != null && !orderList.isEmpty()) {
      builder.setOrderByExpressions(convertOrderByList(orderList));
    }
    if (fetch != null) {
      builder.setLimit(intValue(fetch));
    }
    if (offset != null) {
      builder.setOffset(intValue(offset));
    }
    return builder.build();
  }

  private static List<ExpressionContext> convertSelectList(SqlNodeList selectList) {
    List<ExpressionContext> expressions = new ArrayList<>();
    for (SqlNode node : selectList) {
      expressions.add(toExpression(unwrapAlias(node)));
    }
    return expressions;
  }

  private static List<ExpressionContext> convertExpressionList(SqlNodeList nodes) {
    List<ExpressionContext> expressions = new ArrayList<>();
    for (SqlNode node : nodes) {
      expressions.add(toExpression(node));
    }
    return expressions;
  }

  private static List<OrderByExpressionContext> convertOrderByList(SqlNodeList orderList) {
    List<OrderByExpressionContext> orderBys = new ArrayList<>();
    for (SqlNode node : orderList) {
      boolean asc = true;
      SqlNode expressionNode = node;
      if (node.getKind() == SqlKind.DESCENDING) {
        asc = false;
        expressionNode = ((SqlCall) node).operand(0);
      }
      orderBys.add(new OrderByExpressionContext(toExpression(expressionNode), asc));
    }
    return orderBys;
  }

  /** Strip a trailing {@code AS alias} so the underlying expression is exposed. */
  private static SqlNode unwrapAlias(SqlNode node) {
    if (node.getKind() == SqlKind.AS) {
      return ((SqlCall) node).operand(0);
    }
    return node;
  }

  // ----------------------------------------------------------------------------------------------
  // Expression conversion (identifier / literal / function)
  // ----------------------------------------------------------------------------------------------

  private static ExpressionContext toExpression(SqlNode node) {
    if (node instanceof SqlIdentifier) {
      SqlIdentifier identifier = (SqlIdentifier) node;
      return ExpressionContext.forIdentifier(identifier.isStar() ? "*" : identifierName(identifier));
    }
    if (node instanceof SqlLiteral) {
      return ExpressionContext.forLiteral(
          new LiteralContext(literalValue(node), node instanceof SqlCharStringLiteral));
    }
    if (node instanceof SqlCall) {
      SqlCall call = (SqlCall) node;
      String functionName = call.getOperator().getName().toLowerCase();
      if (AGGREGATIONS.contains(functionName)) {
        List<ExpressionContext> arguments = new ArrayList<>();
        for (SqlNode operand : call.getOperandList()) {
          arguments.add(toExpression(operand));
        }
        return ExpressionContext.forFunction(
            new FunctionContext(FunctionContext.Type.AGGREGATION, functionName, arguments));
      }
      throw new IllegalArgumentException("Unsupported function in select/order-by: " + functionName);
    }
    throw new IllegalArgumentException("Unsupported expression: " + node);
  }

  // ----------------------------------------------------------------------------------------------
  // Filter conversion
  // ----------------------------------------------------------------------------------------------

  private static FilterContext toFilter(SqlNode node) {
    SqlKind kind = node.getKind();
    switch (kind) {
      case AND:
        return FilterContext.forAnd(toFilterList((SqlCall) node));
      case OR:
        return FilterContext.forOr(toFilterList((SqlCall) node));
      case NOT:
        return FilterContext.forNot(toFilter(((SqlCall) node).operand(0)));
      case IN:
        return FilterContext.forPredicate(toInPredicate((SqlCall) node));
      case BETWEEN:
        return FilterContext.forPredicate(toBetweenPredicate((SqlCall) node));
      case EQUALS:
      case NOT_EQUALS:
      case GREATER_THAN:
      case GREATER_THAN_OR_EQUAL:
      case LESS_THAN:
      case LESS_THAN_OR_EQUAL:
        return FilterContext.forPredicate(toComparisonPredicate((SqlBasicCall) node));
      default:
        throw new IllegalArgumentException("Unsupported filter expression: " + node);
    }
  }

  private static List<FilterContext> toFilterList(SqlCall call) {
    List<FilterContext> children = new ArrayList<>();
    for (SqlNode operand : call.getOperandList()) {
      children.add(toFilter(operand));
    }
    return children;
  }

  private static Predicate toInPredicate(SqlCall call) {
    ExpressionContext lhs = toExpression(call.operand(0));
    List<String> values = new ArrayList<>();
    for (SqlNode value : (SqlNodeList) call.operand(1)) {
      values.add(literalValue(value));
    }
    return new InPredicate(lhs, values);
  }

  private static Predicate toBetweenPredicate(SqlCall call) {
    // Operands: [value, lower, upper]. The ASYMMETRIC/SYMMETRIC flag lives on the operator.
    ExpressionContext lhs = toExpression(call.operand(0));
    String lower = literalValue(call.operand(1));
    String upper = literalValue(call.operand(2));
    return new RangePredicate(lhs, lower, true, upper, true);
  }

  private static Predicate toComparisonPredicate(SqlBasicCall call) {
    SqlNode left = call.operand(0);
    SqlNode right = call.operand(1);
    SqlKind kind = call.getKind();
    // Normalize so the (column or aggregation) expression is on the left; flip the operator if the
    // query wrote it as `literal <op> expression`. This also lets HAVING compare an aggregation
    // expression (e.g. `sum(clicks) > 100`) since the left operand need not be a bare identifier.
    SqlNode expressionNode;
    SqlNode valueNode;
    if (right instanceof SqlLiteral) {
      expressionNode = left;
      valueNode = right;
    } else if (left instanceof SqlLiteral) {
      expressionNode = right;
      valueNode = left;
      kind = flip(kind);
    } else {
      throw new IllegalArgumentException("Unsupported comparison (needs a literal operand): " + call);
    }
    ExpressionContext lhs = toExpression(expressionNode);
    String value = literalValue(valueNode);
    switch (kind) {
      case EQUALS:
        return new EqPredicate(lhs, value);
      case NOT_EQUALS:
        return new NotEqPredicate(lhs, value);
      case GREATER_THAN:
        return new RangePredicate(lhs, value, false, RangePredicate.UNBOUNDED, true);
      case GREATER_THAN_OR_EQUAL:
        return new RangePredicate(lhs, value, true, RangePredicate.UNBOUNDED, true);
      case LESS_THAN:
        return new RangePredicate(lhs, RangePredicate.UNBOUNDED, true, value, false);
      case LESS_THAN_OR_EQUAL:
        return new RangePredicate(lhs, RangePredicate.UNBOUNDED, true, value, true);
      default:
        throw new IllegalArgumentException("Unsupported comparison: " + kind);
    }
  }

  private static SqlKind flip(SqlKind kind) {
    switch (kind) {
      case GREATER_THAN:
        return SqlKind.LESS_THAN;
      case GREATER_THAN_OR_EQUAL:
        return SqlKind.LESS_THAN_OR_EQUAL;
      case LESS_THAN:
        return SqlKind.GREATER_THAN;
      case LESS_THAN_OR_EQUAL:
        return SqlKind.GREATER_THAN_OR_EQUAL;
      default:
        return kind;
    }
  }

  // ----------------------------------------------------------------------------------------------
  // Leaf helpers
  // ----------------------------------------------------------------------------------------------

  private static String identifierName(SqlIdentifier identifier) {
    return identifier.isSimple() ? identifier.getSimple()
        : identifier.names.get(identifier.names.size() - 1);
  }

  private static String literalValue(SqlNode node) {
    if (!(node instanceof SqlLiteral)) {
      throw new IllegalArgumentException("Expected a literal but found: " + node);
    }
    return ((SqlLiteral) node).toValue();
  }

  private static int intValue(SqlNode node) {
    return Integer.parseInt(literalValue(node));
  }
}
