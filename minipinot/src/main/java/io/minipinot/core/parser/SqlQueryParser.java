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

/**
 * A tiny hand-rolled recursive-descent SQL parser that turns a query string into a
 * {@link QueryContext}. It stands in for Pinot's Calcite-based parser
 * ({@code CalciteSqlParser} + {@code QueryContextConverterUtils}) so MiniPinot has no external SQL
 * dependency, while producing the very same request/expression model the execution layer consumes.
 *
 * <p>Supported grammar:
 * <pre>
 *   SELECT (*| expr (',' expr)*)
 *   FROM table
 *   [WHERE filter]
 *   [GROUP BY column (',' column)*]
 *   [ORDER BY expr [ASC|DESC] (',' ...)* ]
 *   [LIMIT n [OFFSET m]]
 * </pre>
 * where {@code expr} is a column, {@code *}, or an aggregation {@code count|sum|min|max|avg(arg)},
 * and {@code filter} supports {@code AND/OR/NOT}, parentheses, and the predicates
 * {@code = != <> < <= > >= IN BETWEEN}.
 */
public final class SqlQueryParser {
  private static final Set<String> AGGREGATIONS = Set.of("count", "sum", "min", "max", "avg");

  private final List<Token> _tokens;
  private int _pos;

  private SqlQueryParser(List<Token> tokens) {
    _tokens = tokens;
  }

  public static QueryContext compile(String sql) {
    List<Token> tokens = tokenize(sql);
    return new SqlQueryParser(tokens).parseQuery();
  }

  // ----------------------------------------------------------------------------------------------
  // Parser
  // ----------------------------------------------------------------------------------------------

  private QueryContext parseQuery() {
    expectKeyword("select");
    List<ExpressionContext> selectExpressions = parseSelectList();
    expectKeyword("from");
    // table name — consumed but unused (MiniPinot queries a set of segments directly).
    nextIdentifier();

    QueryContext.Builder builder = new QueryContext.Builder().setSelectExpressions(selectExpressions);

    if (peekKeyword("where")) {
      next();
      builder.setFilter(parseOr());
    }
    if (peekKeyword("group")) {
      next();
      expectKeyword("by");
      builder.setGroupByExpressions(parseGroupByList());
    }
    if (peekKeyword("order")) {
      next();
      expectKeyword("by");
      builder.setOrderByExpressions(parseOrderByList());
    }
    if (peekKeyword("limit")) {
      next();
      parseLimitOffset(builder);
    }
    if (!eof()) {
      throw error("Unexpected trailing token '" + peek().text + "'");
    }
    return builder.build();
  }

  private List<ExpressionContext> parseSelectList() {
    List<ExpressionContext> expressions = new ArrayList<>();
    expressions.add(parseExpression());
    while (peekSymbol(",")) {
      next();
      expressions.add(parseExpression());
    }
    return expressions;
  }

  private List<ExpressionContext> parseGroupByList() {
    List<ExpressionContext> expressions = new ArrayList<>();
    expressions.add(ExpressionContext.forIdentifier(nextIdentifier()));
    while (peekSymbol(",")) {
      next();
      expressions.add(ExpressionContext.forIdentifier(nextIdentifier()));
    }
    return expressions;
  }

  private List<OrderByExpressionContext> parseOrderByList() {
    List<OrderByExpressionContext> orderBys = new ArrayList<>();
    orderBys.add(parseOrderByItem());
    while (peekSymbol(",")) {
      next();
      orderBys.add(parseOrderByItem());
    }
    return orderBys;
  }

  private OrderByExpressionContext parseOrderByItem() {
    ExpressionContext expression = parseExpression();
    boolean asc = true;
    if (peekKeyword("asc")) {
      next();
    } else if (peekKeyword("desc")) {
      next();
      asc = false;
    }
    return new OrderByExpressionContext(expression, asc);
  }

  private void parseLimitOffset(QueryContext.Builder builder) {
    int first = nextInt();
    if (peekSymbol(",")) {
      // MySQL style: LIMIT offset, count
      next();
      builder.setOffset(first).setLimit(nextInt());
    } else {
      builder.setLimit(first);
      if (peekKeyword("offset")) {
        next();
        builder.setOffset(nextInt());
      }
    }
  }

  /** A select / order-by / aggregation-argument expression: {@code *}, column, or aggregation. */
  private ExpressionContext parseExpression() {
    if (peekSymbol("*")) {
      next();
      return ExpressionContext.forIdentifier("*");
    }
    Token token = peek();
    if (token.kind == Kind.STRING || token.kind == Kind.NUMBER) {
      next();
      return ExpressionContext.forLiteral(new LiteralContext(token.text, token.kind == Kind.STRING));
    }
    String identifier = nextIdentifier();
    if (AGGREGATIONS.contains(identifier.toLowerCase()) && peekSymbol("(")) {
      next();
      List<ExpressionContext> arguments = new ArrayList<>();
      arguments.add(parseExpression());
      while (peekSymbol(",")) {
        next();
        arguments.add(parseExpression());
      }
      expectSymbol(")");
      return ExpressionContext.forFunction(
          new FunctionContext(FunctionContext.Type.AGGREGATION, identifier.toLowerCase(), arguments));
    }
    return ExpressionContext.forIdentifier(identifier);
  }

  // ---- Filter tree: OR (lowest precedence) -> AND -> NOT -> primary --------------------------------

  private FilterContext parseOr() {
    List<FilterContext> children = new ArrayList<>();
    children.add(parseAnd());
    while (peekKeyword("or")) {
      next();
      children.add(parseAnd());
    }
    return children.size() == 1 ? children.get(0) : FilterContext.forOr(children);
  }

  private FilterContext parseAnd() {
    List<FilterContext> children = new ArrayList<>();
    children.add(parseNot());
    while (peekKeyword("and")) {
      next();
      children.add(parseNot());
    }
    return children.size() == 1 ? children.get(0) : FilterContext.forAnd(children);
  }

  private FilterContext parseNot() {
    if (peekKeyword("not")) {
      next();
      return FilterContext.forNot(parsePrimary());
    }
    return parsePrimary();
  }

  private FilterContext parsePrimary() {
    if (peekSymbol("(")) {
      next();
      FilterContext filter = parseOr();
      expectSymbol(")");
      return filter;
    }
    return FilterContext.forPredicate(parsePredicate());
  }

  private Predicate parsePredicate() {
    ExpressionContext lhs = ExpressionContext.forIdentifier(nextIdentifier());
    if (peekKeyword("in")) {
      next();
      expectSymbol("(");
      List<String> values = new ArrayList<>();
      values.add(nextValue());
      while (peekSymbol(",")) {
        next();
        values.add(nextValue());
      }
      expectSymbol(")");
      return new InPredicate(lhs, values);
    }
    if (peekKeyword("between")) {
      next();
      String lower = nextValue();
      expectKeyword("and");
      String upper = nextValue();
      return new RangePredicate(lhs, lower, true, upper, true);
    }
    String op = nextSymbol();
    switch (op) {
      case "=":
        return new EqPredicate(lhs, nextValue());
      case "!=":
      case "<>":
        return new NotEqPredicate(lhs, nextValue());
      case ">":
        return new RangePredicate(lhs, nextValue(), false, RangePredicate.UNBOUNDED, true);
      case ">=":
        return new RangePredicate(lhs, nextValue(), true, RangePredicate.UNBOUNDED, true);
      case "<":
        return new RangePredicate(lhs, RangePredicate.UNBOUNDED, true, nextValue(), false);
      case "<=":
        return new RangePredicate(lhs, RangePredicate.UNBOUNDED, true, nextValue(), true);
      default:
        throw error("Unsupported operator '" + op + "'");
    }
  }

  // ----------------------------------------------------------------------------------------------
  // Token cursor helpers
  // ----------------------------------------------------------------------------------------------

  private boolean eof() {
    return _pos >= _tokens.size();
  }

  private Token peek() {
    if (eof()) {
      throw error("Unexpected end of query");
    }
    return _tokens.get(_pos);
  }

  private Token next() {
    Token token = peek();
    _pos++;
    return token;
  }

  private boolean peekKeyword(String keyword) {
    return !eof() && peek().kind == Kind.IDENT && peek().text.equalsIgnoreCase(keyword);
  }

  private boolean peekSymbol(String symbol) {
    return !eof() && peek().kind == Kind.SYMBOL && peek().text.equals(symbol);
  }

  private void expectKeyword(String keyword) {
    if (!peekKeyword(keyword)) {
      throw error("Expected keyword '" + keyword + "'");
    }
    next();
  }

  private void expectSymbol(String symbol) {
    if (!peekSymbol(symbol)) {
      throw error("Expected '" + symbol + "'");
    }
    next();
  }

  private String nextSymbol() {
    Token token = next();
    if (token.kind != Kind.SYMBOL) {
      throw error("Expected an operator but found '" + token.text + "'");
    }
    return token.text;
  }

  private String nextIdentifier() {
    Token token = next();
    if (token.kind != Kind.IDENT) {
      throw error("Expected an identifier but found '" + token.text + "'");
    }
    return token.text;
  }

  /** A predicate operand: a quoted string or a number, returned as its raw text. */
  private String nextValue() {
    Token token = next();
    if (token.kind != Kind.STRING && token.kind != Kind.NUMBER) {
      throw error("Expected a value but found '" + token.text + "'");
    }
    return token.text;
  }

  private int nextInt() {
    Token token = next();
    if (token.kind != Kind.NUMBER) {
      throw error("Expected a number but found '" + token.text + "'");
    }
    return Integer.parseInt(token.text);
  }

  private IllegalArgumentException error(String message) {
    return new IllegalArgumentException("Parse error: " + message);
  }

  // ----------------------------------------------------------------------------------------------
  // Tokenizer
  // ----------------------------------------------------------------------------------------------

  private enum Kind {
    IDENT, NUMBER, STRING, SYMBOL
  }

  private static final class Token {
    final Kind kind;
    final String text;

    Token(Kind kind, String text) {
      this.kind = kind;
      this.text = text;
    }
  }

  private static List<Token> tokenize(String sql) {
    List<Token> tokens = new ArrayList<>();
    int i = 0;
    int n = sql.length();
    while (i < n) {
      char c = sql.charAt(i);
      if (Character.isWhitespace(c)) {
        i++;
        continue;
      }
      if (c == '\'' || c == '"') {
        char quote = c;
        StringBuilder sb = new StringBuilder();
        i++;
        while (i < n) {
          char d = sql.charAt(i);
          if (d == quote) {
            // Doubled quote is an escaped quote inside the string.
            if (i + 1 < n && sql.charAt(i + 1) == quote) {
              sb.append(quote);
              i += 2;
              continue;
            }
            break;
          }
          sb.append(d);
          i++;
        }
        if (i >= n) {
          throw new IllegalArgumentException("Parse error: unterminated string literal");
        }
        i++; // consume closing quote
        tokens.add(new Token(Kind.STRING, sb.toString()));
        continue;
      }
      if (Character.isDigit(c) || (c == '-' && i + 1 < n && Character.isDigit(sql.charAt(i + 1)))) {
        int start = i;
        i++;
        while (i < n && (Character.isDigit(sql.charAt(i)) || sql.charAt(i) == '.')) {
          i++;
        }
        tokens.add(new Token(Kind.NUMBER, sql.substring(start, i)));
        continue;
      }
      if (Character.isLetter(c) || c == '_') {
        int start = i;
        i++;
        while (i < n && (Character.isLetterOrDigit(sql.charAt(i)) || sql.charAt(i) == '_')) {
          i++;
        }
        tokens.add(new Token(Kind.IDENT, sql.substring(start, i)));
        continue;
      }
      // Multi-char operators first.
      if (i + 1 < n) {
        String two = sql.substring(i, i + 2);
        if (two.equals("!=") || two.equals("<>") || two.equals("<=") || two.equals(">=")) {
          tokens.add(new Token(Kind.SYMBOL, two));
          i += 2;
          continue;
        }
      }
      if ("()*,=<>".indexOf(c) >= 0) {
        tokens.add(new Token(Kind.SYMBOL, String.valueOf(c)));
        i++;
        continue;
      }
      throw new IllegalArgumentException("Parse error: unexpected character '" + c + "'");
    }
    return tokens;
  }
}
