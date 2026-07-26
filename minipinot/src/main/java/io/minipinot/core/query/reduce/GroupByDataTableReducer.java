package io.minipinot.core.query.reduce;

import io.minipinot.core.datatable.DataTable;
import io.minipinot.core.query.aggregation.function.AggregationFunction;
import io.minipinot.core.query.utils.ValueComparators;
import io.minipinot.core.request.ExpressionContext;
import io.minipinot.core.request.OrderByExpressionContext;
import io.minipinot.core.request.QueryContext;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reduces a grouped aggregation. It (1) merges the per-segment group maps by value-key, (2) extracts
 * each group's final aggregation values, (3) applies {@code HAVING}, (4) projects the select list
 * (group columns + aggregations, in query order), then (5) applies {@code ORDER BY} and
 * {@code LIMIT}/{@code OFFSET}. Mirrors Pinot's {@code GroupByDataTableReducer}.
 */
public final class GroupByDataTableReducer implements DataTableReducer {

  @Override
  public ResultTable reduce(List<DataTable> dataTables, QueryContext query,
      List<AggregationFunction> functions) {
    List<String> groupByColumns = new ArrayList<>();
    for (ExpressionContext expression : query.getGroupByExpressions()) {
      groupByColumns.add(expression.getIdentifier());
    }
    int numFunctions = functions.size();

    // (1) Merge groups across segments by value-key.
    Map<List<Object>, Object[]> merged = new LinkedHashMap<>();
    for (DataTable dataTable : dataTables) {
      for (Map.Entry<List<Object>, Object[]> entry : dataTable.getGroups().entrySet()) {
        Object[] accumulators = merged.get(entry.getKey());
        if (accumulators == null) {
          merged.put(entry.getKey(), entry.getValue().clone());
        } else {
          Object[] other = entry.getValue();
          for (int i = 0; i < numFunctions; i++) {
            accumulators[i] = functions.get(i).merge(accumulators[i], other[i]);
          }
        }
      }
    }

    List<ExpressionContext> selectExpressions = query.getSelectExpressions();
    List<OrderByExpressionContext> orderBys = query.getOrderByExpressions();
    boolean ordered = orderBys != null && !orderBys.isEmpty();

    List<Object[]> outputRows = new ArrayList<>();
    List<Object[]> orderKeys = ordered ? new ArrayList<>() : null;

    // (2)-(4) For each group: extract finals, apply HAVING, project the select list.
    for (Map.Entry<List<Object>, Object[]> entry : merged.entrySet()) {
      List<Object> key = entry.getKey();
      Object[] accumulators = entry.getValue();

      Map<String, Object> byColumn = new HashMap<>();
      for (int i = 0; i < groupByColumns.size(); i++) {
        byColumn.put(groupByColumns.get(i), key.get(i));
      }
      Map<String, Object> byFunction = new HashMap<>();
      for (int i = 0; i < numFunctions; i++) {
        byFunction.put(functions.get(i).getResultName(),
            functions.get(i).extractFinalResult(accumulators[i]));
      }

      java.util.function.Function<ExpressionContext, Object> resolver = expression -> {
        if (expression.getType() == ExpressionContext.Type.FUNCTION) {
          return byFunction.get(expression.getFunction().toString());
        }
        return byColumn.get(expression.getIdentifier());
      };

      if (query.getHavingFilter() != null
          && !new HavingFilterHandler(query.getHavingFilter(), resolver).isMatch()) {
        continue;
      }

      Object[] row = new Object[selectExpressions.size()];
      for (int i = 0; i < selectExpressions.size(); i++) {
        row[i] = resolver.apply(selectExpressions.get(i));
      }
      outputRows.add(row);

      if (ordered) {
        Object[] orderKey = new Object[orderBys.size()];
        for (int i = 0; i < orderBys.size(); i++) {
          orderKey[i] = resolver.apply(orderBys.get(i).getExpression());
        }
        orderKeys.add(orderKey);
      }
    }

    // (5) Global ORDER BY then OFFSET/LIMIT.
    if (ordered) {
      List<Integer> indices = new ArrayList<>();
      for (int i = 0; i < outputRows.size(); i++) {
        indices.add(i);
      }
      java.util.Comparator<Object[]> keyComparator = ValueComparators.orderByComparator(orderBys);
      indices.sort((a, b) -> keyComparator.compare(orderKeys.get(a), orderKeys.get(b)));
      List<Object[]> sorted = new ArrayList<>(outputRows.size());
      for (int index : indices) {
        sorted.add(outputRows.get(index));
      }
      outputRows = sorted;
    }

    List<String> columnNames = new ArrayList<>(selectExpressions.size());
    for (ExpressionContext expression : selectExpressions) {
      columnNames.add(displayName(expression));
    }
    return new ResultTable(columnNames, applyOffsetLimit(outputRows, query));
  }

  private static List<Object[]> applyOffsetLimit(List<Object[]> rows, QueryContext query) {
    int from = Math.min(query.getOffset(), rows.size());
    int to = Math.min(from + query.getLimit(), rows.size());
    return new ArrayList<>(rows.subList(from, to));
  }

  private static String displayName(ExpressionContext expression) {
    return expression.getType() == ExpressionContext.Type.FUNCTION
        ? expression.getFunction().toString() : expression.getIdentifier();
  }
}
