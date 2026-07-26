package io.minipinot.core.query.aggregation.function;

import io.minipinot.core.request.ExpressionContext;
import io.minipinot.core.request.FunctionContext;
import io.minipinot.core.request.QueryContext;
import java.util.ArrayList;
import java.util.List;

/**
 * Builds the concrete {@link AggregationFunction} for a parsed {@link FunctionContext}. Mirrors
 * Pinot's {@code AggregationFunctionFactory}.
 */
public final class AggregationFunctionFactory {

  private AggregationFunctionFactory() {
  }

  public static AggregationFunction get(FunctionContext function) {
    ExpressionContext argument = function.getArguments().get(0);
    switch (function.getFunctionName()) {
      case "count":
        return new CountAggregationFunction(argument);
      case "sum":
        return new SumAggregationFunction(argument);
      case "min":
        return new MinAggregationFunction(argument);
      case "max":
        return new MaxAggregationFunction(argument);
      case "avg":
        return new AvgAggregationFunction(argument);
      default:
        throw new IllegalArgumentException("Unsupported aggregation function: "
            + function.getFunctionName());
    }
  }

  /**
   * The ordered list of aggregation functions in a query's select list (skipping plain group-by
   * identifiers). Mirrors Pinot's {@code AggregationFunctionUtils#getAggregationFunctions}.
   */
  public static List<AggregationFunction> getAggregationFunctions(QueryContext query) {
    List<AggregationFunction> functions = new ArrayList<>();
    for (ExpressionContext expression : query.getSelectExpressions()) {
      if (expression.getType() == ExpressionContext.Type.FUNCTION
          && expression.getFunction().getType() == FunctionContext.Type.AGGREGATION) {
        functions.add(get(expression.getFunction()));
      }
    }
    return functions;
  }
}

