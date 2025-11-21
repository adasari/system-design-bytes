package com.taskflow.parallel;

import com.taskflow.core.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * DynamicForkTask enables dynamic parallel execution of tasks based on runtime data.
 * 
 * <p>This task type allows for creating and executing a variable number of tasks
 * in parallel, where the specific tasks to execute are determined dynamically
 * based on the current execution context. This is useful for scenarios where
 * the number and type of parallel operations depend on runtime data.</p>
 * 
 * <p><b>Key Features:</b></p>
 * <ul>
 *   <li><b>Dynamic Task Generation</b>: Tasks are created at runtime based on context data</li>
 *   <li><b>Concurrent Execution</b>: All generated tasks execute in parallel</li>
 *   <li><b>Concurrency Control</b>: Optional limit on maximum concurrent task executions</li>
 *   <li><b>Result Aggregation</b>: Configurable aggregation of individual task results</li>
 *   <li><b>Wait Strategies</b>: Options to wait for all tasks or proceed when any completes</li>
 * </ul>
 * 
 * <p><b>Usage Example:</b></p>
 * <pre>{@code
 * DynamicForkTask dynamicTask = DynamicForkTask.builder()
 *     .name("processUserRequests")
 *     .taskGenerator(context -> {
 *         List<String> userIds = (List<String>) context.get("userIds");
 *         Map<String, Task<?>> tasks = new HashMap<>();
 *         for (String userId : userIds) {
 *             tasks.put("user_" + userId, new ProcessUserTask(userId));
 *         }
 *         return tasks;
 *     })
 *     .resultAggregator((context, results) -> {
 *         Map<String, Object> summary = new HashMap<>();
 *         summary.put("totalProcessed", results.size());
 *         summary.put("successCount", 
 *             results.values().stream().mapToLong(r -> r.isSuccess() ? 1 : 0).sum());
 *         return summary;
 *     })
 *     .maxConcurrency(5)
 *     .waitForAll(true)
 *     .build();
 * }</pre>
 * 
 * @see Task
 * @see ExecutionContext
 * @see TaskResult
 */
public class DynamicForkTask implements Task<Map<String, Object>> {
    private final String taskName;
    private final Function<ExecutionContext, Map<String, Task<?>>> dynamicTaskGenerator;
    private final BiFunction<ExecutionContext, Map<String, TaskResult<?>>, Map<String, Object>> resultAggregator;
    private final boolean waitForAll;
    private final int maxConcurrency;

    private DynamicForkTask(Builder builder) {
        this.taskName = builder.taskName;
        this.dynamicTaskGenerator = builder.dynamicTaskGenerator;
        this.resultAggregator = builder.resultAggregator;
        this.waitForAll = builder.waitForAll;
        this.maxConcurrency = builder.maxConcurrency;
    }

    @Override
    public CompletableFuture<TaskResult<Map<String, Object>>> execute(ExecutionContext context) {
        try {
            Map<String, Task<?>> dynamicTasks = dynamicTaskGenerator.apply(context);

            if (dynamicTasks == null || dynamicTasks.isEmpty()) {
                return CompletableFuture.completedFuture(
                    TaskResult.success(taskName, Collections.emptyMap())
                );
            }

            context.put(taskName + ".fork.tasks", dynamicTasks.keySet());
            context.put(taskName + ".fork.count", dynamicTasks.size());

            Map<String, CompletableFuture<TaskResult<?>>> futures = new ConcurrentHashMap<>();
            Map<String, TaskResult<?>> results = new ConcurrentHashMap<>();

            List<Map.Entry<String, Task<?>>> taskEntries = new ArrayList<>(dynamicTasks.entrySet());

            if (maxConcurrency > 0) {
                return executeWithConcurrencyLimit(context, taskEntries, futures, results);
            } else {
                return executeAllConcurrent(context, taskEntries, futures, results);
            }

        } catch (Exception e) {
            return CompletableFuture.completedFuture(TaskResult.failure(taskName, e.getMessage()));
        }
    }

    private CompletableFuture<TaskResult<Map<String, Object>>> executeAllConcurrent(
            ExecutionContext context,
            List<Map.Entry<String, Task<?>>> tasks,
            Map<String, CompletableFuture<TaskResult<?>>> futures,
            Map<String, TaskResult<?>> results) {

        for (Map.Entry<String, Task<?>> entry : tasks) {
            String taskId = entry.getKey();
            Task<?> task = entry.getValue();

            ExecutionContext forkContext = context.copy();
            forkContext.put("fork.taskId", taskId);

            CompletableFuture<?> rawFuture = task.execute(forkContext)
                    .whenComplete((result, error) -> {
                        if (result != null) {
                            results.put(taskId, result);
                            context.setTaskResult(taskName + "." + taskId, result);
                        }
                    });

            @SuppressWarnings("unchecked")
            CompletableFuture<TaskResult<?>> future = (CompletableFuture<TaskResult<?>>) rawFuture;
            futures.put(taskId, future);
        }

        CompletableFuture<Void> allFutures = waitForAll
                ? CompletableFuture.allOf(futures.values().toArray(new CompletableFuture[0]))
                : CompletableFuture.anyOf(futures.values().toArray(new CompletableFuture[0]))
                    .thenApply(v -> null);

        return allFutures.thenApply(v -> {
            Map<String, Object> aggregatedResult = resultAggregator != null
                    ? resultAggregator.apply(context, results)
                    : defaultAggregation(results);

            context.put(taskName + ".join.results", aggregatedResult);
            return TaskResult.success(taskName, aggregatedResult);
        });
    }

    private CompletableFuture<TaskResult<Map<String, Object>>> executeWithConcurrencyLimit(
            ExecutionContext context,
            List<Map.Entry<String, Task<?>>> tasks,
            Map<String, CompletableFuture<TaskResult<?>>> futures,
            Map<String, TaskResult<?>> results) {

        return CompletableFuture.supplyAsync(() -> {
            Queue<Map.Entry<String, Task<?>>> taskQueue = new LinkedList<>(tasks);
            Set<CompletableFuture<TaskResult<?>>> runningFutures = ConcurrentHashMap.newKeySet();

            while (!taskQueue.isEmpty() || !runningFutures.isEmpty()) {
                while (runningFutures.size() < maxConcurrency && !taskQueue.isEmpty()) {
                    Map.Entry<String, Task<?>> entry = taskQueue.poll();
                    String taskId = entry.getKey();
                    Task<?> task = entry.getValue();

                    ExecutionContext forkContext = context.copy();
                    forkContext.put("fork.taskId", taskId);

                    CompletableFuture<?> rawFuture = task.execute(forkContext)
                            .whenComplete((result, error) -> {
                                if (result != null) {
                                    results.put(taskId, result);
                                    context.setTaskResult(taskName + "." + taskId, result);
                                }
                            });

                    @SuppressWarnings("unchecked")
                    CompletableFuture<TaskResult<?>> future = (CompletableFuture<TaskResult<?>>) rawFuture;
                    futures.put(taskId, future);
                    runningFutures.add(future);

                    future.whenComplete((r, e) -> runningFutures.remove(future));
                }

                if (!runningFutures.isEmpty()) {
                    CompletableFuture.anyOf(runningFutures.toArray(new CompletableFuture[0])).join();
                }
            }

            Map<String, Object> aggregatedResult = resultAggregator != null
                    ? resultAggregator.apply(context, results)
                    : defaultAggregation(results);

            context.put(taskName + ".join.results", aggregatedResult);
            return TaskResult.success(taskName, aggregatedResult);
        });
    }

    private Map<String, Object> defaultAggregation(Map<String, TaskResult<?>> results) {
        Map<String, Object> aggregated = new HashMap<>();
        aggregated.put("totalTasks", results.size());
        aggregated.put("successfulTasks", results.values().stream()
                .filter(TaskResult::isSuccess).count());
        aggregated.put("failedTasks", results.values().stream()
                .filter(TaskResult::isFailed).count());

        Map<String, Object> taskResults = new HashMap<>();
        results.forEach((taskId, result) -> {
            if (result.getData() != null) {
                taskResults.put(taskId, result.getData());
            }
        });
        aggregated.put("results", taskResults);

        return aggregated;
    }

    @Override
    public String getName() {
        return taskName;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String taskName = "DynamicForkTask";
        private Function<ExecutionContext, Map<String, Task<?>>> dynamicTaskGenerator;
        private BiFunction<ExecutionContext, Map<String, TaskResult<?>>, Map<String, Object>> resultAggregator;
        private boolean waitForAll = true;
        private int maxConcurrency = 0;

        public Builder name(String taskName) {
            this.taskName = taskName;
            return this;
        }

        public Builder taskGenerator(Function<ExecutionContext, Map<String, Task<?>>> dynamicTaskGenerator) {
            this.dynamicTaskGenerator = dynamicTaskGenerator;
            return this;
        }

        public Builder resultAggregator(BiFunction<ExecutionContext, Map<String, TaskResult<?>>, Map<String, Object>> resultAggregator) {
            this.resultAggregator = resultAggregator;
            return this;
        }

        public Builder waitForAll(boolean waitForAll) {
            this.waitForAll = waitForAll;
            return this;
        }

        public Builder maxConcurrency(int maxConcurrency) {
            this.maxConcurrency = maxConcurrency;
            return this;
        }

        public DynamicForkTask build() {
            if (dynamicTaskGenerator == null) {
                throw new IllegalStateException("Dynamic task generator is required");
            }
            return new DynamicForkTask(this);
        }
    }
}
