package com.taskflow.parallel;

import com.taskflow.core.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ForkJoinTask<T, R> implements Task<List<R>> {
    private final String taskName;
    private final Function<ExecutionContext, List<T>> splitter;
    private final Function<T, Task<R>> taskFactory;
    private final Function<List<R>, R> joiner;
    private final boolean failFast;
    private final int maxParallelism;
    
    private ForkJoinTask(Builder<T, R> builder) {
        this.taskName = builder.taskName;
        this.splitter = builder.splitter;
        this.taskFactory = builder.taskFactory;
        this.joiner = builder.joiner;
        this.failFast = builder.failFast;
        this.maxParallelism = builder.maxParallelism;
    }
    
    @Override
    public CompletableFuture<TaskResult<List<R>>> execute(ExecutionContext context) {
        try {
            List<T> items = splitter.apply(context);
            
            if (items == null || items.isEmpty()) {
                return CompletableFuture.completedFuture(
                    TaskResult.success(taskName, Collections.emptyList())
                );
            }
            
            context.put(taskName + ".fork.count", items.size());
            context.put(taskName + ".fork.items", items);
            
            List<CompletableFuture<TaskResult<R>>> futures = new ArrayList<>();
            
            for (int i = 0; i < items.size(); i++) {
                T item = items.get(i);
                Task<R> task = taskFactory.apply(item);
                
                ExecutionContext forkContext = context.copy();
                forkContext.put("fork.index", i);
                forkContext.put("fork.item", item);
                
                CompletableFuture<TaskResult<R>> future = task.execute(forkContext);
                
                if (failFast) {
                    future = future.handle((result, error) -> {
                        if (error != null || (result != null && result.isFailed())) {
                            futures.forEach(f -> f.cancel(true));
                            throw new RuntimeException("Fork task failed", error);
                        }
                        return result;
                    });
                }
                
                futures.add(future);
                
                if (maxParallelism > 0 && (i + 1) % maxParallelism == 0) {
                    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
                }
            }
            
            return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .thenApply(v -> {
                        List<R> results = futures.stream()
                                .map(CompletableFuture::join)
                                .filter(TaskResult::isSuccess)
                                .map(TaskResult::getData)
                                .collect(Collectors.toList());
                        
                        if (joiner != null && !results.isEmpty()) {
                            R joinedResult = joiner.apply(results);
                            context.put(taskName + ".join.result", joinedResult);
                            results = Collections.singletonList(joinedResult);
                        }
                        
                        context.put(taskName + ".join.count", results.size());
                        
                        return TaskResult.success(taskName, results);
                    })
                    .exceptionally(error -> TaskResult.failure(taskName, error.getCause() != null ? error.getCause().getMessage() : error.getMessage()));
                    
        } catch (Exception e) {
            return CompletableFuture.completedFuture(TaskResult.failure(taskName, e));
        }
    }
    
    @Override
    public String getName() {
        return taskName;
    }
    
    public static <T, R> Builder<T, R> builder() {
        return new Builder<>();
    }
    
    public static class Builder<T, R> {
        private String taskName = "ForkJoinTask";
        private Function<ExecutionContext, List<T>> splitter;
        private Function<T, Task<R>> taskFactory;
        private Function<List<R>, R> joiner;
        private boolean failFast = false;
        private int maxParallelism = 0;
        
        public Builder<T, R> name(String taskName) {
            this.taskName = taskName;
            return this;
        }
        
        public Builder<T, R> splitter(Function<ExecutionContext, List<T>> splitter) {
            this.splitter = splitter;
            return this;
        }
        
        public Builder<T, R> taskFactory(Function<T, Task<R>> taskFactory) {
            this.taskFactory = taskFactory;
            return this;
        }
        
        public Builder<T, R> joiner(Function<List<R>, R> joiner) {
            this.joiner = joiner;
            return this;
        }
        
        public Builder<T, R> failFast(boolean failFast) {
            this.failFast = failFast;
            return this;
        }
        
        public Builder<T, R> maxParallelism(int maxParallelism) {
            this.maxParallelism = maxParallelism;
            return this;
        }
        
        public ForkJoinTask<T, R> build() {
            if (splitter == null || taskFactory == null) {
                throw new IllegalStateException("Splitter and task factory are required");
            }
            return new ForkJoinTask<>(this);
        }
    }
}