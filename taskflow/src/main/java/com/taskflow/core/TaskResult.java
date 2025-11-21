package com.taskflow.core;

import java.time.Instant;
import java.util.Optional;

/**
 * TaskResult encapsulates the outcome of task execution in the workflow engine.
 * 
 * <p>This class provides a standardized way to represent task execution results,
 * including success/failure status, output data, error information, and execution metrics.</p>
 * 
 * <p><b>Key Components:</b></p>
 * <ul>
 *   <li><b>Status</b>: SUCCESS, FAILED, or other TaskStatus values</li>
 *   <li><b>Data</b>: Typed output data from successful task execution</li>
 *   <li><b>Error Information</b>: Error messages and exceptions for failed tasks</li>
 *   <li><b>Timing Metrics</b>: Start time, end time, and execution duration</li>
 * </ul>
 * 
 * <p><b>Factory Methods:</b></p>
 * <ul>
 *   <li>{@link #success(String, Object)} - Create successful task result</li>
 *   <li>{@link #failure(String, String)} - Create failed task result with error message</li>
 *   <li>{@link #failure(String, Exception)} - Create failed task result with exception</li>
 * </ul>
 * 
 * <p><b>Usage Example:</b></p>
 * <pre>{@code
 * // Successful task
 * TaskResult<String> success = TaskResult.success("httpTask", responseBody);
 * 
 * // Failed task
 * TaskResult<String> failure = TaskResult.failure("httpTask", "Connection timeout");
 * 
 * // Check result status
 * if (result.isSuccess()) {
 *     String data = result.getData();
 *     // Process success data
 * } else {
 *     String error = result.getErrorMessage();
 *     // Handle error
 * }
 * }</pre>
 * 
 * @param <T> The type of data returned by the task
 * @see TaskStatus
 * @see ExecutionContext
 */
public class TaskResult<T> {
    private final String taskName;
    private final T data;
    private final TaskStatus status;
    private final String errorMessage;
    private final Exception exception;
    private final Instant startTime;
    private final Instant endTime;
    private final long executionTimeMs;
    
    private TaskResult(Builder<T> builder) {
        this.taskName = builder.taskName;
        this.data = builder.data;
        this.status = builder.status;
        this.errorMessage = builder.errorMessage;
        this.exception = builder.exception;
        this.startTime = builder.startTime;
        this.endTime = builder.endTime;
        this.executionTimeMs = builder.executionTimeMs;
    }
    
    public static <T> TaskResult<T> success(String taskName, T data) {
        return TaskResult.<T>builder()
                .taskName(taskName)
                .data(data)
                .status(TaskStatus.SUCCESS)
                .build();
    }
    
    public static <T> TaskResult<T> failure(String taskName, String errorMessage) {
        return TaskResult.<T>builder()
                .taskName(taskName)
                .status(TaskStatus.FAILED)
                .errorMessage(errorMessage)
                .build();
    }
    
    public static <T> TaskResult<T> failure(String taskName, Exception exception) {
        return TaskResult.<T>builder()
                .taskName(taskName)
                .status(TaskStatus.FAILED)
                .exception(exception)
                .errorMessage(exception.getMessage())
                .build();
    }
    
    public static <T> TaskResult<T> skipped(String taskName) {
        return TaskResult.<T>builder()
                .taskName(taskName)
                .status(TaskStatus.SKIPPED)
                .build();
    }
    
    public static <T> TaskResult<T> cancelled(String taskName) {
        return TaskResult.<T>builder()
                .taskName(taskName)
                .status(TaskStatus.CANCELLED)
                .build();
    }
    
    public String getTaskName() {
        return taskName;
    }
    
    public T getData() {
        return data;
    }
    
    public Optional<T> getDataOptional() {
        return Optional.ofNullable(data);
    }
    
    public TaskStatus getStatus() {
        return status;
    }
    
    public String getErrorMessage() {
        return errorMessage;
    }
    
    public Optional<Exception> getException() {
        return Optional.ofNullable(exception);
    }
    
    public boolean isSuccess() {
        return status == TaskStatus.SUCCESS;
    }
    
    public boolean isFailed() {
        return status == TaskStatus.FAILED;
    }
    
    public boolean isSkipped() {
        return status == TaskStatus.SKIPPED;
    }
    
    public boolean isCancelled() {
        return status == TaskStatus.CANCELLED;
    }
    
    public Instant getStartTime() {
        return startTime;
    }
    
    public Instant getEndTime() {
        return endTime;
    }
    
    public long getExecutionTimeMs() {
        return executionTimeMs;
    }
    
    public static <T> Builder<T> builder() {
        return new Builder<>();
    }
    
    public static class Builder<T> {
        private String taskName;
        private T data;
        private TaskStatus status;
        private String errorMessage;
        private Exception exception;
        private Instant startTime;
        private Instant endTime;
        private long executionTimeMs;
        
        public Builder<T> taskName(String taskName) {
            this.taskName = taskName;
            return this;
        }
        
        public Builder<T> data(T data) {
            this.data = data;
            return this;
        }
        
        public Builder<T> status(TaskStatus status) {
            this.status = status;
            return this;
        }
        
        public Builder<T> errorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
            return this;
        }
        
        public Builder<T> exception(Exception exception) {
            this.exception = exception;
            return this;
        }
        
        public Builder<T> startTime(Instant startTime) {
            this.startTime = startTime;
            return this;
        }
        
        public Builder<T> endTime(Instant endTime) {
            this.endTime = endTime;
            return this;
        }
        
        public Builder<T> executionTimeMs(long executionTimeMs) {
            this.executionTimeMs = executionTimeMs;
            return this;
        }
        
        public TaskResult<T> build() {
            if (taskName == null) {
                taskName = "UnnamedTask";
            }
            if (status == null) {
                status = TaskStatus.SUCCESS;
            }
            if (startTime != null && endTime != null) {
                executionTimeMs = endTime.toEpochMilli() - startTime.toEpochMilli();
            }
            return new TaskResult<>(this);
        }
    }
}