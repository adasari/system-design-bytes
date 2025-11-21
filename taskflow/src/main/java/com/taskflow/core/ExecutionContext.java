package com.taskflow.core;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Collections;

/**
 * ExecutionContext manages the state and data flow during workflow execution.
 * 
 * <p>This class serves as the central data store and communication mechanism between tasks
 * within a workflow instance. It provides thread-safe storage for:</p>
 * 
 * <ul>
 *   <li><b>Global Data</b>: Key-value pairs accessible to all tasks in the workflow</li>
 *   <li><b>Task Results</b>: Outputs from completed tasks, indexed by task reference name</li>
 *   <li><b>Metadata</b>: Additional context information for workflow execution</li>
 *   <li><b>Cancellation State</b>: Flag to signal workflow cancellation</li>
 * </ul>
 * 
 * <p><b>Key Features:</b></p>
 * <ul>
 *   <li>Thread-safe operations using ConcurrentHashMap</li>
 *   <li>Type-safe data retrieval with Optional return types</li>
 *   <li>Automatic task result propagation to global data</li>
 *   <li>Context copying for fork/parallel execution</li>
 *   <li>Cancellation support for graceful workflow termination</li>
 * </ul>
 * 
 * <p><b>Usage Example:</b></p>
 * <pre>{@code
 * ExecutionContext context = new ExecutionContext("workflow-123");
 * context.put("inputData", Arrays.asList("item1", "item2"));
 * context.setTaskResult("task1", TaskResult.success("task1", "result"));
 * 
 * // Access task result in subsequent tasks
 * Optional<String> result = context.get("task1.result", String.class);
 * }</pre>
 * 
 * @see TaskResult
 * @see WorkflowProcessor
 */
public class ExecutionContext {
    private final String workflowInstanceId;
    private final Map<String, Object> globalData;
    private final Map<String, TaskResult<?>> taskResults;
    private final Map<String, Object> metadata;
    private volatile boolean cancelled = false;

    public ExecutionContext(String workflowInstanceId) {
        this.workflowInstanceId = workflowInstanceId;
        this.globalData = new ConcurrentHashMap<>();
        this.taskResults = new ConcurrentHashMap<>();
        this.metadata = new ConcurrentHashMap<>();
    }

    public ExecutionContext(String workflowInstanceId, Map<String, Object> initialData) {
        this(workflowInstanceId);
        if (initialData != null) {
            this.globalData.putAll(initialData);
        }
    }

    public String getWorkflowInstanceId() {
        return workflowInstanceId;
    }

    public void put(String key, Object value) {
        globalData.put(key, value);
    }

    public <T> Optional<T> get(String key, Class<T> type) {
        Object value = globalData.get(key);
        if (value != null && type.isInstance(value)) {
            return Optional.of(type.cast(value));
        }
        return Optional.empty();
    }

    public Object get(String key) {
        return globalData.get(key);
    }

    public boolean containsKey(String key) {
        return globalData.containsKey(key);
    }

    public Map<String, Object> getAllData() {
        return Collections.unmodifiableMap(new ConcurrentHashMap<>(globalData));
    }

    public void setTaskResult(String stepId, TaskResult<?> result) {
        taskResults.put(stepId, result);
        if (result.isSuccess() && result.getData() != null) {
            put(stepId + ".result", result.getData());
        }
    }

    public <T> Optional<TaskResult<T>> getTaskResult(String stepId, Class<T> type) {
        TaskResult<?> result = taskResults.get(stepId);
        if (result != null) {
            try {
                @SuppressWarnings("unchecked")
                TaskResult<T> typedResult = (TaskResult<T>) result;
                return Optional.of(typedResult);
            } catch (ClassCastException e) {
                return Optional.empty();
            }
        }
        return Optional.empty();
    }

    public TaskResult<?> getTaskResult(String stepId) {
        return taskResults.get(stepId);
    }

    public Map<String, TaskResult<?>> getAllTaskResults() {
        return Collections.unmodifiableMap(new ConcurrentHashMap<>(taskResults));
    }

    public void setMetadata(String key, Object value) {
        metadata.put(key, value);
    }

    public Object getMetadata(String key) {
        return metadata.get(key);
    }

    public void cancel() {
        this.cancelled = true;
    }

    public boolean isCancelled() {
        return cancelled;
    }

    public void merge(Map<String, Object> data) {
        if (data != null) {
            globalData.putAll(data);
        }
    }

    public ExecutionContext copy() {
        ExecutionContext copy = new ExecutionContext(workflowInstanceId, this.globalData);
        copy.taskResults.putAll(this.taskResults);
        copy.metadata.putAll(this.metadata);
        copy.cancelled = this.cancelled;
        return copy;
    }
}
