package com.taskflow.core;

import java.time.Instant;
import java.time.Duration;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.stream.Collectors;

public class WorkflowResult {
    private final String workflowId;
    private final String workflowName;
    private final WorkflowState state;
    private final Map<String, TaskResult<?>> stepResults;
    private final Map<String, Object> outputData;
    private final List<String> errors;
    private final Instant startTime;
    private final Instant endTime;
    private final Duration executionTime;
    
    private WorkflowResult(Builder builder) {
        this.workflowId = builder.workflowId;
        this.workflowName = builder.workflowName;
        this.state = builder.state;
        this.stepResults = new HashMap<>(builder.stepResults);
        this.outputData = new HashMap<>(builder.outputData);
        this.errors = new ArrayList<>(builder.errors);
        this.startTime = builder.startTime;
        this.endTime = builder.endTime;
        this.executionTime = builder.executionTime;
    }
    
    public String getWorkflowId() {
        return workflowId;
    }
    
    public String getWorkflowName() {
        return workflowName;
    }
    
    public WorkflowState getState() {
        return state;
    }
    
    public Map<String, TaskResult<?>> getStepResults() {
        return Collections.unmodifiableMap(stepResults);
    }
    
    public TaskResult<?> getStepResult(String stepId) {
        return stepResults.get(stepId);
    }
    
    public Map<String, Object> getOutputData() {
        return Collections.unmodifiableMap(outputData);
    }
    
    public Object getOutput(String key) {
        return outputData.get(key);
    }
    
    public List<String> getErrors() {
        return Collections.unmodifiableList(errors);
    }
    
    public boolean isSuccess() {
        return state == WorkflowState.COMPLETED;
    }
    
    public boolean isFailed() {
        return state == WorkflowState.FAILED;
    }
    
    public boolean isCancelled() {
        return state == WorkflowState.CANCELLED;
    }
    
    public Instant getStartTime() {
        return startTime;
    }
    
    public Instant getEndTime() {
        return endTime;
    }
    
    public Duration getExecutionTime() {
        return executionTime;
    }
    
    public List<String> getFailedSteps() {
        return stepResults.entrySet().stream()
                .filter(entry -> entry.getValue().isFailed())
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }
    
    public List<String> getSuccessfulSteps() {
        return stepResults.entrySet().stream()
                .filter(entry -> entry.getValue().isSuccess())
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }
    
    public int getTotalSteps() {
        return stepResults.size();
    }
    
    public int getCompletedSteps() {
        return (int) stepResults.values().stream()
                .filter(TaskResult::isSuccess)
                .count();
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    public static class Builder {
        private String workflowId;
        private String workflowName;
        private WorkflowState state;
        private Map<String, TaskResult<?>> stepResults = new HashMap<>();
        private Map<String, Object> outputData = new HashMap<>();
        private List<String> errors = new ArrayList<>();
        private Instant startTime;
        private Instant endTime;
        private Duration executionTime;
        
        public Builder workflowId(String workflowId) {
            this.workflowId = workflowId;
            return this;
        }
        
        public Builder workflowName(String workflowName) {
            this.workflowName = workflowName;
            return this;
        }
        
        public Builder state(WorkflowState state) {
            this.state = state;
            return this;
        }
        
        public Builder stepResults(Map<String, TaskResult<?>> stepResults) {
            this.stepResults.putAll(stepResults);
            return this;
        }
        
        public Builder addStepResult(String stepId, TaskResult<?> result) {
            this.stepResults.put(stepId, result);
            return this;
        }
        
        public Builder outputData(Map<String, Object> outputData) {
            this.outputData.putAll(outputData);
            return this;
        }
        
        public Builder addOutput(String key, Object value) {
            this.outputData.put(key, value);
            return this;
        }
        
        public Builder errors(List<String> errors) {
            this.errors.addAll(errors);
            return this;
        }
        
        public Builder addError(String error) {
            this.errors.add(error);
            return this;
        }
        
        public Builder startTime(Instant startTime) {
            this.startTime = startTime;
            return this;
        }
        
        public Builder endTime(Instant endTime) {
            this.endTime = endTime;
            return this;
        }
        
        public Builder executionTime(Duration executionTime) {
            this.executionTime = executionTime;
            return this;
        }
        
        public WorkflowResult build() {
            if (startTime != null && endTime != null) {
                executionTime = Duration.between(startTime, endTime);
            }
            return new WorkflowResult(this);
        }
    }
}