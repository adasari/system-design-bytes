package com.taskflow.core;

import lombok.Getter;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.time.Duration;

@Getter
public class WorkflowDefinition {
    private final String name;
    private final int version;
    private final String description;
    private final List<TaskDefinition> tasks;
    private final Map<String, Object> inputParameters;
    private final Map<String, Object> outputParameters;
    private final Duration timeout;
    private final int maxRetries;
    private final boolean restartable;
    private final WorkflowType workflowType;

    private WorkflowDefinition(Builder builder) {
        this.name = builder.name;
        this.version = builder.version;
        this.description = builder.description;
        this.tasks = new ArrayList<>(builder.tasks);
        this.inputParameters = new HashMap<>(builder.inputParameters);
        this.outputParameters = new HashMap<>(builder.outputParameters);
        this.timeout = builder.timeout;
        this.maxRetries = builder.maxRetries;
        this.restartable = builder.restartable;
        this.workflowType = builder.workflowType;
    }

    public Map<String, Object> getInputParameters() {
        return new HashMap<>(inputParameters);
    }

    public Map<String, Object> getOutputParameters() {
        return new HashMap<>(outputParameters);
    }

    public static Builder builder() {
        return new Builder();
    }

    public enum WorkflowType {
        SIMPLE,
        DECISION,
        FORK_JOIN,
        DYNAMIC,
        SUB_WORKFLOW
    }

    public static class Builder {
        private String name;
        private int version = 1;
        private String description;
        private List<TaskDefinition> tasks = new ArrayList<>();
        private Map<String, Object> inputParameters = new HashMap<>();
        private Map<String, Object> outputParameters = new HashMap<>();
        private Duration timeout = Duration.ofHours(1);
        private int maxRetries = 3;
        private boolean restartable = true;
        private WorkflowType workflowType = WorkflowType.SIMPLE;

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder version(int version) {
            this.version = version;
            return this;
        }

        public Builder description(String description) {
            this.description = description;
            return this;
        }

        public Builder addTask(TaskDefinition task) {
            this.tasks.add(task);
            return this;
        }

        public Builder tasks(List<TaskDefinition> tasks) {
            this.tasks.addAll(tasks);
            return this;
        }

        public Builder inputParameter(String key, Object value) {
            this.inputParameters.put(key, value);
            return this;
        }

        public Builder outputParameter(String key, Object value) {
            this.outputParameters.put(key, value);
            return this;
        }

        public Builder timeout(Duration timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder maxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public Builder restartable(boolean restartable) {
            this.restartable = restartable;
            return this;
        }

        public Builder workflowType(WorkflowType workflowType) {
            this.workflowType = workflowType;
            return this;
        }

        public WorkflowDefinition build() {
            if (name == null) {
                throw new IllegalStateException("Workflow name is required");
            }
            return new WorkflowDefinition(this);
        }
    }
}
