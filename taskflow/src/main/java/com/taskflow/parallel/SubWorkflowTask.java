package com.taskflow.parallel;

import com.taskflow.core.*;
import com.taskflow.executor.WorkflowExecutor;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

public class SubWorkflowTask implements Task<WorkflowResult> {
    private final String taskName;
    private final WorkflowDefinition subWorkflowDefinition;
    private final WorkflowExecutor executor;
    private final Map<String, Object> inputMapping;
    private final Map<String, String> outputMapping;
    private final boolean inheritContext;

    private SubWorkflowTask(Builder builder) {
        this.taskName = builder.taskName;
        this.subWorkflowDefinition = builder.subWorkflowDefinition;
        this.executor = builder.executor;
        this.inputMapping = new HashMap<>(builder.inputMapping);
        this.outputMapping = new HashMap<>(builder.outputMapping);
        this.inheritContext = builder.inheritContext;
    }

    @Override
    public CompletableFuture<TaskResult<WorkflowResult>> execute(ExecutionContext context) {
        try {
            Map<String, Object> subWorkflowInputs = new HashMap<>();

            if (inheritContext) {
                subWorkflowInputs.putAll(context.getAllData());
            }

            for (Map.Entry<String, Object> entry : inputMapping.entrySet()) {
                String targetKey = entry.getKey();
                Object sourceValue = entry.getValue();

                if (sourceValue instanceof String) {
                    String sourcePath = (String) sourceValue;
                    if (sourcePath.startsWith("${") && sourcePath.endsWith("}")) {
                        String contextKey = sourcePath.substring(2, sourcePath.length() - 1);
                        Object value = context.get(contextKey);
                        if (value != null) {
                            subWorkflowInputs.put(targetKey, value);
                        }
                    } else {
                        subWorkflowInputs.put(targetKey, sourceValue);
                    }
                } else {
                    subWorkflowInputs.put(targetKey, sourceValue);
                }
            }

            subWorkflowInputs.put("parentWorkflowId", context.getWorkflowInstanceId());
            subWorkflowInputs.put("isSubWorkflow", true);

            CompletableFuture<WorkflowResult> subWorkflowFuture = executor.execute(
                subWorkflowDefinition, subWorkflowInputs
            );

            return subWorkflowFuture.thenApply(result -> {
                for (Map.Entry<String, String> entry : outputMapping.entrySet()) {
                    String sourceKey = entry.getKey();
                    String targetKey = entry.getValue();

                    Object value = result.getOutput(sourceKey);
                    if (value != null) {
                        context.put(targetKey, value);
                    }
                }

                context.put(taskName + ".subWorkflowId", result.getWorkflowId());
                context.put(taskName + ".subWorkflowState", result.getState());

                if (result.isSuccess()) {
                    return TaskResult.success(taskName, result);
                } else {
                    return TaskResult.failure(taskName,
                        "Sub-workflow failed: " + String.join(", ", result.getErrors()));
                }
            });

        } catch (Exception e) {
            return CompletableFuture.completedFuture(TaskResult.failure(taskName, e));
        }
    }

    @Override
    public String getName() {
        return taskName;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String taskName = "SubWorkflow";
        private WorkflowDefinition subWorkflowDefinition;
        private WorkflowExecutor executor;
        private Map<String, Object> inputMapping = new HashMap<>();
        private Map<String, String> outputMapping = new HashMap<>();
        private boolean inheritContext = false;

        public Builder name(String taskName) {
            this.taskName = taskName;
            return this;
        }

        public Builder subWorkflow(WorkflowDefinition subWorkflowDefinition) {
            this.subWorkflowDefinition = subWorkflowDefinition;
            return this;
        }

        public Builder executor(WorkflowExecutor executor) {
            this.executor = executor;
            return this;
        }

        public Builder mapInput(String targetKey, Object sourceValue) {
            this.inputMapping.put(targetKey, sourceValue);
            return this;
        }

        public Builder mapOutput(String sourceKey, String targetKey) {
            this.outputMapping.put(sourceKey, targetKey);
            return this;
        }

        public Builder inheritContext(boolean inheritContext) {
            this.inheritContext = inheritContext;
            return this;
        }

        public SubWorkflowTask build() {
            if (subWorkflowDefinition == null || executor == null) {
                throw new IllegalStateException("Sub-workflow definition and executor are required");
            }
            return new SubWorkflowTask(this);
        }
    }
}
