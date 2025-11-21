package com.taskflow.core;

import java.time.Duration;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

public class TaskDefinition {
    private final String name;
    private final String taskReferenceName;
    private final TaskType type;
    private final Map<String, Object> inputParameters;
    private final Map<String, Object> outputParameters;
    private final Duration timeout;
    private final int retryCount;
    private final Duration retryDelay;
    private final RetryLogic retryLogic;
    private final boolean optional;
    private final List<String> dependencies;
    private final String decisionCases;
    private final String loopCondition;
    private final int loopCount;
    private final String expression;           // Expression for dynamic evaluation
    private final Map<String, Object> taskSpecificConfig; // Task-specific configuration
    private final String domain;               // Task domain for isolation
    private final Map<String, String> rateLimitConfig;    // Rate limiting configuration
    private final boolean asyncComplete;       // Async task completion
    private final String evaluatorType;        // Expression evaluator (javascript, groovy, etc)
    private final List<TaskDefinition> forkTasks;         // Tasks to fork
    private final List<String> joinOn;         // Task references to join on

    private TaskDefinition(Builder builder) {
        this.name = builder.name;
        this.taskReferenceName = builder.taskReferenceName;
        this.type = builder.type;
        this.inputParameters = new HashMap<>(builder.inputParameters);
        this.outputParameters = new HashMap<>(builder.outputParameters);
        this.timeout = builder.timeout;
        this.retryCount = builder.retryCount;
        this.retryDelay = builder.retryDelay;
        this.retryLogic = builder.retryLogic;
        this.optional = builder.optional;
        this.dependencies = new ArrayList<>(builder.dependencies);
        this.decisionCases = builder.decisionCases;
        this.loopCondition = builder.loopCondition;
        this.loopCount = builder.loopCount;
        this.expression = builder.expression;
        this.taskSpecificConfig = new HashMap<>(builder.taskSpecificConfig);
        this.domain = builder.domain;
        this.rateLimitConfig = new HashMap<>(builder.rateLimitConfig);
        this.asyncComplete = builder.asyncComplete;
        this.evaluatorType = builder.evaluatorType;
        this.forkTasks = new ArrayList<>(builder.forkTasks);
        this.joinOn = new ArrayList<>(builder.joinOn);
    }

    public String getName() {
        return name;
    }

    public String getTaskReferenceName() {
        return taskReferenceName;
    }

    public TaskType getType() {
        return type;
    }

    public Map<String, Object> getInputParameters() {
        return new HashMap<>(inputParameters);
    }

    public Map<String, Object> getOutputParameters() {
        return new HashMap<>(outputParameters);
    }

    public Duration getTimeout() {
        return timeout;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public Duration getRetryDelay() {
        return retryDelay;
    }

    public RetryLogic getRetryLogic() {
        return retryLogic;
    }

    public boolean isOptional() {
        return optional;
    }

    public List<String> getDependencies() {
        return new ArrayList<>(dependencies);
    }

    public String getDecisionCases() {
        return decisionCases;
    }

    public String getLoopCondition() {
        return loopCondition;
    }

    public int getLoopCount() {
        return loopCount;
    }

    public String getExpression() {
        return expression;
    }

    public Map<String, Object> getTaskSpecificConfig() {
        return new HashMap<>(taskSpecificConfig);
    }

    public String getDomain() {
        return domain;
    }

    public Map<String, String> getRateLimitConfig() {
        return new HashMap<>(rateLimitConfig);
    }

    public boolean isAsyncComplete() {
        return asyncComplete;
    }

    public String getEvaluatorType() {
        return evaluatorType;
    }

    public List<TaskDefinition> getForkTasks() {
        return new ArrayList<>(forkTasks);
    }

    public List<String> getJoinOn() {
        return new ArrayList<>(joinOn);
    }

    /**
     * Check if this is a system task (executed by workflow engine)
     */
    public boolean isSystemTask() {
        switch (type) {
            case HTTP:
            case DECISION:
            case SWITCH:
            case FORK_JOIN:
            case FORK_JOIN_DYNAMIC:
            case JOIN:
            case SUB_WORKFLOW:
            case EVENT:
            case WAIT:
            case JSON_JQ_TRANSFORM:
            case SET_VARIABLE:
            case TERMINATE:
            case LAMBDA:
            case INLINE:
                return true;
            default:
                return false;
        }
    }

    public enum TaskType {
        // Core task types
        SIMPLE,          // Basic user-defined task
        DYNAMIC,         // Dynamically created task at runtime

        // Control flow tasks
        DECISION,        // Simple if-else branching
        SWITCH,          // Multi-case branching like switch statement

        // Parallel execution tasks
        FORK_JOIN,       // Static parallel execution with predefined tasks
        FORK_JOIN_DYNAMIC, // Dynamic parallel execution
        JOIN,            // Synchronization point for parallel tasks

        // Loop tasks
        DO_WHILE,        // Loop with condition
        FOR_EACH,        // Iterate over collection

        // Workflow composition
        SUB_WORKFLOW,    // Execute another workflow as task

        // System integration tasks
        HTTP,            // HTTP REST API calls
        EVENT,           // Publish events to message systems
        WAIT,            // Wait for external events/signals

        // Data processing tasks
        JSON_JQ_TRANSFORM, // JSON transformation using JQ
        SET_VARIABLE,    // Set workflow variables

        // External system tasks
        // (SQS is used only for event-based workflow triggering)

        // Execution control
        TERMINATE,       // Terminate workflow with status
        LAMBDA,          // Execute inline code
        INLINE,          // Execute inline script

        // Human workflow tasks
        HUMAN,           // Human task requiring manual intervention

        // Rate limiting and resilience
        RATE_LIMIT,      // Rate limiting task execution
        CIRCUIT_BREAKER  // Circuit breaker pattern
    }

    public enum RetryLogic {
        FIXED,
        EXPONENTIAL_BACKOFF,
        LINEAR_BACKOFF
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String name;
        private String taskReferenceName;
        private TaskType type = TaskType.SIMPLE;
        private Map<String, Object> inputParameters = new HashMap<>();
        private Map<String, Object> outputParameters = new HashMap<>();
        private Duration timeout = Duration.ofMinutes(5);
        private int retryCount = 3;
        private Duration retryDelay = Duration.ofSeconds(1);
        private RetryLogic retryLogic = RetryLogic.EXPONENTIAL_BACKOFF;
        private boolean optional = false;
        private List<String> dependencies = new ArrayList<>();
        private String decisionCases;
        private String loopCondition;
        private int loopCount = 1;
        private String expression;
        private Map<String, Object> taskSpecificConfig = new HashMap<>();
        private String domain;
        private Map<String, String> rateLimitConfig = new HashMap<>();
        private boolean asyncComplete = false;
        private String evaluatorType = "javascript";
        private List<TaskDefinition> forkTasks = new ArrayList<>();
        private List<String> joinOn = new ArrayList<>();

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder taskReferenceName(String taskReferenceName) {
            this.taskReferenceName = taskReferenceName;
            return this;
        }

        public Builder type(TaskType type) {
            this.type = type;
            return this;
        }

        public Builder inputParameters(Map<String, Object> inputParameters) {
            this.inputParameters.putAll(inputParameters);
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

        public Builder retryCount(int retryCount) {
            this.retryCount = retryCount;
            return this;
        }

        public Builder retryDelay(Duration retryDelay) {
            this.retryDelay = retryDelay;
            return this;
        }

        public Builder retryLogic(RetryLogic retryLogic) {
            this.retryLogic = retryLogic;
            return this;
        }

        public Builder optional(boolean optional) {
            this.optional = optional;
            return this;
        }

        public Builder dependencies(List<String> dependencies) {
            this.dependencies.addAll(dependencies);
            return this;
        }

        public Builder joinOn(List<String> joinOn) {
            this.joinOn.addAll(joinOn);
            return this;
        }

        public Builder addDependency(String dependency) {
            this.dependencies.add(dependency);
            return this;
        }

        public Builder decisionCases(String decisionCases) {
            this.decisionCases = decisionCases;
            return this;
        }

        public Builder loopCondition(String loopCondition) {
            this.loopCondition = loopCondition;
            return this;
        }

        public Builder loopCount(int loopCount) {
            this.loopCount = loopCount;
            return this;
        }

        public Builder expression(String expression) {
            this.expression = expression;
            return this;
        }

        public Builder taskConfig(String key, Object value) {
            this.taskSpecificConfig.put(key, value);
            return this;
        }

        public Builder domain(String domain) {
            this.domain = domain;
            return this;
        }

        public Builder rateLimitConfig(String key, String value) {
            this.rateLimitConfig.put(key, value);
            return this;
        }

        public Builder asyncComplete(boolean asyncComplete) {
            this.asyncComplete = asyncComplete;
            return this;
        }

        public Builder evaluatorType(String evaluatorType) {
            this.evaluatorType = evaluatorType;
            return this;
        }

        public Builder addForkTask(TaskDefinition forkTask) {
            this.forkTasks.add(forkTask);
            return this;
        }

        public Builder addJoinOn(String taskReference) {
            this.joinOn.add(taskReference);
            return this;
        }

        // Helper methods for common task configurations
        public Builder httpTask(String url, String method) {
            return this.type(TaskType.HTTP)
                      .taskConfig("http_request", Map.of(
                          "uri", url,
                          "method", method
                      ));
        }

        public Builder switchTask(String switchExpression, Map<String, List<TaskDefinition>> decisionCases) {
            return this.type(TaskType.SWITCH)
                      .expression(switchExpression)
                      .taskConfig("decisionCases", decisionCases);
        }

        public Builder forkJoinTask(List<List<TaskDefinition>> forkTasks) {
            return this.type(TaskType.FORK_JOIN)
                      .taskConfig("forkTasks", forkTasks);
        }

        public Builder subWorkflowTask(String workflowName, Integer version, Map<String, Object> subWorkflowParam) {
            return this.type(TaskType.SUB_WORKFLOW)
                      .taskConfig("subWorkflowParam", Map.of(
                          "name", workflowName,
                          "version", version,
                          "taskToDomain", subWorkflowParam
                      ));
        }

        public TaskDefinition build() {
            if (name == null) {
                throw new IllegalStateException("Task name is required");
            }
            if (taskReferenceName == null) {
                taskReferenceName = name;
            }

            // Validate task-specific requirements
            validateTaskConfiguration();

            return new TaskDefinition(this);
        }

        private void validateTaskConfiguration() {
            switch (type) {
                case HTTP:
                    if (!taskSpecificConfig.containsKey("http_request")) {
                        throw new IllegalStateException("HTTP task requires http_request configuration");
                    }
                    break;
                case SWITCH:
                case DECISION:
                    if (expression == null) {
                        throw new IllegalStateException("Switch/Decision task requires expression");
                    }
                    break;
                case FORK_JOIN:
                    if (forkTasks.isEmpty() && !taskSpecificConfig.containsKey("forkTasks")) {
                        throw new IllegalStateException("Fork-Join task requires fork tasks");
                    }
                    break;
                case SUB_WORKFLOW:
                    if (!taskSpecificConfig.containsKey("subWorkflowParam")) {
                        throw new IllegalStateException("Sub-workflow task requires subWorkflowParam configuration");
                    }
                    break;
                case DO_WHILE:
                case FOR_EACH:
                    if (loopCondition == null) {
                        throw new IllegalStateException("Loop task requires loop condition");
                    }
                    break;
            }
        }
    }
}
