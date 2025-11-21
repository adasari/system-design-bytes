package com.taskflow.subworkflow;

import com.taskflow.core.*;
import com.taskflow.definition.WorkflowDefinitionService;
import com.taskflow.persistence.entity.TaskExecutionEntity;
import com.taskflow.persistence.entity.WorkflowInstanceEntity;
import com.taskflow.persistence.service.WorkflowPersistenceService;
import jakarta.persistence.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Manager for sub-workflow execution in our task flow engine.
 * Handles workflow composition and nested workflow execution with simple patterns.
 */
@Component
public class SubWorkflowManager {

    private static final Logger logger = LoggerFactory.getLogger(SubWorkflowManager.class);

    @Autowired
    private WorkflowPersistenceService persistenceService;

    @Autowired
    private WorkflowDefinitionService definitionService;

    /**
     * Execute a sub-workflow as part of a parent workflow
     */
    @Transactional
    public CompletableFuture<TaskResult<Map<String, Object>>> executeSubWorkflow(
            String parentWorkflowId,
            TaskDefinition subWorkflowTask,
            ExecutionContext parentContext) {

        logger.info("Starting sub-workflow execution for parent: {}", parentWorkflowId);

        return CompletableFuture.supplyAsync(() -> {
            try {
                // Extract sub-workflow parameters
                Map<String, Object> subWorkflowParams =
                    (Map<String, Object>) subWorkflowTask.getInputParameters().get("subWorkflowParam");

                if (subWorkflowParams == null) {
                    return TaskResult.failure(subWorkflowTask.getName(), "Sub-workflow parameters not found");
                }

                String workflowName = (String) subWorkflowParams.get("name");
                Integer version = (Integer) subWorkflowParams.get("version");

                // Get workflow definition
                Optional<WorkflowDefinition> workflowDef;
                if (version != null) {
                    workflowDef = definitionService.getWorkflowDefinition(workflowName, version);
                } else {
                    workflowDef = definitionService.getLatestWorkflowDefinition(workflowName);
                }

                if (!workflowDef.isPresent()) {
                    return TaskResult.failure(subWorkflowTask.getName(), "Sub-workflow definition not found: " + workflowName);
                }

                // Create sub-workflow instance
                String subWorkflowId = UUID.randomUUID().toString();
                String correlationId = parentWorkflowId + "_sub_" + subWorkflowId;

                // Prepare input data from parent context and task inputs
                Map<String, Object> subWorkflowInputs = prepareSubWorkflowInputs(
                    subWorkflowTask.getInputParameters(), parentContext
                );

                WorkflowInstanceEntity subWorkflowInstance = persistenceService.createWorkflowInstance(
                    subWorkflowId,
                    workflowName,
                    workflowDef.get().getVersion(),
                    subWorkflowInputs,
                    correlationId,
                        null
                );

                // Set parent workflow reference
                subWorkflowInstance.setParentWorkflowId(parentWorkflowId);

                // Create initial tasks for sub-workflow
                persistenceService.createInitialTasks(subWorkflowInstance, workflowDef.get());

                // Wait for sub-workflow completion
                SubWorkflowResult result = waitForSubWorkflowCompletion(
                    subWorkflowId, subWorkflowTask.getTimeout()
                );

                // Prepare result
                Map<String, Object> resultData = Map.of(
                    "subWorkflowId", subWorkflowId,
                    "workflowName", workflowName,
                    "version", workflowDef.get().getVersion(),
                    "state", result.getFinalState().toString(),
                    "output", result.getOutputData(),
                    "executionTime", result.getExecutionTimeMs()
                );

                if (result.getFinalState() == WorkflowState.COMPLETED) {
                    // Merge sub-workflow output into parent context
                    mergeSubWorkflowOutput(parentContext, subWorkflowTask.getTaskReferenceName(),
                                         result.getOutputData());

                    return TaskResult.success(subWorkflowTask.getName(), resultData);
                } else {
                    return TaskResult.failure(subWorkflowTask.getName(), "Sub-workflow failed: " + result.getErrorMessage());
                }

            } catch (Exception e) {
                logger.error("Error executing sub-workflow for parent: {}", parentWorkflowId, e);
                return TaskResult.failure(subWorkflowTask.getName(), "Sub-workflow execution error: " + e.getMessage());
            }
        });
    }

    /**
     * Get sub-workflow status
     */
    public SubWorkflowStatus getSubWorkflowStatus(String subWorkflowId) {
        try {
            WorkflowInstanceEntity subWorkflow = persistenceService.getWorkflowInstance(subWorkflowId);
            if (subWorkflow == null) {
                return new SubWorkflowStatus(subWorkflowId, WorkflowState.NOT_STARTED,
                                           "Sub-workflow not found", null, null);
            }

            var tasks = persistenceService.getTaskExecutions(subWorkflowId);
            ExecutionContext context = persistenceService.loadExecutionContext(subWorkflowId);

            return new SubWorkflowStatus(
                subWorkflowId,
                subWorkflow.getState(),
                null,
                tasks.size(),
                context != null ? context.getAllData() : Map.of()
            );

        } catch (Exception e) {
            logger.error("Error getting sub-workflow status: {}", subWorkflowId, e);
            return new SubWorkflowStatus(subWorkflowId, WorkflowState.FAILED,
                                       "Error retrieving status: " + e.getMessage(), null, null);
        }
    }

    /**
     * Cancel sub-workflow execution
     */
    @Transactional
    public boolean cancelSubWorkflow(String subWorkflowId, String reason) {
        try {
            logger.info("Cancelling sub-workflow: {} - Reason: {}", subWorkflowId, reason);

            WorkflowInstanceEntity subWorkflow = persistenceService.getWorkflowInstance(subWorkflowId);
            if (subWorkflow == null) {
                logger.warn("Sub-workflow not found for cancellation: {}", subWorkflowId);
                return false;
            }

            if (subWorkflow.getState() == WorkflowState.COMPLETED ||
                subWorkflow.getState() == WorkflowState.FAILED ||
                subWorkflow.getState() == WorkflowState.CANCELLED) {
                logger.info("Sub-workflow already in final state: {} - {}",
                    subWorkflowId, subWorkflow.getState());
                return false;
            }

            // Cancel the sub-workflow
            persistenceService.updateWorkflowState(subWorkflowId, WorkflowState.CANCELLED);
            persistenceService.cancelPendingTasks(subWorkflowId);

            // Add cancellation reason to workflow
            if (reason != null) {
                subWorkflow.addError("Cancelled: " + reason);
            }

            logger.info("Sub-workflow cancelled successfully: {}", subWorkflowId);
            return true;

        } catch (Exception e) {
            logger.error("Error cancelling sub-workflow: {}", subWorkflowId, e);
            return false;
        }
    }

    /**
     * Get all sub-workflows for a parent workflow
     */
    public List<SubWorkflowInfo> getSubWorkflows(String parentWorkflowId) {
        try {
            List<WorkflowInstanceEntity> subWorkflows = persistenceService.getSubWorkflows(parentWorkflowId);

            return subWorkflows.stream()
                .map(this::createSubWorkflowInfo)
                .collect(Collectors.toList());

        } catch (Exception e) {
            logger.error("Error getting sub-workflows for parent: {}", parentWorkflowId, e);
            return Collections.emptyList();
        }
    }

    /**
     * Retry failed sub-workflow
     */
    @Transactional
    public boolean retrySubWorkflow(String subWorkflowId) {
        try {
            logger.info("Retrying sub-workflow: {}", subWorkflowId);

            WorkflowInstanceEntity subWorkflow = persistenceService.getWorkflowInstance(subWorkflowId);
            if (subWorkflow == null || subWorkflow.getState() != WorkflowState.FAILED) {
                logger.warn("Sub-workflow not found or not in failed state: {}", subWorkflowId);
                return false;
            }

            // Reset failed tasks
            persistenceService.resetFailedTasks(subWorkflowId);

            // Reset workflow state
            persistenceService.updateWorkflowState(subWorkflowId, WorkflowState.RUNNING);

            logger.info("Sub-workflow retry initiated: {}", subWorkflowId);
            return true;

        } catch (Exception e) {
            logger.error("Error retrying sub-workflow: {}", subWorkflowId, e);
            return false;
        }
    }

    private Map<String, Object> prepareSubWorkflowInputs(
            Map<String, Object> taskInputs, ExecutionContext parentContext) {

        Map<String, Object> subWorkflowInputs = new java.util.HashMap<>(taskInputs);

        // Add parent context data using simple variable substitution
        subWorkflowInputs.forEach((key, value) -> {
            if (value instanceof String) {
                String substitutedValue = substituteVariables((String) value, parentContext);
                if (!substitutedValue.equals(value)) {
                    subWorkflowInputs.put(key, substitutedValue);
                }
            }
        });

        return subWorkflowInputs;
    }

    /**
     * Simple variable substitution - replaces ${variable} with context values
     * Consistent with SystemTaskExecutor design
     */
    private String substituteVariables(String text, ExecutionContext context) {
        if (text == null || !text.contains("${")) {
            return text;
        }

        String result = text;
        // Simple regex replacement for ${variable} patterns
        Pattern pattern = Pattern.compile("\\$\\{([^}]+)\\}");
        Matcher matcher = pattern.matcher(text);

        while (matcher.find()) {
            String variable = matcher.group(1);
            Object value = context.get(variable);
            String replacement = value != null ? value.toString() : "";
            result = result.replace("${" + variable + "}", replacement);
        }

        return result;
    }


    private SubWorkflowResult waitForSubWorkflowCompletion(
            String subWorkflowId, java.time.Duration timeout) {

        long startTime = System.currentTimeMillis();
        long timeoutMs = timeout != null ? timeout.toMillis() : 300000; // 5 minutes default

        while (System.currentTimeMillis() - startTime < timeoutMs) {
            try {
                WorkflowInstanceEntity subWorkflow = persistenceService.getWorkflowInstance(subWorkflowId);
                if (subWorkflow == null) {
                    return new SubWorkflowResult(WorkflowState.FAILED, Map.of(),
                        "Sub-workflow not found", System.currentTimeMillis() - startTime);
                }

                WorkflowState state = subWorkflow.getState();
                if (state == WorkflowState.COMPLETED || state == WorkflowState.FAILED ||
                    state == WorkflowState.CANCELLED) {

                    ExecutionContext context = persistenceService.loadExecutionContext(subWorkflowId);
                    Map<String, Object> outputData = context != null ? context.getAllData() : Map.of();

                    String errorMessage = null;
                    if (state != WorkflowState.COMPLETED && !subWorkflow.getErrors().isEmpty()) {
                        errorMessage = subWorkflow.getErrors().get(subWorkflow.getErrors().size() - 1);
                    }

                    return new SubWorkflowResult(state, outputData, errorMessage,
                        System.currentTimeMillis() - startTime);
                }

                Thread.sleep(1000); // Wait 1 second before checking again

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return new SubWorkflowResult(WorkflowState.FAILED, Map.of(),
                    "Sub-workflow wait interrupted", System.currentTimeMillis() - startTime);
            } catch (Exception e) {
                logger.error("Error waiting for sub-workflow completion: {}", subWorkflowId, e);
                return new SubWorkflowResult(WorkflowState.FAILED, Map.of(),
                    "Error waiting for completion: " + e.getMessage(),
                    System.currentTimeMillis() - startTime);
            }
        }

        // Timeout reached
        return new SubWorkflowResult(WorkflowState.FAILED, Map.of(),
            "Sub-workflow execution timeout", timeoutMs);
    }

    private void mergeSubWorkflowOutput(ExecutionContext parentContext,
                                       String taskReferenceName,
                                       Map<String, Object> subWorkflowOutput) {

        // Add sub-workflow output to parent context
        parentContext.put(taskReferenceName + "_output", subWorkflowOutput);

        // Merge specific output parameters if configured
        subWorkflowOutput.forEach((key, value) -> {
            parentContext.put(taskReferenceName + "." + key, value);
        });
    }

    private SubWorkflowInfo createSubWorkflowInfo(WorkflowInstanceEntity workflow) {
        return new SubWorkflowInfo(
            workflow.getWorkflowInstanceId(),
            workflow.getWorkflowName(),
            workflow.getWorkflowVersion(),
            workflow.getState(),
            workflow.getStartedAt(),
            workflow.getCompletedAt(),
            workflow.getCorrelationId()
        );
    }

    /**
     * Sub-workflow execution result
     */
    public static class SubWorkflowResult {
        private final WorkflowState finalState;
        private final Map<String, Object> outputData;
        private final String errorMessage;
        private final long executionTimeMs;

        public SubWorkflowResult(WorkflowState finalState, Map<String, Object> outputData,
                               String errorMessage, long executionTimeMs) {
            this.finalState = finalState;
            this.outputData = outputData;
            this.errorMessage = errorMessage;
            this.executionTimeMs = executionTimeMs;
        }

        public WorkflowState getFinalState() { return finalState; }
        public Map<String, Object> getOutputData() { return outputData; }
        public String getErrorMessage() { return errorMessage; }
        public long getExecutionTimeMs() { return executionTimeMs; }
    }

    /**
     * Sub-workflow status information
     */
    public static class SubWorkflowStatus {
        private final String subWorkflowId;
        private final WorkflowState state;
        private final String errorMessage;
        private final Integer totalTasks;
        private final Map<String, Object> output;

        public SubWorkflowStatus(String subWorkflowId, WorkflowState state, String errorMessage,
                               Integer totalTasks, Map<String, Object> output) {
            this.subWorkflowId = subWorkflowId;
            this.state = state;
            this.errorMessage = errorMessage;
            this.totalTasks = totalTasks;
            this.output = output;
        }

        public String getSubWorkflowId() { return subWorkflowId; }
        public WorkflowState getState() { return state; }
        public String getErrorMessage() { return errorMessage; }
        public Integer getTotalTasks() { return totalTasks; }
        public Map<String, Object> getOutput() { return output; }
    }

    /**
     * Sub-workflow information
     */
    public static class SubWorkflowInfo {
        private final String workflowId;
        private final String workflowName;
        private final Integer version;
        private final WorkflowState state;
        private final java.time.Instant startedAt;
        private final java.time.Instant completedAt;
        private final String correlationId;

        public SubWorkflowInfo(String workflowId, String workflowName, Integer version,
                             WorkflowState state, java.time.Instant startedAt,
                             java.time.Instant completedAt, String correlationId) {
            this.workflowId = workflowId;
            this.workflowName = workflowName;
            this.version = version;
            this.state = state;
            this.startedAt = startedAt;
            this.completedAt = completedAt;
            this.correlationId = correlationId;
        }

        public String getWorkflowId() { return workflowId; }
        public String getWorkflowName() { return workflowName; }
        public Integer getVersion() { return version; }
        public WorkflowState getState() { return state; }
        public java.time.Instant getStartedAt() { return startedAt; }
        public java.time.Instant getCompletedAt() { return completedAt; }
        public String getCorrelationId() { return correlationId; }
    }
}
