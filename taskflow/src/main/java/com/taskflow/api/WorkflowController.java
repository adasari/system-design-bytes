package com.taskflow.api;

import com.taskflow.core.*;
import com.taskflow.definition.WorkflowDefinitionService;
import com.taskflow.executor.WorkflowExecutor;
import com.taskflow.persistence.entity.TaskExecutionEntity;
import com.taskflow.persistence.entity.WorkflowDefinitionEntity;
import com.taskflow.persistence.entity.WorkflowInstanceEntity;
import com.taskflow.persistence.service.WorkflowPersistenceService;
import com.taskflow.processor.WorkflowProcessor;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Unified REST API for workflow execution and management.
 *
 * <p>Combines workflow execution, status management, and monitoring into a single controller.</p>
 *
 * <p><b>Features:</b></p>
 * <ul>
 *   <li>Start workflows with version support</li>
 *   <li>Monitor workflow execution status</li>
 *   <li>Manage workflow lifecycle (pause, resume, cancel, retry)</li>
 *   <li>Task-level operations and monitoring</li>
 *   <li>System health and metrics</li>
 * </ul>
 */
@RestController
@RequestMapping("/api/v1/workflows")
@Tag(name = "Workflows", description = "Unified workflow execution and management API")
public class WorkflowController {

    private static final Logger logger = LoggerFactory.getLogger(WorkflowController.class);

    @Autowired
    private WorkflowExecutor workflowExecutor;

    @Autowired
    private WorkflowPersistenceService persistenceService;

    @Autowired
    private WorkflowProcessor workflowProcessor;

    @Autowired
    private WorkflowDefinitionService definitionService;

    // =========================== Workflow Execution ===========================

    /**
     * Start workflow execution by name (uses latest version)
     */
    @PostMapping("/name/{workflowName}")
    @Operation(summary = "Start workflow execution by name",
               description = "Starts the latest version of a workflow")
    public ResponseEntity<WorkflowExecutionResponse> startWorkflow(
            @PathVariable String workflowName,
            @Valid @RequestBody StartWorkflowRequest request,
            Authentication authentication) {

        return startWorkflowWithVersion(workflowName, null, request, authentication);
    }

    /**
     * Start workflow execution by name with specific version
     */
    @PostMapping("/name/{workflowName}/version/{version}/start")
    @Operation(summary = "Start workflow execution by name and version",
               description = "Starts a specific version of a workflow")
    public ResponseEntity<WorkflowExecutionResponse> startWorkflowWithVersion(
            @PathVariable String workflowName,
            @PathVariable @Parameter(description = "Workflow version number") Integer version,
            @Valid @RequestBody StartWorkflowRequest request,
            Authentication authentication) {

        String correlationId = request.getCorrelationId();
        if (correlationId == null) {
            correlationId = UUID.randomUUID().toString();
        }

        String triggeredBy = authentication != null ? authentication.getName() : "anonymous";

        try {
            logger.info("Starting workflow: {} version: {} correlationId: {} triggeredBy: {}",
                workflowName, version, correlationId, triggeredBy);

            // Add execution metadata to inputs
            Map<String, Object> inputs = new HashMap<>(request.getInputData());
            inputs.put("triggeredBy", triggeredBy);
            inputs.put("correlationId", correlationId);

            CompletableFuture<WorkflowResult> future = workflowExecutor.execute(
                workflowName, version, inputs, correlationId);

            // For async execution, return immediately with correlation ID
            if (!request.isWaitForCompletion()) {
                return ResponseEntity.accepted().body(new WorkflowExecutionResponse(
                    null,  // workflowInstanceId will be set when workflow starts
                    workflowName,
                    version,
                    correlationId,
                    "PENDING",
                    "Workflow execution started",
                    null
                ));
            }

            // For sync execution, wait for completion with timeout
            try {
                long timeoutMs = request.getTimeoutSeconds() != null
                    ? request.getTimeoutSeconds() * 1000L
                    : 300000L; // 5 minutes default

                WorkflowResult result = future.get(timeoutMs, TimeUnit.MILLISECONDS);

                return ResponseEntity.ok(new WorkflowExecutionResponse(
                    result.getWorkflowId(),
                    workflowName,
                    version,
                    correlationId,
                    result.getState().name(),
                    "Workflow completed successfully",
                    result.getOutputData()
                ));

            } catch (java.util.concurrent.TimeoutException e) {
                return ResponseEntity.status(HttpStatus.REQUEST_TIMEOUT).body(
                    new WorkflowExecutionResponse(
                        null,
                        workflowName,
                        version,
                        correlationId,
                        "TIMEOUT",
                        "Workflow execution timeout",
                        null
                    ));
            }

        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(new WorkflowExecutionResponse(
                null, workflowName, version, correlationId, "ERROR", e.getMessage(), null));
        } catch (Exception e) {
            logger.error("Error starting workflow: {} version: {}", workflowName, version, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(
                new WorkflowExecutionResponse(
                    null, workflowName, version, correlationId, "ERROR",
                    "Internal server error: " + e.getMessage(), null));
        }
    }

    @PostMapping
    @Operation(summary = "Create and run workflow")
    public ResponseEntity<WorkflowExecutionResponse> createAndStartWorkflow(
            @Valid @RequestBody WorkflowExecutionRequest request) {

        String workflowInstanceId = UUID.randomUUID().toString();
        String correlationId = request.getCorrelationId() != null ?
            request.getCorrelationId() : UUID.randomUUID().toString();

        String createdBy = "anonymous";

        try {
            WorkflowDefinitionEntity entity = null;
            if (request.getWorkflowDefinition() != null) {
                entity = definitionService.registerWorkflow(
                    request.getWorkflowDefinition(), createdBy);
            } else {

            }

            WorkflowInstanceEntity instance = persistenceService.createWorkflowInstance(
                entity.getWorkflowId(),
                workflowInstanceId,
                request.getVersion(),
                request.getInputs(),
                correlationId,
                null
            );

            if (request.getWorkflowDefinition() != null) {
                persistenceService.createInitialTasks(instance, request.getWorkflowDefinition());
            }

            return ResponseEntity.status(HttpStatus.CREATED)
                .body(new WorkflowExecutionResponse(
                    instance.getWorkflowInstanceId(),
                    request.getWorkflowName(),
                    request.getVersion(),
                    correlationId,
                    instance.getState().toString(),
                    "Workflow started successfully",
                    null
                ));
        } catch (Exception e) {
            logger.error("Error starting legacy workflow", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new WorkflowExecutionResponse(
                    null, request.getWorkflowName(), request.getVersion(), correlationId,
                    "ERROR", "Failed to start workflow: " + e.getMessage(), null));
        }
    }

    // =========================== Workflow Management ===========================

    /**
     * Get workflow execution status by instance ID
     */
    @GetMapping("/instances/{workflowInstanceId}")
    @Operation(summary = "Get workflow execution status")
    public ResponseEntity<WorkflowStatusResponse> getWorkflowInstanceStatus(
            @PathVariable String workflowInstanceId) {

        WorkflowInstanceEntity instance = persistenceService.getWorkflowInstance(workflowInstanceId);
        if (instance == null) {
            return ResponseEntity.notFound().build();
        }

        List<TaskExecutionEntity> tasks = persistenceService.getTaskExecutions(workflowInstanceId);

        return ResponseEntity.ok(new WorkflowStatusResponse(
            instance.getWorkflowInstanceId(),
            instance.getWorkflowName(),
            instance.getWorkflowVersion(),
            instance.getState(),
            instance.getStartedAt(),
            instance.getCompletedAt(),
            instance.getCorrelationId(),
            instance.getTriggerType(),
            tasks,
            instance.getOutputData(),
            instance.getErrors()
        ));
    }

    /**
     * List workflow instances with filtering
     */
    @GetMapping
    @Operation(summary = "List workflow instances", description = "Get paginated list of workflow instances with optional filtering")
    public ResponseEntity<Page<WorkflowInstanceEntity>> listWorkflowInstances(
            @RequestParam(required = false) String state,
            @RequestParam(required = false) String correlationId,
            @RequestParam(required = false) String workflowName,
            @RequestParam(required = false) String triggerType,
            Pageable pageable) {

        Page<WorkflowInstanceEntity> workflows;

        if (state != null) {
            workflows = persistenceService.findWorkflowsByState(
                WorkflowState.valueOf(state), pageable
            );
        } else if (correlationId != null) {
            workflows = persistenceService.findWorkflowsByCorrelationId(
                correlationId, pageable
            );
        } else {
            workflows = persistenceService.findAllWorkflows(pageable);
        }

        return ResponseEntity.ok(workflows);
    }

    /**
     * Pause workflow execution
     */
    @PutMapping("/instances/{workflowInstanceId}/pause")
    @Operation(summary = "Pause workflow execution")
    public ResponseEntity<ApiResponse> pauseWorkflowInstance(
            @PathVariable String workflowInstanceId,
            Authentication authentication) {

        try {
            persistenceService.updateWorkflowState(workflowInstanceId, WorkflowState.PAUSED);
            return ResponseEntity.ok(new ApiResponse("Workflow paused successfully"));
        } catch (Exception e) {
            logger.error("Error pausing workflow {}", workflowInstanceId, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new ApiResponse("Failed to pause workflow: " + e.getMessage()));
        }
    }

    /**
     * Resume workflow execution
     */
    @PutMapping("/instances/{workflowInstanceId}/resume")
    @Operation(summary = "Resume workflow execution")
    public ResponseEntity<ApiResponse> resumeWorkflowInstance(
            @PathVariable String workflowInstanceId,
            Authentication authentication) {

        try {
            persistenceService.updateWorkflowState(workflowInstanceId, WorkflowState.RUNNING);
            persistenceService.requeuePendingTasks(workflowInstanceId);
            return ResponseEntity.ok(new ApiResponse("Workflow resumed successfully"));
        } catch (Exception e) {
            logger.error("Error resuming workflow {}", workflowInstanceId, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new ApiResponse("Failed to resume workflow: " + e.getMessage()));
        }
    }

    /**
     * Cancel workflow execution
     */
    @DeleteMapping("/instances/{workflowInstanceId}/cancel")
    @Operation(summary = "Cancel workflow execution")
    public ResponseEntity<ApiResponse> cancelWorkflowInstance(
            @PathVariable String workflowInstanceId,
            Authentication authentication) {

        try {
            persistenceService.updateWorkflowState(workflowInstanceId, WorkflowState.CANCELLED);
            persistenceService.cancelPendingTasks(workflowInstanceId);
            return ResponseEntity.ok(new ApiResponse("Workflow cancelled successfully"));
        } catch (Exception e) {
            logger.error("Error cancelling workflow {}", workflowInstanceId, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new ApiResponse("Failed to cancel workflow: " + e.getMessage()));
        }
    }

    /**
     * Retry failed workflow
     */
    @PostMapping("/instances/{workflowInstanceId}/retry")
    @Operation(summary = "Retry failed workflow")
    public ResponseEntity<WorkflowExecutionResponse> retryWorkflowInstance(
            @PathVariable String workflowInstanceId,
            Authentication authentication) {

        WorkflowInstanceEntity instance = persistenceService.getWorkflowInstance(workflowInstanceId);
        if (instance == null) {
            return ResponseEntity.notFound().build();
        }

        try {
            persistenceService.resetFailedTasks(workflowInstanceId);
            persistenceService.updateWorkflowState(workflowInstanceId, WorkflowState.RUNNING);

            return ResponseEntity.ok(new WorkflowExecutionResponse(
                workflowInstanceId,
                instance.getWorkflowName(),
                instance.getWorkflowVersion(),
                instance.getCorrelationId(),
                WorkflowState.RUNNING.toString(),
                "Workflow retry initiated",
                null
            ));
        } catch (Exception e) {
            logger.error("Error retrying workflow {}", workflowInstanceId, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new WorkflowExecutionResponse(
                    workflowInstanceId, instance.getWorkflowName(), instance.getWorkflowVersion(),
                    instance.getCorrelationId(), "ERROR", "Failed to retry workflow: " + e.getMessage(), null));
        }
    }

    // =========================== Task Management ===========================

    /**
     * Get workflow tasks
     */
    @GetMapping("/instances/{workflowInstanceId}/tasks")
    @Operation(summary = "Get workflow tasks", description = "Get all tasks for a workflow instance")
    public ResponseEntity<List<TaskExecutionEntity>> getWorkflowInstanceTasks(
            @PathVariable String workflowInstanceId) {

        List<TaskExecutionEntity> tasks = persistenceService.getTaskExecutions(workflowInstanceId);
        return ResponseEntity.ok(tasks);
    }

    /**
     * Get task details
     */
    @GetMapping("/instances/{workflowInstanceId}/tasks/{taskId}")
    @Operation(summary = "Get task details")
    public ResponseEntity<TaskExecutionEntity> getTaskDetails(
            @PathVariable String workflowInstanceId,
            @PathVariable Long taskId) {

        TaskExecutionEntity task = persistenceService.getTaskExecution(taskId);
        if (task == null || !task.getWorkflow().getWorkflowInstanceId().equals(workflowInstanceId)) {
            return ResponseEntity.notFound().build();
        }

        return ResponseEntity.ok(task);
    }

    /**
     * Retry specific task
     */
    @PostMapping("/instances/{workflowInstanceId}/tasks/{taskId}/retry")
    @Operation(summary = "Retry task")
    public ResponseEntity<ApiResponse> retryTask(
            @PathVariable String workflowInstanceId,
            @PathVariable Long taskId,
            Authentication authentication) {

        try {
            persistenceService.retryTask(taskId);
            return ResponseEntity.ok(new ApiResponse("Task retry initiated"));
        } catch (Exception e) {
            logger.error("Error retrying task {} in workflow {}", taskId, workflowInstanceId, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new ApiResponse("Failed to retry task: " + e.getMessage()));
        }
    }

    // =========================== System Monitoring ===========================

    /**
     * Get processor status
     */
    @GetMapping("/processor/status")
    @Operation(summary = "Get workflow processor status")
    public ResponseEntity<ProcessorStatus> getProcessorStatus() {
        return ResponseEntity.ok(new ProcessorStatus(
            workflowProcessor.isHealthy(),
            workflowProcessor.getRunningWorkflowCount(),
            workflowProcessor.getAvailableSlots()
        ));
    }

    /**
     * Get workflow metrics
     */
    @GetMapping("/metrics")
    @Operation(summary = "Get workflow execution metrics")
    public ResponseEntity<WorkflowPersistenceService.WorkflowMetrics> getMetrics(
            @RequestParam(required = false, defaultValue = "1h") String timeWindow) {

        WorkflowPersistenceService.WorkflowMetrics metrics = persistenceService.getMetrics(timeWindow);
        return ResponseEntity.ok(metrics);
    }

    /**
     * System health check
     */
    @GetMapping("/health")
    @Operation(summary = "System health check")
    public ResponseEntity<HealthStatus> health() {
        boolean isProcessorHealthy = workflowProcessor.isHealthy();
        boolean isDatabaseHealthy = persistenceService.isDatabaseHealthy();

        HealthStatus status = new HealthStatus(
            isProcessorHealthy && isDatabaseHealthy,
            isDatabaseHealthy,
            isProcessorHealthy
        );

        return ResponseEntity
            .status(status.isHealthy() ? HttpStatus.OK : HttpStatus.SERVICE_UNAVAILABLE)
            .body(status);
    }

    // =========================== Workflow Definition Management ===========================

    /**
     * Register a new workflow definition or create a new version
     */
    @PostMapping("/definitions")
    @Operation(summary = "Register workflow definition",
               description = "Creates a new workflow or a new version of existing workflow")
    public ResponseEntity<WorkflowDefinitionResponse> registerWorkflowDefinition(
            @Valid @RequestBody WorkflowDefinitionRequest request,
            Authentication authentication) {

        String createdBy = authentication != null ? authentication.getName() : "anonymous";

        try {
            WorkflowDefinitionEntity entity = definitionService.registerWorkflow(
                request.getDefinition(), createdBy);

            WorkflowDefinitionResponse response = new WorkflowDefinitionResponse(
                entity.getWorkflowId(),
                entity.getWorkflowName(),
                entity.getVersion(),
                entity.getStatus().name(),
                entity.getIsLatest(),
                "Workflow definition registered successfully"
            );

            return ResponseEntity.ok(response);

        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest()
                .body(new WorkflowDefinitionResponse(null, null, null, null, null, e.getMessage()));
        } catch (Exception e) {
            logger.error("Error registering workflow definition", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new WorkflowDefinitionResponse(null, null, null, null, null,
                    "Internal server error: " + e.getMessage()));
        }
    }

    /**
     * Get workflow definition by name and version
     */
    @GetMapping("/definitions/name/{workflowName}/version/{version}")
    @Operation(summary = "Get workflow definition by name and version")
    public ResponseEntity<WorkflowDefinition> getWorkflowDefinition(
            @PathVariable String workflowName,
            @PathVariable Integer version) {

        Optional<WorkflowDefinition> definition =
            definitionService.getWorkflowDefinition(workflowName, version);

        return definition.map(ResponseEntity::ok)
            .orElse(ResponseEntity.notFound().build());
    }

    /**
     * Get latest version of workflow definition by name
     */
    @GetMapping("/definitions/name/{workflowName}/latest")
    @Operation(summary = "Get latest version of workflow definition by name")
    public ResponseEntity<WorkflowDefinition> getLatestWorkflowDefinition(
            @PathVariable String workflowName) {

        Optional<WorkflowDefinition> definition =
            definitionService.getLatestWorkflowDefinition(workflowName);

        return definition.map(ResponseEntity::ok)
            .orElse(ResponseEntity.notFound().build());
    }

    /**
     * Get workflow definition by workflow ID and version
     */
    @GetMapping("/definitions/id/{workflowId}/version/{version}")
    @Operation(summary = "Get workflow definition by ID and version")
    public ResponseEntity<WorkflowDefinition> getWorkflowDefinitionById(
            @PathVariable String workflowId,
            @PathVariable Integer version) {

        Optional<WorkflowDefinition> definition =
            definitionService.getWorkflowDefinitionById(workflowId, version);

        return definition.map(ResponseEntity::ok)
            .orElse(ResponseEntity.notFound().build());
    }

    /**
     * Get latest version of workflow definition by workflow ID
     */
    @GetMapping("/definitions/id/{workflowId}/latest")
    @Operation(summary = "Get latest version of workflow definition by ID")
    public ResponseEntity<WorkflowDefinition> getLatestWorkflowDefinitionById(
            @PathVariable String workflowId) {

        Optional<WorkflowDefinition> definition =
            definitionService.getLatestWorkflowDefinitionById(workflowId);

        return definition.map(ResponseEntity::ok)
            .orElse(ResponseEntity.notFound().build());
    }

    /**
     * Get all versions of a workflow definition
     */
    @GetMapping("/definitions/name/{workflowName}/versions")
    @Operation(summary = "Get all versions of a workflow definition")
    public ResponseEntity<List<WorkflowDefinitionEntity>> getAllWorkflowVersions(
            @PathVariable String workflowName) {

        List<WorkflowDefinitionEntity> versions = definitionService.getAllVersions(workflowName);
        return ResponseEntity.ok(versions);
    }

    /**
     * Get all active workflow definitions (latest versions only)
     */
    @GetMapping("/definitions/active")
    @Operation(summary = "Get all active workflow definitions")
    public ResponseEntity<List<WorkflowDefinitionEntity>> getAllActiveWorkflowDefinitions() {
        List<WorkflowDefinitionEntity> workflows = definitionService.getAllActiveWorkflows();
        return ResponseEntity.ok(workflows);
    }

    /**
     * Get all workflow definitions (with pagination)
     */
    @GetMapping("/definitions")
    @Operation(summary = "Get all workflow definitions", description = "Get paginated list of all workflow definitions")
    public ResponseEntity<List<WorkflowDefinitionEntity>> getAllWorkflowDefinitions(
            @RequestParam(required = false) String status,
            @RequestParam(required = false) Boolean latestOnly) {

        if (Boolean.TRUE.equals(latestOnly)) {
            if ("ACTIVE".equals(status)) {
                return ResponseEntity.ok(definitionService.getAllActiveWorkflows());
            } else {
                // Return all latest versions regardless of status
                List<WorkflowDefinitionEntity> latestVersions = definitionService.getAllActiveWorkflows(); // TODO: Add method for all latest
                return ResponseEntity.ok(latestVersions);
            }
        }

        // For now, return active workflows. In a full implementation, this would support more filtering
        return ResponseEntity.ok(definitionService.getAllActiveWorkflows());
    }

    /**
     * Deprecate a specific workflow version
     */
    @PutMapping("/definitions/name/{workflowName}/version/{version}/deprecate")
    @Operation(summary = "Deprecate a workflow version")
    public ResponseEntity<ApiResponse> deprecateWorkflowVersion(
            @PathVariable String workflowName,
            @PathVariable Integer version,
            @Valid @RequestBody DeprecateRequest request,
            Authentication authentication) {

        String deprecatedBy = authentication != null ? authentication.getName() : "anonymous";

        try {
            definitionService.deprecateVersion(workflowName, version, deprecatedBy, request.getReason());
            return ResponseEntity.ok(new ApiResponse("Workflow version deprecated successfully"));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(new ApiResponse(e.getMessage()));
        } catch (Exception e) {
            logger.error("Error deprecating workflow version", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new ApiResponse("Failed to deprecate workflow version: " + e.getMessage()));
        }
    }

    /**
     * Set a specific version as latest
     */
    @PutMapping("/definitions/name/{workflowName}/version/{version}/set-latest")
    @Operation(summary = "Set a workflow version as latest")
    public ResponseEntity<ApiResponse> setLatestWorkflowVersion(
            @PathVariable String workflowName,
            @PathVariable Integer version,
            Authentication authentication) {

        String updatedBy = authentication != null ? authentication.getName() : "anonymous";

        try {
            definitionService.setLatestVersion(workflowName, version, updatedBy);
            return ResponseEntity.ok(new ApiResponse("Workflow version set as latest successfully"));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(new ApiResponse(e.getMessage()));
        } catch (Exception e) {
            logger.error("Error setting latest workflow version", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new ApiResponse("Failed to set latest workflow version: " + e.getMessage()));
        }
    }

    /**
     * Check if workflow exists
     */
    @GetMapping("/definitions/name/{workflowName}/exists")
    @Operation(summary = "Check if workflow exists")
    public ResponseEntity<Boolean> workflowExists(@PathVariable String workflowName) {
        boolean exists = definitionService.workflowExists(workflowName);
        return ResponseEntity.ok(exists);
    }

    /**
     * Get workflow ID by name
     */
    @GetMapping("/definitions/name/{workflowName}/id")
    @Operation(summary = "Get workflow ID by name")
    public ResponseEntity<String> getWorkflowId(@PathVariable String workflowName) {
        Optional<String> workflowId = definitionService.getWorkflowId(workflowName);
        return workflowId.map(ResponseEntity::ok)
            .orElse(ResponseEntity.notFound().build());
    }

    // =========================== DTOs ===========================

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class StartWorkflowRequest {
        @NotNull
        private Map<String, Object> inputData = new HashMap<>();

        private String correlationId;

        private boolean waitForCompletion = false;

        private Long timeoutSeconds;

        private Integer priority;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class WorkflowExecutionResponse {
        private String workflowInstanceId;
        private String workflowName;
        private Integer version;
        private String correlationId;
        private String status;
        private String message;
        private Map<String, Object> outputData;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class WorkflowStatusResponse {
        private String workflowInstanceId;
        private String workflowName;
        private Integer workflowVersion;
        private WorkflowState state;
        private java.time.Instant startedAt;
        private java.time.Instant completedAt;
        private String correlationId;
        private WorkflowInstanceEntity.TriggerType triggerType;
        private List<TaskExecutionEntity> tasks;
        private String outputData;
        private List<String> errors;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ApiResponse {
        private String message;
    }

    @Getter
    @AllArgsConstructor
    public static class ProcessorStatus {
        private final boolean healthy;
        private final int runningWorkflows;
        private final int availableSlots;
    }

    @Getter
    @AllArgsConstructor
    public static class HealthStatus {
        private final boolean healthy;
        private final boolean databaseHealthy;
        private final boolean processorHealthy;
    }

    // Workflow Definition DTOs

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class WorkflowDefinitionRequest {
        @NotNull
        @Valid
        private WorkflowDefinition definition;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class WorkflowDefinitionResponse {
        private String workflowId;
        private String workflowName;
        private Integer version;
        private String status;
        private Boolean isLatest;
        private String message;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DeprecateRequest {
        @NotBlank
        private String reason;
    }

    // Legacy DTOs for backward compatibility
    @Getter
    @Setter
    @NoArgsConstructor
    public static class WorkflowExecutionRequest {
        private String workflowName;
        private Integer version;
        private Map<String, Object> inputs;
        private String correlationId;
        private WorkflowDefinition workflowDefinition;
    }
}
