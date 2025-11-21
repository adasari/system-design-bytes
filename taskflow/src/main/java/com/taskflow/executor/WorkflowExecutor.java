package com.taskflow.executor;

import com.taskflow.core.*;
import com.taskflow.definition.WorkflowDefinitionService;
import com.taskflow.persistence.entity.WorkflowInstanceEntity;
import com.taskflow.persistence.service.WorkflowPersistenceService;
import com.taskflow.processor.WorkflowProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * WorkflowExecutor provides a high-level API for workflow execution.
 * 
 * <p>This class serves as a facade around the WorkflowProcessor, providing
 * a simplified interface for workflow execution while leveraging the full
 * power of the distributed, database-backed workflow processing engine.</p>
 * 
 * <p><b>Key Features:</b></p>
 * <ul>
 *   <li>Simple API for workflow execution</li>
 *   <li>Automatic workflow registration and versioning</li>
 *   <li>Asynchronous execution with CompletableFuture</li>
 *   <li>Integration with persistence layer</li>
 *   <li>Support for both programmatic and pre-registered workflows</li>
 * </ul>
 * 
 * <p><b>Usage Example:</b></p>
 * <pre>{@code
 * @Autowired
 * private WorkflowExecutor executor;
 * 
 * // Execute a pre-registered workflow
 * CompletableFuture<WorkflowResult> future = executor.execute(
 *     "orderProcessing",
 *     Map.of("orderId", "12345", "amount", 99.99)
 * );
 * 
 * // Execute with specific version
 * CompletableFuture<WorkflowResult> future = executor.execute(
 *     "orderProcessing", 
 *     2,  // version 
 *     inputs
 * );
 * 
 * // Execute a WorkflowDefinition directly
 * WorkflowDefinition workflow = WorkflowDefinition.builder()
 *     .name("customWorkflow")
 *     .tasks(tasks)
 *     .build();
 * 
 * CompletableFuture<WorkflowResult> future = executor.execute(workflow, inputs);
 * }</pre>
 * 
 * @see WorkflowProcessor
 * @see WorkflowPersistenceService
 * @see WorkflowDefinitionService
 */
@Component
public class WorkflowExecutor {
    private static final Logger logger = LoggerFactory.getLogger(WorkflowExecutor.class);
    
    @Autowired
    private WorkflowProcessor workflowProcessor;
    
    @Autowired
    private WorkflowPersistenceService persistenceService;
    
    @Autowired
    private WorkflowDefinitionService definitionService;
    
    @Autowired
    private TaskRegistry taskRegistry;
    
    // Track running workflows for status queries
    private final Map<String, WorkflowInstanceEntity> runningWorkflows = new ConcurrentHashMap<>();
    
    /**
     * Execute a workflow by name (uses latest version)
     */
    public CompletableFuture<WorkflowResult> execute(String workflowName, Map<String, Object> inputs) {
        return execute(workflowName, null, inputs, null);
    }
    
    /**
     * Execute a workflow by name and version
     */
    public CompletableFuture<WorkflowResult> execute(String workflowName, Integer version, 
                                                     Map<String, Object> inputs) {
        return execute(workflowName, version, inputs, null);
    }
    
    /**
     * Execute a workflow by name with optional version and correlation ID
     */
    public CompletableFuture<WorkflowResult> execute(String workflowName, Integer version, 
                                                     Map<String, Object> inputs, String correlationId) {
        Optional<WorkflowDefinition> definitionOpt;
        
        if (version != null) {
            definitionOpt = definitionService.getWorkflowDefinition(workflowName, version);
            if (definitionOpt.isEmpty()) {
                throw new IllegalArgumentException(
                    String.format("Workflow not found: %s v%d", workflowName, version));
            }
        } else {
            definitionOpt = definitionService.getLatestWorkflowDefinition(workflowName);
            if (definitionOpt.isEmpty()) {
                throw new IllegalArgumentException("Workflow not found: " + workflowName);
            }
        }
        
        return execute(definitionOpt.get(), inputs, correlationId);
    }
    
    /**
     * Execute a workflow definition directly
     */
    public CompletableFuture<WorkflowResult> execute(WorkflowDefinition definition, 
                                                     Map<String, Object> inputs) {
        return execute(definition, inputs, null);
    }
    
    /**
     * Execute a workflow definition with a specific correlation ID
     * Used by WorkflowSchedulerService for scheduled workflow executions
     */
    public CompletableFuture<WorkflowResult> execute(WorkflowDefinition definition, 
                                                     Map<String, Object> inputs,
                                                     String correlationId) {
        String workflowInstanceId = UUID.randomUUID().toString();
        if (correlationId == null) {
            correlationId = (String) inputs.getOrDefault("correlationId", workflowInstanceId);
        }
        
        logger.info("Starting workflow execution: {} [{}]", definition.getName(), workflowInstanceId);
        
        try {
            // Ensure workflow definition is registered
            if (definitionService.getWorkflowDefinition(definition.getName(), definition.getVersion()).isEmpty()) {
                definitionService.registerWorkflow(definition);
            }
            
            // Create workflow instance in database
            WorkflowInstanceEntity instance = persistenceService.createWorkflowInstance(
                workflowInstanceId,
                definition.getName(),
                definition.getVersion(),
                inputs,
                correlationId,
                    null
            );
            
            // Create initial tasks
            persistenceService.createInitialTasks(instance, definition);
            
            // Track running workflow
            runningWorkflows.put(workflowInstanceId, instance);
            
            // Start async monitoring of workflow completion
            return CompletableFuture.supplyAsync(() -> {
                try {
                    // Wait for workflow processor to pick up and process the workflow
                    return waitForCompletion(workflowInstanceId, definition.getTimeout());
                } finally {
                    runningWorkflows.remove(workflowInstanceId);
                }
            });
            
        } catch (Exception e) {
            logger.error("Failed to start workflow: {}", definition.getName(), e);
            runningWorkflows.remove(workflowInstanceId);
            return CompletableFuture.failedFuture(e);
        }
    }
    
    /**
     * Execute a workflow and wait synchronously for completion
     */
    public WorkflowResult executeSync(WorkflowDefinition definition, Map<String, Object> inputs) {
        return execute(definition, inputs).join();
    }
    
    /**
     * Execute a workflow by name and wait synchronously
     */
    public WorkflowResult executeSync(String workflowName, Map<String, Object> inputs) {
        return execute(workflowName, inputs).join();
    }
    
    /**
     * Get the current status of a running workflow
     */
    public Optional<WorkflowState> getWorkflowStatus(String workflowInstanceId) {
        WorkflowInstanceEntity instance = persistenceService.getWorkflowInstance(workflowInstanceId);
        return instance != null ? Optional.of(instance.getState()) : Optional.empty();
    }
    
    /**
     * Cancel a running workflow
     */
    public boolean cancelWorkflow(String workflowInstanceId) {
        logger.info("Cancelling workflow: {}", workflowInstanceId);
        
        WorkflowInstanceEntity instance = persistenceService.getWorkflowInstance(workflowInstanceId);
        if (instance == null) {
            return false;
        }
        
        if (instance.getState() == WorkflowState.COMPLETED || 
            instance.getState() == WorkflowState.FAILED ||
            instance.getState() == WorkflowState.CANCELLED) {
            return false; // Already in terminal state
        }
        
        persistenceService.updateWorkflowState(workflowInstanceId, WorkflowState.CANCELLED);
        persistenceService.cancelPendingTasks(workflowInstanceId);
        
        return true;
    }
    
    /**
     * Pause a running workflow
     */
    public boolean pauseWorkflow(String workflowInstanceId) {
        logger.info("Pausing workflow: {}", workflowInstanceId);
        
        WorkflowInstanceEntity instance = persistenceService.getWorkflowInstance(workflowInstanceId);
        if (instance == null || instance.getState() != WorkflowState.RUNNING) {
            return false;
        }
        
        persistenceService.updateWorkflowState(workflowInstanceId, WorkflowState.PAUSED);
        return true;
    }
    
    /**
     * Resume a paused workflow
     */
    public boolean resumeWorkflow(String workflowInstanceId) {
        logger.info("Resuming workflow: {}", workflowInstanceId);
        
        WorkflowInstanceEntity instance = persistenceService.getWorkflowInstance(workflowInstanceId);
        if (instance == null || instance.getState() != WorkflowState.PAUSED) {
            return false;
        }
        
        persistenceService.updateWorkflowState(workflowInstanceId, WorkflowState.RUNNING);
        return true;
    }
    
    /**
     * Retry a failed workflow from the beginning
     */
    public CompletableFuture<WorkflowResult> retryWorkflow(String workflowInstanceId) {
        WorkflowInstanceEntity instance = persistenceService.getWorkflowInstance(workflowInstanceId);
        if (instance == null || instance.getState() != WorkflowState.FAILED) {
            return CompletableFuture.failedFuture(
                new IllegalStateException("Workflow not found or not in failed state"));
        }
        
        // Create new workflow instance with same inputs
//        return execute(instance.getWorkflowName(), instance.getWorkflowVersion(),
//                      instance.getInputData());
        return null;
    }
    
    /**
     * Register a custom task implementation
     */
    public void registerTask(String taskName, Task<?> task) {
        taskRegistry.register(taskName, task);
    }
    
    /**
     * Wait for workflow completion
     */
    private WorkflowResult waitForCompletion(String workflowInstanceId, Duration timeout) {
        long startTime = System.currentTimeMillis();
        long timeoutMs = timeout != null ? timeout.toMillis() : Duration.ofHours(1).toMillis();
        
        while (System.currentTimeMillis() - startTime < timeoutMs) {
            WorkflowInstanceEntity instance = persistenceService.getWorkflowInstance(workflowInstanceId);
            
            if (instance == null) {
                throw new IllegalStateException("Workflow instance not found: " + workflowInstanceId);
            }
            
            WorkflowState state = instance.getState();
            
            // Check for terminal states
            if (state == WorkflowState.COMPLETED || 
                state == WorkflowState.FAILED || 
                state == WorkflowState.CANCELLED ||
                state == WorkflowState.TERMINATED) {
                
                // Load final execution context
                ExecutionContext context = persistenceService.loadExecutionContext(workflowInstanceId);
                
                return WorkflowResult.builder()
                    .workflowId(workflowInstanceId)
                    .workflowName(instance.getWorkflowName())
                    .state(state)
                    .startTime(instance.getStartedAt())
                    .endTime(instance.getCompletedAt() != null ? instance.getCompletedAt() : Instant.now())
                    .outputData(context != null ? context.getAllData() : Map.of())
                    .errors(instance.getErrors())
                    .build();
            }
            
            try {
                Thread.sleep(1000); // Poll every second
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Workflow execution interrupted", e);
            }
        }
        
        // Timeout reached
        logger.warn("Workflow {} timed out after {} ms", workflowInstanceId, timeoutMs);
        cancelWorkflow(workflowInstanceId);
        
        return WorkflowResult.builder()
            .workflowId(workflowInstanceId)
            .state(WorkflowState.TIMED_OUT)
            .startTime(Instant.now().minusMillis(timeoutMs))
            .endTime(Instant.now())
            .errors(java.util.List.of("Workflow execution timed out"))
            .build();
    }
}