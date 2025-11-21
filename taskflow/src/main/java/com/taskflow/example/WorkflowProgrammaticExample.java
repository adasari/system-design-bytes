package com.taskflow.example;

import com.taskflow.core.*;
import com.taskflow.executor.TaskRegistry;
import com.taskflow.executor.WorkflowExecutor;
import com.taskflow.parallel.DynamicForkTask;
import com.taskflow.parallel.ForkJoinTask;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * Comprehensive examples of programmatically defining and executing workflows.
 * 
 * This class demonstrates various patterns for creating workflows in code,
 * including simple sequential flows, parallel execution, conditional logic,
 * loops, and sub-workflows.
 */
@Component
public class WorkflowProgrammaticExample {

    @Autowired
    private WorkflowExecutor workflowExecutor;
    
    @Autowired
    private TaskRegistry taskRegistry;

    /**
     * Example 1: Simple Sequential Workflow
     * 
     * Order Processing: Validate -> Check Inventory -> Process Payment -> Ship
     */
    public WorkflowDefinition createOrderProcessingWorkflow() {
        return WorkflowDefinition.builder()
            .name("orderProcessing")
            .version(1)
            .description("Process customer orders sequentially")
            .timeout(Duration.ofMinutes(30))
            
            // Task 1: Validate Order
            .addTask(TaskDefinition.builder()
                .name("ValidateOrder")
                .taskReferenceName("validateOrder")
                .type(TaskDefinition.TaskType.SIMPLE)
                .inputParameters(Map.of(
                    "orderId", "${orderId}",
                    "customerId", "${customerId}"
                ))
                .build())
            
            // Task 2: Check Inventory
            .addTask(TaskDefinition.builder()
                .name("CheckInventory")
                .taskReferenceName("checkInventory")
                .type(TaskDefinition.TaskType.HTTP)
                .inputParameters(Map.of(
                    "uri", "https://api.inventory.com/check",
                    "method", "POST",
                    "body", Map.of(
                        "items", "${order.items}",
                        "warehouse", "${order.warehouse}"
                    ),
                    "headers", Map.of(
                        "Authorization", "Bearer ${authToken}",
                        "Content-Type", "application/json"
                    )
                ))
                .dependencies(List.of("validateOrder"))
                .retryCount(3)
                .timeout(Duration.ofSeconds(30))
                .build())
            
            // Task 3: Process Payment
            .addTask(TaskDefinition.builder()
                .name("ProcessPayment")
                .taskReferenceName("processPayment")
                .type(TaskDefinition.TaskType.SIMPLE)
                .inputParameters(Map.of(
                    "amount", "${order.totalAmount}",
                    "paymentMethod", "${order.paymentMethod}",
                    "customerId", "${customerId}"
                ))
                .dependencies(List.of("checkInventory"))
                .build())
            
            // Task 4: Ship Order
            .addTask(TaskDefinition.builder()
                .name("ShipOrder")
                .taskReferenceName("shipOrder")
                .type(TaskDefinition.TaskType.SIMPLE)
                .inputParameters(Map.of(
                    "orderId", "${orderId}",
                    "shippingAddress", "${order.shippingAddress}",
                    "shippingMethod", "${order.shippingMethod}"
                ))
                .dependencies(List.of("processPayment"))
                .build())
            
            .build();
    }

    /**
     * Example 2: Parallel Execution with Fork-Join
     * 
     * Process payment and prepare shipping in parallel, then notify customer
     */
    public WorkflowDefinition createParallelWorkflow() {
        return WorkflowDefinition.builder()
            .name("parallelOrderProcessing")
            .version(1)
            .description("Process order with parallel tasks")
            
            // Validate order first
            .addTask(TaskDefinition.builder()
                .name("ValidateOrder")
                .taskReferenceName("validateOrder")
                .type(TaskDefinition.TaskType.SIMPLE)
                .build())
            
            // Fork: Process payment and shipping in parallel
            .addTask(TaskDefinition.builder()
                .name("ForkTasks")
                .taskReferenceName("forkPaymentShipping")
                .type(TaskDefinition.TaskType.FORK_JOIN)
                .inputParameters(Map.of(
                    "forkTasks", List.of(
                        // Branch 1: Payment processing
                        List.of(
                            TaskDefinition.builder()
                                .name("ProcessPayment")
                                .taskReferenceName("processPayment")
                                .type(TaskDefinition.TaskType.SIMPLE)
                                .inputParameters(Map.of(
                                    "amount", "${order.amount}"
                                ))
                                .build(),
                            TaskDefinition.builder()
                                .name("UpdatePaymentStatus")
                                .taskReferenceName("updatePaymentStatus")
                                .type(TaskDefinition.TaskType.SIMPLE)
                                .build()
                        ),
                        // Branch 2: Shipping preparation
                        List.of(
                            TaskDefinition.builder()
                                .name("AllocateInventory")
                                .taskReferenceName("allocateInventory")
                                .type(TaskDefinition.TaskType.SIMPLE)
                                .build(),
                            TaskDefinition.builder()
                                .name("GenerateShippingLabel")
                                .taskReferenceName("generateShippingLabel")
                                .type(TaskDefinition.TaskType.SIMPLE)
                                .build()
                        )
                    )
                ))
                .dependencies(List.of("validateOrder"))
                .build())
            
            // Join: Send notification after both branches complete
            .addTask(TaskDefinition.builder()
                .name("JOIN")
                .taskReferenceName("joinTasks")
                .type(TaskDefinition.TaskType.JOIN)
                .dependencies(List.of("forkPaymentShipping"))
                .joinOn(List.of("processPayment", "generateShippingLabel"))
                .build())
            
            // Notify customer
            .addTask(TaskDefinition.builder()
                .name("NotifyCustomer")
                .taskReferenceName("notifyCustomer")
                .type(TaskDefinition.TaskType.SIMPLE)
                .dependencies(List.of("joinTasks"))
                .build())
            
            .build();
    }

    /**
     * Example 3: Conditional Workflow with Decision Tasks
     * 
     * Route orders based on priority and amount
     */
    public WorkflowDefinition createConditionalWorkflow() {
        return WorkflowDefinition.builder()
            .name("conditionalOrderRouting")
            .version(1)
            .description("Route orders based on conditions")
            
            // Check order priority
            .addTask(TaskDefinition.builder()
                .name("CheckOrderPriority")
                .taskReferenceName("checkPriority")
                .type(TaskDefinition.TaskType.DECISION)
                .inputParameters(Map.of(
                    "caseValueParam", "order.priority",
                    "decisionCases", Map.of(
                        "HIGH", List.of("expressProcessing"),
                        "NORMAL", List.of("standardProcessing"),
                        "LOW", List.of("economyProcessing")
                    ),
                    "defaultCase", List.of("standardProcessing")
                ))
                .build())
            
            // Express processing path
            .addTask(TaskDefinition.builder()
                .name("ExpressProcessing")
                .taskReferenceName("expressProcessing")
                .type(TaskDefinition.TaskType.SIMPLE)
                .inputParameters(Map.of(
                    "expedite", true,
                    "maxProcessingTime", "2h"
                ))
                .optional(true)
                .build())
            
            // Standard processing path
            .addTask(TaskDefinition.builder()
                .name("StandardProcessing")
                .taskReferenceName("standardProcessing")
                .type(TaskDefinition.TaskType.SIMPLE)
                .optional(true)
                .build())
            
            // Economy processing path
            .addTask(TaskDefinition.builder()
                .name("EconomyProcessing")
                .taskReferenceName("economyProcessing")
                .type(TaskDefinition.TaskType.SIMPLE)
                .optional(true)
                .build())
            
            // Final notification (all paths converge here)
            .addTask(TaskDefinition.builder()
                .name("FinalNotification")
                .taskReferenceName("finalNotification")
                .type(TaskDefinition.TaskType.SIMPLE)
                .dependencies(List.of("expressProcessing", "standardProcessing", "economyProcessing"))
                .build())
            
            .build();
    }

    /**
     * Example 4: Dynamic Fork - Process variable number of items
     */
    public WorkflowDefinition createDynamicForkWorkflow() {
        // Register the dynamic fork task
        DynamicForkTask dynamicTask = DynamicForkTask.builder()
            .name("ProcessOrderItems")
            .taskGenerator(context -> {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> items = (List<Map<String, Object>>) context.get("orderItems");
                Map<String, Task<?>> tasks = new HashMap<>();
                
                for (int i = 0; i < items.size(); i++) {
                    Map<String, Object> item = items.get(i);
                    String taskId = "processItem_" + i;
                    
                    // Create a task for each item
                    Task<Map<String, Object>> itemTask = ctx -> {
                        // Process individual item
                        Map<String, Object> result = new HashMap<>();
                        result.put("itemId", item.get("id"));
                        result.put("processed", true);
                        result.put("timestamp", System.currentTimeMillis());
                        return CompletableFuture.completedFuture(
                            TaskResult.success("processItem", result)
                        );
                    };
                    
                    tasks.put(taskId, itemTask);
                }
                
                return tasks;
            })
            .resultAggregator((context, results) -> {
                Map<String, Object> summary = new HashMap<>();
                summary.put("totalItems", results.size());
                summary.put("successCount", results.values().stream()
                    .filter(TaskResult::isSuccess)
                    .count());
                return summary;
            })
            .maxConcurrency(5)
            .waitForAll(true)
            .build();
        
        taskRegistry.register("DynamicItemProcessor", dynamicTask);
        
        return WorkflowDefinition.builder()
            .name("dynamicItemProcessing")
            .version(1)
            .description("Process variable number of items dynamically")
            
            .addTask(TaskDefinition.builder()
                .name("DynamicItemProcessor")
                .taskReferenceName("processItems")
                .type(TaskDefinition.TaskType.DYNAMIC)
                .inputParameters(Map.of(
                    "orderItems", "${orderItems}"
                ))
                .build())
            
            .addTask(TaskDefinition.builder()
                .name("AggregateResults")
                .taskReferenceName("aggregateResults")
                .type(TaskDefinition.TaskType.SIMPLE)
                .dependencies(List.of("processItems"))
                .build())
            
            .build();
    }

    /**
     * Example 5: Sub-Workflow Composition
     */
    public WorkflowDefinition createMainWorkflowWithSubWorkflow() {
        return WorkflowDefinition.builder()
            .name("mainWorkflow")
            .version(1)
            .description("Main workflow that calls sub-workflows")
            
            // Prepare data
            .addTask(TaskDefinition.builder()
                .name("PrepareData")
                .taskReferenceName("prepareData")
                .type(TaskDefinition.TaskType.SIMPLE)
                .build())
            
            // Call sub-workflow
            .addTask(TaskDefinition.builder()
                .name("ProcessSubWorkflow")
                .taskReferenceName("subWorkflow1")
                .type(TaskDefinition.TaskType.SUB_WORKFLOW)
                .inputParameters(Map.of(
                    "subWorkflowParam", Map.of(
                        "name", "orderProcessing",
                        "version", 1,
                        "inputs", Map.of(
                            "orderId", "${prepareData.result.orderId}",
                            "customerId", "${prepareData.result.customerId}"
                        )
                    )
                ))
                .dependencies(List.of("prepareData"))
                .build())
            
            // Process sub-workflow results
            .addTask(TaskDefinition.builder()
                .name("ProcessResults")
                .taskReferenceName("processResults")
                .type(TaskDefinition.TaskType.SIMPLE)
                .inputParameters(Map.of(
                    "subWorkflowOutput", "${subWorkflow1.output}"
                ))
                .dependencies(List.of("subWorkflow1"))
                .build())
            
            .build();
    }

    /**
     * Example 6: Workflow with Wait and Terminate conditions
     */
    public WorkflowDefinition createWaitAndTerminateWorkflow() {
        return WorkflowDefinition.builder()
            .name("waitAndTerminate")
            .version(1)
            .description("Workflow with wait and conditional termination")
            
            // Initial processing
            .addTask(TaskDefinition.builder()
                .name("InitialProcessing")
                .taskReferenceName("initialProcessing")
                .type(TaskDefinition.TaskType.SIMPLE)
                .build())
            
            // Wait for external trigger
            .addTask(TaskDefinition.builder()
                .name("WaitForApproval")
                .taskReferenceName("waitForApproval")
                .type(TaskDefinition.TaskType.WAIT)
                .inputParameters(Map.of(
                    "duration", "60"  // Wait 60 seconds
                ))
                .dependencies(List.of("initialProcessing"))
                .build())
            
            // Check termination condition
            .addTask(TaskDefinition.builder()
                .name("CheckTermination")
                .taskReferenceName("checkTermination")
                .type(TaskDefinition.TaskType.DECISION)
                .inputParameters(Map.of(
                    "caseValueParam", "shouldTerminate",
                    "decisionCases", Map.of(
                        "true", List.of("terminateWorkflow"),
                        "false", List.of("continueProcessing")
                    )
                ))
                .dependencies(List.of("waitForApproval"))
                .build())
            
            // Terminate if needed
            .addTask(TaskDefinition.builder()
                .name("TerminateWorkflow")
                .taskReferenceName("terminateWorkflow")
                .type(TaskDefinition.TaskType.TERMINATE)
                .inputParameters(Map.of(
                    "terminationStatus", "COMPLETED",
                    "workflowOutput", Map.of(
                        "message", "Workflow terminated based on condition"
                    )
                ))
                .optional(true)
                .build())
            
            // Continue processing
            .addTask(TaskDefinition.builder()
                .name("ContinueProcessing")
                .taskReferenceName("continueProcessing")
                .type(TaskDefinition.TaskType.SIMPLE)
                .optional(true)
                .build())
            
            .build();
    }

    /**
     * Execute a workflow example
     */
    public void executeWorkflowExample() {
        // Create workflow
        WorkflowDefinition workflow = createOrderProcessingWorkflow();
        
        // Prepare input data
        Map<String, Object> inputs = new HashMap<>();
        inputs.put("orderId", "ORD-12345");
        inputs.put("customerId", "CUST-67890");
        inputs.put("order", Map.of(
            "items", List.of(
                Map.of("id", "ITEM-1", "quantity", 2),
                Map.of("id", "ITEM-2", "quantity", 1)
            ),
            "totalAmount", 99.99,
            "paymentMethod", "CREDIT_CARD",
            "shippingAddress", "123 Main St, City, State",
            "shippingMethod", "EXPRESS",
            "priority", "HIGH",
            "warehouse", "WH-001"
        ));
        inputs.put("authToken", "secret-token-123");
        
        // Execute workflow asynchronously
        CompletableFuture<WorkflowResult> future = workflowExecutor.execute(workflow, inputs);
        
        // Handle result
        future.thenAccept(result -> {
            System.out.println("Workflow completed!");
            System.out.println("State: " + result.getState());
            System.out.println("Output: " + result.getOutputData());
            System.out.println("Execution time: " + 
                Duration.between(result.getStartTime(), result.getEndTime()).toMillis() + "ms");
        }).exceptionally(error -> {
            System.err.println("Workflow failed: " + error.getMessage());
            return null;
        });
        
        // Or execute synchronously
        WorkflowResult result = workflowExecutor.executeSync(workflow, inputs);
        System.out.println("Sync execution result: " + result.getState());
    }

    /**
     * Register custom tasks
     */
    public void registerCustomTasks() {
        // Register a simple custom task
        taskRegistry.register("ValidateOrder", context -> {
            String orderId = (String) context.get("orderId");
            String customerId = (String) context.get("customerId");
            
            // Validation logic
            boolean isValid = orderId != null && customerId != null;
            
            Map<String, Object> result = Map.of(
                "valid", isValid,
                "orderId", orderId,
                "customerId", customerId,
                "validatedAt", System.currentTimeMillis()
            );
            
            return CompletableFuture.completedFuture(
                isValid 
                    ? TaskResult.success("ValidateOrder", result)
                    : TaskResult.failure("ValidateOrder", "Invalid order data")
            );
        });
        
        // Register a task that processes payment
        taskRegistry.register("ProcessPayment", context -> {
            Double amount = (Double) context.get("amount");
            String paymentMethod = (String) context.get("paymentMethod");
            
            // Simulate payment processing
            Map<String, Object> paymentResult = Map.of(
                "transactionId", UUID.randomUUID().toString(),
                "amount", amount,
                "method", paymentMethod,
                "status", "SUCCESS",
                "processedAt", System.currentTimeMillis()
            );
            
            return CompletableFuture.completedFuture(
                TaskResult.success("ProcessPayment", paymentResult)
            );
        });
        
        // Register shipping task
        taskRegistry.register("ShipOrder", context -> {
            String orderId = (String) context.get("orderId");
            String shippingAddress = (String) context.get("shippingAddress");
            
            Map<String, Object> shippingResult = Map.of(
                "trackingNumber", "TRACK-" + UUID.randomUUID().toString(),
                "orderId", orderId,
                "address", shippingAddress,
                "carrier", "FedEx",
                "estimatedDelivery", System.currentTimeMillis() + 86400000 // +1 day
            );
            
            return CompletableFuture.completedFuture(
                TaskResult.success("ShipOrder", shippingResult)
            );
        });
    }
}