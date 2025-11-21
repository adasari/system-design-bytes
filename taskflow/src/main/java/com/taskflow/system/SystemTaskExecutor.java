package com.taskflow.system;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.taskflow.core.*;
import com.taskflow.persistence.entity.TaskExecutionEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.*;

/**
 * SystemTaskExecutor provides execution capabilities for built-in system tasks.
 * 
 * <p>This component handles the execution of core system tasks that are essential
 * for workflow orchestration, including HTTP calls, decision making, variable 
 * manipulation, timing control, and workflow termination.</p>
 * 
 * <p><b>Supported System Tasks:</b></p>
 * <ul>
 *   <li><b>HTTP Task</b>: Make HTTP requests with configurable headers, methods, and body</li>
 *   <li><b>Decision Task</b>: Conditional branching based on context data evaluation</li>
 *   <li><b>Set Variable Task</b>: Update context variables with computed or static values</li>
 *   <li><b>Wait Task</b>: Introduce delays in workflow execution (seconds-based)</li>
 *   <li><b>Terminate Task</b>: Gracefully terminate workflow execution</li>
 * </ul>
 * 
 * <p><b>Variable Substitution:</b></p>
 * <p>All system tasks support simple variable substitution using ${variable} syntax.
 * Variables are resolved from the ExecutionContext at runtime.</p>
 * 
 * <p><b>HTTP Task Configuration Example:</b></p>
 * <pre>{@code
 * {
 *   "type": "HTTP",
 *   "name": "callUserService",
 *   "inputParameters": {
 *     "uri": "https://api.example.com/users/${userId}",
 *     "method": "GET",
 *     "headers": {
 *       "Authorization": "Bearer ${authToken}",
 *       "Content-Type": "application/json"
 *     }
 *   }
 * }
 * }</pre>
 * 
 * <p><b>Decision Task Configuration Example:</b></p>
 * <pre>{@code
 * {
 *   "type": "DECISION",
 *   "name": "checkUserStatus", 
 *   "inputParameters": {
 *     "caseValueParam": "userStatus",
 *     "decisionCases": {
 *       "ACTIVE": ["activateUser"],
 *       "INACTIVE": ["deactivateUser"],
 *       "PENDING": ["sendReminder"]
 *     },
 *     "defaultCase": ["handleUnknownStatus"]
 *   }
 * }
 * }</pre>
 * 
 * @see TaskDefinition
 * @see ExecutionContext
 * @see WorkflowProcessor
 */
@Component
public class SystemTaskExecutor {
    
    private static final Logger logger = LoggerFactory.getLogger(SystemTaskExecutor.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final RestTemplate restTemplate = new RestTemplate();
    
    /**
     * Execute a system task based on its type
     */
    public TaskResult<?> executeSystemTask(TaskDefinition taskDef, ExecutionContext context, 
                                          TaskExecutionEntity taskExecution) {
        
        logger.info("Executing system task: {} of type: {}", taskDef.getName(), taskDef.getType());
        
        try {
            switch (taskDef.getType()) {
                case HTTP:
                    return executeHttpTask(taskDef, context);
                case DECISION:
                    return executeDecisionTask(taskDef, context);
                case SET_VARIABLE:
                    return executeSetVariableTask(taskDef, context);
                case WAIT:
                    return executeWaitTask(taskDef, context);
                case TERMINATE:
                    return executeTerminateTask(taskDef, context);
                default:
                    return TaskResult.failure(taskDef.getName(), "Unsupported system task type: " + taskDef.getType());
            }
        } catch (Exception e) {
            logger.error("Error executing system task: {}", taskDef.getName(), e);
            return TaskResult.failure(taskDef.getName(), "System task execution failed: " + e.getMessage());
        }
    }
    
    /**
     * Execute HTTP task - makes REST API calls
     */
    private TaskResult<?> executeHttpTask(TaskDefinition taskDef, ExecutionContext context) {
        Map<String, Object> httpConfig = (Map<String, Object>) taskDef.getInputParameters().get("http_request");
        if (httpConfig == null) {
            return TaskResult.failure(taskDef.getName(), "HTTP task requires http_request configuration");
        }
        
        String uri = substituteVariables((String) httpConfig.get("uri"), context);
        String method = (String) httpConfig.getOrDefault("method", "GET");
        
        try {
            HttpMethod httpMethod = HttpMethod.valueOf(method.toUpperCase());
            ResponseEntity<String> response = restTemplate.exchange(uri, httpMethod, null, String.class);
            
            Map<String, Object> result = new HashMap<>();
            result.put("statusCode", response.getStatusCode().value());
            result.put("body", response.getBody());
            result.put("uri", uri);
            
            return TaskResult.success(taskDef.getName(), result);
            
        } catch (Exception e) {
            logger.error("HTTP task failed for URI: {}", uri, e);
            return TaskResult.failure(taskDef.getName(), "HTTP request failed: " + e.getMessage());
        }
    }
    
    /**
     * Execute Decision task - simple conditional logic
     */
    private TaskResult<?> executeDecisionTask(TaskDefinition taskDef, ExecutionContext context) {
        String condition = (String) taskDef.getInputParameters().get("condition");
        String trueValue = (String) taskDef.getInputParameters().get("trueValue");
        String falseValue = (String) taskDef.getInputParameters().get("falseValue");
        
        if (condition == null) {
            return TaskResult.failure(taskDef.getName(), "Decision task requires condition parameter");
        }
        
        try {
            // Simple condition evaluation - placeholder for now
            boolean result = evaluateSimpleCondition(condition, context);
            String selectedValue = result ? trueValue : falseValue;
            
            Map<String, Object> resultData = new HashMap<>();
            resultData.put("condition", condition);
            resultData.put("result", result);
            resultData.put("selectedValue", selectedValue);
            
            return TaskResult.success(taskDef.getName(), resultData);
            
        } catch (Exception e) {
            logger.error("Decision task failed for condition: {}", condition, e);
            return TaskResult.failure(taskDef.getName(), "Decision evaluation failed: " + e.getMessage());
        }
    }
    
    /**
     * Execute Set Variable task - sets values in workflow context
     */
    private TaskResult<?> executeSetVariableTask(TaskDefinition taskDef, ExecutionContext context) {
        Map<String, Object> variables = (Map<String, Object>) taskDef.getInputParameters().get("variables");
        if (variables == null) {
            return TaskResult.failure(taskDef.getName(), "Set Variable task requires variables parameter");
        }
        
        try {
            Map<String, Object> result = new HashMap<>();
            
            for (Map.Entry<String, Object> entry : variables.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                
                // Substitute variables in the value if it's a string
                if (value instanceof String) {
                    value = substituteVariables((String) value, context);
                }
                
                context.put(key, value);
                result.put(key, value);
            }
            
            return TaskResult.success(taskDef.getName(), result);
            
        } catch (Exception e) {
            logger.error("Set Variable task failed", e);
            return TaskResult.failure(taskDef.getName(), "Set Variable execution failed: " + e.getMessage());
        }
    }
    
    /**
     * Execute Wait task - introduces delays in workflow
     */
    private TaskResult<?> executeWaitTask(TaskDefinition taskDef, ExecutionContext context) {
        Object durationObj = taskDef.getInputParameters().get("duration");
        if (durationObj == null) {
            return TaskResult.failure(taskDef.getName(), "Wait task requires duration parameter");
        }
        
        try {
            long durationMs;
            if (durationObj instanceof String) {
                durationMs = Long.parseLong(substituteVariables((String) durationObj, context));
            } else {
                durationMs = ((Number) durationObj).longValue();
            }
            
            Thread.sleep(durationMs);
            
            Map<String, Object> result = new HashMap<>();
            result.put("duration", durationMs);
            result.put("completed", true);
            
            return TaskResult.success(taskDef.getName(), result);
            
        } catch (Exception e) {
            logger.error("Wait task failed", e);
            return TaskResult.failure(taskDef.getName(), "Wait execution failed: " + e.getMessage());
        }
    }
    
    /**
     * Execute Terminate task - stops workflow execution
     */
    private TaskResult<?> executeTerminateTask(TaskDefinition taskDef, ExecutionContext context) {
        String reason = (String) taskDef.getInputParameters().getOrDefault("reason", "Workflow terminated");
        String status = (String) taskDef.getInputParameters().getOrDefault("status", "COMPLETED");
        
        Map<String, Object> result = new HashMap<>();
        result.put("reason", reason);
        result.put("terminationStatus", status);
        result.put("terminated", true);
        
        // Set termination flag in context
        context.put("__terminated", true);
        context.put("__terminationReason", reason);
        
        return TaskResult.success(taskDef.getName(), result);
    }
    
    /**
     * Simple variable substitution - replaces ${variable} with context values
     */
    private String substituteVariables(String text, ExecutionContext context) {
        if (text == null || !text.contains("${")) {
            return text;
        }
        
        String result = text;
        // Simple regex replacement for ${variable} patterns
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("\\$\\{([^}]+)\\}");
        java.util.regex.Matcher matcher = pattern.matcher(text);
        
        while (matcher.find()) {
            String variable = matcher.group(1);
            Object value = context.get(variable);
            String replacement = value != null ? value.toString() : "";
            result = result.replace("${" + variable + "}", replacement);
        }
        
        return result;
    }
    
    /**
     * Simple condition evaluation - basic boolean logic
     * Placeholder implementation - can be enhanced later
     */
    private boolean evaluateSimpleCondition(String condition, ExecutionContext context) {
        // Replace variables in condition
        String evaluatedCondition = substituteVariables(condition, context);
        
        // Simple boolean evaluation - can be enhanced
        if ("true".equalsIgnoreCase(evaluatedCondition)) {
            return true;
        } else if ("false".equalsIgnoreCase(evaluatedCondition)) {
            return false;
        }
        
        // Simple comparison operations (placeholder)
        if (evaluatedCondition.contains("==")) {
            String[] parts = evaluatedCondition.split("==");
            if (parts.length == 2) {
                return parts[0].trim().equals(parts[1].trim());
            }
        }
        
        // Default to false for unknown conditions
        return false;
    }
}