package com.taskflow.api;

import com.taskflow.persistence.entity.ScheduledWorkflowEntity;
import com.taskflow.scheduling.WorkflowSchedulerService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * REST API for managing scheduled workflows.
 */
@RestController
@RequestMapping("/api/v1/workflows/schedules")
@Tag(name = "Scheduled Workflows", description = "Manage scheduled workflow execution")
public class ScheduledWorkflowController {
    
    private static final Logger logger = LoggerFactory.getLogger(ScheduledWorkflowController.class);
    
    @Autowired
    private WorkflowSchedulerService schedulerService;
    
    /**
     * Create a one-time scheduled workflow
     */
    @PostMapping("/one-time")
    @Operation(summary = "Schedule a workflow to run once at a specific time")
    public ResponseEntity<ScheduleResponse> scheduleOneTimeWorkflow(
            @Valid @RequestBody OneTimeScheduleRequest request,
            Authentication authentication) {
        
        logger.info("Creating one-time schedule for workflow: {}", request.getWorkflowName());
        
        String scheduleId = schedulerService.scheduleWorkflow(
            request.getWorkflowName(),
            request.getWorkflowVersion(),
            request.getExecutionTime(),
            request.getInputData(),
            request.getDescription(),
            authentication != null ? authentication.getName() : "system"
        );
        
        return ResponseEntity.ok(new ScheduleResponse(scheduleId, "One-time schedule created successfully"));
    }
    
    /**
     * Create a recurring scheduled workflow
     */
    @PostMapping("/recurring")
    @Operation(summary = "Schedule a workflow to run at regular intervals")
    public ResponseEntity<ScheduleResponse> scheduleRecurringWorkflow(
            @Valid @RequestBody RecurringScheduleRequest request,
            Authentication authentication) {
        
        logger.info("Creating recurring schedule for workflow: {}", request.getWorkflowName());
        
        Duration interval = Duration.ofSeconds(request.getIntervalSeconds());
        
        String scheduleId = schedulerService.scheduleRecurringWorkflow(
            request.getWorkflowName(),
            request.getWorkflowVersion(),
            interval,
            request.isFixedRate(),
            request.getInputData(),
            request.getDescription(),
            authentication != null ? authentication.getName() : "system",
            request.getStartTime(),
            request.getEndTime()
        );
        
        return ResponseEntity.ok(new ScheduleResponse(scheduleId, "Recurring schedule created successfully"));
    }
    
    /**
     * Create a cron-based scheduled workflow
     */
    @PostMapping("/cron")
    @Operation(summary = "Schedule a workflow using a cron expression")
    public ResponseEntity<ScheduleResponse> scheduleCronWorkflow(
            @Valid @RequestBody CronScheduleRequest request,
            Authentication authentication) {
        
        logger.info("Creating cron schedule for workflow: {} with expression: {}", 
            request.getWorkflowName(), request.getCronExpression());
        
        try {
            String scheduleId = schedulerService.scheduleCronWorkflow(
                request.getWorkflowName(),
                request.getWorkflowVersion(),
                request.getCronExpression(),
                request.getInputData(),
                request.getDescription(),
                authentication != null ? authentication.getName() : "system",
                request.getTimezone(),
                request.getEndTime()
            );
            
            return ResponseEntity.ok(new ScheduleResponse(scheduleId, "Cron schedule created successfully"));
            
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest()
                .body(new ScheduleResponse(null, "Invalid cron expression: " + e.getMessage()));
        }
    }
    
    /**
     * Get schedule details
     */
    @GetMapping("/{scheduleId}")
    @Operation(summary = "Get details of a scheduled workflow")
    public ResponseEntity<ScheduledWorkflowEntity> getSchedule(@PathVariable String scheduleId) {
        Optional<ScheduledWorkflowEntity> schedule = schedulerService.getSchedule(scheduleId);
        
        return schedule.map(ResponseEntity::ok)
            .orElse(ResponseEntity.notFound().build());
    }
    
    /**
     * Get all schedules for a workflow
     */
    @GetMapping("/workflow/{workflowName}")
    @Operation(summary = "Get all schedules for a specific workflow")
    public ResponseEntity<List<ScheduledWorkflowEntity>> getWorkflowSchedules(
            @PathVariable String workflowName) {
        
        List<ScheduledWorkflowEntity> schedules = schedulerService.getSchedulesByWorkflow(workflowName);
        return ResponseEntity.ok(schedules);
    }
    
    /**
     * Get all active schedules
     */
    @GetMapping("/active")
    @Operation(summary = "Get all active scheduled workflows")
    public ResponseEntity<List<ScheduledWorkflowEntity>> getActiveSchedules() {
        List<ScheduledWorkflowEntity> schedules = schedulerService.getActiveSchedules();
        return ResponseEntity.ok(schedules);
    }
    
    /**
     * Pause a scheduled workflow
     */
    @PutMapping("/{scheduleId}/pause")
    @Operation(summary = "Pause a scheduled workflow")
    public ResponseEntity<ScheduleResponse> pauseSchedule(@PathVariable String scheduleId) {
        boolean success = schedulerService.pauseSchedule(scheduleId);
        
        if (success) {
            return ResponseEntity.ok(new ScheduleResponse(scheduleId, "Schedule paused successfully"));
        } else {
            return ResponseEntity.notFound().build();
        }
    }
    
    /**
     * Resume a paused scheduled workflow
     */
    @PutMapping("/{scheduleId}/resume")
    @Operation(summary = "Resume a paused scheduled workflow")
    public ResponseEntity<ScheduleResponse> resumeSchedule(@PathVariable String scheduleId) {
        boolean success = schedulerService.resumeSchedule(scheduleId);
        
        if (success) {
            return ResponseEntity.ok(new ScheduleResponse(scheduleId, "Schedule resumed successfully"));
        } else {
            return ResponseEntity.notFound().build();
        }
    }
    
    /**
     * Delete a scheduled workflow
     */
    @DeleteMapping("/{scheduleId}")
    @Operation(summary = "Delete a scheduled workflow")
    public ResponseEntity<ScheduleResponse> deleteSchedule(@PathVariable String scheduleId) {
        boolean success = schedulerService.deleteSchedule(scheduleId);
        
        if (success) {
            return ResponseEntity.ok(new ScheduleResponse(scheduleId, "Schedule deleted successfully"));
        } else {
            return ResponseEntity.notFound().build();
        }
    }
    
    /**
     * Trigger immediate execution of a scheduled workflow
     */
    @PostMapping("/{scheduleId}/execute")
    @Operation(summary = "Trigger immediate execution of a scheduled workflow")
    public ResponseEntity<ScheduleResponse> executeNow(@PathVariable String scheduleId) {
        Optional<ScheduledWorkflowEntity> scheduleOpt = schedulerService.getSchedule(scheduleId);
        
        if (scheduleOpt.isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        
        // TODO: Implement immediate execution
        return ResponseEntity.ok(new ScheduleResponse(scheduleId, "Execution triggered"));
    }
    
    // Request/Response DTOs
    
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class OneTimeScheduleRequest {
        @NotBlank
        private String workflowName;
        
        private Integer workflowVersion;
        
        @NotNull
        private Instant executionTime;
        
        private Map<String, Object> inputData;
        
        private String description;
    }
    
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class RecurringScheduleRequest {
        @NotBlank
        private String workflowName;
        
        private Integer workflowVersion;
        
        @NotNull
        private Long intervalSeconds;
        
        private boolean fixedRate = true;
        
        private Map<String, Object> inputData;
        
        private String description;
        
        private Instant startTime;
        
        private Instant endTime;
    }
    
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CronScheduleRequest {
        @NotBlank
        private String workflowName;
        
        private Integer workflowVersion;
        
        @NotBlank
        private String cronExpression;
        
        private Map<String, Object> inputData;
        
        private String description;
        
        private String timezone = "UTC";
        
        private Instant endTime;
    }
    
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ScheduleResponse {
        private String scheduleId;
        private String message;
    }
}