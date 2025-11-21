package com.taskflow.persistence.repository;

import com.taskflow.core.TaskStatus;
import com.taskflow.persistence.entity.TaskExecutionEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import jakarta.persistence.LockModeType;
import java.time.Instant;
import java.util.List;

@Repository
public interface TaskExecutionRepository extends JpaRepository<TaskExecutionEntity, Long> {

    @Query("SELECT t FROM TaskExecutionEntity t WHERE t.workflow.workflowId = :workflowId ORDER BY t.sequenceNumber")
    List<TaskExecutionEntity> findByWorkflowId(@Param("workflowId") String workflowId);

    List<TaskExecutionEntity> findByStatus(TaskStatus status);

    @Query("SELECT t FROM TaskExecutionEntity t WHERE t.status = :status AND t.workerId = :workerId")
    List<TaskExecutionEntity> findByStatusAndWorkerId(@Param("status") TaskStatus status,
                                                      @Param("workerId") String workerId);

    // Distributed task claiming with pessimistic locking
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query(value = "SELECT * FROM task_executions t " +
                   "WHERE t.status = 'PENDING' " +
                   "AND t.worker_id IS NULL " +
                   "AND (t.scheduled_at IS NULL OR t.scheduled_at <= :now) " +
                   "AND NOT EXISTS (" +
                   "  SELECT 1 FROM task_executions d " +
                   "  WHERE d.workflow_id = t.workflow_id " +
                   "  AND t.task_id = ANY(string_to_array(t.dependencies, ',')) " +
                   "  AND d.status NOT IN ('SUCCESS', 'SKIPPED')" +
                   ") " +
                   "ORDER BY t.workflow_id, t.sequence_number " +
                   "LIMIT 1 " +
                   "FOR UPDATE SKIP LOCKED",
           nativeQuery = true)
    TaskExecutionEntity claimNextPendingTask(@Param("nodeId") String nodeId,
                                            @Param("workerId") String workerId,
                                            @Param("now") Instant now);

    // Poll multiple pending tasks for batch processing
    @Query(value = "SELECT * FROM task_executions t " +
                   "WHERE t.status = 'PENDING' " +
                   "AND t.worker_id IS NULL " +
                   "AND (t.scheduled_at IS NULL OR t.scheduled_at <= :now) " +
                   "ORDER BY t.workflow_id, t.sequence_number " +
                   "LIMIT :limit " +
                   "FOR UPDATE SKIP LOCKED",
           nativeQuery = true)
    List<TaskExecutionEntity> pollPendingTasksForNode(@Param("nodeId") String nodeId,
                                                     @Param("limit") int limit,
                                                     @Param("now") Instant now);

    // Try to acquire a specific task with optimistic locking
    @Modifying
    @Transactional
    @Query("UPDATE TaskExecutionEntity t SET t.workerId = :workerId, t.status = 'RUNNING', t.startedAt = :now " +
           "WHERE t.id = :taskId AND t.workerId IS NULL AND t.status = 'PENDING'")
    int tryAcquireTask(@Param("taskId") Long taskId,
                       @Param("workerId") String workerId,
                       @Param("now") Instant now);

    default boolean tryAcquireTask(Long taskId, String workerId) {
        return tryAcquireTask(taskId, workerId, Instant.now()) > 0;
    }

    // Release a task back to the pool
    @Modifying
    @Transactional
    @Query("UPDATE TaskExecutionEntity t SET t.workerId = NULL, t.status = 'PENDING' " +
           "WHERE t.id = :taskId")
    void releaseTask(@Param("taskId") Long taskId);

    // Update task status with worker validation
    @Modifying
    @Transactional
    @Query("UPDATE TaskExecutionEntity t SET t.status = :status, t.completedAt = CASE WHEN :status IN ('SUCCESS', 'FAILED', 'CANCELLED', 'SKIPPED') THEN :now ELSE t.completedAt END " +
           "WHERE t.id = :taskId AND t.workerId = :workerId")
    int updateTaskStatus(@Param("taskId") Long taskId,
                        @Param("status") String status,
                        @Param("workerId") String workerId,
                        @Param("now") Instant now);

    default void updateTaskStatus(Long taskId, String status, String workerId) {
        updateTaskStatus(taskId, status, workerId, Instant.now());
    }

    // Find tasks ready for execution (dependencies satisfied)
    @Query(value = "SELECT t.* FROM task_executions t " +
                   "WHERE t.status = 'PENDING' " +
                   "AND t.worker_id IS NULL " +
                   "AND NOT EXISTS (" +
                   "  SELECT 1 FROM task_executions d " +
                   "  WHERE d.workflow_id = t.workflow_id " +
                   "  AND d.task_reference_name IN (" +
                   "    SELECT unnest(string_to_array(t.dependencies, ','))" +
                   "  ) " +
                   "  AND d.status NOT IN ('SUCCESS', 'SKIPPED')" +
                   ") " +
                   "ORDER BY t.workflow_id, t.sequence_number " +
                   "LIMIT :limit",
           nativeQuery = true)
    List<TaskExecutionEntity> findReadyTasks(@Param("limit") int limit);

    // Find unassigned tasks for rebalancing
    @Query("SELECT t FROM TaskExecutionEntity t WHERE t.status = 'PENDING' AND t.workerId IS NULL")
    List<TaskExecutionEntity> findUnassignedTasks();

    // Assign task to a specific node/worker
    @Modifying
    @Transactional
    @Query("UPDATE TaskExecutionEntity t SET t.workerId = :workerId WHERE t.id = :taskId AND t.workerId IS NULL")
    int assignTaskToNode(@Param("taskId") Long taskId, @Param("workerId") String workerId);

    // Find stale running tasks (for recovery)
    @Query("SELECT t FROM TaskExecutionEntity t WHERE t.status = 'RUNNING' AND t.startedAt < :threshold")
    List<TaskExecutionEntity> findStaleTasks(@Param("threshold") Instant threshold);

    // Reset stale tasks for retry
    @Modifying
    @Transactional
    @Query("UPDATE TaskExecutionEntity t SET t.status = 'PENDING', t.workerId = NULL, t.retryCount = t.retryCount + 1 " +
           "WHERE t.status = 'RUNNING' AND t.startedAt < :threshold AND t.retryCount < t.maxRetries")
    int resetStaleTasks(@Param("threshold") Instant threshold);

    // Update task with result data
    @Modifying
    @Transactional
    @Query("UPDATE TaskExecutionEntity t SET t.outputData = :outputData, t.status = :status, " +
           "t.completedAt = :now, t.executionTimeMs = :executionTime " +
           "WHERE t.id = :taskId")
    void updateTaskResult(@Param("taskId") Long taskId,
                         @Param("outputData") String outputData,
                         @Param("status") TaskStatus status,
                         @Param("executionTime") Long executionTime,
                         @Param("now") Instant now);

    // Update task error information
    @Modifying
    @Transactional
    @Query("UPDATE TaskExecutionEntity t SET t.errorMessage = :errorMessage, " +
           "t.errorType = :errorType, t.errorDetails = :errorDetails, " +
           "t.status = 'FAILED', t.completedAt = :now " +
           "WHERE t.id = :taskId")
    void updateTaskError(@Param("taskId") Long taskId,
                        @Param("errorMessage") String errorMessage,
                        @Param("errorType") String errorType,
                        @Param("errorDetails") String errorDetails,
                        @Param("now") Instant now);

    // Count tasks by status for a workflow
    @Query("SELECT COUNT(t) FROM TaskExecutionEntity t WHERE t.workflow.workflowId = :workflowId AND t.status = :status")
    long countByWorkflowAndStatus(@Param("workflowId") String workflowId, @Param("status") TaskStatus status);

    // Batch update tasks for workflow cancellation
    @Modifying
    @Transactional
    @Query("UPDATE TaskExecutionEntity t SET t.status = 'CANCELLED', t.completedAt = :now " +
           "WHERE t.workflow.workflowId = :workflowId AND t.status IN ('PENDING', 'RUNNING')")
    int cancelWorkflowTasks(@Param("workflowId") String workflowId, @Param("now") Instant now);

    default int cancelWorkflowTasks(String workflowId) {
        return cancelWorkflowTasks(workflowId, Instant.now());
    }
}
