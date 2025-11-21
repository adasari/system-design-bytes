package com.taskflow.persistence.repository;

import com.taskflow.core.WorkflowState;
import com.taskflow.persistence.entity.WorkflowInstanceEntity;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
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
import java.util.Optional;

@Repository
public interface WorkflowInstanceRepository extends JpaRepository<WorkflowInstanceEntity, String> {

    Optional<WorkflowInstanceEntity> findByWorkflowId(String workflowId);

    List<WorkflowInstanceEntity> findByState(WorkflowState state);

    Page<WorkflowInstanceEntity> findByState(WorkflowState state, Pageable pageable);

    List<WorkflowInstanceEntity> findByCorrelationId(String correlationId);

    Page<WorkflowInstanceEntity> findByCorrelationId(String correlationId, Pageable pageable);

    List<WorkflowInstanceEntity> findByParentWorkflowId(String parentWorkflowId);

    @Query("SELECT w FROM WorkflowInstanceEntity w WHERE w.state IN :states")
    List<WorkflowInstanceEntity> findByStateIn(@Param("states") List<WorkflowState> states);

    @Query("SELECT w FROM WorkflowInstanceEntity w WHERE w.parentWorkflowId = :parentWorkflowId")
    List<WorkflowInstanceEntity> getSubWorkflows(String parentWorkflowId);

    // Claim next pending workflow with pessimistic locking
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query(value = "SELECT * FROM workflow_instances " +
                   "WHERE state IN ('NOT_STARTED', 'PENDING') " +
                   "AND (processor_id IS NULL OR processor_id = '') " +
                   "ORDER BY priority DESC, started_at ASC " +
                   "LIMIT 1 " +
                   "FOR UPDATE SKIP LOCKED",
           nativeQuery = true)
    WorkflowInstanceEntity claimNextPendingWorkflow();

    // Alternative claiming with optimistic locking
    @Modifying
    @Transactional
    @Query("UPDATE WorkflowInstanceEntity w SET w.processorId = :processorId, w.state = 'RUNNING', w.lastUpdated = :now " +
           "WHERE w.workflowInstanceId = :workflowInstanceId AND w.processorId IS NULL AND w.state IN ('NOT_STARTED', 'PENDING')")
    int tryClaimWorkflow(@Param("workflowInstanceId") String workflowInstanceId,
                        @Param("processorId") String processorId,
                        @Param("now") Instant now);

    // Release workflow claim
    @Modifying
    @Transactional
    @Query("UPDATE WorkflowInstanceEntity w SET w.processorId = NULL, w.state = 'PENDING' " +
           "WHERE w.workflowInstanceId = :workflowInstanceId")
    void releaseWorkflowClaim(@Param("workflowInstanceId") String workflowInstanceId);

    // Find stale workflows that haven't been updated recently
    @Query("SELECT w FROM WorkflowInstanceEntity w WHERE w.state = 'RUNNING' AND w.lastUpdated < :threshold")
    List<WorkflowInstanceEntity> findStaleWorkflows(@Param("threshold") Instant threshold);

    // Reset stale workflows for reprocessing
    @Modifying
    @Transactional
    @Query("UPDATE WorkflowInstanceEntity w SET w.processorId = NULL, w.state = 'PENDING', w.retryCount = w.retryCount + 1 " +
           "WHERE w.state = 'RUNNING' AND w.lastUpdated < :threshold AND w.retryCount < w.maxRetries")
    int resetStaleWorkflows(@Param("threshold") Instant threshold);

    // Mark workflow as failed
    @Modifying
    @Transactional
    @Query("UPDATE WorkflowInstanceEntity w SET w.state = 'FAILED', w.completedAt = :now, w.lastUpdated = :now " +
           "WHERE w.workflowInstanceId = :workflowInstanceId")
    void markWorkflowFailed(@Param("workflowInstanceId") String workflowInstanceId, @Param("now") Instant now);

    // Update workflow state
    @Modifying
    @Transactional
    @Query("UPDATE WorkflowInstanceEntity w SET w.state = :state, w.lastUpdated = :now " +
           "WHERE w.workflowInstanceId = :workflowInstanceId")
    void updateState(@Param("workflowInstanceId") String workflowInstanceId,
                    @Param("state") WorkflowState state,
                    @Param("now") Instant now);

    // Update workflow context data
    @Modifying
    @Transactional
    @Query("UPDATE WorkflowInstanceEntity w SET w.contextData = :contextData, w.lastUpdated = :now " +
           "WHERE w.workflowInstanceId = :workflowInstanceId")
    void updateContext(@Param("workflowInstanceId") String workflowInstanceId,
                      @Param("contextData") String contextData,
                      @Param("now") Instant now);

    // Update workflow output data
    @Modifying
    @Transactional
    @Query("UPDATE WorkflowInstanceEntity w SET w.outputData = :outputData, w.lastUpdated = :now " +
           "WHERE w.workflowInstanceId = :workflowInstanceId")
    void updateOutput(@Param("workflowInstanceId") String workflowInstanceId,
                     @Param("outputData") String outputData,
                     @Param("now") Instant now);

    // Metrics queries
    @Query(value = "SELECT COUNT(*) FROM workflow_instances WHERE state = :state", nativeQuery = true)
    long countByState(@Param("state") String state);

    @Query(value = "SELECT COUNT(*) FROM workflow_instances WHERE started_at >= :since", nativeQuery = true)
    long countStartedSince(@Param("since") Instant since);

    @Query(value = "SELECT COUNT(*) FROM workflow_instances WHERE completed_at >= :since AND state = 'COMPLETED'", nativeQuery = true)
    long countCompletedSince(@Param("since") Instant since);

    @Query(value = "SELECT COUNT(*) FROM workflow_instances WHERE completed_at >= :since AND state = 'FAILED'", nativeQuery = true)
    long countFailedSince(@Param("since") Instant since);

    @Query(value = "SELECT AVG(EXTRACT(EPOCH FROM (completed_at - started_at))) FROM workflow_instances " +
                   "WHERE completed_at IS NOT NULL AND started_at IS NOT NULL AND completed_at >= :since",
           nativeQuery = true)
    Double averageExecutionTimeSince(@Param("since") Instant since);

    // Queue-specific queries
    @Query("SELECT COUNT(w) FROM WorkflowInstanceEntity w WHERE w.state IN ('NOT_STARTED', 'PENDING')")
    long countPendingWorkflows();

    @Query("SELECT COUNT(w) FROM WorkflowInstanceEntity w WHERE w.state = 'RUNNING'")
    long countRunningWorkflows();

    // Get workflows by priority for queue management
    @Query("SELECT w FROM WorkflowInstanceEntity w WHERE w.state IN ('NOT_STARTED', 'PENDING') " +
           "ORDER BY w.priority DESC, w.startedAt ASC")
    List<WorkflowInstanceEntity> findPendingWorkflowsOrderedByPriority(Pageable pageable);

    // Conditional state transition with optimistic locking
    @Modifying
    @Transactional
    @Query("UPDATE WorkflowInstanceEntity w SET w.state = :newState, w.lastUpdated = :now " +
           "WHERE w.workflowInstanceId = :workflowInstanceId AND w.state = :expectedState")
    int transitionState(@Param("workflowInstanceId") String workflowInstanceId,
                       @Param("expectedState") WorkflowState expectedState,
                       @Param("newState") WorkflowState newState,
                       @Param("now") Instant now);


}
