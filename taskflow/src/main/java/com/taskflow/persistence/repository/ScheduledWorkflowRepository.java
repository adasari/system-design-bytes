package com.taskflow.persistence.repository;

import com.taskflow.persistence.entity.ScheduledWorkflowEntity;
import com.taskflow.persistence.entity.ScheduledWorkflowEntity.ScheduleStatus;
import com.taskflow.persistence.entity.ScheduledWorkflowEntity.ScheduleType;
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

/**
 * Repository for scheduled workflow persistence and queries.
 */
@Repository
public interface ScheduledWorkflowRepository extends JpaRepository<ScheduledWorkflowEntity, String> {
    
    /**
     * Find all active schedules that are due for execution
     */
    @Query("SELECT s FROM ScheduledWorkflowEntity s " +
           "WHERE s.status = 'ACTIVE' " +
           "AND s.nextExecutionTime <= :now " +
           "AND (s.startTime IS NULL OR s.startTime <= :now) " +
           "AND (s.endTime IS NULL OR s.endTime > :now) " +
           "ORDER BY s.nextExecutionTime")
    List<ScheduledWorkflowEntity> findDueSchedules(@Param("now") Instant now);
    
    /**
     * Find and lock a schedule for execution
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT s FROM ScheduledWorkflowEntity s " +
           "WHERE s.scheduleId = :scheduleId " +
           "AND s.status = 'ACTIVE'")
    Optional<ScheduledWorkflowEntity> findAndLockSchedule(@Param("scheduleId") String scheduleId);
    
    /**
     * Find schedules by workflow name
     */
    List<ScheduledWorkflowEntity> findByWorkflowName(String workflowName);
    
    /**
     * Find schedules by workflow name and version
     */
    List<ScheduledWorkflowEntity> findByWorkflowNameAndWorkflowVersion(
        String workflowName, Integer workflowVersion);
    
    /**
     * Find schedules by status
     */
    List<ScheduledWorkflowEntity> findByStatus(ScheduleStatus status);
    
    /**
     * Find schedules by type
     */
    List<ScheduledWorkflowEntity> findByScheduleType(ScheduleType type);
    
    /**
     * Find schedules created by user
     */
    List<ScheduledWorkflowEntity> findByCreatedBy(String createdBy);
    
    /**
     * Find active recurring schedules for recovery
     */
    @Query("SELECT s FROM ScheduledWorkflowEntity s " +
           "WHERE s.status = 'ACTIVE' " +
           "AND s.scheduleType IN ('FIXED_RATE', 'FIXED_DELAY', 'CRON')")
    List<ScheduledWorkflowEntity> findActiveRecurringSchedules();
    
    /**
     * Update next execution time
     */
    @Modifying
    @Transactional
    @Query("UPDATE ScheduledWorkflowEntity s " +
           "SET s.nextExecutionTime = :nextTime, " +
           "s.updatedAt = :now " +
           "WHERE s.scheduleId = :scheduleId")
    void updateNextExecutionTime(
        @Param("scheduleId") String scheduleId,
        @Param("nextTime") Instant nextTime,
        @Param("now") Instant now);
    
    /**
     * Update schedule status
     */
    @Modifying
    @Transactional
    @Query("UPDATE ScheduledWorkflowEntity s " +
           "SET s.status = :status, " +
           "s.updatedAt = :now " +
           "WHERE s.scheduleId = :scheduleId")
    void updateStatus(
        @Param("scheduleId") String scheduleId,
        @Param("status") ScheduleStatus status,
        @Param("now") Instant now);
    
    /**
     * Record successful execution
     */
    @Modifying
    @Transactional
    @Query("UPDATE ScheduledWorkflowEntity s " +
           "SET s.lastExecutionTime = :executionTime, " +
           "s.executionCount = s.executionCount + 1, " +
           "s.consecutiveFailures = 0, " +
           "s.lastWorkflowInstanceId = :workflowInstanceId, " +
           "s.lastWorkflowStatus = :workflowStatus, " +
           "s.updatedAt = :executionTime " +
           "WHERE s.scheduleId = :scheduleId")
    void recordSuccessfulExecution(
        @Param("scheduleId") String scheduleId,
        @Param("executionTime") Instant executionTime,
        @Param("workflowInstanceId") String workflowInstanceId,
        @Param("workflowStatus") String workflowStatus);
    
    /**
     * Record failed execution
     */
    @Modifying
    @Transactional
    @Query("UPDATE ScheduledWorkflowEntity s " +
           "SET s.lastExecutionTime = :executionTime, " +
           "s.executionCount = s.executionCount + 1, " +
           "s.consecutiveFailures = s.consecutiveFailures + 1, " +
           "s.lastWorkflowInstanceId = :workflowInstanceId, " +
           "s.lastWorkflowStatus = :workflowStatus, " +
           "s.updatedAt = :executionTime " +
           "WHERE s.scheduleId = :scheduleId")
    void recordFailedExecution(
        @Param("scheduleId") String scheduleId,
        @Param("executionTime") Instant executionTime,
        @Param("workflowInstanceId") String workflowInstanceId,
        @Param("workflowStatus") String workflowStatus);
    
    /**
     * Find schedules with errors (too many consecutive failures)
     */
    @Query("SELECT s FROM ScheduledWorkflowEntity s " +
           "WHERE s.status = 'ERROR' " +
           "OR (s.consecutiveFailures >= s.maxConsecutiveFailures)")
    List<ScheduledWorkflowEntity> findSchedulesWithErrors();
    
    /**
     * Find expired schedules
     */
    @Query("SELECT s FROM ScheduledWorkflowEntity s " +
           "WHERE s.status = 'ACTIVE' " +
           "AND s.endTime IS NOT NULL " +
           "AND s.endTime <= :now")
    List<ScheduledWorkflowEntity> findExpiredSchedules(@Param("now") Instant now);
    
    /**
     * Clean up old completed schedules
     */
    @Modifying
    @Transactional
    @Query("DELETE FROM ScheduledWorkflowEntity s " +
           "WHERE s.status IN ('COMPLETED', 'EXPIRED') " +
           "AND s.updatedAt < :before")
    int deleteOldCompletedSchedules(@Param("before") Instant before);
}