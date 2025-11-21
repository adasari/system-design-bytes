package com.taskflow.persistence.repository;

import com.taskflow.persistence.entity.WorkflowDefinitionEntity;
import com.taskflow.persistence.entity.WorkflowDefinitionEntity.DefinitionStatus;
import com.taskflow.persistence.entity.WorkflowDefinitionId;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Repository for workflow definition persistence and queries.
 * 
 * <p>Supports:</p>
 * <ul>
 *   <li>Version management and queries</li>
 *   <li>Latest version tracking</li>
 *   <li>Status-based filtering</li>
 *   <li>Workflow definition history</li>
 * </ul>
 */
@Repository
public interface WorkflowDefinitionRepository extends JpaRepository<WorkflowDefinitionEntity, WorkflowDefinitionId> {
    
    /**
     * Find the latest version of a workflow by workflow ID
     */
    @Query("SELECT w FROM WorkflowDefinitionEntity w " +
           "WHERE w.workflowId = :workflowId " +
           "AND w.isLatest = true")
    Optional<WorkflowDefinitionEntity> findLatestByWorkflowId(@Param("workflowId") String workflowId);
    
    /**
     * Find the latest version of a workflow by workflow name
     */
    @Query("SELECT w FROM WorkflowDefinitionEntity w " +
           "WHERE w.workflowName = :workflowName " +
           "AND w.isLatest = true")
    Optional<WorkflowDefinitionEntity> findLatestByWorkflowName(@Param("workflowName") String workflowName);
    
    /**
     * Find a specific version by workflow ID and version number
     */
    Optional<WorkflowDefinitionEntity> findByWorkflowIdAndVersion(String workflowId, Integer version);
    
    /**
     * Find a specific version by workflow name and version number
     */
    @Query("SELECT w FROM WorkflowDefinitionEntity w " +
           "WHERE w.workflowName = :workflowName " +
           "AND w.version = :version")
    Optional<WorkflowDefinitionEntity> findByWorkflowNameAndVersion(
        @Param("workflowName") String workflowName, 
        @Param("version") Integer version);
    
    /**
     * Find all versions of a workflow by workflow ID, ordered by version desc
     */
    @Query("SELECT w FROM WorkflowDefinitionEntity w " +
           "WHERE w.workflowId = :workflowId " +
           "ORDER BY w.version DESC")
    List<WorkflowDefinitionEntity> findAllVersionsByWorkflowId(@Param("workflowId") String workflowId);
    
    /**
     * Find all versions of a workflow by workflow name, ordered by version desc
     */
    @Query("SELECT w FROM WorkflowDefinitionEntity w " +
           "WHERE w.workflowName = :workflowName " +
           "ORDER BY w.version DESC")
    List<WorkflowDefinitionEntity> findAllVersionsByWorkflowName(@Param("workflowName") String workflowName);
    
    /**
     * Find all workflows with specific status
     */
    List<WorkflowDefinitionEntity> findByStatusOrderByWorkflowNameAscVersionDesc(DefinitionStatus status);
    
    /**
     * Find all latest workflow versions
     */
    @Query("SELECT w FROM WorkflowDefinitionEntity w " +
           "WHERE w.isLatest = true " +
           "ORDER BY w.workflowName ASC")
    List<WorkflowDefinitionEntity> findAllLatestVersions();
    
    /**
     * Find all active latest workflow versions  
     */
    @Query("SELECT w FROM WorkflowDefinitionEntity w " +
           "WHERE w.isLatest = true " +
           "AND w.status = 'ACTIVE' " +
           "ORDER BY w.workflowName ASC")
    List<WorkflowDefinitionEntity> findAllActiveLatestVersions();
    
    /**
     * Get the next version number for a workflow
     */
    @Query("SELECT COALESCE(MAX(w.version), 0) + 1 FROM WorkflowDefinitionEntity w " +
           "WHERE w.workflowId = :workflowId")
    Integer getNextVersionNumber(@Param("workflowId") String workflowId);
    
    /**
     * Mark all versions of a workflow as not latest (used when creating new version)
     */
    @Modifying
    @Transactional
    @Query("UPDATE WorkflowDefinitionEntity w " +
           "SET w.isLatest = false, w.updatedAt = :now " +
           "WHERE w.workflowId = :workflowId")
    void clearLatestFlags(@Param("workflowId") String workflowId, @Param("now") Instant now);
    
    /**
     * Mark a specific version as latest
     */
    @Modifying
    @Transactional
    @Query("UPDATE WorkflowDefinitionEntity w " +
           "SET w.isLatest = true, w.updatedAt = :now " +
           "WHERE w.workflowId = :workflowId " +
           "AND w.version = :version")
    void setAsLatest(@Param("workflowId") String workflowId, 
                     @Param("version") Integer version, 
                     @Param("now") Instant now);
    
    /**
     * Update workflow status
     */
    @Modifying
    @Transactional
    @Query("UPDATE WorkflowDefinitionEntity w " +
           "SET w.status = :status, " +
           "w.updatedAt = :now, " +
           "w.updatedBy = :updatedBy " +
           "WHERE w.workflowId = :workflowId " +
           "AND w.version = :version")
    void updateStatus(@Param("workflowId") String workflowId,
                     @Param("version") Integer version,
                     @Param("status") DefinitionStatus status,
                     @Param("updatedBy") String updatedBy,
                     @Param("now") Instant now);
    
    /**
     * Deprecate a workflow version
     */
    @Modifying
    @Transactional
    @Query("UPDATE WorkflowDefinitionEntity w " +
           "SET w.status = 'DEPRECATED', " +
           "w.deprecatedAt = :now, " +
           "w.deprecatedBy = :deprecatedBy, " +
           "w.deprecationReason = :reason, " +
           "w.isLatest = false, " +
           "w.updatedAt = :now " +
           "WHERE w.workflowId = :workflowId " +
           "AND w.version = :version")
    void deprecateVersion(@Param("workflowId") String workflowId,
                         @Param("version") Integer version,
                         @Param("deprecatedBy") String deprecatedBy,
                         @Param("reason") String reason,
                         @Param("now") Instant now);
    
    /**
     * Find workflows by tags (contains search)
     */
    @Query("SELECT w FROM WorkflowDefinitionEntity w " +
           "WHERE w.isLatest = true " +
           "AND w.tags LIKE %:tag% " +
           "ORDER BY w.workflowName ASC")
    List<WorkflowDefinitionEntity> findByTag(@Param("tag") String tag);
    
    /**
     * Check if a workflow name exists
     */
    boolean existsByWorkflowName(String workflowName);
    
    /**
     * Check if a specific workflow ID exists
     */
    boolean existsByWorkflowId(String workflowId);
    
    /**
     * Count active workflow definitions
     */
    @Query("SELECT COUNT(w) FROM WorkflowDefinitionEntity w " +
           "WHERE w.isLatest = true AND w.status = 'ACTIVE'")
    long countActiveWorkflows();
    
    /**
     * Find workflows created by a specific user
     */
    @Query("SELECT w FROM WorkflowDefinitionEntity w " +
           "WHERE w.createdBy = :createdBy " +
           "ORDER BY w.createdAt DESC")
    List<WorkflowDefinitionEntity> findByCreatedBy(@Param("createdBy") String createdBy);
}