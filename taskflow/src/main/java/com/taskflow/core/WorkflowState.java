package com.taskflow.core;

public enum WorkflowState {
    NOT_STARTED,
    RUNNING,
    PAUSED,
    COMPLETED,
    FAILED,
    CANCELLED,
    TIMED_OUT,
    TERMINATED
}