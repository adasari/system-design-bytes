package com.taskflow.executor;

import com.taskflow.core.Task;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

public class TaskRegistry {
    private final Map<String, Task<?>> tasks;
    
    public TaskRegistry() {
        this.tasks = new ConcurrentHashMap<>();
    }
    
    public void register(String taskName, Task<?> task) {
        if (taskName == null || task == null) {
            throw new IllegalArgumentException("Task name and task cannot be null");
        }
        tasks.put(taskName, task);
    }
    
    public void unregister(String taskName) {
        tasks.remove(taskName);
    }
    
    public Task<?> getTask(String taskName) {
        return tasks.get(taskName);
    }
    
    public boolean hasTask(String taskName) {
        return tasks.containsKey(taskName);
    }
    
    public void clear() {
        tasks.clear();
    }
    
    public int size() {
        return tasks.size();
    }
}