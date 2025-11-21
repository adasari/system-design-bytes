package com.taskflow.builder;

import com.taskflow.core.*;
import com.taskflow.executor.WorkflowExecutor;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public class WorkflowBuilder {
    private String name;
    private int version = 1;
    private String description;
    private final List<TaskDefinition> tasks = new ArrayList<>();
    private final Map<String, Object> defaultInputs = new HashMap<>();
    private Duration timeout = Duration.ofHours(1);
    private int maxRetries = 3;
    private WorkflowExecutor executor;
    
    public WorkflowBuilder(String name) {
        this.name = name;
    }
    
    public static WorkflowBuilder create(String name) {
        return new WorkflowBuilder(name);
    }
    
    public WorkflowBuilder version(int version) {
        this.version = version;
        return this;
    }
    
    public WorkflowBuilder description(String description) {
        this.description = description;
        return this;
    }
    
    public WorkflowBuilder timeout(Duration timeout) {
        this.timeout = timeout;
        return this;
    }
    
    public WorkflowBuilder maxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
        return this;
    }
    
    public WorkflowBuilder executor(WorkflowExecutor executor) {
        this.executor = executor;
        return this;
    }
    
    public WorkflowBuilder defaultInput(String key, Object value) {
        this.defaultInputs.put(key, value);
        return this;
    }
    
    public TaskChainBuilder addTask(String taskName) {
        return new TaskChainBuilder(this, taskName);
    }
    
    public ParallelTaskBuilder parallel() {
        return new ParallelTaskBuilder(this);
    }
    
    public ConditionalTaskBuilder conditional(String condition) {
        return new ConditionalTaskBuilder(this, condition);
    }
    
    public LoopTaskBuilder loop(int count) {
        return new LoopTaskBuilder(this, count);
    }
    
    public SubWorkflowBuilder subWorkflow(WorkflowDefinition subWorkflow) {
        return new SubWorkflowBuilder(this, subWorkflow);
    }
    
    public WorkflowDefinition build() {
        return WorkflowDefinition.builder()
                .name(name)
                .version(version)
                .description(description)
                .tasks(tasks)
                .timeout(timeout)
                .maxRetries(maxRetries)
                .build();
    }
    
    public CompletableFuture<WorkflowResult> execute(Map<String, Object> inputs) {
        if (executor == null) {
            executor = new WorkflowExecutor();
        }
        
        Map<String, Object> mergedInputs = new HashMap<>(defaultInputs);
        if (inputs != null) {
            mergedInputs.putAll(inputs);
        }
        
        return executor.execute(build(), mergedInputs);
    }
    
    void addTaskDefinition(TaskDefinition task) {
        tasks.add(task);
    }
    
    public class TaskChainBuilder {
        private final WorkflowBuilder workflowBuilder;
        private final TaskDefinition.Builder taskBuilder;
        private String previousTaskName;
        
        TaskChainBuilder(WorkflowBuilder workflowBuilder, String taskName) {
            this.workflowBuilder = workflowBuilder;
            this.taskBuilder = TaskDefinition.builder().name(taskName);
        }
        
        public TaskChainBuilder withInput(String key, Object value) {
            taskBuilder.inputParameter(key, value);
            return this;
        }
        
        public TaskChainBuilder withOutput(String key, Object value) {
            taskBuilder.outputParameter(key, value);
            return this;
        }
        
        public TaskChainBuilder timeout(Duration timeout) {
            taskBuilder.timeout(timeout);
            return this;
        }
        
        public TaskChainBuilder retries(int count) {
            taskBuilder.retryCount(count);
            return this;
        }
        
        public TaskChainBuilder retryDelay(Duration delay) {
            taskBuilder.retryDelay(delay);
            return this;
        }
        
        public TaskChainBuilder retryLogic(TaskDefinition.RetryLogic logic) {
            taskBuilder.retryLogic(logic);
            return this;
        }
        
        public TaskChainBuilder optional() {
            taskBuilder.optional(true);
            return this;
        }
        
        public TaskChainBuilder dependsOn(String... taskNames) {
            for (String taskName : taskNames) {
                taskBuilder.addDependency(taskName);
            }
            return this;
        }
        
        public TaskChainBuilder then(String nextTaskName) {
            String currentTaskName = taskBuilder.build().getName();
            workflowBuilder.addTaskDefinition(taskBuilder.build());
            
            TaskChainBuilder nextBuilder = new TaskChainBuilder(workflowBuilder, nextTaskName);
            nextBuilder.taskBuilder.addDependency(currentTaskName);
            return nextBuilder;
        }
        
        public WorkflowBuilder end() {
            workflowBuilder.addTaskDefinition(taskBuilder.build());
            return workflowBuilder;
        }
    }
    
    public class ParallelTaskBuilder {
        private final WorkflowBuilder workflowBuilder;
        private final List<TaskDefinition> parallelTasks = new ArrayList<>();
        private final Set<String> dependencies = new HashSet<>();
        
        ParallelTaskBuilder(WorkflowBuilder workflowBuilder) {
            this.workflowBuilder = workflowBuilder;
        }
        
        public ParallelTaskBuilder task(String taskName) {
            TaskDefinition.Builder builder = TaskDefinition.builder()
                    .name(taskName)
                    .type(TaskDefinition.TaskType.SIMPLE);
            
            for (String dep : dependencies) {
                builder.addDependency(dep);
            }
            
            parallelTasks.add(builder.build());
            return this;
        }
        
        public ParallelTaskBuilder task(TaskDefinition task) {
            parallelTasks.add(task);
            return this;
        }
        
        public ParallelTaskBuilder dependsOn(String... taskNames) {
            dependencies.addAll(Arrays.asList(taskNames));
            return this;
        }
        
        public JoinBuilder join(String joinTaskName) {
            for (TaskDefinition task : parallelTasks) {
                workflowBuilder.addTaskDefinition(task);
            }
            
            List<String> parallelTaskNames = parallelTasks.stream()
                    .map(TaskDefinition::getName)
                    .collect(ArrayList::new, (list, name) -> list.add(name), ArrayList::addAll);
            
            return new JoinBuilder(workflowBuilder, joinTaskName, parallelTaskNames);
        }
        
        public WorkflowBuilder end() {
            for (TaskDefinition task : parallelTasks) {
                workflowBuilder.addTaskDefinition(task);
            }
            return workflowBuilder;
        }
    }
    
    public class JoinBuilder {
        private final WorkflowBuilder workflowBuilder;
        private final TaskDefinition.Builder joinTask;
        
        JoinBuilder(WorkflowBuilder workflowBuilder, String joinTaskName, List<String> dependencies) {
            this.workflowBuilder = workflowBuilder;
            this.joinTask = TaskDefinition.builder()
                    .name(joinTaskName)
                    .type(TaskDefinition.TaskType.JOIN);
            
            for (String dep : dependencies) {
                joinTask.addDependency(dep);
            }
        }
        
        public WorkflowBuilder end() {
            workflowBuilder.addTaskDefinition(joinTask.build());
            return workflowBuilder;
        }
    }
    
    public class ConditionalTaskBuilder {
        private final WorkflowBuilder workflowBuilder;
        private final String condition;
        private TaskDefinition ifTask;
        private TaskDefinition elseTask;
        
        ConditionalTaskBuilder(WorkflowBuilder workflowBuilder, String condition) {
            this.workflowBuilder = workflowBuilder;
            this.condition = condition;
        }
        
        public ConditionalTaskBuilder ifTrue(String taskName) {
            this.ifTask = TaskDefinition.builder()
                    .name(taskName)
                    .decisionCases(condition)
                    .type(TaskDefinition.TaskType.DECISION)
                    .build();
            return this;
        }
        
        public ConditionalTaskBuilder ifFalse(String taskName) {
            this.elseTask = TaskDefinition.builder()
                    .name(taskName)
                    .decisionCases("!" + condition)
                    .type(TaskDefinition.TaskType.DECISION)
                    .build();
            return this;
        }
        
        public WorkflowBuilder end() {
            if (ifTask != null) {
                workflowBuilder.addTaskDefinition(ifTask);
            }
            if (elseTask != null) {
                workflowBuilder.addTaskDefinition(elseTask);
            }
            return workflowBuilder;
        }
    }
    
    public class LoopTaskBuilder {
        private final WorkflowBuilder workflowBuilder;
        private final int loopCount;
        private final List<TaskDefinition> loopTasks = new ArrayList<>();
        
        LoopTaskBuilder(WorkflowBuilder workflowBuilder, int loopCount) {
            this.workflowBuilder = workflowBuilder;
            this.loopCount = loopCount;
        }
        
        public LoopTaskBuilder task(String taskName) {
            TaskDefinition task = TaskDefinition.builder()
                    .name(taskName)
                    .type(TaskDefinition.TaskType.DO_WHILE)
                    .loopCount(loopCount)
                    .build();
            loopTasks.add(task);
            return this;
        }
        
        public WorkflowBuilder end() {
            for (TaskDefinition task : loopTasks) {
                workflowBuilder.addTaskDefinition(task);
            }
            return workflowBuilder;
        }
    }
    
    public class SubWorkflowBuilder {
        private final WorkflowBuilder workflowBuilder;
        private final WorkflowDefinition subWorkflow;
        
        SubWorkflowBuilder(WorkflowBuilder workflowBuilder, WorkflowDefinition subWorkflow) {
            this.workflowBuilder = workflowBuilder;
            this.subWorkflow = subWorkflow;
        }
        
        public WorkflowBuilder end() {
            TaskDefinition subWorkflowTask = TaskDefinition.builder()
                    .name("SubWorkflow_" + subWorkflow.getName())
                    .type(TaskDefinition.TaskType.SUB_WORKFLOW)
                    .inputParameter("subWorkflowDefinition", subWorkflow)
                    .build();
            
            workflowBuilder.addTaskDefinition(subWorkflowTask);
            return workflowBuilder;
        }
    }
}