package com.galafis.workflow;

import java.util.*;
import java.util.concurrent.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.function.Consumer;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * Workflow Engine - Enterprise Business Process Management
 * Provides state machine-based workflow execution with support for
 * parallel execution, conditional transitions, and event-driven processing.
 *
 * @author Gabriel Demetrios Lafis
 * @version 2.0.0
 */
public class WorkflowEngine {

    private static final Logger LOGGER = Logger.getLogger(WorkflowEngine.class.getName());
    private final Map<String, WorkflowDefinition> definitions;
    private final Map<String, WorkflowInstance> instances;
    private final ExecutorService executorService;
    private final List<WorkflowListener> listeners;

    public WorkflowEngine() {
        this.definitions = new ConcurrentHashMap<>();
        this.instances = new ConcurrentHashMap<>();
        this.executorService = Executors.newFixedThreadPool(
                Runtime.getRuntime().availableProcessors() * 2);
        this.listeners = new CopyOnWriteArrayList<>();
    }

    // ---- Workflow Definition ----

    public static class WorkflowDefinition {
        private final String id;
        private final String name;
        private final String version;
        private final Map<String, State> states;
        private final String initialState;
        private final List<Transition> transitions;

        public WorkflowDefinition(String id, String name, String version,
                                  String initialState) {
            this.id = id;
            this.name = name;
            this.version = version;
            this.initialState = initialState;
            this.states = new LinkedHashMap<>();
            this.transitions = new ArrayList<>();
        }

        public String getId() { return id; }
        public String getName() { return name; }
        public String getVersion() { return version; }
        public String getInitialState() { return initialState; }
        public Map<String, State> getStates() { return Collections.unmodifiableMap(states); }
        public List<Transition> getTransitions() { return Collections.unmodifiableList(transitions); }

        public WorkflowDefinition addState(State state) {
            states.put(state.getName(), state);
            return this;
        }

        public WorkflowDefinition addTransition(Transition transition) {
            transitions.add(transition);
            return this;
        }
    }

    // ---- State ----

    public static class State {
        public enum Type { START, TASK, DECISION, PARALLEL_GATEWAY, END }

        private final String name;
        private final Type type;
        private final Consumer<WorkflowContext> action;

        public State(String name, Type type, Consumer<WorkflowContext> action) {
            this.name = name;
            this.type = type;
            this.action = action;
        }

        public String getName() { return name; }
        public Type getType() { return type; }

        public void execute(WorkflowContext context) {
            if (action != null) {
                action.accept(context);
            }
        }
    }

    // ---- Transition ----

    public static class Transition {
        private final String fromState;
        private final String toState;
        private final String event;
        private final TransitionCondition condition;

        public Transition(String fromState, String toState, String event,
                          TransitionCondition condition) {
            this.fromState = fromState;
            this.toState = toState;
            this.event = event;
            this.condition = condition;
        }

        public String getFromState() { return fromState; }
        public String getToState() { return toState; }
        public String getEvent() { return event; }

        public boolean evaluate(WorkflowContext context) {
            return condition == null || condition.evaluate(context);
        }
    }

    @FunctionalInterface
    public interface TransitionCondition {
        boolean evaluate(WorkflowContext context);
    }

    // ---- Workflow Context ----

    public static class WorkflowContext {
        private final Map<String, Object> variables;
        private final List<String> auditLog;

        public WorkflowContext() {
            this.variables = new ConcurrentHashMap<>();
            this.auditLog = new CopyOnWriteArrayList<>();
        }

        public void setVariable(String key, Object value) {
            variables.put(key, value);
            auditLog.add(timestamp() + " SET " + key + " = " + value);
        }

        public Object getVariable(String key) {
            return variables.get(key);
        }

        @SuppressWarnings("unchecked")
        public <T> T getVariable(String key, Class<T> type) {
            Object val = variables.get(key);
            if (val != null && type.isInstance(val)) {
                return (T) val;
            }
            return null;
        }

        public Map<String, Object> getAllVariables() {
            return Collections.unmodifiableMap(variables);
        }

        public List<String> getAuditLog() {
            return Collections.unmodifiableList(auditLog);
        }

        void addLogEntry(String entry) {
            auditLog.add(timestamp() + " " + entry);
        }

        private String timestamp() {
            return LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        }
    }

    // ---- Workflow Instance ----

    public static class WorkflowInstance {
        public enum Status { CREATED, RUNNING, PAUSED, COMPLETED, FAILED, CANCELLED }

        private final String instanceId;
        private final WorkflowDefinition definition;
        private final WorkflowContext context;
        private String currentState;
        private Status status;
        private final LocalDateTime createdAt;
        private LocalDateTime completedAt;
        private final List<String> stateHistory;

        public WorkflowInstance(String instanceId, WorkflowDefinition definition) {
            this.instanceId = instanceId;
            this.definition = definition;
            this.context = new WorkflowContext();
            this.currentState = definition.getInitialState();
            this.status = Status.CREATED;
            this.createdAt = LocalDateTime.now();
            this.stateHistory = new ArrayList<>();
            stateHistory.add(currentState);
        }

        public String getInstanceId() { return instanceId; }
        public String getCurrentState() { return currentState; }
        public Status getStatus() { return status; }
        public WorkflowContext getContext() { return context; }
        public LocalDateTime getCreatedAt() { return createdAt; }
        public LocalDateTime getCompletedAt() { return completedAt; }
        public List<String> getStateHistory() { return Collections.unmodifiableList(stateHistory); }

        void transitionTo(String newState) {
            String oldState = this.currentState;
            this.currentState = newState;
            this.stateHistory.add(newState);
            context.addLogEntry("TRANSITION " + oldState + " -> " + newState);
        }

        void setStatus(Status status) {
            this.status = status;
            if (status == Status.COMPLETED || status == Status.FAILED) {
                this.completedAt = LocalDateTime.now();
            }
        }
    }

    // ---- Listener Interface ----

    public interface WorkflowListener {
        default void onStateChanged(WorkflowInstance instance, String from, String to) {}
        default void onWorkflowCompleted(WorkflowInstance instance) {}
        default void onWorkflowFailed(WorkflowInstance instance, Exception error) {}
    }

    // ---- Engine Methods ----

    public void registerDefinition(WorkflowDefinition definition) {
        definitions.put(definition.getId(), definition);
        LOGGER.info("Registered workflow definition: " + definition.getName()
                + " v" + definition.getVersion());
    }

    public WorkflowInstance createInstance(String definitionId) {
        WorkflowDefinition def = definitions.get(definitionId);
        if (def == null) {
            throw new IllegalArgumentException("Unknown workflow: " + definitionId);
        }
        String instanceId = "WF-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        WorkflowInstance instance = new WorkflowInstance(instanceId, def);
        instances.put(instanceId, instance);
        LOGGER.info("Created workflow instance: " + instanceId);
        return instance;
    }

    public CompletableFuture<WorkflowInstance> executeAsync(String instanceId) {
        return CompletableFuture.supplyAsync(() -> {
            WorkflowInstance instance = instances.get(instanceId);
            if (instance == null) {
                throw new IllegalArgumentException("Unknown instance: " + instanceId);
            }
            execute(instance);
            return instance;
        }, executorService);
    }

    public void execute(WorkflowInstance instance) {
        instance.setStatus(WorkflowInstance.Status.RUNNING);
        LOGGER.info("Executing workflow: " + instance.getInstanceId());

        try {
            while (instance.getStatus() == WorkflowInstance.Status.RUNNING) {
                State currentState = instance.definition.getStates().get(instance.getCurrentState());
                if (currentState == null) {
                    throw new IllegalStateException("State not found: " + instance.getCurrentState());
                }

                // Execute state action
                currentState.execute(instance.getContext());

                // Check for end state
                if (currentState.getType() == State.Type.END) {
                    instance.setStatus(WorkflowInstance.Status.COMPLETED);
                    listeners.forEach(l -> l.onWorkflowCompleted(instance));
                    LOGGER.info("Workflow completed: " + instance.getInstanceId());
                    break;
                }

                // Find valid transition
                String oldState = instance.getCurrentState();
                boolean transitioned = false;

                for (Transition t : instance.definition.getTransitions()) {
                    if (t.getFromState().equals(oldState) && t.evaluate(instance.getContext())) {
                        instance.transitionTo(t.getToState());
                        final String from = oldState;
                        final String to = t.getToState();
                        listeners.forEach(l -> l.onStateChanged(instance, from, to));
                        transitioned = true;
                        break;
                    }
                }

                if (!transitioned) {
                    throw new IllegalStateException(
                            "No valid transition from state: " + oldState);
                }
            }
        } catch (Exception e) {
            instance.setStatus(WorkflowInstance.Status.FAILED);
            instance.getContext().addLogEntry("FAILED: " + e.getMessage());
            listeners.forEach(l -> l.onWorkflowFailed(instance, e));
            LOGGER.log(Level.SEVERE, "Workflow failed: " + instance.getInstanceId(), e);
        }
    }

    public void sendEvent(String instanceId, String event, Map<String, Object> data) {
        WorkflowInstance instance = instances.get(instanceId);
        if (instance == null) {
            throw new IllegalArgumentException("Unknown instance: " + instanceId);
        }
        if (data != null) {
            data.forEach(instance.getContext()::setVariable);
        }
        instance.getContext().addLogEntry("EVENT received: " + event);
    }

    public void addListener(WorkflowListener listener) {
        listeners.add(listener);
    }

    public WorkflowInstance getInstance(String instanceId) {
        return instances.get(instanceId);
    }

    public Map<String, Object> getEngineStats() {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("totalDefinitions", definitions.size());
        stats.put("totalInstances", instances.size());
        stats.put("runningInstances", instances.values().stream()
                .filter(i -> i.getStatus() == WorkflowInstance.Status.RUNNING).count());
        stats.put("completedInstances", instances.values().stream()
                .filter(i -> i.getStatus() == WorkflowInstance.Status.COMPLETED).count());
        stats.put("failedInstances", instances.values().stream()
                .filter(i -> i.getStatus() == WorkflowInstance.Status.FAILED).count());
        return stats;
    }

    public void shutdown() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    // ---- Demo ----

    public static void main(String[] args) {
        System.out.println("=== Java Workflow Engine ===");
        System.out.println("Enterprise Business Process Management\n");

        WorkflowEngine engine = new WorkflowEngine();

        // Build an order-processing workflow definition
        WorkflowDefinition orderWorkflow = new WorkflowDefinition(
                "order-processing", "Order Processing", "1.0", "received");

        orderWorkflow
            .addState(new State("received", State.Type.START,
                ctx -> ctx.setVariable("step", "Order received")))
            .addState(new State("validate", State.Type.TASK,
                ctx -> {
                    double amount = ctx.getVariable("amount") != null
                            ? (Double) ctx.getVariable("amount") : 100.0;
                    ctx.setVariable("valid", amount > 0 && amount < 100000);
                    ctx.setVariable("step", "Validation complete");
                }))
            .addState(new State("check_inventory", State.Type.TASK,
                ctx -> {
                    ctx.setVariable("inStock", true);
                    ctx.setVariable("step", "Inventory checked");
                }))
            .addState(new State("process_payment", State.Type.TASK,
                ctx -> ctx.setVariable("step", "Payment processed")))
            .addState(new State("ship", State.Type.TASK,
                ctx -> ctx.setVariable("step", "Order shipped")))
            .addState(new State("completed", State.Type.END,
                ctx -> ctx.setVariable("step", "Order completed")))
            .addState(new State("rejected", State.Type.END,
                ctx -> ctx.setVariable("step", "Order rejected")));

        orderWorkflow
            .addTransition(new Transition("received", "validate", "start", null))
            .addTransition(new Transition("validate", "check_inventory", "validated",
                ctx -> Boolean.TRUE.equals(ctx.getVariable("valid"))))
            .addTransition(new Transition("validate", "rejected", "invalid",
                ctx -> !Boolean.TRUE.equals(ctx.getVariable("valid"))))
            .addTransition(new Transition("check_inventory", "process_payment", "in_stock",
                ctx -> Boolean.TRUE.equals(ctx.getVariable("inStock"))))
            .addTransition(new Transition("process_payment", "ship", "paid", null))
            .addTransition(new Transition("ship", "completed", "shipped", null));

        engine.registerDefinition(orderWorkflow);

        // Add listener
        engine.addListener(new WorkflowListener() {
            @Override
            public void onStateChanged(WorkflowInstance inst, String from, String to) {
                System.out.println("  Transition: " + from + " -> " + to);
            }

            @Override
            public void onWorkflowCompleted(WorkflowInstance inst) {
                System.out.println("  Workflow COMPLETED: " + inst.getInstanceId());
            }
        });

        // Create and execute instance
        WorkflowInstance instance = engine.createInstance("order-processing");
        instance.getContext().setVariable("amount", 250.00);
        instance.getContext().setVariable("customerId", "CUST-001");

        engine.execute(instance);

        // Print results
        System.out.println("\nFinal State : " + instance.getCurrentState());
        System.out.println("Status      : " + instance.getStatus());
        System.out.println("History     : " + instance.getStateHistory());
        System.out.println("\nAudit Log:");
        instance.getContext().getAuditLog().forEach(e -> System.out.println("  " + e));
        System.out.println("\nEngine Stats: " + engine.getEngineStats());

        engine.shutdown();
    }
}
