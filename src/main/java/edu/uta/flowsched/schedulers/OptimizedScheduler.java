package edu.uta.flowsched.schedulers;

import com.google.ortools.sat.*;
import edu.uta.flowsched.*;
import org.apache.commons.lang3.tuple.Triple;
import org.onosproject.net.Link;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static edu.uta.flowsched.Util.LOG_TIME_FORMATTER;

/**
 * OptimizedScheduler uses a CP-SAT model to minimize the max completion time across clients
 * by selecting optimal paths for weight distribution in synchronous FL.
 */
public class OptimizedScheduler extends SmartFlowScheduler {
    private static SmartFlowScheduler S2C_INSTANCE, C2S_INSTANCE;

    // Small constant for numerical stability when adjusting RTT
    private static final double EPSILON = 1e-2;
    // Scaling factor to convert floating-point time to integer for CP-SAT
    private static final long SCALE = 1_000_000L;

    // Track current assignments to optionally avoid unnecessary changes
    private final Map<FLHost, MyPath> currentAssignments;

    Set<FLHost> uniqueClients = new HashSet<>();

    public static SmartFlowScheduler getInstance(FlowDirection direction) {
        if (S2C_INSTANCE == null) {
            S2C_INSTANCE = new OptimizedScheduler(FlowDirection.S2C);
        }
        if (C2S_INSTANCE == null) {
            C2S_INSTANCE = new OptimizedScheduler(FlowDirection.C2S);
        }
        return direction == FlowDirection.S2C ? S2C_INSTANCE : C2S_INSTANCE;
    }

    private OptimizedScheduler(FlowDirection direction) {
        super(direction);
        OrToolsLoader.INSTANCE.loadNativeLibraries();
        this.currentAssignments = new ConcurrentHashMap<>();
    }

    /**
     * Precomputes adjusted RTT and per-link tau (scaled estimated time) values for all viable paths.
     *
     * @param clientPaths       Map from client to its set of candidate paths
     * @param modelLinks        Output set of all links used by any viable path
     * @param adjustedRttByPath Output map from path ID to adjusted RTT
     * @param tauMap            Output map from Triple(clientID, pathID, linkID) to tau
     * @param dataRemaining     Map from client to bits remaining
     */
    private void precomputePathProperties(
            Map<FLHost, Set<MyPath>> clientPaths,
            Set<MyLink> modelLinks,
            Map<String, Double> adjustedRttByPath,
            Map<Triple<String, String, String>, Long> tauMap,
            Map<FLHost, Long> dataRemaining) {

        for (Map.Entry<FLHost, Set<MyPath>> entry : clientPaths.entrySet()) {
            FLHost client = entry.getKey();
            long bitsRemaining = dataRemaining.getOrDefault(client, 0L);
            if (bitsRemaining == 0) {
                continue;
            }
            double dataMbit = Util.bitToMbit(bitsRemaining);

            for (MyPath path : entry.getValue()) {
                double pathRtt = path.getEffectiveRTT();
                double pathPLoss = path.getPacketLossProbability();
                double adjustedRtt = pathRtt * Math.sqrt(pathPLoss + EPSILON);

                boolean computationsOk = true;
                for (Link rawLink : path.linksNoEdge()) {
                    MyLink link = (MyLink) rawLink;
                    double capMbit = Util.bitToMbit(link.getEstimatedFreeCapacity());
                    if (capMbit <= EPSILON) {
                        computationsOk = false;
                        break;
                    }
                    long tau = (long) Math.ceil(dataMbit * adjustedRtt * SCALE / capMbit);
                    if (tau < 0) {
                        computationsOk = false;
                        break;
                    }
                    tauMap.put(Triple.of(client.getFlClientID(), path.id(), link.id()), tau);
                    modelLinks.add(link);
                }

                if (computationsOk) {
                    adjustedRttByPath.put(path.id(), adjustedRtt);
                }
            }
        }
    }

    /**
     * Builds and solves the CP-SAT model to select one path per client minimizing the max completion time.
     *
     * @param clientPaths       Map from client to its set of candidate paths
     * @param dataRemaining     Map from client to bits remaining
     * @param logger            Internal logger to collect messages
     * @param adjustedRttByPath Map from pathID to adjusted RTT
     * @param tauMap            Map from Triple(clientID,pathID,linkID) to tau
     * @param modelLinks        Set of all links used in the model
     * @return A map of clients to selected path, or null if infeasible
     */
    private Map<FLHost, MyPath> solveModel(
            Map<FLHost, Set<MyPath>> clientPaths,
            Map<FLHost, Long> dataRemaining,
            StringBuilder logger,
            Map<String, Double> adjustedRttByPath,
            Map<Triple<String, String, String>, Long> tauMap,
            Set<MyLink> modelLinks) {

        CpModel model = new CpModel();
        Map<FLHost, Map<MyPath, BoolVar>> xVars = new HashMap<>();

        // 1) Create decision variables x[c][p] only for viable paths
        for (FLHost client : clientPaths.keySet()) {
            long bits = dataRemaining.getOrDefault(client, 0L);
            if (bits == 0) {
                continue; // No data means no decision needed
            }
            Map<MyPath, BoolVar> pathVars = new HashMap<>();
            for (MyPath path : clientPaths.get(client)) {
                if (adjustedRttByPath.containsKey(path.id())) {
                    String varName = String.format("x_%s_%s", client.getFlClientID(), path.id());
                    pathVars.put(path, model.newBoolVar(varName));
                }
            }
            if (!pathVars.isEmpty()) {
                xVars.put(client, pathVars);
            } else {
                logger.append(String.format("\tWARNING: Client %s has data but no viable paths.\n", client.getFlClientID()));
            }
        }

        // 2) For each client, enforce exactly one path selected
        for (Map.Entry<FLHost, Map<MyPath, BoolVar>> entry : xVars.entrySet()) {
            Collection<BoolVar> vars = entry.getValue().values();
            if (!vars.isEmpty()) {
                // Convert Collection<BoolVar> to array for addExactlyOne
                BoolVar[] varArray = vars.toArray(new BoolVar[0]);
                model.addExactlyOne(varArray);
            }
        }

        if (xVars.isEmpty() && clientPaths.values().stream().anyMatch(set -> !set.isEmpty())) {
            logger.append("\tNo variables to optimize but clients have data.\n");
            return Collections.emptyMap();
        }

        // 3) ActiveFlows per link
        Map<MyLink, IntVar> activeFlows = new HashMap<>();
        for (MyLink link : modelLinks) {
            List<BoolVar> flowsOnLink = new ArrayList<>();
            for (Map.Entry<FLHost, Map<MyPath, BoolVar>> entry : xVars.entrySet()) {
                for (Map.Entry<MyPath, BoolVar> pv : entry.getValue().entrySet()) {
                    MyPath path = pv.getKey();
                    if (path.linksNoEdge().stream().anyMatch(l -> ((MyLink) l).id().equals(link.id()))) {
                        flowsOnLink.add(pv.getValue());
                    }
                }
            }
            IntVar af = model.newIntVar(0, xVars.size(), String.format("ActiveFlows_%s", link.id()));
            if (flowsOnLink.isEmpty()) {
                model.addEquality(af, 0);
            } else {
                BoolVar[] flowArray = flowsOnLink.toArray(new BoolVar[0]);
                model.addEquality(af, LinearExpr.sum(flowArray));
            }
            activeFlows.put(link, af);
        }

        // 4) Create T variable (max completion time)
        long maxTau = tauMap.values().stream().mapToLong(v -> v).max().orElse(0);
        long Tupper = (maxTau == 0 ? SCALE : maxTau) * (xVars.isEmpty() ? 1 : xVars.size());
        if (Tupper <= 0) {
            Tupper = SCALE;
        }
        IntVar T = model.newIntVar(0, Tupper, "T");

        // 5) For each client-path-link, enforce T >= tau * ActiveFlows on link if path chosen
        for (Map.Entry<FLHost, Map<MyPath, BoolVar>> clientEntry : xVars.entrySet()) {
            String clientId = clientEntry.getKey().getFlClientID();
            for (Map.Entry<MyPath, BoolVar> pathEntry : clientEntry.getValue().entrySet()) {
                String pathId = pathEntry.getKey().id();
                BoolVar xVar = pathEntry.getValue();
                for (Link rawLink : pathEntry.getKey().linksNoEdge()) {
                    MyLink link = (MyLink) rawLink;
                    Long tauVal = tauMap.get(Triple.of(clientId, pathId, link.id()));
                    if (tauVal == null) {
                        logger.append(String.format("\tWARNING: Missing tau for %s-%s-%s\n", clientId, pathId, link.id()));
                        continue;
                    }
                    if (tauVal == 0) {
                        continue;
                    }
                    IntVar af = activeFlows.get(link);
                    if (af == null) {
                        logger.append(String.format("\tERROR: No activeFlows var for link %s\n", link.id()));
                        continue;
                    }
                    IntVar prod = model.newIntVar(0, Tupper, String.format("prod_%s_%s_%s", clientId, pathId, link.id()));
                    model.addMultiplicationEquality(prod, af, model.newConstant(tauVal));
                    model.addGreaterOrEqual(T, prod).onlyEnforceIf(xVar);
                }
            }
        }

        // 6) Minimize T if there are variables
        if (!xVars.isEmpty()) {
            model.minimize(T);
        } else {
            logger.append("\tSkipping minimization since no decision vars.\n");
        }

        // 7) Solve with time and thread limits
        CpSolver solver = new CpSolver();
        solver.getParameters().setMaxTimeInSeconds(10);
        solver.getParameters().setNumWorkers(Math.max(1, Runtime.getRuntime().availableProcessors() / 2));

        long startTime = System.currentTimeMillis();
        CpSolverStatus status = solver.solve(model);
        long duration = System.currentTimeMillis() - startTime;
        logger.append(String.format("\tSolve time: %d ms, Status: %s\n", duration, status));

        if (status != CpSolverStatus.OPTIMAL && status != CpSolverStatus.FEASIBLE) {
            return null;
        }
        if (xVars.isEmpty()) {
            return Collections.emptyMap();
        }

        // 8) Extract assignments
        Map<FLHost, MyPath> solution = new HashMap<>();
        for (Map.Entry<FLHost, Map<MyPath, BoolVar>> clientEntry : xVars.entrySet()) {
            for (Map.Entry<MyPath, BoolVar> pv : clientEntry.getValue().entrySet()) {
                if (solver.booleanValue(pv.getValue())) {
                    solution.put(clientEntry.getKey(), pv.getKey());
                    break;
                }
            }
        }
        return solution;
    }

    /**
     * Public method to optimize paths, applies threshold-based path switches.
     */
    public Map<FLHost, MyPath> optimizePaths(
            Map<FLHost, Set<MyPath>> clientPaths,
            Map<FLHost, Long> dataRemaining,
            StringBuilder logger,
            Map<FLHost, MyPath> prevAssignments,
            double threshold) {

        Set<MyLink> modelLinks = new HashSet<>();
        Map<String, Double> adjustedRttByPath = new HashMap<>();
        Map<Triple<String, String, String>, Long> tauMap = new HashMap<>();

        precomputePathProperties(clientPaths, modelLinks, adjustedRttByPath, tauMap, dataRemaining);
        if (adjustedRttByPath.isEmpty() && dataRemaining.values().stream().anyMatch(v -> v > 0)) {
            logger.append("\tNo viable paths after precompute. Using previous assignments.\n");
            return new HashMap<>(prevAssignments);
        }

        Map<FLHost, MyPath> rawSolution = solveModel(
                clientPaths, dataRemaining, logger, adjustedRttByPath, tauMap, modelLinks);
        if (rawSolution == null) {
            logger.append("\tSolver failed. Using previous assignments.\n");
            return new HashMap<>(prevAssignments);
        }
        if (rawSolution.isEmpty() && dataRemaining.values().stream().anyMatch(v -> v > 0)) {
            logger.append("\tEmpty solver result but data remains. Using previous assignments.\n");
            return new HashMap<>(prevAssignments);
        }

        // Compute active flows under optimal solution
        Map<String, Integer> activeFlowsOpt = computeActiveFlows(rawSolution, modelLinks);
        Map<FLHost, MyPath> finalAssignments = new HashMap<>();

        for (FLHost client : clientPaths.keySet()) {
            long bits = dataRemaining.getOrDefault(client, 0L);
            if (bits == 0) {
                continue;
            }
            MyPath currentPath = prevAssignments.get(client);
            MyPath candidatePath = rawSolution.get(client);
            if (candidatePath == null) {
                // No new path, keep current if exists
                if (currentPath != null) {
                    finalAssignments.put(client, currentPath);
                    logger.append(String.format("\tClient %s: no candidate, keeping current path.\n", client.getFlClientCID()));
                } else {
                    logger.append(String.format("\tWARNING: Client %s has no path at all.\n", client.getFlClientCID()));
                }
                continue;
            }
            if (currentPath == null || candidatePath.id().equals(currentPath.id())) {
                finalAssignments.put(client, candidatePath);
                continue;
            }
            long perfNew = calculateCompletionTime(client, candidatePath, activeFlowsOpt, dataRemaining, adjustedRttByPath, tauMap);
            Map<FLHost, MyPath> stayContext = new HashMap<>(rawSolution);
            stayContext.put(client, currentPath);
            Map<String, Integer> activeFlowsStay = computeActiveFlows(stayContext, modelLinks);
            long perfCurrent = calculateCompletionTime(client, currentPath, activeFlowsStay, dataRemaining, adjustedRttByPath, tauMap);

            if (perfNew < perfCurrent * (1 - threshold)) {
                finalAssignments.put(client, candidatePath);
                logger.append(String.format("\tClient %s: switching to a new path (newPerf=%d, oldPerf=%d).\n", client.getFlClientCID(), perfNew, perfCurrent));
            } else {
                finalAssignments.put(client, currentPath);
                logger.append(String.format("\tClient %s: keeping current path (newPerf=%d, oldPerf=%d).\n", client.getFlClientCID(), perfNew, perfCurrent));
            }
        }
        return finalAssignments;
    }

    /**
     * Computes how many flows traverse each link given a map of assignments.
     */
    private Map<String, Integer> computeActiveFlows(
            Map<FLHost, MyPath> assignments,
            Set<MyLink> allLinks) {
        Map<String, Integer> counts = new HashMap<>();
        for (MyLink link : allLinks) {
            counts.put(link.id(), 0);
        }
        for (MyPath path : assignments.values()) {
            if (path == null) continue;
            for (Link rawLink : path.linksNoEdge()) {
                MyLink link = (MyLink) rawLink;
                counts.merge(link.id(), 1, Integer::sum);
            }
        }
        return counts;
    }

    /**
     * Calculates the (scaled) completion time for a given client-path under the provided active flows.
     */
    private long calculateCompletionTime(
            FLHost client,
            MyPath path,
            Map<String, Integer> activeFlows,
            Map<FLHost, Long> dataRemaining,
            Map<String, Double> adjustedRttByPath,
            Map<Triple<String, String, String>, Long> tauMap) {
        if (!adjustedRttByPath.containsKey(path.id())) {
            return Long.MAX_VALUE;
        }
        long bits = dataRemaining.getOrDefault(client, 0L);
        if (bits == 0 || path.linksNoEdge().isEmpty()) {
            return 0L;
        }
        long maxTime = 0L;
        String clientId = client.getFlClientID();
        for (Link rawLink : path.linksNoEdge()) {
            MyLink link = (MyLink) rawLink;
            Long tauVal = tauMap.get(Triple.of(clientId, path.id(), link.id()));
            if (tauVal == null) {
                return Long.MAX_VALUE;
            }
            int flows = activeFlows.getOrDefault(link.id(), 0);
            maxTime = Math.max(maxTime, tauVal * flows);
        }
        return maxTime;
    }

    @Override
    protected void phase1(ProcessingContext context, AtomicLong phase1Time) {
        StringBuilder internalLogger = new StringBuilder(String.format("\tPhase1 %s\n", LocalDateTime.now().format(LOG_TIME_FORMATTER)));
        long start = System.currentTimeMillis();
        Map<FLHost, Set<MyPath>> toProcess = new HashMap<>();

        FLHost client;
        while ((client = context.needPhase1Processing.poll()) != null) {
            Set<MyPath> paths = new HashSet<>(context.clientPaths.get(client));
            toProcess.put(client, paths);
        }


        Map<FLHost, MyPath> newAssignments = optimizePaths(toProcess, context.deltaDataExchanged, internalLogger, currentAssignments, 0.25);
        if (newAssignments.isEmpty()) {
            internalLogger.append("\tNo new assignments in this round.\n");
            Util.log("greedy" + direction, internalLogger.toString());
            return;
        }

        for (Map.Entry<FLHost, MyPath> entry : newAssignments.entrySet()) {
            StringBuilder clientLogger = new StringBuilder();
            MyPath currentPath = entry.getKey().getCurrentPath();
            if (!entry.getValue().equals(currentPath)) {
                entry.getKey().setLastPathChange(System.currentTimeMillis());
                clientLogger.append(String.format("\t- Client %s: \n", entry.getKey().getFlClientCID()));
                String currentPathFormat = Optional.ofNullable(currentPath).map(MyPath::format).orElse("No Path");
                String newPathFormat = entry.getValue().format();
                clientLogger.append(String.format("\t\tCurrent Path: %s\n", currentPathFormat));
                PathRulesInstaller.INSTANCE.installPathRules(entry.getKey(), entry.getValue(), false);
                Set<FLHost> affectedClients = entry.getKey().assignNewPath(entry.getValue());
                clientLogger.append(String.format("\t\tNew Path: %s\n", newPathFormat));
                // The client is among the affected clients from the addition
                updateTimeAndRate(context, affectedClients, internalLogger);
            }
            internalLogger.append(clientLogger);
        }
        phase1Time.addAndGet(System.currentTimeMillis() - start);
        Util.log("greedy" + this.direction, internalLogger.toString());
        currentAssignments.putAll(newAssignments);
    }


}
