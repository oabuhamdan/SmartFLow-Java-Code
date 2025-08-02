package edu.uta.flowsched.schedulers;

import edu.uta.flowsched.FLHost;
import edu.uta.flowsched.FlowDirection;
import edu.uta.flowsched.MyPath;
import edu.uta.flowsched.Util;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static edu.uta.flowsched.Util.LOG_TIME_FORMATTER;

public class PlainFreeCapacityScheduler extends SmartFlowScheduler {
    private static final SmartFlowScheduler S2C_INSTANCE = new PlainFreeCapacityScheduler(FlowDirection.S2C);
    private static final SmartFlowScheduler C2S_INSTANCE = new PlainFreeCapacityScheduler(FlowDirection.C2S);

    private PlainFreeCapacityScheduler(FlowDirection direction) {
        super(direction);
    }

    public static SmartFlowScheduler getInstance(FlowDirection direction) {
        return direction.equals(FlowDirection.S2C) ? S2C_INSTANCE : C2S_INSTANCE;
    }

    protected HashMap<MyPath, Double> scorePaths(List<MyPath> paths, boolean initial) {
        Function<MyPath, Number> pathScore = path -> path.getBottleneckFreeCap() / 1e6;
        HashMap<MyPath, Double> pathScores = new HashMap<>();
        paths.forEach(path -> pathScores.put(path, pathScore.apply(path).doubleValue()));
        return pathScores;
    }

    @Override
    protected void phase1(ProcessingContext context, AtomicLong phase1Total) {
        StringBuilder internalLogger = new StringBuilder(String.format("\tPhase 1 -------------%s------------- \n", LocalDateTime.now().format(LOG_TIME_FORMATTER)));
        long tik = System.currentTimeMillis();
        FLHost client;
        while ((client = context.needPhase1Processing.poll()) != null) {
            StringBuilder clientLogger = new StringBuilder();
            MyPath currentPath = client.getCurrentPath();
            if (currentPath == null) {
                clientLogger.append(String.format("\t- Client %s: \n", client.getFlClientCID()));
                HashMap<MyPath, Double> bestPaths = scorePaths(context.clientPaths.get(client), false);
                Map.Entry<MyPath, Double> bestPath = bestPaths.entrySet()
                        .stream()
                        .max(Comparator.comparingDouble(Map.Entry::getValue))
                        .orElse(null);
                if (bestPath != null) {
                    Set<FLHost> affectedClients = client.assignNewPath(bestPath.getKey());
                    String newPathFormat = bestPath.getKey().format();
                    clientLogger.append(String.format("\t\tNew Path: %s\n", newPathFormat));
                    updateTimeAndRate(context, affectedClients, internalLogger);
                }
            }
            internalLogger.append(clientLogger);
        }
        phase1Total.addAndGet(System.currentTimeMillis() - tik);
        Util.log("greedy" + this.direction, internalLogger.toString());
    }
}
