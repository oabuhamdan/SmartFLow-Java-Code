package edu.uta.flowsched.schedulers;

import edu.uta.flowsched.*;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static edu.uta.flowsched.Util.POISON_CLIENT;
import static edu.uta.flowsched.Util.formatMessage;
import static edu.uta.flowsched.schedulers.CONFIGS.SWITCH_THRESHOLD;

public class GreedySmartFlow extends SmartFlowScheduler {
    private static SmartFlowScheduler S2C_INSTANCE, C2S_INSTANCE;

    public static SmartFlowScheduler getInstance(FlowDirection direction) {
        if (S2C_INSTANCE == null) {
            S2C_INSTANCE = new GreedySmartFlow(FlowDirection.S2C);
        }
        if (C2S_INSTANCE == null) {
            C2S_INSTANCE = new GreedySmartFlow(FlowDirection.C2S);
        }
        return direction == FlowDirection.S2C ? S2C_INSTANCE : C2S_INSTANCE;
    }

    private GreedySmartFlow(FlowDirection direction) {
        super(direction);
    }

    @Override
    protected void phase1(ProcessingContext context, AtomicLong phase1Total) throws InterruptedException {
        StringBuilder phase1Logger = new StringBuilder(formatMessage(0, "Phase 1 at %s:", LocalDateTime.now().format(Util.LOG_TIME_FORMATTER)));
        FLHost client = context.needPhase1Processing.take(); // Will wait until there is a client available
        if (client.equals(POISON_CLIENT)) return;
        long tic = System.currentTimeMillis();
        while (client != null) { // Will loop over all the available clients
            StringBuilder clientLogger = new StringBuilder(formatMessage(1, "- Client %s:", client.getFlClientCID()));
            try {
                Set<MyPath> paths = new HashSet<>(context.clientPaths.get(client));
                MyPath currentPath = client.getCurrentPath();

                if (currentPath != null) {// Simulate Path
                    SimMyPath simPath = new SimMyPath(currentPath);
                    paths.remove(currentPath); // replace old with sim
                    paths.add(simPath);
                }

                GreedyScoreCompute scorer = new GreedyScoreCompute(paths);
                if (shouldSwitchPath(currentPath, scorer, clientLogger)) {
                    String currentPathFormat = Optional.ofNullable(currentPath).map(MyPath::format).orElse("No Path");
                    clientLogger.append(formatMessage(2, "- Current Path: %s", currentPathFormat));
                    PathRulesInstaller.INSTANCE.installPathRules(client, scorer.highestScorePath, false);
                    Set<FLHost> affectedClients = client.assignNewPath(scorer.highestScorePath);
                    clientLogger.append(formatMessage(2, "- New Path: %s", scorer.highestScorePath.format()));
                    updateTimeAndRate(context, affectedClients, phase1Logger);
                }
                context.needPhase2Processing.add(client);
                phase1Logger.append(clientLogger);
                client = context.needPhase1Processing.poll();
            } catch (Exception e) {
                String trace = e.getMessage() + "; " + Arrays.toString(Arrays.stream(e.getStackTrace()).toArray());
                clientLogger.append(formatMessage(2, "ERROR: %s ", trace));
            }
        }

        phase1Total.addAndGet(System.currentTimeMillis() - tic);
        Util.log("smartflow" + this.direction, phase1Logger.toString());
    }

    private boolean shouldSwitchPath(MyPath currentPath, GreedyScoreCompute scorer, StringBuilder clientLogger) {
        if (currentPath == null) return true;
        if (scorer.getHighestScoredPath().equals(currentPath)) {
            clientLogger.append(formatMessage(2, "Current Path is Best Path, Returning..."));
            return false;
        }
        double currentScore = scorer.getScore(currentPath);
        double bestScore = scorer.getScore(scorer.getHighestScoredPath());
        boolean switching = (bestScore - currentScore) / currentScore >= SWITCH_THRESHOLD;
        if (switching)
            clientLogger.append(formatMessage(2, "Switching - New Score: (%.2f), Current Score: (%.2f)", bestScore, currentScore));
        else
            clientLogger.append(formatMessage(2, "No Switching - Current Score (%.2f), New Score (%.2f)", bestScore, currentScore));
        return switching;
    }

    @Override
    public void initialSort(ProcessingContext context, List<FLHost> clients) {
        clients.sort(Comparator.comparing(c -> {
            try {
                GreedyScoreCompute scorer = new GreedyScoreCompute(context.clientPaths.get(c));
                return scorer.getScore(scorer.getHighestScoredPath());
            } catch (Exception e) {
                String trace = e.getMessage() + "; " + Arrays.toString(Arrays.stream(e.getStackTrace()).toArray());
                Util.log("general", formatMessage(2, "ERROR: %s ", trace));
            }
            return 0.0;
        }));
    }

    public static class GreedyScoreCompute {
        private Map<MyPath, Double> pathScores;
        private MyPath highestScorePath;

        public GreedyScoreCompute(Collection<MyPath> paths) {
            this.pathScores = new HashMap<>();
            double minEffectiveScore = Double.MAX_VALUE;
            double maxEffectiveScore = Double.MIN_VALUE;
            Map<MyPath, Double> rawScores = new HashMap<>();
            this.highestScorePath = null;
            try {
                for (MyPath path : paths) {
                    double score = path.effectiveScore();
                    rawScores.put(path, score);
                    minEffectiveScore = Math.min(minEffectiveScore, score);
                    maxEffectiveScore = Math.max(maxEffectiveScore, score);
                }
                for (Map.Entry<MyPath, Double> entry : rawScores.entrySet()) {
                    double normalized = normalize(entry.getValue(), minEffectiveScore, maxEffectiveScore);
                    pathScores.put(entry.getKey(), normalized);
                }
                this.highestScorePath = pathScores.entrySet().stream()
                        .max(Map.Entry.comparingByValue())
                        .orElseThrow(() -> new IllegalStateException("No paths available"))
                        .getKey();
            } catch (Exception e) {
                String trace = e.getMessage() + "; " + Arrays.toString(Arrays.stream(e.getStackTrace()).toArray());
                Util.log("general", formatMessage(2, "ERROR: %s ", trace));
            }
        }

        public double getScore(MyPath path) {
            return pathScores.getOrDefault(path, 0.0);
        }

        public MyPath getHighestScoredPath() {
            return this.highestScorePath;
        }

        private double normalize(double rawScore, double minVal, double maxVal) {
            if (Math.abs(maxVal - minVal) < 1e-10)
                return 0.5;
            return (rawScore - minVal) / (maxVal - minVal);
        }
    }
}
