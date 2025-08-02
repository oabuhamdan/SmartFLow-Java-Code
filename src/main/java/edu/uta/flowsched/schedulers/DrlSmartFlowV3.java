package edu.uta.flowsched.schedulers;

import edu.uta.flowsched.*;
import org.apache.commons.lang3.tuple.Pair;
import org.deeplearning4j.nn.api.Model;
import org.deeplearning4j.nn.api.OptimizationAlgorithm;
import org.deeplearning4j.nn.conf.ComputationGraphConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.layers.BatchNormalization;
import org.deeplearning4j.nn.conf.layers.DenseLayer;
import org.deeplearning4j.nn.conf.layers.OutputLayer;
import org.deeplearning4j.nn.graph.ComputationGraph;
import org.deeplearning4j.optimize.listeners.ScoreIterationListener;
import org.deeplearning4j.util.ModelSerializer;
import org.nd4j.linalg.activations.Activation;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.learning.config.Adam;
import org.nd4j.linalg.lossfunctions.LossFunctions;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static edu.uta.flowsched.Util.POISON_CLIENT;
import static edu.uta.flowsched.Util.formatMessage;
import static edu.uta.flowsched.schedulers.CONFIGS.DATA_SIZE;
import static edu.uta.flowsched.schedulers.CONFIGS.SWITCH_THRESHOLD;

public class DrlSmartFlowV3 extends SmartFlowScheduler {

    /* ---------------------- Static helpers & singleton plumb‑up ---------------------- */
    public static final int BATCH_SIZE = 32;
    public static final int TRAIN_INTERVAL_SEC = 10;
    public static final int REPLAY_CAPACITY = 4096;
    public static final int CHECKPOINT_EVERY_SEC = 100;
    public static final double LEARNING_RATE = 1e-3;
    public static final String DRL_MODEL_PATH = "/var/opt/onos/rl_model_%s.zip";
    private static SmartFlowScheduler S2C_INSTANCE, C2S_INSTANCE;
    private static final int NUM_INPUTS = 10;
    private static final double TOP_TIER_REWARD = 1, MIDDLE_TIER_REWARD = 0, BOTTOM_TIER_REWARD = -1, SWITCH_PENALTY = -0.5;
    private static final DynamicScalar REAL_TIME_REWARD_SCALAR = new DynamicScalar(-1, 1);
    /* -----------------------  Model & training infrastructure ----------------------- */
    private final ComputationGraph rlModel;
    private final ArrayBlockingQueue<Pair<INDArray, Double>> replay;
    private final ConcurrentMap<FLHost, Deque<INDArray>> clientFeats;

    public static SmartFlowScheduler getInstance(FlowDirection direction) {
        if (S2C_INSTANCE == null) S2C_INSTANCE = new DrlSmartFlowV3(FlowDirection.S2C);
        if (C2S_INSTANCE == null) C2S_INSTANCE = new DrlSmartFlowV3(FlowDirection.C2S);
        return direction == FlowDirection.S2C ? S2C_INSTANCE : C2S_INSTANCE;
    }


    private DrlSmartFlowV3(FlowDirection direction) {
        super(direction);
        Nd4jNativeLoader.load();
        rlModel = loadOrInitModel();
        replay = new ArrayBlockingQueue<>(REPLAY_CAPACITY);
        clientFeats = new ConcurrentHashMap<>();
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(this::trainOnReplay, TRAIN_INTERVAL_SEC, TRAIN_INTERVAL_SEC, TimeUnit.SECONDS);
    }

    private ComputationGraph loadOrInitModel() {
        File f = new File(String.format(DRL_MODEL_PATH, this.direction));
        try {
            if (f.exists()) {
                Util.log("smartflow" + this.direction, formatMessage(0, "Loading DRL model from %s", f));
                return ModelSerializer.restoreComputationGraph(f);
            }
            // build fresh model
            ComputationGraphConfiguration cfg = new NeuralNetConfiguration.Builder().seed(1234)
                    .optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT)
                    .updater(new Adam(LEARNING_RATE)).graphBuilder().addInputs("in")
                    .addLayer("BN", new BatchNormalization.Builder().nOut(NUM_INPUTS).build(), "in")
                    .addLayer("L1", new DenseLayer.Builder().nIn(NUM_INPUTS).nOut(128).activation(Activation.RELU).build(), "BN")
                    .addLayer("L2", new DenseLayer.Builder().nIn(128).nOut(64).activation(Activation.RELU).build(), "L1")
                    .addLayer("L3", new DenseLayer.Builder().nIn(64).nOut(32).activation(Activation.RELU).build(), "L2")
                    .addLayer("out", new OutputLayer.Builder(LossFunctions.LossFunction.MSE).activation(Activation.IDENTITY).nIn(32).nOut(1).build(), "L3")
                    .setOutputs("out").build();
            ComputationGraph m = new ComputationGraph(cfg);
            m.init();
            m.setListeners(new OnosScoreListener(1));
            Util.log("smartflow" + this.direction, formatMessage(0, "Created fresh DRL model (params=%s)", m.numParams()));
            return m;
        } catch (Exception e) {
            String trace = e.getMessage() + "; " + Arrays.toString(Arrays.stream(e.getStackTrace()).toArray());
            Util.log("smartflow" + this.direction, formatMessage(2, "ERROR: %s ", trace));
            throw new RuntimeException("Error Init RL Model");
        }
    }

    private static class OnosScoreListener extends ScoreIterationListener {
        private final int printFrequency;

        public OnosScoreListener(int frequency) {
            super(frequency);
            this.printFrequency = frequency;
        }

        @Override
        public void iterationDone(Model model, int iteration, int epoch) {
            if (iteration % printFrequency != 0) return;
            double score = model.score();
            Util.log("general", formatMessage(0, "**** iter=%s score=%.4f ****", iteration, score));
        }
    }
    /* ------------------------------  Phase‑1  (decision) ----------------------------- */

    @Override
    protected void phase1(ProcessingContext context, AtomicLong phase1Total) throws InterruptedException {
        StringBuilder phase1Logger = new StringBuilder(formatMessage(0, "Phase 1 (DRL‑online):"));
        FLHost client = context.needPhase1Processing.take();
        if (client.equals(POISON_CLIENT)) return;
        long tik = System.currentTimeMillis();
        int processed = 0;
        while (client != null) {
            StringBuilder clientLogger = new StringBuilder(formatMessage(1, "- Client %s:", client.getFlClientCID()));
            try {
                List<MyPath> paths = new ArrayList<>(context.clientPaths.get(client));

                Map<MyPath, Double> scores = new HashMap<>();
                MyPath currentPath = client.getCurrentPath();

                for (MyPath p : paths) {
                    INDArray feat = extractFeatures(client, p, p.equals(currentPath));
                    scores.put(p, rlModel.outputSingle(feat).getDouble(0));
                }

                MyPath bestPath = scores.entrySet().stream().max(Map.Entry.comparingByValue()).get().getKey();

                boolean switching = shouldSwitchPath(client, currentPath, bestPath, scores, clientLogger);
                if (switching) {
                    if (currentPath != null) {
                        INDArray previousPathFeatures = extractFeatures(client, currentPath, true);
                        offerExperience(previousPathFeatures, SWITCH_PENALTY); // Punishing it for Switching
                    }
                    client.setLastPathChange(System.currentTimeMillis());
                    PathRulesInstaller.INSTANCE.installPathRules(client, bestPath, false);
                    Set<FLHost> affected = client.assignNewPath(bestPath);
                    updateTimeAndRate(context, affected, phase1Logger);
                    clientLogger.append(formatMessage(2, "Switched ⇢ %s", bestPath.format()));
                }

                clientFeats.computeIfAbsent(client, k -> new ConcurrentLinkedDeque<>()).add(extractFeatures(client, client.getCurrentPath(), true));

                context.needPhase2Processing.add(client);
                phase1Logger.append(clientLogger);
                client = context.needPhase1Processing.poll();
                processed++;
            } catch (Exception e) {
                String trace = e.getMessage() + "; " + Arrays.toString(Arrays.stream(e.getStackTrace()).toArray());
                Util.log("smartflow" + this.direction, formatMessage(2, "ERROR: %s ", trace));
            }
        }
        phase1Total.addAndGet(System.currentTimeMillis() - tik);
        Util.log("smartflow" + direction, phase1Logger.toString());
        Util.log("smartflow" + direction, formatMessage(1, "Processed %s clients (DRL)", processed));
    }

    /* ------------------ SHOULD‑SWITCH  (added, was missing) ------------------------- */
    private boolean shouldSwitchPath(FLHost client, MyPath currentPath, MyPath bestPath, Map<MyPath, Double> scores, StringBuilder logger) {
        if (currentPath == null) {
            logger.append(formatMessage(2, "First-time assignment"));
            return true; // first‑time assignment
        }
        if (bestPath.equals(currentPath)) {
            logger.append(formatMessage(2, "Current path is already best"));
            return false;
        }

        // Enforce a cooldown period to prevent path flapping.
        if (Util.getAgeInSeconds(client.getLastPathChange()) <= Util.POLL_FREQ * 1.5) {
            logger.append(formatMessage(2, "Hold ➜ in switch cooldown period"));
            return false;
        }

        double currScore = scores.getOrDefault(currentPath, 0.0);
        double bestScore = scores.get(bestPath);

        // The switching threshold logic is retained for relative improvement check.
        double improvement = (bestScore - currScore) / Math.max(currScore, 1e-6);
        if (improvement >= SWITCH_THRESHOLD) logger.append(formatMessage(2, "Switch ➜ Δscore=%.3f", improvement));
        else logger.append(formatMessage(2, "Hold ➜ Δscore=%.3f (below TH)", improvement));
        return improvement >= SWITCH_THRESHOLD;
    }
    /* ------------------------------  Phase‑2  (stats)  ------------------------------ */

    @Override
    protected void extraClientPhase2ClientProcessing(ProcessingContext context, FLHost client, StringBuilder clientLogger) {
        INDArray feat = clientFeats.get(client).peekLast();
        double reward = computeRealTimeReward(client, clientLogger);
        offerExperience(feat, reward);
    }

    private double computeRealTimeReward(FLHost client, StringBuilder clientLogger) {
        double throughput = Util.bitToMbit(client.networkStats.getLastPositiveRate(direction));
        double reward = REAL_TIME_REWARD_SCALAR.scale(throughput);
        clientLogger.append(formatMessage(2, "Reward=%.4f", reward));
        return reward;
    }

    @Override
    protected void endOfRoundProcessing(ProcessingContext context) {
        computeEndOfRoundReward(context);
    }

    private void computeEndOfRoundReward(ProcessingContext context) {
        double index = 1;
        int totalClients = ClientInformationDatabase.INSTANCE.getTotalFLClients();
        StringBuilder rewardsLogger = new StringBuilder(formatMessage(1, "Rewards for Round %s", round.get()));
        for (FLHost client : context.completedClients) {
            double reward = index / totalClients <= 0.33 ? TOP_TIER_REWARD : index / totalClients <= 0.66 ? MIDDLE_TIER_REWARD : BOTTOM_TIER_REWARD;
            rewardsLogger.append(formatMessage(2, "- Client %s -> Reward: %.1f", client.getFlClientCID(), reward));
            clientFeats.get(client).forEach(feat -> offerExperience(feat, reward));
            index++;
        }
        Util.log("smartflow" + direction, rewardsLogger.toString());
    }


    /* ---------------------------  Feature & reward utils  --------------------------- */
    private INDArray extractFeatures(FLHost client, MyPath path, boolean isCurrent) {
        // Path Metrics
        double packetLoss = path.getPacketLossProbability();
        double rtt = path.getEffectiveRTT() / 100.0;
        double projFairShare = Util.bitToMbit(path.getProjectedFairShare()) / 100.0;
        double curFairShare = Util.bitToMbit(path.getCurrentFairShare()) / 100.0;
        double activeFLows = path.getCurrentActiveFlows() / ClientInformationDatabase.INSTANCE.getTotalFLClients();
        double freeCap = Util.bitToMbit(path.getBottleneckFreeCap()) / 100.0;

        // Client Metrics
        double lastPathChange = Util.getAgeInSeconds(client.getLastPathChange()) / (Util.POLL_FREQ * 3.0);
        double dataRemaining = DATA_SIZE - client.networkStats.getRoundExchangedData(this.direction) / (double) DATA_SIZE;
        double lastPosRate = Util.bitToMbit(client.networkStats.getLastPositiveRate(this.direction)) / 100.0;
        double isCurrVal = isCurrent ? 1 : 0;

        double[] feat = {packetLoss, rtt, projFairShare, curFairShare, activeFLows, freeCap, lastPathChange, dataRemaining, lastPosRate, isCurrVal};
        return Nd4j.create(feat).reshape(1, feat.length);
    }

    private void offerExperience(INDArray feat, double reward) {
        if (feat == null) return;
        if (!replay.offer(Pair.of(feat, reward))) {
            replay.poll();
            replay.offer(Pair.of(feat, reward));
        }
    }

    /* ------------------------------  Online trainer  -------------------------------- */

    private void trainOnReplay() {
        try {
            if (replay.size() < BATCH_SIZE || rlModel == null) return;
            List<Pair<INDArray, Double>> batch = new ArrayList<>(BATCH_SIZE);
            replay.drainTo(batch, BATCH_SIZE);
            if (batch.isEmpty()) return;
            INDArray input = Nd4j.vstack(batch.stream().map(Pair::getLeft).toArray(INDArray[]::new));
            INDArray labels = Nd4j.create(batch.size(), 1);
            for (int i = 0; i < batch.size(); i++) labels.putScalar(i, 0, batch.get(i).getRight());
            DataSet ds = new DataSet(input, labels);
            rlModel.fit(ds); // direct fit – no deprecated iterator
            maybeCheckpointModel();
        } catch (Exception e) {
            String trace = e.getMessage() + "; " + Arrays.toString(Arrays.stream(e.getStackTrace()).toArray());
            Util.log("general", formatMessage(2, "ERROR: %s ", trace));
        }
    }

    private void maybeCheckpointModel() {
        long now = System.currentTimeMillis();
        if (now % (CHECKPOINT_EVERY_SEC * 1000L) < TRAIN_INTERVAL_SEC * 1000L) {
            try {
                ModelSerializer.writeModel(rlModel, new File(String.format(DRL_MODEL_PATH, this.direction)), true);
            } catch (IOException ignored) {
                // It's okay to ignore, not a critical failure.
            }
        }
    }

    /* ------------------------------  Initial sort  ----------------------------------- */

    @Override
    public void initialSort(ProcessingContext context, List<FLHost> clients) {
        clients.sort(Comparator.comparingDouble(c -> {
            MyPath p = context.clientPaths.get(c).iterator().next();
            return rlModel.outputSingle(extractFeatures(c, p, false)).getDouble(0);
        }));
    }


    private static class DynamicScalar {
        private final double targetMin;
        private final double targetMax;
        private double dataMin = Double.POSITIVE_INFINITY;
        private double dataMax = Double.NEGATIVE_INFINITY;

        public DynamicScalar(double targetMin, double targetMax) {
            this.targetMin = targetMin;
            this.targetMax = targetMax;
        }

        private void add(double x) {
            if (x < dataMin) dataMin = x;
            if (x > dataMax) dataMax = x;
        }

        // Scale a value into the target range
        public double scale(double x) {
            add(x);
            if (dataMin == dataMax) return (targetMin + targetMax) / 2.0;  // Prevent divide by zero
            return targetMin + (x - dataMin) * (targetMax - targetMin) / (dataMax - dataMin);
        }
    }
}