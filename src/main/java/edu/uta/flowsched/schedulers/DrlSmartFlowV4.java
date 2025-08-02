package edu.uta.flowsched.schedulers;

import edu.uta.flowsched.*;
import org.deeplearning4j.nn.api.Model;
import org.deeplearning4j.nn.conf.ComputationGraphConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.inputs.InputType;
import org.deeplearning4j.nn.conf.layers.DenseLayer;
import org.deeplearning4j.nn.conf.layers.OutputLayer;
import org.deeplearning4j.nn.graph.ComputationGraph;
import org.deeplearning4j.optimize.listeners.ScoreIterationListener;
import org.deeplearning4j.rl4j.network.ac.ActorCriticLoss;
import org.nd4j.linalg.activations.Activation;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.MultiDataSet;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.indexing.NDArrayIndex;
import org.nd4j.linalg.learning.config.Adam;
import org.nd4j.linalg.lossfunctions.LossFunctions;
import org.nd4j.shade.guava.cache.CacheBuilder;
import org.nd4j.shade.guava.cache.CacheLoader;
import org.nd4j.shade.guava.cache.LoadingCache;
import org.nd4j.shade.guava.util.concurrent.ListenableFuture;
import org.nd4j.shade.guava.util.concurrent.ListeningExecutorService;
import org.nd4j.shade.guava.util.concurrent.MoreExecutors;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static edu.uta.flowsched.Util.POISON_CLIENT;
import static edu.uta.flowsched.Util.formatMessage;
import static edu.uta.flowsched.schedulers.CONFIGS.DATA_SIZE;
import static edu.uta.flowsched.schedulers.DrlSmartFlowV4.FEATURES_PER_PATH;

public class DrlSmartFlowV4 extends SmartFlowScheduler {
    /* ----- LSTM history --------- */
    private static double rewardMean = 0, rewardVar = 1;
    private static final double MOMENTUM = 0.01;
    private static SmartFlowScheduler S2C_INSTANCE, C2S_INSTANCE;
    private final PolicyNetwork policy;
    private final List<Transition> roundExperience = new CopyOnWriteArrayList<>();
    private final ConcurrentHashMap<FLHost, ActionContext> observationCache;
    private final LoadingCache<String, INDArray> localObsCache;
    private final ExecutorService executorService;
    public static final int FEATURES_PER_PATH = 6;

    public static SmartFlowScheduler getInstance(FlowDirection direction) {
        if (S2C_INSTANCE == null) S2C_INSTANCE = new DrlSmartFlowV4(FlowDirection.S2C);
        if (C2S_INSTANCE == null) C2S_INSTANCE = new DrlSmartFlowV4(FlowDirection.C2S);
        return direction == FlowDirection.S2C ? S2C_INSTANCE : C2S_INSTANCE;
    }

    private DrlSmartFlowV4(FlowDirection dir) {
        super(dir);
        Nd4jNativeLoader.load();
        policy = PolicyNetwork.loadOrCreate(dir);
        observationCache = new ConcurrentHashMap<>();
        executorService = Executors.newSingleThreadExecutor();
        localObsCache = CacheBuilder.newBuilder()
                .recordStats()
                .refreshAfterWrite(Util.POLL_FREQ, TimeUnit.SECONDS)
                .build(new LocalOpsCacheLoader());
    }

    /* ===================================================================
       PHASE 1 — select a path for every waiting client
    =================================================================== */

    @Override
    protected void phase1(ProcessingContext context, AtomicLong phase1Total) throws Exception {
        FLHost cl = context.needPhase1Processing.take();
        if (cl.equals(POISON_CLIENT)) return; // Round Done

        List<FLHost> clientsToProcess = new LinkedList<>(); // Don't forget the client you already took.
        while (cl != null) {
            if (Util.getAgeInSeconds(cl.getLastPathChange()) <= Util.POLL_FREQ * 1.5)
                context.needPhase2Processing.add(cl);
            else
                clientsToProcess.add(cl);
            cl = context.needPhase1Processing.poll();
        }

        if (clientsToProcess.isEmpty()) return;

        long tic = System.currentTimeMillis();
        StringBuilder phase1Logger = new StringBuilder(formatMessage(0, "Phase 1 (DRL batched for %d clients):", clientsToProcess.size()));
        // 2. Build observations and prepare inputs for the batched call.
        long tempTec = System.currentTimeMillis();
        INDArray globalObs = localObsCache.get("global");
        phase1Logger.append(formatMessage(0, "Took %s ms to build globalObs", System.currentTimeMillis() - tempTec));

        List<INDArray> localObsList = new ArrayList<>();
        List<Integer> validActionsList = new ArrayList<>();
        List<List<MyPath>> pathsPerClient = new ArrayList<>();

        for (FLHost client : clientsToProcess) {
            localObsList.add(localObsCache.get("flclient" + client.getFlClientCID()));
            List<MyPath> paths = context.clientPaths.get(client);
            pathsPerClient.add(paths);
            validActionsList.add(paths.size());
        }

        INDArray batchedLocalObs = Nd4j.vstack(localObsList);
        phase1Logger.append(formatMessage(0, "Took %s ms to build batched LocalOps", System.currentTimeMillis() - tempTec));

        // 3. Call chooseActions to get all decisions in one go.
        tempTec = System.currentTimeMillis();
        int[] actions = policy.chooseActions(batchedLocalObs, globalObs, validActionsList);
        phase1Logger.append(formatMessage(0, "Took %s ms to choose all actions", System.currentTimeMillis() - tempTec));

        // 4. Process the results for each client.
        for (int i = 0; i < clientsToProcess.size(); i++) {
            FLHost client = clientsToProcess.get(i);
            List<MyPath> paths = pathsPerClient.get(i);

            StringBuilder clientLogger = new StringBuilder(formatMessage(1, "- Client %s:", client.getFlClientCID()));

            MyPath bestPath = paths.get(actions[i]);
            MyPath currentPath = client.getCurrentPath();

            boolean switching = shouldSwitchPath(client, currentPath, bestPath, clientLogger);

            if (switching) {
                Set<FLHost> affected = client.assignNewPath(bestPath);
                PathRulesInstaller.INSTANCE.installPathRules(client, bestPath, false);
                updateTimeAndRate(context, affected, clientLogger);
            }

            // Cache the context for Phase 2, detaching observations to prevent memory leaks.
            observationCache.put(client, new ActionContext(localObsList.get(i).detach(), globalObs.detach(), actions[i]));
            context.needPhase2Processing.add(client);
            phase1Logger.append(clientLogger);
        }

        Util.log("smartflow" + direction, phase1Logger.toString());
        phase1Total.addAndGet(System.currentTimeMillis() - tic);
    }

    private boolean shouldSwitchPath(FLHost client, MyPath currentPath, MyPath bestPath, StringBuilder logger) {
        if (currentPath == null) {
            logger.append(formatMessage(2, "First-time assignment"));
            return true; // first‑time assignment
        }
        if (bestPath.equals(currentPath)) {
            logger.append(formatMessage(2, "Current path is already best"));
            return false;
        }
        logger.append(formatMessage(2, "Switched ⇢ %s", bestPath.format()));
        return true;
    }

    /* ===================================================================
       PHASE 2 — compute reward and train one step
    =================================================================== */
    @Override
    protected void extraClientPhase2ClientProcessing(ProcessingContext ctx, FLHost cl, StringBuilder clientLogger) {
        ActionContext actionContext = observationCache.remove(cl);
        if (actionContext == null) return;

        double rRate = (double) Util.bitToMbit(cl.networkStats.getLastPositiveRate(this.direction)) / MyLink.DEFAULT_CAPACITY;
        double rProgress = cl.networkStats.getLastPositiveData(this.direction) / (double) DATA_SIZE;

        double rOverlap = 0;
        if (cl.getCurrentPath() != null) {
            int pathFlows = (int) cl.getCurrentPath().getCurrentActiveFlows();
            rOverlap = -0.1 * (pathFlows - 1) / ctx.totalClients;
        }
        double rSwitch = (Util.getAgeInSeconds(cl.getLastPathChange()) <= Util.POLL_FREQ) ? -.1 : 0.0;
        double reward = rRate + rProgress + rOverlap + rSwitch;

        double normReward = getNormReward(reward);

        clientLogger.append(formatMessage(2, "NormReward %.3f, Reward %.3f (rate=%.2f, progress=%.2f, overlap=%.2f, switch=%.2f)",
                normReward, reward, rRate, rProgress, rOverlap, rSwitch));
        roundExperience.add(new Transition(actionContext.localObs, actionContext.globalObs, actionContext.action, normReward));
    }

    private double getNormReward(double reward) {
        // Normalize
        rewardMean = (1 - MOMENTUM) * rewardMean + MOMENTUM * reward;
        rewardVar = (1 - MOMENTUM) * rewardVar + MOMENTUM * Math.pow(reward - rewardMean, 2);
        double std = Math.sqrt(rewardVar);
        return (reward - rewardMean) / (std + 1e-6);
    }

    // --- MODIFIED: Global observation now covers all clients ---
    private INDArray buildGlobalObservation(ProcessingContext context) {
        List<INDArray> clientObservations = new ArrayList<>();
        for (FLHost client : context.clientPaths.keySet()) {
            clientObservations.add(localObsCache.getUnchecked("flclient" + client.getFlClientCID()));
        }
        if (clientObservations.isEmpty()) {
            int fixedGlobalFeatureSize = (FEATURES_PER_PATH * CONFIGS.PATHS_LIMIT * 3) + 2;
            return Nd4j.zeros(1, fixedGlobalFeatureSize);
        }

        INDArray allClientsMatrix = Nd4j.vstack(clientObservations);

        INDArray meanFeatures = allClientsMatrix.mean(0).reshape(1, -1);
        INDArray maxFeatures = allClientsMatrix.max(0).reshape(1, -1);
        INDArray stdDevFeatures = allClientsMatrix.std(0).reshape(1, -1);

        double totalClients = context.totalClients;
        double activeClients = context.totalClients - context.completedClients.size();
        INDArray activeRatio = Nd4j.create(new float[]{(float) (activeClients / totalClients)}).reshape(1, -1);
        INDArray progressEstimate = Nd4j.create(new float[]{(float) ((totalClients - activeClients) / totalClients)}).reshape(1, -1);
        return Nd4j.hstack(meanFeatures, maxFeatures, stdDevFeatures, activeRatio, progressEstimate);
    }

    private INDArray buildObservation(List<MyPath> kPaths) {
        INDArray[] rows = new INDArray[CONFIGS.PATHS_LIMIT];
        for (int i = 0; i < CONFIGS.PATHS_LIMIT; i++) {
            rows[i] = i < kPaths.size() ? extractFeatures(kPaths.get(i)) : Nd4j.zeros(FEATURES_PER_PATH);
        }
        return Nd4j.hstack(rows).reshape(1, (long) CONFIGS.PATHS_LIMIT * FEATURES_PER_PATH);
    }

    private INDArray extractFeatures(MyPath p) {
        return Nd4j.create(new float[]{
                (float) Util.bitToMbit(p.getProjectedFairShare()) / MyLink.DEFAULT_CAPACITY,
                (float) Util.bitToMbit(p.getBottleneckFreeCap()) / MyLink.DEFAULT_CAPACITY,
                (float) p.getEffectiveRTT() / 7,
                (float) Math.log1p(p.getPacketLossProbability()),
                (float) p.getCurrentActiveFlows() / ClientInformationDatabase.INSTANCE.getTotalFLClients(),
                (float) p.linksNoEdge().size() / 10,
        });
    }

    @Override
    protected void endOfRoundProcessing(ProcessingContext context) {
        if (!roundExperience.isEmpty()) {
            executorService.submit(this::train);
        }
        String stragglers = context.completedClients.stream()
                .skip(Math.max(0, context.totalClients - 3)) // skip to last 3
                .map(FLHost::getFlClientCID)
                .collect(Collectors.joining(","));

        Util.log("stragglers", formatMessage(0, "%s,%s", direction, stragglers));
    }

    void train() {
        Util.log("smartflow" + direction, formatMessage(0, "End of round. Training on %d experiences...", roundExperience.size()));
        policy.trainBatch(new ArrayList<>(roundExperience)); // Pass a copy
        roundExperience.clear(); // Clear for the next round
        policy.saveCheckpoint(direction);
    }

    private class LocalOpsCacheLoader extends CacheLoader<String, INDArray> {
        private final ListeningExecutorService executorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(4));

        @Override
        public ListenableFuture<INDArray> reload(String key, INDArray oldValue) throws Exception {
            return executorService.submit(() -> getINDArray(key).get());
        }

        @Override
        public INDArray load(String key) throws Exception {
            return getINDArray(key);
        }

        private INDArray getINDArray(String key) {
            if (key.startsWith("flclient")) {
                FLHost client = ClientInformationDatabase.INSTANCE.getHostByFLCID(key.substring(8));
                return buildObservation(context.clientPaths.get(client));
            } else if (key.startsWith("global")) {
                return buildGlobalObservation(context);
            } else {
                Util.log("general", "Wrong Key!");
                return Nd4j.zeros(1);
            }
        }
    }
}

final class PolicyNetwork {
    public static final String DRL_MODEL_PATH = "/var/opt/onos/%s.zip";
    private static final String POLICY = "pi";
    private static final String VALUE = "v";
    private final ComputationGraph net;
    private static final int N_IN_LOCAL = CONFIGS.PATHS_LIMIT * FEATURES_PER_PATH;
    private static final int N_IN_GLOBAL = (CONFIGS.PATHS_LIMIT * FEATURES_PER_PATH * 3) + 2;
    private static final int N_OUT = CONFIGS.PATHS_LIMIT;
    private static final boolean DEEPER_MODEL = true;

    static PolicyNetwork loadOrCreate(FlowDirection dir) {
        File f = new File(String.format(DRL_MODEL_PATH, "rl_model_" + dir));
        try {
            if (f.exists()) {
                Util.log("general", formatMessage(0, "Loading DRL model from %s", f));
                return new PolicyNetwork(ComputationGraph.load(f, true), dir);
            }
        } catch (Exception ignored) {
            Util.log("general", formatMessage(0, "Could not load model, creating a fresh one."));
        }
        return new PolicyNetwork(buildFresh(), dir);
    }

    private PolicyNetwork(ComputationGraph cg, FlowDirection direction) {
        this.net = cg;
        this.net.setListeners(new UtilScoreListener(1, direction));
    }

    private static ComputationGraph buildFresh() {
        NeuralNetConfiguration.Builder nb = new NeuralNetConfiguration.Builder()
                .seed(42).l2(1e-6).updater(new Adam(1e-4));

        ComputationGraphConfiguration.GraphBuilder g = nb.graphBuilder().addInputs("in_local", "in_global");
        // Simplified dense layers (no LSTM)
        g.addLayer("dense_actor_0", new DenseLayer.Builder().nIn(N_IN_LOCAL).nOut(256).activation(Activation.RELU).build(), "in_local");
        String actorOutput = "dense_actor_0";
        if (DEEPER_MODEL) {
            g.addLayer("dense_actor_1", new DenseLayer.Builder().nIn(256).nOut(128).activation(Activation.RELU).build(), "dense_actor_0");
            actorOutput = "dense_actor_1";
        }
        // --- Deeper Critic Network ---
        g.addLayer("dense_critic_0", new DenseLayer.Builder().nIn(N_IN_GLOBAL).nOut(256).activation(Activation.RELU).build(), "in_global");
        String criticOutput = "dense_critic_0";
        if (DEEPER_MODEL) {
            g.addLayer("dense_critic_1", new DenseLayer.Builder().nIn(256).nOut(128).activation(Activation.RELU).build(), "dense_critic_0");
            criticOutput = "dense_critic_1";
        }
        // --- Output Layers ---
        g.addLayer(POLICY, new OutputLayer.Builder(new ActorCriticLoss()).nOut(N_OUT).activation(Activation.SOFTMAX).build(), actorOutput);
        g.addLayer(VALUE, new OutputLayer.Builder(LossFunctions.LossFunction.MSE).nOut(1).activation(Activation.IDENTITY).build(), criticOutput);

        g.setOutputs(POLICY, VALUE);
        g.setInputTypes(InputType.feedForward(N_IN_LOCAL), InputType.feedForward(N_IN_GLOBAL));

        ComputationGraph cg = new ComputationGraph(g.build());
        cg.init();
        return cg;
    }

    int chooseAction(INDArray localObs, INDArray globalObs, int validActions) {
        INDArray pi = net.feedForward(new INDArray[]{localObs, globalObs}, false).get(POLICY);
        INDArray piSlice = pi.get(NDArrayIndex.point(0), NDArrayIndex.interval(0, validActions));
        double sum = piSlice.sumNumber().doubleValue();

        if (sum <= 0 || Double.isNaN(sum) || Double.isInfinite(sum)) {
            return ThreadLocalRandom.current().nextInt(validActions);
        }
        piSlice.divi(sum);
        INDArray cdf = Nd4j.cumsum(piSlice, 1);
        double randomDraw = Math.random();
        // Find the first index where the cumulative probability is greater than our random draw.
        for (int i = 0; i < cdf.length(); i++) {
            if (randomDraw < cdf.getDouble(i)) {
                // This is our sampled action
                return i;
            }
        }
        return validActions - 1;
    }

    public int[] chooseActions(INDArray batchedLocalObs, INDArray globalObs, List<Integer> validActionsPerClient) {
        int batchSize = batchedLocalObs.rows();
        INDArray batchedGlobalObs = Nd4j.tile(globalObs, batchSize, 1);
        INDArray piBatch = net.feedForward(new INDArray[]{batchedLocalObs, batchedGlobalObs}, false).get(POLICY);
        int[] chosenActions = new int[batchSize];
        for (int i = 0; i < batchSize; i++) {
            int validActions = validActionsPerClient.get(i);
            INDArray pi = piBatch.getRow(i); // Probabilities for the i-th client.
            INDArray piSlice = pi.get(NDArrayIndex.interval(0, validActions));
            double sum = piSlice.sumNumber().doubleValue();
            if (sum <= 0 || Double.isNaN(sum) || Double.isInfinite(sum)) {
                chosenActions[i] = ThreadLocalRandom.current().nextInt(validActions);
                continue;
            }
            piSlice.divi(sum);
            INDArray cdf = Nd4j.cumsum(piSlice, 1);
            double randomDraw = Math.random();

            int action = validActions - 1;
            for (int j = 0; j < cdf.length(); j++) {
                if (randomDraw < cdf.getDouble(j)) {
                    action = j;
                    break;
                }
            }
            chosenActions[i] = action;
        }

        return chosenActions;
    }


    long trainBatch(List<Transition> batch) {
        long tic = System.currentTimeMillis();
        int B = batch.size();
        INDArray in_local = Nd4j.concat(0, batch.stream().map(t -> t.localObs).toArray(INDArray[]::new));
        INDArray in_global = Nd4j.concat(0, batch.stream().map(t -> t.globalObs).toArray(INDArray[]::new));
        INDArray act = Nd4j.create(B, 1);
        INDArray rew = Nd4j.create(B, 1);
        for (int i = 0; i < B; i++) {
            act.putScalar(i, 0, batch.get(i).action);
            rew.putScalar(i, 0, batch.get(i).reward);
        }
        Map<String, INDArray> out = net.feedForward(new INDArray[]{in_local, in_global}, true);
        INDArray pi = out.get(POLICY);
        INDArray v = out.get(VALUE);
        INDArray adv = rew.sub(v);
        INDArray labelPi = Nd4j.zeros(B, N_OUT);
        for (int i = 0; i < B; i++) {
            labelPi.putScalar(i, (int) act.getDouble(i), adv.getDouble(i));
        }
        MultiDataSet mds = new MultiDataSet(new INDArray[]{in_local, in_global}, new INDArray[]{labelPi, rew});
        net.fit(mds);
        return System.currentTimeMillis() - tic;
    }

    void saveCheckpoint(FlowDirection dir) {
        try {
            File chk = new File(String.format(DRL_MODEL_PATH, "rl_model_" + dir));
            net.save(chk, true);
            Util.log("general", formatMessage(0, "Saved DRL model checkpoint for %s", dir));
        } catch (IOException e) {
            Util.log("general", formatMessage(0, "ERROR: Could not save DRL model. %s", e.getMessage()));
        }
    }
}

final class Transition {
    final INDArray localObs;
    final INDArray globalObs;
    final int action;
    final double reward;

    Transition(INDArray local, INDArray global, int a, double r) {
        localObs = local;
        globalObs = global;
        action = a;
        reward = r;
    }
}

final class UtilScoreListener extends ScoreIterationListener {
    private final int printIterations;
    private final FlowDirection direction;

    UtilScoreListener(int printIterations, FlowDirection direction) {
        super(printIterations);
        this.printIterations = printIterations;
        this.direction = direction;
    }

    @Override
    public void iterationDone(Model model, int iteration, int epoch) {
        if (iteration % this.printIterations == 0) {
            double score = model.score();
            Util.log("general", formatMessage(0, "DRL Training (%s): Score at iteration %s is %.5f", this.direction, iteration, score));
        }
    }
}

final class ActionContext {
    final INDArray localObs;
    final INDArray globalObs;
    final int action;

    ActionContext(INDArray localObs, INDArray globalObs, int action) {
        this.localObs = localObs;
        this.globalObs = globalObs;
        this.action = action;
    }
}

