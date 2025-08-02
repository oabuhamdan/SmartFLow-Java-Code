package edu.uta.flowsched.schedulers;

import edu.uta.flowsched.*;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static edu.uta.flowsched.Util.bitToMbit;
import static edu.uta.flowsched.Util.formatMessage;
import static edu.uta.flowsched.schedulers.CONFIGS.DATA_SIZE;

public abstract class SmartFlowScheduler {
    public static SmartFlowScheduler S2C;
    public static SmartFlowScheduler C2S;
    protected ExecutorService mainExecutor;
    protected ScheduledExecutorService phase2Executor;
    protected FlowDirection direction;
    protected final AtomicInteger round;
    protected final AtomicBoolean isPhase2Scheduled;
    protected final ProcessingContext context;

    protected SmartFlowScheduler(FlowDirection direction) {
        this.direction = direction;
        round = new AtomicInteger(1);
        context = new ProcessingContext();
        mainExecutor = Executors.newSingleThreadExecutor();
        phase2Executor = Executors.newSingleThreadScheduledExecutor();
        isPhase2Scheduled = new AtomicBoolean(false); // Because Future.IsDone is not so reliable
    }

    public static void activate() {
        S2C = DrlSmartFlowV4.getInstance(FlowDirection.S2C);
        C2S = DrlSmartFlowV4.getInstance(FlowDirection.C2S);

    }

    public static void deactivate() {
        S2C.mainExecutor.shutdownNow();
        S2C.phase2Executor.shutdownNow();
        C2S.mainExecutor.shutdownNow();
        C2S.phase2Executor.shutdownNow();
    }

    public static void startRound() {
        S2C.mainExecutor.submit(() -> S2C.main());
        C2S.mainExecutor.submit(() -> C2S.main());
    }

    public void addClientToQueue(FLHost client) {
        initClient(client);
        context.needPhase1Processing.offer(client);
    }

    public void addClientsToQueue(List<FLHost> clients) {
        clients.forEach(this::initClient);
        initialSort(context, clients);
        context.needPhase1Processing.addAll(clients);
    }

    public void initialSort(ProcessingContext context, List<FLHost> clients) {
    }

    private void initClient(FLHost client) {
        client.clearPath();
        client.setLastPathChange(0);
        context.deltaDataExchanged.put(client, DATA_SIZE);
        context.assumedRates.put(client, 0L);
        context.completionTimes.put(client, 100);
        List<MyPath> paths =PathInformationDatabase.INSTANCE.getPaths(client, this.direction).stream()
                .sorted(Comparator.comparingInt(MyPath::hashCode))
                .collect(Collectors.toList());
        context.clientPaths.put(client, paths);
    }

    protected void main() {
        AtomicLong phase1TotalTime = new AtomicLong(0), phase2TotalTime = new AtomicLong(0);
        Supplier<String> time = () -> LocalDateTime.now().format(Util.LOG_TIME_FORMATTER);
        Util.log("smartflow" + this.direction, formatMessage(0, "******** Round %s Started at %s **********", round, time.get()));
        context.totalClients = ClientInformationDatabase.INSTANCE.getTotalFLClients();
        while (context.completedClients.size() < context.totalClients) {
            try {
                phase1(context, phase1TotalTime);
                phase2(context, phase2TotalTime);
            } catch (Exception e) {
                String trace = e.getMessage() + "; " + Arrays.toString(Arrays.stream(e.getStackTrace()).toArray());
                Util.log("smartflow" + this.direction, formatMessage(0, "ERROR: %s ", trace));
            }
        }
        endOfRoundProcessing(context);
        context.clear();
        Util.log("overhead", formatMessage(0, "%s,%s,phase1,%s", direction, round.get(), phase1TotalTime.get()));
        Util.log("overhead", formatMessage(0, "%s,%s,phase2,%s", direction, round.get(), phase2TotalTime.get()));
        Util.log("smartflow" + this.direction, formatMessage(0, "******** Round %s Completed at %s ********\n", round.getAndIncrement(), time.get()));
        Util.flushWriters();
    }

    protected void endOfRoundProcessing(ProcessingContext context) {

    }


    abstract protected void phase1(ProcessingContext context, AtomicLong phase1Total) throws Exception;

    protected void phase2(ProcessingContext context, AtomicLong phase2Total) {
        if (!context.needPhase2Processing.isEmpty() && !isPhase2Scheduled.get()) {
            phase2Executor.schedule(() -> updateClientsStats(context, phase2Total), (int) (Util.POLL_FREQ * 1.5), TimeUnit.SECONDS);
            isPhase2Scheduled.set(true);
        }
    }

    protected void updateClientsStats(ProcessingContext context, AtomicLong phase2TotalTime) {
        Supplier<String> time = () -> LocalDateTime.now().format(Util.LOG_TIME_FORMATTER);
        StringBuilder phase2Logger = new StringBuilder(formatMessage(0, "Phase 2 at %s:", time.get()));
        long tik = System.currentTimeMillis();
        List<FLHost> needsFurtherProcessing = new LinkedList<>();
        FLHost client;
        while ((client = context.needPhase2Processing.poll()) != null) {
            StringBuilder clientLogger = new StringBuilder(formatMessage(1, "- Client %s: ", client.getFlClientCID()));
            try {
                long assignedRate = Math.max(client.networkStats.getLastPositiveRate(this.direction), (long) 1e6);
                long dataRemain = DATA_SIZE - client.networkStats.getRoundExchangedData(this.direction);
                int remainingTime = (int) (dataRemain / assignedRate);
                if (dataRemain <= 5e5 || remainingTime < Util.POLL_FREQ / 2) {
                    if (!client.clearPath()) {
                        clientLogger.append(formatMessage(2, "Warn: Client has no Path!"));
                    }
                    context.completedClients.add(client);
                    client.networkStats.resetExchangedData(direction);
                } else {
                    needsFurtherProcessing.add(client);
                }
                clientLogger.append(formatMessage(2, "Art Rate: %sMbps, ", bitToMbit(context.assumedRates.get(client))))
                        .append(formatMessage(2, "Real Rate: %sMbps, ", bitToMbit(assignedRate)))
                        .append(formatMessage(2, "Real Rem Data: %sMbits, ", bitToMbit(dataRemain)))
                        .append(formatMessage(2, "Real Rem Time: %ss", dataRemain / assignedRate));
                extraClientPhase2ClientProcessing(context, client, clientLogger);
            } catch (Exception e) {
                String trace = e.getMessage() + "; " + Arrays.toString(Arrays.stream(e.getStackTrace()).toArray());
                Util.log("smartflow" + this.direction, formatMessage(2, "ERROR: %s ", trace));
            }
            phase2Logger.append(clientLogger);
        }
        isPhase2Scheduled.set(false);
        phase2Logger.append(formatMessage(1, "******* Completed %s Clients at *******", context.completedClients.size(), time.get()));
        Util.log("smartflow" + this.direction, phase2Logger.toString());
        if (context.completedClients.size() >= context.totalClients) {
            context.needPhase1Processing.offer(Util.POISON_CLIENT);
        }
        context.needPhase1Processing.addAll(needsFurtherProcessing);
        phase2TotalTime.addAndGet(System.currentTimeMillis() - tik);
        Util.flushWriters();
    }

    protected void extraClientPhase2ClientProcessing(ProcessingContext context, FLHost client, StringBuilder clientLogger) {
    }

    protected void updateTimeAndRate(ProcessingContext context, Set<FLHost> affectedClients, StringBuilder internalLogger) {
        for (FLHost affectedClient : affectedClients) {
            if (affectedClient.getCurrentPath() != null) {
                long updatedFairShare = affectedClient.getCurrentPath().getCurrentFairShare();
                long dataRemain = context.deltaDataExchanged.getOrDefault(affectedClient, 0L);
                int updatedCompletionTime = (int) Math.round(1.0 * dataRemain / updatedFairShare);
                context.completionTimes.put(affectedClient, updatedCompletionTime);
                context.assumedRates.put(affectedClient, updatedFairShare);
            } else {
                internalLogger.append(formatMessage(2, "Client %s has no current path!", affectedClient.getFlClientCID()));
            }
        }
    }
}
