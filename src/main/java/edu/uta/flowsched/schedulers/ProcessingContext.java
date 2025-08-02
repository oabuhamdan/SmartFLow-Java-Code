package edu.uta.flowsched.schedulers;

import edu.uta.flowsched.FLHost;
import edu.uta.flowsched.MyPath;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ProcessingContext {
    public Queue<FLHost> clientQueue;
    public Map<FLHost, Integer> completionTimes;
    public Map<FLHost, List<MyPath>> clientPaths;
    public Map<FLHost, Long> deltaDataExchanged;
    public Map<FLHost, Long> assumedRates;
    public LinkedBlockingQueue<FLHost> needPhase1Processing;
    public ConcurrentLinkedQueue<FLHost> needPhase2Processing;
    public ConcurrentLinkedQueue<FLHost> completedClients;
    public int totalClients;
    public double rGapPenalty;

    public ProcessingContext(ProcessingContext otherContext) {
        this.clientQueue = otherContext.clientQueue;
        this.completionTimes = otherContext.completionTimes;
        this.clientPaths = otherContext.clientPaths;
        this.deltaDataExchanged = otherContext.deltaDataExchanged;
        this.assumedRates = otherContext.assumedRates;
        this.needPhase1Processing = otherContext.needPhase1Processing;
        this.needPhase2Processing = otherContext.needPhase2Processing;
        this.completedClients = otherContext.completedClients;
        this.totalClients = otherContext.totalClients;
        this.rGapPenalty = otherContext.rGapPenalty;
    }

    public ProcessingContext() {
        this.clientQueue = new ConcurrentLinkedQueue<>();
        this.completionTimes = new ConcurrentHashMap<>();
        this.deltaDataExchanged = new ConcurrentHashMap<>();
        this.assumedRates = new ConcurrentHashMap<>();
        this.clientPaths = new ConcurrentHashMap<>();
        this.needPhase1Processing = new LinkedBlockingQueue<>();
        this.needPhase2Processing = new ConcurrentLinkedQueue<>();
        this.completedClients = new ConcurrentLinkedQueue<>();
        this.totalClients = 0;
        this.rGapPenalty = 0;
    }

    public void clear() {
        this.clientQueue.clear();
        this.completionTimes.clear();
        this.deltaDataExchanged.clear();
        this.assumedRates.clear();
        this.clientPaths.clear();
        this.needPhase1Processing.clear();
        this.needPhase2Processing.clear();
        this.completedClients.clear();
        this.totalClients = 0;
        this.rGapPenalty = 0;
    }
}
