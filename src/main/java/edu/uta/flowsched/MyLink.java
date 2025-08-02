package edu.uta.flowsched;

import org.onosproject.net.DefaultLink;
import org.onosproject.net.Link;
import org.onosproject.net.provider.ProviderId;

import java.io.Serializable;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class MyLink extends DefaultLink implements Serializable {
    private static final ProviderId PID = new ProviderId("flowsched", "edu.uta.flowsched", true);
    public static final long DEFAULT_CAPACITY = 95_000_000;

    private final AtomicInteger activeFlows;
    private final long defaultCapacity;
    private final BoundedConcurrentLinkedQueue<Long> linkThroughput;
    private final BoundedConcurrentLinkedQueue<Integer> latency;
    private final BoundedConcurrentLinkedQueue<Double> packetLoss;
    private final AtomicLong reservedCapacity;
    private final Set<FLHost> clientsUsingPath;

    public MyLink(Link link) {
        super(PID, link.src(), link.dst(), link.type(), link.state(), link.annotations());
        this.defaultCapacity = DEFAULT_CAPACITY;
        this.linkThroughput = new BoundedConcurrentLinkedQueue<>(3);
        this.latency = new BoundedConcurrentLinkedQueue<>(3);
        this.packetLoss = new BoundedConcurrentLinkedQueue<>(3);
        this.reservedCapacity = new AtomicLong(0);
        this.activeFlows = new AtomicInteger(0);
        this.clientsUsingPath = ConcurrentHashMap.newKeySet();
    }

    public int getActiveFlows() {
        return activeFlows.get();
    }

    public Set<FLHost> getClientsUsingLink() {
        return Collections.unmodifiableSet(clientsUsingPath);
    }

    public long getDefaultCapacity() {
        return defaultCapacity;
    }

    public long getEstimatedFreeCapacity() {
        return getDefaultCapacity() - getThroughput();
    }

    public long getThroughput() {
//        return Optional.ofNullable(this.linkThroughput.peekLast()).orElse(0L);
        return (long) Util.weightedAverage(this.linkThroughput, true);
    }

    public void setCurrentThroughput(long currentThroughput) {
        this.linkThroughput.add(currentThroughput);
    }

    public double getLatency() {
//        return Optional.ofNullable(this.latency.peekLast()).orElse(0);
        return Util.weightedAverage(this.latency, true);
    }

    public double getPacketLoss() {
//        return Optional.ofNullable(this.packetLoss.peekLast()).orElse(0.0);
        return Util.weightedAverage(this.packetLoss, true);
    }

    public void setLatency(int latency) {
        this.latency.add(latency);
    }

    public void setPacketLoss(double packetLoss) {
        this.packetLoss.add(packetLoss);
    }

    public long getReservedCapacity() {
        return reservedCapacity.get();
    }

    public void addClient(FLHost client) {
        activeFlows.incrementAndGet();
        clientsUsingPath.add(client);
    }

    public void removeClient(FLHost client) {
        if (activeFlows.get() > 0)
            activeFlows.decrementAndGet();
        clientsUsingPath.remove(client);
    }

    public static long estimateFairShare(double linkCapacityMbps, double freeCap, int currentActiveFlows, double windowSeconds) {
        final double MSS_BYTES = 1460;    // typical Ethernet MSS
        final double C = 0.4;         // CUBIC aggression   (RFC 8312)
        final double BETA = 0.7;
        final double RTT_Millis = 5;

        int totalFlows = currentActiveFlows + 1;
        double fairShare = linkCapacityMbps / totalFlows;  // long-term equal split
        double residual = Math.max(0.0, freeCap);

        double steadyMbps;
        if (residual >= fairShare) {
            double leftover = residual - fairShare;
            steadyMbps = 0.9 * fairShare + leftover / totalFlows;
        } else {
            steadyMbps = 0.9 * fairShare;
        }

        double bytesPerSecFair = (steadyMbps * 1_000_000) / 8.0;
        double cwndFair = bytesPerSecFair * (RTT_Millis / 1_000.0) / MSS_BYTES;

        double cwnd0 = 10;                      // RFC 6928 initial window ≈ 10 MSS
        if (cwndFair <= cwnd0) return (long) steadyMbps;  // already above fair cwnd
        double K = Math.cbrt(cwnd0 * BETA / C);
        double tEq = K + Math.cbrt((cwndFair - cwnd0) / C);   // seconds to reach cwndFair
        double penalty = Math.min(1.0, windowSeconds / tEq);
        return (long) (penalty * steadyMbps);   // Mbps averaged over the time window
    }

    public long getCurrentFairShare() {
        int flows = this.activeFlows.get();
        return getEstimatedFreeCapacity() / Math.max(flows, 1);
    }

    public long getProjectedFairShare() {
        int flows = this.activeFlows.get() + 1;
        return getEstimatedFreeCapacity() / flows;
    }


    public String format() {
        return Util.formatHostId(this.src().elementId()) + " -> " + Util.formatHostId(this.dst().elementId());
    }

    public String id() {
        return String.valueOf(hashCode());
    }
}
