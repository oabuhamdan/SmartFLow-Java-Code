package edu.uta.flowsched;

import org.onlab.graph.ScalarWeight;
import org.onosproject.net.DefaultPath;
import org.onosproject.net.Link;
import org.onosproject.net.Path;
import org.onosproject.net.provider.ProviderId;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;

public class MyPath extends DefaultPath {
    private static final ProviderId PID = new ProviderId("flowsched", "edu.uta.flowsched", true);
    private final Set<Link> linksWithoutEdge;
    private long lastUpdated;


    public MyPath(Path path) {
        super(PID, path.links().stream().map(LinkInformationDatabase.INSTANCE::getLinkInformation)
                .collect(Collectors.toList()), new ScalarWeight(1));
        this.linksWithoutEdge = links().stream().filter(link -> !link.type().equals(Type.EDGE)).collect(Collectors.toSet());
        lastUpdated = 0;
    }

    public Set<Link> linksNoEdge() {
        return linksWithoutEdge;
    }

    public Set<FLHost> addClient(FLHost client) {
        Set<FLHost> affectedClients = ConcurrentHashMap.newKeySet();
        linksNoEdge().forEach(link -> {
            ((MyLink) link).addClient(client); // Putting it first to calculate it with the affected clients
            affectedClients.addAll(((MyLink) link).getClientsUsingLink());
        });
        return affectedClients;
    }

    public Set<FLHost> removeClient(FLHost client) {
        Set<FLHost> affectedClients = ConcurrentHashMap.newKeySet();
        linksNoEdge().forEach(link -> {
            ((MyLink) link).removeClient(client);
            affectedClients.addAll(((MyLink) link).getClientsUsingLink());
        });
        return affectedClients;
    }

    private long getFairShare(ToLongFunction<? super Link> function) {
        return linksNoEdge().stream()
                .mapToLong(function)
                .min().orElse(0);
    }

    public long getCurrentFairShare() {
        return getFairShare(link -> ((MyLink) link).getCurrentFairShare());
    }

    public long getProjectedFairShare() {
        return getFairShare(link -> ((MyLink) link).getProjectedFairShare());
    }

    public long getBottleneckFreeCap() {
        return linksNoEdge().stream()
                .mapToLong(link -> ((MyLink) link).getEstimatedFreeCapacity())
                .min()
                .orElse(0);
    }

    public double getEffectiveRTT() {
        double totalLatency = 0;
        for (Link link : linksNoEdge()) {
            totalLatency += ((MyLink) link).getLatency();
            MyLink linkInverse = LinkInformationDatabase.INSTANCE.getLinkInverse(link);
            totalLatency += linkInverse.getLatency();
        }
        return totalLatency;
    }

    public double getPacketLossProbability() {
        double survivalRate = 1.0;
        for (Link link : linksNoEdge()) {
            survivalRate *= (1 - ((MyLink) link).getPacketLoss());
        }
        return 1 - survivalRate;
    }

    public long cubicCapBps() {
        // Use the Mathis equation to estimate throughput as Mathis_throughput.
        // Irrevelant here, since our latency is the queuing latency, not the base one.
        final double MSS_BITS = 8 * 1460;           // 1 segment in bits
        final double K_CUBIC = 1.1;
        double rtt = 2 * getEffectiveRTT() / 1000.0;            // s
        double loss = Math.max(getPacketLossProbability(), 1e-10);    // avoid div‑by‑0
        long bps = (long) ((K_CUBIC * MSS_BITS) / (rtt * Math.sqrt(loss)));
        return Math.min(bps, MyLink.DEFAULT_CAPACITY);
    }

    public double effectiveScore() {
        //https://ieeexplore.ieee.org/document/8680730
        // https://dl.acm.org/doi/10.1145/1111322.1111336
        double p = getPacketLossProbability();
        double effectiveRtt = getEffectiveRTT();
        double bottleneckLink = Util.bitToMbit(getProjectedFairShare());
        double adjustedEffectiveRTT = effectiveRtt * Math.sqrt (p + 1e-2);
        return bottleneckLink / adjustedEffectiveRTT;
    }

    public double getCurrentActiveFlows() {
        return linksNoEdge().stream()
                .mapToInt(link -> ((MyLink) link).getActiveFlows())
                .max()
                .orElse(0);
    }

    public String format() {
        StringBuilder stringBuilder = new StringBuilder();
        for (Link link : this.links()) {
            stringBuilder.append(Util.formatHostId(link.src().elementId()));
            stringBuilder.append(" -> ");
        }
        stringBuilder.append(Util.formatHostId(this.dst().elementId()));
        return stringBuilder.toString();
    }

    public String id() {
        return String.valueOf(hashCode());
    }
}