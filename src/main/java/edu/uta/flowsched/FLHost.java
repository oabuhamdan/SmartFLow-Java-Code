package edu.uta.flowsched;

import org.onlab.packet.IpAddress;
import org.onlab.packet.MacAddress;
import org.onlab.packet.VlanId;
import org.onosproject.net.Annotations;
import org.onosproject.net.DefaultHost;
import org.onosproject.net.HostId;
import org.onosproject.net.HostLocation;
import org.onosproject.net.provider.ProviderId;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class FLHost extends DefaultHost {
    private String flClientID;
    private String flClientCID;
    private long lastPathChange;
    public final NetworkStats networkStats;
    private MyPath currentPath;
    private final AtomicInteger lastUsedPathPriority;

    public FLHost(ProviderId providerId, HostId id, MacAddress mac, VlanId vlan, HostLocation location, Set<IpAddress> ips
            , String flClientID, String flClientCID, Annotations... annotations) {
        super(providerId, id, mac, vlan, location, ips, annotations);
        this.flClientID = flClientID;
        this.flClientCID = flClientCID;
        this.networkStats = new NetworkStats();
        this.lastPathChange = System.currentTimeMillis();
        this.lastUsedPathPriority = new AtomicInteger(20);
    }


    public String getFlClientCID() {
        return flClientCID;
    }

    public void setFlClientCID(String flClientCID) {
        this.flClientCID = flClientCID;
    }

    public String getFlClientID() {
        return flClientID;
    }

    public void setFlClientID(String flClientID) {
        this.flClientID = flClientID;
    }


    @Override
    public String toString() {
        return "FLHost{" +
                "flClientID='" + flClientID + '\'' +
                ", flClientCID='" + flClientCID + '\'' +
                '}';
    }

    public long getLastPathChange() {
        return lastPathChange;
    }

    public void setLastPathChange(long lastPathChange) {
        this.lastPathChange = lastPathChange;
    }

    public void setCurrentPath(MyPath path) {
        this.currentPath = path;
    }

    public MyPath getCurrentPath() {
        return currentPath;
    }

    public int getAndIncrementPathPriority() {
        return lastUsedPathPriority.getAndIncrement();
    }

    public Set<FLHost> assignNewPath(MyPath newPath) {
        Set<FLHost> affectedClients = ConcurrentHashMap.newKeySet();
        if (this.currentPath != null)
            affectedClients.addAll(currentPath.removeClient(this));
        setLastPathChange(System.currentTimeMillis());
        setCurrentPath(newPath);
        affectedClients.addAll(newPath.addClient(this));
        return affectedClients;
    }

    public boolean clearPath() {
        if (this.currentPath == null) {
            return false;
        }
        currentPath.removeClient(this);
        setCurrentPath(null);
        return true;
    }

    public static class NetworkStats {
        private final BoundedConcurrentLinkedQueue<Long> lastPositiveTXRate;
        private final BoundedConcurrentLinkedQueue<Long> lastPositiveRXRate;

        private final BoundedConcurrentLinkedQueue<Long> lastPositiveTXData;
        private final BoundedConcurrentLinkedQueue<Long> lastPositiveRXData;
        private final BoundedConcurrentLinkedQueue<Long> lastTXRate;
        private final BoundedConcurrentLinkedQueue<Long> lastRXRate;
        private final AtomicLong roundSentData;
        private final AtomicLong roundReceivedData;

        public NetworkStats() {
            this.lastTXRate = new BoundedConcurrentLinkedQueue<>(3);
            this.lastRXRate = new BoundedConcurrentLinkedQueue<>(3);
            this.lastPositiveTXData = new BoundedConcurrentLinkedQueue<>(3);
            this.lastPositiveRXData = new BoundedConcurrentLinkedQueue<>(3);
            this.lastPositiveTXRate = new BoundedConcurrentLinkedQueue<>(3);
            this.lastPositiveRXRate = new BoundedConcurrentLinkedQueue<>(3);
            this.roundSentData = new AtomicLong(0);
            this.roundReceivedData = new AtomicLong(0);
        }

        public long getLastPositiveTXRate() {
            return Optional.ofNullable(this.lastPositiveTXRate.peekLast()).orElse(0L);
        }

        public long getLastPositiveRXRate() {
            return Optional.ofNullable(this.lastPositiveRXRate.peekLast()).orElse(0L);
        }


        public long getLastPositiveTXData() {
            return Optional.ofNullable(this.lastPositiveTXData.peekLast()).orElse(0L);
        }

        public long getLastPositiveRXData() {
            return Optional.ofNullable(this.lastPositiveRXData.peekLast()).orElse(0L);
        }

        public long getLastPositiveData(FlowDirection direction) {
            return direction.equals(FlowDirection.S2C) ? this.getLastPositiveRXData() : this.getLastPositiveTXData();
        }

        public long getLastRate(FlowDirection direction) {
            return direction.equals(FlowDirection.S2C) ? this.getLastRXRate() : this.getLastTXRate();
        }

        public long getLastPositiveRate(FlowDirection direction) {
            return direction.equals(FlowDirection.S2C) ? this.getLastPositiveRXRate() : this.getLastPositiveTXRate();
        }

        public long getRoundExchangedData(FlowDirection dir) {
            return dir.equals(FlowDirection.S2C) ? this.roundReceivedData.get() : this.roundSentData.get();
        }

        public long getLastTXRate() {
            return Optional.ofNullable(this.lastTXRate.peekLast()).orElse(0L);
//            return (long) Util.weightedAverage(lastTXRate, true);
        }

        public long getLastRXRate() {
            return Optional.ofNullable(this.lastRXRate.peekLast()).orElse(0L);
//            return (long) Util.weightedAverage(lastRXRate, true);
        }

        public void setLastPositiveTXRate(long lastPositiveTXRate) {
            this.lastPositiveTXRate.add(lastPositiveTXRate);
        }

        public void setLastPositiveRXRate(long lastPositiveRXRate) {
            this.lastPositiveRXRate.add(lastPositiveRXRate);
        }


        public void setLastPositiveTXData(long lastPositiveTXData) {
            this.lastPositiveTXData.add(lastPositiveTXData);
        }

        public void setLastPositiveRXData(long lastPositiveRXData) {
            this.lastPositiveRXData.add(lastPositiveRXData);
        }

        public void setLastTXRate(long lastTXRate) {
            this.lastTXRate.add(lastTXRate);
        }

        public void setLastRXRate(long lastRXRate) {
            this.lastRXRate.add(lastRXRate);
        }

        public void accumulateReceivedData(long value) {this.roundReceivedData.addAndGet(value);}

        public void accumulateSentData(long value) {
            this.roundSentData.addAndGet(value);
        }

        public void resetExchangedData(FlowDirection flowDirection) {
            if (flowDirection.equals(FlowDirection.S2C))
                this.roundReceivedData.set(0);
            else
                this.roundSentData.set(0);
        }
    }
}
