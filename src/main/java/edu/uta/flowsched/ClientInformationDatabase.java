package edu.uta.flowsched;

import org.onlab.packet.MacAddress;
import org.onlab.util.KryoNamespace;
import org.onosproject.net.Host;
import org.onosproject.net.HostId;
import org.onosproject.net.HostLocation;
import org.onosproject.net.device.DeviceEvent;
import org.onosproject.net.device.DeviceListener;
import org.onosproject.net.device.PortStatistics;
import org.onosproject.store.serializers.KryoNamespaces;
import org.onosproject.store.service.EventuallyConsistentMap;
import org.onosproject.store.service.WallClockTimestamp;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.onosproject.net.device.DeviceEvent.Type.PORT_STATS_UPDATED;

public class ClientInformationDatabase {
    public static final ClientInformationDatabase INSTANCE = new ClientInformationDatabase();
    private EventuallyConsistentMap<Host, FLHost> FL_HOSTS;
    private PortThroughputWatcher portThroughputWatcher;

    protected void activate() {
        KryoNamespace.Builder mySerializer = KryoNamespace.newBuilder().register(KryoNamespaces.API).register(FLHost.class);
        portThroughputWatcher = new PortThroughputWatcher();
        Services.deviceService.addListener(portThroughputWatcher);

        FL_HOSTS = Services.storageService.<Host, FLHost>eventuallyConsistentMapBuilder()
                .withName("FL_HOSTS")
                .withTimestampProvider((k, v) -> new WallClockTimestamp())
                .withSerializer(mySerializer).build();
    }

    public FLHost updateHost(MacAddress mac, String flClientID, String flClientCID) {
        HostId hostId = HostId.hostId(mac);
        Host host = Services.hostService.getHost(hostId);
        FLHost flHost;
        if (FL_HOSTS.containsKey(host)) {
            flHost = FL_HOSTS.get(host);
            flHost.setFlClientCID(flClientCID);
            flHost.setFlClientID(flClientID);
        } else {
            flHost = new FLHost(host.providerId(), host.id(), host.mac(), host.vlan(), host.location(), host.ipAddresses(), flClientID, flClientCID
                    , host.annotations());
            FL_HOSTS.put(host, flHost);
        }
        return flHost;
    }

    public FLHost getHostByFLCID(String FLCID) {
        return FL_HOSTS.values().stream().filter(flHost -> FLCID.equals(flHost.getFlClientCID())).findFirst().orElseThrow();
    }

    public Optional<FLHost> getHostByHostID(HostId hostId) {
        return Optional.ofNullable(FL_HOSTS.get(Services.hostService.getHost(hostId)));
    }

    public List<FLHost> getHostsByFLIDs(Set<String> FLIDs) {
        return FL_HOSTS.values().stream().filter(flHost -> FLIDs.contains(flHost.getFlClientID())).collect(Collectors.toList());
    }

    protected void deactivate() {
        FL_HOSTS.clear();
        Services.deviceService.removeListener(portThroughputWatcher);
    }

    public Collection<FLHost> getFLHosts() {
        return Collections.unmodifiableCollection(FL_HOSTS.values());
    }

    public int getTotalFLClients() {
        return FL_HOSTS.size();
    }

    private class PortThroughputWatcher implements DeviceListener {
        AtomicInteger currentCount = new AtomicInteger(0);
        long threshold = (long) 1e5; // 100Kb
        int deviceCount = Services.deviceService.getDeviceCount();

        @Override
        public void event(DeviceEvent event) {
            DeviceEvent.Type type = event.type();
            try {
                if (type == PORT_STATS_UPDATED) {
                    if (currentCount.incrementAndGet() >= deviceCount) {
                        currentCount.set(0);
                        updateDeviceLinksUtilization();
                    }
                }
            } catch (Exception e) {
                Util.log("general", "Error inside PortThroughputWatcher..." + Arrays.toString(e.getStackTrace()));
            }
        }

        private void updateDeviceLinksUtilization() {
            getFLHosts().forEach(flHost -> {
                HostLocation location = flHost.location();
                PortStatistics portStatistics = Services.deviceService.getDeltaStatisticsForPort(location.deviceId(), location.port());
                long receivedBits = portStatistics.bytesReceived() * 8;
                long sentBits = portStatistics.bytesSent() * 8;
                long receivedRate = receivedBits / Util.POLL_FREQ;
                long sentRate = sentBits / Util.POLL_FREQ;

                flHost.networkStats.setLastRXRate(sentRate); // RX Rate for Host is the TX Rate for Port
                flHost.networkStats.setLastTXRate(receivedRate); // TX Rate for Host is the RX Rate for Port
                flHost.networkStats.accumulateReceivedData(sentBits);
                flHost.networkStats.accumulateSentData(receivedBits);

                if (sentRate > threshold) {
                    flHost.networkStats.setLastPositiveRXRate(sentRate);
                    flHost.networkStats.setLastPositiveRXData(sentBits);
                }
                if (receivedRate > threshold) {
                    flHost.networkStats.setLastPositiveTXRate(receivedRate);
                    flHost.networkStats.setLastPositiveTXData(receivedBits);
                }
            });
        }
    }
}
