package edu.uta.flowsched;


import org.onlab.util.KryoNamespace;
import org.onosproject.net.ConnectPoint;
import org.onosproject.net.DeviceId;
import org.onosproject.net.Link;
import org.onosproject.net.device.DeviceEvent;
import org.onosproject.net.device.DeviceListener;
import org.onosproject.net.device.PortStatistics;
import org.onosproject.store.serializers.KryoNamespaces;
import org.onosproject.store.service.EventuallyConsistentMap;
import org.onosproject.store.service.WallClockTimestamp;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static org.onosproject.net.device.DeviceEvent.Type.PORT_STATS_UPDATED;

public class LinkInformationDatabase {

    public static final LinkInformationDatabase INSTANCE = new LinkInformationDatabase();
    private EventuallyConsistentMap<Link, MyLink> LINK_INFORMATION_MAP;
    private LinkThroughputWatcher linkThroughputWatcher;

    private ScheduledExecutorService executor;

    protected void activate() {
        linkThroughputWatcher = new LinkThroughputWatcher();
        executor = Executors.newSingleThreadScheduledExecutor();

        Services.deviceService.addListener(linkThroughputWatcher);
        KryoNamespace.Builder mySerializer = KryoNamespace.newBuilder().register(KryoNamespaces.API)
                .register(Link.class)
                .register(ConnectPoint.class)
                .register(MyPath.class)
                .register(MyLink.class);

        LINK_INFORMATION_MAP = Services.storageService.<Link, MyLink>eventuallyConsistentMapBuilder()
                .withName("mylink_info_map")
                .withTimestampProvider((k, v) -> new WallClockTimestamp())
                .withSerializer(mySerializer).build();

        for (Link link : Services.linkService.getLinks()) {
            LINK_INFORMATION_MAP.put(link, new MyLink(link));
        }
    }

    protected void deactivate() {
        Services.deviceService.removeListener(linkThroughputWatcher);
        LINK_INFORMATION_MAP.clear();
        executor.shutdownNow();
    }

    public void refreshComponent() {
        deactivate();
        activate();
    }

    public void updateDeviceLinksUtilization(Set<Link> links, long rate) {
        links.forEach(link -> {
            if (LINK_INFORMATION_MAP.containsKey(link))
                LINK_INFORMATION_MAP.get(link).setCurrentThroughput(rate);
        });
    }

    public MyLink getLinkInformation(Link link) {
        MyLink myLink = LINK_INFORMATION_MAP.get(link);
        if (myLink == null) {
            myLink = new MyLink(link);
            LINK_INFORMATION_MAP.put(link, myLink);
        }
        return myLink;
    }
    public MyLink getLinkInverse(Link link) {
        Link inverse = Services.linkService.getLink(link.dst(), link.src());
        return getLinkInformation(inverse);
    }

    public HashSet<MyLink> getAllLinkInformation() {
        return new HashSet<>(LINK_INFORMATION_MAP.values());
    }

    public Set<MyLink> allLinksNoEdge() {
        return LINK_INFORMATION_MAP.values().stream()
                .filter(myLink -> !myLink.type().equals(Link.Type.EDGE))
                .collect(Collectors.toSet());
    }

    private class LinkThroughputWatcher implements DeviceListener {
        @Override
        public void event(DeviceEvent event) {
            try {
                DeviceEvent.Type type = event.type();
                DeviceId deviceId = event.subject().id();
                if (type == PORT_STATS_UPDATED) {
                    List<PortStatistics> portStatisticsList = Services.deviceService.getPortDeltaStatistics(deviceId);
                    portStatisticsList.forEach(portStatistics -> {
                        long receivedRate = (portStatistics.bytesReceived() * 8) / Util.POLL_FREQ;
                        long sentRate = (portStatistics.bytesSent() * 8) / Util.POLL_FREQ;
                        Set<Link> ingressLinks = Services.linkService.getIngressLinks(new ConnectPoint(deviceId, portStatistics.portNumber()));
                        updateDeviceLinksUtilization(ingressLinks, receivedRate);
                        Set<Link> egressLinks = Services.linkService.getEgressLinks(new ConnectPoint(deviceId, portStatistics.portNumber()));
                        updateDeviceLinksUtilization(egressLinks, sentRate);
                    });
                }
            } catch (Exception e) {
                Util.log("general", "Error inside LinkThroughputWatcher..." + Arrays.toString(e.getStackTrace()));
            }
        }
    }
}