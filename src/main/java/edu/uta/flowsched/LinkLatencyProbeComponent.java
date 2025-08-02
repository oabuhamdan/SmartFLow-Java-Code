package edu.uta.flowsched;

import org.onlab.packet.Data;
import org.onlab.packet.Ethernet;
import org.onosproject.net.DeviceId;
import org.onosproject.net.Link;
import org.onosproject.net.flow.DefaultTrafficSelector;
import org.onosproject.net.flow.DefaultTrafficTreatment;
import org.onosproject.net.flow.TrafficSelector;
import org.onosproject.net.flow.TrafficTreatment;
import org.onosproject.net.packet.DefaultOutboundPacket;
import org.onosproject.net.packet.PacketContext;
import org.onosproject.net.packet.PacketPriority;
import org.onosproject.net.packet.PacketProcessor;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.*;

import static edu.uta.flowsched.Services.appId;
import static edu.uta.flowsched.Services.packetService;


public class LinkLatencyProbeComponent {
    private static final String PROBE_SPLITTER = ";";
    private static final short PROBE_ETHERTYPE = 0x3366;
    private static final String PROBE_SRC = "20:15:08:10:00:05";
    private static final String PROBE_DST = "20:15:08:10:00:09";
    private static final int NUMBER_PROBE_PACKETS = 50;
    private ScheduledExecutorService probeScheduler;
    private ExecutorService probeWorker;
    private MaoLinkProbeReceiver linkProbeReceiver;
    private final Map<String, MyLink> linkIDs = new ConcurrentHashMap<>();
    private final Map<String, Integer> linkPacketSequence = new ConcurrentHashMap<>();
    private final Map<MyLink, BoundedConcurrentLinkedQueue<PacketSeqTimeStamp>> linkPackets = new ConcurrentHashMap<>();
    public static final LinkLatencyProbeComponent INSTANCE = new LinkLatencyProbeComponent();

    public void activate() {
        Util.log("link_csv", "timestamp,link,throughput,latency,loss");
        Set<MyLink> localLinks = LinkInformationDatabase.INSTANCE.allLinksNoEdge();
        localLinks.forEach(link -> {
            linkIDs.put(link.id(), link);
            linkPackets.put(link, new BoundedConcurrentLinkedQueue<>(NUMBER_PROBE_PACKETS));
            linkPacketSequence.put(link.id(), 0);
        });

        linkProbeReceiver = new MaoLinkProbeReceiver();
        packetService.addProcessor(linkProbeReceiver, PacketProcessor.advisor(1));
        requestPushPacket();

        probeWorker = Executors.newFixedThreadPool(10);
        probeScheduler = Executors.newSingleThreadScheduledExecutor();
        probeScheduler.scheduleWithFixedDelay(this::sendPacketProbes, 0, Math.max(Util.POLL_FREQ, 5), TimeUnit.SECONDS);
    }

    public void deactivate() {
        probeScheduler.shutdown();
        probeWorker.shutdown();
        cancelPushPacket();
        packetService.removeProcessor(linkProbeReceiver);
    }

    private void requestPushPacket() {
        TrafficSelector selector = DefaultTrafficSelector.builder().matchEthType(PROBE_ETHERTYPE).build();
        packetService.requestPackets(selector, PacketPriority.HIGH, appId);
    }

    private void cancelPushPacket() {
        TrafficSelector selector = DefaultTrafficSelector.builder().matchEthType(PROBE_ETHERTYPE).build();
        packetService.cancelPackets(selector, PacketPriority.HIGH, appId);
    }

    private void sendPacketProbes() {
        for (MyLink link : linkIDs.values()) {
            probeWorker.submit(() -> probeOneLine(link));
        }
    }
    private void probeOneLine(MyLink link) {
        try {
            DeviceId deviceId = link.src().deviceId();
            TrafficTreatment treatmentAll = DefaultTrafficTreatment.builder().setOutput(link.src().port()).build();
            Ethernet probePkt = new Ethernet();
            probePkt.setDestinationMACAddress(PROBE_DST);
            probePkt.setSourceMACAddress(PROBE_SRC);
            probePkt.setEtherType(PROBE_ETHERTYPE);

            int startSeq = linkPacketSequence.get(link.id());
            for (int i = startSeq; i < startSeq + NUMBER_PROBE_PACKETS; i++) {
                byte[] probeData = (i + PROBE_SPLITTER + link.id() + PROBE_SPLITTER + System.currentTimeMillis()).getBytes();
                probePkt.setPayload(new Data(probeData));
                packetService.emit(new DefaultOutboundPacket(deviceId, treatmentAll, ByteBuffer.wrap(probePkt.serialize())));
            }
            linkPacketSequence.computeIfPresent(link.id(), (k, v) -> NUMBER_PROBE_PACKETS + v);
            Thread.sleep(100);
            updateLinkStats(link);
        } catch (Exception e) {
            Util.log("general", "Error: " + e.getMessage() + "...." + Arrays.toString(Arrays.stream(e.getStackTrace()).toArray()));
        }
    }

    private void updateLinkStats(MyLink link) {
        StringBuilder builder = new StringBuilder();
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss");
        var packets = linkPackets.get(link);
        if (packets.isEmpty()) {
            Util.log("general", String.format("Link %s doesn't have packets", link.format()));
            link.setLatency(4);
            link.setPacketLoss(0);
            return;
        }
        double packetLoss = getPacketLoss(packets);
        packetLoss = Math.max(0.0, Math.min(1.0, packetLoss));
        int averageLatency = (int) packets.stream().mapToLong(p -> p.latency).average().orElse(0.0);
        link.setLatency(getScaledValue(averageLatency));
        link.setPacketLoss(packetLoss);
        builder.append(String.format("%s,%s,%s,%s,%.2f\n", now.format(formatter), link.format(), link.getThroughput(), averageLatency, packetLoss));
        Util.log("link_csv", builder.toString());
    }

    private double getPacketLoss(BoundedConcurrentLinkedQueue<PacketSeqTimeStamp> packets) {
        int size = packets.size();
        Optional<PacketSeqTimeStamp> lastPacket = Optional.ofNullable(packets.peekLast());
        Optional<PacketSeqTimeStamp> firstPacket = Optional.ofNullable(packets.peekFirst());
        int diff = lastPacket.map(p -> p.sequence).orElse(0) - firstPacket.map(p -> p.sequence).orElse(0) + 1;
        return 1 - (size * 1.0 / diff);
    }

    private int getScaledValue(long value){
        if (value <= 50)
            return 2;
        if (value <= 150)
            return 3;
        if (value <= 250)
            return 4;
        if (value <= 400)
            return 5;
        if (value <= 500)
            return 6;
        else
            return 7;
    }
    static class PacketSeqTimeStamp {
        int sequence;
        long latency;

        public PacketSeqTimeStamp(int sequence, long latency) {
            this.sequence = sequence;
            this.latency = latency;
        }
    }

    private class MaoLinkProbeReceiver implements PacketProcessor {
        @Override
        public void process(PacketContext context) {
            if (context.isHandled()) {
                return;
            }
            try {
                Ethernet pkt = context.inPacket().parsed();
                if (pkt.getEtherType() == PROBE_ETHERTYPE) {
                    byte[] probePacket = pkt.getPayload().serialize();
                    String[] packet = new String(probePacket).split(PROBE_SPLITTER);
                    int seq = Integer.parseInt(packet[0]);
                    String linkId = packet[1];
                    long timestamp = Long.parseLong(packet[2]);
                    long latency = System.currentTimeMillis() - timestamp;
                    Link link = linkIDs.getOrDefault(linkId, null);
                    if (link != null)
                        linkPackets.get(link).add(new PacketSeqTimeStamp(seq, latency));
                    context.block();
                }
            } catch (Exception e) {
                Util.log("general", "Error: " + e.getMessage() + "...." + Arrays.toString(Arrays.stream(e.getStackTrace()).toArray()));
            }
        }
    }
}

// C. Metter, V. Burger, Z. Hu, K. Pei and F. Wamser, "Towards an Active Probing Extension for the ONOS SDN Controller," 2018 28th International Telecommunication Networks and Applications Conference (ITNAC)