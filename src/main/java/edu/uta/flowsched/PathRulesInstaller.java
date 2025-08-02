package edu.uta.flowsched;

import org.onlab.packet.IPv4;
import org.onosproject.net.DeviceId;
import org.onosproject.net.Link;
import org.onosproject.net.Path;
import org.onosproject.net.PortNumber;
import org.onosproject.net.flow.*;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.onlab.packet.Ethernet.TYPE_IPV4;

public class PathRulesInstaller {
    public static final PathRulesInstaller INSTANCE = new PathRulesInstaller();
    private static final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    public void installPathRules(FLHost flHost, Path path, boolean permanent) {
        List<FlowRule> rules = new LinkedList<>();
        List<Link> links = path.links();
        PortNumber outPort;
        DeviceId deviceId;
        PortNumber inPort = links.get(0).dst().port();
        for (int i = 1; i < links.size(); i++) {
            outPort = links.get(i).src().port();
            deviceId = links.get(i).src().deviceId();
            rules.add(getFlowEntry(deviceId, flHost, outPort, inPort, path, permanent));
            inPort = links.get(i).dst().port();
        }
        Services.flowRuleService.applyFlowRules(rules.toArray(new FlowRule[0]));
    }


    private FlowRule getFlowEntry(DeviceId dId, FLHost host, PortNumber outPort, PortNumber inPort, Path path, boolean permanent) {
        TrafficTreatment treatment = DefaultTrafficTreatment.builder()
                .setOutput(outPort)
                .build();

        TrafficSelector.Builder selector = DefaultTrafficSelector.builder()
                .matchInPort(inPort)
                .matchEthDst(path.dst().hostId().mac())
                .matchEthSrc(path.src().hostId().mac())
                .matchEthType(TYPE_IPV4)
                .matchIPProtocol(IPv4.PROTOCOL_TCP);


        FlowRule.Builder ruleBuilder = DefaultFlowRule.builder()
                .withTreatment(treatment)
                .withSelector(selector.build())
                .forDevice(dId)
                .withPriority(host.getAndIncrementPathPriority())
                .fromApp(Services.appId);

        if (permanent)
            ruleBuilder.makePermanent();
        else
            ruleBuilder.makeTemporary(10);

        return ruleBuilder.build();
    }
}