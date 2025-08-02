package edu.uta.flowsched;

import org.onlab.packet.IPv4;
import org.onlab.packet.IpPrefix;
import org.onlab.packet.TpPort;
import org.onosproject.net.Device;
import org.onosproject.net.flow.*;

public class PrioritizeZMQPackets {

    private void installZMQPriorityRules() {
        for (Device device : Services.deviceService.getAvailableDevices()) {
            TrafficSelector selector = DefaultTrafficSelector.builder()
                    .matchIPDst(IpPrefix.valueOf("10.0.0.250/32"))
                    .matchIPProtocol(IPv4.PROTOCOL_TCP)
                    .matchTcpDst(TpPort.tpPort(5555))
                    .build();

            // 2. Define treatment (set DSCP to 46)
            TrafficTreatment treatment = DefaultTrafficTreatment.builder()
                    .setIpDscp((byte) 46) // EF (Expedited Forwarding)
                    .build();

            // 3. Define the flow rule
            FlowRule flowRule = DefaultFlowRule.builder()
                    .forDevice(device.id())
                    .withSelector(selector)
                    .withTreatment(treatment)
                    .withPriority(40000)
                    .fromApp(Services.appId)
                    .makePermanent()
                    .build();

            // 4. Apply the rule
            Services.flowRuleService.applyFlowRules(flowRule);
        }
    }
}
