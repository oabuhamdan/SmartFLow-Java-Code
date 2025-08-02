package edu.uta.flowsched;

import org.onlab.osgi.DefaultServiceDirectory;
import org.onosproject.cfg.ComponentConfigService;
import org.onosproject.cluster.ClusterService;
import org.onosproject.core.ApplicationId;
import org.onosproject.core.CoreService;
import org.onosproject.net.config.NetworkConfigService;
import org.onosproject.net.device.DeviceService;
import org.onosproject.net.driver.DriverService;
import org.onosproject.net.edge.EdgePortService;
import org.onosproject.net.flow.FlowRuleService;
import org.onosproject.net.flowobjective.FlowObjectiveService;
import org.onosproject.net.host.HostService;
import org.onosproject.net.intent.IntentService;
import org.onosproject.net.link.LinkService;
import org.onosproject.net.meter.MeterService;
import org.onosproject.net.packet.PacketService;
import org.onosproject.net.statistic.FlowStatisticService;
import org.onosproject.net.topology.PathService;
import org.onosproject.net.topology.TopologyService;
import org.onosproject.store.service.StorageService;

public class Services {
    public static CoreService coreService = DefaultServiceDirectory.getService(CoreService.class);
    public static ApplicationId appId = coreService.registerApplication("edu.uta.flowsched");
    public static LinkService linkService = DefaultServiceDirectory.getService(LinkService.class);
    public static DeviceService deviceService = DefaultServiceDirectory.getService(DeviceService.class);
    public static FlowRuleService flowRuleService = DefaultServiceDirectory.getService(FlowRuleService.class);
    public static FlowStatisticService flowStatsService = DefaultServiceDirectory.getService(FlowStatisticService.class);
    public static HostService hostService = DefaultServiceDirectory.getService(HostService.class);
    public static NetworkConfigService configService = DefaultServiceDirectory.getService(NetworkConfigService.class);
    public static ComponentConfigService cfgService = DefaultServiceDirectory.getService(ComponentConfigService.class);
    public static PacketService packetService = DefaultServiceDirectory.getService(PacketService.class);
    public static FlowObjectiveService flowObjectiveService = DefaultServiceDirectory.getService(FlowObjectiveService.class);
    //    public static OpenFlowController openFlowControllerService = DefaultServiceDirectory.getService(OpenFlowController.class);
    public static StorageService storageService = DefaultServiceDirectory.getService(StorageService.class);
    public static PathService pathService = DefaultServiceDirectory.getService(PathService.class);
    public static ClusterService clusterService = DefaultServiceDirectory.getService(ClusterService.class);
    public static IntentService intentService = DefaultServiceDirectory.getService(IntentService.class);
    public static TopologyService topologyService = DefaultServiceDirectory.getService(TopologyService.class);
    public static EdgePortService edgePortService = DefaultServiceDirectory.getService(EdgePortService.class);
    public static DriverService driverService = DefaultServiceDirectory.getService(DriverService.class);
    public static MeterService meterService = DefaultServiceDirectory.getService(MeterService.class);
}