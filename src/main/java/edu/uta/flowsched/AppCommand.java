package edu.uta.flowsched;

import org.apache.karaf.shell.api.action.Argument;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.lifecycle.Service;
import org.onosproject.cli.AbstractShellCommand;
import org.onosproject.net.DeviceId;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Sample Apache Karaf CLI command.
 */
@Service
@Command(scope = "onos", name = "flowsched")
public class AppCommand extends AbstractShellCommand {
    @Argument(name = "type", description = "type of stats to print", required = true)
    String type = "";
    @Argument(name = "deviceid", description = "type of stats to print", required = false, index = 1)
    String deviceID = "";
    static ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(0);
    static ScheduledFuture<?> scheduledFuture;

    @Override
    protected void doExecute() {
        switch (type) {
            case "link-info":
                Util.log("link_util.csv", "link,free,used");
                scheduledFuture = scheduledExecutorService.scheduleAtFixedRate(this::linkInfo, Util.POLL_FREQ, Util.POLL_FREQ, TimeUnit.SECONDS);
                break;
            case "stop-link-info":
                if (scheduledFuture != null) {
                    scheduledFuture.cancel(true);
                    scheduledFuture = null;
                }
                break;
            case "meter":
                addMeter(DeviceId.deviceId(deviceID));
                break;
            case "refresh-links":
                LinkInformationDatabase.INSTANCE.refreshComponent();
                break;
            case "paths":
                PathInformationDatabase.INSTANCE.printAll();
        }
    }

    private void addMeter(DeviceId deviceId) {
//        ReserveCapacity.call(deviceId);
        print("Reserved Capacity Successfully");
    }

    void linkInfo() {
        for (MyLink link : LinkInformationDatabase.INSTANCE.getAllLinkInformation()) {
            Util.log("link_util.csv", String.format("%s,%s,%s", Util.formatLink(link), link.getEstimatedFreeCapacity(), link.getThroughput()));
        }
        Util.log("link_util.csv", ",,");
    }

}
