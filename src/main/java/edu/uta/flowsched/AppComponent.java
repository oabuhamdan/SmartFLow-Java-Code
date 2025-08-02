/*
 * Copyright 2024-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uta.flowsched;

import edu.uta.flowsched.schedulers.SmartFlowScheduler;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Skeletal ONOS application component.
 */

// TODO: Try packet dropping
@Component(immediate = true,
        service = {AppComponent.class})
public class AppComponent {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    @Activate
    protected void activate() {
        Services.cfgService.setProperty("org.onosproject.provider.of.device.impl.OpenFlowDeviceProvider", "portStatsPollFrequency", System.getenv("POLL_FREQ"));
        createLogDir();
        LinkInformationDatabase.INSTANCE.activate();
        PathInformationDatabase.INSTANCE.activate();
        ClientInformationDatabase.INSTANCE.activate();
        LinkLatencyProbeComponent.INSTANCE.activate();
        SmartFlowScheduler.activate();
        ZeroMQServer.INSTANCE.activate();
        executor.scheduleAtFixedRate(Util::flushWriters, 10, 10, TimeUnit.SECONDS);
        log.info("Started");
    }

    @Deactivate
    protected void deactivate() {
        LinkInformationDatabase.INSTANCE.deactivate();
        PathInformationDatabase.INSTANCE.deactivate();
        ZeroMQServer.INSTANCE.deactivate();
        ClientInformationDatabase.INSTANCE.deactivate();
        SmartFlowScheduler.deactivate();
        LinkLatencyProbeComponent.INSTANCE.deactivate();
        Util.closeWriters();
        log.info("Stopped");
    }

    private void createLogDir() {
        String logDirectory = "/home/osama/flow_sched_logs/";
        File directory = new File(logDirectory);
        if (!directory.exists()) {
            boolean dirCreated = directory.mkdirs();
            if (dirCreated) {
                log.info("Log directory created.");
            } else {
                log.error("Failed to create log directory.");
            }
        } else {
            log.info("Log directory already exists.");
        }
    }

}
