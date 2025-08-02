package edu.uta.flowsched;


import org.onlab.packet.MacAddress;
import org.onlab.packet.VlanId;
import org.onosproject.net.ElementId;
import org.onosproject.net.HostId;
import org.onosproject.net.HostLocation;
import org.onosproject.net.Link;
import org.onosproject.net.provider.ProviderId;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;


public class Util {
    public static final int POLL_FREQ = Integer.parseInt(System.getenv("POLL_FREQ"));
    public static final MacAddress FL_SERVER_MAC = MacAddress.valueOf("00:00:00:00:00:FA");
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(Util.class);
    final static ConcurrentHashMap<String, BufferedWriter> LOGGERS = new ConcurrentHashMap<>();
    public static DateTimeFormatter LOG_TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");
    public static FLHost POISON_CLIENT = new FLHost(ProviderId.NONE, HostId.NONE, MacAddress.NONE, VlanId.NONE, HostLocation.NONE, Collections.emptySet(), "", "");

    public static String formatHostId(ElementId elementId) {
        if (elementId instanceof HostId) {
            HostId dst = (HostId) elementId;
            if (dst.mac() == Util.FL_SERVER_MAC)
                return "FLServer";
            else {
                Optional<FLHost> host = ClientInformationDatabase.INSTANCE.getHostByHostID(dst);
                return "FL#" + host.map(FLHost::getFlClientCID).orElse(dst.toString().substring(15));
            }
        } else { // Switch
            return elementId.toString().substring(15);
        }
    }

    public static double weightedAverage(Collection<? extends Number> collection, boolean mostRecentFirst) {
        int size = collection.size();
        double weightedSum = 0.0;
        double totalWeight = 0.0;
        Iterator<? extends Number> iterator = collection.iterator();
        Function<Integer, Integer> weightFun = mostRecentFirst ? i -> i : i -> size - i;
        int i = 0;
        while (iterator.hasNext()) {
            double weight = Math.pow(3, weightFun.apply(i));
            Number value = iterator.next();
            weightedSum += value.doubleValue() * weight;
            totalWeight += weight;
            i++;
        }

        return totalWeight == 0 ? 0 : weightedSum / totalWeight;
    }

    public static long bitToMbit(Number num) {
        if (num == null)
            return 0;
        return num.longValue() / 1_000_000;
    }

    public static long getAgeInSeconds(long savedTimeMillis) {
        long currentTimeMillis = System.currentTimeMillis();
        long timeDifferenceMillis = currentTimeMillis - savedTimeMillis;
        return timeDifferenceMillis / 1000;
    }

    public static String formatLink(Link link) {
        if (link == null) {
            return "";
        }
        final String LINK_STRING_FORMAT = "%s -> %s";
        String src = link.src().elementId().toString().substring(15);
        String dst = link.dst().elementId().toString().substring(15);
        return String.format(LINK_STRING_FORMAT, src, dst);
    }

    public static void log(String loggerNames, String message) {
        String[] loggers = loggerNames.split(",");
        for (String loggerName : loggers) {
            try {
                BufferedWriter logger = LOGGERS.computeIfAbsent(loggerName, name -> {
                    try {
                        return new BufferedWriter(new FileWriter(String.format("/home/osama/flow_sched_logs/%s.log", name)));
                    } catch (IOException e) {
                        LOGGER.error("Error while creating file writer for logger: " + name, e);
                        return null;
                    }
                });

                if (logger == null) {
                    continue; // If we failed to create the logger, skip this logger
                }
                logger.write(message + "\n");
            } catch (Exception e) {
                LOGGER.error("Error While Logging: " + loggerName + " => " + e.getMessage(), e);
            }
        }
    }

    public static void flushWriters() {
        LOGGERS.forEach((s, bufferedWriter) -> {
            try {
                bufferedWriter.flush();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    static void closeWriters() {
        LOGGERS.forEach((s, bufferedWriter) -> {
            try {
                bufferedWriter.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public static String formatMessage(int level, String format, Object... args) {
        String tabs = "\t".repeat(Math.max(0, level));
        String message = String.format(format, args);
        return tabs + message + "\n";
    }
}