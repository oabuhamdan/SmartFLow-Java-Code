package edu.uta.flowsched;

import org.onlab.util.KryoNamespace;
import org.onosproject.net.HostId;
import org.onosproject.net.Path;
import org.onosproject.store.serializers.KryoNamespaces;
import org.onosproject.store.service.EventuallyConsistentMap;
import org.onosproject.store.service.WallClockTimestamp;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static edu.uta.flowsched.schedulers.CONFIGS.PATHS_LIMIT;

public class PathInformationDatabase {
    public static final PathInformationDatabase INSTANCE = new PathInformationDatabase();
    private static ExecutorService executorService;

    private EventuallyConsistentMap<HostId, Set<MyPath>> CLIENT_TO_SERVER_PATHS;
    private EventuallyConsistentMap<HostId, Set<MyPath>> SERVER_TO_CLIENT_PATHS;

    protected void activate() {
        KryoNamespace.Builder mySerializer = KryoNamespace.newBuilder().register(KryoNamespaces.API)
                .register(MyLink.class)
                .register(MyPath.class);

        CLIENT_TO_SERVER_PATHS = Services.storageService.<HostId, Set<MyPath>>eventuallyConsistentMapBuilder()
                .withName("CLIENT_TO_SERVER_PATHS")
                .withTimestampProvider((k, v) -> new WallClockTimestamp())
                .withSerializer(mySerializer).build();

        SERVER_TO_CLIENT_PATHS = Services.storageService.<HostId, Set<MyPath>>eventuallyConsistentMapBuilder()
                .withName("SERVER_TO_CLIENTS_PATHS")
                .withTimestampProvider((k, v) -> new WallClockTimestamp())
                .withSerializer(mySerializer).build();

        executorService = Executors.newCachedThreadPool();
    }

    protected void deactivate() {
        executorService.shutdownNow();
        CLIENT_TO_SERVER_PATHS.clear();
        SERVER_TO_CLIENT_PATHS.clear();
    }

    public Set<MyPath> getPathsToServer(FLHost host) {
        if (!CLIENT_TO_SERVER_PATHS.containsKey(host.id())) {
            setPathsToServer(host.id());
        }
        return CLIENT_TO_SERVER_PATHS.get(host.id());
    }

    public Set<MyPath> getPathsToClient(FLHost host) {
        if (!SERVER_TO_CLIENT_PATHS.containsKey(host.id())) {
            setPathsToClient(host.id());
        }
        return SERVER_TO_CLIENT_PATHS.get(host.id());
    }

    public Set<MyPath> getPaths(FLHost host, FlowDirection direction) {
        return FlowDirection.S2C.equals(direction) ? getPathsToClient(host) : getPathsToServer(host);
    }

    public void setPathsToServer(HostId hostId) {
        Set<MyPath> paths = Services.pathService.getKShortestPaths(hostId, HostId.hostId(Util.FL_SERVER_MAC))
                .limit(PATHS_LIMIT).map(MyPath::new)
                .collect(Collectors.toSet());

        PathRulesInstaller.INSTANCE.installPathRules(ClientInformationDatabase.INSTANCE.getHostByHostID(hostId).get(), (Path) paths.toArray()[0], true);
        CLIENT_TO_SERVER_PATHS.put(hostId, paths);
    }

    public void setPathsToClient(HostId hostId) {
        Set<MyPath> paths = Services.pathService.getKShortestPaths(HostId.hostId(Util.FL_SERVER_MAC), hostId)
                .limit(PATHS_LIMIT).map(MyPath::new)
                .collect(Collectors.toSet());

        PathRulesInstaller.INSTANCE.installPathRules(ClientInformationDatabase.INSTANCE.getHostByHostID(hostId).get(), (Path) paths.toArray()[0], true);
        SERVER_TO_CLIENT_PATHS.put(hostId, paths);
    }

    void printAll() {
        Util.log("paths", "***************Server to Clients Paths***************");
        for (FLHost host : ClientInformationDatabase.INSTANCE.getFLHosts()) {
            StringBuilder stringBuilder = new StringBuilder(String.format("***** Paths for Client %s ****\n", host.getFlClientCID()));
            getPathsToClient(host).forEach(myPath -> stringBuilder.append(myPath.format()).append("\n"));
            Util.log("paths", stringBuilder.toString());
        }
        Util.log("paths", "***************Client to Server Paths***************");
        for (FLHost host : ClientInformationDatabase.INSTANCE.getFLHosts()) {
            StringBuilder stringBuilder = new StringBuilder(String.format("***** Paths for Client %s ****\n", host.getFlClientCID()));
            getPathsToServer(host).forEach(myPath -> stringBuilder.append(myPath.format()).append("\n"));
            Util.log("paths", stringBuilder.toString());
        }
    }
}
