package edu.uta.flowsched;

import edu.uta.flowsched.schedulers.SmartFlowScheduler;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.onlab.packet.MacAddress;
import org.onosproject.net.HostId;
import org.zeromq.ZMQ;

import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ZeroMQServer {

    public static ZeroMQServer INSTANCE = new ZeroMQServer();
    private static final ExecutorService executorService = Executors.newCachedThreadPool();
    private static final ClientInformationDatabase clientInformationDatabase = ClientInformationDatabase.INSTANCE;
    private static final PathInformationDatabase pathInformationDatabase = PathInformationDatabase.INSTANCE;
    ZMQ.Socket socket;
    ZMQ.Context context;

    void activate() {
        context = ZMQ.context(1);
        socket = context.socket(ZMQ.PULL);
        String ip = System.getenv("ONOS_IP");
        socket.bind(String.format("tcp://%s:5555", ip));
        executorService.submit(this::listener);
    }

    private void listener() {
        while (!context.isClosed()) {
            String message = new String(socket.recv(0));
            executorService.submit(() -> handleMessage(message));
        }
    }

    private void handleMessage(String message) {
        try {
            JSONObject jsonObject = new JSONObject(message);
            MessageType messageType = MessageType.fromId(jsonObject.getInt("message_type"));
            long clientTimeMS = jsonObject.getLong("time_ms");
            long serverTimeMS = System.currentTimeMillis();
            Object jsonMessage = jsonObject.get("message");
            Util.log("overhead", String.format("zmq,%s,%s", messageType, serverTimeMS - clientTimeMS));
            if (messageType == MessageType.UPDATE_DIRECTORY) {
                JSONObject clientInfo = (JSONObject) jsonMessage;
                updateDirectory(clientInfo);
            } else if (messageType == MessageType.SERVER_TO_CLIENTS) {
                JSONArray clients = (JSONArray) jsonMessage;
                handleServerToClientsPaths(clients);
            } else if (messageType == MessageType.CLIENT_TO_SERVER) {
                String clientCID = String.valueOf(jsonObject.get("sender_id"));
                handleClientToServerPath(clientCID, clientTimeMS);
            }
        } catch (Exception e) {
            Util.log("general", String.format("Error processing message: %s", e.getMessage()));
            Util.log("general", String.format("%s", (Object[]) e.getStackTrace()));
        }
    }

    private void updateDirectory(JSONObject clientInfo) throws JSONException {
        long tik = System.currentTimeMillis();
        MacAddress macAddress = MacAddress.valueOf(clientInfo.getString("mac"));
        String flClientID = clientInfo.getString("node_id");
        String flClientCID = clientInfo.getString("client_cid");
        clientInformationDatabase.updateHost(macAddress, flClientID, flClientCID);
        HostId hostId = HostId.hostId(macAddress);
        PathInformationDatabase.INSTANCE.setPathsToClient(hostId);
        PathInformationDatabase.INSTANCE.setPathsToServer(hostId);
        Util.log("overhead", String.format("controller,update_directory,%s", System.currentTimeMillis() - tik));
    }

    private void handleClientToServerPath(String clientID, long time) {
        FLHost host = clientInformationDatabase.getHostByFLCID(clientID);
        SmartFlowScheduler.C2S.addClientToQueue(host);
    }

    private void handleServerToClientsPaths(JSONArray clients) throws JSONException {
        HashSet<String> hashSet = new HashSet<>();
        for (int i = 0; i < clients.length(); i++) {
            hashSet.add(clients.getString(i));
        }
        List<FLHost> hosts = clientInformationDatabase.getHostsByFLIDs(hashSet);
        SmartFlowScheduler.S2C.addClientsToQueue(hosts);
        SmartFlowScheduler.startRound();
    }

    void deactivate() {
        executorService.shutdownNow();
        context.term();
        context.close();
        socket.close();
    }

    enum MessageType {
        UPDATE_DIRECTORY(1),
        SERVER_TO_CLIENTS(2),
        CLIENT_TO_SERVER(3);

        MessageType(int id) {
            this.id = id;
        }

        public static MessageType fromId(int id) {
            for (MessageType type : MessageType.values()) {
                if (type.id == id) {
                    return type;
                }
            }
            throw new IllegalArgumentException("No MessageType with id " + id);
        }

        private final int id;
    }
}
