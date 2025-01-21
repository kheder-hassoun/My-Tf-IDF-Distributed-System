import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.util.List;
import java.util.Scanner;
import java.util.TreeMap;

public class Application implements Watcher {
    private static final Logger logger = LoggerFactory.getLogger(Application.class);
    private static final String address = "192.168.10.133:2181";
    private static final int SESSION_TIMEOUT = 3000; //dead client
    private static final int DEFAULT_PORT = 8080;
    private ZooKeeper zooKeeper;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        System.out.println("Enter Port  **this port number that the leader node will use to communicate with the " +
                "worker nodes.\n" +
                "\n **");
        Scanner scanner = new Scanner(System.in);
        int currentServerPort = scanner.nextInt();

        Application application = new Application();
        ZooKeeper zooKeeper = application.connectToZookeeper();

        ServiceRegistry serviceRegistry = new ServiceRegistry(zooKeeper);
        OnElectionAction onElectionAction = new OnElectionAction(serviceRegistry, currentServerPort);

        LeaderElection leaderElection = new LeaderElection(zooKeeper, onElectionAction);
        leaderElection.volunteerForLeadership();
        leaderElection.reelectLeader();

        RestTemplate restTemplate = new RestTemplate();
        String leaderUrl = "http://localhost:8080/leader/start";

        // Loop until a worker becomes available
        while (true) {
            List<String> serviceAddresses = ServiceRegistry.getAllServiceAddresses();
            if (serviceAddresses != null && !serviceAddresses.isEmpty()) {
                break;
            }
            System.out.println("No workers available. Waiting for 5 seconds...");
            Thread.sleep(5000);
        }

        // Send the search query to the leader
        try {
            ResponseEntity<TreeMap> response = restTemplate.postForEntity(leaderUrl, "fast food", TreeMap.class);
            TreeMap<String, Double> results = response.getBody();
            System.out.println("Search results: " + results);
        } catch (Exception e) {
            System.err.println("Failed to send query to leader: " + e.getMessage());
        }

        application.run();
        application.close();
    }

    public ZooKeeper connectToZookeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(address, SESSION_TIMEOUT, this);
        return zooKeeper;
    }

    public void run() throws InterruptedException {
        synchronized (zooKeeper) {
            zooKeeper.wait();
        }
    }

    private void close() throws InterruptedException {
        this.zooKeeper.close();
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        switch (watchedEvent.getType()) {
            case None:
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    logger.debug("Successfully connected to Zookeeper");
                } else if (watchedEvent.getState() == Event.KeeperState.Disconnected) {
                    synchronized (zooKeeper) {
                        logger.debug("Disconnected from Zookeeper");
                        zooKeeper.notifyAll();
                    }
                } else if (watchedEvent.getState() == Event.KeeperState.Closed) {
                    logger.debug("Closed Successfully");
                }
                break;
        }
    }
}
