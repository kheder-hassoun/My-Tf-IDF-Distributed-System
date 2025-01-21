import org.apache.zookeeper.KeeperException;

import java.net.InetAddress;
import java.net.UnknownHostException;


public class OnElectionAction implements OnElectionCallback {
    private final ServiceRegistry serviceRegistry;
    private final int port;
    public OnElectionAction(ServiceRegistry serviceRegistry, int port) {
        this.serviceRegistry = serviceRegistry;
        this.port = port;
    }
    @Override
    public void onElectedToBeLeader() {
        try {
            String ipAddress = InetAddress.getLocalHost().getHostAddress();
            serviceRegistry.unregisterFromCluster();
            serviceRegistry.registerToClusterAsCoordinator(ipAddress);
            serviceRegistry.registerForUpdates();
            Leader leader = new Leader();
           // GrpcServerLauncher.start();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        } catch (KeeperException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    @Override
    public void onWorker() {
        try {
            String ipAddress = InetAddress.getLocalHost().getHostAddress();
            System.out.println("worker get its ip and its : " +ipAddress);
            String currentServerAddress = String.format("%s:%s", ipAddress, port);
            serviceRegistry.registerToCluster(currentServerAddress);
            Worker worker = new Worker();
           // worker.start();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }
}
