package datacloud.zookeeper.pubsub;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs.Ids;

import datacloud.zookeeper.ZkClient;

public class Publisher extends ZkClient {

	public Publisher(String name, String servers) throws IOException, KeeperException, InterruptedException {
		super(name, servers);
	}

	public void publish(String topic, String message) {
		try {
			if (zk().exists('/' + topic, false) == null) {
				zk().create('/' + topic, message.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			} else {
				zk().setData('/' + topic, message.getBytes(), -1);
			}
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void process(WatchedEvent arg0) {
	}
}
