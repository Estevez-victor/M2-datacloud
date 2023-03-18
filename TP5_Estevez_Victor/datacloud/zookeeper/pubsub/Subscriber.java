package datacloud.zookeeper.pubsub;

import static datacloud.zookeeper.util.ConfConst.EMPTY_CONTENT;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs.Ids;

import datacloud.zookeeper.ZkClient;

public class Subscriber extends ZkClient {
	private Map<String, List<String>> archive;

	public Subscriber(String name, String servers) throws IOException, KeeperException, InterruptedException {
		super(name, servers);
		archive = new HashMap<String, List<String>>();
	}

	public void subscribe(String topic) {
		try {
			if (zk().exists('/' + topic, this) == null) {
				zk().create('/' + topic, EMPTY_CONTENT, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			}
			zk().getData('/' + topic, this, null);
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
	}

	public List<String> received(String topic) {
		List<String> l = archive.get('/' + topic);
		if (l == null || l.size() == 0) {
			return Collections.emptyList();
		}
		return l;
	}

	@Override
	public void process(WatchedEvent watchedEvent) {
		if (watchedEvent.getType() == Event.EventType.NodeDataChanged) {
			try {
				String topic = watchedEvent.getPath();
				String message = new String(zk().getData(topic, this, null));
				List<String> list = archive.get(topic);
				if (list == null) {
					list = new ArrayList<String>();
					list.add(message);
					archive.put(topic, list);
				} else {
					list.add(message);
				}
			} catch (KeeperException | InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
