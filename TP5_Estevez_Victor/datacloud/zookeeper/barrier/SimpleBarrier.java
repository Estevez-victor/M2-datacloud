package datacloud.zookeeper.barrier;

import static datacloud.zookeeper.util.ConfConst.EMPTY_CONTENT;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;

import datacloud.zookeeper.ZkClient;

public class SimpleBarrier implements Watcher {
	private String path;
	private ZkClient zkclient;
	private final Object mutex = new Object();

	public SimpleBarrier(ZkClient zkclient, String path) {
		this.zkclient = zkclient;
		this.path = path;
		try {
			if (zkclient.zk().exists(path, false) == null) {
				zkclient.zk().create(path, EMPTY_CONTENT, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			}
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void sync() {
		try {
			synchronized (mutex) {
				while (zkclient.zk().exists(path, this) != null) {
					mutex.wait();
				}
			}
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void process(WatchedEvent watchedEvent) {
		if (watchedEvent.getType() == Event.EventType.NodeDeleted) {
			synchronized (mutex) {
				mutex.notify();
			}
		}
	}

}