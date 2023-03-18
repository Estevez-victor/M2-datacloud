package datacloud.zookeeper.barrier;

import static datacloud.zookeeper.util.ConfConst.EMPTY_CONTENT;

import java.math.BigInteger;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;

import datacloud.zookeeper.ZkClient;

public class BoundedBarrier implements Watcher {
	private ZkClient zkclient;
	private String path;
	private final Object mutex = new Object();
	private int N;

	public BoundedBarrier(ZkClient zkclient, String path, int N) {
		this.zkclient = zkclient;
		this.path = path;
		try {
			if (zkclient.zk().exists(path, false) == null) {
				zkclient.zk().create(path, BigInteger.valueOf(N).toByteArray(), Ids.OPEN_ACL_UNSAFE,
						CreateMode.PERSISTENT);
				this.N = N;
			} else {
				this.N = sizeBarrier();
			}
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void sync() {
		String me = null;
		try {
			synchronized (mutex) {
				me = zkclient.zk().create(path + "/sync", EMPTY_CONTENT, Ids.OPEN_ACL_UNSAFE,
						CreateMode.EPHEMERAL_SEQUENTIAL);
				while (zkclient.zk().getChildren(path, this).size() < N) {
					mutex.wait();
				}
				Thread.sleep(50);
				zkclient.zk().delete(me, -1);
				if (zkclient.zk().getChildren(path, false).size() == 0) {
					zkclient.zk().delete(path, -1);
				}
			}
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
	}

	public int sizeBarrier() {
		byte[] bytes = null;
		try {
			bytes = zkclient.zk().getData(path, false, null);
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
		int res = 0;
		for (byte b : bytes) {
			res = (res << 8) + (b & 0xFF);
		}
		return res;
	}

	@Override
	public void process(WatchedEvent watchedEvent) {
		if (watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
			synchronized (mutex) {
				mutex.notify();
			}
		}
	}
}
