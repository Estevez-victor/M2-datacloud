package datacloud.zookeeper.membership;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;

import datacloud.zookeeper.ZkClient;

public class ClientMembership extends ZkClient {
	private List<String> lzks;

	public ClientMembership(String name, String servers) throws IOException, KeeperException, InterruptedException {
		super(name, servers);
		lzks = new ArrayList<String>();
	}


	public List<String> getMembers() {
		try {
			lzks = zk().getChildren("/ids", true);
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
		return lzks;
	}


	@Override
	public void process(WatchedEvent arg0) {
		// TODO Auto-generated method stub
	}

}