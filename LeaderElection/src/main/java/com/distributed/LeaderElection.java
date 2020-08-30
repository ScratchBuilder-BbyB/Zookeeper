package com.distributed;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

public class LeaderElection implements Watcher {

	private ZooKeeper zooKeeper = null;
	private String currentZnodeName;
	private static final String ZOOKEEPER_LOCATION = "localhost:2181";
	private static final int SESSION_TIMEOUT = 3000; // Time after which Zookeeper server will treat client as dead
	private static final String ELECTION_NAMESPACE = "/election";

	public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
		LeaderElection election = new LeaderElection();
		election.connctToZookeeper();
		election.standInElection();
		election.electLeader();
		election.run();
		election.close();
		System.out.println("Disconnected from Zookeeper, exiting application.");
	}

	void standInElection() throws KeeperException, InterruptedException {
		String zNodePrefix = ELECTION_NAMESPACE + "/c_"; // Here c stands for candidate
		String zNodeFullPath = zooKeeper.create(zNodePrefix, new byte[] {}, ZooDefs.Ids.OPEN_ACL_UNSAFE,
				CreateMode.EPHEMERAL_SEQUENTIAL);

		System.out.println("znode name : " + zNodeFullPath);
		this.currentZnodeName = zNodeFullPath.replace(ELECTION_NAMESPACE + "/", "");
	}
	
	void electLeader() throws KeeperException, InterruptedException {
		List<String> children = this.zooKeeper.getChildren(ELECTION_NAMESPACE, false);
		
		Collections.sort(children);
		String leaderZnode = children.get(0);
		if(leaderZnode.equals(currentZnodeName))
			System.out.println("I am the leader.");
		else
			System.out.println("The leader is "+ leaderZnode + " . I am "+ currentZnodeName);
	}

	private void close() throws InterruptedException {
		this.zooKeeper.close();

	}

	private void connctToZookeeper() throws IOException {
		this.zooKeeper = new ZooKeeper(ZOOKEEPER_LOCATION, SESSION_TIMEOUT, this);
	}

	public void process(WatchedEvent event) {
		switch (event.getType()) {
		case None:
			if (event.getState() == Event.KeeperState.SyncConnected)
				System.out.println("Successfully connected to Zookeeper.");
			else {
				synchronized (zooKeeper) {
					System.out.println("Disconnected from zookeeper event");
					zooKeeper.notifyAll();
				}
			}
			break;

		default:
			break;
		}

	}

	public void run() throws InterruptedException {
		synchronized (zooKeeper) {
			zooKeeper.wait();
		}

	}

}
