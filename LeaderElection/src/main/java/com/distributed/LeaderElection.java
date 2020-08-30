package com.distributed;

import java.io.IOException;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class LeaderElection implements Watcher{
	
	private ZooKeeper zooKeeper = null;
	private static final String ZOOKEEPER_LOCATION = "localhost:2181";
	private static final int SESSION_TIMEOUT = 3000;
	
	public static void main(String[] args) throws IOException, InterruptedException {
		LeaderElection election = new LeaderElection();
		election.connctToZookeeper();
		election.run();
		election.close();
		System.out.println("Disconnected from Zookeeper, exiting application.");
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
			if(event.getState() == Event.KeeperState.SyncConnected)
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
