package com.distributed;

import java.io.IOException;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class WatchersAndTriggers implements Watcher {
	private static final int SESSION_TIMEOUT = 3000;
	private static ZooKeeper zooKeeper;
	private static final String ZOOKEEPER_ADDRESS = "localhost:2181";

	public static void main(String[] args) throws IOException, InterruptedException {
		WatchersAndTriggers watchersAndTriggers = new WatchersAndTriggers();
		watchersAndTriggers.connectToZookeeper();
		watchersAndTriggers.run();
		watchersAndTriggers.close();

	}

	private void close() throws InterruptedException {
		this.zooKeeper.close();

	}

	void connectToZookeeper() throws IOException {
		this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
	}

	@Override
	public void process(WatchedEvent event) {
		switch (event.getType()) {
		case None:
			if (event.getState() == Event.KeeperState.SyncConnected)
				System.out.println("Successfully connected to Zookeeper!");
			else {
				synchronized (zooKeeper) {
					System.out.println("Disconnected from Zookeeper Event.");
					zooKeeper.notifyAll();
				}
			}
			break;
		}

	}

	void run() throws InterruptedException {
		synchronized (zooKeeper) {
			zooKeeper.wait();
		}
	}
}
