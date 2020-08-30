package com.distributed;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class WatchersAndTriggers implements Watcher {
	private static final int SESSION_TIMEOUT = 3000;
	private static ZooKeeper zooKeeper;
	private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
	private static final String TARGET_ZNODE = "/target_znode";

	public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
		WatchersAndTriggers watchersAndTriggers = new WatchersAndTriggers();
		watchersAndTriggers.connectToZookeeper();
		watchersAndTriggers.watchTargetNode();
		watchersAndTriggers.run();
		watchersAndTriggers.close();

	}

	private void close() throws InterruptedException {
		WatchersAndTriggers.zooKeeper.close();

	}

	void connectToZookeeper() throws IOException {
		WatchersAndTriggers.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
	}

	public void watchTargetNode() throws KeeperException, InterruptedException {
		Stat stat = WatchersAndTriggers.zooKeeper.exists(TARGET_ZNODE, this);
		if (stat == null)
			return;
		byte[] data = WatchersAndTriggers.zooKeeper.getData(TARGET_ZNODE, this, stat);
		List<String> children = WatchersAndTriggers.zooKeeper.getChildren(TARGET_ZNODE, this);

		System.out.println("Target Node Data : " + new String(data) + " children are " + children);
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
		case NodeCreated:
			System.out.println(TARGET_ZNODE + " was created.");
			break;
		case NodeDeleted:
			System.out.println(TARGET_ZNODE + " was deleted.");
			break;
		case NodeChildrenChanged:
			System.out.println(TARGET_ZNODE + " was children changed.");
			break;
		case NodeDataChanged:
			System.out.println(TARGET_ZNODE + " was data changed.");
		}
		try {
			watchTargetNode();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	void run() throws InterruptedException {
		synchronized (zooKeeper) {
			zooKeeper.wait();
		}
	}
}
