package com.enn.zkCluster;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

/**
 * zkCluster 测试
 * 
 * @author houjianpo
 *
 */
public class ZKDemo {
	// 会话超时时间， 设置为与系统默认时间一致
	private static final int SESSION_TIMEOUT = 30 * 1000;

	// 创建zookeeper实例
	private ZooKeeper zk;

	// 创建Watcher实例
	private Watcher wh = new Watcher() {

		/**
		 * Watched事件
		 */
		public void process(WatchedEvent event) {
			System.out.println("WatchedEvent >>> " + event.toString());
		}
	};

	// 初始化zookeeper实例
	@SuppressWarnings("static-access")
	private void createZKInstance() throws Exception {
		// 连接到zk服务，多个服务可以用,分割写
		zk = new ZooKeeper("10.4.82.161:3000,10.4.82.161:3001,10.4.82.161:3002,10.4.82.161:3003,10.4.82.161:3004", this.SESSION_TIMEOUT, this.wh);
	}
	
	// zookeeper 操作
	private void ZKOperations() throws IOException, InterruptedException, KeeperException {
		System.out.println("\n1. 创建Zookeeper 节点(znode : zoo2, 数据: myData2, 权限: OPEN_ACL_UNSAFE, 节点类型: Persistent.)");
		zk.create("/zoo2", "myData2".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		
		System.out.println("\n2. 查看是否创建成功:");
		System.out.println(new String(zk.getData("/zoo2", this.wh, null)));// 添加Watcher,设置下次修改监控
		
		// 前面一行我们添加了对/zoo2节点的监视，所以这里对/zoo2进行修改的时候，会触发Watch时间
		System.out.println("\n3. 修改/zoo2节点数据");
		zk.setData("/zoo2", "helloword".getBytes(), -1);
		
		// 这里再次进行修改，则不会触发Watch事件，这就是我们验证ZK的一个特性“一次触发”，也就是说设置一次监控，只会对下次操作起一次作用。
		System.out.println("\n3-1. 再次修改/zoo2节点数据");
		zk.setData("/zoo2", "helloword-ABCD".getBytes(), -1);
		
		// 查看
		System.out.println("\n4. 查看是否修改成功:");
		System.out.println(new String(zk.getData("/zoo2", false, null)));// 本次不再设置Watch监控
		
		// 删除节点
		System.out.println("\n5. 删除节点");
		zk.delete("/zoo2", -1);
		
		System.out.println("\n6. 查看节点是否被删除:");
		System.out.println("节点状态: [" + zk.exists("/zoo2", false) + " ]" );
	}
	
	// 关闭zookeeper连接
	@SuppressWarnings("unused")
	private void ZKClose() throws InterruptedException {
		if(zk.getState() == ZooKeeper.States.CONNECTED){
			zk.close();
		}
	}
	
	public static void main(String[] args) throws Exception {
		ZKDemo zkDemo = new ZKDemo();
		zkDemo.createZKInstance();
		zkDemo.ZKOperations();
		zkDemo.ZKClose();
	}

}
