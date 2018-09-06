package ZK;

import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;

public class CRUD {

	ZooKeeper zk = null;
	private static final String connectString = "hadoop-master:2181,hadoop-slave02:2181,hadoop-slave03:2181";

	private int sessionTimeout = 2000;

	@Before
	public void init() throws Exception {
		zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			public void process(WatchedEvent event) {
				System.out.println(event.getPath()+"  "+event.getType());
				try {
					zk.getChildren("/", true);
				} catch (KeeperException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});
	}
	
	//添加节点及数据
	@Test
	public void create() throws Exception, InterruptedException {
		zk.create("/eclipse/e", "eclipse".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	}
	
	
	//遍历节点
	@Test
	public void getChildren() throws Exception, InterruptedException {
		List<String> list = zk.getChildren("/", true);
		for (String string : list) {
			System.out.println(string);
		}
		
		Thread.sleep(Long.MAX_VALUE);
	}
	
	
	//取到节点的数据
	@Test
	public void getData() throws Exception, Exception {
		byte[] bs = zk.getData("/eclipse", false, null);
		System.out.println(new String(bs));
		
	}
	
	//删除节点
	@Test
	public void delete() throws Exception, Exception {
		zk.delete("/eclipse", -1);
	}
	
	//修改节点的数据
	@Test
	public void setData() throws Exception, Exception {
		zk.setData("/eclipse", "hello eclipse".getBytes(), -1);
	}
	
	
	//监听
	@Test
	public void watch() throws Exception {
		zk.getData("/eclipse", new Watcher() {
			public void process(WatchedEvent event) {
				System.out.println(event.getPath()+"  "+event.getType());
			}
		}, null);
		Thread.sleep(Long.MAX_VALUE);
	}
	
	@After
	public void close() throws Exception {
		zk.close();
	}

}
