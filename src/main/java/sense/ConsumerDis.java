package sense;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class ConsumerDis {
	
	List<String> onlineServers =null;
	ZooKeeper zk = null;
	public static  void main(String[] args) throws Exception {
		ConsumerDis consumer = new ConsumerDis();
		
		consumer.connectZK();
		
		consumer.getOnlineServers();
		
		consumer.requestService();
		
	}
	
	//建立连接
		public void connectZK() throws Exception {
			zk = new ZooKeeper("hadoop-master:2181,hadoop-slave02:2181,hadoop-slave03:2181", 2000, new Watcher() {
				
				public void process(WatchedEvent event) {
					try {
						getOnlineServers();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			});
		}
		
		public void getOnlineServers() throws Exception{
			List<String> serverList = new ArrayList();
			List<String> children = zk.getChildren("/servers", false);
			for (String child : children) {
				byte[] data = zk.getData("/servers/"+child, false, null);
				serverList.add(new String(data));
			}
			
			onlineServers = serverList;
			System.out.println("本消费者查询了一次服务器列表: "+serverList);
		}
		
		public void requestService() throws Exception {
			Random random = new Random();
			while(true) {
				Thread.sleep(2000);
				if(onlineServers.size()==0) {
					System.out.println("还没有在线的服务器");
					continue;
				}
				String server = onlineServers.get(random.nextInt(onlineServers.size()));
				System.out.println("本消费者本次挑选了服务器:"+server);
				
			}
		}

}
