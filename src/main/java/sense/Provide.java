package sense;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.Provider;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class Provide {
	
	ZooKeeper zk = null;
	
	public static void main(String[] args) throws Exception {
		
		Provide provide = new Provide();
		
		provide.connectZK();
		
		provide.registServerInfo();
		
		provide.handleService();
		
	}
	
	//建立连接
		public void connectZK() throws Exception {
			zk = new ZooKeeper("hadoop-master:2181,hadoop-slave02:2181,hadoop-slave03:2181", 2000, null);
		}

	
	//注册服务信息
	public void registServerInfo() throws Exception {
		String hostName = InetAddress.getLocalHost().getHostName();
		
		if(zk.exists("/servers", false)==null){
			zk.create("/servers", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
		
		String path = zk.create("/servers/server", (hostName+":"+8080).getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		
		System.out.println(hostName+"成功注册了一个节点 : "+path);
	}
	
	//等待请求，处理业务
	public void handleService() throws Exception {
		ServerSocket ss = new ServerSocket(8080);
		while(true) {
			Socket accept = ss.accept();
			
			
		}
		
	}
}
