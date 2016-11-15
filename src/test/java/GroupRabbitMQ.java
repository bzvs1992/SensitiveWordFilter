import java.io.IOException;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;


public class GroupRabbitMQ implements Consumer{
	
	public static void main(String[] args) {
		
		GroupRabbitMQ t = new GroupRabbitMQ();
		String userName="addata";
		String password="M2evmkqQzJeG";
		String virtualHost="/bs-arch";
		String hostName="rbmqv1.pro.gomeplus.com";
		int portNumber=5672;
		
		ConnectionFactory factory = new ConnectionFactory();
		
		factory.setUsername(userName);
		factory.setPassword(password);
		factory.setVirtualHost(virtualHost);
		factory.setHost(hostName);
		factory.setPort(portNumber);
		
        Connection connection;
		try {
			connection = factory.newConnection();
			Channel channel = connection.createChannel();
			
			 /** 声明要连接的队列 */
			channel.queueDeclare("addata.group", true, false, false, null);
	        System.out.println("等待消息产生：");
	        t.waitMesseage(channel);
			 /** 创建消费者对象，用于读取消息 */
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ShutdownSignalException e) {
			e.printStackTrace();
		} catch (ConsumerCancelledException e) {
			e.printStackTrace();
		}
	}
	
	
	public void waitMesseage(Channel channel) throws IOException{
		channel.basicConsume("addata.group", true, this);
	}

	public void handleCancel(String arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

	public void handleCancelOk(String arg0) {
		// TODO Auto-generated method stub
		
	}

	public void handleConsumeOk(String arg0) {
		// TODO Auto-generated method stub
		
	}

	public void handleDelivery(String arg0, Envelope arg1,
			BasicProperties arg2, byte[] body) throws IOException {
		 String  message = new String(body,"UTF-8");
		 System.out.println(message);
		 
	}

	public void handleRecoverOk(String arg0) {
		// TODO Auto-generated method stub
		
	}

	public void handleShutdownSignal(String arg0, ShutdownSignalException arg1) {
		// TODO Auto-generated method stub
		
	}

}
