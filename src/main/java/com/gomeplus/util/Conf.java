package com.gomeplus.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by wangxiaojing on 2016/9/29.
 */
public class Conf {

    //配置文件
    private Properties properties;

    // ES 集群名字
    private final static String ES_CLUSTER_NAME = "cluster.name";

    // ES 主机地址
    private final static String ES_HOSTNAME = "es.node";
    // ES 默认通信端口
    private final static int ES_PORT = 9300;

    //kafka的主题
    private final static String TOPIC = "kafka.topic";

    //zk地址里列表，不带端口
    private final static String ZK_SERVERS= "zkServers";

    // zk port
    private final static String ZK_PORT = "zkPort";

    //进度信息记录于zookeeper的哪个路径下
    private final static String ZK_ROOT= "zkRoot";

    private final static String STREAMING_NUMThreads = "streaming.numThreads";
    //配置文件名字
    private final static String CONFIG_FILE = "conf.properties";

    //进度记录的id，想要一个新的Spout读取之前的记录，应把它的id设为跟之前的一样。
    private final static String STORM_ID= "stormId";

    private final static String STORM_NAME = "storm.name";

    //kafka broker 地址
    private final static String BOOTSTRAP_SERVERS = "bootstrap.servers";

    // 输入到kafka的主题
    private final static String STORM_TO_KAFKA_TOPIC = "storm.to.kafka.topic";

    private final static String REDIS_HOSTS = "redis.servers";

    // RabbitMQ host
    private final static String RABBITMQ_HOST = "rabbitMQ.host";

    //RabbitMQ port
    private final static String RABBITMQ_PORT = "rabbitMQ.port";

    // RabbitMQ username
    private final static String RABBITMQ_USERNAME = "rabbitMQ.username";
    // RabbitMQ password
    private final static String RABBITMQ_PASSWORD = "rabbitMQ.password";
    // RabbitMQ virtualHost
    private final static String RABBITMQ_VIRTUAL_HOST = "rabbitMQ.virtualHost";
    // RabbitMQ 消息队列名称打算进行文本过滤的消息流
    private final static String RABBITMQ_QUEUE_NAME = "rabbitMQ.queue.name";
    // RabbitMQ 经过文本过滤后输出的消息流
    private final static String RABBITMQ_PRODUCER_NAME = "rabbitMQ.producer.name";
    // 定义文本数据在json结构体中的位置
    private final static String JSON_TEXT = "json.text";
    // 输出到队列指定的exchange，这个不能为空
    private final static String RABBITMQ_EXCHANGE_NAME = "rabbitMQ.exchange.name";

    private final static String STORM_SEEDS = "storm.seeds";

    private Logger loggers;

    private HashMap<String,String> commendHashMap;

    final static   String[][] opts = {
            {"--"+ STORM_ID },
            {"--" + STORM_NAME},
            {"--" + STORM_TO_KAFKA_TOPIC},
            {"--" + TOPIC },
            {"--streaming.group"},
            {"--" + RABBITMQ_QUEUE_NAME},
            {"--" + RABBITMQ_VIRTUAL_HOST},
            {"--" + RABBITMQ_PRODUCER_NAME},
            {"--"+ RABBITMQ_EXCHANGE_NAME},
            {"--"+ STORM_SEEDS}
    };

    final static String[][] switches = {
            { "--help", "-h" },
            { "--version", "-v" },
    };


    public Conf(){
        loggers = LoggerFactory.getLogger(Conf.class);
        // 命令行解析
        commendHashMap = new HashMap<>();
        // 通过配置文件
        try {
            properties = new Properties();
            InputStream inputStream = Conf.class.getClassLoader().getResourceAsStream(CONFIG_FILE);

            if (inputStream == null){
                throw new RuntimeException(CONFIG_FILE + " not found in classpath");
            }else{
                properties.load(inputStream);
                inputStream.close();
            }
        } catch (FileNotFoundException fnf) {
            loggers.error("No configuration file" + CONFIG_FILE);
            fnf.printStackTrace();
            throw new RuntimeException("No configuration file " + CONFIG_FILE + " found in classpath.", fnf);
        } catch (IOException ie) {
            loggers.error("Can't read configuration file" + CONFIG_FILE);
            ie.printStackTrace();
            throw new IllegalArgumentException("Can't read configuration file " + CONFIG_FILE, ie);
        }
    }

    public String getEsHostname(){
        return properties.getProperty(ES_HOSTNAME);
    }

    public String getEsClusterName(){
        return  properties.getProperty(ES_CLUSTER_NAME);
    }

    public String getTopic(){
        return commendHashMap.containsKey("--"+ TOPIC) ?
                commendHashMap.get("--"+ TOPIC):
                properties.getProperty(TOPIC);
    }

    public String getZkServers(){
        return  properties.getProperty(ZK_SERVERS);
    }

    public String getZkPort(){
        return  properties.getProperty(ZK_PORT,"2181");
    }

    public String getZkRoot(){
        return properties.getProperty(ZK_ROOT);
    }
    public String getStormId(){
        return commendHashMap.containsKey("--" + STORM_ID) ?
                commendHashMap.get("--" + STORM_ID):
                properties.getProperty(STORM_ID);
    }

    public String getStormName(){
        return commendHashMap.containsKey("--" +STORM_NAME) ?
                commendHashMap.get("--" +STORM_NAME) :
                properties.getProperty(STORM_NAME);
    }

    public String getBootstrapServers(){
        return properties.getProperty(BOOTSTRAP_SERVERS);
    }

    public String getStormToKafkaTopic(){
        return commendHashMap.containsKey("--" + STORM_TO_KAFKA_TOPIC) ?
                commendHashMap.get("--" + STORM_TO_KAFKA_TOPIC):
                properties.getProperty(STORM_TO_KAFKA_TOPIC);
    }

    public String getRedisHosts(){
        return  properties.getProperty(REDIS_HOSTS);
    }

    public String getRabbitMQHost(){
        return  properties.getProperty(RABBITMQ_HOST);
    }

    public int getRabbitMQPort(){
        return Integer.parseInt(properties.getProperty(RABBITMQ_PORT));
    }

    public String getRabbitMQUsername(){
        return properties.getProperty(RABBITMQ_USERNAME);
    }

    public String getRabbitMQPassword(){
        return  properties.getProperty(RABBITMQ_PASSWORD);
    }

    public String getRabbitMQVirtualHost(){
        return commendHashMap.containsKey("--"+ RABBITMQ_VIRTUAL_HOST) ?
                commendHashMap.get("--"+ RABBITMQ_VIRTUAL_HOST) :
                properties.getProperty(RABBITMQ_VIRTUAL_HOST);
    }

    public String getRabbitMQQueueName(){
        return commendHashMap.containsKey("--" + RABBITMQ_QUEUE_NAME) ?
                commendHashMap.get("--" + RABBITMQ_QUEUE_NAME):
                properties.getProperty(RABBITMQ_QUEUE_NAME);
    }

    public String getJsonText(){
        return properties.getProperty(JSON_TEXT);
    }

    public String getRabbitMQProducerName(){
        return commendHashMap.containsKey("--" + RABBITMQ_PRODUCER_NAME) ?
                commendHashMap.get("--" + RABBITMQ_PRODUCER_NAME):
                properties.getProperty(RABBITMQ_PRODUCER_NAME);
    }
    public String getRabbitMQExchangeName(){
        return commendHashMap.containsKey("--" + RABBITMQ_EXCHANGE_NAME) ?
                commendHashMap.get("--" + RABBITMQ_EXCHANGE_NAME):
                properties.getProperty(RABBITMQ_EXCHANGE_NAME);
    }

    public String getStormSeeds(){
        return commendHashMap.containsKey("--" + STORM_SEEDS)?
                commendHashMap.get("--" + STORM_SEEDS):
                properties.getProperty(STORM_SEEDS);
    }

    public String getStreamingNumThreads(){
        return properties.getProperty(STREAMING_NUMThreads,"2");
    }

    public void parse(String[] args) {
        Pattern eqSeparatedOpt = Pattern.compile("(--[^=]+)=(.+)");

        int idx = 0;
        for (String arg : args) {
            idx++;
            String value = null;

            Matcher m = eqSeparatedOpt.matcher(arg);
            if (m.matches()) {
                arg = m.group(1);
                value = m.group(2);
                commendHashMap.put(arg,value);
            }
            // Look for options with a value.
            String name = findCliOption(arg, opts);
            if (name != null) {
                if (value == null) {
                    if (idx == args.length - 1) {
                        throw new IllegalArgumentException(
                                String.format("Missing argument for option '%s'.", arg));
                    }
                    value = args[idx];
                    commendHashMap.put(name,value);
                }
                continue;
            }

            // Look for a switch.
            name = findCliOption(arg, switches);
            if (name != null) {
                continue;
            }
        }

        if (idx < args.length) {
            idx++;
        }
    }

    private  String findCliOption(String name, String[][] available) {
        for (String[] candidates : available) {
            for (String candidate : candidates) {
                if (candidate.equals(name)) {
                    return candidates[0];
                }
            }
        }
        return null;
    }
}
