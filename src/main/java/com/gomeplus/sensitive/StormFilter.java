package com.gomeplus.sensitive;

import com.gomeplus.util.Conf;
import org.apache.commons.cli.*;
import org.apache.lucene.search.similarities.Distribution;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.*;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

import java.util.*;

import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by wangxiaojing on 2016/9/29.
 */

public class StormFilter {

    private static final String KAFKA_SPOUT_ID = "kafka_sensitive_word_id";

    private static final String SENSITIVE_FILTER = "sensitive_filter";

    private static final String SEND_TO_KAFKA = "send_to_kafka";

    private Logger loggers =  LoggerFactory.getLogger(StormFilter.class);

    private static final String LOCAL = "local";

    private static final String CLUSTER = "cluster";

    private static final String MODEL = "model";
    /**
     * 敏感词获取接口操作
     */
    public static void main(String[] args) throws Exception {
        Conf conf = new Conf();
        String topic = conf.getTopic();
        String[] zkServers = conf.getZkServers().split(",");
        List<String> zkHosts = null;
        for(String zkServer:zkServers){
            String zkHost = zkServer.split(":")[0];
            zkHosts.add(zkHost);
        }
        //进度信息记录于zookeeper的哪个路径下
        String zkRoot = conf.getZkRoot();
        String zkStr = conf.getZkServers();
        String clientId = conf.getStormId();
        //用以获取Kafka broker和partition的信息
        BrokerHosts brokerHosts = new ZkHosts(zkStr);

        SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, clientId);
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        //只有在local模式下需要记录读取状态时，才需要设置
        spoutConf.zkServers = zkHosts;
        spoutConf.zkPort = Integer.valueOf(conf.getZkPort());
        TopologyBuilder builder = new TopologyBuilder();

        //从kafka的消息队里获取数据到KAFKA_SPOUT_ID内
        builder.setSpout(KAFKA_SPOUT_ID, new KafkaSpout(spoutConf), 1);
        //将过滤的数据输出命名为SENSITIVE_FILTER的的bolt中
        builder.setBolt(SENSITIVE_FILTER, new SensitiveWordBolt()).shuffleGrouping(KAFKA_SPOUT_ID);
        // 创建kafka bolt 将数据发送到kafka
        KafkaBolt bolt = new KafkaBolt();
        // 设置producer配置
        Properties props = new Properties();
        props.put("bootstrap.servers", conf.getBootstrapServers());
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        bolt.withProducerProperties(props);
        // 将bolt产生的数据 输出数据到kafka
        bolt.withTopicSelector(new DefaultTopicSelector(conf.getStormToKafkaTopic()));
        //管道名称SEND_TO_KAFKA
        builder.setBolt(SEND_TO_KAFKA,bolt,1).shuffleGrouping(SENSITIVE_FILTER);
        // 设置storm 的配置
        Config config = new Config();

        config.put("kafka.broker.properties", props);

        String name = conf.getStormName();
        if (args != null && args.length > 0) {
            config.put(Config.NIMBUS_HOST, args[0]);
            config.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(name, config, builder.createTopology());
        } else {
            config.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(name, config, builder.createTopology());
            Thread.sleep(60000);
            cluster.killTopology(name);
            cluster.shutdown();
        }
    }
}
