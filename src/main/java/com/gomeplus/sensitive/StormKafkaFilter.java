package com.gomeplus.sensitive;

import com.gomeplus.util.Conf;
import kafka.utils.ZKConfig;
import org.I0Itec.zkclient.ZkClient;
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


/**
 * Created by wangxiaojing on 2016/9/29.
 */

public class StormKafkaFilter {

    private static final String KAFKA_SPOUT_ID = "kafka_sensitive_word_id";

    private static final String SENSITIVE_FILTER = "sensitive_filter";

    private static final String SEND_TO_KAFKA = "send_to_kafka";

    private static final String LOCAL = "local";

    private static final String CLUSTER = "cluster";

    private static final String MODEL = "model";

    // 设置链接zk超时时间
    private static final int zkSessionTimeoutMs = 30000;
    private static final int zkConnectionTimeoutMs = 30000;
    /**
     * 敏感词获取接口操作
     */
    public static void main(String[] args) throws Exception {
        Conf conf = new Conf();
        conf.parse(args);
        String topic = conf.getTopic();
        String outputTopic= conf.getStormToKafkaTopic();
        String[] zkServers = conf.getZkServers().split(",");
        List<String> zkHosts = new ArrayList<>();
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

        //通过zk获取topic的分区信息
        ZkClient zk = new ZkClient(zkStr, zkSessionTimeoutMs, zkConnectionTimeoutMs);
        String inputPath = "/brokers/topics/"+topic+"/partitions";
        String outputPath = "/brokers/topics/"+outputTopic+"/partitions";
        int inputNum = zk.exists(inputPath)?zk.getChildren(inputPath).size():1;
        int outputNum = zk.exists(outputPath)?zk.getChildren(outputPath).size():1;
        zk.close();

        //从kafka的消息队里获取数据到KAFKA_SPOUT_ID内
        builder.setSpout(KAFKA_SPOUT_ID, new KafkaSpout(spoutConf), inputNum);
        //将过滤的数据输出命名为SENSITIVE_FILTER的的bolt中
        builder.setBolt(SENSITIVE_FILTER, new SensitiveWordKafkaBolt(),3*inputNum).shuffleGrouping(KAFKA_SPOUT_ID);
        // 创建kafka bolt 将数据发送到kafka
        // 设置producer配置
        Properties props = new Properties();
        props.put("bootstrap.servers", conf.getBootstrapServers());
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaBolt bolt = new KafkaBolt()
                .withProducerProperties(props)
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper())
                .withTopicSelector(new DefaultTopicSelector(outputTopic));

        // 将bolt产生的数据 输出数据到kafka
        //管道名称SEND_TO_KAFKA
        builder.setBolt(SEND_TO_KAFKA,bolt,outputNum).shuffleGrouping(SENSITIVE_FILTER);
        // 设置storm 的配置
        Config config = new Config();
        config.setNumAckers(0);
        String name = conf.getStormName();
        String stormSeeds = conf.getStormSeeds();
        if (null != stormSeeds) {
            String[] stormSeedsA = stormSeeds.split(",");
            if(stormSeedsA.length == 1){
                config.put(Config.NIMBUS_HOST,conf.getStormSeeds());
            }else{
                List<String> stormSeedsList = new ArrayList<>();
                for (String stormSeed : stormSeedsA) {
                    stormSeedsList.add(stormSeed);
                }
                config.put(Config.NIMBUS_SEEDS, stormSeedsList);
            }
            config.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(name, config, builder.createTopology());
        } else {
            config.setMaxTaskParallelism(1);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(name, config, builder.createTopology());
            Thread.sleep(60000);
            cluster.killTopology(name);
            cluster.shutdown();
        }
    }
}
