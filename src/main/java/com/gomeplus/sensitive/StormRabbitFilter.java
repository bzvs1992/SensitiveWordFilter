package com.gomeplus.sensitive;

import com.gomeplus.util.Conf;
import io.latent.storm.rabbitmq.RabbitMQBolt;
import io.latent.storm.rabbitmq.RabbitMQSpout;
import io.latent.storm.rabbitmq.TupleToMessage;
import io.latent.storm.rabbitmq.TupleToMessageNonDynamic;
import io.latent.storm.rabbitmq.config.*;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.*;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.spout.Scheme;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by wangxiaojing on 2016/9/29.
 */

public class StormRabbitFilter {

    private static final String RABBIT_SPOUT_ID = "rabbit_sensitive_word_id";

    private static final String SENSITIVE_FILTER = "sensitive_filter";

    private static final String SEND_TO_RABBITMQ = "send_to_rabbitMQ";

    private static Logger loggers =  LoggerFactory.getLogger(StormRabbitFilter.class);
    /**
     * 敏感词获取接口操作
     */
    public static void main(String[] args) throws Exception {
        Conf conf = new Conf();

        TopologyBuilder builder = new TopologyBuilder();

        String rabbitMQHost = conf.getRabbitMQHost();
        String rabbitMQUserName = conf.getRabbitMQUsername();
        int rabbitMQPort = conf.getRabbitMQPort();
        String rabbitMQPassword = conf.getRabbitMQPassword();
        String rabbitMQVirtualHost = conf.getRabbitMQVirtualHost();
        String rabbitMQQueueName = conf.getRabbitMQQueueName();
        String rabbitProducerName = conf.getRabbitMQProducerName();
        String rabbitExchangeName = conf.getRabbitMQExchangeName();
        if(rabbitExchangeName.length()<=0){
            throw new Exception("RabbitMQ queue unbind Exchange,please set ");
        }

        // rabbit mq 的spout
        Scheme scheme =new MyCustomMessageScheme();
        RabbitMQSpout spout = new RabbitMQSpout(scheme);
        ConnectionConfig connectionConfig = new ConnectionConfig(rabbitMQHost,
                rabbitMQPort, rabbitMQUserName, rabbitMQPassword, rabbitMQVirtualHost, 10);
        ConsumerConfig spoutConfig = new ConsumerConfigBuilder()
                .connection(connectionConfig)
                .queue(rabbitMQQueueName)
                .prefetch(200)
                .requeueOnFail()
                .build();

        //添加spout到TopologyBuilder中,产生数据源
        builder.setSpout(RABBIT_SPOUT_ID, spout, 1)
                .addConfigurations(spoutConfig.asMap()).setMaxSpoutPending(200);

        //将过滤的数据输出命名为SENSITIVE_FILTER的的bolt中
        builder.setBolt(SENSITIVE_FILTER, new SensitiveWordBolt()).shuffleGrouping(RABBIT_SPOUT_ID);


        TupleToMessage schemeT = new TupleToMessageNonDynamic() {
            @Override
            protected  byte[] extractBody(Tuple input) {
                return input.getStringByField("text").getBytes();
            }
        };

        ProducerConfig sinkConfig = new ProducerConfigBuilder()
                .connection(connectionConfig)
                .contentEncoding("UTF-8")
                .contentType("application/json")
                .exchange(rabbitExchangeName)
                .routingKey(rabbitProducerName)
                .persistent()
                .build();

        //将数据输出到rabbitMQ
        builder.setBolt(SEND_TO_RABBITMQ, new RabbitMQBolt(schemeT),1)
                .addConfigurations(sinkConfig.asMap())
                .shuffleGrouping(SENSITIVE_FILTER);

        // 设置storm 的配置
        Config config = new Config();
        //config.put("kafka.broker.properties", props);

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

/**
 * 反序列化rabbitMQ的消息
 * 自定义MQ消息的Schema
 * */
class MyCustomMessageScheme implements Scheme {


    public List<Object> deserialize(ByteBuffer ser) {
        List objs = new ArrayList();
        //反序列化string
        String str = null;
        try {
            str = new String(ser.array(), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        //  返回UUID,String,Number
        objs.add(UUID.randomUUID().toString());
        objs.add(str);
        String numStr = Math.round(Math.random() * 8999 + 1000) + "";
        objs.add(numStr);

        return objs;
    }

    /**
     * 定义spout输出的Fileds*/
    public Fields getOutputFields() {
        //依次返回UUID，String,Number，需要与上述返回的List列表一一对应
        return new Fields("id", "str", "num");
    }
}
