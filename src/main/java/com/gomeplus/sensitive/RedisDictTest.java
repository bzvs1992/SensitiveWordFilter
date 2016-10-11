package com.gomeplus.sensitive;

import com.gomeplus.util.Conf;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.wltea.analyzer.dic.Dictionary;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.wltea.analyzer.dic.RedisDict;
/**
 * Created by wangxiaojing on 2016/10/11.
 */
public class RedisDictTest {

    private JedisCluster jc=null;
    public static ESLogger logger = Loggers.getLogger("ik-analyzer");
    public void redisDictTest(){
        Set<HostAndPort> hps = new HashSet<HostAndPort>();
        hps.add(new HostAndPort("10.69.10.51", 6379));
        GenericObjectPoolConfig poolConfig =new GenericObjectPoolConfig();
        poolConfig.setJmxEnabled(false);
        jc = new JedisCluster(hps, 5000, 10, poolConfig);
    }

    public static void main(String[] args){
        /**/
        ScheduledExecutorService pool = Executors.newScheduledThreadPool(1);
        pool.scheduleAtFixedRate(new RedisDict(),0,60, TimeUnit.SECONDS);
        /*
        Set<HostAndPort> hps = new HashSet<HostAndPort>();
        Conf conf = new Conf();
        String[] redisHosts = conf.getRedisHosts().split(";");
        for (String redisHost : redisHosts) {
            String[] hp = redisHost.split(":");
            Dictionary.logger.info(hp[0] + ": port : " + hp[1]);
            hps.add(new HostAndPort(hp[0], Integer.valueOf(hp[1]).intValue()));
        }
        GenericObjectPoolConfig poolConfig =new GenericObjectPoolConfig();
        poolConfig.setJmxEnabled(false);
        JedisCluster jc = new JedisCluster(hps, 5000, 10, poolConfig);
        //jc.subscribe("ik_main");
        Set set = jc.smembers("ik_main");
        Iterator t = set.iterator();
        ArrayList<String> words = new ArrayList<String>();
        while (t.hasNext()) {
            Object obj = t.next();
            words.add(obj.toString());
            try{
                logger.info(obj.toString());
            }catch (Exception e){
                logger.info(e.toString());
            }
        }

*/
    }
}
