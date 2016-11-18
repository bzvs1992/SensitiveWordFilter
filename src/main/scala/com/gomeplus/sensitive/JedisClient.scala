package com.gomeplus.sensitive

import com.gomeplus.util.Conf
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{HostAndPort, JedisCluster}

/**
 * Created by wangxiaojing on 16/11/7.
 */
object JedisClient extends Serializable{

  def getJedisCluster(): JedisCluster ={
    val config = new Conf
    val redisHost = config.getRedisHosts.split(",")
    // 获取redis地址
    val jedisClusterNodes = new java.util.HashSet[HostAndPort]()
    redisHost.foreach(x=>{
      val redisHostAndPort = x.split(":")
      jedisClusterNodes.add(new HostAndPort(redisHostAndPort(0),redisHostAndPort(1).toInt))
    })

    val redisTimeout = 3000
    val poolConfig: GenericObjectPoolConfig = new GenericObjectPoolConfig
    poolConfig.setJmxEnabled(false)
    val jc:JedisCluster = new JedisCluster(jedisClusterNodes, redisTimeout, 10, poolConfig)
    jc
  }
}
