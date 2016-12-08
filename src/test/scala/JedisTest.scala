import com.gomeplus.util.Conf
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{HostAndPort, JedisCluster}

/**
 * Created by wangxiaojing on 16/10/28.
 */
object JedisTest {

  def main(args: Array[String]) {

      val config = new Conf
     config.parse(args)
      val redisHost = config.getRedisHosts.split(";")
      // 获取redis地址
      val jedisClusterNodes = new java.util.HashSet[HostAndPort]()
      redisHost.foreach(x => {
        val redisHostAndPort = x.split(":")
        jedisClusterNodes.add(new HostAndPort(redisHostAndPort(0), redisHostAndPort(1).toInt))
      })

      val redisTimeout = 3000
      val poolConfig: GenericObjectPoolConfig = new GenericObjectPoolConfig
      poolConfig.setJmxEnabled(false)
      val jc: JedisCluster = new JedisCluster(jedisClusterNodes, redisTimeout, 10, poolConfig)
      val out = jc.smembers("ik_main")
      print(out)
  }
}
