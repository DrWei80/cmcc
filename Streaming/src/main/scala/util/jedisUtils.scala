package util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object jedisUtils {
  val config =new JedisPoolConfig
  //设置最大连接数为20
  config.setMaxTotal(20)
  //设置最大空闲连接为10
  config.setMaxIdle(10)

  //加载连接池
  val pool=new JedisPool(config,"wqm02s",6379)
  //获取jedis
  def getJedis():Jedis={
    val jedis=pool.getResource
    jedis.auth("123456")
    jedis
  }

}
