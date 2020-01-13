import redis.clients.jedis.{JedisPool, JedisPoolConfig}

object redisConnectionTest2 {
  //利用连接池方式连接redis
  def main(args: Array[String]): Unit = {

    //设置配置选项
    val config =new JedisPoolConfig
    //设置最大连接数为20
    config.setMaxTotal(20)
    //设置最大空闲连接为10
    config.setMaxIdle(10)

    //加载连接池
    val pool=new JedisPool(config,"wqm02s",6379)
    //获取jedis
    val jedis=pool.getResource
    jedis.auth("123456")

    println(jedis.ping())

  }
}
