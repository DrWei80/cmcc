import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

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

    stringDemo(jedis)

  }
  //操作字符串
  def stringDemo(jedis:Jedis)={
    jedis.set("name","胡锦涛")
    println(jedis.get("name"))

    jedis.append("name","很帅")
    println(jedis.get("name"))

    jedis.getSet("count","100")//替换
    jedis.incr("count")
    println(jedis.get("count"))
    jedis.incrBy("count",99)
    println(jedis.get("count"))


    jedis.decr("count")
    jedis.decrBy("count",38)
    jedis.incrByFloat("count",0.009)//变成浮点数之后不能再操作整数
    println(jedis.get("count"))

    println(jedis.strlen("name"))//获取长度

    jedis.mset("age","19","sex","女","address","北京天安门")

    println(jedis.mget("name", "age", "sex", "address", "count"))//返回一个List



  }
}
