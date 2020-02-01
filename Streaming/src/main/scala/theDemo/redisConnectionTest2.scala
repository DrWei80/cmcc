package theDemo

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

    hsetDemo(jedis)
//    listDemo(jedis)
//    zsetDemo(jedis)
//    stringDemo(jedis)
//    setDemo(jedis)

  }
  /**
    * Hash 散列
    */
  def hsetDemo(jedis:Jedis)={
    jedis.hset("user","name","zhangsan")
    jedis.hset("user","age","28")
    // 获取某一个key值
    println(jedis.hget("user", "age"))
    // 获取所有
    println(jedis.hgetAll("user"))
    // 自增
    jedis.hincrBy("user","age",1)
    // 获取hash字段数量
    println(jedis.hlen("user"))
    // 获取多个值
    println(jedis.hmget("user", "age", "name"))
    // 获取所有的value值
    println(jedis.hvals("user"))
    // 获取所有的key
    println(jedis.hkeys("user"))
    // 删除字段
    jedis.hdel("user","age")
  }

  /**
    * 队列 List
    */

  def listDemo(jedis:Jedis)={
    jedis.lpush("name","wqm","zyx")
    jedis.rpush("name","猪","55")
    println(jedis.lrange("name", 0, -1))
    jedis.lpop("name")
    println(jedis.lrange("name", 0, -1))
  }
  //zset
//        元素不重复，有序集合
//       元素排序按照score排序
//       score值越大，排名越靠后
  def zsetDemo(jedis:Jedis)={
  jedis.zadd("price",1,"5")
  jedis.zadd("price",2,"4")
  jedis.zadd("price",3,"3")
  jedis.zadd("price",4,"2")
  jedis.zadd("price",5,"1")
  println(jedis.zrange("price", 0, -1))
  jedis.zincrby("price",10,"3")
  println(jedis.zrange("price", 0, -1))
  println(jedis.zscore("price", "3"))
  }

  //操作Set,不重复&无序
  def setDemo(jedis:Jedis)={
    //向一个set中添加元素
    jedis.sadd("city","北京","北京","北京","深圳","广州")
    jedis.sadd("flower","梅","玫瑰","牡丹","食人")
    println(jedis.smembers("city"))
    println(jedis.smembers("flower"))
    jedis.srem("flower","梅","玫瑰","牡丹")
    println(jedis.smembers("flower"))
    //合并set
    jedis.sunionstore("city&flower","city","flower")
    println(jedis.smembers("city&flower"))
    //获取随机元素
    println(jedis.srandmember("city", 2))
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
