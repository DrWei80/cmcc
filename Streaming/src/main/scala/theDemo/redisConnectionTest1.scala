package theDemo

import redis.clients.jedis.Jedis

object redisConnectionTest1 {
  def main(args: Array[String]): Unit = {
    val jedis = new Jedis("wqm02s", 6379)
    jedis.auth("123456")

    println(jedis.ping())
    jedis.set("a","我是你爸爸")
    val val1=jedis.get("a")
    println(val1)
  }
}
