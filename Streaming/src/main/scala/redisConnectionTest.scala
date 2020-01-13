import redis.clients.jedis.Jedis

object redisConnectionTest {
  def main(args: Array[String]): Unit = {
    val jedis = new Jedis("wqm02s", 6379)
    jedis.auth("123456")
    println(jedis.ping())
  }
}
