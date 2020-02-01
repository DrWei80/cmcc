package streaming

import org.apache.spark.rdd.RDD
import redis.clients.jedis.Jedis

object dealWithBusinessByRedis {
  // 1.充值订单量, 充值金额, 充值成功数
  def num1(businessProfileData: RDD[(Int, String, Int, Long, String, String, String, String)],jedis:Jedis): Unit ={
    businessProfileData.collect().foreach{case(a,b,c,d,e,f,g,h)=>{
      jedis.incrBy("orders",a)
      jedis.incrByFloat("reChargeAmount",b.toDouble)
      jedis.incrBy("reChargeSuccessCounts",c)
    }}
  }

  def num2(businessProfileData:RDD[(Int, String, Int, Long, String, String, String, String)],jedis:Jedis)={
    businessProfileData.collect().foreach{case(a,b,c,d,e,f,g,h)=>{
      jedis.hincrBy(e,f,a.toLong)
    }}
  }
}
