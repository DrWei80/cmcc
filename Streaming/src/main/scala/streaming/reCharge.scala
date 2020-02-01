package streaming

import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import util.{DBUtils, KafkaOffsetZK, jedisUtils, timeUtils}

/**
  * 充值通知
  */
object reCharge {

  def main(args: Array[String]): Unit = {

    // 屏蔽log
    Logger.getLogger("org").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName("reCharge").setMaster("local[*]")
      // 反压机制 1.5以前的
      // .set("spark.streaming.kafka.maxRatePerPartition","1000")
      // 背压机制 1.5 以后功能，动态限速
      .set("spark.streaming.backpressure.enabled","true")
    val ssc = new StreamingContext(conf,Seconds(5))
    // 配置节点信息
    val BOOTSTRAP_SERVER = "wqm01s:9092,wqm02s:9092,wqm03s:9092"
    val ZK_SERVER = "wqm01s:2181,wqm02s:2181,wqm03s:2181"
    // 消费者组
    val GROUP_ID = "sz_consumer"
    // topic
    val topics = Array("wtf01")
    // 配置Kafka参数
    val kafkaParams = Map[String,Object](
      "bootstrap.servers"-> BOOTSTRAP_SERVER,
      // kafka需要配置解码器
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      // 配置消费者组
      "group.id"-> GROUP_ID,
      // 设置从头消费
      "auto.offset.reset"-> "latest",
      // 设置是否自动提交offset
      "enable.auto.commit"-> "false"
    )
    // 采用Zookeeper手动维护Offset
    val zkManager = new KafkaOffsetZK(ZK_SERVER)
    // 获取offset
    val fromOffset = zkManager.getFromOffset(topics,GROUP_ID)
    // 创建数据流
    var stream:InputDStream[ConsumerRecord[String, String]] = null
    // 判断  如果不是第一次读取数据 offset大于0
    if(fromOffset.size > 0){
      stream = KafkaUtils.createDirectStream(
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics,kafkaParams,fromOffset)
      )
    }else{
      // 如果是第一次消费数据 从0开始读取
      stream = KafkaUtils.createDirectStream[String,String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics,kafkaParams)
      )
      println("第一次消费数据")
    }

    val jedis=jedisUtils.getJedis()


    val city=ssc
      .sparkContext
      .textFile("e:/sparkData/city.txt")

    val cityMap=city
      .map(line=>{
        val data=line.split("\\s+")
        (data(0),data(1))
      }).collectAsMap()

    val broadcastMapOfCity=ssc.sparkContext.broadcast(cityMap)


    // 处理数据流
    stream.foreachRDD(rdd=>{
      // 为了后续更新Offset做准备
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      // 输出  逻辑计算在这地方，可以存储Hbase 或者 HDFS


      val businessProfileData: RDD[(Int, String, Int, Long, String, String, String, String)] =rdd
        // 获取value
        .map(t=>JSON.parseObject(t.value()))
        .filter(t=>{
          //过滤留下接口连接成功的数据
          t.getString("serviceName").equalsIgnoreCase("reChargeNotifyReq")&&
          t.getString("interFacRst").equals("0000")
        })
        // 字段处理
        .map(t=>{
        // 将字符串转换为JSON串

//        1.充值订单量, 充值金额, 充值成功数
        //serviceName:reChargeNotifyReq
        //chargefee:?
        //bussinessRst:0000
        //interFacRst:0000
        //2.实时充值业务办理趋势, 主要统计全网每分钟的订单量数据
        // requestId
        // receiveNotifyTime充值结束时间
        val reChargeAmount=t.getString("chargefee")

        val reChargeSuccessCounts={
          if(t.getString("bussinessRst").equalsIgnoreCase("0000")) 1 else 0
        }

        val startTime=t.getString("requestId")
        val endTime=t.getString("receiveNotifyTime")
        val reChargeTime={
          if(reChargeSuccessCounts==1) timeUtils.costtime(startTime,endTime) else 0
        }

        val province=broadcastMapOfCity.value.getOrElse(t.getString("provinceCode"),"UnknowCity")


        (1,reChargeAmount,
          reChargeSuccessCounts,reChargeTime,
          startTime.substring(0,8),startTime.substring(0,12),
        startTime.substring(0,10),province)//充值订单量, 充值金额, 充值成功数
      }).cache()



      //处理业务1、2
      dealWithBusinessByRedis.num1(businessProfileData,jedis)
      dealWithBusinessByRedis.num2(businessProfileData,jedis)

      //统计每小时各个省份的充值失败数据量
//      dealWithBusinessByMySQL.num1(businessProfileData)

      //以省份为维度统计订单量排名前 10 的省份数据,
      // 并且统计每个省份的订单成功率，
      // 只保留一位小数，存入MySQL中，进行前台页面显示
      dealWithBusinessByMySQL.num2(businessProfileData,ssc)

      // 更新Offset
      zkManager.UpdateOffset(offsetRanges,GROUP_ID)
    })

    // 启动程序
    ssc.start()
    ssc.awaitTermination()
  }

}
