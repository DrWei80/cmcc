package streaming

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import util.DBUtils

object dealWithBusinessByMySQL {
  //以省份为维度统计订单量排名前 10 的省份数据,
  // 并且统计每个省份的订单成功率，
  // 只保留一位小数，存入MySQL中，进行前台页面显示
  def num2(businessProfileData: RDD[(Int, String, Int, Long, String, String, String, String)], ssc: StreamingContext) = {
    val reduce=businessProfileData
      .map(line=>{
        //省份，订单数，成功数
        (line._8,List(line._1,line._3))
      })
      .reduceByKey((a,b)=>{
        // 拉链操作，将第一条数据的第一个元素与第二条数据的第一个元素放在一个元组
        // list(1,2).zip(list(1,2)) = list((1,1),(2,2))
        a.zip(b).map(line=>{
          line._1+line._2
        })
      })
      .sortBy(_._2(0))
      .map(t=>(t._1,t._2(0),(t._2(1)/t._2(0))*100))
      .take(10)
    val line=ssc
      .sparkContext
      .makeRDD(reduce)
      .foreachPartition(line=>{
        val conn=DBUtils.getConn()
        line.foreach(t=>{
          val sql=s"insert into top10 (province,count,success)" +
            s"values('${t._1}',${t._2},${t._3})"
          val stat=conn.createStatement()
          stat.executeUpdate(sql)
        })
        DBUtils.releaseCon(conn)
      })


    null
  }



  def num1(businessProfileData: RDD[(Int, String, Int, Long, String, String, String, String)]) = {

    //统计每小时各个省份的充值失败数据量
    //充值失败=订单数-充值成功数
    businessProfileData
      .map(line=>{
        ((line._8,line._7),line._1-line._3)//省，时间，数量
      })
      .reduceByKey(_+_)
      .foreachPartition(line=>{
        val conn=DBUtils.getConn
        line.foreach(line=>{
          val sql=s"insert into provinceHours (province,times,counts) " +
            s"values('${line._1._1}','${line._1._2}','${line._2}')"
          val stm=conn.createStatement()
          stm.executeUpdate(sql)
        })
        DBUtils.releaseCon(conn)
      })



  }


}
