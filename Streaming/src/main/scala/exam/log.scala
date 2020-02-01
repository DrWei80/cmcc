package exam

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object log {



  /**
    * 统计log数据，过滤不符合规则数据（未写薪资）
    * 1. 统计年薪岗位占总岗位的比例？
    * 2. 统计各个省市的平均薪资和岗位数量
    * （薪资取最高，例如1-1.5万，取1.5万）？
    * 3. 统计薪资（薪资取最高，例如1-1.5万，取1.5万）
    * 超过2万的岗位数量，将结果保存Redis即可。
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
      .setAppName("log")
      .setMaster("local[*]")
    val ssc=new SparkContext(conf)
    val jobData=ssc
      .textFile("D:/课件/sz1902/day05/51job.txt")
      .filter(line=>{
        line.split("\\s+").length==5
      })
    //大数据架构师	青岛海联软件科技有限公司	青岛	0.8-1.3万/月	11-22
    //大数据开发工程师	广州餐道信息科技有限公司	广州	1.2-2万/月	11-22
    //0                 1                 2     3         4

//1. 统计年薪岗位占总岗位的比例？
    num1(jobData)//    年薪岗位占总岗位的比例为0.06674757281553398
//    2. 统计各个省市的平均薪资和岗位数量
    //（薪资取最高，例如1-1.5万，取1.5万）？
    num2(jobData)
    //    3. 统计薪资（薪资取最高，例如1-1.5万，取1.5万）
    //    超过2万的岗位数量，将结果保存Redis即可。
    num3(jobData)

  }

  def num3(jobData: RDD[String]): Unit = {
    jobData.map(line=>{
        val data=line.split("\\s+")
        val salary=data(3)
        ("1",(transformSalary(salary),1))
      })
      .filter(line=>{
        line._2._1>2
      })
      .map(line=>{
        (line._1,line._2._2)
      })
      .reduceByKey(_+_)
      .foreach(line=>{
        println(s"薪资超过2万的岗位数量为:${line._2}")
      })
  }


  def num2(jobData: RDD[String]): Unit = {
    jobData.
      map(line=>{
        val data=line.split("\\s+")
        val province=data(2)
        val salary=data(3)
        (province,(transformSalary(salary),1))
      }).reduceByKey((a,b)=>{
        (a._1+b._1,a._2+b._2)
    }).map(line=>{
      (line._1,line._2._1/line._2._2,line._2._2)
    }).foreach(line=>{
      println(s"${line._1}的平均薪资为:${line._2.formatted("%.2f")}万/月,共有${line._3}个岗位")
    })
  }
  //转换单位为 万/月
  def transformSalary(str:String):Double={
    var salaryNum=str.substring(str.indexOf("-")+1,str.indexOf("/")-1).toDouble
    val salaryUnit=str.substring(str.indexOf("/")-1,(str.indexOf("/")))
    val salaryTime=str.substring(str.indexOf("/")+1)
    if(salaryUnit.equals("千")){
      salaryNum=salaryNum/10
    }else if(salaryUnit.equals("元")){
      salaryNum=salaryNum/10000
    }
    if(salaryTime.equals("年")){
      salaryNum=salaryNum/12
    }
    salaryNum
  }


  def num1(jobData:RDD[String])={
    jobData
      .map(line=>{
        val data=line.split("\\s+")
        var yearJob=0
        var totalJob=0
        if(data(3).contains("年")){
          yearJob=1
          totalJob=1
        }else{
          yearJob=0
          totalJob=1
        }
        ("1",(totalJob,yearJob))
      })
      .reduceByKey((a, b) => {
        (a._1 + b._1, a._2 + b._2)
      })
      .map(line=>{
        val yearJ=line._2._2
        val totalJ=line._2._1
        val num =yearJ.toDouble/totalJ.toDouble
        num
      }).foreach(line=>{
      println(s"年薪岗位占总岗位的比例为${line}")
    })
  }
}
