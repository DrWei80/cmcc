package util
import java.sql.{Connection, DriverManager, SQLException}
import java.util

object DBUtils {

  private val max_connection = 10 //连接池总数
  private val connection_num = 10 //产生连接数
  private var current_num = 0 //当前连接池已产生的连接数
  private val pools = new util.LinkedList[Connection]() //连接池
  /**
    * 加载驱动
    */
  private def before() {
    if (current_num > max_connection && pools.isEmpty()) {
      Thread.sleep(2000)
      before()
    } else {
      Class.forName("com.mysql.jdbc.Driver")
    }
  }
  /**
    * 获得连接
    */
  private def initConn(): Connection = {
    val conn = DriverManager.getConnection(
      "jdbc:mysql://localhost:3306/spark1",
      "root",
      "123456"
    )
    conn
  }
  /**
    * 初始化连接池
    */
  private def initConnectionPool(): util.LinkedList[Connection] = {
    AnyRef.synchronized({
      if (pools.isEmpty()) {
        before()
        for (i <- 1 to connection_num.toInt) {
          pools.push(initConn())
          current_num += 1
        }
      }
      pools
    })
  }
  /**
    * 获得连接
    */
  def getConn():Connection={
    initConnectionPool()
    pools.poll()
  }
  /**
    * 释放连接
    */
  def releaseCon(con:Connection){
    pools.push(con)
  }
}
