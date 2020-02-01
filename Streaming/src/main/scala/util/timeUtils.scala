package util

import java.text.SimpleDateFormat

object timeUtils {
  def costtime(startTime: String, endTime: String) = {
    val time = startTime.substring(0,17)
    val df = new SimpleDateFormat("yyyyMMddHHmmssSSS")
    // 开始 和 结束
    val st = df.parse(time).getTime
    val et = df.parse(endTime).getTime
    et - st
  }
}
