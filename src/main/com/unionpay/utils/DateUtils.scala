package com.unionpay.utils

import java.text.SimpleDateFormat
import java.util.Calendar

/**
  * 日期工具类
  * Created by tzq on 2016/10/14.
  */
object DateUtils {

  val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

  /**
    * 获取指定时间的昨天日期（格式：2016-10-13）
    * @param start_dt 指定时间
    * @return
    */
  def getYesterdayByJob(start_dt:String):String= {

    val cal: Calendar = Calendar.getInstance()
    cal.setTime(dateFormat.parse(start_dt))

    cal.add(Calendar.DATE, -1)
    val yesterday = dateFormat.format(cal.getTime())
    yesterday
  }


  /**
    * 增加一天
    * @param start_dt 指定时间
    * @return
    */
  def addOneDay(start_dt:String):String= {

    val cal: Calendar = Calendar.getInstance()
    cal.setTime(dateFormat.parse(start_dt))
    cal.add(Calendar.DATE, 1)
    val one = dateFormat.format(cal.getTime())
     one
  }



  /**
    * 获取当前系统时间的昨天日期（格式：2016-10-13）
    * @return
    */
  def getYesterday():String= {
    val cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    val yesterday = dateFormat.format(cal.getTime())
    yesterday
  }

  /**
    * 获取开始日期和结束日期质之间的间隔天数
    * @param start_dt
    * @param end_dt
    * @return
    */
   def getIntervalDays(start_dt:String,end_dt:String):Long={
     val days=(dateFormat.parse(end_dt).getTime-dateFormat.parse(start_dt).getTime)/(1000*3600*24)
     days
   }

  /**
    * MAIN TEST FOR DATEUTILS FUNCTION
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val start_dt="2016-09-08"
    val end_dt="2016-09-09"
    println(getYesterday())
    println(getYesterdayByJob(start_dt))
    println(getIntervalDays(start_dt,end_dt))
    println(addOneDay(end_dt))
  }
}
