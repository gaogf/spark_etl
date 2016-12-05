package com.unionpay.utils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

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
    * 获取当前系统时间，格式为：yyyy-MM-dd HH:mm:ss
    * @return
    */
  def getCurrentSystemTime():String={
    val now:Date = new Date()
    val dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    dateFormat.format( now )
  }

  /**
    * 计算方法运行时间开销
    * @param actionName 传入当前JOB名称
    * @param action  待测试代码
    * @tparam U
    * @return
    */
  def timeCost[U](actionName: String)(action: => U): Unit = {
    val start = System.currentTimeMillis()
    val res = action
    val ss:Int =((System.currentTimeMillis() - start)/1000).toInt
    val MM:Int = ss/60
    val hh:Int = MM/60
    val dd:Int = hh/24
    println(s"[TimeCost] $actionName "+dd+"d "+(hh-dd*24)+"h "+(MM-hh*60)+"m "+(ss-MM*60)+"s " , "Total："+(System.currentTimeMillis() - start)+"ms ")
  }

}
