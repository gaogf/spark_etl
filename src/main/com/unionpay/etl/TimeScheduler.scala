package com.unionpay.etl
import java.text.SimpleDateFormat
import java.util.Date
import com.unionpay.jdbc.UPSQL_TIMEPARAMS_JDBC
import com.unionpay.utils.DateUtils
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 重抽历史设置调度的时间区间，接收2个时间参数,格式为：yyyy-MM-dd
  * 每日每日调度不传参
  * Created by tzq on 2016/10/19.
  */
object TimeScheduler {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TimeScheduler")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new HiveContext(sc)

    //先获取并查看上一次的调度日期
    val rowParams = UPSQL_TIMEPARAMS_JDBC.readTimeParams(sqlContext)
    println("###上次调度的是日期为：start_dt:" + rowParams.getString(0) + "  end_dt:" + rowParams.getString(1))
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")


    //注： 传入参数则取参数日期，没有参数则默认系统时间减一天
    val start_dt = if (args.length >= 2) args(0) else DateUtils.getYesterdayByJob(DateUtils.dateFormat.format(new Date()))
    val end_dt = if (args.length >= 2) args(1) else DateUtils.getYesterdayByJob(DateUtils.dateFormat.format(new Date()))


    //注： 判断传入参数是否合理,功能开发中
//    println("请输入重抽历史的开始时间")
//    val lineDate_st =Console.readLine()
//    val date_st = dateFormat.parse(lineDate_st)
//    if (lineDate_st.equals(dateFormat.format(date_st))) {
//      System.out.println("Date is " + date_st.toString())
//      val start_dt =dateFormat.format(date_st)
//    } else {
//      throw new IllegalArgumentException("Illegal date!")
//    }
//
//    println("请输入重抽历史的结束时间")
//    val lineDate_end =Console.readLine()
//    val date_end = dateFormat.parse(lineDate_end)
//    if (lineDate_end.equals(dateFormat.format(date_end))) {
//      System.out.println("Date is " + date_end.toString())
//      val end_dt =dateFormat.format(date_st)
//    } else {
//      throw new IllegalArgumentException("Illegal date!")
//    }
//
//    if(dateFormat.format(date_end)> dateFormat.format(date_st))
//      println("日期都合理，程序开始运行")
//    else
//      println("结束时间小于开始时间，日期不合理，程序未运行")



    UPSQL_TIMEPARAMS_JDBC.save2Mysql(sc, sqlContext, start_dt, end_dt)
    println(s"###保存当前调度的时间(start_dt:$start_dt,end_dt:$end_dt)成功！")


  }
}

