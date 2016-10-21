package com.unionpay.etl
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
    val rowParams=UPSQL_TIMEPARAMS_JDBC.readTimeParams(sqlContext)
    println("###上次调度的是日期为：start_dt:"+rowParams.getString(0)+"  end_dt:"+rowParams.getString(1))

    //注： 传入参数则取参数日期，没有参数则默认系统时间减一天
   val start_dt=if(args.length>=2) args(0) else DateUtils.getYesterdayByJob(DateUtils.dateFormat.format(new Date()))
   val end_dt=if(args.length>=2) args(1) else DateUtils.getYesterdayByJob(DateUtils.dateFormat.format(new Date()))

    UPSQL_TIMEPARAMS_JDBC.save2Mysql(sc,sqlContext,start_dt,end_dt)
    println(s"###保存当前调度的时间(start_dt:$start_dt,end_dt:$end_dt)成功！")

  }

}

