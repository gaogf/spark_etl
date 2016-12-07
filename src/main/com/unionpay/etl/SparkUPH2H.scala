package com.unionpay.etl
import java.text.SimpleDateFormat

import com.unionpay.conf.ConfigurationManager
import com.unionpay.constant.Constants
import com.unionpay.jdbc.UPSQL_TIMEPARAMS_JDBC
import com.unionpay.utils.DateUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Days, LocalDate}

/**
  * 作业：抽取银联Hive的数据到钱包Hive数据仓库
  */
object SparkUPH2H {
  //UP NAMENODE URL
  private val up_namenode=ConfigurationManager.getProperty(Constants.UP_NAMENODE)
  //UP HIVE DATA ROOT URL
  private val up_hivedataroot=ConfigurationManager.getProperty(Constants.UP_HIVEDATAROOT)
  //指定HIVE数据库名
  private lazy val hive_dbname =ConfigurationManager.getProperty(Constants.HIVE_DBNAME)
  private  lazy  val dateFormatter=DateTimeFormat.forPattern("yyyy-MM-dd")
  private  lazy  val dateFormat_2=DateTimeFormat.forPattern("yyyyMMdd")

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkUPH2H")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new HiveContext(sc)

    val rowParams=UPSQL_TIMEPARAMS_JDBC.readTimeParams(sqlContext)
    val start_dt=DateUtils.getYesterdayByJob(rowParams.getString(0))  //获取开始日期：start_dt-1
    val end_dt=rowParams.getString(1)//结束日期

    println(s"#### SparkUPH2H 数据抽取的起始日期为: $start_dt --  $end_dt")

    val JobName = if(args.length>0) args(0) else None
    println(s"#### The Current Job Name is ： [$JobName]")
    JobName match {
      /**
        * 每日模板job
        */
      case "JOB_HV_39"  => JOB_HV_39(sqlContext,end_dt) //CODE BY YX
      case "JOB_HV_41"  => JOB_HV_41(sqlContext,start_dt,end_dt) //CODE BY XTP
      case "JOB_HV_49"  => JOB_HV_49 //CODE BY YX
      case "JOB_HV_50"  =>  JOB_HV_50(sqlContext,start_dt,end_dt) //CODE BY XTP
      case "JOB_HV_52"  => JOB_HV_52(sqlContext,end_dt) //CODE BY YX
      case "JOB_HV_55"  =>  JOB_HV_55(sqlContext,start_dt,end_dt) //CODE BY TZQ
      case "JOB_HV_56"  =>  JOB_HV_56(sqlContext,start_dt,end_dt) //CODE BY TZQ
      case "JOB_HV_57"  =>  JOB_HV_57(sqlContext,start_dt,end_dt) //CODE BY TZQ
      case "JOB_HV_58"  =>  JOB_HV_58(sqlContext,start_dt,end_dt) //CODE BY XTP
      case "JOB_HV_59"  =>  JOB_HV_59(sqlContext,start_dt,end_dt) //CODE BY XTP
      case "JOB_HV_60"  =>  JOB_HV_60(sqlContext,start_dt,end_dt) //CODE BY XTP
      case "JOB_HV_61"  =>  JOB_HV_61(sqlContext,start_dt,end_dt) //CODE BY XTP
      case "JOB_HV_62"  =>  JOB_HV_62(sqlContext,start_dt,end_dt) //CODE BY XTP
      case "JOB_HV_63"  =>  JOB_HV_63(sqlContext,start_dt,end_dt) //CODE BY XTP
      case "JOB_HV_64"  =>  JOB_HV_64(sqlContext,start_dt,end_dt) //CODE BY XTP
      case "JOB_HV_65"  =>  JOB_HV_65(sqlContext,start_dt,end_dt) //CODE BY XTP
      case "JOB_HV_71"  =>  JOB_HV_71(sqlContext,start_dt,end_dt) //CODE BY XTP

      /**
        * 指标套表job
        */

      case _ => println("#### No Case Job,Please Input JobName")

    }

    sc.stop()
  }


  /**
    * JobName: JOB_HV_39
    * Feature: uphive.rtdtrs_dtl_achis -> hive.hive_achis_trans
    * @author YangXue
    * @time 2016-08-30
    * @param sqlContext,end_dt
    */

  def JOB_HV_39(implicit sqlContext: HiveContext,end_dt:String) = {
    println("#### JOB_HV_39(rtdtrs_dtl_achis -> hive_achis_trans)")

    val today_dt = end_dt
    println("#### JOB_HV_39 增量抽取的时间范围为: "+end_dt)

    DateUtils.timeCost("JOB_HV_39") {
      val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/incident/ods/hive_achis_trans/part_settle_dt=$today_dt")
      println(s"#### JOB_HV_39 read $up_namenode/ 数据完成时间为:"+DateUtils.getCurrentSystemTime())

      df.registerTempTable("spark_hive_achis_trans")
      println("#### JOB_HV_39 registerTempTable--spark_hive_achis_trans 完成的系统时间为:"+DateUtils.getCurrentSystemTime())

      if(!Option(df).isEmpty){
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql(s"insert overwrite table hive_achis_trans partition(part_settle_dt='$today_dt') select * from spark_hive_achis_trans")
        println("#### JOB_HV_39 分区数据插入完成的时间为："+DateUtils.getCurrentSystemTime())
      } else {
        println(s"#### JOB_HV_39 read $up_namenode/ 无数据！")
      }
    }
  }


  /**
    * hive-job-41 2016-11-03
    * rtdtrs_dtl_cups to hive_cups_trans
    * @author Xue
    * @param sqlContext
    */

    // Xue create function about partition by date ^_^
    def JOB_HV_41(implicit sqlContext: HiveContext,start_dt: String, end_dt: String)  {
      var sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val start = LocalDate.parse(start_dt, dateFormatter)
      val end = LocalDate.parse(end_dt, dateFormatter)
      val days = Days.daysBetween(start, end).getDays
      val dateStrs = for (day <- 0 to days) {
        val currentDay = (start.plusDays(day).toString(dateFormatter))
        val currentDay_ft = (start.plusDays(day).toString(dateFormat_2))
        println(currentDay_ft)
        val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/incident/ods/hive_cups_trans/part_settle_dt=$currentDay")
        println(s"###### read $up_namenode/ successful ######")
        df.registerTempTable("spark_hive_cups_trans")

        println(s"=========插入'$currentDay'分区的数据=========")
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql(s"alter table hive_cups_trans drop partition (part_settle_dt='$currentDay')")
        println(s"alter table hive_cups_trans drop partition (part_settle_dt='$currentDay') successfully!")
        sqlContext.sql(s"insert into hive_cups_trans partition (part_settle_dt='$currentDay') select * from spark_hive_cups_trans htempa where htempa.hp_settle_dt='$currentDay_ft'")
        println(s"insert into hive_cups_trans partition (part_settle_dt='$currentDay') successfully!")
      }
    }


  /**
    * JobName: JOB_HV_49
    * Feature: uphive.rtapam_prv_ucbiz_cdhd_bas_inf -> hive_ucbiz_cdhd_bas_inf
    * @author YangXue
    * @time 2016-09-14
    * @param sqlContext
    */
  def JOB_HV_49(implicit sqlContext: HiveContext) = {
    println("#### JOB_HV_49(rtapam_prv_ucbiz_cdhd_bas_inf -> hive_ucbiz_cdhd_bas_inf)")
    println("#### JOB_HV_49 为全量抽取的表")

    DateUtils.timeCost("JOB_HV_49"){
      val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/participant/user/hive_ucbiz_cdhd_bas_inf")
      println(s"#### JOB_HV_49 read $up_namenode/ 数据完成时间为:"+DateUtils.getCurrentSystemTime())

      df.registerTempTable("spark_ucbiz_cdhd_bas_inf")
      println("#### JOB_HV_49 registerTempTable--spark_ucbiz_cdhd_bas_inf 完成的系统时间为:"+DateUtils.getCurrentSystemTime())

      if(!Option(df).isEmpty){
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql("insert overwrite table hive_ucbiz_cdhd_bas_inf select * from spark_ucbiz_cdhd_bas_inf")
        println("#### JOB_HV_49 全量数据插入完成的时间为："+DateUtils.getCurrentSystemTime())
      }else{
        println(s"#### JOB_HV_49 read $up_namenode/ 无数据！")
      }
    }
  }


  /**
    * hive-job-50 2016-11-03
    * org_tdapp_keyvalue to hive_org_tdapp_keyvalue
    * @author Xue
    * @param sqlContext
    */

    // Xue create function about partition by date ^_^
    def JOB_HV_50(implicit sqlContext: HiveContext,start_dt: String, end_dt: String)  {

      var sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val start = LocalDate.parse(start_dt, dateFormatter)
      val end = LocalDate.parse(end_dt, dateFormatter)
      val days = Days.daysBetween(start, end).getDays
      val dateStrs = for (day <- 0 to days) {
        val currentDay = (start.plusDays(day).toString(dateFormatter))
        println("######JOB_HV_50######")
        val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/incident/td/hive_org_tdapp_keyvalue/part_updays=$currentDay")
        println(s"###### read $up_namenode/ successful ######")
        df.registerTempTable("spark_hive_org_tdapp_keyvalue")

        val daytime:DataFrame = sqlContext.sql(
          s"""
              |select distinct
              |daytime
              |from spark_hive_org_tdapp_keyvalue
            """.stripMargin
            )
        daytime.show()

        val time_lsit = daytime.select("daytime").rdd.map(r => r(0).asInstanceOf[String]).collect().toList

        increase_ListBuffer(time_lsit)

        def increase_ListBuffer(list:List[String]) :List[String]={
          import scala.collection.mutable.ListBuffer
          var result = ListBuffer[String]()
          for(element <- list){
            val dt = DateTime.parse(element,dateFormat_2)
            val days = element
            val days_fmt = dateFormatter.print(dt)
            sqlContext.sql(s"use $hive_dbname")
            sqlContext.sql(s"alter table hive_org_tdapp_keyvalue drop partition (part_daytime='$days_fmt',part_updays='$currentDay')")
            println(s"alter table hive_org_tdapp_keyvalue drop partition (part_daytime='$days_fmt',part_updays='$currentDay') successfully!")
            sqlContext.sql(s"insert into hive_org_tdapp_keyvalue partition (part_daytime='$days_fmt',part_updays='$currentDay') select * from spark_hive_org_tdapp_keyvalue htempa where htempa.daytime='$days'")
            println(s"insert into hive_org_tdapp_keyvalue partition (part_daytime='$days_fmt',part_updays='$currentDay') successfully!")
            result += element+1
          }
          result.toList
        }
      }
    }


  /**
    * JobName: JOB_HV_52
    * Feature: uphive.stmtrs_bsl_active_card_acq_branch_mon1 -> hive.hive_active_card_acq_branch_mon
    * @author YangXue
    * @time 2016-08-29
    * @param sqlContext,end_dt
    */
  def JOB_HV_52(implicit sqlContext: HiveContext, end_dt: String) = {
    println("#### JOB_HV_52(stmtrs_bsl_active_card_acq_branch_mon1 -> hive_active_card_acq_branch_mon)")

    val part_dt = end_dt.substring(0, 7)
    println("#### JOB_HV_52 增量抽取的时间范围为: " +part_dt)

    DateUtils.timeCost("JOB_HV_52") {
      val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/product/card/hive_active_card_acq_branch_mon/part_settle_month=$part_dt")
      println(s"#### JOB_HV_52 read $up_namenode/ 数据完成时间为:" + DateUtils.getCurrentSystemTime())

      df.registerTempTable("spark_active_card_acq_branch_mon")
      println("#### JOB_HV_52 registerTempTable--spark_active_card_acq_branch_mon 完成的系统时间为:" + DateUtils.getCurrentSystemTime())

      if(!Option(df).isEmpty){
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql(
          s"""
             |insert overwrite table hive_active_card_acq_branch_mon partition(part_settle_month='$part_dt')
             |select * from spark_active_card_acq_branch_mon
       """.stripMargin)
        println("#### JOB_HV_52 分区数据插入完成的时间为："+DateUtils.getCurrentSystemTime())
      }else{
        println(s"#### JOB_HV_52 read $up_namenode/ 无数据！")
      }
    }
  }


  /**
    * hive-job-55 2016-11-15
    * org_tdapp_tactivity to  hive_org_tdapp_tactivity
    * @author tzq
    * @param sqlContext
    */
  def JOB_HV_55(implicit sqlContext: HiveContext,start_dt: String,end_dt:String) = {
    val start = LocalDate.parse(start_dt, dateFormatter)
    val end = LocalDate.parse(end_dt, dateFormatter)
    val days = Days.daysBetween(start, end).getDays
    for (day <- 0 to days) {
      val currentDay = (start.plusDays(day).toString(dateFormatter))
      println("######JOB_HV_55######")
      val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/incident/td/hive_org_tdapp_tactivity/part_updays=$currentDay")
      println(s"###### read $up_namenode/ successful ######")
      df.registerTempTable("spark_hive_org_tdapp_tactivity")

      val daytime:DataFrame = sqlContext.sql(
        s"""
           |select distinct
           |daytime
           |from spark_hive_org_tdapp_tactivity
            """.stripMargin
      )
      val times = daytime.select("daytime").rdd.map(r => r(0).asInstanceOf[String]).collect().toList

      increase_ListBuffer(times)

      def increase_ListBuffer(list:List[String]) {

        for(element <- list){
          val dt = DateTime.parse(element,dateFormat_2)
          val days = element
          val days_fmt = dateFormatter.print(dt)

          sqlContext.sql(s"use $hive_dbname")
          sqlContext.sql(s"alter table hive_org_tdapp_tactivity drop partition (part_daytime='$days_fmt',part_updays='$currentDay')")
          println(s"alter table hive_org_tdapp_tactivity drop partition (part_daytime='$days_fmt',part_updays='$currentDay') successfully!")

          sqlContext.sql(s"insert into hive_org_tdapp_tactivity partition (part_daytime='$days_fmt',part_updays='$currentDay') select * from spark_hive_org_tdapp_tactivity hott where hott.daytime='$days'")
          println(s"insert into hive_org_tdapp_tactivity partition (part_daytime='$days_fmt',part_updays='$currentDay') successfully!")

        }
      }
    }
  }

  /**
    * hive-job-56 2016-11-24
    * org_tdapp_tlaunch to  hive_org_tdapp_tlaunch
    * @author tzq
    * @param sqlContext
    */
  def JOB_HV_56(implicit sqlContext: HiveContext,start_dt: String,end_dt:String) = {
    val start = LocalDate.parse(start_dt, dateFormatter)
    val end = LocalDate.parse(end_dt, dateFormatter)
    val days = Days.daysBetween(start, end).getDays
     for (day <- 0 to days) {
      val currentDay = (start.plusDays(day).toString(dateFormatter))
      println("######JOB_HV_56######")
      val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/incident/td/hive_org_tdapp_tlaunch/part_updays=$currentDay")
      println(s"###### read $up_namenode/ successful ######")
      df.registerTempTable("spark_hive_org_tdapp_tlaunch")

      val daytime:DataFrame = sqlContext.sql(
        s"""
           |select distinct
           |daytime
           |from spark_hive_org_tdapp_tlaunch
            """.stripMargin
      )
      val times = daytime.select("daytime").rdd.map(r => r(0).asInstanceOf[String]).collect().toList

      increase_ListBuffer(times)

      def increase_ListBuffer(list:List[String]) {

        for(element <- list){
          val dt = DateTime.parse(element,dateFormat_2)
          val days = element
          val days_fmt = dateFormatter.print(dt)

          sqlContext.sql(s"use $hive_dbname")
          sqlContext.sql(s"alter table hive_org_tdapp_tlaunch drop partition (part_daytime='$days_fmt',part_updays='$currentDay')")
          println(s"alter table hive_org_tdapp_tlaunch drop partition (part_daytime='$days_fmt',part_updays='$currentDay') successfully!")
          sqlContext.sql(s"insert into hive_org_tdapp_tlaunch partition (part_daytime='$days_fmt',part_updays='$currentDay') select * from spark_hive_org_tdapp_tlaunch hott where hott.daytime='$days'")
          println(s"insert into hive_org_tdapp_tlaunch partition (part_daytime='$days_fmt',part_updays='$currentDay') successfully!")

        }
      }
    }
  }


  /**
    * hive-job-57   2016-11-24
    * org_tdapp_terminate to  hive_org_tdapp_terminate
    * @author tzq
    * @param sqlContext
    */
  def JOB_HV_57(implicit sqlContext: HiveContext,start_dt: String,end_dt:String) = {
    val start = LocalDate.parse(start_dt, dateFormatter)
    val end = LocalDate.parse(end_dt, dateFormatter)
    val days = Days.daysBetween(start, end).getDays
    for (day <- 0 to days) {
      val currentDay = (start.plusDays(day).toString(dateFormatter))
      println("######JOB_HV_57######")
      val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/incident/td/hive_org_tdapp_terminate/part_updays=$currentDay")
      println(s"###### read $up_namenode/ successful ######")
      df.registerTempTable("spark_hive_org_tdapp_terminate")

      val daytime:DataFrame = sqlContext.sql(
        s"""
           |select distinct
           |daytime
           |from spark_hive_org_tdapp_terminate
            """.stripMargin
      )
      val times = daytime.select("daytime").rdd.map(r => r(0).asInstanceOf[String]).collect().toList

      increase_ListBuffer(times)

      def increase_ListBuffer(list:List[String]) {

        for(element <- list){
          val dt = DateTime.parse(element,dateFormat_2)
          val days = element
          val days_fmt = dateFormatter.print(dt)

          sqlContext.sql(s"use $hive_dbname")
          sqlContext.sql(s"alter table hive_org_tdapp_terminate drop partition (part_daytime='$days_fmt',part_updays='$currentDay')")
          println(s"alter table hive_org_tdapp_terminate drop partition (part_daytime='$days_fmt',part_updays='$currentDay') successfully!")

          sqlContext.sql(s"insert into hive_org_tdapp_terminate partition (part_daytime='$days_fmt',part_updays='$currentDay') select * from spark_hive_org_tdapp_terminate hott where hott.daytime='$days'")
          println(s"insert into hive_org_tdapp_terminate partition (part_daytime='$days_fmt',part_updays='$currentDay') successfully!")

        }
      }
    }
  }


  /**
    * hive-job-58 2016-11-15
    * org_tdapp_activitynew to hive_org_tdapp_activitynew
    * @author Xue
    * @param sqlContext
    */

  // Xue create function about partition by date ^_^
  def JOB_HV_58(implicit sqlContext: HiveContext,start_dt: String, end_dt: String)  {

    var sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val start = LocalDate.parse(start_dt, dateFormatter)
    val end = LocalDate.parse(end_dt, dateFormatter)
    val days = Days.daysBetween(start, end).getDays
    val dateStrs = for (day <- 0 to days) {
      val currentDay = (start.plusDays(day).toString(dateFormatter))
      println("######JOB_HV_58######")
      val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/incident/td/hive_org_tdapp_activitynew/part_updays=$currentDay")
      println(s"###### read $up_namenode/ successful ######")
      df.registerTempTable("spark_hive_org_tdapp_activitynew")

      val daytime:DataFrame = sqlContext.sql(
        s"""
           |select distinct
           |daytime
           |from spark_hive_org_tdapp_activitynew
            """.stripMargin
      )
      daytime.show(10)

      val time_lsit = daytime.select("daytime").rdd.map(r => r(0).asInstanceOf[String]).collect().toList

      increase_ListBuffer(time_lsit)

      def increase_ListBuffer(list:List[String]) :List[String]={
        import scala.collection.mutable.ListBuffer
        var result = ListBuffer[String]()
        for(element <- list){
          val dt = DateTime.parse(element,dateFormat_2)
          val days = element
          val days_fmt = dateFormatter.print(dt)
          sqlContext.sql(s"use $hive_dbname")
          sqlContext.sql(s"alter table hive_org_tdapp_activitynew drop partition (part_daytime='$days_fmt',part_updays='$currentDay')")
          println(s"alter table hive_org_tdapp_activitynew drop partition (part_daytime='$days_fmt',part_updays='$currentDay') successfully!")
          sqlContext.sql(s"insert into hive_org_tdapp_activitynew partition (part_daytime='$days_fmt',part_updays='$currentDay') select * from spark_hive_org_tdapp_activitynew htempa where htempa.daytime='$days'")
          println(s"insert into hive_org_tdapp_activitynew partition (part_daytime='$days_fmt',part_updays='$currentDay') successfully!")
          result += element+1
        }
        result.toList
      }
    }
  }


  /**
    * hive-job-59 2016-11-16
    * org_tdapp_devicenew to hive_org_tdapp_devicenew
    * @author Xue
    * @param sqlContext
    */

  // Xue create function about partition by date ^_^
  def JOB_HV_59(implicit sqlContext: HiveContext,start_dt: String, end_dt: String)  {

    var sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val start = LocalDate.parse(start_dt, dateFormatter)
    val end = LocalDate.parse(end_dt, dateFormatter)
    val days = Days.daysBetween(start, end).getDays
    val dateStrs = for (day <- 0 to days) {
      val currentDay = (start.plusDays(day).toString(dateFormatter))
      println("######JOB_HV_59######")
      val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/incident/td/hive_org_tdapp_devicenew/part_updays=$currentDay")
      println(s"###### read $up_namenode/ successful ######")
      df.registerTempTable("spark_hive_org_tdapp_devicenew")

      val daytime:DataFrame = sqlContext.sql(
        s"""
           |select distinct
           |daytime
           |from spark_hive_org_tdapp_devicenew
            """.stripMargin
      )
      daytime.show(10)

      val time_lsit = daytime.select("daytime").rdd.map(r => r(0).asInstanceOf[String]).collect().toList

      increase_ListBuffer(time_lsit)

      def increase_ListBuffer(list:List[String]) :List[String]={
        import scala.collection.mutable.ListBuffer
        var result = ListBuffer[String]()
        for(element <- list){
          val dt = DateTime.parse(element,dateFormat_2)
          val days = element
          val days_fmt = dateFormatter.print(dt)
          sqlContext.sql(s"use $hive_dbname")
          sqlContext.sql(s"alter table hive_org_tdapp_devicenew drop partition (part_daytime='$days_fmt',part_updays='$currentDay')")
          println(s"alter table hive_org_tdapp_devicenew drop partition (part_daytime='$days_fmt',part_updays='$currentDay') successfully!")
          sqlContext.sql(s"insert into hive_org_tdapp_devicenew partition (part_daytime='$days_fmt',part_updays='$currentDay') select * from spark_hive_org_tdapp_devicenew htempa where htempa.daytime='$days'")
          println(s"insert into hive_org_tdapp_devicenew partition (part_daytime='$days_fmt',part_updays='$currentDay') successfully!")
          result += element+1
        }
        result.toList
      }
    }
  }


  /**
    * hive-job-60 2016-11-17
    * org_tdapp_eventnew to hive_org_tdapp_eventnew
    * @author Xue
    * @param sqlContext
    */

  // Xue create function about partition by date ^_^
  def JOB_HV_60(implicit sqlContext: HiveContext,start_dt: String, end_dt: String)  {

    var sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val start = LocalDate.parse(start_dt, dateFormatter)
    val end = LocalDate.parse(end_dt, dateFormatter)
    val days = Days.daysBetween(start, end).getDays
    val dateStrs = for (day <- 0 to days) {
      val currentDay = (start.plusDays(day).toString(dateFormatter))
      println("######JOB_HV_60######")
      val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/incident/td/hive_org_tdapp_eventnew/part_updays=$currentDay")
      println(s"###### read $up_namenode/ successful ######")
      df.registerTempTable("spark_hive_org_tdapp_eventnew")

      val daytime:DataFrame = sqlContext.sql(
        s"""
           |select distinct
           |daytime
           |from spark_hive_org_tdapp_eventnew
            """.stripMargin
      )
      daytime.show(10)

      val time_lsit = daytime.select("daytime").rdd.map(r => r(0).asInstanceOf[String]).collect().toList

      increase_ListBuffer(time_lsit)

      def increase_ListBuffer(list:List[String]) :List[String]={
        import scala.collection.mutable.ListBuffer
        var result = ListBuffer[String]()
        for(element <- list){
          val dt = DateTime.parse(element,dateFormat_2)
          val days = element
          val days_fmt = dateFormatter.print(dt)
          sqlContext.sql(s"use $hive_dbname")
          sqlContext.sql(s"alter table hive_org_tdapp_eventnew drop partition (part_daytime='$days_fmt',part_updays='$currentDay')")
          println(s"alter table hive_org_tdapp_eventnew drop partition (part_daytime='$days_fmt',part_updays='$currentDay') successfully!")
          sqlContext.sql(s"insert into hive_org_tdapp_eventnew partition (part_daytime='$days_fmt',part_updays='$currentDay') select * from spark_hive_org_tdapp_eventnew htempa where htempa.daytime='$days'")
          println(s"insert into hive_org_tdapp_eventnew partition (part_daytime='$days_fmt',part_updays='$currentDay') successfully!")
          result += element+1
        }
        result.toList
      }
    }
  }


  /**
    * hive-job-61 2016-11-22
    * org_tdapp_exceptionnew to hive_org_tdapp_exceptionnew
    * @author Xue
    * @param sqlContext
    */

  // Xue create function about partition by date ^_^
  def JOB_HV_61(implicit sqlContext: HiveContext,start_dt: String, end_dt: String)  {

    var sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val start = LocalDate.parse(start_dt, dateFormatter)
    val end = LocalDate.parse(end_dt, dateFormatter)
    val days = Days.daysBetween(start, end).getDays
    val dateStrs = for (day <- 0 to days) {
      val currentDay = (start.plusDays(day).toString(dateFormatter))
      println("######JOB_HV_61######")
      val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/incident/td/hive_org_tdapp_exceptionnew/part_updays=$currentDay")
      println(s"###### read $up_namenode/ successful ######")
      df.registerTempTable("spark_hive_org_tdapp_exceptionnew")

      val daytime:DataFrame = sqlContext.sql(
        s"""
           |select distinct
           |daytime
           |from spark_hive_org_tdapp_exceptionnew
            """.stripMargin
      )
      daytime.show(10)

      val time_lsit = daytime.select("daytime").rdd.map(r => r(0).asInstanceOf[String]).collect().toList

      increase_ListBuffer(time_lsit)

      def increase_ListBuffer(list:List[String]) :List[String]={
        import scala.collection.mutable.ListBuffer
        var result = ListBuffer[String]()
        for(element <- list){
          val dt = DateTime.parse(element,dateFormat_2)
          val days = element
          val days_fmt = dateFormatter.print(dt)
          sqlContext.sql(s"use $hive_dbname")
          sqlContext.sql(s"alter table hive_org_tdapp_exceptionnew drop partition (part_daytime='$days_fmt',part_updays='$currentDay')")
          println(s"alter table hive_org_tdapp_exceptionnew drop partition (part_daytime='$days_fmt',part_updays='$currentDay') successfully!")
          sqlContext.sql(s"insert into hive_org_tdapp_exceptionnew partition (part_daytime='$days_fmt',part_updays='$currentDay') select * from spark_hive_org_tdapp_exceptionnew htempa where htempa.daytime='$days'")
          println(s"insert into hive_org_tdapp_exceptionnew partition (part_daytime='$days_fmt',part_updays='$currentDay') successfully!")
          result += element+1
        }
        result.toList
      }
    }
  }


  /**
    * hive-job-62 2016-11-22
    * org_tdapp_tlaunchnew to hive_org_tdapp_tlaunchnew
    * @author Xue
    * @param sqlContext
    */

  // Xue create function about partition by date ^_^
  def JOB_HV_62(implicit sqlContext: HiveContext,start_dt: String, end_dt: String)  {

    var sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val start = LocalDate.parse(start_dt, dateFormatter)
    val end = LocalDate.parse(end_dt, dateFormatter)
    val days = Days.daysBetween(start, end).getDays
    val dateStrs = for (day <- 0 to days) {
      val currentDay = (start.plusDays(day).toString(dateFormatter))
      println("######JOB_HV_62######")
      val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/incident/td/hive_org_tdapp_tlaunchnew/part_updays=$currentDay")
      println(s"###### read $up_namenode/ successful ######")
      df.registerTempTable("spark_hive_org_tdapp_tlaunchnew")

      val daytime:DataFrame = sqlContext.sql(
        s"""
           |select distinct
           |daytime
           |from spark_hive_org_tdapp_tlaunchnew
            """.stripMargin
      )
      daytime.show(10)

      val time_lsit = daytime.select("daytime").rdd.map(r => r(0).asInstanceOf[String]).collect().toList

      increase_ListBuffer(time_lsit)

      def increase_ListBuffer(list:List[String]) :List[String]={
        import scala.collection.mutable.ListBuffer
        var result = ListBuffer[String]()
        for(element <- list){
          val dt = DateTime.parse(element,dateFormat_2)
          val days = element
          val days_fmt = dateFormatter.print(dt)
          sqlContext.sql(s"use $hive_dbname")
          sqlContext.sql(s"alter table hive_org_tdapp_tlaunchnew drop partition (part_daytime='$days_fmt',part_updays='$currentDay')")
          println(s"alter table hive_org_tdapp_tlaunchnew drop partition (part_daytime='$days_fmt',part_updays='$currentDay') successfully!")
          sqlContext.sql(s"insert into hive_org_tdapp_tlaunchnew partition (part_daytime='$days_fmt',part_updays='$currentDay') select * from spark_hive_org_tdapp_tlaunchnew htempa where htempa.daytime='$days'")
          println(s"insert into hive_org_tdapp_tlaunchnew partition (part_daytime='$days_fmt',part_updays='$currentDay') successfully!")
          result += element+1
        }
        result.toList
      }
    }
  }


  /**
    * hive-job-63 2016-11-22
    * org_tdapp_device to hive_org_tdapp_device
    * @author Xue
    * @param sqlContext
    */

  // Xue create function about partition by date ^_^
  def JOB_HV_63(implicit sqlContext: HiveContext,start_dt: String, end_dt: String)  {

    var sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val start = LocalDate.parse(start_dt, dateFormatter)
    val end = LocalDate.parse(end_dt, dateFormatter)
    val days = Days.daysBetween(start, end).getDays
    val dateStrs = for (day <- 0 to days) {
      val currentDay = (start.plusDays(day).toString(dateFormatter))
      println("######JOB_HV_63######")
      val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/incident/td/hive_org_tdapp_device/part_updays=$currentDay")
      println(s"###### read $up_namenode/ successful ######")
      df.registerTempTable("spark_hive_org_tdapp_device")

      val daytime:DataFrame = sqlContext.sql(
        s"""
           |select distinct
           |daytime
           |from spark_hive_org_tdapp_device
            """.stripMargin
      )
      daytime.show(10)

      val time_lsit = daytime.select("daytime").rdd.map(r => r(0).asInstanceOf[String]).collect().toList

      increase_ListBuffer(time_lsit)

      def increase_ListBuffer(list:List[String]) :List[String]={
        import scala.collection.mutable.ListBuffer
        var result = ListBuffer[String]()
        for(element <- list){
          val dt = DateTime.parse(element,dateFormat_2)
          val days = element
          val days_fmt = dateFormatter.print(dt)
          sqlContext.sql(s"use $hive_dbname")
          sqlContext.sql(s"alter table hive_org_tdapp_device drop partition (part_daytime='$days_fmt',part_updays='$currentDay')")
          println(s"alter table hive_org_tdapp_device drop partition (part_daytime='$days_fmt',part_updays='$currentDay') successfully!")
          sqlContext.sql(s"insert into hive_org_tdapp_device partition (part_daytime='$days_fmt',part_updays='$currentDay') select * from spark_hive_org_tdapp_device htempa where htempa.daytime='$days'")
          println(s"insert into hive_org_tdapp_device partition (part_daytime='$days_fmt',part_updays='$currentDay') successfully!")
          result += element+1
        }
        result.toList
      }
    }
  }


  /**
    * hive-job-64 2016-11-25
    * org_tdapp_exception to hive_org_tdapp_exception
    * @author Xue
    * @param sqlContext
    */

  // Xue create function about partition by date ^_^
  def JOB_HV_64(implicit sqlContext: HiveContext,start_dt: String, end_dt: String)  {

    var sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val start = LocalDate.parse(start_dt, dateFormatter)
    val end = LocalDate.parse(end_dt, dateFormatter)
    val days = Days.daysBetween(start, end).getDays
    val dateStrs = for (day <- 0 to days) {
      val currentDay = (start.plusDays(day).toString(dateFormatter))
      println("######JOB_HV_64######")
      val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/incident/td/hive_org_tdapp_exception/part_updays=$currentDay")
      println(s"###### read $up_namenode/ successful ######")
      df.registerTempTable("spark_hive_org_tdapp_exception")

      val daytime:DataFrame = sqlContext.sql(
        s"""
           |select distinct
           |daytime
           |from spark_hive_org_tdapp_exception
            """.stripMargin
      )
      daytime.show(10)

      val time_lsit = daytime.select("daytime").rdd.map(r => r(0).asInstanceOf[String]).collect().toList

      increase_ListBuffer(time_lsit)

      def increase_ListBuffer(list:List[String]) :List[String]={
        import scala.collection.mutable.ListBuffer
        var result = ListBuffer[String]()
        for(element <- list){
          val dt = DateTime.parse(element,dateFormat_2)
          val days = element
          val days_fmt = dateFormatter.print(dt)
          sqlContext.sql(s"use $hive_dbname")
          sqlContext.sql(s"alter table hive_org_tdapp_exception drop partition (part_daytime='$days_fmt',part_updays='$currentDay')")
          println(s"alter table hive_org_tdapp_exception drop partition (part_daytime='$days_fmt',part_updays='$currentDay') successfully!")
          sqlContext.sql(s"insert into hive_org_tdapp_exception partition (part_daytime='$days_fmt',part_updays='$currentDay') select * from spark_hive_org_tdapp_exception htempa where htempa.daytime='$days'")
          println(s"insert into hive_org_tdapp_exception partition (part_daytime='$days_fmt',part_updays='$currentDay') successfully!")
          result += element+1
        }
        result.toList
      }
    }
  }


  /**
    * hive-job-65 2016-11-25
    * org_tdapp_newuser to hive_org_tdapp_newuser
    * @author Xue
    * @param sqlContext
    */
  // Xue create function about partition by date ^_^
  def JOB_HV_65(implicit sqlContext: HiveContext,start_dt: String, end_dt: String)  {

    var sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val start = LocalDate.parse(start_dt, dateFormatter)
    val end = LocalDate.parse(end_dt, dateFormatter)
    val days = Days.daysBetween(start, end).getDays
    val dateStrs = for (day <- 0 to days) {
      val currentDay = (start.plusDays(day).toString(dateFormatter))
      println("######JOB_HV_65######")
      val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/incident/td/hive_org_tdapp_newuser/part_updays=$currentDay")
      println(s"###### read $up_namenode/ successful ######")
      df.registerTempTable("spark_hive_org_tdapp_newuser")

      val daytime:DataFrame = sqlContext.sql(
        s"""
           |select distinct
           |daytime
           |from spark_hive_org_tdapp_newuser
            """.stripMargin
      )
      daytime.show(10)

      val time_lsit = daytime.select("daytime").rdd.map(r => r(0).asInstanceOf[String]).collect().toList

      increase_ListBuffer(time_lsit)

      def increase_ListBuffer(list:List[String]) :List[String]={
        import scala.collection.mutable.ListBuffer
        var result = ListBuffer[String]()
        for(element <- list){
          val dt = DateTime.parse(element,dateFormat_2)
          val days = element
          val days_fmt = dateFormatter.print(dt)
          sqlContext.sql(s"use $hive_dbname")
          sqlContext.sql(s"alter table hive_org_tdapp_newuser drop partition (part_daytime='$days_fmt',part_updays='$currentDay')")
          println(s"alter table hive_org_tdapp_newuser drop partition (part_daytime='$days_fmt',part_updays='$currentDay') successfully!")
          sqlContext.sql(s"insert into hive_org_tdapp_newuser partition (part_daytime='$days_fmt',part_updays='$currentDay') select * from spark_hive_org_tdapp_newuser htempa where htempa.daytime='$days'")
          println(s"insert into hive_org_tdapp_newuser partition (part_daytime='$days_fmt',part_updays='$currentDay') successfully!")
          result += element+1
        }
        result.toList
      }
    }
  }


  /**
    * hive-job-71 2016-11-28  循环抽取
    * hive_ach_order_inf -> hbkdb.rtdtrs_dtl_ach_order_inf (测试环境只有这一天：20151109 有数据)
    *
    * 测试时间段为:
    * start_dt="2015-11-09"
    * end_dt="2015-11-09"
    *
    * 数据量：16415条
    *
    * @author tzq
    * @param sqlContext
    * @param end_dt
    * @param start_dt
    * @param interval
    */
  def JOB_HV_71(implicit sqlContext: HiveContext,start_dt: String,end_dt:String) = {
    println("######JOB_HV_71######")
    val interval=DateUtils.getIntervalDays(start_dt,end_dt).toInt
    var part_dt = start_dt
    sqlContext.sql(s"use $hive_dbname")

    if(interval>=0){
      for(i <- 0 to interval){
        val temp=(part_dt.toString()).replace("-","")//转为yyMMdd
        val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/incident/order/hive_ach_order_inf/hp_trans_dt=$temp")
        println(s"###### read $up_namenode/ at partition= $temp successful ######")
        df.registerTempTable("spark_hive_ach_order_inf")

        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql(s"alter table hive_ach_order_inf drop partition(part_hp_trans_dt='$part_dt')")
        sqlContext.sql(
          s"""
             |insert into table hive_ach_order_inf partition(part_hp_trans_dt='$part_dt')
             |select * from spark_hive_ach_order_inf
       """.stripMargin)
        println(s"#### insert into table(hive_ach_order_inf) at partition= $part_dt successful ####")

        //循环抽取：天数加1
        part_dt=DateUtils.addOneDay(part_dt)//yyyy-MM-dd
      }
    }
  }

}
