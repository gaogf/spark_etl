package com.unionpay.etl
import com.unionpay.conf.ConfigurationManager
import com.unionpay.constant.Constants
import com.unionpay.utils.DateUtils
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
/**
  * UP HIVE -> UPW HIVE
  */
object SparkUPH2H {

  //计算开始日期：start_dt-1
  private  val start_dt=DateUtils.getYesterdayByJob(ConfigurationManager.getProperty(Constants.START_DT))
  //结束日期
  private  val end_dt=ConfigurationManager.getProperty(Constants.END_DT)
  //UP NAMENODE URL
  private val up_namenode=ConfigurationManager.getProperty(Constants.UP_NAMENODE)
  //UP HIVE DATA ROOT URL
  private val up_hivedataroot=ConfigurationManager.getProperty(Constants.UP_HIVEDATAROOT)
  //指定HIVE数据库名
  private lazy val hive_dbname =ConfigurationManager.getProperty(Constants.HIVE_DBNAME)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkUPH2H")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new HiveContext(sc)

    JOB_HV_39 //hive-job40依赖
    JOB_HV_49 //hive-job3依赖
    JOB_HV_52 //

    sc.stop()
  }

  /**
    * hive-job-39 2016-08-30
    * rtdtrs_dtl_achis to hive_achis_trans
    * @author winslow yang
    * @param sqlContext
    */
  def JOB_HV_39(implicit sqlContext: SQLContext) = {

    println("######JOB_HV_39######")

    val today_dt = end_dt
    val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/hive_achis_trans/part_settle_dt=$today_dt")
    println(s"###### read $up_namenode/ successful ######")
    df.registerTempTable("spark_hive_achis_trans")

    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql(s"alter table hive_achis_trans drop partition (part_settle_dt='$today_dt')")
    sqlContext.sql(s"alter table hive_achis_trans add partition (part_settle_dt='$today_dt')")
    sqlContext.sql(s"insert into table hive_achis_trans partition(part_settle_dt='$today_dt') select * from spark_hive_achis_trans")
    println("#### insert into table success ####")

  }


  /**
    * hive-job-49 2016-09-14
    * rtapam_prv_ucbiz_cdhd_bas_inf to hive_ucbiz_cdhd_bas_inf
    * @author winslow yang
    * @param sqlContext
    */
  def JOB_HV_49(implicit sqlContext: HiveContext) = {

    println("######JOB_HV_49######")

    val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/hive_ucbiz_cdhd_bas_inf")
    println(s"###### read $up_namenode successful ######")
    df.registerTempTable("spark_ucbiz_cdhd_bas_inf")

    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("truncate table hive_ucbiz_cdhd_bas_inf")
    sqlContext.sql("insert into table hive_ucbiz_cdhd_bas_inf select * from spark_ucbiz_cdhd_bas_inf")
    println("#### insert into table (hive_ucbiz_cdhd_bas_inf) success ####")

  }


  /**
    * hive-job-52 2016-08-29
    * stmtrs_bsl_active_card_acq_branch_mon1 to hive_active_card_acq_branch_mon
    * @author winslow yang
    * @param sqlContext
    */
  def JOB_HV_52(implicit sqlContext: HiveContext) = {

    println("######JOB_HV_52######")
    val part_dt = end_dt.substring(0,7)
    val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/hive_active_card_acq_branch_mon/part_settle_month=$part_dt")
    println(s"###### read $up_namenode/ successful ######")
    df.registerTempTable("spark_active_card_acq_branch_mon")

    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql(s"alter table hive_active_card_acq_branch_mon drop partition(part_settle_month='$part_dt')")
    sqlContext.sql(
      s"""
         |insert into table hive_active_card_acq_branch_mon partition(part_settle_month='$part_dt')
         |select * from spark_active_card_acq_branch_mon
       """.stripMargin)
    println("#### insert into table(hive_active_card_acq_branch_mon) success ####")

  }

}
