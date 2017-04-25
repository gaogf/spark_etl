package com.unionpay.etl

import com.unionpay.conf.ConfigurationManager
import com.unionpay.constant.Constants
import com.unionpay.jdbc.{UPSQL_JDBC, UPSQL_TIMEPARAMS_JDBC}
import com.unionpay.jdbc.UPSQL_JDBC.DataFrame2Mysql
import com.unionpay.utils.DateUtils
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
/**
  * 作业：抽取钱包Hive数据仓库中的数据到UPSQL数据库
  */
object SparkHive2Mysql {

  private lazy val hive_dbname =ConfigurationManager.getProperty(Constants.HIVE_DBNAME)

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("SparkHive2Mysql")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .set("spark.Kryoserializer.buffer.max","1024m")
      .set("spark.yarn.driver.memoryOverhead","1024")
      .set("spark.yarn.executor.memoryOverhead","2048")
      .set("spark.newwork.buffer.timeout","300s")
      .set("spark.executor.heartbeatInterval","30s")
      .set("spark.driver.extraJavaOptions","-XX:+UseG1GC -XX:+UseCompressedOops")
      .set("spark.executor.extraJavaOptions","-XX:+UseG1GC -XX:+UseCompressedOops")
      .set("spark.memory.useLegacyMode","true")
      .set("spark.sql.shuffle.partitions","200")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new HiveContext(sc)

    var start_dt: String = s"0000-00-00"
    var end_dt: String = s"0000-00-00"

    /**
      * 从数据库中获取当前JOB的执行起始和结束日期。
      * 日常调度使用。
      */
    //    val rowParams=UPSQL_TIMEPARAMS_JDBC.readTimeParams(sqlContext)
    //     start_dt=DateUtils.getYesterdayByJob(rowParams.getString(0))//获取开始日期：start_dt-1
    //     end_dt=rowParams.getString(1)//结束日期

    /**
      * 从命令行获取当前JOB的执行起始和结束日期。
      * 无规则日期的增量数据抽取，主要用于数据初始化和调试。
      */
    if (args.length > 1) {
      start_dt = args(1)
      end_dt = args(2)
    } else {
      println("#### 请指定 SparkHive2Mysql 数据抽取的起始日期和结束日期 ！")
      System.exit(0)
    }

    //获取开始日期和结束日期的间隔天数
    val interval=DateUtils.getIntervalDays(start_dt,end_dt).toInt

    println(s"#### SparkHive2Mysql 数据清洗的起始日期为: $start_dt --  $end_dt")

    val JobName = if(args.length>0) args(0) else None
    println(s"#### The Current Job Name is ： [$JobName]")
    JobName match {
      /**
        * 每日模板job
        */
      case "JOB_DM_1"  => JOB_DM_1(sqlContext,start_dt,end_dt,interval)     //CODE BY YX
      case "JOB_DM_3"  => JOB_DM_3(sqlContext,start_dt,end_dt,interval)     //CODE BY YX
      case "JOB_DM_9"  => JOB_DM_9(sqlContext,start_dt,end_dt,interval)     //CODE BY XTP
      case "JOB_DM_55"  => JOB_DM_55(sqlContext,start_dt,end_dt)            //CODE BY TZQ
      case "JOB_DM_61"  => JOB_DM_61(sqlContext,start_dt,end_dt,interval)   //CODE BY YX
      case "JOB_DM_62"  => JOB_DM_62(sqlContext,start_dt,end_dt,interval)   //CODE BY TZQ
      case "JOB_DM_63"  => JOB_DM_63(sqlContext,start_dt,end_dt,interval)   //CODE BY XTP
      case "JOB_DM_64"  => JOB_DM_64(sqlContext,start_dt,end_dt,interval)   //CODE BY XTP
      case "JOB_DM_65"  => JOB_DM_65(sqlContext,start_dt,end_dt,interval)   //CODE BY XTP
      case "JOB_DM_66"  => JOB_DM_66(sqlContext,start_dt,end_dt,interval)   //CODE BY TZQ
      case "JOB_DM_67"  => JOB_DM_67(sqlContext,start_dt,end_dt)            //CODE BY YX
      case "JOB_DM_68"  => JOB_DM_68(sqlContext,start_dt,end_dt)            //CODE BY YX
      case "JOB_DM_69"  => JOB_DM_69(sqlContext,start_dt,end_dt)            //CODE BY TZQ
      case "JOB_DM_70"  => JOB_DM_70(sqlContext,start_dt,end_dt)            //CODE BY TZQ
      case "JOB_DM_71"  => JOB_DM_71(sqlContext,start_dt,end_dt)            //CODE BY TZQ
      case "JOB_DM_72"  => JOB_DM_72(sqlContext,start_dt,end_dt)            //CODE BY YX
      case "JOB_DM_73"  => JOB_DM_73(sqlContext,start_dt,end_dt)            //CODE BY XTP
      case "JOB_DM_74"  => JOB_DM_74(sqlContext,start_dt,end_dt)            //CODE BY XTP
      case "JOB_DM_75"  => JOB_DM_75(sqlContext,start_dt,end_dt)            //CODE BY XTP
      case "JOB_DM_76"  => JOB_DM_76(sqlContext,start_dt,end_dt,interval)   //CODE BY TZQ
      case "JOB_DM_78"  => JOB_DM_78(sqlContext,start_dt,end_dt,interval)   //CODE BY XTP
      case "JOB_DM_86"  => JOB_DM_86(sqlContext,start_dt,end_dt,interval)   //CODE BY XTP
      case "JOB_DM_87"  => JOB_DM_87(sqlContext,start_dt,end_dt,interval)   //CODE BY TZQ

      /**
        * 指标套表job
        */
      case "JOB_DM_2"  => JOB_DM_2(sqlContext,start_dt,end_dt,interval)     //CODE BY XTP
      case "JOB_DM_4"  => JOB_DM_4(sqlContext,start_dt,end_dt,interval)     //CODE BY XTP
      case "JOB_DM_5"  => JOB_DM_5(sqlContext,start_dt,end_dt,interval)     //CODE BY TZQ
      case "JOB_DM_6"  => JOB_DM_6(sqlContext,start_dt,end_dt,interval)     //CODE BY TZQ
      case "JOB_DM_7"  => JOB_DM_7(sqlContext,start_dt,end_dt,interval)     //CODE BY XTP
      case "JOB_DM_8"  => JOB_DM_8(sqlContext,start_dt,end_dt,interval)     //CODE BY XTP

      case "JOB_DM_10" =>JOB_DM_10(sqlContext,start_dt,end_dt,interval)     //CODE BY TZQ
      case "JOB_DM_11" =>JOB_DM_11(sqlContext,start_dt,end_dt,interval)     //CODE BY TZQ
      case "JOB_DM_12" =>JOB_DM_12(sqlContext,start_dt,end_dt,interval)     //CODE BY TZQ
      case "JOB_DM_13" =>JOB_DM_13(sqlContext,start_dt,end_dt,interval)     //CODE BY TZQ
      case "JOB_DM_14" =>JOB_DM_14(sqlContext,start_dt,end_dt,interval)     //CODE BY TZQ
      case "JOB_DM_15" =>JOB_DM_15(sqlContext,start_dt,end_dt,interval)     //CODE BY TZQ
      case "JOB_DM_16" =>JOB_DM_16(sqlContext,start_dt,end_dt,interval)     //CODE BY TZQ
      case "JOB_DM_17" =>JOB_DM_17(sqlContext,start_dt,end_dt,interval)     //CODE BY TZQ

      case "JOB_DM_18" =>JOB_DM_18(sqlContext,start_dt,end_dt,interval)     //CODE BY XTP
      case "JOB_DM_19" =>JOB_DM_19(sqlContext,start_dt,end_dt,interval)     //CODE BY XTP
      case "JOB_DM_20" =>JOB_DM_20(sqlContext,start_dt,end_dt,interval)     //CODE BY XTP
      case "JOB_DM_21" =>JOB_DM_21(sqlContext,start_dt,end_dt,interval)     //CODE BY XTP
      case "JOB_DM_22" =>JOB_DM_22(sqlContext,start_dt,end_dt,interval)     //CODE BY XTP
      case "JOB_DM_23" =>JOB_DM_23(sqlContext,start_dt,end_dt,interval)     //CODE BY XTP

      case "JOB_DM_24" =>JOB_DM_24(sqlContext,start_dt,end_dt,interval)     //CODE BY TZQ
      case "JOB_DM_25" =>JOB_DM_25(sqlContext,start_dt,end_dt,interval)     //CODE BY TZQ
      case "JOB_DM_26" =>JOB_DM_26(sqlContext,start_dt,end_dt,interval)     //CODE BY TZQ
      case "JOB_DM_27" =>JOB_DM_27(sqlContext,start_dt,end_dt,interval)     //CODE BY TZQ
      case "JOB_DM_28" =>JOB_DM_28(sqlContext,start_dt,end_dt,interval)     //CODE BY TZQ
      case "JOB_DM_29" =>JOB_DM_29(sqlContext,start_dt,end_dt,interval)     //CODE BY TZQ
      case "JOB_DM_30" =>JOB_DM_30(sqlContext,start_dt,end_dt)              //CODE BY TZQ
      case "JOB_DM_31" =>JOB_DM_31(sqlContext,start_dt,end_dt)              //CODE BY TZQ

      case "JOB_DM_32" =>JOB_DM_32(sqlContext,start_dt,end_dt,interval)   //CODE BY XTP   测试报错，带修正
      case "JOB_DM_33" =>JOB_DM_33(sqlContext,start_dt,end_dt,interval)     //CODE BY XTP

      case "JOB_DM_34" =>JOB_DM_34(sqlContext,start_dt,end_dt)              //CODE BY TZQ
      case "JOB_DM_35" =>JOB_DM_35(sqlContext,start_dt,end_dt)              //CODE BY TZQ
      case "JOB_DM_36" =>JOB_DM_36(sqlContext,start_dt,end_dt,interval)     //CODE BY TZQ  [测试未通过,解决中]
      case "JOB_DM_37" =>JOB_DM_37(sqlContext,start_dt,end_dt)              //CODE BY TZQ
      case "JOB_DM_38" =>JOB_DM_38(sqlContext,start_dt,end_dt)              //CODE BY TZQ
      case "JOB_DM_39" =>JOB_DM_39(sqlContext,start_dt,end_dt,interval)     //CODE BY TZQ
      case "JOB_DM_40" =>JOB_DM_40(sqlContext,start_dt,end_dt)              //CODE BY TZQ
      case "JOB_DM_41" =>JOB_DM_41(sqlContext,start_dt,end_dt)              //CODE BY TZQ
      case "JOB_DM_42" =>JOB_DM_42(sqlContext,start_dt,end_dt)              //CODE BY TZQ
      case "JOB_DM_43" =>JOB_DM_43(sqlContext,start_dt,end_dt)              //CODE BY TZQ

      case "JOB_DM_44" =>JOB_DM_44(sqlContext,start_dt,end_dt,interval)     //CODE BY XTP
      case "JOB_DM_45" =>JOB_DM_45(sqlContext,start_dt,end_dt,interval)     //CODE BY XTP
      case "JOB_DM_46" =>JOB_DM_46(sqlContext,start_dt,end_dt,interval)     //CODE BY XTP
      case "JOB_DM_47" =>JOB_DM_47(sqlContext,start_dt,end_dt,interval)     //CODE BY XTP
      case "JOB_DM_48" =>JOB_DM_48(sqlContext,start_dt,end_dt,interval)     //CODE BY XTP

      case "JOB_DM_49" =>JOB_DM_49(sqlContext,start_dt,end_dt)              //CODE BY TZQ
      case "JOB_DM_50" =>JOB_DM_50(sqlContext,start_dt,end_dt)              //CODE BY TZQ
      case "JOB_DM_51" =>JOB_DM_51(sqlContext,start_dt,end_dt)              //CODE BY TZQ
      case "JOB_DM_52" =>JOB_DM_52(sqlContext,start_dt,end_dt)              //CODE BY TZQ

      case "JOB_DM_53"  => JOB_DM_53(sqlContext,start_dt,end_dt,interval)   //CODE BY XTP   [测试出错，待解决]
      case "JOB_DM_54" =>  JOB_DM_54(sqlContext,start_dt,end_dt)              //CODE BY XTP 无数据
      case "JOB_DM_56"  => JOB_DM_56(sqlContext,start_dt,end_dt,interval)   //CODE BY XTP   [测试出错，待解决]
      case "JOB_DM_57"  => JOB_DM_57(sqlContext,start_dt,end_dt,interval)   //CODE BY XTP   [测试出错，待解决]
      case "JOB_DM_58"  => JOB_DM_58(sqlContext,start_dt,end_dt,interval)   //CODE BY XTP
      case "JOB_DM_59"  => JOB_DM_59(sqlContext,start_dt,end_dt,interval)   //CODE BY XTP
      case "JOB_DM_60"  => JOB_DM_60(sqlContext,start_dt,end_dt,interval)   //CODE BY XTP

      case "JOB_DM_79"  => JOB_DM_79(sqlContext,start_dt,end_dt,interval)   //CODE BY XTP
      case "JOB_DM_80"  => JOB_DM_80(sqlContext,start_dt,end_dt,interval)   //CODE BY XTP
      case "JOB_DM_81"  => JOB_DM_81(sqlContext,start_dt,end_dt,interval)   //CODE BY XTP
      case "JOB_DM_82"  => JOB_DM_82(sqlContext,start_dt,end_dt,interval)   //CODE BY XTP
      case "JOB_DM_83"  => JOB_DM_83(sqlContext,start_dt,end_dt,interval)   //CODE BY XTP
      case "JOB_DM_84"  => JOB_DM_84(sqlContext,start_dt,end_dt,interval)   //CODE BY XTP
      case "JOB_DM_85"  => JOB_DM_85(sqlContext,start_dt,end_dt,interval)   //CODE BY XTP


      case "JOB_DM_88"  => JOB_DM_88(sqlContext,start_dt,end_dt,interval)   //CODE BY TZQ
      case "JOB_DM_89"  => JOB_DM_89(sqlContext,start_dt,end_dt,interval)   //CODE BY TZQ
      case "JOB_DM_90"  => JOB_DM_90(sqlContext,start_dt,end_dt,interval)   //CODE BY XTP
      case "JOB_DM_91"  => JOB_DM_91(sqlContext,start_dt,end_dt,interval)   //CODE BY XTP
      case "JOB_DM_92"  => JOB_DM_92(sqlContext,start_dt,end_dt,interval)   //CODE BY TZQ
      case "JOB_DM_93"  => JOB_DM_93(sqlContext,start_dt,end_dt,interval)   //CODE BY TZQ
      case "JOB_DM_94"  => JOB_DM_94(sqlContext,start_dt,end_dt,interval)   //CODE BY TZQ
      case "JOB_DM_95"  => JOB_DM_95(sqlContext,start_dt,end_dt,interval)   //CODE BY TZQ  [内存溢出,解决中]
      case "JOB_DM_96"  => JOB_DM_96(sqlContext,start_dt,end_dt,interval)   //CODE BY TZQ
      case "JOB_DM_97"  => JOB_DM_97(sqlContext,start_dt,end_dt,interval)   //CODE BY XTP
      case "JOB_DM_98"  => JOB_DM_98(sqlContext,start_dt,end_dt,interval)   //CODE BY XTP

      case _ => println("#### No Case Job,Please Input JobName")
    }

    sc.stop()


  }


  /**
    * JobName: JOB_DM_1
    * Feature: hive_pri_acct_inf,hive_card_bind_inf,hive_acc_trans -> dm_user_card_nature
    *
    * @author YangXue
    * @time 2016-09-01
    * @param sqlContext,start_dt,end_dt
    */
  def JOB_DM_1(implicit sqlContext: HiveContext, start_dt: String, end_dt: String, interval: Int) = {
    println("#### JOB_DM_1(hive_pri_acct_inf,hive_card_bind_inf,hive_acc_trans -> dm_user_card_nature)")

    DateUtils.timeCost("JOB_DM_1") {
      UPSQL_JDBC.delete("dm_user_mobile_home", "report_dt", start_dt, end_dt)
      println("#### JOB_DM_1 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())

      var today_dt = start_dt
      if (interval >= 0) {
        sqlContext.sql(s"use $hive_dbname")
        for (i <- 0 to interval) {
          println(s"#### JOB_DM_1 spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())
          val results = sqlContext.sql(
            s"""
               |select
               |a.phone_location as mobile_home ,
               |'$today_dt' as report_dt,
               |a.tpre   as   reg_tpre_add_num    ,
               |a.months as   reg_months_add_num  ,
               |a.years  as   reg_year_add_num    ,
               |a.total  as   reg_totle_add_num   ,
               |b.tpre   as   effect_tpre_add_num ,
               |b.months as   effect_months_add_num  ,
               |b.years  as   effect_year_add_num ,
               |b.total  as   effect_totle_add_num,
               |0        as   batch_tpre_add_num  ,
               |0        as   batch_year_add_num  ,
               |0        as   batch_totle_add_num ,
               |0        as   client_tpre_add_num ,
               |0        as   client_year_add_num ,
               |0        as   client_totle_add_num,
               |c.tpre   as   deal_tpre_add_num   ,
               |c.years  as   deal_year_add_num   ,
               |c.total  as   deal_totle_add_num  ,
               |d.realnm_num as realnm_num
               |from (
               |select
               |t.phone_location,
               |sum(case when to_date(t.rec_crt_ts)='$today_dt' then  1  else 0 end) as tpre,
               |sum(case when to_date(t.rec_crt_ts)>=trunc('$today_dt','MM') and to_date(t.rec_crt_ts)<='$today_dt'  then  1 else 0 end) as months,
               |sum(case when to_date(t.rec_crt_ts)>=trunc('$today_dt','YYYY') and to_date(t.rec_crt_ts)<='$today_dt'  then  1 else 0 end) as years,
               |sum(case when to_date(t.rec_crt_ts)<='$today_dt' then  1 else 0 end) as total
               |from
               |(select cdhd_usr_id,rec_crt_ts, phone_location,realnm_in from hive_pri_acct_inf where usr_st='1' or (usr_st='2' and note='BDYX_FREEZE')
               |) t
               |group by t.phone_location  ) a
               |left join
               |(
               |select
               |phone_location,
               |sum(case when to_date(bind_dt)='$today_dt'  then  1  else 0 end) as tpre,
               |sum(case when to_date(bind_dt)>=trunc('$today_dt','MM') and  to_date(bind_dt)<='$today_dt' then   1 else 0 end) as months,
               |sum(case when to_date(bind_dt)>=trunc('$today_dt','YYYY') and  to_date(bind_dt)<=' $today_dt' then  1 else 0 end) as years,
               |sum(case when to_date(bind_dt)<='$today_dt'  then  1 else 0 end) as total
               |from (select rec_crt_ts,phone_location,cdhd_usr_id from hive_pri_acct_inf
               |where usr_st='1' ) a
               |inner join (select distinct cdhd_usr_id, min(rec_crt_ts) as bind_dt from hive_card_bind_inf where card_auth_st in ('1','2','3')
               |group by cdhd_usr_id) b
               |on a.cdhd_usr_id=b.cdhd_usr_id
               |group by phone_location ) b
               |on a.phone_location =b.phone_location
               |left join
               |(
               |select
               |phone_location,
               |sum(case when to_date(rec_crt_ts)='$today_dt'  and to_date(trans_dt)='$today_dt'  then  1  else 0 end) as tpre,
               |sum(case when to_date(rec_crt_ts)>=trunc('$today_dt','YYYY') and to_date(rec_crt_ts)<='$today_dt'
               |and to_date(trans_dt)>=trunc('$today_dt','YYYY') and  to_date(trans_dt)<='$today_dt' then  1 else 0 end) as years,
               |sum(case when to_date(rec_crt_ts)<='$today_dt' and  to_date(trans_dt)<='$today_dt' then  1 else 0 end) as total
               |from (select phone_location,cdhd_usr_id,rec_crt_ts from hive_pri_acct_inf where usr_st='1') a
               |inner join (select distinct cdhd_usr_id,trans_dt from hive_acc_trans ) b
               |on a.cdhd_usr_id=b.cdhd_usr_id
               |group by phone_location ) c
               |on a.phone_location=c.phone_location
               |left join
               |(
               |select
               |phone_location,
               |count(*) as realnm_num
               |from hive_pri_acct_inf
               |where  realnm_in='01'
               |and to_date(rec_crt_ts)='$today_dt'
               |group by  phone_location) d
               |on  a.phone_location=d.phone_location
             """.stripMargin)
          println(s"#### JOB_DM_1 spark sql 清洗[$today_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())
          //println(s"#### JOB_DM_1 spark sql 清洗[$today_dt]数据 results:"+results.count())

          if (!Option(results).isEmpty) {
            results.save2Mysql("dm_user_mobile_home")
            println(s"#### JOB_DM_1 [$today_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime()
            )
          } else {
            println(s"#### JOB_DM_1 spark sql 清洗[$today_dt]数据无结果集！")
          }
          today_dt = DateUtils.addOneDay(today_dt)
        }
      }
    }
  }


  /**
    * JOB_DM_2/10-14
    * dm_user_idcard_home->hive_pri_acct_inf,hive_acc_trans
    * Code by Xue
    *
    * @param sqlContext
    * @return
    */
  def JOB_DM_2 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("###JOB_DM_2(dm_user_idcard_home->hive_pri_acct_inf,hive_acc_trans)")
    DateUtils.timeCost("JOB_DM_2") {
      UPSQL_JDBC.delete("dm_user_idcard_home","report_dt",start_dt,end_dt)
      println("#### JOB_DM_2 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      var today_dt=start_dt
      if(interval>=0 ){
        println(s"#### JOB_DM_2 spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())
        sqlContext.sql(s"use $hive_dbname")
        for(i <- 0 to interval){
          val results = sqlContext.sql(
            s"""
               |select
               |nvl(a.id_area_nm,'其它') as idcard_home,
               |'$today_dt' as report_dt,
               |sum(a.tpre)   as   reg_tpre_add_num    ,
               |sum(a.years)  as   reg_year_add_num    ,
               |sum(a.total)  as   reg_totle_add_num   ,
               |sum(b.tpre)   as   effect_tpre_add_num ,
               |sum(b.years)  as   effect_year_add_num ,
               |sum(b.total)  as   effect_totle_add_num,
               |sum(c.tpre)   as   deal_tpre_add_num   ,
               |sum(c.years)  as   deal_year_add_num   ,
               |sum(c.total)  as   deal_totle_add_num
               |from
               |(
               |select
               |case when tempe.city_card in ('大连','宁波','厦门','青岛','深圳') then tempe.city_card else tempe.province_card end as id_area_nm,
               |count(distinct(case when tempe.rec_crt_ts='$today_dt'  then tempe.cdhd_usr_id end)) as tpre,
               |count(distinct(case when tempe.rec_crt_ts>=trunc('$today_dt','YYYY') and tempe.rec_crt_ts<='$today_dt'  then tempe.cdhd_usr_id end)) as years,
               |count(distinct(case when tempe.rec_crt_ts<='$today_dt' then  tempe.cdhd_usr_id end)) as total
               |from
               |(select cdhd_usr_id,to_date(rec_crt_ts) as rec_crt_ts, city_card,province_card from hive_pri_acct_inf where usr_st='1' ) tempe
               |group by (case when city_card in ('大连','宁波','厦门','青岛','深圳') then city_card else province_card end)
               |) a
               |left join
               |(
               |select
               |case when tempa.city_card in ('大连','宁波','厦门','青岛','深圳') then tempa.city_card else tempa.province_card end as id_area_nm,
               |count(distinct(case when tempa.rec_crt_ts='$today_dt'  and tempb.bind_dt='$today_dt'  then  tempa.cdhd_usr_id end)) as tpre,
               |count(distinct(case when tempa.rec_crt_ts>=trunc('$today_dt','YYYY') and tempa.rec_crt_ts<='$today_dt'
               |and tempb.bind_dt>=trunc('$today_dt','YYYY') and  tempb.bind_dt<='$today_dt' then  tempa.cdhd_usr_id end)) as years,
               |count(distinct(case when tempa.rec_crt_ts<='$today_dt' and  tempb.bind_dt<='$today_dt'  then  tempa.cdhd_usr_id end)) as total
               |from
               |(
               |select to_date(rec_crt_ts) as rec_crt_ts,city_card,province_card,cdhd_usr_id from hive_pri_acct_inf
               |where usr_st='1' ) tempa
               |inner join (select distinct cdhd_usr_id , to_date(rec_crt_ts) as  bind_dt  from hive_card_bind_inf where card_auth_st in ('1','2','3') ) tempb
               |on tempa.cdhd_usr_id=tempb.cdhd_usr_id
               |group by (case when tempa.city_card in ('大连','宁波','厦门','青岛','深圳') then tempa.city_card else tempa.province_card end) ) b
               |on a.id_area_nm =b.id_area_nm
               |left join
               |(
               |select
               |case when tempc.city_card in ('大连','宁波','厦门','青岛','深圳') then tempc.city_card else tempc.province_card end as id_area_nm,
               |count(distinct(case when tempc.rec_crt_ts='$today_dt'  and tempd.trans_dt='$today_dt'  then tempc.cdhd_usr_id end)) as tpre,
               |count(distinct(case when tempc.rec_crt_ts>=trunc('$today_dt','YYYY') and tempc.rec_crt_ts<='$today_dt'
               |and tempd.trans_dt>=trunc('$today_dt','YYYY') and  tempd.trans_dt<='$today_dt' then  tempc.cdhd_usr_id end)) as years,
               |count(distinct(case when tempc.rec_crt_ts<='$today_dt' and  tempd.trans_dt<='$today_dt'  then  tempc.cdhd_usr_id end)) as total
               |from
               |(select city_card,city_card,province_card,cdhd_usr_id,to_date(rec_crt_ts) as rec_crt_ts  from hive_pri_acct_inf
               |where usr_st='1') tempc
               |inner join (select distinct cdhd_usr_id,to_date(trans_dt) as trans_dt  from hive_acc_trans ) tempd
               |on tempc.cdhd_usr_id=tempd.cdhd_usr_id
               |group by (case when tempc.city_card in ('大连','宁波','厦门','青岛','深圳') then tempc.city_card else tempc.province_card end) ) c
               |on a.id_area_nm=c.id_area_nm
               |group by nvl(a.id_area_nm,'其它'),'$today_dt'
               | """.stripMargin)


          println(s"#### JOB_DM_2 spark sql 清洗[$today_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())

          if (!Option(results).isEmpty) {
            results.save2Mysql("dm_user_idcard_home")
            println(s"#### JOB_DM_2 [$today_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime()
            )
          } else {
            println(s"#### JOB_DM_2 spark sql 清洗[$today_dt]数据无结果集！")
          }
          today_dt=DateUtils.addOneDay(today_dt)
        }
      }
    }
  }


  /**
    * JobName: JOB_DM_3
    * Feature: hive_pri_acct_inf,hive_inf_source_dtl,hive_acc_trans,hive_card_bind_inf,hive_inf_source_class-> dm_user_card_nature
    *
    * @author YangXue
    * @time 2016-09-12
    * @param sqlContext,start_dt,end_dt
    */
  def JOB_DM_3(implicit sqlContext: HiveContext, start_dt: String, end_dt: String, interval: Int) = {
    println("#### JOB_DM_3(hive_pri_acct_inf,hive_inf_source_dtl,hive_acc_trans,hive_card_bind_inf,hive_inf_source_class-> dm_user_card_nature)")

    DateUtils.timeCost("JOB_DM_3") {
      UPSQL_JDBC.delete("dm_user_regist_channel", "report_dt", start_dt, end_dt)
      println("#### JOB_DM_3 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())

      var today_dt = start_dt
      if (interval >= 0) {
        sqlContext.sql(s"use $hive_dbname")
        for (i <- 0 to interval) {
          println(s"#### JOB_DM_3 spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())
          val results = sqlContext.sql(
            s"""
               |select
               |d.class as regist_channel ,
               |d.access_nm as reg_son_chn ,
               |'$today_dt' as report_dt ,
               |sum(a.tpre) as reg_tpre_add_num ,
               |sum(a.years) as reg_year_add_num ,
               |sum(a.total) as reg_totle_add_num ,
               |sum(b.tpre) as effect_tpre_add_num ,
               |sum(b.years) as effect_year_add_num ,
               |sum(b.total) as effect_totle_add_num ,
               |0 as batch_tpre_add_num ,
               |0 as batch_year_add_num ,
               |0 as batch_totle_add_num ,
               |0 as client_tpre_add_num ,
               |0 as client_year_add_num ,
               |0 as client_totle_add_num ,
               |sum(c.tpre) as deal_tpre_add_num ,
               |sum(c.years) as deal_year_add_num ,
               |sum(c.total) as deal_totle_add_num
               |from
               |(
               |select
               |a.inf_source,
               |count(distinct(
               |case
               |when to_date(a.rec_crt_ts)='$today_dt'
               |then a.cdhd_usr_id
               |end)) as tpre,
               |count(distinct(
               |case
               |when to_date(a.rec_crt_ts)>=trunc('$today_dt','YYYY')
               |and to_date(a.rec_crt_ts)<='$today_dt'
               |then a.cdhd_usr_id
               |end)) as years,
               |count(distinct(
               |case
               |when to_date(a.rec_crt_ts)<='$today_dt'
               |then a.cdhd_usr_id
               |end)) as total
               |from
               |(
               |select
               |inf_source,
               |cdhd_usr_id,
               |rec_crt_ts
               |from
               |hive_pri_acct_inf
               |where
               |usr_st='1') a
               |group by
               |a.inf_source ) a
               |left join
               |(
               |select
               |a.inf_source,
               |count(distinct(
               |case
               |when to_date(a.rec_crt_ts)='$today_dt'
               |and to_date(b.card_dt)='$today_dt'
               |then a.cdhd_usr_id
               |end)) as tpre,
               |count(distinct(
               |case
               |when to_date(a.rec_crt_ts)>=trunc('$today_dt','YYYY')
               |and to_date(a.rec_crt_ts)<='$today_dt'
               |and to_date(b.card_dt)>=trunc('$today_dt','YYYY')
               |and to_date(b.card_dt)<='$today_dt'
               |then a.cdhd_usr_id
               |end)) as years,
               |count(distinct(
               |case
               |when to_date(a.rec_crt_ts)<='$today_dt'
               |and to_date(b.card_dt)<='$today_dt'
               |then a.cdhd_usr_id
               |end)) as total
               |from
               |(
               |select
               |inf_source,
               |cdhd_usr_id,
               |rec_crt_ts
               |from
               |hive_pri_acct_inf
               |where
               |usr_st='1' ) a
               |inner join
               |(
               |select distinct
               |(cdhd_usr_id),
               |rec_crt_ts as card_dt
               |from
               |hive_card_bind_inf
               |where
               |card_auth_st in ('1','2','3') ) b
               |on
               |a.cdhd_usr_id=b.cdhd_usr_id
               |group by
               |a.inf_source) b
               |on
               |trim(a.inf_source)=trim(b.inf_source)
               |left join
               |(
               |select
               |a.inf_source,
               |count(distinct (a.cdhd_usr_id)),
               |count(distinct(
               |case
               |when to_date(a.rec_crt_ts)='$today_dt'
               |and to_date(b.trans_dt)='$today_dt'
               |then a.cdhd_usr_id
               |end)) as tpre,
               |count(distinct(
               |case
               |when to_date(a.rec_crt_ts)>=trunc('$today_dt','YYYY')
               |and to_date(a.rec_crt_ts)<='$today_dt'
               |and to_date(b.trans_dt)>=trunc('$today_dt','YYYY')
               |and to_date(b.trans_dt)<='$today_dt'
               |then a.cdhd_usr_id
               |end)) as years,
               |count(distinct(
               |case
               |when to_date(a.rec_crt_ts)<='$today_dt'
               |and to_date(b.trans_dt)<='$today_dt'
               |then a.cdhd_usr_id
               |end)) as total
               |from
               |(
               |select
               |inf_source,
               |cdhd_usr_id,
               |rec_crt_ts
               |from
               |hive_pri_acct_inf
               |where
               |usr_st='1' ) a
               |inner join
               |(
               |select distinct
               |(cdhd_usr_id),
               |trans_dt
               |from
               |hive_acc_trans) b
               |on
               |a.cdhd_usr_id=b.cdhd_usr_id
               |group by
               |a.inf_source ) c
               |on
               |trim(a.inf_source)=trim(c.inf_source)
               |left join
               |(
               |select
               |dtl.access_id,
               |dtl.access_nm,
               |cla.class
               |from
               |hive_inf_source_dtl dtl
               |left join
               |hive_inf_source_class cla
               |on
               |trim(cla.access_nm)=trim(dtl.access_nm) ) d
               |on
               |trim(a.inf_source)=trim(d.access_id)
               |group by
               |d.class, d.access_nm
          """.stripMargin)
          println(s"#### JOB_DM_3 spark sql 清洗[$today_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())
          //println(s"#### JOB_DM_3 spark sql 清洗[$today_dt]数据 results:"+results.count())
          if (!Option(results).isEmpty) {
            results.save2Mysql("dm_user_regist_channel")
            println(s"#### JOB_DM_3 [$today_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
          } else {
            println(s"#### JOB_DM_3 spark sql 清洗[$today_dt]数据无结果集！")
          }
          today_dt = DateUtils.addOneDay(today_dt)
        }
      }
    }
  }


  /**
    * JOB_DM_4/10-14
    * DM_USER_CARD_AUTH->HIVE_PRI_ACCT_INF,HIVE_CARD_BIND_INF,HIVE_ACC_TRANS
    * Code by Xue
    *
    * @param sqlContext
    * @return
    */
  def JOB_DM_4 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("###JOB_DM_4(dm_user_card_auth->hive_pri_acct_inf,hive_card_bind_inf,hive_acc_trans)")
    DateUtils.timeCost("JOB_DM_4") {
      UPSQL_JDBC.delete("dm_user_card_auth","report_dt",start_dt,end_dt)
      println("#### JOB_DM_4 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      var today_dt=start_dt
      if(interval>=0){
        sqlContext.sql(s"use $hive_dbname")
        for(i <- 0 to interval){
          println(s"#### JOB_DM_4 spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())
          val results = sqlContext.sql(
            s"""
               |select
               |a.card_auth_nm as card_auth,
               |a.realnm_in as diff_name,
               |'$today_dt' as report_dt,
               |sum(a.tpre)   as   effect_tpre_add_num ,
               |sum(a.years)  as   effect_year_add_num ,
               |sum(a.total)  as   effect_totle_add_num,
               |sum(b.tpre)   as   deal_tpre_add_num   ,
               |sum(b.years)  as   deal_year_add_num   ,
               |sum(b.total)  as   deal_totle_add_num
               |
               |from (
               |select
               |(case when tempb.card_auth_st='0' then   '未认证'
               | when tempb.card_auth_st='1' then   '支付认证'
               | when tempb.card_auth_st='2' then   '可信认证'
               | when tempb.card_auth_st='3' then   '可信+支付认证'
               |else '未认证' end) as card_auth_nm,
               |tempa.realnm_in as realnm_in,
               |count(distinct(case when tempa.rec_crt_ts='$today_dt'  and tempb.card_dt='$today_dt'  then tempa.cdhd_usr_id end)) as tpre,
               |count(distinct(case when tempa.rec_crt_ts>=trunc('$today_dt','YYYY') and tempa.rec_crt_ts<='$today_dt'
               |and tempb.card_dt>=trunc('$today_dt','YYYY')  and tempb.CARD_DT<='$today_dt' then  tempa.cdhd_usr_id end)) as years,
               |count(distinct(case when tempa.rec_crt_ts<='$today_dt' and tempb.card_dt<='$today_dt'  then tempa.cdhd_usr_id end)) as total
               |from
               |(select cdhd_usr_id,to_date(rec_crt_ts) as rec_crt_ts,realnm_in from hive_pri_acct_inf
               |where usr_st='1' ) tempa
               |inner join
               |(select distinct tempe.cdhd_usr_id as cdhd_usr_id,
               |tempe.card_auth_st as card_auth_st,
               |to_date(tempe.rec_crt_ts) as card_dt
               |from hive_card_bind_inf tempe) tempb
               |on tempa.cdhd_usr_id=tempb.cdhd_usr_id
               |group by
               |case when tempb.card_auth_st='0' then   '未认证'
               | when tempb.card_auth_st='1' then   '支付认证'
               | when tempb.card_auth_st='2' then   '可信认证'
               | when tempb.card_auth_st='3' then   '可信+支付认证'
               |else '未认证' end,tempa.realnm_in
               |) a
               |
               |left join
               |
               |(
               |select
               |(case when tempc.card_auth_st='0' then   '未认证'
               | when tempc.card_auth_st='1' then   '支付认证'
               | when tempc.card_auth_st='2' then   '可信认证'
               | when tempc.card_auth_st='3' then   '可信+支付认证'
               |else '未认证' end) as card_auth_nm,
               |count(distinct(case when tempc.rec_crt_ts='$today_dt'  and tempd.trans_dt='$today_dt' then  tempc.cdhd_usr_id end)) as tpre,
               |count(distinct(case when tempc.rec_crt_ts>=trunc('$today_dt','YYYY') and tempc.rec_crt_ts<='$today_dt'
               |and tempd.trans_dt>=trunc('$today_dt','YYYY') and  tempd.trans_dt<='$today_dt' then  tempc.cdhd_usr_id end)) as years,
               |count(distinct(case when tempc.rec_crt_ts<='$today_dt' and  tempd.trans_dt<='$today_dt'  then  tempc.cdhd_usr_id end)) as total
               |from
               |(select distinct cdhd_usr_id,card_auth_st,to_date(rec_crt_ts) as rec_crt_ts from hive_card_bind_inf) tempc
               |inner join (select distinct cdhd_usr_id,to_date(trans_dt) as trans_dt from hive_acc_trans ) tempd
               |on tempc.cdhd_usr_id=tempd.cdhd_usr_id
               |group by
               |case when tempc.card_auth_st='0' then   '未认证'
               | when tempc.card_auth_st='1' then   '支付认证'
               | when tempc.card_auth_st='2' then   '可信认证'
               | when tempc.card_auth_st='3' then   '可信+支付认证'
               |else '未认证' end
               |) b
               |on a.card_auth_nm=b.card_auth_nm
               |group by a.card_auth_nm,
               |a.realnm_in,
               |'$today_dt'
               | """.stripMargin)

          println(s"#### JOB_DM_4 spark sql 清洗[$today_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())

          if (!Option(results).isEmpty) {
            results.save2Mysql("dm_user_card_auth")
            println(s"#### JOB_DM_4 [$today_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime()
            )
          } else {
            println(s"#### JOB_DM_4 spark sql 清洗[$today_dt]数据无结果集！")
          }
          today_dt=DateUtils.addOneDay(today_dt)
        }
      }
    }
  }

  /**
    * JobName: JOB_DM_5
    * Feature: hive_pri_acct_inf+hive_acc_trans+hive_card_bind_inf+hive_card_bin->dm_user_card_iss
    *
    * @author tzq
    * @time 2016-9-27
    * @param sqlContext,start_dt,end_dt,interval
    */
  def JOB_DM_5(implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("###JOB_DM_5(hive_pri_acct_inf+hive_acc_trans+hive_card_bind_inf+hive_card_bin->dm_user_card_iss)")

    DateUtils.timeCost("JOB_DM_5"){
      UPSQL_JDBC.delete("dm_user_card_iss","report_dt",start_dt,end_dt);
      println( "#### JOB_DM_5 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())

      var today_dt=start_dt
      if(interval>0 ){
        sqlContext.sql(s"use $hive_dbname")
        for(i <- 0 to interval){
          println(s"#### JOB_DM_5 spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())
          val results=sqlContext.sql(
            s"""
               |
             |select
               |trim(a.iss_ins_cn_nm) as card_iss,
               |'$today_dt' as report_dt,
               |nvl(sum(a.tpre),0) as effect_tpre_add_num ,
               |nvl(sum(a.years),0) as effect_year_add_num ,
               |nvl(sum(a.total),0) as effect_totle_add_num,
               |0 as batch_tpre_add_num,
               |0 as batch_year_add_num,
               |0 as batch_totle_add_num,
               |0 as client_tpre_add_num,
               |0 as client_year_add_num,
               |0 as client_totle_add_num,
               |nvl(sum(b.tpre),0) as deal_tpre_add_num ,
               |nvl(sum(b.years),0) as deal_year_add_num ,
               |nvl(sum(b.total),0) as  deal_totle_add_num
               |from
               |(
               |select iss_ins_cn_nm,
               |count(distinct(case when substr(rec_crt_ts,1,10)='$today_dt' and substr(card_dt,1,10)='$today_dt' then a.cdhd_usr_id end)) as tpre,
               |count(distinct(case when substr(rec_crt_ts,1,10)>=trunc('$today_dt',"YY") and substr(rec_crt_ts,1,10)<='$today_dt'
               |and substr(card_dt,1,10)>=trunc('$today_dt',"YY") and substr(card_dt,1,10)<='$today_dt' then a.cdhd_usr_id end)) as years,
               |count(distinct(case when substr(rec_crt_ts,1,10)<='$today_dt' and substr(card_dt,1,10)<='$today_dt' then a.cdhd_usr_id end)) as total
               |
                   |from (
               |select cdhd_usr_id, rec_crt_ts
               |from hive_pri_acct_inf
               |where usr_st='1'
               |) a
               |inner join (
               |select distinct cdhd_usr_id,iss_ins_cn_nm,rec_crt_ts as card_dt
               |from hive_card_bind_inf where card_auth_st in ('1','2','3')
               |) b
               |on a.cdhd_usr_id=b.cdhd_usr_id
               |group by iss_ins_cn_nm) a
               |
                   |left join
               |(
               |select iss_ins_cn_nm,
               |count(distinct(case when substr(rec_crt_ts,1,10)='$today_dt' then cdhd_usr_id end)) as tpre,
               |count(distinct(case when substr(rec_crt_ts,1,10)>=trunc('$today_dt',"YY")
               |and substr(rec_crt_ts,1,10)<='$today_dt' then cdhd_usr_id end)) as years,
               |count(distinct(case when substr(rec_crt_ts,1,10)<='$today_dt' then cdhd_usr_id end)) as total
               |from (select iss_ins_cn_nm,card_bin from hive_card_bin
               |) a
               |inner join
               |(select distinct cdhd_usr_id,substr(card_no,1,8) as card_bin,rec_crt_ts from hive_acc_trans ) b
               |on a.card_bin=b.card_bin
               |group by iss_ins_cn_nm ) b
               |on a.iss_ins_cn_nm=b.iss_ins_cn_nm
               |group by trim(a.iss_ins_cn_nm),'$today_dt'
               |
      """.stripMargin)
          println(s"#### JOB_DM_5 spark sql 清洗[$today_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())
          println(s"###JOB_DM_5------$today_dt results:"+results.count())
          if(!Option(results).isEmpty){
            results.save2Mysql("dm_user_card_iss")
            println(s"#### JOB_DM_5 [$today_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
          }else{
            println(s"#### JOB_DM_5 spark sql 清洗[$today_dt]数据无结果集！")
          }
          today_dt=DateUtils.addOneDay(today_dt)
        }
      }
    }

  }

  /**
    * JobName: JOB_DM_6
    * Feature: dm_user_card_nature
    *
    * @author tzq
    * @time   2016-9-27
    * @param sqlContext,start_dt,end_dt,interval
    */
  def JOB_DM_6(implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("JOB_DM_6(dm_user_card_nature->hive_pri_acct_inf+hive_card_bind_inf+hive_acc_trans)")
    DateUtils.timeCost("JOB_DM_6"){
      UPSQL_JDBC.delete("dm_user_card_nature","report_dt",start_dt,end_dt)
      println( "#### JOB_DM_6 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())

      var today_dt=start_dt
      if(interval>=0 ){
        sqlContext.sql(s"use $hive_dbname")
        println(s"#### JOB_DM_6 spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())
        for(i <- 0 to interval){
          val results=sqlContext.sql(
            s"""
               |select
               |a.card_auth_nm as card_nature,
               |'$today_dt'   as report_dt,
               |nvl(a.tpre,0)   as   effect_tpre_add_num ,
               |nvl(a.years,0)  as   effect_year_add_num ,
               |nvl(a.total,0) as   effect_totle_add_num,
               |0 as batch_tpre_add_num,
               |0 as batch_year_add_num,
               |0 as batch_totle_add_num,
               |0 as client_tpre_add_num,
               |0 as client_year_add_num,
               |0 as client_totle_add_num,
               |nvl(b.tpre,0)   as   deal_tpre_add_num ,
               |nvl(b.years,0)  as   deal_year_add_num ,
               |nvl(b.total,0) as   deal_totle_add_num
               |
               |from (
               |select
               |(case when card_auth_st='0' then   '默认'
               |  when card_auth_st='1' then   '支付认证'
               |  when card_auth_st='2' then   '可信认证'
               |  when card_auth_st='3' then   '可信+支付认证'
               | else '--' end) as card_auth_nm,
               |count(distinct(case when substr(rec_crt_ts,1,10)='$today_dt'  and substr(CARD_DT,1,10)='$today_dt'  then a.cdhd_usr_id end)) as tpre,
               |count(distinct(case when substr(rec_crt_ts,1,10)>=trunc('$today_dt',"YY") and substr(rec_crt_ts,1,10)<='$today_dt'
               |     and substr(CARD_DT,1,10)>=trunc('$today_dt',"YY") and  substr(CARD_DT,1,10)<='$today_dt' then  a.cdhd_usr_id end)) as years,
               |count(distinct(case when substr(rec_crt_ts,1,10)<='$today_dt' and  substr(CARD_DT,1,10)<='$today_dt'  then  a.cdhd_usr_id end)) as total
               |from
               |(select cdhd_usr_id,rec_crt_ts from hive_pri_acct_inf
               |where usr_st='1' ) a
               |inner join (select distinct cdhd_usr_id,card_auth_st,rec_crt_ts as CARD_DT from hive_card_bind_inf ) b
               |on a.cdhd_usr_id=b.cdhd_usr_id
               |group by
               |case when card_auth_st='0' then   '默认'
               |  when card_auth_st='1' then   '支付认证'
               |  when card_auth_st='2' then   '可信认证'
               |  when card_auth_st='3' then   '可信+支付认证'
               | else '--' end) a
               |
               | left join
               |(
               |select
               |(case when card_auth_st='0' then   '默认'
               |  when card_auth_st='1' then   '支付认证'
               |  when card_auth_st='2' then   '可信认证'
               |  when card_auth_st='3' then   '可信+支付认证'
               | else '--' end) as card_auth_nm,
               |count(distinct(case when substr(rec_crt_ts,1,10)='$today_dt'  and substr(trans_dt,1,10)='$today_dt'  then a.cdhd_usr_id end)) as tpre,
               |count(distinct(case when substr(rec_crt_ts,1,10)>=trunc('$today_dt',"YY") and substr(rec_crt_ts,1,10)<='$today_dt'
               |and substr(trans_dt,1,10)>=trunc('$today_dt',"YY") and  substr(trans_dt,1,10)<='$today_dt' then  a.cdhd_usr_id end)) as years,
               |count(distinct(case when substr(rec_crt_ts,1,10)<='$today_dt' and  substr(trans_dt,1,10)<='$today_dt'  then a.cdhd_usr_id end)) as total
               |from (select distinct cdhd_usr_id,card_auth_st,rec_crt_ts from hive_card_bind_inf) a
               |inner join (select distinct cdhd_usr_id,trans_dt from hive_acc_trans ) b
               |on a.cdhd_usr_id=b.cdhd_usr_id
               |group by
               |case when card_auth_st='0' then   '默认'
               |  when card_auth_st='1' then   '支付认证'
               |  when card_auth_st='2' then   '可信认证'
               |  when card_auth_st='3' then   '可信+支付认证'
               | else '--' end) b
               | on a.card_auth_nm=b.card_auth_nm
               |
            """.stripMargin)
          println(s"#### JOB_DM_6 spark sql 清洗[$today_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())
          println(s"###JOB_DM_6------$today_dt results:"+results.count())

          if(!Option(results).isEmpty){
            results.save2Mysql("dm_user_card_nature")
            println(s"#### JOB_DM_6 [$today_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
          }else{
            println(s"#### JOB_DM_6 spark sql 清洗[$today_dt]数据无结果集！")
          }
          today_dt=DateUtils.addOneDay(today_dt)
        }
      }
    }

  }



  /**
    * dm-job-07 20161205
    * dm_user_card_level->HIVE_CARD_BIN,HIVE_CARD_BIND_INF,HIVE_PRI_ACCT_INF
    *
    * @author Xue
    * @param sqlContext
    */
  def JOB_DM_7(implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("###JOB_DM_7(dm_user_card_level->hive_card_bin,hive_card_bind_inf,hive_pri_acct_inf)")
    DateUtils.timeCost("JOB_DM_7") {
      UPSQL_JDBC.delete("dm_user_card_level","report_dt",start_dt,end_dt)
      println("#### JOB_DM_7 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      var today_dt=start_dt

      if(interval>=0 ){
        sqlContext.sql(s"use $hive_dbname")
        for(i <- 0 to interval){

          val results = sqlContext.sql(
            s"""
               |select
               |nvl(nvl(tempa.card_lvl,tempb.card_lvl),'其它') as card_level,
               |'$today_dt' as report_dt,
               |sum(tempa.tpre)   as   effect_tpre_add_num  ,
               |sum(tempa.years)  as   effect_year_add_num  ,
               |sum(tempa.total)  as   effect_totle_add_num ,
               |sum(tempb.tpre)   as   deal_tpre_add_num    ,
               |sum(tempb.years)  as   deal_year_add_num    ,
               |sum(tempb.total)  as   deal_totle_add_num
               |from
               |(
               |select a.card_lvl as card_lvl,
               |count(distinct(case when to_date(c.rec_crt_ts)='$today_dt'  and to_date(b.card_dt)='$today_dt'  then b.cdhd_usr_id end)) as tpre,
               |count(distinct(case when to_date(c.rec_crt_ts)>=trunc('$today_dt','YYYY') and to_date(c.rec_crt_ts)<='$today_dt'
               |     and to_date(b.card_dt)>=trunc('$today_dt','YYYY') and  to_date(b.card_dt)<='$today_dt' then  b.cdhd_usr_id end)) as years,
               |count(distinct(case when to_date(c.rec_crt_ts)<='$today_dt' and  to_date(b.card_dt)<='$today_dt'  then  b.cdhd_usr_id end)) as total
               |from
               |(select card_lvl,card_bin from hive_card_bin ) a
               |inner join
               |(select distinct cdhd_usr_id, rec_crt_ts as card_dt ,substr(bind_card_no,1,8) as card_bin from hive_card_bind_inf where card_auth_st in ('1','2','3')  ) b
               |on a.card_bin=b.card_bin
               |inner join
               |(select cdhd_usr_id,rec_crt_ts from hive_pri_acct_inf where usr_st='1'  ) c on b.cdhd_usr_id=c.cdhd_usr_id
               |group by a.card_lvl ) tempa
               |
               |left join
               |(
               |select a.card_lvl as card_lvl,
               |count(distinct(case when to_date(b.trans_dt)='$today_dt'    then b.cdhd_usr_id end)) as tpre,
               |count(distinct(case when to_date(b.trans_dt)>=trunc('$today_dt','YYYY') and to_date(b.trans_dt)<='$today_dt' then  b.cdhd_usr_id end)) as years,
               |count(distinct(case when to_date(b.trans_dt)='$today_dt'  then  b.cdhd_usr_id end)) as total
               |from
               |(select card_lvl,card_bin from HIVE_CARD_BIN ) a
               |inner join
               |(select distinct cdhd_usr_id,trans_dt,substr(card_no,1,8) as card_bin  from hive_trans_dtl ) b on a.card_bin=b.card_bin
               |group by a.card_lvl ) tempb
               |on tempa.card_lvl=tempb.card_lvl
               |group by nvl(nvl(tempa.card_lvl,tempb.card_lvl),'其它'),'$today_dt'
      """.stripMargin)

          println(s"#### JOB_DM_7 spark sql 清洗[$today_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())

          if (!Option(results).isEmpty) {
            results.save2Mysql("dm_user_card_level")
            println(s"#### JOB_DM_7 [$today_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime()
            )
          } else {
            println(s"#### JOB_DM_7 spark sql 清洗[$today_dt]数据无结果集！")
          }
          today_dt=DateUtils.addOneDay(today_dt)
        }
      }
    }
  }


  /**
    * dm-job-08 20161205
    * dm_store_input_branch->hive_mchnt_inf_wallet,hive_preferential_mchnt_inf,hive_chara_grp_def_bat,hive_access_bas_inf
    *
    * @author Xue
    * @param sqlContext
    */
  def JOB_DM_8(implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("###JOB_DM_8(dm_store_input_branch->hive_mchnt_inf_wallet,hive_preferential_mchnt_inf,hive_chara_grp_def_bat,hive_access_bas_inf)")
    DateUtils.timeCost("JOB_DM_8") {
      UPSQL_JDBC.delete("dm_store_input_branch","report_dt",start_dt,end_dt)
      println("#### JOB_DM_8 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      var today_dt=start_dt

      if(interval>=0 ){
        println(s"#### JOB_DM_8 spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())
        sqlContext.sql(s"use $hive_dbname")
        for(i <- 0 to interval){

          val results = sqlContext.sql(
            s"""
               |select
               |nvl(nvl(nvl(a.cup_branch_ins_id_nm,c.branch_division_cd),d.cup_branch_ins_id_nm),'其它') as input_branch,
               |'$today_dt' as report_dt,
               |sum(a.tpre)   as   store_tpre_add_num  ,
               |sum(a.years)  as   store_year_add_num  ,
               |sum(a.total)  as   store_totle_add_num ,
               |sum(c.tpre)   as   active_tpre_add_num ,
               |sum(c.years)  as   active_year_add_num ,
               |sum(c.total)  as   active_totle_add_num,
               |sum(d.tpre)   as   coupon_tpre_add_num ,
               |sum(d.years)  as   coupon_year_add_num ,
               |sum(d.total)  as   coupon_totle_add_num
               |from(
               |select cup_branch_ins_id_nm,
               |count(distinct(case when to_date(rec_crt_ts)='$today_dt'  then mchnt_cd end)) as tpre,
               |count(distinct(case when to_date(rec_crt_ts)>=trunc('$today_dt','YYYY') and to_date(rec_crt_ts)<='$today_dt' then  mchnt_cd end)) as years,
               |count(distinct(case when to_date(rec_crt_ts)<='$today_dt'  then mchnt_cd end)) as total
               |from hive_mchnt_inf_wallet where substr(open_buss_bmp,1,2)<>00
               |group by  cup_branch_ins_id_nm) a
               |
               |left join
               |
               |(
               |select
               |tempa.branch_division_cd as branch_division_cd,
               |count(distinct(case when to_date(tempa.rec_crt_ts)='$today_dt'  and to_date(tempa.valid_begin_dt)='$today_dt' and tempa.valid_end_dt='$today_dt'  then tempa.mchnt_cd end)) as tpre,
               |count(distinct(case when to_date(tempa.rec_crt_ts)>=trunc('$today_dt','YYYY') and to_date(tempa.rec_crt_ts)<='$today_dt'
               |     and to_date(valid_begin_dt)>=trunc('$today_dt','YYYY') and  to_date(tempa.valid_end_dt)<='$today_dt' then  tempa.mchnt_cd end)) as years,
               |count(distinct(case when to_date(tempa.rec_crt_ts)<='$today_dt' and  to_date(tempa.valid_begin_dt)='$today_dt' and to_date(tempa.valid_end_dt)='$today_dt'  then  tempa.mchnt_cd end)) as total
               |from (
               |select distinct access.cup_branch_ins_id_nm as branch_division_cd,b.rec_crt_ts as rec_crt_ts ,bill.valid_begin_dt as valid_begin_dt, bill.valid_end_dt as valid_end_dt,b.mchnt_cd as mchnt_cd
               |from (select distinct mchnt_cd,rec_crt_ts from hive_preferential_mchnt_inf
               |where mchnt_cd like 'T%' and mchnt_st='2' and mchnt_nm not like '%验证%' and mchnt_nm not like '%测试%'
               |and brand_id<>68988 ) b
               |inner join hive_chara_grp_def_bat grp on b.mchnt_cd=grp.chara_data
               |inner join hive_access_bas_inf access on access.ch_ins_id_cd=b.mchnt_cd
               |inner join (select distinct(chara_grp_cd),valid_begin_dt,valid_end_dt from hive_ticket_bill_bas_inf  ) bill
               |on bill.chara_grp_cd=grp.chara_grp_cd
               |)tempa
               |group by tempa.branch_division_cd )c
               |on a.cup_branch_ins_id_nm=c.branch_division_cd
               |
               |left join
               |
               |(select
               |cup_branch_ins_id_nm,
               |count(distinct(case when to_date(rec_crt_ts)='$today_dt'  then mchnt_cd end)) as tpre,
               |count(distinct(case when to_date(rec_crt_ts)>=trunc('$today_dt','YYYY') and to_date(rec_crt_ts)<='$today_dt' then mchnt_cd end)) as years,
               |count(distinct(case when to_date(rec_crt_ts)<='$today_dt'  then mchnt_cd end)) as total
               |from hive_mchnt_inf_wallet  where substr(open_buss_bmp,1,2) in (10,11)
               |group by cup_branch_ins_id_nm) d
               |on a.cup_branch_ins_id_nm=d.cup_branch_ins_id_nm
               |group by nvl(nvl(nvl(a.cup_branch_ins_id_nm,c.branch_division_cd),d.cup_branch_ins_id_nm),'其它'),'$today_dt'
      """.stripMargin)

          println(s"#### JOB_DM_8 spark sql 清洗[$today_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())

          if (!Option(results).isEmpty) {
            results.save2Mysql("dm_store_input_branch")
            println(s"#### JOB_DM_8 [$today_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime()
            )
          } else {
            println(s"#### JOB_DM_8 spark sql 清洗[$today_dt]数据无结果集！")
          }
          today_dt=DateUtils.addOneDay(today_dt)
        }
      }
    }
  }


  /**
    * JOB_DM_9/10-14
    * dm_store_domain_branch_company->hive_mchnt_inf_wallet,hive_preferential_mchnt_inf,hive_mchnt_tp,hive_mchnt_tp_grp
    * Code by Xue
    *
    * @param sqlContext
    * @return
    */
  def JOB_DM_9 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("###JOB_DM_9(dm_store_domain_branch_company->hive_mchnt_inf_wallet,hive_preferential_mchnt_inf,hive_mchnt_tp,hive_mchnt_tp_grp)")
    DateUtils.timeCost("JOB_DM_9") {
      UPSQL_JDBC.delete(s"dm_store_domain_branch_company","report_dt",start_dt,end_dt)
      println("#### JOB_DM_9 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      var today_dt=start_dt
      if(interval>=0 ){
        sqlContext.sql(s"use $hive_dbname")
        for(i <- 0 to interval){
          println(s"#### JOB_DM_9 spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())
          val results = sqlContext.sql(
            s"""
               |select
               |case when a.gb_region_nm is null then nvl(b.cup_branch_ins_id_nm,nvl(c.gb_region_nm,'其它')) else a.gb_region_nm end as branch_area,
               |'$today_dt' as report_dt,
               |sum(a.tpre)   as   store_tpre_add_num  ,
               |sum(a.years)  as   store_year_add_num  ,
               |sum(a.total)  as   store_totle_add_num ,
               |sum(b.tpre)   as   active_tpre_add_num ,
               |sum(b.years)  as   active_year_add_num ,
               |sum(b.total)  as   active_totle_add_num,
               |sum(c.tpre)   as   coupon_tpre_add_num ,
               |sum(c.years)  as   coupon_year_add_num ,
               |sum(c.total)  as   coupon_totle_add_num
               |FROM
               |(
               |select
               |tempe.prov_division_cd as gb_region_nm,
               |count(case when tempe.rec_crt_ts='$today_dt'  then 1 end) as tpre,
               |count(case when tempe.rec_crt_ts>=trunc('$today_dt','YYYY') and tempe.rec_crt_ts<='$today_dt' then  1 end) as years,
               |count(case when tempe.rec_crt_ts<='$today_dt' then 1 end) as total
               |from
               |(
               |select distinct  prov_division_cd,to_date(rec_crt_ts) as rec_crt_ts,mchnt_prov, mchnt_city_cd, mchnt_county_cd, mchnt_addr
               |from hive_preferential_mchnt_inf
               |where  mchnt_st='2' and mchnt_nm not like '%验证%' and mchnt_nm not like '%测试%' and  brand_id<>68988
               |union all
               |select prov_division_cd, to_date(rec_crt_ts) as rec_crt_ts,mchnt_prov, mchnt_city_cd, mchnt_county_cd, mchnt_addr
               |from hive_preferential_mchnt_inf
               |where mchnt_st='2' and mchnt_nm not like '%验证%' and mchnt_nm not like '%测试%' and brand_id=68988
               |)  tempe
               |group by  tempe.prov_division_cd
               |) a
               |
               |full outer join
               |(select
               |tempb.prov_division_cd as cup_branch_ins_id_nm,
               |sum(case when to_date(tempb.trans_dt)='$today_dt'  then tempb.cnt end) as tpre,
               |sum(case when to_date(tempb.trans_dt)>=trunc('$today_dt','YYYY') and to_date(tempb.trans_dt)<='$today_dt' then  tempb.cnt end) as years,
               |sum(case when to_date(tempb.trans_dt)<='$today_dt'  then  tempb.cnt end) as total
               |from
               |(
               |select nvl(tp.prov_division_cd,'总公司') as prov_division_cd,tp.trans_dt, sum(cnt) as cnt
               |from(
               |select
               |t1.prov_division_cd,
               |t1.trans_dt,
               |count(*) as cnt
               |from (
               |select distinct mchnt.prov_division_cd , mchnt_prov,mchnt_city_cd, mchnt_addr , a.trans_dt
               |from (
               |select distinct card_accptr_cd,card_accptr_term_id,trans_dt
               |from hive_acc_trans
               |where um_trans_id in ('AC02000065','AC02000063') and
               |buss_tp in ('02','04','05','06') and sys_det_cd='S'
               |) a
               |left join hive_store_term_relation b
               |on a.card_accptr_cd=b.mchnt_cd and a.card_accptr_term_id=b.term_id
               |left join hive_preferential_mchnt_inf mchnt
               |on b.third_party_ins_id=mchnt.mchnt_cd
               |where b.third_party_ins_id is not null
               |) t1
               |group by t1.prov_division_cd,t1.trans_dt
               |
               |union all
               |
               |select
               |t2.prov_division_cd,
               |t2.trans_dt,
               |count(*) as cnt
               |from (
               |select distinct mcf.prov_division_cd, a.card_accptr_cd,a.card_accptr_term_id, a.trans_dt
               |from (
               |select distinct card_accptr_cd,card_accptr_term_id, trans_dt,acpt_ins_id_cd
               |from hive_acc_trans
               |where um_trans_id in ('AC02000065','AC02000063') and
               |buss_tp in ('02','04','05','06') and sys_det_cd='S'
               |) a
               |left join hive_store_term_relation b
               |on a.card_accptr_cd=b.mchnt_cd and a.card_accptr_term_id=b.term_id
               |left join  (select distinct ins_id_cd,cup_branch_ins_id_nm as prov_division_cd from hive_ins_inf where length(trim(cup_branch_ins_id_cd))<>0
               |union all
               |select distinct ins_id_cd, cup_branch_ins_id_nm as prov_division_cd from hive_aconl_ins_bas where length(trim(cup_branch_ins_id_cd))<>0  )mcf
               |on a.acpt_ins_id_cd=concat('000',mcf.ins_id_cd)
               |
               |where b.third_party_ins_id is null
               |) t2
               |group by t2.prov_division_cd,t2.trans_dt
               |
               |union all
               |select
               |dis.cup_branch_ins_id_nm as prov_division_cd,
               |dis.settle_dt as trans_dt,
               |count(distinct dis.term_id) as cnt
               |from
               |(select cup_branch_ins_id_nm,term_id,settle_dt
               |from hive_prize_discount_result ) dis
               |left join
               |(select term_id from hive_store_term_relation ) rt
               |on dis.term_id=rt.term_id
               |where rt.term_id is null
               |group by dis.cup_branch_ins_id_nm,dis.settle_dt
               |) tp
               |group by tp.prov_division_cd ,tp.trans_dt
               |
               |) tempb
               |group by tempb.prov_division_cd) b
               |on a.gb_region_nm=b.cup_branch_ins_id_nm
               |
               |full  outer join
               |(
               |select
               |tempd.gb_region_nm as gb_region_nm,
               |count(distinct(case when to_date(tempd.rec_crt_ts)='$today_dt'  then tempd.mchnt_cd end)) as tpre,
               |count(distinct(case when to_date(tempd.rec_crt_ts)>=trunc('$today_dt','YYYY') and to_date(tempd.rec_crt_ts)<='$today_dt' then  tempd.mchnt_cd end)) as years,
               |count(distinct(case when to_date(tempd.rec_crt_ts)<='$today_dt'  then tempd.mchnt_cd end)) as total
               |from hive_mchnt_inf_wallet tempd
               |where substr(tempd.open_buss_bmp,1,2) in (10,11)
               |group by tempd.gb_region_nm) c
               |on a.gb_region_nm=c.gb_region_nm
               |group by
               |case when a.gb_region_nm is null then nvl(b.cup_branch_ins_id_nm,nvl(c.gb_region_nm,'其它')) else a.gb_region_nm end
               | """.stripMargin)
          println(s"#### JOB_DM_9 spark sql 清洗[$today_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())

          if (!Option(results).isEmpty) {
            results.save2Mysql("dm_store_domain_branch_company")
            println(s"#### JOB_DM_9 [$today_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime()
            )
          } else {
            println(s"#### JOB_DM_9 spark sql 清洗[$today_dt]数据无结果集！")
          }
          today_dt = DateUtils.addOneDay(today_dt)

        }
      }
    }
  }

  /**
    * JOB_DM_10/11-7
    * DM_STORE_DIRECT_CONTACT_TRAN
    *
    * @author TZQ
    * @param sqlContext
    * @return
    */
  def JOB_DM_10 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("###JOB_DM_10（DM_STORE_DIRECT_CONTACT_TRAN）###")
    DateUtils.timeCost("JOB_DM_10"){
      UPSQL_JDBC.delete(s"DM_STORE_DIRECT_CONTACT_TRAN","REPORT_DT",start_dt,end_dt)
      println( "#### JOB_DM_10 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      var today_dt=start_dt
      if(interval >=0 ){
        sqlContext.sql(s"use $hive_dbname")
        for(i <- 0 to interval){
          val results = sqlContext.sql(
            s"""
               |select
               |tmp.project_name as project_name,
               |'$today_dt' as report_dt,
               |tmp.tpre as store_tpre_add_num ,
               |tmp.years as store_year_add_num ,
               |tmp.total as store_totle_add_num
               |from
               |(
               |select
               |case when trans.FWD_INS_ID_CD in ('00097310','00093600','00095210','00098700','00098500','00097700',
               |'00096400','00096500','00155800','00095840','00097000','00085500','00096900','00093930',
               |'00094200','00093900','00096100','00092210','00092220','00092900','00091600','00092400',
               |'00098800','00098200','00097900','00091900','00092600','00091200','00093320','00031000',
               |'00094500','00094900','00091100','00094520','00093000','00093310') then '直联' else '间联' end as PROJECT_NAME,
               |count(distinct(case when to_date(trans.MSG_SETTLE_DT)='$today_dt' then MCHNT_CD end)) as tpre,
               |count(distinct(case when to_date(trans.MSG_SETTLE_DT)>=trunc(to_date(trans.MSG_SETTLE_DT),"YYYY") and to_date(trans.MSG_SETTLE_DT)<='$today_dt' then MCHNT_CD end)) as years,
               |count(distinct(case when to_date(trans.MSG_SETTLE_DT)<='$today_dt' then MCHNT_CD end)) as total
               |
               |from
               |hive_acc_trans trans
               |
               |left join
               |
               |hive_ins_inf ins
               |on trans.acpt_ins_id_cd=ins.ins_id_cd
               |where trans.fwd_ins_id_cd in ('00097310','00093600','00095210','00098700','00098500','00097700',
               |'00096400','00096500','00155800','00095840','00097000','00085500','00096900','00093930',
               |'00094200','00093900','00096100','00092210','00092220','00092900','00091600','00092400',
               |'00098800','00098200','00097900','00091900','00092600','00091200','00093320','00031000',
               |'00094500','00094900','00091100','00094520','00093000','00093310')
               |
               |group by
               |case when trans.fwd_ins_id_cd in ('00097310','00093600','00095210','00098700','00098500','00097700',
               |'00096400','00096500','00155800','00095840','00097000','00085500','00096900','00093930',
               |'00094200','00093900','00096100','00092210','00092220','00092900','00091600','00092400',
               |'00098800','00098200','00097900','00091900','00092600','00091200','00093320','00031000',
               |'00094500','00094900','00091100','00094520','00093000','00093310') then '直联' else '间联' end
               |
               |
               |union all
               |
               |select
               |case when trans.internal_trans_tp='C00022' then '1.0 规范'
               |when trans.internal_trans_tp='C20022' then '2.0 规范' else '--' end as project_name ,
               |count(distinct(case when to_date(trans.msg_settle_dt)='$today_dt' then mchnt_cd end)) as tpre,
               |count(distinct(case when to_date(trans.msg_settle_dt)>=trunc(to_date(trans.msg_settle_dt),"YYYY") and to_date(trans.msg_settle_dt)<='$today_dt' then mchnt_cd end)) as years,
               |count(distinct(case when to_date(trans.msg_settle_dt)<='$today_dt' then mchnt_cd end)) as total
               |from
               |hive_acc_trans trans
               |left join
               |hive_ins_inf ins
               |on trans.acpt_ins_id_cd=ins.ins_id_cd
               |where internal_trans_tp in ('C00022','C20022')
               |group by case when trans.internal_trans_tp='C00022' then '1.0 规范'
               |when trans.internal_trans_tp='C20022' then '2.0 规范' else '--' end
               |union all
               |select
               |case when trans.internal_trans_tp='C00023' then '终端不改造' else '终端改造' end as project_name,
               |count(distinct(case when to_date(trans.msg_settle_dt)='$today_dt' then mchnt_cd end)) as tpre,
               |count(distinct(case when to_date(trans.msg_settle_dt)>=trunc(to_date(trans.msg_settle_dt),"YYYY") and to_date(trans.msg_settle_dt)<='$today_dt' then MCHNT_CD end)) as years,
               |count(distinct(case when to_date(trans.msg_settle_dt)<='$today_dt' then mchnt_cd end)) as total
               |from
               |hive_acc_trans trans
               |left join
               |hive_ins_inf ins
               |on trans.acpt_ins_id_cd=ins.ins_id_cd
               |where internal_trans_tp in ('C00022','C20022','C00023')
               |group by case when trans.internal_trans_tp='C00023' then '终端不改造' else '终端改造' end
               |) tmp
               | """.stripMargin)
          println(s"### JOB_DM_10 ------$today_dt results:"+results.count())
          if(!Option(results).isEmpty){
            results.save2Mysql("DM_STORE_DIRECT_CONTACT_TRAN")
            println(s"#### JOB_DM_10 [$today_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
          }else{
            println(s"#### JOB_DM_10 spark sql 清洗[$today_dt]数据无结果集！")
          }
          today_dt=DateUtils.addOneDay(today_dt)
        }
      }
    }
  }


  /**
    * JobName: JOB_DM_11
    * Feature: DM_DEVELOPMENT_ORG_CLASS
    *
    * @author tzq
    * @time 2016-11-7
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    * @param interval
    */
  def JOB_DM_11 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("###JOB_DM_11（DM_DEVELOPMENT_ORG_CLASS）###")
    DateUtils.timeCost("JOB_DM_11"){
      UPSQL_JDBC.delete(s"DM_DEVELOPMENT_ORG_CLASS","REPORT_DT",start_dt,end_dt)
      println( "#### JOB_DM_11 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())

      var today_dt=start_dt
      if(interval>=0 ){
        sqlContext.sql(s"use $hive_dbname")
        for(i <- 0 to interval){
          println(s"#### JOB_DM_11 spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())
          val results = sqlContext.sql(
            s"""
               |select
               |(case when a.extend_ins_id_cd_class is null then nvl(b.extend_ins_id_cd_class,c.extend_ins_id_cd_class) else  nvl(a.extend_ins_id_cd_class,'其它') end) as org_class,
               |'$today_dt'              as    report_dt         ,
               |sum(c.extend_ins_tpre)    as    extend_ins_tpre   ,
               |sum(c.extend_ins_years)   as    extend_ins_years  ,
               |sum(c.extend_ins_total)   as    extend_ins_total  ,
               |sum(a.brand_tpre)         as    brand_tpre        ,
               |sum(a.brand_years)        as    brand_years       ,
               |sum(a.brand_total)        as    brand_total       ,
               |sum(a.store_tpre )        as    store_tpre        ,
               |sum(a.store_years)        as    store_years       ,
               |sum(a.store_total)        as    store_total       ,
               |sum(b.active_store_tpre)  as    active_store_tpre ,
               |sum(b.active_store_years) as    active_store_years,
               |sum(b.active_store_total) as    active_store_total,
               |sum(b.tran_store_tpre)    as    tran_store_tpre   ,
               |sum(b.tran_store_years)   as    tran_store_years  ,
               |sum(b.tran_store_total)   as    tran_store_total
               |from
               |(select extend_ins_id_cd_class,
               |sum(case when to_date(brand_ts)='$today_dt' and to_date(pre_ts)='$today_dt' then sto_num end) as store_tpre,
               |sum(case when to_date(brand_ts)>=trunc('$today_dt','YYYY') and to_date(brand_ts)<='$today_dt' and to_date(pre_ts)>=trunc('$today_dt','YYYY') and to_date(pre_ts)<='$today_dt' then sto_num end) as store_years,
               |sum(case when to_date(brand_ts)<='$today_dt' and to_date(pre_ts)='$today_dt' then sto_num end) as store_total,
               |sum(case when to_date(brand_ts)='$today_dt' and to_date(pre_ts)='$today_dt' then brand_num end) as brand_tpre,
               |sum(case when to_date(brand_ts)>=trunc('$today_dt','YYYY') and to_date(brand_ts)<='$today_dt' and to_date(pre_ts)>=trunc('$today_dt','YYYY') and to_date(pre_ts)<='$today_dt' then brand_num end) as brand_years,
               |sum(case when to_date(brand_ts)<='$today_dt' and to_date(pre_ts)='$today_dt' then brand_num end) as brand_total
               |from
               |( select
               |(case
               |when substr(t0.extend_ins_id_cd,1,4)>='0100' and substr(t0.extend_ins_id_cd,1,4)<='0599' then '银行'
               |when substr(t0.extend_ins_id_cd,1,4)>='1400' and substr(t0.extend_ins_id_cd,1,4)<='1699' or (substr(t0.extend_ins_id_cd,1,4)>='4800' and substr(t0.extend_ins_id_cd,1,4)<='4999' and substr(t0.extend_ins_id_cd,1,4)<>'4802') then '非金机构'
               |when substr(t0.extend_ins_id_cd,1,4) = '4802' then '银商收单'
               |when substr(t0.extend_ins_id_cd,1,4) in ('4990','4991','8804') or substr(t0.extend_ins_id_cd,1,1) in ('c','C') then '第三方机构'
               |else t0.extend_ins_id_cd end ) as extend_ins_id_cd_class,
               |t0.sto_num,t0.brand_num,t0.brand_ts ,t0.pre_ts
               |from
               |(select bas.extend_ins_id_cd,count(distinct pre.mchnt_cd) as sto_num,count(distinct pre.brand_id) as brand_num ,brand.rec_crt_ts as brand_ts ,pre.rec_crt_ts as pre_ts
               |from hive_access_bas_inf bas
               |left join hive_preferential_mchnt_inf pre
               |on bas.ch_ins_id_cd=pre.mchnt_cd
               |left join hive_brand_inf brand on brand.brand_id=pre.brand_id
               |where
               |( substr(bas.extend_ins_id_cd,1,4) <'0000' or substr(bas.extend_ins_id_cd,1,4) >'0099' )
               |and extend_ins_id_cd<>''
               |and pre.mchnt_st='2' and
               |pre.rec_crt_ts>='2015-01-01-00.00.00.000000'
               |and brand.rec_crt_ts>='2015-01-01-00.00.00.000000'
               |group by bas.extend_ins_id_cd,brand.rec_crt_ts,pre.rec_crt_ts) t0
               |) tmp group by extend_ins_id_cd_class ) a
               |
             |full outer join
               |
             |(select extend_ins_id_cd_class,
               |sum(case when to_date(brand_ts)='$today_dt' and to_date(pre_ts)='$today_dt' then sto_num end) as active_store_tpre,
               |sum(case when to_date(brand_ts)>=trunc('$today_dt','YYYY') and to_date(brand_ts)<='$today_dt' and to_date(pre_ts)>=trunc('$today_dt','YYYY') and to_date(pre_ts)<='$today_dt' then sto_num end) as active_store_years,
               |sum(case when to_date(brand_ts)<='$today_dt' and to_date(pre_ts)<='$today_dt' then sto_num end) as active_store_total,
               |sum(case when to_date(brand_ts)='$today_dt' and to_date(pre_ts)='$today_dt' then brand_num end) as tran_store_tpre,
               |sum(case when to_date(brand_ts)>=trunc('$today_dt','YYYY') and to_date(brand_ts)<='$today_dt' and to_date(pre_ts)>=trunc('$today_dt','YYYY') and to_date(pre_ts)<='$today_dt' then brand_num end) as tran_store_years,
               |sum(case when to_date(brand_ts)<='$today_dt' and to_date(pre_ts)<='$today_dt' then brand_num end) as tran_store_total
               |from
               |( select
               |(case
               |when substr(t1.extend_ins_id_cd,1,4) >= '0100' and substr(t1.extend_ins_id_cd,1,4) <='0599' then '银行'
               |when substr(t1.extend_ins_id_cd,1,4) >= '1400' and substr(t1.extend_ins_id_cd,1,4) <= '1699' or (substr(t1.extend_ins_id_cd,1,4) >= '4800' and substr(t1.extend_ins_id_cd,1,4) <='4999' and substr(t1.extend_ins_id_cd,1,4)<>'4802') then '非金机构'
               |when substr(t1.extend_ins_id_cd,1,4) = '4802' then '银商收单'
               |when substr(t1.extend_ins_id_cd,1,4) in ('4990','4991','8804') or substr(t1.extend_ins_id_cd,1,1) in ('c','C') then '第三方机构'
               |else t1.extend_ins_id_cd end ) as extend_ins_id_cd_class,
               |t1.sto_num,t1.brand_num,t1.brand_ts ,t1.pre_ts
               |from
               |(select
               |bas.extend_ins_id_cd,
               |count(distinct pre.mchnt_cd) as sto_num,
               |count(distinct pre.brand_id) as brand_num ,
               |brand.rec_crt_ts as brand_ts ,
               |pre.rec_crt_ts as pre_ts
               |from hive_access_bas_inf bas
               |left join hive_preferential_mchnt_inf pre
               |on bas.ch_ins_id_cd=pre.mchnt_cd
               |left join hive_brand_inf brand
               |on brand.brand_id=pre.brand_id
               |where
               |( substr(bas.extend_ins_id_cd,1,4) <'0000' or substr(bas.extend_ins_id_cd,1,4) >'0099' )
               |and length(trim(extend_ins_id_cd))<>0
               |and pre.mchnt_st='2'
               |group by bas.extend_ins_id_cd,brand.rec_crt_ts,pre.rec_crt_ts) t1
               |) tmp1 group by extend_ins_id_cd_class ) b
               |on a.extend_ins_id_cd_class=b.extend_ins_id_cd_class
               |full outer join
               |(
               |select extend_ins_id_cd_class,
               |count(case when to_date(entry_ts)='$today_dt' then extend_ins_id_cd_class end) as extend_ins_tpre,
               |count(case when to_date(entry_ts)>=trunc('$today_dt','YYYY') and to_date(entry_ts)<='$today_dt' then extend_ins_id_cd_class end) as extend_ins_years,
               |count(case when to_date(entry_ts)<='$today_dt' then extend_ins_id_cd_class end) as extend_ins_total
               |from (
               |select
               |(case
               |when substr(extend_ins_id_cd,1,4) >= '0100' and substr(extend_ins_id_cd,1,4) <='0599' then '银行'
               |when substr(extend_ins_id_cd,1,4) >= '1400' and substr(extend_ins_id_cd,1,4) <= '1699' or (substr(extend_ins_id_cd,1,4) >= '4800' and substr(extend_ins_id_cd,1,4) <='4999' and substr(extend_ins_id_cd,1,4)<>'4802') then '非金机构'
               |when substr(extend_ins_id_cd,1,4) = '4802' then '银商收单'
               |when substr(extend_ins_id_cd,1,4) in ('4990','4991','8804') or substr(extend_ins_id_cd,1,1) in ('c','C') then '第三方机构'
               |else extend_ins_id_cd end ) as extend_ins_id_cd_class,extend_ins_id_cd,min(entry_ts) as entry_ts
               |from hive_access_bas_inf
               |group by (case
               |when substr(extend_ins_id_cd,1,4) >= '0100' and substr(extend_ins_id_cd,1,4) <='0599' then '银行'
               |when substr(extend_ins_id_cd,1,4) >= '1400' and substr(extend_ins_id_cd,1,4) <= '1699' or (substr(extend_ins_id_cd,1,4) >= '4800' and substr(extend_ins_id_cd,1,4) <='4999' and substr(extend_ins_id_cd,1,4)<>'4802') then '非金机构'
               |when substr(extend_ins_id_cd,1,4) = '4802' then '银商收单'
               |when substr(extend_ins_id_cd,1,4) in ('4990','4991','8804') or substr(extend_ins_id_cd,1,1) in ('c','C') then '第三方机构'
               |else extend_ins_id_cd end ),extend_ins_id_cd
               |having min(entry_ts) >='2015-01-01-00.00.00.000000'
               |) tmp3
               |group by extend_ins_id_cd_class) c
               |on a.extend_ins_id_cd_class=c.extend_ins_id_cd_class
               |group by (case when a.extend_ins_id_cd_class is null then nvl(b.extend_ins_id_cd_class,c.extend_ins_id_cd_class) else  nvl(a.extend_ins_id_cd_class,'其它') end)
               |
             | """.stripMargin)
          println(s"#### JOB_DM_11 spark sql 清洗[$today_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())

          println(s"###JOB_DM_11------$today_dt results:"+results.count())
          if(!Option(results).isEmpty){
            results.save2Mysql("dm_development_org_class")
            println(s"#### JOB_DM_11 [$today_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
          }else{
            println(s"#### JOB_DM_11 spark sql 清洗[$today_dt]数据无结果集！")
          }
          today_dt=DateUtils.addOneDay(today_dt)
        }
      }
    }
  }


  /**
    * JobName:JOB_DM_12
    * Feature:DM_COUPON_PUB_DOWN_BRANCH
    *
    * @author tzq
    * @time  2016-12-6
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    * @param interval
    */
  def JOB_DM_12 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("###JOB_DM_12(DM_COUPON_PUB_DOWN_BRANCH)### "+DateUtils.getCurrentSystemTime())
    DateUtils.timeCost("JOB_DM_12"){
      UPSQL_JDBC.delete(s"DM_COUPON_PUB_DOWN_BRANCH","REPORT_DT",start_dt,end_dt)
      println( "#### JOB_DM_12 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      var today_dt=start_dt
      if(interval>=0 ){
        sqlContext.sql(s"use $hive_dbname")
        for(i <- 0 to interval){
          println(s"#### JOB_DM_12 spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())
          val results =sqlContext.sql(
            s"""
               |select
               |a.cup_branch_ins_id_nm  as coupon_branch,
               |'$today_dt'             as report_dt,
               |a.coupon_class          as class_tpre_add_num,
               |a.coupon_publish        as amt_tpre_add_num,
               |a.coupon_down           as dowm_tpre_add_num,
               |b.batch                 as batch_tpre_add_num
               |from
               |(
               |select cup_branch_ins_id_nm,
               |count(*) as coupon_class ,
               |sum(case when dwn_total_num = -1 then dwn_num else dwn_total_num end) as coupon_publish ,
               |sum(dwn_num) as coupon_down
               |from hive_ticket_bill_bas_inf bill
               |where bill_nm not like '%测试%' and bill_nm not like '%验证%' and bill_id <>'Z00000000020415'
               |and bill_id<>'Z00000000020878' and bill_nm not like '%满2元减1%' and bill_nm not like '%满2分减1分%'
               |and bill_nm not like '%满2减1%' and bill_nm not like '%满2抵1%' and bill_nm not like '测%' and dwn_total_num-dwn_num<100000
               |and to_date(rec_crt_ts)='$today_dt'
               |group by cup_branch_ins_id_nm
               |) a
               |
               |left join
               |(
               |select cup_branch_ins_id_nm,
               |sum(adj.adj_ticket_bill) as batch
               |from
               |hive_ticket_bill_acct_adj_task adj
               |inner join
               |hive_ticket_bill_bas_inf bill
               |on adj.bill_id=bill.bill_id
               |where bill_nm not like '%测试%' and bill_nm not like '%验证%' and bill.bill_id <>'Z00000000020415'
               |and bill.bill_id<>'Z00000000020878' and bill_nm not like '%满2元减1%' and bill_nm not like '%满2分减1分%'
               |and bill_nm not like '%满2减1%' and bill_nm not like '%满2抵1%' and bill_nm not like '测%' and dwn_total_num-dwn_num<100000
               |and usr_tp='1' and to_date(adj.rec_crt_ts)='$today_dt'
               |and current_st='1'
               |group by cup_branch_ins_id_nm
               |) b
               |on a.cup_branch_ins_id_nm=b.cup_branch_ins_id_nm
               |
          """.stripMargin)
          println(s"#### JOB_DM_12 spark sql 清洗[$today_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())

          println(s"###JOB_DM_12------$today_dt results:"+results.count())
          if(!Option(results).isEmpty){
            results.save2Mysql("DM_COUPON_PUB_DOWN_BRANCH")
            println(s"#### JOB_DM_12 [$today_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
          }else{
            println(s"#### JOB_DM_12 spark sql 清洗[$today_dt]数据无结果集！")
          }
          today_dt=DateUtils.addOneDay(today_dt)
        }
      }
    }
  }
  /**
    * JobName:JOB_DM_13
    * Feature:DM_COUPON_PUB_DOWN_IF_ICCARD
    *
    * @author tzq
    * @time 2016-12-6
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    * @param interval
    */
  def JOB_DM_13 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("###JOB_DM_13(DM_COUPON_PUB_DOWN_IF_ICCARD)### "+DateUtils.getCurrentSystemTime())
    DateUtils.timeCost("JOB_DM_13"){
      UPSQL_JDBC.delete(s"DM_COUPON_PUB_DOWN_IF_ICCARD","REPORT_DT",start_dt,end_dt)
      println( "#### JOB_DM_13 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())

      var today_dt=start_dt
      if(interval>=0 ){
        sqlContext.sql(s"use $hive_dbname")
        for(i <- 0 to interval){
          println(s"#### JOB_DM_13 spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())
          val results =sqlContext.sql(
            s"""
               |select
               |a.if_iccard       as if_iccard,
               |'$today_dt'       as report_dt,
               |a.coupon_class    as class_tpre_add_num,
               |a.coupon_publish  as amt_tpre_add_num,
               |a.dwn_num         as dowm_tpre_add_num,
               |b.batch           as batch_tpre_add_num
               |from
               |(
               |select
               |case when pos_entry_md_cd in ('01','05','07','95','98') then '仅限ic卡' else '非仅限ic卡' end as if_iccard,
               |count(*) as coupon_class ,
               |sum(case when dwn_total_num = -1 then dwn_num else dwn_total_num end) as coupon_publish ,
               |sum(dwn_num) as dwn_num
               |from hive_download_trans as dtl,hive_ticket_bill_bas_inf as bill
               |where dtl.bill_id=bill.bill_id
               |and dtl.um_trans_id in ('12','17')
               |and dtl.trans_st='1' and trans_dt='$today_dt' and dtl.bill_nm not like '%机场%' and dtl.bill_nm not like '%住两晚送一晚%'
               |and bill.bill_sub_tp in ('01','03') and bill.bill_nm not like '%测试%' and bill.bill_nm not like '%验证%' and bill.bill_id <>'Z00000000020415'
               |and bill.bill_id<>'Z00000000020878' and bill.bill_nm not like '%满2元减1%' and bill.bill_nm not like '%满2分减1分%'
               |and bill.bill_nm not like '%满2减1%' and bill.bill_nm not like '%满2抵1%' and bill.bill_nm not like '测%' and dwn_total_num-dwn_num<100000
               |group by case when pos_entry_md_cd in ('01','05','07','95','98') then '仅限ic卡' else '非仅限ic卡' end
               |) a
               |left join
               |(
               |select
               |case when pos_entry_md_cd in ('01','05','07','95','98') then '仅限ic卡' else '非仅限ic卡' end as if_iccard,
               |sum(adj.adj_ticket_bill) as batch
               |from
               |hive_ticket_bill_acct_adj_task adj
               |inner join
               |(
               |select bill.cup_branch_ins_id_cd,dtl.bill_id,pos_entry_md_cd
               |from hive_download_trans as dtl,hive_ticket_bill_bas_inf as bill
               |where dtl.bill_id=bill.bill_id
               |and dtl.um_trans_id in ('12','17')
               |and dtl.trans_st='1' and trans_dt='$today_dt' and dtl.bill_nm not like '%机场%' and dtl.bill_nm not like '%住两晚送一晚%'
               |and bill.bill_sub_tp in ('01','03') and bill.bill_nm not like '%测试%' and bill.bill_nm not like '%验证%' and bill.bill_id <>'Z00000000020415'
               |and bill.bill_id<>'Z00000000020878' and bill.bill_nm not like '%满2元减1%' and bill.bill_nm not like '%满2分减1分%'
               |and bill.bill_nm not like '%满2减1%' and bill.bill_nm not like '%满2抵1%' and bill.bill_nm not like '测%' and dwn_total_num-dwn_num<100000
               |) b
               |on adj.bill_id=b.bill_id
               |group by case when pos_entry_md_cd in ('01','05','07','95','98') then '仅限ic卡' else '非仅限ic卡' end )b
               |on a.if_iccard=b.if_iccard
               |
          """.stripMargin)
          println(s"#### JOB_DM_13 spark sql 清洗[$today_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())

          println(s"###JOB_DM_13------$today_dt results:"+results.count())
          if(!Option(results).isEmpty){
            results.save2Mysql("DM_COUPON_PUB_DOWN_IF_ICCARD")
            println(s"#### JOB_DM_13 [$today_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
          }else{
            println(s"#### JOB_DM_13 spark sql 清洗[$today_dt]数据无结果集！")
          }
          today_dt=DateUtils.addOneDay(today_dt)
        }
      }
    }
  }

  /**
    * JobName: JOB_DM_14
    * Feature: DM_COUPON_SHIPP_DELIVER_MERCHANT
    *
    * @author tzq
    * @time 2016-12-6
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    * @param interval
    */
  def JOB_DM_14 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("###JOB_DM_14(DM_COUPON_SHIPP_DELIVER_MERCHANT)### "+DateUtils.getCurrentSystemTime())
    DateUtils.timeCost("JOB_DM_14"){
      UPSQL_JDBC.delete(s"DM_COUPON_SHIPP_DELIVER_MERCHANT","REPORT_DT",start_dt,end_dt)
      println( "#### JOB_DM_14 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())

      var today_dt=start_dt
      if(interval>=0 ){
        sqlContext.sql(s"use $hive_dbname")
        for(i <- 0 to interval){
          println(s"#### JOB_DM_14 spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())
          val results =sqlContext.sql(
            s"""
               |select
               |mchnt_nm                                   as merchant_nm,
               |bill_sub_tp                                as bill_tp,
               |'$today_dt'                                as report_dt,
               |sum(trans_num)                             as deal_num,
               |sum(trans_succ_num)                        as succ_deal_num,
               |sum(trans_succ_num)/sum(trans_num)*100     as deal_succ_rate,
               |sum(trans_amt)                             as deal_amt,
               |sum(del_usr_num)                           as deal_usr_num,
               |sum(card_num)                              as deal_card_num
               |from(
               |select
               |dtl.mchnt_nm,
               |bill.bill_sub_tp,
               |0 as trans_num,
               |count(distinct dtl.trans_seq) as trans_succ_num,
               |sum(dtl.trans_at) as trans_amt,
               |count(distinct dtl.cdhd_usr_id) as del_usr_num,
               |count(distinct dtl.card_no) as card_num
               |from hive_bill_order_trans as dtl,
               |hive_bill_sub_order_trans as sub_dtl,
               |hive_ticket_bill_bas_inf as bill
               |where dtl.bill_order_id=sub_dtl.bill_order_id
               |and sub_dtl.bill_id=bill.bill_id
               |and dtl.order_st='00' and dtl.trans_dt='$today_dt'
               |and bill.bill_sub_tp in ('04','07','08')
               |group by dtl.mchnt_nm,bill.bill_sub_tp
               |
               |union all
               |
               |select
               |dtl.mchnt_nm,
               |bill.bill_sub_tp,
               |count(distinct dtl.trans_seq) as trans_num,
               |0 as trans_succ_num ,
               |0 as trans_amt ,
               |0 as del_usr_num ,
               |0 as card_num
               |from hive_bill_order_trans as dtl,
               |hive_bill_sub_order_trans as sub_dtl,
               |hive_ticket_bill_bas_inf as bill
               |where dtl.bill_order_id=sub_dtl.bill_order_id
               |and sub_dtl.bill_id=bill.bill_id
               |and dtl.order_st<>'00' and dtl.trans_dt='$today_dt'
               |and bill.bill_sub_tp in ('04','07','08')
               |group by dtl.mchnt_nm,bill.bill_sub_tp
               |) a
               |group by mchnt_nm,bill_sub_tp
               |
          """.stripMargin)
          println(s"#### JOB_DM_14 spark sql 清洗[$today_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())
          println(s"###JOB_DM_14------$today_dt results:"+results.count())
          if(!Option(results).isEmpty){
            results.save2Mysql("DM_COUPON_SHIPP_DELIVER_MERCHANT")
            println(s"#### JOB_DM_14 [$today_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
          }else{
            println(s"#### JOB_DM_14 spark sql 清洗[$today_dt]数据无结果集！")
          }
          today_dt=DateUtils.addOneDay(today_dt)
        }
      }
    }
  }

  /**
    * JobName:JOB_DM_15
    * Feature:DM_COUPON_SHIPP_DELIVER_ISS
    *
    * @author tzq
    * @time 2016-12-6
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    * @param interval
    */
  def JOB_DM_15 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("###JOB_DM_15(DM_COUPON_SHIPP_DELIVER_ISS)### "+DateUtils.getCurrentSystemTime())
    DateUtils.timeCost("JOB_DM_15"){

      UPSQL_JDBC.delete(s"DM_COUPON_SHIPP_DELIVER_ISS","REPORT_DT",start_dt,end_dt)
      println( "#### JOB_DM_15 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())

      var today_dt=start_dt
      if(interval>=0 ){
        sqlContext.sql(s"use $hive_dbname")
        for(i <- 0 to interval){
          println(s"#### JOB_DM_15 spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())

          val results =sqlContext.sql(
            s"""
               |select
               |trim(iss_ins_cn_nm)                    as  card_iss,
               |bill_sub_tp                            as  bill_tp,
               |'$today_dt'                            as  report_dt,
               |sum(trans_num)                         as  deal_num,
               |sum(trans_succ_num)                    as  succ_deal_num,
               |sum(trans_succ_num)/sum(trans_num)*100 as  deal_succ_rate,
               |sum(trans_amt)                         as  deal_amt,
               |sum(del_usr_num)                       as  deal_usr_num,
               |sum(card_num)                          as  deal_card_num
               |from(
               |select
               |card.iss_ins_cn_nm,
               |bill.bill_sub_tp,
               |0  as trans_num,
               |count(distinct dtl.trans_seq) as trans_succ_num,
               |sum(dtl.trans_at) as trans_amt,
               |count(distinct dtl.cdhd_usr_id)  as del_usr_num,
               |count(distinct dtl.card_no) as card_num
               |from
               |hive_bill_order_trans as dtl,
               |hive_bill_sub_order_trans as  sub_dtl,
               |hive_ticket_bill_bas_inf as bill,
               |hive_card_bind_inf as card
               |where dtl.bill_order_id=sub_dtl.bill_order_id
               |and sub_dtl.bill_id=bill.bill_id
               |and card.cdhd_usr_id=dtl.cdhd_usr_id
               |and dtl.order_st='00' and dtl.trans_dt='$today_dt'
               |and  bill.bill_sub_tp in ('04','07','08')
               |group by iss_ins_cn_nm,bill.bill_sub_tp
               |
               |union all
               |
               |select
               |card.iss_ins_cn_nm,
               |bill.bill_sub_tp,
               |count(distinct dtl.trans_seq) as trans_num,
               |0 as trans_succ_num ,
               |0 as trans_amt ,
               |0 as del_usr_num ,
               |0 as card_num
               |from hive_bill_order_trans as dtl,
               |hive_bill_sub_order_trans as sub_dtl,
               |hive_ticket_bill_bas_inf as bill,
               |hive_card_bind_inf as card
               |where dtl.bill_order_id=sub_dtl.bill_order_id
               |and sub_dtl.bill_id=bill.bill_id
               |and card.cdhd_usr_id=dtl.cdhd_usr_id
               |and dtl.order_st<>'00' and dtl.trans_dt='$today_dt'
               |and  bill.bill_sub_tp in ('04','07','08')
               |group by card.iss_ins_cn_nm,bill.bill_sub_tp
               |) a
               |group by iss_ins_cn_nm,bill_sub_tp
               |
               |
          """.stripMargin)
          println(s"#### JOB_DM_15 spark sql 清洗[$today_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())
          println(s"###JOB_DM_15------$today_dt results:"+results.count())

          if(!Option(results).isEmpty){
            results.save2Mysql("DM_COUPON_SHIPP_DELIVER_ISS")
            println(s"#### JOB_DM_15 [$today_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
          }else{
            println(s"#### JOB_DM_15 spark sql 清洗[$today_dt]数据无结果集！")
          }
          today_dt=DateUtils.addOneDay(today_dt)
        }
      }
    }
  }

  /**
    * JobName: JOB_DM_16
    * Feature: DM_COUPON_SHIPP_DELIVER_BRANCH
    *
    * @author tzq
    * @time 2016-12-7
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    * @param interval
    */
  def JOB_DM_16 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("###JOB_DM_16(DM_COUPON_SHIPP_DELIVER_BRANCH)### "+DateUtils.getCurrentSystemTime())
    DateUtils.timeCost("JOB_DM_16"){
      UPSQL_JDBC.delete(s"DM_COUPON_SHIPP_DELIVER_BRANCH","REPORT_DT",start_dt,end_dt)
      println( "#### JOB_DM_16 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      var today_dt=start_dt
      if(interval>=0 ){
        sqlContext.sql(s"use $hive_dbname")
        for(i <- 0 to interval){
          println(s"#### JOB_DM_16 spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())
          val results =sqlContext.sql(
            s"""
               |select
               |cup_branch_ins_id_nm                     as branch_nm     ,
               |bill_sub_tp                              as bill_tp       ,
               |'$today_dt'                              as report_dt     ,
               |sum(trans_num)                           as deal_num      ,
               |sum(trans_succ_num)                      as succ_deal_num ,
               |sum(trans_succ_num)/sum(trans_num)*100   as deal_succ_rate,
               |sum(trans_amt)                           as deal_amt      ,
               |sum(del_usr_num)                         as deal_usr_num  ,
               |sum(card_num)                            as deal_card_num
               |from(
               |select
               |bill.cup_branch_ins_id_nm,
               |bill.bill_sub_tp,
               |0  as trans_num,
               |count(distinct dtl.trans_seq) as trans_succ_num,
               |sum(dtl.trans_at) as trans_amt,
               |count(distinct dtl.cdhd_usr_id)  as del_usr_num,
               |count(distinct dtl.card_no) as card_num
               |from hive_bill_order_trans as dtl,
               |hive_bill_sub_order_trans as sub_dtl,
               |hive_ticket_bill_bas_inf as bill
               |where dtl.bill_order_id=sub_dtl.bill_order_id
               |and sub_dtl.bill_id=bill.bill_id
               |and dtl.order_st='00'and dtl.trans_dt='$today_dt'
               |and  bill.bill_sub_tp in ('04','07','08')
               |group by bill.cup_branch_ins_id_nm,bill.bill_sub_tp
               |
               |union all
               |select
               |bill.cup_branch_ins_id_nm,
               |bill.bill_sub_tp,
               |count(distinct dtl.trans_seq) as trans_num,
               |0 as trans_succ_num ,
               |0 as trans_amt ,
               |0 as del_usr_num ,
               |0 as card_num
               |from hive_bill_order_trans as dtl,
               |hive_bill_sub_order_trans as sub_dtl,
               |hive_ticket_bill_bas_inf as bill
               |where dtl.bill_order_id=sub_dtl.bill_order_id
               |and sub_dtl.bill_id=bill.bill_id
               |and dtl.order_st<>'00'and dtl.trans_dt='$today_dt'
               |and  bill.bill_sub_tp in ('04','07','08')
               |group by  bill.cup_branch_ins_id_nm,bill.bill_sub_tp
               |) a
               |group by cup_branch_ins_id_nm,bill_sub_tp
               |
          """.stripMargin)
          println(s"#### JOB_DM_16 spark sql 清洗[$today_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())

          println(s"###JOB_DM_16------$today_dt results:"+results.count())
          if(!Option(results).isEmpty){
            results.save2Mysql("DM_COUPON_SHIPP_DELIVER_BRANCH")
            println(s"#### JOB_DM_16 [$today_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
          }else{
            println(s"#### JOB_DM_16 spark sql 清洗[$today_dt]数据无结果集！")
          }
          today_dt=DateUtils.addOneDay(today_dt)
        }
      }
    }
  }

  /**
    * JobName: JOB_DM_17
    * Feature: DM_COUPON_SHIPP_DELIVER_PHOME_AREA
    *
    * @author tzq
    * @time 2016-12-9
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    * @param interval
    */
  def JOB_DM_17 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("###JOB_DM_17(DM_COUPON_SHIPP_DELIVER_PHOME_AREA)### "+DateUtils.getCurrentSystemTime())
    DateUtils.timeCost("JOB_DM_17"){
      UPSQL_JDBC.delete(s"DM_COUPON_SHIPP_DELIVER_PHOME_AREA","REPORT_DT",start_dt,end_dt)
      println( "#### JOB_DM_17 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      var today_dt=start_dt
      if(interval>=0 ){
        sqlContext.sql(s"use $hive_dbname")
        for(i <- 0 to interval){
          println(s"#### JOB_DM_17 spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())
          val results =sqlContext.sql(
            s"""
               |select
               |phone_location as phone_nm ,
               |bill_sub_tp as bill_tp ,
               |'$today_dt' as report_dt ,
               |sum(trans_num) as deal_num ,
               |sum(trans_succ_num) as succ_deal_num ,
               |sum(trans_succ_num)/sum(trans_num)*100 as deal_succ_rate,
               |sum(trans_amt) as deal_amt ,
               |sum(del_usr_num) as deal_usr_num ,
               |sum(card_num) as deal_card_num
               |from(
               |select
               |acc.phone_location,
               |bill.bill_sub_tp,
               |0 as trans_num,
               |count(distinct dtl.trans_seq) as trans_succ_num,
               |sum(dtl.trans_at) as trans_amt,
               |count(distinct dtl.cdhd_usr_id) as del_usr_num,
               |count(distinct dtl.card_no) as card_num
               |from hive_bill_order_trans as dtl,
               |hive_bill_sub_order_trans as sub_dtl,
               |hive_ticket_bill_bas_inf as bill,
               |hive_pri_acct_inf as acc
               |where dtl.bill_order_id=sub_dtl.bill_order_id
               |and sub_dtl.bill_id=bill.bill_id
               |and dtl.cdhd_usr_id=acc.cdhd_usr_id
               |and dtl.order_st='00'and dtl.trans_dt='$today_dt'
               |and bill.bill_sub_tp in ('04','07','08')
               |group by acc.phone_location ,bill.bill_sub_tp
               |
               |union all
               |
               |select
               |acc.phone_location,
               |bill.bill_sub_tp,
               |count(distinct dtl.trans_seq) as trans_num,
               |0 as trans_succ_num ,
               |0 as trans_amt ,
               |0 as del_usr_num ,
               |0 as card_num
               |from hive_bill_order_trans as dtl,
               |hive_bill_sub_order_trans as sub_dtl,
               |hive_ticket_bill_bas_inf as bill,
               |hive_pri_acct_inf as acc
               |where dtl.bill_order_id=sub_dtl.bill_order_id
               |and sub_dtl.bill_id=bill.bill_id
               |and dtl.cdhd_usr_id=acc.cdhd_usr_id
               |and dtl.order_st<>'00'and dtl.trans_dt='$today_dt'
               |and bill.bill_sub_tp in ('04','07','08')
               |group by acc.phone_location ,bill.bill_sub_tp
               |) a
               |group by phone_location,bill_sub_tp
               |
          """.stripMargin)
          println(s"#### JOB_DM_17 spark sql 清洗[$today_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())

          println(s"###JOB_DM_17------$today_dt results:"+results.count())
          if(!Option(results).isEmpty){
            results.save2Mysql("DM_COUPON_SHIPP_DELIVER_PHOME_AREA")
            println(s"#### JOB_DM_17 [$today_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
          }else{
            println(s"#### JOB_DM_17 spark sql 清洗[$today_dt]数据无结果集！")
          }
          today_dt=DateUtils.addOneDay(today_dt)
        }
      }
    }
  }


  /**
    * JOB_DM_18
    * dm_coupon_shipp_deliver_address->hive_bill_order_trans,hive_bill_sub_order_trans,hive_ticket_bill_bas_inf
    *
    * @author XTP
    * @param sqlContext
    */
  def JOB_DM_18(implicit sqlContext: HiveContext, start_dt: String, end_dt: String, interval: Int) = {
    println("###JOB_DM_18(dm_coupon_shipp_deliver_address->hive_bill_order_trans,hive_bill_sub_order_trans,hive_ticket_bill_bas_inf)")
    DateUtils.timeCost("JOB_DM_18") {
      UPSQL_JDBC.delete("dm_coupon_shipp_deliver_address", "report_dt", start_dt, end_dt);
      println("#### JOB_DM_18 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      var today_dt = start_dt
      if (interval >= 0) {
        sqlContext.sql(s"use $hive_dbname")
        for (i <- 0 to interval) {
          println(s"#### JOB_DM_18 spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())
          val results = sqlContext.sql(
            s"""
               |select
               |tempa.delivery_district_nm as deliver_address,
               |tempa.bill_sub_tp as bill_tp,
               |'$today_dt' as report_dt,
               |sum(tempa.trans_num) as  deal_num,
               |sum(tempa.trans_succ_num) as succ_deal_num,
               |sum(tempa.trans_succ_num)/sum(tempa.trans_num)*100 as deal_succ_rate,
               |sum(tempa.trans_amt)   as deal_amt,
               |sum(tempa.del_usr_num) as deal_usr_num,
               |sum(tempa.card_num) as deal_card_num
               |from
               |(
               |select
               |dtl.delivery_district_nm,
               |bill.bill_sub_tp,
               |0  as trans_num,
               |count(distinct dtl.trans_seq) as trans_succ_num,
               |sum(dtl.trans_at) as trans_amt,
               |count(distinct dtl.cdhd_usr_id)  as del_usr_num,
               |count(distinct dtl.card_no) as card_num
               |from hive_bill_order_trans as dtl,
               |hive_bill_sub_order_trans as sub_dtl,
               |hive_ticket_bill_bas_inf as bill
               |where dtl.bill_order_id=sub_dtl.bill_order_id
               |and sub_dtl.bill_id=bill.bill_id
               |and dtl.order_st='00' and dtl.part_trans_dt='$today_dt'
               |and  bill.bill_sub_tp ='08'
               |group by dtl.delivery_district_nm,bill.bill_sub_tp
               |
               |union all
               |
               |select
               |dtl.delivery_district_nm,
               |bill.bill_sub_tp,
               |count(distinct dtl.trans_seq) as trans_num,
               |0 as trans_succ_num ,
               |0 as trans_amt ,
               |0 as del_usr_num ,
               |0 as card_num
               |from hive_bill_order_trans as dtl,
               |hive_bill_sub_order_trans as sub_dtl,
               |hive_ticket_bill_bas_inf as bill
               |where dtl.bill_order_id=sub_dtl.bill_order_id
               |and sub_dtl.bill_id=bill.bill_id
               |and dtl.order_st<>'00' and dtl.part_trans_dt='$today_dt'
               |and  bill.bill_sub_tp ='08'
               |group by  dtl.delivery_district_nm,bill.bill_sub_tp
               |) tempa
               |group by tempa.delivery_district_nm,tempa.bill_sub_tp
               |
      """.stripMargin)

          println(s"#### JOB_DM_18 spark sql 清洗[$today_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())

          if (!Option(results).isEmpty) {
            results.save2Mysql("dm_coupon_shipp_deliver_address")
            println(s"#### JOB_DM_18 [$today_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime()
            )
          } else {
            println(s"#### JOB_DM_18 spark sql 清洗[$today_dt]数据无结果集！")
          }
          today_dt = DateUtils.addOneDay(today_dt)
        }
      }
    }
  }

  /**
    * JOB_DM_19 20170307
    * dm_swept_area_usr_device->hive_passive_code_pay_trans,hive_pri_acct_inf
    * Code by Xue
    *
    * @param sqlContext
    * @return
    */
  def JOB_DM_19(implicit sqlContext: HiveContext, start_dt: String, end_dt: String, interval: Int) = {
    println("###JOB_DM_19(dm_swept_area_usr_device->hive_passive_code_pay_trans,hive_pri_acct_inf)")
    DateUtils.timeCost("JOB_DM_19") {
      UPSQL_JDBC.delete(s"dm_swept_area_usr_device", "report_dt", start_dt, end_dt)
      println("#### JOB_DM_19 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      var today_dt = start_dt
      if (interval >= 0) {
        println(s"#### JOB_DM_19  spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())
        sqlContext.sql(s"use $hive_dbname")
        for (i <- 0 to interval) {
          val results = sqlContext.sql(
            s"""
               |select
               |nvl(tempa.phone_location,nvl(tempb.phone_location,nvl(tempc.phone_location,nvl(tempd.phone_location,
               |nvl(tempe.phone_location,nvl(tempf.phone_location,nvl(tempg.phone_location,temph.phone_location)))))))
               |as usr_device_area,
               |'$today_dt' as report_dt,
               |sum(tempa.count)   as gen_qrcode_num,
               |sum(tempb.usr_cnt) as gen_qrcode_usr_num,
               |sum(tempc.count)   as sweep_num,
               |sum(tempd.usr_cnt) as sweep_usr_num,
               |sum(tempe.count)   as pay_num,
               |sum(tempf.usr_cnt) as pay_usr_num,
               |sum(tempg.count)   as pay_succ_num,
               |sum(tempg.amt)     as pay_amt,
               |sum(temph.usr_cnt) as pay_succ_usr_num
               |from
               |(select
               |c.phone_location,
               |sum(c.cnt) as count
               |from
               |(select distinct b.phone_location,a.rec_crt_ts, a.cdhd_usr_id,a.cnt
               |from
               |(select cdhd_usr_id,rec_crt_ts,count(*) as cnt
               |from hive_passive_code_pay_trans
               |where to_date(rec_crt_ts)='$today_dt'
               |and tran_certi like '10%'
               |group by cdhd_usr_id,rec_crt_ts) a
               |inner join hive_pri_acct_inf b
               |on a.cdhd_usr_id=b.cdhd_usr_id) c
               |group by c.phone_location ) tempa
               |
               |full outer join
               |
               |(select
               |c.phone_location,
               |count(distinct(c.cdhd_usr_id)) as usr_cnt
               |from
               |( select distinct b.phone_location, a.cdhd_usr_id,  a.rec_crt_ts
               |from
               |( select distinct (cdhd_usr_id), rec_crt_ts
               |from  hive_passive_code_pay_trans
               |where  to_date(rec_crt_ts)='$today_dt'
               |and tran_certi like '10%') a
               |inner join  hive_pri_acct_inf b
               |on  a.cdhd_usr_id=b.cdhd_usr_id) c
               |group by  c.phone_location) tempb
               |on tempa.phone_location=tempb.phone_location
               |
               |full outer join
               |
               |(select
               |c.phone_location,
               |sum(c.cnt) as count
               |from
               |(select distinct (b.phone_location) as phone_location, a.cdhd_usr_id,a.cnt
               |from
               |(select cdhd_usr_id,rec_crt_ts,count(*) as cnt
               |from hive_passive_code_pay_trans
               |where to_date(rec_crt_ts)='$today_dt' and tran_certi like '10%'
               | group by cdhd_usr_id,rec_crt_ts) a
               |inner join hive_pri_acct_inf b on a.cdhd_usr_id=b.cdhd_usr_id)c
               |group by c.phone_location ) tempc
               |on tempa.phone_location=tempc.phone_location
               |
               |full outer join
               |
               |(select
               |c.phone_location,
               |count(distinct(c.cdhd_usr_id)) as usr_cnt
               |from
               |(select distinct (b.phone_location) as phone_location, a.cdhd_usr_id
               |from
               |(select distinct(cdhd_usr_id),rec_crt_ts
               |from hive_passive_code_pay_trans
               |where to_date(rec_crt_ts)='$today_dt'
               |and tran_certi like '10%' ) a
               |inner join hive_pri_acct_inf b
               |on a.cdhd_usr_id=b.cdhd_usr_id)c
               |group by c.phone_location ) tempd
               |on tempa.phone_location=tempd.phone_location
               |
               |full outer join
               |
               |(select
               |c.phone_location,
               |sum(c.cnt) as count
               |from
               |(select distinct b.phone_location, a.cdhd_usr_id,a.cnt
               |from
               |(select cdhd_usr_id,rec_crt_ts,count(*) as cnt
               |from hive_passive_code_pay_trans
               |where to_date(rec_crt_ts)='$today_dt'
               |and tran_certi like '10%'
               |and trans_tp='00' and trans_st in ('03','04','05','06','08','09','10','11')
               |group by cdhd_usr_id,rec_crt_ts) a
               |inner join hive_pri_acct_inf b on a.cdhd_usr_id=b.cdhd_usr_id) c
               |group by c.phone_location ) tempe
               |on tempa.phone_location=tempe.phone_location
               |
               |full outer join
               |
               |(select
               |c.phone_location,
               |count(distinct(c.cdhd_usr_id)) as usr_cnt
               |from
               |(select distinct  b.phone_location, a.cdhd_usr_id
               |from
               |(select distinct(cdhd_usr_id),rec_crt_ts
               |from hive_passive_code_pay_trans
               |where to_date(rec_crt_ts)='$today_dt'
               | and tran_certi like '10%'
               |and trans_tp='00' and trans_st in ('03','04','05','06','08','09','10','11')) a
               |inner join hive_pri_acct_inf b
               |on a.cdhd_usr_id=b.cdhd_usr_id) c
               |group by c.phone_location ) tempf
               |on tempa.phone_location=tempf.phone_location
               |
               |full outer join
               |
               |(select
               |c.phone_location,
               |sum(c.cnt) as count,
               |sum(c.trans_at) as amt
               |from
               |(select distinct b.phone_location, a.cdhd_usr_id,a.cnt,a.trans_at
               |from
               |(select cdhd_usr_id,rec_crt_ts,sum(trans_at) as trans_at ,count(*) as cnt
               |from hive_passive_code_pay_trans
               |where to_date(rec_crt_ts)='$today_dt'
               |and tran_certi like '10%'
               |and trans_tp='00' and trans_st in ('04','10')
               | group by cdhd_usr_id,rec_crt_ts) a
               |inner join hive_pri_acct_inf b
               |on a.cdhd_usr_id=b.cdhd_usr_id) c
               |group by c.phone_location ) tempg
               |on tempa.phone_location=tempg.phone_location
               |
               |full outer join
               |
               |(select
               |c.phone_location,
               |count(distinct(c.cdhd_usr_id)) as usr_cnt
               |from
               |(select distinct b.phone_location, a.cdhd_usr_id
               |from
               |(select distinct (cdhd_usr_id),rec_crt_ts
               |from hive_passive_code_pay_trans
               |where to_date(rec_crt_ts)='$today_dt'
               |and tran_certi like '10%'
               |and trans_tp='00' and trans_st in ('04','10')) a
               |inner join hive_pri_acct_inf b
               |on a.cdhd_usr_id=b.cdhd_usr_id) c
               |group by c.phone_location ) temph
               |on tempa.phone_location=temph.phone_location
               |group by nvl(tempa.phone_location,nvl(tempb.phone_location,nvl(tempc.phone_location,nvl(tempd.phone_location,
               |nvl(tempe.phone_location,nvl(tempf.phone_location,nvl(tempg.phone_location,temph.phone_location)))))))
               | """.stripMargin)
          println(s"#### JOB_DM_19 spark sql 清洗[$today_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())

          if (!Option(results).isEmpty) {
            results.save2Mysql("dm_swept_area_usr_device")
            println(s"#### JOB_DM_19 [$today_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime()
            )
          } else {
            println(s"#### JOB_DM_19 spark sql 清洗[$today_dt]数据无结果集！")
          }
          today_dt = DateUtils.addOneDay(today_dt)
        }
      }
    }
  }


  /**
    * JOB_DM_20 20170307
    * dm_swept_area_store_address->hive_passive_code_pay_trans,hive_mchnt_inf_wallet
    * Code by Xue
    *
    * @param sqlContext
    * @return
    */
  def JOB_DM_20(implicit sqlContext: HiveContext, start_dt: String, end_dt: String, interval: Int) = {
    println("###JOB_DM_20(dm_swept_area_store_address->hive_passive_code_pay_trans,hive_mchnt_inf_wallet)")
    DateUtils.timeCost("JOB_DM_20") {
      UPSQL_JDBC.delete(s"dm_swept_area_store_address", "report_dt", start_dt, end_dt)
      println("#### JOB_DM_20 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      var today_dt = start_dt
      if (interval >= 0) {
        println(s"#### JOB_DM_20 spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())
        sqlContext.sql(s"use $hive_dbname")
        for (i <- 0 to interval) {
          val results = sqlContext.sql(
            s"""
               |select
               |nvl(tempa.gb_region_nm,nvl(tempb.gb_region_nm,nvl(tempc.gb_region_nm,nvl(tempd.gb_region_nm,
               |nvl(tempe.gb_region_nm,nvl(tempf.gb_region_nm,nvl(tempg.gb_region_nm,temph.gb_region_nm)))))))
               |as store_address,
               |'$today_dt' as report_dt,
               |sum(tempa.count)   as gen_qrcode_num,
               |sum(tempb.usr_cnt) as gen_qrcode_usr_num,
               |sum(tempc.count)   as sweep_num,
               |sum(tempd.usr_cnt) as sweep_usr_num,
               |sum(tempe.count)   as pay_num,
               |sum(tempf.usr_cnt) as pay_usr_num,
               |sum(tempg.count)   as pay_succ_num,
               |sum(tempg.amt)     as pay_amt,
               |sum(temph.usr_cnt) as pay_succ_usr_num
               |
               |from
               |(select
               |c.gb_region_nm as gb_region_nm,
               |sum(c.cnt) as count
               |from
               |(select distinct b.gb_region_nm,a.rec_crt_ts, a.cdhd_usr_id,a.cnt
               |from
               |(select cdhd_usr_id,rec_crt_ts,mchnt_cd,count(*) as cnt
               |from hive_passive_code_pay_trans
               |where to_date(rec_crt_ts)='$today_dt'
               |and tran_certi like '10%'
               |group by cdhd_usr_id,rec_crt_ts,mchnt_cd) a
               |inner join hive_mchnt_inf_wallet b
               |on  a.mchnt_cd=b.mchnt_cd) c
               |group by c.gb_region_nm ) tempa
               |
               |full outer join
               |
               |(select
               |c.gb_region_nm as gb_region_nm,
               |count(distinct(c.cdhd_usr_id)) as usr_cnt
               |from
               |( select distinct b.gb_region_nm, a.cdhd_usr_id, a.rec_crt_ts
               |from
               |( select distinct (cdhd_usr_id), rec_crt_ts,mchnt_cd
               |from  hive_passive_code_pay_trans
               |where  to_date(rec_crt_ts)='$today_dt'
               |and tran_certi like '10%') a
               |inner join hive_mchnt_inf_wallet b
               |on  a.mchnt_cd=b.mchnt_cd) c
               |group by  c.gb_region_nm) tempb
               |on tempa.gb_region_nm=tempb.gb_region_nm
               |
               |full outer join
               |
               |(select c.gb_region_nm as gb_region_nm,
               |sum(c.cnt) as count
               |from
               |(select distinct b.gb_region_nm, a.cdhd_usr_id,a.cnt
               |from
               |(select cdhd_usr_id,rec_crt_ts,mchnt_cd,count(*) as cnt
               |from hive_passive_code_pay_trans
               |where to_date(rec_crt_ts)='$today_dt' and tran_certi like '10%'
               |and mchnt_cd is not null and mchnt_cd<>'' group by cdhd_usr_id,rec_crt_ts,mchnt_cd) a
               |inner join hive_mchnt_inf_wallet b
               |on  a.mchnt_cd=b.mchnt_cd)c
               |group by c.gb_region_nm ) tempc
               |on tempa.gb_region_nm=tempc.gb_region_nm
               |
               |full outer join
               |
               |(select c.gb_region_nm as gb_region_nm,
               |count(distinct(c.cdhd_usr_id)) as usr_cnt
               |from
               |(select distinct b.gb_region_nm, a.cdhd_usr_id
               |from
               |(select distinct(cdhd_usr_id),rec_crt_ts,mchnt_cd
               |from hive_passive_code_pay_trans
               |where to_date(rec_crt_ts)='$today_dt'
               |and tran_certi like '10%') a
               |inner join hive_mchnt_inf_wallet b
               |on  a.mchnt_cd=b.mchnt_cd) c
               |group by c.gb_region_nm ) tempd
               |on tempa.gb_region_nm=tempd.gb_region_nm
               |
               |full outer join
               |
               |(select c.gb_region_nm as gb_region_nm,
               |sum(c.cnt) as count
               |from
               |(select distinct b.gb_region_nm, a.cdhd_usr_id,a.cnt
               |from
               |(select cdhd_usr_id,rec_crt_ts,mchnt_cd,count(*) as cnt
               |from hive_passive_code_pay_trans
               |where to_date(rec_crt_ts)='$today_dt'
               |and tran_certi like '10%'
               |and trans_tp='00' and trans_st in ('03','04','05','06','08','09','10','11')
               |group by cdhd_usr_id,rec_crt_ts,mchnt_cd) a
               |inner join hive_mchnt_inf_wallet b
               |on  a.mchnt_cd=b.mchnt_cd) c
               |group by c.gb_region_nm ) tempe
               |on tempa.gb_region_nm=tempe.gb_region_nm
               |
               |full outer join
               |
               |(select c.gb_region_nm as gb_region_nm,
               |count(distinct(c.cdhd_usr_id)) as usr_cnt
               |from
               |(select distinct  b.gb_region_nm, a.cdhd_usr_id
               |from
               |(select distinct(cdhd_usr_id),rec_crt_ts ,mchnt_cd
               |from hive_passive_code_pay_trans
               |where to_date(rec_crt_ts)='$today_dt'
               | and tran_certi like '10%'
               |and trans_tp='00' and trans_st in ('03','04','05','06','08','09','10','11')) a
               |inner join hive_mchnt_inf_wallet b
               |on  a.mchnt_cd=b.mchnt_cd) c
               |group by gb_region_nm ) tempf
               |on tempa.gb_region_nm=tempf.gb_region_nm
               |
               |full outer join
               |
               |(select
               |c.gb_region_nm as gb_region_nm,
               |sum(c.cnt) as count,
               |sum(c.trans_at) as amt
               |from
               |(select distinct b.gb_region_nm, a.cdhd_usr_id,a.cnt,a.trans_at
               |from
               |(select cdhd_usr_id,rec_crt_ts,mchnt_cd,sum(trans_at) as trans_at ,count(*) as cnt
               |from hive_passive_code_pay_trans
               |where to_date(rec_crt_ts)='$today_dt'
               |and tran_certi like '10%'
               |and trans_tp='00' and trans_st in ('04','10')
               | group by cdhd_usr_id,rec_crt_ts,mchnt_cd) a
               |inner join hive_mchnt_inf_wallet b
               |on  a.mchnt_cd=b.mchnt_cd) c
               |group by c.gb_region_nm ) tempg
               |on tempa.gb_region_nm=tempg.gb_region_nm
               |
               |full outer join
               |
               |(select c.gb_region_nm as gb_region_nm,
               |count(distinct(c.cdhd_usr_id)) as usr_cnt
               |from (select distinct b.gb_region_nm, a.cdhd_usr_id
               |from (select distinct (cdhd_usr_id),rec_crt_ts ,mchnt_cd
               |from hive_passive_code_pay_trans
               |where to_date(rec_crt_ts)='$today_dt'
               |and tran_certi like '10%'
               |and trans_tp='00' and trans_st in ('04','10')) a
               |inner join hive_mchnt_inf_wallet b
               |on  a.mchnt_cd=b.mchnt_cd) c
               |group by c.gb_region_nm ) temph
               |on tempa.gb_region_nm=temph.gb_region_nm
               |group by nvl(tempa.gb_region_nm,nvl(tempb.gb_region_nm,nvl(tempc.gb_region_nm,nvl(tempd.gb_region_nm,
               |nvl(tempe.gb_region_nm,nvl(tempf.gb_region_nm,nvl(tempg.gb_region_nm,temph.gb_region_nm)))))))
               |
               | """.stripMargin)
          println(s"#### JOB_DM_20 spark sql 清洗[$today_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())

          if (!Option(results).isEmpty) {
            results.save2Mysql("dm_swept_area_store_address")
            println(s"#### JOB_DM_20 [$today_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime()
            )
          } else {
            println(s"#### JOB_DM_20 spark sql 清洗[$today_dt]数据无结果集！")
          }
          today_dt = DateUtils.addOneDay(today_dt)
        }
      }
    }
  }



  /**
    * JOB_DM_21 20170307
    * dm_swept_store_class_mcc->hive_passive_code_pay_trans,hive_pri_acct_inf
    * Code by Xue
    *
    * @param sqlContext
    * @return
    */
  def JOB_DM_21(implicit sqlContext: HiveContext, start_dt: String, end_dt: String, interval: Int) = {
    println("###JOB_DM_21(dm_swept_store_class_mcc->hive_passive_code_pay_trans,hive_pri_acct_inf)")
    DateUtils.timeCost("JOB_DM_21") {
      UPSQL_JDBC.delete(s"dm_swept_store_class_mcc", "report_dt", start_dt, end_dt)
      println("#### JOB_DM_21 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      var today_dt = start_dt
      if (interval >= 0) {
        println(s"#### JOB_DM_21  spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())
        sqlContext.sql(s"use $hive_dbname")
        for (i <- 0 to interval) {
          val results = sqlContext.sql(
            s"""
               |select
               |tempg.grp_nm as store_frist_nm,
               |tempg.tp_nm as store_second_nm,
               |'$today_dt' as report_dt,
               |sum(tempa.count)   as sweep_num,
               |sum(tempb.usr_cnt) as sweep_usr_num,
               |sum(tempc.count)   as pay_num,
               |sum(tempd.usr_cnt) as pay_usr_num,
               |sum(tempe.count)   as pay_succ_num,
               |sum(tempe.amt)     as pay_amt,
               |sum(tempf.usr_cnt) as pay_succ_usr_num
               |
               |from
               |(select
               |tp_grp.mchnt_tp_grp_desc_cn as grp_nm ,
               |tp.mchnt_tp_desc_cn         as tp_nm,
               |tp.mchnt_tp
               |from  hive_mchnt_tp tp
               |left join hive_mchnt_tp_grp tp_grp
               |on tp.mchnt_tp_grp=tp_grp.mchnt_tp_grp) tempg
               |
               |left join
               |
               |(select
               |c.mchnt_tp as mchnt_tp,
               |sum(c.cnt) as count
               |from
               |(select  a.cdhd_usr_id,a.cnt,b.mchnt_tp as mchnt_tp
               |from
               |(select cdhd_usr_id,rec_crt_ts,mchnt_cd,count(*) as cnt
               |from hive_passive_code_pay_trans
               |where to_date(rec_crt_ts)='$today_dt' and tran_certi like '10%'
               |group by cdhd_usr_id,rec_crt_ts,mchnt_cd) a
               |inner join hive_mchnt_inf_wallet b
               |on  a.mchnt_cd=b.mchnt_cd) c
               |group by c.mchnt_tp ) tempa
               |on tempa.mchnt_tp=tempg.mchnt_tp
               |left join
               |
               |(select
               |c.mchnt_tp as mchnt_tp,
               |count(distinct(c.cdhd_usr_id)) as usr_cnt
               |from
               |(select  a.cdhd_usr_id,b.mchnt_tp as mchnt_tp
               |from
               |(select distinct(cdhd_usr_id),rec_crt_ts,mchnt_cd
               |from hive_passive_code_pay_trans
               |where to_date(rec_crt_ts)='$today_dt'
               | and tran_certi like '10%') a
               |inner join hive_mchnt_inf_wallet b
               |on  a.mchnt_cd=b.mchnt_cd )c
               |group by c.mchnt_tp ) tempb
               |on tempg.mchnt_tp=tempb.mchnt_tp
               |
               |left join
               |
               |(select
               |c.mchnt_tp as mchnt_tp,
               |sum(c.cnt) as count
               |from
               |(select a.cdhd_usr_id,a.cnt,b.mchnt_tp as mchnt_tp
               |from
               |(select cdhd_usr_id,rec_crt_ts,mchnt_cd,count(*) as cnt
               |from hive_passive_code_pay_trans
               |where to_date(rec_crt_ts)='$today_dt'
               |and tran_certi like '10%' and mchnt_cd is not null and mchnt_cd<>''
               |and trans_tp='00' and trans_st in ('03','04','05','06','08','09','10','11')
               |group by cdhd_usr_id,rec_crt_ts,mchnt_cd) a
               |inner join hive_mchnt_inf_wallet b
               |on  a.mchnt_cd=b.mchnt_cd) c
               |group by c.mchnt_tp  ) tempc
               |on tempg.mchnt_tp=tempc.mchnt_tp
               |
               |left join
               |
               |(select
               |c.mchnt_tp as mchnt_tp,
               |count(distinct(c.cdhd_usr_id)) as usr_cnt
               |from
               |(select  a.cdhd_usr_id,b.mchnt_tp as mchnt_tp
               |from
               |(select distinct(cdhd_usr_id),rec_crt_ts ,mchnt_cd
               |from hive_passive_code_pay_trans
               |where to_date(rec_crt_ts)='$today_dt'
               |and tran_certi like '10%'  and mchnt_cd is not null and mchnt_cd<>''
               |and trans_tp='00' and trans_st in ('03','04','05','06','08','09','10','11')) a
               |inner join hive_mchnt_inf_wallet b
               |on  a.mchnt_cd=b.mchnt_cd) c
               |group by c.mchnt_tp ) tempd
               |on tempg.mchnt_tp=tempd.mchnt_tp
               |
               |left join
               |
               |(select
               |c.mchnt_tp as mchnt_tp,
               |sum(c.cnt) as count,
               |sum(c.trans_at) as amt
               |from
               |(select a.cdhd_usr_id,a.cnt,a.trans_at,b.mchnt_tp as mchnt_tp
               |from
               |(select cdhd_usr_id,rec_crt_ts,mchnt_cd,sum(trans_at) as trans_at ,count(*) as cnt
               |from hive_passive_code_pay_trans
               |where to_date(rec_crt_ts)='$today_dt'
               |and tran_certi like '10%' and mchnt_cd is not null and mchnt_cd<>''
               |and trans_tp='00' and trans_st in ('04','10')
               | group by cdhd_usr_id,rec_crt_ts,mchnt_cd) a
               |inner join hive_mchnt_inf_wallet b
               |on  a.mchnt_cd=b.mchnt_cd) c
               |group by c.mchnt_tp) tempe
               |on tempg.mchnt_tp=tempe.mchnt_tp
               |
               |left join
               |
               |(select
               |c.mchnt_tp as mchnt_tp,
               |count(distinct(c.cdhd_usr_id)) as usr_cnt
               |from
               |(select a.cdhd_usr_id,b.mchnt_tp as mchnt_tp
               |from
               |(select distinct (cdhd_usr_id),rec_crt_ts ,mchnt_cd
               |from hive_passive_code_pay_trans
               |where to_date(rec_crt_ts)='$today_dt'
               |and tran_certi like '10%'  and mchnt_cd is not null and mchnt_cd<>''
               |and trans_tp='00' and trans_st in ('04','10')) a
               |inner join hive_mchnt_inf_wallet b
               |on  a.mchnt_cd=b.mchnt_cd) c
               |group by c.mchnt_tp ) tempf
               |on tempg.mchnt_tp=tempf.mchnt_tp
               |where length(trim(tempa.mchnt_tp))>0  or length(trim(tempb.mchnt_tp))>0  or length(trim(tempc.mchnt_tp))>0 or length(trim(tempd.mchnt_tp))>0
               |or length(trim(tempe.mchnt_tp))>0 or length(trim(tempf.mchnt_tp))>0
               |group by tempg.grp_nm ,tempg.tp_nm
               | """.stripMargin)
          println(s"#### JOB_DM_21 spark sql 清洗[$today_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())

          if (!Option(results).isEmpty) {
            results.save2Mysql("dm_swept_store_class_mcc")
            println(s"#### JOB_DM_21 [$today_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime()
            )
          } else {
            println(s"#### JOB_DM_21 spark sql 清洗[$today_dt]数据无结果集！")
          }
          today_dt = DateUtils.addOneDay(today_dt)
        }
      }
    }
  }



  /**
    * JOB_DM_22 20170307
    * dm_swept_store_class_mcc->hive_passive_code_pay_trans,hive_mchnt_inf_wallet
    * Code by Xue
    *
    * @param sqlContext
    * @return
    */
  def JOB_DM_22(implicit sqlContext: HiveContext, start_dt: String, end_dt: String, interval: Int) = {
    println("###JOB_DM_22(dm_swept_merchant->hive_passive_code_pay_trans,hive_mchnt_inf_wallet)")
    DateUtils.timeCost("JOB_DM_22") {
      UPSQL_JDBC.delete(s"dm_swept_store_class_mcc", "report_dt", start_dt, end_dt)
      println("#### JOB_DM_22 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      var today_dt = start_dt
      if (interval >= 0) {
        println(s"#### JOB_DM_22  spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())
        sqlContext.sql(s"use $hive_dbname")
        for (i <- 0 to interval) {
          val results = sqlContext.sql(
            s"""
               |select
               |final.mchnt_cn_nm as store_frist_nm,
               |'$today_dt' as report_dt,
               |sum(final.type) as type,
               |sum(final.sweep_num) as sweep_num,
               |sum(final.sweep_usr_num) as sweep_usr_num,
               |sum(final.pay_num) as pay_num,
               |sum(final.pay_usr_num) as pay_usr_num,
               |sum(final.pay_succ_num) as pay_succ_num,
               |sum(final.pay_amt) as pay_amt,
               |sum(final.pay_succ_usr_num) as pay_succ_usr_num
               |
               |from
               |
               |(select
               |nvl(tempa.mchnt_cn_nm,nvl(tempb.mchnt_cn_nm,nvl(tempc.mchnt_cn_nm,nvl(tempd.mchnt_cn_nm,
               |nvl(tempe.mchnt_cn_nm,tempf.mchnt_cn_nm)))))
               |as mchnt_cn_nm ,
               |'0' as type,
               |sum(tempa.count) as sweep_num,
               |sum(tempb.usr_cnt) as sweep_usr_num,
               |sum(tempc.count) as pay_num,
               |sum(tempd.usr_cnt) as pay_usr_num,
               |sum(tempe.count) as pay_succ_num,
               |sum(tempe.amt) as pay_amt,
               |sum(tempf.usr_cnt) as pay_succ_usr_num
               |from
               |(
               |select c.mchnt_cn_nm,
               |sum(c.cnt) as count
               |from
               |(select distinct b.mchnt_cn_nm, a.cdhd_usr_id,a.cnt
               |from
               |(select cdhd_usr_id,rec_crt_ts,mchnt_cd,count(*) as cnt
               |from hive_passive_code_pay_trans
               |where to_date(rec_crt_ts)='$today_dt' and tran_certi like '10%'
               |group by cdhd_usr_id,rec_crt_ts,mchnt_cd) a
               |inner join hive_mchnt_inf_wallet b
               |on a.mchnt_cd=b.mchnt_cd) c
               |group by c.mchnt_cn_nm ) tempa
               |
               |full outer join
               |
               |(
               |select c.mchnt_cn_nm,
               |count(distinct(c.cdhd_usr_id)) as usr_cnt
               |from
               |(select distinct b.mchnt_cn_nm, a.cdhd_usr_id
               |from
               |(select distinct(cdhd_usr_id),rec_crt_ts,mchnt_cd
               |from hive_passive_code_pay_trans
               |where to_date(rec_crt_ts)='$today_dt'
               |and tran_certi like '10%') a
               |inner join hive_mchnt_inf_wallet b
               |on a.mchnt_cd=b.mchnt_cd) c
               |group by c.mchnt_cn_nm ) tempb
               |on tempa.mchnt_cn_nm=tempb.mchnt_cn_nm
               |
               |full outer join
               |
               |(
               |select c.mchnt_cn_nm,
               |sum(c.cnt) as count
               |from
               |(select distinct b.mchnt_cn_nm, a.cdhd_usr_id,a.cnt
               |from
               |(select cdhd_usr_id,rec_crt_ts,mchnt_cd,count(*) as cnt
               |from hive_passive_code_pay_trans
               |where to_date(rec_crt_ts)='$today_dt'
               |and tran_certi like '10%'
               |and trans_tp='00' and trans_st in ('03','04','05','06','08','09','10','11')
               |group by cdhd_usr_id,rec_crt_ts,mchnt_cd) a
               |inner join hive_mchnt_inf_wallet b
               |on a.mchnt_cd=b.mchnt_cd) c
               |group by c.mchnt_cn_nm ) tempc
               |on tempa.mchnt_cn_nm=tempc.mchnt_cn_nm
               |
               |full outer join
               |
               |(select c.mchnt_cn_nm,
               |count(distinct(c.cdhd_usr_id)) as usr_cnt
               |from
               |(select distinct b.mchnt_cn_nm, a.cdhd_usr_id
               |from
               |(select distinct(cdhd_usr_id),rec_crt_ts ,mchnt_cd
               |from hive_passive_code_pay_trans
               |where to_date(rec_crt_ts)='$today_dt'
               |and tran_certi like '10%'
               |and trans_tp='00' and trans_st in ('03','04','05','06','08','09','10','11')) a
               |inner join hive_mchnt_inf_wallet b
               |on a.mchnt_cd=b.mchnt_cd) c
               |group by c.mchnt_cn_nm ) tempd
               |on tempa.mchnt_cn_nm=tempd.mchnt_cn_nm
               |
               |full outer join
               |
               |(select
               |c.mchnt_cn_nm,
               |sum(c.cnt) as count,
               |sum(c.trans_at) as amt
               |from
               |(select distinct b.mchnt_cn_nm, a.cdhd_usr_id,a.cnt,a.trans_at
               |from
               |(select cdhd_usr_id,rec_crt_ts,mchnt_cd,sum(trans_at) as trans_at ,count(*) as cnt
               |from hive_passive_code_pay_trans
               |where to_date(rec_crt_ts)='$today_dt'
               |and tran_certi like '10%'
               |and trans_tp='00' and trans_st in ('04','10')
               |group by cdhd_usr_id,rec_crt_ts,mchnt_cd) a
               |inner join hive_mchnt_inf_wallet b
               |on a.mchnt_cd=b.mchnt_cd) c
               |group by c.mchnt_cn_nm ) tempe
               |on tempa.mchnt_cn_nm=tempe.mchnt_cn_nm
               |
               |full outer join
               |
               |(select c.mchnt_cn_nm,
               |count(distinct(c.cdhd_usr_id)) as usr_cnt
               |from
               |(select distinct b.mchnt_cn_nm, a.cdhd_usr_id
               |from
               |(select distinct (cdhd_usr_id),rec_crt_ts ,mchnt_cd
               |from hive_passive_code_pay_trans
               |where to_date(rec_crt_ts)='$today_dt'
               |and tran_certi like '10%'
               |and trans_tp='00' and trans_st in ('04','10')) a
               |inner join hive_mchnt_inf_wallet b
               |on a.mchnt_cd=b.mchnt_cd) c
               |group by c.mchnt_cn_nm ) tempf
               |on tempa.mchnt_cn_nm=tempf.mchnt_cn_nm
               |group by nvl(tempa.mchnt_cn_nm,nvl(tempb.mchnt_cn_nm,nvl(tempc.mchnt_cn_nm,nvl(tempd.mchnt_cn_nm,
               |nvl(tempe.mchnt_cn_nm,tempf.mchnt_cn_nm)))))
               |order by sweep_num
               |desc limit 20
               |
               |union all
               |
               |select
               |nvl(ta.mchnt_cn_nm,nvl(tb.mchnt_cn_nm,nvl(tc.mchnt_cn_nm,nvl(td.mchnt_cn_nm,
               |nvl(te.mchnt_cn_nm,tf.mchnt_cn_nm)))))
               |as mchnt_cn_nm ,
               |'1' as type,
               |sum(ta.count) as sweep_num,
               |sum(tb.usr_cnt) as sweep_usr_num,
               |sum(tc.count) as pay_num,
               |sum(td.usr_cnt) as pay_usr_num,
               |sum(te.count) as pay_succ_num,
               |sum(te.amt) as pay_amt,
               |sum(tf.usr_cnt) as pay_succ_usr_num
               |
               |from
               |(
               |select c.mchnt_cn_nm,
               |sum(c.cnt) as count
               |from
               |(select distinct b.mchnt_cn_nm, a.cdhd_usr_id,a.cnt
               |from
               |(select cdhd_usr_id,rec_crt_ts,mchnt_cd,count(*) as cnt
               |from hive_passive_code_pay_trans
               |where to_date(rec_crt_ts)='$today_dt' and tran_certi like '10%' and trans_tp='01'
               |group by cdhd_usr_id,rec_crt_ts,mchnt_cd) a
               |inner join hive_mchnt_inf_wallet b
               |on a.mchnt_cd=b.mchnt_cd) c
               |group by c.mchnt_cn_nm ) ta
               |
               |full outer join
               |
               |(
               |select
               |c.mchnt_cn_nm,
               |count(distinct(c.cdhd_usr_id)) as usr_cnt
               |from
               |(
               |select distinct b.mchnt_cn_nm, a.cdhd_usr_id
               |from
               |(select distinct(cdhd_usr_id),rec_crt_ts,mchnt_cd
               |from hive_passive_code_pay_trans
               |where to_date(rec_crt_ts)='$today_dt'
               |and tran_certi like '10%'
               |and trans_tp='01') a
               |inner join hive_mchnt_inf_wallet b
               |on a.mchnt_cd=b.mchnt_cd) c
               |group by c.mchnt_cn_nm
               |) tb
               |on ta.mchnt_cn_nm=tb.mchnt_cn_nm
               |
               |full outer join
               |
               |(select c.mchnt_cn_nm,
               |sum(c.cnt) as count
               |from
               |(select distinct b.mchnt_cn_nm, a.cdhd_usr_id,a.cnt
               |from
               |(select cdhd_usr_id,rec_crt_ts,mchnt_cd,count(*) as cnt
               |from hive_passive_code_pay_trans
               |where to_date(rec_crt_ts)='$today_dt'
               |and tran_certi like '10%'
               |and trans_tp='01' and trans_st in ('03','04','05','06','08','09','10','11')
               |group by cdhd_usr_id,rec_crt_ts,mchnt_cd) a
               |inner join hive_mchnt_inf_wallet b
               |on a.mchnt_cd=b.mchnt_cd) c
               |group by c.mchnt_cn_nm ) tc
               |on ta.mchnt_cn_nm=tc.mchnt_cn_nm
               |
               |full outer join
               |
               |(
               |select c.mchnt_cn_nm,
               |count(distinct(c.cdhd_usr_id)) as usr_cnt
               |from
               |(select distinct b.mchnt_cn_nm, a.cdhd_usr_id
               |from
               |(select distinct(cdhd_usr_id),rec_crt_ts ,mchnt_cd
               |from hive_passive_code_pay_trans
               |where to_date(rec_crt_ts)='$today_dt'
               |and tran_certi like '10%'
               |and trans_tp='01' and trans_st in ('03','04','05','06','08','09','10','11')) a
               |inner join hive_mchnt_inf_wallet b
               |on a.mchnt_cd=b.mchnt_cd)c
               |group by c.mchnt_cn_nm ) td
               |on ta.mchnt_cn_nm=td.mchnt_cn_nm
               |
               |full outer join
               |
               |(select
               |c.mchnt_cn_nm,
               |sum(c.cnt) as count,
               |sum(c.trans_at) as amt
               |from
               |(select distinct b.mchnt_cn_nm, a.cdhd_usr_id,a.cnt,a.trans_at
               |from
               |(select cdhd_usr_id,rec_crt_ts,mchnt_cd,sum(trans_at) as trans_at ,count(*) as cnt
               |from hive_passive_code_pay_trans
               |where to_date(rec_crt_ts)='$today_dt'
               |and tran_certi like '10%'
               |and trans_tp='01' and trans_st in ('04','10')
               |group by cdhd_usr_id,rec_crt_ts,mchnt_cd) a
               |inner join hive_mchnt_inf_wallet b
               |on a.mchnt_cd=b.mchnt_cd) c
               |group by c.mchnt_cn_nm ) te
               |on ta.mchnt_cn_nm=te.mchnt_cn_nm
               |
               |full outer join
               |
               |(select c.mchnt_cn_nm,
               |count(distinct(c.cdhd_usr_id)) as usr_cnt
               |from
               |(select distinct b.mchnt_cn_nm, a.cdhd_usr_id
               |from
               |(select distinct (cdhd_usr_id),rec_crt_ts ,mchnt_cd
               |from hive_passive_code_pay_trans
               |where to_date(rec_crt_ts)='$today_dt'
               |and tran_certi like '10%'
               |and trans_tp='01' and trans_st in ('04','10')) a
               |inner join hive_mchnt_inf_wallet b
               |on a.mchnt_cd=b.mchnt_cd)c
               |group by c.mchnt_cn_nm ) tf
               |on ta.mchnt_cn_nm=tf.mchnt_cn_nm
               |group by nvl(ta.mchnt_cn_nm,nvl(tb.mchnt_cn_nm,nvl(tc.mchnt_cn_nm,nvl(td.mchnt_cn_nm,
               |nvl(te.mchnt_cn_nm,tf.mchnt_cn_nm)))))
               |order by sweep_num
               |desc limit 20
               |) final
               |group by final.mchnt_cn_nm
               | """.stripMargin)
          println(s"#### JOB_DM_22 spark sql 清洗[$today_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())

          if (!Option(results).isEmpty) {
            results.save2Mysql("dm_swept_merchant")
            println(s"#### JOB_DM_22 [$today_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime()
            )
          } else {
            println(s"#### JOB_DM_22 spark sql 清洗[$today_dt]数据无结果集！")
          }
          today_dt = DateUtils.addOneDay(today_dt)
        }
      }
    }
  }


  /**
    * JOB_DM_23 20170307
    * dm_swept_resp_code->hive_passive_code_pay_trans
    * Code by Xue
    *
    * @param sqlContext
    * @return
    */
  def JOB_DM_23(implicit sqlContext: HiveContext, start_dt: String, end_dt: String, interval: Int) = {
    println("###JOB_DM_23(dm_swept_resp_code->hive_passive_code_pay_trans)")
    DateUtils.timeCost("JOB_DM_23") {
      UPSQL_JDBC.delete(s"dm_swept_resp_code", "report_dt", start_dt, end_dt)
      println("#### JOB_DM_23 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      var today_dt = start_dt
      if (interval >= 0) {
        println(s"#### JOB_DM_23  spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())
        sqlContext.sql(s"use $hive_dbname")
        for (i <- 0 to interval) {
          val results = sqlContext.sql(
            s"""
               |select
               |ta.resp_code as resp_code,
               |'$today_dt' as report_dt,
               |count(distinct ta.trans_seq) as sweep_usr_num,
               |sum(ta.trans_at) as sweep_num
               |from hive_passive_code_pay_trans ta
               |where to_date(ta.rec_crt_ts)='$today_dt'
               |and ta.tran_certi like '10%'
               |and length(trim(ta.resp_code))=2
               |group by ta.resp_code
               | """.stripMargin)
          println(s"#### JOB_DM_23 spark sql 清洗[$today_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())

          if (!Option(results).isEmpty) {
            results.save2Mysql("dm_swept_resp_code")
            println(s"#### JOB_DM_23 [$today_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime()
            )
          } else {
            println(s"#### JOB_DM_23 spark sql 清洗[$today_dt]数据无结果集！")
          }
          today_dt = DateUtils.addOneDay(today_dt)
        }
      }
    }
  }

  /**
    * JobName: JOB_DM_24
    * Feature: DM_UNIONPAY_RED_DOMAIN_BRANCH
    *
    * @author tzq
    * @time 2016-12-14
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    * @param interval
    */
  def JOB_DM_24 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("###JOB_DM_24(DM_UNIONPAY_RED_DOMAIN_BRANCH)### "+DateUtils.getCurrentSystemTime())
    DateUtils.timeCost("JOB_DM_24"){
      UPSQL_JDBC.delete(s"DM_UNIONPAY_RED_DOMAIN_BRANCH","REPORT_DT",start_dt,end_dt)
      println( "#### JOB_DM_24 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      var today_dt=start_dt
      if(interval>=0 ){
        sqlContext.sql(s"use $hive_dbname")
        for(i <- 0 to interval){
          println(s"#### JOB_DM_24 spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())
          val results =sqlContext.sql(
            s"""
               |select
               |mchnt.cup_branch_ins_id_nm    as branch_nm,
               |'$today_dt'                   as report_dt,
               |sum(mchnt_stat.cnt)           as tran_num,
               |sum(mchnt_stat.succnt)        as succ_tran_num,
               |sum(mchnt_stat.trans_at_all)  as tran_amt,
               |sum(mchnt_stat.pt_at_all)     as point_amt,
               |sum(mchnt_stat.usr_cnt)       as tran_usr_num,
               |sum(mchnt_stat.card_cnt)      as tran_card_num
               |from (select all.card_accptr_cd ,all.cnt,succ.succnt,succ.pt_at_all,succ.usr_cnt,succ.card_cnt,succ.trans_at_all from
               |(select
               |card_accptr_cd ,
               |count(*) as cnt
               |from hive_acc_trans
               |where fwd_ins_id_cd not in ('00000049998','00000050000') and buss_tp='02' and um_trans_id='AC02000065'
               |and to_date(rec_crt_ts)='$today_dt'
               |group by card_accptr_cd) as all
               |left join
               |(select
               |card_accptr_cd,
               |count(*) as succnt,
               |sum(point_at) as pt_at_all ,
               |count(distinct cdhd_usr_id) as usr_cnt,
               |count(distinct card_no) as card_cnt,
               |sum(case when trans_at is null or trans_at ='' then 0 else trans_at end) as trans_at_all
               |from hive_acc_trans
               |where fwd_ins_id_cd not in ('00000049998','00000050000') and buss_tp='02' and um_trans_id='AC02000065' and sys_det_cd='S'
               |and to_date(rec_crt_ts)='$today_dt'
               |group by card_accptr_cd) as succ
               |on all.card_accptr_cd=succ.card_accptr_cd ) as mchnt_stat, hive_mchnt_inf_wallet as mchnt
               |where mchnt_stat.card_accptr_cd=mchnt.mchnt_cd
               |group by mchnt.cup_branch_ins_id_nm
               |
          """.stripMargin)
          println(s"#### JOB_DM_24 spark sql 清洗[$today_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())

          println(s"###JOB_DM_24------$today_dt results:"+results.count())
          if(!Option(results).isEmpty){
            results.save2Mysql("DM_UNIONPAY_RED_DOMAIN_BRANCH")
            println(s"#### JOB_DM_24 [$today_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
          }else{
            println(s"#### JOB_DM_24 spark sql 清洗[$today_dt]数据无结果集！")
          }
          today_dt=DateUtils.addOneDay(today_dt)
        }
      }
    }
  }

  /**
    * JobName: JOB_DM_25
    * Feature: DM_UNIONPAY_RED_PHONE_AREA
    *
    * @author tzq
    * @time 2016-12-14
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    * @param interval
    */
  def JOB_DM_25 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("###JOB_DM_25(DM_UNIONPAY_RED_PHONE_AREA)### "+DateUtils.getCurrentSystemTime())
    DateUtils.timeCost("JOB_DM_25"){
      UPSQL_JDBC.delete(s"dm_unionpay_red_phone_area","report_dt",start_dt,end_dt)
      println( "#### JOB_DM_25 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      var today_dt=start_dt
      if(interval>=0 ){
        sqlContext.sql(s"use $hive_dbname")
        for(i <- 0 to interval){
          println(s"#### JOB_DM_25 spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())
          val results =sqlContext.sql(
            s"""
               |select phone_location as phone_branch_nm,
               |       '$today_dt' as report_dt,
               |       sum(all.cnt) as tran_num,
               |       sum(succ.succnt) as succ_tran_num,
               |       sum(succ.trans_at_all) as tran_amt,
               |       sum(succ.pt_at_all) as point_amt,
               |       sum(succ.usr_cnt) as tran_usr_num,
               |       sum(succ.card_cnt) as tran_card_num
               |from
               |  (select card_accptr_cd,
               |          phone_location,
               |          count(*) as cnt
               |   from hive_acc_trans a,
               |        hive_pri_acct_inf b
               |   where a.cdhd_usr_id=b.cdhd_usr_id
               |     and fwd_ins_id_cd not in ('00000049998',
               |                               '00000050000')
               |     and buss_tp='02'
               |     and um_trans_id='AC02000065'
               |     and to_date(a.rec_crt_ts)='$today_dt'
               |   group by card_accptr_cd,
               |            phone_location) as all
               |left join
               |  (select card_accptr_cd,
               |          count(*) as succnt,
               |          sum(point_at) as pt_at_all,
               |          count(distinct cdhd_usr_id) as usr_cnt,
               |          count(distinct card_no) as card_cnt,
               |          sum(case
               |                  when trans_at is null
               |                       or trans_at ='' then 0
               |                  else trans_at
               |              end) as trans_at_all
               |   from hive_acc_trans
               |   where fwd_ins_id_cd not in ('00000049998','00000050000')
               |     and buss_tp='02'
               |     and um_trans_id='AC02000065'
               |     and sys_det_cd='s'
               |     and to_date(rec_crt_ts)='$today_dt'
               |   group by card_accptr_cd) as succ on all.card_accptr_cd=succ.card_accptr_cd
               |group by phone_location
               |
          """.stripMargin)
          println(s"#### JOB_DM_25 spark sql 清洗[$today_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())

          println(s"###JOB_DM_25------$today_dt results:"+results.count())
          if(!Option(results).isEmpty){
            results.save2Mysql("DM_UNIONPAY_RED_PHONE_AREA")
            println(s"#### JOB_DM_25 [$today_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
          }else{
            println(s"#### JOB_DM_25 spark sql 清洗[$today_dt]数据无结果集！")
          }
          today_dt=DateUtils.addOneDay(today_dt)
        }
      }
    }
  }

  /**
    * JobName: JOB_DM_26
    * Feature: DM_UNIONPAY_RED_DIRECT_CONTACT_TRAN
    *
    * @author tzq
    * @time 2016-12-15
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    * @param interval
    */
  def JOB_DM_26 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("###JOB_DM_26(DM_UNIONPAY_RED_DIRECT_CONTACT_TRAN )### "+DateUtils.getCurrentSystemTime())
    DateUtils.timeCost("JOB_DM_26"){
      UPSQL_JDBC.delete(s"DM_UNIONPAY_RED_DIRECT_CONTACT_TRAN ","REPORT_DT",start_dt,end_dt)
      println( "#### JOB_DM_26 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      var today_dt=start_dt
      if(interval>=0 ){
        sqlContext.sql(s"use $hive_dbname")
        for(i <- 0 to interval){
          println(s"#### JOB_DM_26 spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())
          val results =sqlContext.sql(
            s"""
               |select project_name    as  project_name,
               |       '$today_dt'     as  report_dt,
               |       allcnt          as  tran_num,
               |       succcnt         as  succ_tran_num,
               |       sum_trans_at    as  tran_amt,
               |       sum_pt_at       as  point_amt,
               |       usr_cnt         as  tran_usr_num,
               |       card_cnt        as  tran_card_num
               |from
               |  (select case
               |              when all.fwd_ins_id_cd in (
               |              '00097310','00093600','00095210','00098700','00098500','00097700',
               |              '00096400','00096500','00155800','00095840','00097000','00085500','00096900','00093930',
               |              '00094200','00093900','00096100','00092210','00092220','00092900','00091600','00092400',
               |              '00098800','00098200','00097900','00091900','00092600','00091200','00093320','00031000',
               |              '00094500','00094900','00091100','00094520','00093000','00093310'
               |              ) then '直联' else '间联'
               |          end as project_name,
               |          sum(all.cnt) as allcnt,
               |          sum(succ.succnt) as succcnt,
               |          sum(succ.trans_at_all) as sum_trans_at,
               |          sum(succ.pt_at_all) as sum_pt_at,
               |          sum(succ.usr_cnt) as usr_cnt,
               |          sum(succ.card_cnt) as card_cnt from
               |     (select card_accptr_cd,fwd_ins_id_cd,phone_location,count(*) as cnt
               |      from hive_acc_trans a, hive_pri_acct_inf b
               |      where a.cdhd_usr_id=b.cdhd_usr_id
               |        and fwd_ins_id_cd not in ('00000049998','00000050000')
               |        and buss_tp='02'
               |        and um_trans_id='AC02000065'
               |        and to_date(a.rec_crt_ts)='$today_dt'
               |      group by card_accptr_cd,fwd_ins_id_cd,phone_location) as all
               |   left join
               |     (select card_accptr_cd,
               |             fwd_ins_id_cd,
               |             count(*) as succnt,
               |             sum(point_at) as pt_at_all,
               |             count(distinct cdhd_usr_id) as usr_cnt,
               |             count(distinct card_no) as card_cnt,
               |             sum(case
               |                     when trans_at is null
               |                          or trans_at ='' then 0
               |                     else trans_at
               |                 end) as trans_at_all
               |      from hive_acc_trans
               |      where fwd_ins_id_cd not in ('00000049998',
               |                                  '00000050000')
               |        and buss_tp='02'
               |        and um_trans_id='AC02000065'
               |        and sys_det_cd='S'
               |        and to_date(rec_crt_ts)='$today_dt'
               |      group by card_accptr_cd,
               |               fwd_ins_id_cd) as succ on all.card_accptr_cd=succ.card_accptr_cd
               |   group by case
               |                when all.fwd_ins_id_cd in (
               |                  '00097310','00093600','00095210','00098700','00098500','00097700',
               |                  '00096400','00096500','00155800','00095840','00097000','00085500','00096900','00093930',
               |                  '00094200','00093900','00096100','00092210','00092220','00092900','00091600','00092400',
               |                  '00098800','00098200','00097900','00091900','00092600','00091200','00093320','00031000',
               |                  '00094500','00094900','00091100','00094520','00093000','00093310'
               |                ) then '直联' else '间联' end
               | union all
               | select             case
               |                        when all.internal_trans_tp='C00023' then '终端不改造'
               |                        else '终端改造'
               |                    end as project_name,
               |                    sum(all.cnt) as allcnt,
               |                    sum(succ.succnt) as succcnt,
               |                    sum(succ.trans_at_all) as sum_trans_at,
               |                    sum(succ.pt_at_all) as sum_pt_at,
               |                    sum(succ.usr_cnt) as usr_cnt,
               |                    sum(succ.card_cnt) as card_cnt from
               |     (select card_accptr_cd,internal_trans_tp,phone_location,count(*) as cnt
               |      from hive_acc_trans a, hive_pri_acct_inf b
               |      where a.cdhd_usr_id=b.cdhd_usr_id
               |        and fwd_ins_id_cd not in ('00000049998','00000050000')
               |        and buss_tp='02'
               |        and um_trans_id='AC02000065'
               |        and to_date(a.rec_crt_ts)='$today_dt'
               |      group by card_accptr_cd,internal_trans_tp,phone_location) as all
               |   left join
               |     (select card_accptr_cd,
               |             internal_trans_tp,
               |             count(*) as succnt,
               |             sum(point_at) as pt_at_all,
               |             count(distinct cdhd_usr_id) as usr_cnt,
               |             count(distinct card_no) as card_cnt,
               |             sum(case
               |                     when trans_at is null
               |                          or trans_at ='' then 0
               |                     else trans_at
               |                 end) as trans_at_all
               |      from hive_acc_trans
               |      where fwd_ins_id_cd not in ('00000049998',
               |                                  '00000050000')
               |        and buss_tp='02'
               |        and um_trans_id='AC02000065'
               |        and sys_det_cd='S'
               |        and to_date(rec_crt_ts)='$today_dt'
               |      group by card_accptr_cd,
               |               internal_trans_tp) as succ on all.card_accptr_cd=succ.card_accptr_cd
               |   group by case
               |                when all.internal_trans_tp='C00023' then '终端不改造'
               |                else '终端改造' end
               |   union all select case
               |                        when all.internal_trans_tp='c00022' then '交易规范 1.0 规范'
               |                        else '交易规范 2.0 规范'
               |                    end as project_name,
               |                    sum(all.cnt) as allcnt,
               |                    sum(succ.succnt) as succcnt,
               |                    sum(succ.trans_at_all) as sum_trans_at,
               |                    sum(succ.pt_at_all) as sum_pt_at,
               |                    sum(succ.usr_cnt) as usr_cnt,
               |                    sum(succ.card_cnt) as card_cnt from
               |     (select card_accptr_cd,internal_trans_tp,phone_location,count(*) as cnt
               |      from hive_acc_trans a, hive_pri_acct_inf b
               |      where a.cdhd_usr_id=b.cdhd_usr_id
               |        and fwd_ins_id_cd not in ('00000049998','00000050000')
               |        and buss_tp='02'
               |        and um_trans_id='AC02000065'
               |        and internal_trans_tp in ('C00022','C20022')
               |        AND to_date(a.rec_crt_ts)='$today_dt'
               |      group by card_accptr_cd,internal_trans_tp,phone_location) as all
               |   left join
               |     (select card_accptr_cd,
               |             internal_trans_tp,
               |             count(*) as succnt,
               |             sum(point_at) as pt_at_all,
               |             count(distinct cdhd_usr_id) as usr_cnt,
               |             count(distinct card_no) as card_cnt,
               |             sum(case
               |                     when trans_at is null
               |                          or trans_at ='' then 0
               |                     else trans_at
               |                 end) as trans_at_all
               |      from hive_acc_trans
               |      where fwd_ins_id_cd not in ('00000049998','00000050000')
               |        and buss_tp='02'
               |        and um_trans_id='AC02000065'
               |        and internal_trans_tp in ('C00022', 'C20022')
               |        and sys_det_cd='S'
               |        and to_date(rec_crt_ts)='$today_dt'
               |      group by card_accptr_cd,
               |               internal_trans_tp) as succ on all.card_accptr_cd=succ.card_accptr_cd
               |   group by case
               |                when all.internal_trans_tp='c00022' then '交易规范 1.0 规范'
               |                else '交易规范 2.0 规范'
               |            end) tmp
               |
          """.stripMargin)
          println(s"#### JOB_DM_26 spark sql 清洗[$today_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())

          println(s"###JOB_DM_26------$today_dt results:"+results.count())
          if(!Option(results).isEmpty){
            results.save2Mysql("DM_UNIONPAY_RED_DIRECT_CONTACT_TRAN ")
            println(s"#### JOB_DM_26 [$today_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
          }else{
            println(s"#### JOB_DM_26 spark sql 清洗[$today_dt]数据无结果集！")
          }
          today_dt=DateUtils.addOneDay(today_dt)
        }
      }
    }
  }

  /**
    * JobName: JOB_DM_27
    * Feature: DM_UNIONPAY_RED_MCC
    *
    * @author tzq
    * @time 2016-12-15
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    * @param interval
    */
  def JOB_DM_27 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("###JOB_DM_27(DM_UNIONPAY_RED_MCC )### "+DateUtils.getCurrentSystemTime())
    DateUtils.timeCost("JOB_DM_27"){
      UPSQL_JDBC.delete(s"DM_UNIONPAY_RED_MCC ","REPORT_DT",start_dt,end_dt)
      println( "#### JOB_DM_27 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      var today_dt=start_dt
      if(interval>=0 ){
        sqlContext.sql(s"use $hive_dbname")
        for(i <- 0 to interval){
          println(s"#### JOB_DM_27 spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())
          val results =sqlContext.sql(
            s"""
               |select mchnt.grp_nm                 as store_frist_nm,
               |       mchnt.tp_nm                  as store_second_nm,
               |       '$today_dt'                 as report_dt,
               |       sum(mchnt_stat.cnt)          as tran_num,
               |       sum(mchnt_stat.succnt)       as succ_tran_num,
               |       sum(mchnt_stat.trans_at_all) as tran_amt,
               |       sum(mchnt_stat.pt_at_all)    as point_amt,
               |       sum(mchnt_stat.usr_cnt)      as tran_usr_num,
               |       sum(mchnt_stat.card_cnt)     as tran_card_num
               |from
               |  (select all.card_accptr_cd,
               |          all.mchnt_tp,
               |          all.cnt,
               |          succ.succnt,
               |          succ.pt_at_all,
               |          succ.trans_at_all,
               |          succ.usr_cnt,
               |          succ.card_cnt
               |   from
               |     (select card_accptr_cd,
               |             mchnt_tp,
               |             count(*) as cnt
               |      from hive_acc_trans
               |      where fwd_ins_id_cd not in ('00000049998',
               |                                  '00000050000')
               |        and buss_tp='02'
               |        and um_trans_id='AC02000065'
               |        and to_date(rec_crt_ts)='$today_dt'
               |      group by card_accptr_cd,mchnt_tp) as all
               |   left join
               |     (select card_accptr_cd,
               |             mchnt_tp,
               |             count(*) as succnt,
               |             sum(point_at) as pt_at_all,
               |             count(distinct cdhd_usr_id) as usr_cnt,
               |             count(distinct card_no) as card_cnt,
               |             sum(case
               |                     when trans_at is null
               |                          or trans_at ='' then 0
               |                     else trans_at
               |                 end) as trans_at_all
               |      from hive_acc_trans
               |      where fwd_ins_id_cd not in ('00000049998',
               |                                  '00000050000')
               |        and buss_tp='02'
               |        and um_trans_id='AC02000065'
               |        and sys_det_cd='S'
               |        and to_date(rec_crt_ts)='$today_dt'
               |      group by card_accptr_cd,mchnt_tp) as succ on all.card_accptr_cd=succ.card_accptr_cd
               |   and all.mchnt_tp=succ.mchnt_tp) as mchnt_stat,
               |
               |  (select tp_grp.mchnt_tp_grp_desc_cn as grp_nm,
               |          tp.mchnt_tp_desc_cn as tp_nm,
               |          tp.mchnt_tp
               |   from hive_mchnt_tp tp
               |   left join hive_mchnt_tp_grp tp_grp on tp.mchnt_tp_grp=tp_grp.mchnt_tp_grp) as mchnt
               |where mchnt.mchnt_tp=mchnt_stat.mchnt_tp
               |group by mchnt.grp_nm,mchnt.tp_nm
          """.stripMargin)
          println(s"#### JOB_DM_27 spark sql 清洗[$today_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())

          println(s"###JOB_DM_27------$today_dt results:"+results.count())
          if(!Option(results).isEmpty){
            results.save2Mysql("DM_UNIONPAY_RED_MCC ")
            println(s"#### JOB_DM_27 [$today_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
          }else{
            println(s"#### JOB_DM_27 spark sql 清洗[$today_dt]数据无结果集！")
          }
          today_dt=DateUtils.addOneDay(today_dt)
        }
      }
    }
  }


  /**
    * JobName: JOB_DM_28
    * Feature: DM_UNIONPAY_RED_ISS
    *
    * @author tzq
    * @time 2017-1-3
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    * @param interval
    */
  def JOB_DM_28 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("###JOB_DM_28(DM_UNIONPAY_RED_ISS )### "+DateUtils.getCurrentSystemTime())
    DateUtils.timeCost("JOB_DM_28"){
      UPSQL_JDBC.delete(s"DM_UNIONPAY_RED_ISS ","REPORT_DT",start_dt,end_dt)
      println( "#### JOB_DM_28 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      var today_dt=start_dt
      if(interval>=0 ){
        sqlContext.sql(s"use $hive_dbname")
        for(i <- 0 to interval){
          println(s"#### JOB_DM_28 spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())
          val results =sqlContext.sql(
            s"""
               |select card_bind.iss_ins_cn_nm   as  iss_nm,
               |       '$today_dt'    as  report_dt,
               |       sum(mchnt_stat.cnt)   as  tran_num,
               |       sum(mchnt_stat.succnt)    as  succ_tran_num,
               |       sum(mchnt_stat.trans_at_all)    as  tran_amt,
               |       sum(mchnt_stat.pt_at_all)    as  point_amt,
               |       count(distinct mchnt_stat.cdhd_usr_id)    as  tran_usr_num,
               |       count(distinct mchnt_stat.card_no)    as  tran_card_num
               |from
               |  (select all.card_accptr_cd,
               |          all.cnt,
               |          succ.cdhd_usr_id,
               |          succ.card_no,
               |          succ.succnt,
               |          succ.pt_at_all,
               |          succ.trans_at_all
               |   from
               |     (select card_accptr_cd,
               |             cdhd_usr_id,
               |             card_no,
               |             count(*) as cnt
               |      from hive_acc_trans
               |      where fwd_ins_id_cd not in ('00000049998','00000050000')
               |        and buss_tp='02'
               |        and um_trans_id='AC02000065'
               |        and to_date(rec_crt_ts)='$today_dt'
               |      group by card_accptr_cd,
               |               cdhd_usr_id,
               |               card_no) as all
               |   left join
               |     (select card_accptr_cd,
               |             cdhd_usr_id,
               |             card_no,
               |             count(*) as succnt,
               |             sum(point_at) as pt_at_all,
               |             sum(case
               |                     when trans_at is null
               |                          or trans_at ='' then 0
               |                     else trans_at
               |                 end) as trans_at_all
               |      from hive_acc_trans
               |      where fwd_ins_id_cd not in ('00000049998','00000050000')
               |        and buss_tp='02'
               |        and um_trans_id='AC02000065'
               |        and sys_det_cd='S'
               |        and to_date(rec_crt_ts)='$today_dt'
               |      group by card_accptr_cd,
               |               cdhd_usr_id,
               |               card_no) as succ on all.card_accptr_cd=succ.card_accptr_cd
               |   and all.card_no=succ.card_no) as mchnt_stat,
               |     hive_card_bind_inf as card_bind
               |where mchnt_stat.card_no=card_bind.bind_card_no
               |group by card_bind.iss_ins_cn_nm
          """.stripMargin)
          println(s"#### JOB_DM_28 spark sql 清洗[$today_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())

          println(s"###JOB_DM_28------$today_dt results:"+results.count())
          if(!Option(results).isEmpty){
            results.save2Mysql("DM_UNIONPAY_RED_ISS ")
            println(s"#### JOB_DM_28 [$today_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
          }else{
            println(s"#### JOB_DM_28 spark sql 清洗[$today_dt]数据无结果集！")
          }
          today_dt=DateUtils.addOneDay(today_dt)
        }
      }
    }
  }


  /**
    * JobName: JOB_DM_29
    * Feature: DM_UNIONPAY_RED_GIVE_BRANCH
    *
    * @author tzq
    * @time 2017-1-3
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    * @param interval
    */
  def JOB_DM_29 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("###JOB_DM_29(DM_UNIONPAY_RED_GIVE_BRANCH )### "+DateUtils.getCurrentSystemTime())
    DateUtils.timeCost("JOB_DM_29"){
      UPSQL_JDBC.delete(s"DM_UNIONPAY_RED_GIVE_BRANCH","REPORT_DT",start_dt,end_dt)
      println( "#### JOB_DM_29 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      var today_dt=start_dt
      if(interval>=0 ){
        sqlContext.sql(s"use $hive_dbname")
        for(i <- 0 to interval){
          println(s"#### JOB_DM_29 spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())
          val results =sqlContext.sql(
            s"""
               |select cup_branch_ins_id_nm as branch_nm,
               |       '$today_dt'  as point_tp,
               |       pt_tp as report_dt,
               |       sum(transcnt) as tran_num,
               |       sum(ptsum) as point_amt,
               |       sum(usrsum) as tran_usr_num
               |from
               |  (select cup_branch_ins_id_nm,
               |          '1' as pt_tp,
               |          count(*) as transcnt,
               |          sum(point_at) as ptsum,
               |          count(distinct cdhd_usr_id) as usrsum
               |   from hive_offline_point_trans
               |   where to_date(acct_addup_bat_dt)='$today_dt'
               |     and buss_tp='02'
               |     and oper_st in('0',
               |                    '3')
               |     and um_trans_id in('AD00000002',
               |                        'AD00000003',
               |                        'AD00000007')
               |   group by cup_branch_ins_id_nm
               |   union all select cup_branch_ins_id_nm,
               |                    '0' as pt_tp,
               |                    count(*) as transcnt,
               |                    sum(trans_point_at) as ptsum,
               |                    count(distinct cdhd_usr_id) as usrsum
               |   from hive_online_point_trans
               |   where trans_tp ='18'
               |     and buss_tp='02'
               |     and to_date(trans_dt)='$today_dt'
               |   group by cup_branch_ins_id_nm) t1
               |group by cup_branch_ins_id_nm,pt_tp
               |
          """.stripMargin)
          println(s"#### JOB_DM_29 spark sql 清洗[$today_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())

          println(s"###JOB_DM_29------$today_dt results:"+results.count())
          if(!Option(results).isEmpty){
            results.save2Mysql("DM_UNIONPAY_RED_GIVE_BRANCH")
            println(s"#### JOB_DM_29 [$today_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
          }else{
            println(s"#### JOB_DM_29 spark sql 清洗[$today_dt]数据无结果集！")
          }
          today_dt=DateUtils.addOneDay(today_dt)
        }
      }
    }
  }


  /**
    * JobName: JOB_DM_30
    * Feature: DM_DISC_TKT_ACT_BRANCH_DLY
    *
    * @author tzq
    * @time 2016-12-15
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    */
  def JOB_DM_30 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {
    DateUtils.timeCost("JOB_DM_30"){
      UPSQL_JDBC.delete(s"DM_DISC_TKT_ACT_BRANCH_DLY","REPORT_DT",start_dt,end_dt)
      println( "#### JOB_DM_30 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())

      sqlContext.sql(s"use $hive_dbname")
      println(s"#### JOB_DM_30 spark sql 清洗数据开始时间为:" + DateUtils.getCurrentSystemTime())
      val results =sqlContext.sql(
        s"""
           |select
           |a.cup_branch_ins_id_nm as cup_branch_ins_id_nm,
           |a.trans_dt as report_dt ,
           |a.transcnt as trans_cnt ,
           |b.suctranscnt as suc_trans_cnt ,
           |b.transat as trans_at ,
           |b.discountat as discount_at ,
           |b.transusrcnt as trans_usr_cnt ,
           |b.transcardcnt as trans_card_cnt
           |from
           |(
           |select
           |trim(if(bill.cup_branch_ins_id_nm is null,'总公司',bill.cup_branch_ins_id_nm)) as cup_branch_ins_id_nm,
           |to_date(trans.trans_dt) as trans_dt,
           |count(1) as transcnt
           |from
           |hive_acc_trans trans
           |left join
           |hive_ticket_bill_bas_inf bill
           |on
           |(
           |trans.bill_id=bill.bill_id)
           |where
           |trans.um_trans_id in ('AC02000065','AC02000063')
           |and bill.bill_sub_tp in ('01', '03')
           |and trans.part_trans_dt >= '$start_dt'
           |and trans.part_trans_dt <= '$end_dt'
           |group by
           |if(bill.cup_branch_ins_id_nm is null,'总公司',bill.cup_branch_ins_id_nm),
           |to_date(trans.trans_dt )) a
           |left join
           |(
           |select
           |trim(if(bill.cup_branch_ins_id_nm is null,'总公司',bill.cup_branch_ins_id_nm)) as cup_branch_ins_id_nm,
           |to_date(trans.trans_dt ) as trans_dt,
           |count(1) as suctranscnt,
           |sum(trans.trans_at) as transat,
           |sum(trans.discount_at) as discountat,
           |count(distinct trans.cdhd_usr_id) as transusrcnt,
           |count(distinct trans.pri_acct_no) as transcardcnt
           |from
           |hive_acc_trans trans
           |left join
           |hive_ticket_bill_bas_inf bill
           |on
           |(
           |trans.bill_id=bill.bill_id)
           |where
           |trans.sys_det_cd = 'S'
           |and trans.um_trans_id iN ('AC02000065','AC02000063')
           |and bill.bill_sub_tp in ('01','03')
           |and trans.part_trans_dt >= '$start_dt'
           |and trans.part_trans_dt <= '$end_dt'
           |
               |group by
           |if(bill.cup_branch_ins_id_nm is null,'总公司',bill.cup_branch_ins_id_nm),
           |to_date(trans.trans_dt ))b
           |on
           |a.cup_branch_ins_id_nm = b.cup_branch_ins_id_nm and a.trans_dt = b.trans_dt
          """.stripMargin)

      println(s"#### JOB_DM_30 spark sql 清洗数据完成时间为:" + DateUtils.getCurrentSystemTime())
      println(s"###JOB_DM_30----- results:"+results.count())
      if(!Option(results).isEmpty){
        println(s"#### JOB_DM_30 数据插入开始时间为：" + DateUtils.getCurrentSystemTime())
        results.save2Mysql("DM_DISC_TKT_ACT_BRANCH_DLY")
        println(s"#### JOB_DM_30 数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
      }else{
        println(s"#### JOB_DM_30 spark sql 清洗数据无结果集！")
      }
    }
  }

  /**
    * JobName: JOB_DM_31
    * Feature: DM_DISC_TKT_ACT_MCHNT_BRANCH_DLY
    *
    * @author tzq
    * @time 2016-12-15
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    */
  def JOB_DM_31 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {
    println("###JOB_DM_31(DM_DISC_TKT_ACT_MCHNT_BRANCH_DLY )### "+DateUtils.getCurrentSystemTime())
    DateUtils.timeCost("JOB_DM_31"){
      UPSQL_JDBC.delete(s"DM_DISC_TKT_ACT_MCHNT_BRANCH_DLY ","REPORT_DT",start_dt,end_dt)
      println( "#### JOB_DM_31 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())

      println(s"#### JOB_DM_31 spark sql 清洗数据开始时间为:" + DateUtils.getCurrentSystemTime())
      sqlContext.sql(s"use $hive_dbname")
      val results =sqlContext.sql(
        s"""
           |select
           |    a.cup_branch_ins_id_nm  as cup_branch_ins_id_nm,
           |    a.trans_dt              as report_dt,
           |    a.transcnt              as trans_cnt,
           |    b.suctranscnt           as suc_trans_cnt,
           |    b.transat               as trans_at,
           |    b.discountat            as discount_at,
           |    b.transusrcnt           as trans_usr_cnt,
           |    b.transcardcnt          as trans_card_cnt
           |from
           |    (
           |        select
           |        trim(if(acpt_ins.cup_branch_ins_id_nm is null,'总公司',acpt_ins.cup_branch_ins_id_nm)) as cup_branch_ins_id_nm,
           |            to_date(trans.trans_dt) as trans_dt,
           |            count(1) as transcnt
           |        from
           |            hive_acc_trans trans
           |        left join
           |            hive_mchnt_inf_wallet mchnt
           |        on
           |            (
           |                trans.mchnt_cd=mchnt.mchnt_cd)
           |        left join
           |            hive_branch_acpt_ins_inf acpt_ins
           |        on
           |            (
           |                acpt_ins.ins_id_cd=concat('000',mchnt.acpt_ins_id_cd))
           |        inner join
           |            hive_ticket_bill_bas_inf bill
           |        on
           |            trans.bill_id=bill.bill_id
           |        where
           |            trans.um_trans_id in ('AC02000065',
           |                                  'AC02000063')
           |        and bill.bill_sub_tp in ('01',
           |                                 '03')
           |        and trans.part_trans_dt >= '$start_dt'
           |        and trans.part_trans_dt <= '$end_dt'
           |        group by
           |        trim(if(acpt_ins.cup_branch_ins_id_nm is null,'总公司',acpt_ins.cup_branch_ins_id_nm)),
           |            to_date(trans.trans_dt)) a
           |left join
           |    (
           |        select
           |        trim(if(acpt_ins.cup_branch_ins_id_nm is null,'总公司',acpt_ins.cup_branch_ins_id_nm)) as cup_branch_ins_id_nm,
           |           to_date(trans.trans_dt) as trans_dt,
           |            count(1) as suctranscnt,
           |            sum(trans.trans_at) as transat,
           |            sum(trans.discount_at)          as discountat,
           |            count(distinct trans.cdhd_usr_id) as transusrcnt,
           |            count(distinct trans.pri_acct_no) as transcardcnt
           |        from
           |            hive_acc_trans trans
           |        left join
           |            hive_mchnt_inf_wallet mchnt
           |        on
           |            (
           |                trans.mchnt_cd=mchnt.mchnt_cd)
           |        left join
           |            hive_branch_acpt_ins_inf acpt_ins
           |        on
           |            (
           |                acpt_ins.ins_id_cd=concat('000',mchnt.acpt_ins_id_cd))
           |        inner join
           |            hive_ticket_bill_bas_inf bill
           |        on
           |            trans.bill_id=bill.bill_id
           |        where
           |            trans.sys_det_cd = 'S'
           |        and trans.um_trans_id in ('AC02000065',
           |                                  'AC02000063')
           |        and bill.bill_sub_tp in ('01',
           |                                 '03')
           |        and trans.part_trans_dt >= '$start_dt'
           |        and trans.part_trans_dt <= '$end_dt'
           |        group by
           |        trim(if(acpt_ins.cup_branch_ins_id_nm is null,'总公司',acpt_ins.cup_branch_ins_id_nm)),
           |            to_date(trans.trans_dt))b
           |on
           |a.cup_branch_ins_id_nm = b.cup_branch_ins_id_nm and a.trans_dt = b.trans_dt
          """.stripMargin)
      println(s"#### JOB_DM_31 spark sql 清洗数据完成时间为:" + DateUtils.getCurrentSystemTime())

      println(s"###JOB_DM_31------ results:"+results.count())
      if(!Option(results).isEmpty){
        results.save2Mysql("DM_DISC_TKT_ACT_MCHNT_BRANCH_DLY ")
        println(s"#### JOB_DM_31 数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
      }else{
        println(s"#### JOB_DM_31 spark sql 清洗后数据无结果集！")
      }
    }
  }


  /**
    * JOB_DM_32 20161215
    * dm_disc_tkt_act_mobile_loc_dly->hive_acc_trans
    * Code by Xue
    *
    * @param sqlContext
    * @return
    */
  def JOB_DM_32(implicit sqlContext: HiveContext, start_dt: String, end_dt: String, interval: Int) = {
    println("###JOB_DM_32(dm_disc_tkt_act_mobile_loc_dly->hive_acc_trans)")
    DateUtils.timeCost("JOB_DM_32") {
      UPSQL_JDBC.delete(s"dm_disc_tkt_act_mobile_loc_dly", "report_dt", start_dt, end_dt)
      println("#### JOB_DM_32 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      println(s"#### JOB_DM_32  spark sql 清洗数据开始时间为:" + DateUtils.getCurrentSystemTime())
      sqlContext.sql(s"use $hive_dbname")
      val results = sqlContext.sql(
        s"""
           |select
           |    a.phone_location as mobile_loc,
           |    a.trans_dt as report_dt,
           |    a.transcnt as trans_cnt,
           |    b.suctranscnt as suc_trans_cnt,
           |    b.transat as trans_at,
           |    b.discountat as discount_at,
           |    b.transusrcnt as trans_usr_cnt,
           |    b.transcardcnt as trans_card_cnt
           |from
           |    (
           |        select
           |if(pri_acct.phone_location is null,'总公司',pri_acct.phone_location) as phone_location,
           |            trans.trans_dt,
           |            count(*) as transcnt
           |        from
           |            hive_acc_trans trans
           |        left join
           |            hive_ticket_bill_bas_inf bill
           |        on
           |            (
           |                trans.bill_id=bill.bill_id)
           |        left join
           |            hive_pri_acct_inf pri_acct
           |        on
           |            (
           |                trans.cdhd_usr_id = pri_acct.cdhd_usr_id)
           |        where
           |            trans.um_trans_id in ('AC02000065','AC02000063')
           |        and bill.bill_sub_tp in ('01','03')
           |        and trans.part_trans_dt >= '$start_dt'
           |        and trans.part_trans_dt <= '$end_dt'
           |        group by
           |if(pri_acct.phone_location is null,'总公司',pri_acct.phone_location),
           |            trans.trans_dt) a
           |left join
           |    (
           |        select
           |if(pri_acct.phone_location is null,'总公司',pri_acct.phone_location) as phone_location,
           |            trans.trans_dt,
           |            count(*) as suctranscnt,
           |            sum(trans.trans_at) as transat,
           |            sum(trans.discount_at)          as discountat,
           |            count(distinct trans.cdhd_usr_id) as transusrcnt,
           |            count(distinct trans.pri_acct_no) as transcardcnt
           |        from
           |            hive_acc_trans trans
           |        left join
           |            hive_ticket_bill_bas_inf bill
           |        on
           |            (
           |                trans.bill_id=bill.bill_id)
           |        left join
           |            hive_pri_acct_inf pri_acct
           |        on
           |            (
           |                trans.cdhd_usr_id = pri_acct.cdhd_usr_id)
           |        where
           |            trans.sys_det_cd = 'S'
           |        and trans.um_trans_id in ('AC02000065','AC02000063')
           |        and bill.bill_sub_tp in ('01','03')
           |        and trans.part_trans_dt >= '$start_dt'
           |        and trans.part_trans_dt <= '$end_dt'
           |        group by
           |        if(pri_acct.phone_location is null,'总公司',pri_acct.phone_location),
           |        trans.trans_dt
           |		)b
           |on
           |    (
           |        a.phone_location = b.phone_location
           |    and a.trans_dt = b.trans_dt )
           |
           | """.stripMargin)
      println(s"#### JOB_DM_32 spark sql 清洗数据完成时间为:" + DateUtils.getCurrentSystemTime())

      if (!Option(results).isEmpty) {
        results.save2Mysql("dm_disc_tkt_act_mobile_loc_dly")
        println(s"#### JOB_DM_32 数据插入完成时间为：" + DateUtils.getCurrentSystemTime()
        )
      } else {
        println(s"#### JOB_DM_32 spark sql 清洗数据无结果集！")
      }
    }
  }


  /**
    * JOB_DM_33 20161216
    * dm_disc_tkt_act_link_tp_dly->hive_acc_trans
    * Code by Xue
    *
    * @param sqlContext
    * @return
    */
  def JOB_DM_33(implicit sqlContext: HiveContext, start_dt: String, end_dt: String, interval: Int) = {
    println("###JOB_DM_33(dm_disc_tkt_act_link_tp_dly->hive_acc_trans)")
    DateUtils.timeCost("JOB_DM_33") {
      UPSQL_JDBC.delete(s"dm_disc_tkt_act_link_tp_dly", "report_dt", start_dt, end_dt)
      println("#### JOB_DM_33 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      println(s"#### JOB_DM_33  spark sql 清洗数据开始时间为:" + DateUtils.getCurrentSystemTime())
      sqlContext.sql(s"use $hive_dbname")
      val results = sqlContext.sql(
        s"""
           |select
           |    a.ins_id_cd as link_tp_nm,
           |    a.sys_settle_dt as report_dt,
           |    a.transcnt as trans_cnt,
           |    b.suctranscnt as suc_trans_cnt,
           |    b.transat  as trans_at,
           |    b.discountat as discount_at,
           |    b.transusrcnt as trans_usr_cnt,
           |    b.cardcnt as trans_card_cnt
           |
           |from
           |    (
           |        select
           |            case
           |                when trans.fwd_ins_id_cd in ('00097310',
           |                                             '00093600',
           |                                             '00095210',
           |                                             '00098700',
           |                                             '00098500',
           |                                             '00097700',
           |                                             '00096400',
           |                                             '00096500',
           |                                             '00155800',
           |                                             '00095840',
           |                                             '00097000',
           |                                             '00085500',
           |                                             '00096900',
           |                                             '00093930',
           |                                             '00094200',
           |                                             '00093900',
           |                                             '00096100',
           |                                             '00092210',
           |                                             '00092220',
           |                                             '00092900',
           |                                             '00091600',
           |                                             '00092400',
           |                                             '00098800',
           |                                             '00098200',
           |                                             '00097900',
           |                                             '00091900',
           |                                             '00092600',
           |                                             '00091200',
           |                                             '00093320',
           |                                             '00031000',
           |                                             '00094500',
           |                                             '00094900',
           |                                             '00091100',
           |                                             '00094520',
           |                                             '00093000',
           |                                             '00093310')
           |                then '直连'
           |                else '间连'
           |            end as ins_id_cd,
           |            trans.sys_settle_dt,
           |            count(*) as transcnt
           |        from
           |            hive_acc_trans trans
           |        where
           |            trans.sys_settle_dt >= '$start_dt'
           |        and trans.sys_settle_dt <= '$end_dt'
           |        group by
           |            case
           |                when trans.fwd_ins_id_cd in ('00097310',
           |                                             '00093600',
           |                                             '00095210',
           |                                             '00098700',
           |                                             '00098500',
           |                                             '00097700',
           |                                             '00096400',
           |                                             '00096500',
           |                                             '00155800',
           |                                             '00095840',
           |                                             '00097000',
           |                                             '00085500',
           |                                             '00096900',
           |                                             '00093930',
           |                                             '00094200',
           |                                             '00093900',
           |                                             '00096100',
           |                                             '00092210',
           |                                             '00092220',
           |                                             '00092900',
           |                                             '00091600',
           |                                             '00092400',
           |                                             '00098800',
           |                                             '00098200',
           |                                             '00097900',
           |                                             '00091900',
           |                                             '00092600',
           |                                             '00091200',
           |                                             '00093320',
           |                                             '00031000',
           |                                             '00094500',
           |                                             '00094900',
           |                                             '00091100',
           |                                             '00094520',
           |                                             '00093000',
           |                                             '00093310')
           |                then '直连'
           |                else '间连'
           |            end,
           |            trans.sys_settle_dt) a
           |left join
           |    (
           |        select
           |            case
           |                when trans.fwd_ins_id_cd in ('00097310',
           |                                             '00093600',
           |                                             '00095210',
           |                                             '00098700',
           |                                             '00098500',
           |                                             '00097700',
           |                                             '00096400',
           |                                             '00096500',
           |                                             '00155800',
           |                                             '00095840',
           |                                             '00097000',
           |                                             '00085500',
           |                                             '00096900',
           |                                             '00093930',
           |                                             '00094200',
           |                                             '00093900',
           |                                             '00096100',
           |                                             '00092210',
           |                                             '00092220',
           |                                             '00092900',
           |                                             '00091600',
           |                                             '00092400',
           |                                             '00098800',
           |                                             '00098200',
           |                                             '00097900',
           |                                             '00091900',
           |                                             '00092600',
           |                                             '00091200',
           |                                             '00093320',
           |                                             '00031000',
           |                                             '00094500',
           |                                             '00094900',
           |                                             '00091100',
           |                                             '00094520',
           |                                             '00093000',
           |                                             '00093310')
           |                then '直连'
           |                else '间连'
           |            end as ins_id_cd,
           |            trans.sys_settle_dt,
           |            count(*)          as suctranscnt,
           |            sum(trans.trans_tot_at) as transat,
           |            sum(cast((
           |                    case trim(trans.discount_at)
           |                        when ''
           |                        then '000000000000'
           |                        else trim(trans.discount_at)
           |                    end) as bigint))          as discountat,
           |            count(distinct trans.cdhd_usr_id) as transusrcnt,
           |            count(distinct trans.pri_acct_no)       as cardcnt
           |        from
           |            hive_acc_trans trans
           |        where
           |            trans.chswt_resp_cd='00'
           |        and trans.sys_settle_dt >= '$start_dt'
           |        and trans.sys_settle_dt <= '$end_dt'
           |        group by
           |            case
           |                when trans.fwd_ins_id_cd in ('00097310',
           |                                             '00093600',
           |                                             '00095210',
           |                                             '00098700',
           |                                             '00098500',
           |                                             '00097700',
           |                                             '00096400',
           |                                             '00096500',
           |                                             '00155800',
           |                                             '00095840',
           |                                             '00097000',
           |                                             '00085500',
           |                                             '00096900',
           |                                             '00093930',
           |                                             '00094200',
           |                                             '00093900',
           |                                             '00096100',
           |                                             '00092210',
           |                                             '00092220',
           |                                             '00092900',
           |                                             '00091600',
           |                                             '00092400',
           |                                             '00098800',
           |                                             '00098200',
           |                                             '00097900',
           |                                             '00091900',
           |                                             '00092600',
           |                                             '00091200',
           |                                             '00093320',
           |                                             '00031000',
           |                                             '00094500',
           |                                             '00094900',
           |                                             '00091100',
           |                                             '00094520',
           |                                             '00093000',
           |                                             '00093310')
           |                then '直连'
           |                else '间连'
           |            end,
           |            trans.sys_settle_dt) b
           |on
           |    a.ins_id_cd=b.ins_id_cd
           |and a.sys_settle_dt = b.sys_settle_dt
           |order by
           |    a.transcnt desc
           |
               | """.stripMargin)
      println(s"#### JOB_DM_33 spark sql 清洗数据完成时间为:" + DateUtils.getCurrentSystemTime())

      if (!Option(results).isEmpty) {
        results.save2Mysql("dm_disc_tkt_act_link_tp_dly")
        println(s"#### JOB_DM_33 数据插入完成时间为：" + DateUtils.getCurrentSystemTime()
        )
      } else {
        println(s"#### JOB_DM_33 spark sql 清洗数据无结果集！")
      }
    }
  }




  /**
    * JobName: JOB_DM_34
    * Feature:DM_DISC_TKT_ACT_TRANS_NORM_DLY
    *
    * @author tzq
    * @time 2016-12-19
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    */
  def JOB_DM_34 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {
    println("###JOB_DM_34(DM_DISC_TKT_ACT_TRANS_NORM_DLY )### "+DateUtils.getCurrentSystemTime())
    DateUtils.timeCost("JOB_DM_34"){
      UPSQL_JDBC.delete(s"DM_DISC_TKT_ACT_TRANS_NORM_DLY ","REPORT_DT",start_dt,end_dt)
      println( "#### JOB_DM_34 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())

      println(s"#### JOB_DM_34 spark sql 清洗数据开始时间为:" + DateUtils.getCurrentSystemTime())
      sqlContext.sql(s"use $hive_dbname")
      val results =sqlContext.sql(
        s"""
           |select
           |    a.trans_norm_cd  as trans_norm_nm,
           |    a.sys_settle_dt  as report_dt,
           |    a.transcnt       as trans_cnt,
           |    b.suctranscnt    as suc_trans_cnt,
           |    b.transat        as trans_at,
           |    b.discountat     as discount_at,
           |    b.transusrcnt    as trans_usr_cnt,
           |    b.cardcnt        as trans_card_cnt
           |from
           |    (
           |        select
           |            case
           |                when trans.internal_trans_tp='c00022'
           |                then '1.0规范'
           |                else '2.0规范'
           |            end trans_norm_cd,
           |            trans.sys_settle_dt,
           |            count(*) as transcnt
           |        from
           |            hive_acc_trans trans
           |        left join
           |            hive_ins_inf ins
           |        on
           |            trans.acpt_ins_id_cd=ins.ins_id_cd
           |        where
           |            trans.internal_trans_tp in ('C00022' ,
           |                                        'C20022')
           |        and trans.sys_settle_dt >= '$start_dt'
           |        and trans.sys_settle_dt <= '$end_dt'
           |        group by
           |            case
           |                when trans.internal_trans_tp='c00022'
           |                then '1.0规范'
           |                else '2.0规范'
           |            end,
           |            trans.sys_settle_dt) a
           |left join
           |    (
           |        select
           |            case
           |                when trans.internal_trans_tp='c00022'
           |                then '1.0规范'
           |                else '2.0规范'
           |            end trans_norm_cd,
           |            trans.sys_settle_dt,
           |            count(*)                          as suctranscnt,
           |            sum(trans_tot_at)                 as transat,
           |            sum(trans.discount_at)            as discountat,
           |            count(distinct trans.cdhd_usr_id) as transusrcnt,
           |            count(distinct pri_acct_no)       as cardcnt
           |        from
           |            hive_acc_trans trans
           |        left join
           |            hive_ins_inf ins
           |        on
           |            trans.acpt_ins_id_cd=ins.ins_id_cd
           |        where
           |            trans.chswt_resp_cd='00'
           |        and trans.internal_trans_tp in ('C00022' , 'C20022')
           |        and trans.sys_settle_dt >= '$start_dt'
           |        and trans.sys_settle_dt <= '$end_dt'
           |        group by
           |            case
           |                when trans.internal_trans_tp='C00022'
           |                then '1.0规范'
           |                else '2.0规范'
           |            end,
           |            trans.sys_settle_dt) b
           |on
           |    a.trans_norm_cd=b.trans_norm_cd
           |and a.sys_settle_dt = b.sys_settle_dt
           |order by
           |    a.transcnt desc
          """.stripMargin)
      println(s"#### JOB_DM_34 spark sql 清洗数据完成时间为:" + DateUtils.getCurrentSystemTime())

      println(s"###JOB_DM_34------ results:"+results.count())
      if(!Option(results).isEmpty){
        results.save2Mysql("DM_DISC_TKT_ACT_TRANS_NORM_DLY")
        println(s"#### JOB_DM_34 数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
      }else{
        println(s"#### JOB_DM_34 spark sql 清洗后数据无结果集！")
      }
    }
  }

  /**
    * JobName: JOB_DM_35
    * Feature:DM_DISC_TKT_ACT_TERM_UPG_DLY
    *
    * @author tzq
    * @time 2016-12-20
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    */
  def JOB_DM_35 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {
    println("###JOB_DM_35(DM_DISC_TKT_ACT_TERM_UPG_DLY )### "+DateUtils.getCurrentSystemTime())
    DateUtils.timeCost("JOB_DM_35"){
      UPSQL_JDBC.delete(s"DM_DISC_TKT_ACT_TERM_UPG_DLY ","REPORT_DT",start_dt,end_dt)
      println( "#### JOB_DM_35 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())

      println(s"#### JOB_DM_35 spark sql 清洗数据开始时间为:" + DateUtils.getCurrentSystemTime())
      sqlContext.sql(s"use $hive_dbname")
      val results =sqlContext.sql(
        s"""
           |select
           |    a.term_upgrade_cd  as term_upg_nm,
           |    a.sys_settle_dt    as report_dt,
           |    a.transcnt         as trans_cnt,
           |    b.suctranscnt      as suc_trans_cnt,
           |    b.transat          as trans_at,
           |    b.discountat       as discount_at,
           |    b.transusrcnt      as trans_usr_cnt,
           |    b.cardcnt          as trans_card_cnt
           |from
           |    (
           |        select
           |            case
           |                when trans.internal_trans_tp='C00022'
           |                or  trans.internal_trans_tp='C20022'
           |                then '终端改造'
           |                else '终端不改造'
           |            end term_upgrade_cd,
           |            trans.sys_settle_dt,
           |            count(*) as transcnt
           |        from
           |            hive_acc_trans trans
           |        left join
           |            hive_ins_inf ins
           |        on
           |            trans.acpt_ins_id_cd=ins.ins_id_cd
           |        where
           |            trans.internal_trans_tp in ('C00022' , 'C20022', 'C00023')
           |        and trans.sys_settle_dt >= '$start_dt'
           |        and trans.sys_settle_dt <= '$end_dt'
           |        group by
           |            case
           |                when trans.internal_trans_tp='C00022'
           |                or  trans.internal_trans_tp='C20022'
           |                then '终端改造'
           |                else '终端不改造'
           |            end ,
           |            trans.sys_settle_dt) a
           |left join
           |    (
           |        select
           |            case
           |                when trans.internal_trans_tp='C00022'
           |                or  trans.internal_trans_tp='C20022'
           |                then '终端改造'
           |                else '终端不改造'
           |            end term_upgrade_cd,
           |            trans.sys_settle_dt,
           |            count(*)          as suctranscnt,
           |            sum(trans_tot_at) as transat,
           |            sum(trans.discount_at)          as discountat,
           |            count(distinct trans.cdhd_usr_id) as transusrcnt,
           |            count(distinct pri_acct_no)       as cardcnt
           |        from
           |            hive_acc_trans trans
           |        left join
           |            hive_ins_inf ins
           |        on
           |            trans.acpt_ins_id_cd=ins.ins_id_cd
           |        where
           |            trans.chswt_resp_cd='00'
           |        and trans.internal_trans_tp in ('C00022' ,
           |                                        'C20022',
           |                                        'C00023')
           |        and trans.sys_settle_dt >= '$start_dt'
           |        and trans.sys_settle_dt <= '$end_dt'
           |        group by
           |            case
           |                when trans.internal_trans_tp='C00022'
           |                or  trans.internal_trans_tp='C20022'
           |                then '终端改造'
           |                else '终端不改造'
           |            end ,
           |            trans.sys_settle_dt) b
           |on
           |    a.term_upgrade_cd=b.term_upgrade_cd
           |and a.sys_settle_dt = b.sys_settle_dt
           |order by
           |    a.transcnt desc
           |
          """.stripMargin)
      println(s"#### JOB_DM_35 spark sql 清洗数据完成时间为:" + DateUtils.getCurrentSystemTime())

      println(s"###JOB_DM_35------ results:"+results.count())
      if(!Option(results).isEmpty){
        results.save2Mysql("DM_DISC_TKT_ACT_TERM_UPG_DLY")
        println(s"#### JOB_DM_35 数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
      }else{
        println(s"#### JOB_DM_35 spark sql 清洗后数据无结果集！")
      }
    }
  }

  /**
    * JobName: JOB_DM_36
    * Feature:DM_DISC_TKT_ACT_MCHNT_IND_DLY
    *
    * @author tzq
    * @time 2016-12-21
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    */
  def JOB_DM_36 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("#### JOB_DM_36(DM_DISC_TKT_ACT_MCHNT_IND_DLY )### "+DateUtils.getCurrentSystemTime())
    DateUtils.timeCost("JOB_DM_36"){
      UPSQL_JDBC.delete(s"DM_DISC_TKT_ACT_MCHNT_IND_DLY ","REPORT_DT",start_dt,end_dt)
      println( "#### JOB_DM_36 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())

      var today_dt=start_dt
      if(interval>=0 ){
        sqlContext.sql(s"use $hive_dbname")
        for(i <- 0 to interval){
          println(s"#### JOB_DM_36 spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())

          val results =sqlContext.sql(
            s"""
               |select
               |    if(t.first_ind_nm is null,'????',t.first_ind_nm),
               |    if(t.second_ind_nm is null,'????',t.second_ind_nm),
               |    t.report_dt,
               |    sum(t.trans_cnt),
               |    sum(t.suc_trans_cnt),
               |    sum(t.trans_at),
               |    sum(t.discount_at),
               |    sum(t.trans_usr_cnt),
               |    sum(t.trans_card_cnt)
               |from
               |    (
               |        select
               |            a.first_para_nm  as first_ind_nm,
               |            a.second_para_nm as second_ind_nm,
               |            a.trans_dt       as report_dt,
               |            a.transcnt       as trans_cnt,
               |            b.suctranscnt    as suc_trans_cnt,
               |            b.transat        as trans_at,
               |            b.discountat     as discount_at,
               |            b.transusrcnt    as trans_usr_cnt,
               |            b.transcardcnt   as trans_card_cnt
               |        from
               |            (
               |                select
               |                    mp.mchnt_para_cn_nm     as first_para_nm,
               |                    mp1.mchnt_para_cn_nm    as second_para_nm,
               |                    to_date(trans.trans_dt) as trans_dt,
               |                    count(1)                as transcnt
               |                from
               |                    hive_acc_trans trans
               |                inner join
               |                    hive_store_term_relation str
               |                on
               |                    (
               |                        trans.card_accptr_cd = str.mchnt_cd
               |                    and trans.card_accptr_term_id = str.term_id)
               |                left join
               |                    hive_preferential_mchnt_inf pmi
               |                on
               |                    (
               |                        str.third_party_ins_id = pmi.mchnt_cd)
               |                left join
               |                    hive_mchnt_para mp
               |                on
               |                    (
               |                        pmi.mchnt_first_para = mp.mchnt_para_id)
               |                left join
               |                    hive_mchnt_para mp1
               |                on
               |                    (
               |                        pmi.mchnt_second_para = mp1.mchnt_para_id)
               |                left join
               |                    hive_ticket_bill_bas_inf bill
               |                on
               |                    (
               |                        trans.bill_id=bill.bill_id)
               |                where
               |                    trans.um_trans_id in ('AC02000065',
               |                                          'AC02000063')
               |                and bill.bill_sub_tp in ('01',
               |                                         '03')
               |                and trans.part_trans_dt = '$today_dt'
               |                group by
               |                    mp.mchnt_para_cn_nm,
               |                    mp1.mchnt_para_cn_nm,
               |                    to_date(trans_dt)) a
               |        left join
               |            (
               |                select
               |                    mp.mchnt_para_cn_nm               as first_para_nm,
               |                    mp1.mchnt_para_cn_nm              as second_para_nm,
               |                    to_date(trans.trans_dt)           as trans_dt,
               |                    count(1)                          as suctranscnt,
               |                    sum(trans.trans_at)               as transat,
               |                    sum(trans.discount_at)            as discountat,
               |                    count(distinct trans.cdhd_usr_id) as transusrcnt,
               |                    count(distinct trans.pri_acct_no) as transcardcnt
               |                from
               |                    hive_acc_trans trans
               |                inner join
               |                    hive_store_term_relation str
               |                on
               |                    (
               |                        trans.card_accptr_cd = str.mchnt_cd
               |                    and trans.card_accptr_term_id = str.term_id)
               |                left join
               |                    hive_preferential_mchnt_inf pmi
               |                on
               |                    (
               |                        str.third_party_ins_id = pmi.mchnt_cd)
               |                left join
               |                    hive_mchnt_para mp
               |                on
               |                    (
               |                        pmi.mchnt_first_para = mp.mchnt_para_id)
               |                left join
               |                    hive_mchnt_para mp1
               |                on
               |                    (
               |                        pmi.mchnt_second_para = mp1.mchnt_para_id)
               |                left join
               |                    hive_ticket_bill_bas_inf bill
               |                on
               |                    (
               |                        trans.bill_id=bill.bill_id)
               |                where
               |                    trans.sys_det_cd = 's'
               |                and trans.um_trans_id in ('AC02000065', 'AC02000063')
               |                and bill.bill_sub_tp in ('01','03')
               |                and trans.part_trans_dt = '$today_dt'
               |                group by
               |                    mp.mchnt_para_cn_nm,
               |                    mp1.mchnt_para_cn_nm,
               |                    to_date(trans_dt))b
               |        on
               |            (
               |                a.first_para_nm = b.first_para_nm
               |            and a.second_para_nm = b.second_para_nm
               |            and a.trans_dt = b.trans_dt )
               |        union all
               |        select
               |            a.first_para_nm,
               |            a.second_para_nm,
               |            a.trans_dt,
               |            a.transcnt,
               |            b.suctranscnt,
               |            b.transat,
               |            b.discountat,
               |            b.transusrcnt,
               |            b.transcardcnt
               |        from
               |            (
               |                select
               |                    mp.mchnt_para_cn_nm     as first_para_nm,
               |                    mp1.mchnt_para_cn_nm    as second_para_nm,
               |                    to_date(trans.trans_dt) as trans_dt,
               |                    count(1)                as transcnt
               |                from
               |                    hive_acc_trans trans
               |                left join
               |                    hive_store_term_relation str
               |                on
               |                    (
               |                        trans.card_accptr_cd = str.mchnt_cd
               |                    and trans.card_accptr_term_id = str.term_id)
               |                left join
               |                    (
               |                        select
               |                            mchnt_cd,
               |                            max(third_party_ins_id) over (partition by mchnt_cd) as
               |                            third_party_ins_id
               |                        from
               |                            hive_store_term_relation) str1
               |                on
               |                    (
               |                        trans.card_accptr_cd = str1.mchnt_cd)
               |                left join
               |                    hive_preferential_mchnt_inf pmi
               |                on
               |                    (
               |                        str1.third_party_ins_id = pmi.mchnt_cd)
               |                left join
               |                    hive_mchnt_para mp
               |                on
               |                    (
               |                        pmi.mchnt_first_para = mp.mchnt_para_id)
               |                left join
               |                    hive_mchnt_para mp1
               |                on
               |                    (
               |                        pmi.mchnt_second_para = mp1.mchnt_para_id)
               |                left join
               |                    hive_ticket_bill_bas_inf bill
               |                on
               |                    (
               |                        trans.bill_id=bill.bill_id)
               |                where
               |                    trans.um_trans_id in ('AC02000065',
               |                                          'AC02000063')
               |                and bill.bill_sub_tp in ('01',
               |                                         '03')
               |                and trans.part_trans_dt = '$today_dt'
               |                group by
               |                    mp.mchnt_para_cn_nm,
               |                    mp1.mchnt_para_cn_nm,
               |                    to_date(trans_dt)) a
               |        left join
               |            (
               |                select
               |                    mp.mchnt_para_cn_nm               as first_para_nm,
               |                    mp1.mchnt_para_cn_nm              as second_para_nm,
               |                    to_date(trans.trans_dt)           as trans_dt,
               |                    count(1)                          as suctranscnt,
               |                    sum(trans.trans_at)               as transat,
               |                    sum(trans.discount_at)            as discountat,
               |                    count(distinct trans.cdhd_usr_id) as transusrcnt,
               |                    count(distinct trans.pri_acct_no) as transcardcnt
               |                from
               |                    hive_acc_trans trans
               |                left join
               |                    hive_store_term_relation str
               |                on
               |                    (
               |                        trans.card_accptr_cd = str.mchnt_cd
               |                    and trans.card_accptr_term_id = str.term_id)
               |                left join
               |                    (
               |                        select
               |                            mchnt_cd,
               |                            max(third_party_ins_id) over (partition by mchnt_cd) as
               |                            third_party_ins_id
               |                        from
               |                            hive_store_term_relation) str1
               |                on
               |                    (
               |                        trans.card_accptr_cd = str1.mchnt_cd)
               |                left join
               |                    hive_preferential_mchnt_inf pmi
               |                on
               |                    (
               |                        str1.third_party_ins_id = pmi.mchnt_cd)
               |                left join
               |                    hive_mchnt_para mp
               |                on
               |                    (
               |                        pmi.mchnt_first_para = mp.mchnt_para_id)
               |                left join
               |                    hive_mchnt_para mp1
               |                on
               |                    (
               |                        pmi.mchnt_second_para = mp1.mchnt_para_id)
               |                left join
               |                    hive_ticket_bill_bas_inf bill
               |                on
               |                    (
               |                        trans.bill_id=bill.bill_id)
               |                where
               |                    trans.sys_det_cd = 's'
               |                and trans.um_trans_id in ('AC02000065',
               |                                          'AC02000063')
               |                and bill.bill_sub_tp in ('01',
               |                                         '03')
               |                and trans.part_trans_dt = '$today_dt'
               |                group by
               |                    mp.mchnt_para_cn_nm,
               |                    mp1.mchnt_para_cn_nm,
               |                    to_date(trans_dt))b
               |        on
               |            (
               |                a.first_para_nm = b.first_para_nm
               |            and a.second_para_nm = b.second_para_nm
               |            and a.trans_dt = b.trans_dt )) t
               |group by
               |    if(t.first_ind_nm is null,'????',t.first_ind_nm),
               |    if(t.second_ind_nm is null,'????',t.second_ind_nm),
               |    t.report_dt
               |
          """.stripMargin)

          println(s"#### JOB_DM_36 spark sql 清洗[$today_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())

//          println(s"###JOB_DM_36------$today_dt results:"+results.count())
          if(!Option(results).isEmpty){
            println(s"#### JOB_DM_36 [$today_dt]数据插入开始时间为：" + DateUtils.getCurrentSystemTime())
            results.save2Mysql("DM_DISC_TKT_ACT_MCHNT_IND_DLY")
            println(s"#### JOB_DM_36 [$today_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
          }else{
            println(s"#### JOB_DM_36 spark sql 清洗[$today_dt]数据无结果集！")
          }
          today_dt=DateUtils.addOneDay(today_dt)
        }
      }
    }
  }

  /**
    * JobName: JOB_DM_37
    * Feature:DM_DISC_TKT_ACT_MCHNT_TP_DLY
    *
    * @author tzq
    * @time 2016-12-22
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    */
  def JOB_DM_37 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {
    println("###JOB_DM_37(DM_DISC_TKT_ACT_MCHNT_TP_DLY )### "+DateUtils.getCurrentSystemTime())
    DateUtils.timeCost("JOB_DM_37"){
      UPSQL_JDBC.delete(s"DM_DISC_TKT_ACT_MCHNT_TP_DLY ","REPORT_DT",start_dt,end_dt)
      println( "#### JOB_DM_37 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())

      println(s"#### JOB_DM_37 spark sql 清洗数据开始时间为:" + DateUtils.getCurrentSystemTime())
      sqlContext.sql(s"use $hive_dbname")
      val results =sqlContext.sql(
        s"""
           |select
           |    a.grp_nm       as mchnt_tp_grp,
           |    a.tp_nm        as mchnt_tp,
           |    a.trans_dt     as report_dt,
           |    a.transcnt     as trans_cnt,
           |    b.suctranscnt  as suc_trans_cnt,
           |    b.transat      as trans_at,
           |    b.discountat   as discount_at,
           |    b.usrcnt       as trans_usr_cnt,
           |    b.cardcnt      as trans_card_cnt
           |from
           |    (
           |        select
           |            a1.grp_nm,
           |            a1.tp_nm,
           |            a1.trans_dt,
           |            count(*) as transcnt
           |        from
           |            (
           |                select
           |                    trim(if(tp_grp.mchnt_tp_grp_desc_cn is null,'其他',tp_grp.mchnt_tp_grp_desc_cn)) as grp_nm ,
           |                    trim(if(tp.mchnt_tp_desc_cn is null,'其他',tp.mchnt_tp_desc_cn)) as tp_nm,
           |                    to_date(trans_dt) as trans_dt,
           |                    trans_at,
           |                    discount_at,
           |                    cdhd_usr_id,
           |                    pri_acct_no
           |                from
           |                    hive_acc_trans trans
           |                inner join
           |                    hive_mchnt_inf_wallet mchnt
           |                on
           |                    trans.mchnt_cd=mchnt.mchnt_cd
           |                left join
           |                    hive_mchnt_tp tp
           |                on
           |                    mchnt.mchnt_tp=tp.mchnt_tp
           |                left join
           |                    hive_mchnt_tp_grp tp_grp
           |                on
           |                    tp.mchnt_tp_grp=tp_grp.mchnt_tp_grp
           |                inner join
           |                    hive_ticket_bill_bas_inf bill
           |                on
           |                    trans.bill_id=bill.bill_id
           |                where
           |                    um_trans_id in ('AC02000065',
           |                                    'AC02000063')
           |                and bill_sub_tp in ('01',
           |                                    '03')
           |                and trans.part_trans_dt>='$start_dt'
           |                and trans.part_trans_dt<='$end_dt') a1
           |        group by
           |            a1.grp_nm,
           |            a1.tp_nm,
           |           to_date( a1.trans_dt) ) a
           |inner join
           |    (
           |        select
           |            b1.grp_nm,
           |            b1.tp_nm,
           |            b1.trans_dt,
           |            count(*)                       as suctranscnt,
           |            sum(b1.trans_at)               as transat,
           |            sum(b1.discount_at)            as discountat,
           |            count(distinct b1.cdhd_usr_id) as usrcnt,
           |            count(distinct b1.pri_acct_no) as cardcnt
           |        from
           |            (
           |                select
           |                    trim(if(tp_grp.mchnt_tp_grp_desc_cn is null,'其他',tp_grp.mchnt_tp_grp_desc_cn)) as grp_nm ,
           |                    trim(if(tp.mchnt_tp_desc_cn is null,'其他',tp.mchnt_tp_desc_cn)) as tp_nm,
           |                    to_date(trans_dt) as trans_dt,
           |                    trans_at,
           |                    discount_at,
           |                    cdhd_usr_id,
           |                    pri_acct_no
           |                from
           |                    hive_acc_trans trans
           |                inner join
           |                    hive_mchnt_inf_wallet mchnt
           |                on
           |                    trans.mchnt_cd=mchnt.mchnt_cd
           |                left join
           |                    hive_mchnt_tp tp
           |                on
           |                    mchnt.mchnt_tp=tp.mchnt_tp
           |                left join
           |                    hive_mchnt_tp_grp tp_grp
           |                on
           |                    tp.mchnt_tp_grp=tp_grp.mchnt_tp_grp
           |                inner join
           |                    hive_ticket_bill_bas_inf bill
           |                on
           |                    trans.bill_id=bill.bill_id
           |                where
           |                    sys_det_cd='S'
           |                and um_trans_id in ('AC02000065',
           |                                    'AC02000063')
           |                and bill_sub_tp in ('01','03')
           |                and trans.part_trans_dt>='$start_dt'
           |                and trans.part_trans_dt<='$end_dt' ) b1
           |        group by
           |            b1.grp_nm,
           |            b1.tp_nm,
           |            to_date(b1.trans_dt)) b
           |on
           |    a.grp_nm=b.grp_nm
           |and a.tp_nm=b.tp_nm
           |and a.trans_dt=b.trans_dt
           |order by
           |    a.grp_nm,
           |    a.tp_nm,
           |    a.trans_dt
          """.stripMargin)
      println(s"#### JOB_DM_37 spark sql 清洗数据完成时间为:" + DateUtils.getCurrentSystemTime())

      println(s"###JOB_DM_37------ results:"+results.count())
      if(!Option(results).isEmpty){
        results.save2Mysql("DM_DISC_TKT_ACT_MCHNT_TP_DLY")
        println(s"#### JOB_DM_37 数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
      }else{
        println(s"#### JOB_DM_37 spark sql 清洗后数据无结果集！")
      }
    }
  }


  /**
    * JobName: JOB_DM_38
    * Feature:DM_DISC_TKT_ACT_ISS_INS_DLY
    *
    * @author tzq
    * @time 2016-12-22
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    */
  def JOB_DM_38 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {
    println("###JOB_DM_38(DM_DISC_TKT_ACT_ISS_INS_DLY )### "+DateUtils.getCurrentSystemTime())
    DateUtils.timeCost("JOB_DM_38"){
      UPSQL_JDBC.delete(s"DM_DISC_TKT_ACT_ISS_INS_DLY ","REPORT_DT",start_dt,end_dt)
      println( "#### JOB_DM_38 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())

      println(s"#### JOB_DM_38 spark sql 清洗数据开始时间为:" + DateUtils.getCurrentSystemTime())
      sqlContext.sql(s"use $hive_dbname")
      val results =sqlContext.sql(
        s"""
           |select
           |    a.iss_ins_cn_nm    as iss_ins_nm,
           |    a.trans_dt         as report_dt,
           |    a.transcnt         as trans_cnt,
           |    b.suctranscnt      as suc_trans_cnt,
           |    b.transat          as trans_at,
           |    b.discountat       as discount_at,
           |    b.transusrcnt      as trans_usr_cnt,
           |    b.transcardcnt     as trans_card_cnt
           |from
           |    (
           |        select
           |            trim(if(cbi.iss_ins_cn_nm is null,'其他',cbi.iss_ins_cn_nm)) as iss_ins_cn_nm,
           |            to_date(trans.trans_dt) as trans_dt,
           |            count(1) as transcnt
           |        from
           |            hive_acc_trans trans
           |        left join
           |            hive_card_bind_inf cbi
           |        on (trans.card_no = cbi.bind_card_no)
           |        left join
           |            hive_ticket_bill_bas_inf bill
           |        on
           |            (
           |                trans.bill_id=bill.bill_id)
           |        where
           |            trans.um_trans_id in ('AC02000065','AC02000063')
           |        and bill.bill_sub_tp in ('01','03')
           |        and trans.part_trans_dt >= '$start_dt'
           |        and trans.part_trans_dt <= '$end_dt'
           |        group by
           |            trim(if(cbi.iss_ins_cn_nm is null,'其他',cbi.iss_ins_cn_nm)),
           |            to_date(trans.trans_dt)) a
           |left join
           |    (
           |        select
           |            trim(if(cbi.iss_ins_cn_nm is null,'其他',cbi.iss_ins_cn_nm)) as iss_ins_cn_nm,
           |            to_date(trans.trans_dt) as trans_dt,
           |            count(1) as suctranscnt,
           |            sum(trans.trans_at) as transat,
           |            sum(trans.discount_at)          as discountat,
           |            count(distinct trans.cdhd_usr_id) as transusrcnt,
           |            count(distinct trans.pri_acct_no) as transcardcnt
           |        from
           |            hive_acc_trans trans
           |        left join
           |            hive_card_bind_inf cbi
           |        on (trans.card_no = cbi.bind_card_no)
           |        left join
           |            hive_ticket_bill_bas_inf bill
           |        on
           |            (
           |                trans.bill_id=bill.bill_id)
           |        where
           |            trans.sys_det_cd = 'S'
           |        and trans.um_trans_id in ('AC02000065', 'AC02000063')
           |        and bill.bill_sub_tp in ('01', '03')
           |        and trans.part_trans_dt >= '$start_dt'
           |        and trans.part_trans_dt <= '$end_dt'
           |        group by
           |            trim(if(cbi.iss_ins_cn_nm is null,'其他',cbi.iss_ins_cn_nm)),
           |            to_date(trans.trans_dt))b
           |on
           |a.iss_ins_cn_nm = b.iss_ins_cn_nm and a.trans_dt = b.trans_dt
           |
          """.stripMargin)
      println(s"#### JOB_DM_38 spark sql 清洗数据完成时间为:" + DateUtils.getCurrentSystemTime())

      println(s"###JOB_DM_38------ results:"+results.count())
      if(!Option(results).isEmpty){
        results.save2Mysql("DM_DISC_TKT_ACT_ISS_INS_DLY")
        println(s"#### JOB_DM_38 数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
      }else{
        println(s"#### JOB_DM_38 spark sql 清洗后数据无结果集！")
      }
    }
  }



  /**
    * JobName: JOB_DM_39
    * Feature:DM_DISC_TKT_ACT_BRAND_INF_DLY
    *
    * @author tzq
    * @time 2016-12-22
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    */
  def JOB_DM_39 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("###JOB_DM_39(DM_DISC_TKT_ACT_BRAND_INF_DLY )### "+DateUtils.getCurrentSystemTime())
    DateUtils.timeCost("JOB_DM_39"){
      UPSQL_JDBC.delete(s"DM_DISC_TKT_ACT_BRAND_INF_DLY ","REPORT_DT",start_dt,end_dt)

      println( "#### JOB_DM_39 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())

      var today_dt=start_dt
      if(interval>=0 ){
        sqlContext.sql(s"use $hive_dbname")
        for(i <- 0 to interval){
          println(s"#### JOB_DM_39 spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())
          val results =sqlContext.sql(
            s"""
               |select
               |    a.brand_nm  as brand_nm,
               |    a.trans_dt  as report_dt,
               |    a.transcnt  as trans_cnt,
               |    b.suctranscnt  as suc_trans_cnt,
               |    b.transat  as trans_at,
               |    b.discountat  as discount_at,
               |    b.transusrcnt  as trans_usr_cnt,
               |    b.transcardcnt  as trans_card_cnt
               |from
               |    (
               |        select
               |            if(brand.brand_nm is null,'其他',substr(brand.brand_nm,1,42)) as brand_nm,
               |            to_date(trans.trans_dt) as trans_dt,
               |            count(1) as transcnt
               |        from
               |            hive_acc_trans trans
               |        inner join
               |            hive_ticket_bill_bas_inf bill
               |        on
               |            trans.bill_id=bill.bill_id
               |        left join
               |            hive_chara_grp_def_bat chara_grp
               |        on
               |            bill.chara_grp_cd=chara_grp.chara_grp_cd
               |        inner join
               |            hive_preferential_mchnt_inf pre_mchnt
               |        on
               |            pre_mchnt.mchnt_cd=chara_grp.chara_data
               |        inner join
               |            hive_brand_inf brand
               |        on
               |            pre_mchnt.brand_id=brand.brand_id
               |        where
               |            trans.um_trans_id in ('AC02000065',
               |                                  'AC02000063')
               |        and bill.bill_sub_tp in ('01', '03')
               |        and trans.part_trans_dt ='$today_dt'
               |        group by
               |           if(brand.brand_nm is null,'其他',substr(brand.brand_nm,1,42)),
               |            to_date(trans.trans_dt)) a
               |left join
               |    (
               |        select
               |            if(brand.brand_nm is null,'其他',substr(brand.brand_nm,1,42)) as brand_nm,
               |            to_date(trans.trans_dt) as trans_dt,
               |            count(1) as suctranscnt,
               |            sum(trans.trans_at) as transat,
               |            sum(trans.discount_at)          as discountat,
               |            count(distinct trans.cdhd_usr_id) as transusrcnt,
               |            count(distinct trans.pri_acct_no) as transcardcnt
               |        from
               |            hive_acc_trans trans
               |        inner join
               |            hive_ticket_bill_bas_inf bill
               |        on
               |            trans.bill_id=bill.bill_id
               |        left join
               |            hive_chara_grp_def_bat chara_grp
               |        on
               |            bill.chara_grp_cd=chara_grp.chara_grp_cd
               |        inner join
               |            hive_preferential_mchnt_inf pre_mchnt
               |        on
               |            pre_mchnt.mchnt_cd=chara_grp.chara_data
               |        inner join
               |            hive_brand_inf brand
               |        on
               |            pre_mchnt.brand_id=brand.brand_id
               |        where
               |            trans.sys_det_cd = 'S'
               |        and trans.um_trans_id in ('AC02000065', 'AC02000063')
               |        and bill.bill_sub_tp in ('01', '03')
               |        and trans.part_trans_dt = '$today_dt'
               |        group by
               |            if(brand.brand_nm is null,'其他',substr(brand.brand_nm,1,42)),
               |            to_date(trans.trans_dt))b
               |on a.brand_nm = b.brand_nm and a.trans_dt = b.trans_dt
               |where a.trans_dt='$today_dt'
               |order by a.transcnt desc limit 10
               |
          """.stripMargin)
          println(s"#### JOB_DM_39 spark sql 清洗[$today_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())

          //          println(s"###JOB_DM_39------$today_dt results:"+results.count())
          if(!Option(results).isEmpty){
            println(s"#### JOB_DM_39 [$today_dt]数据插入开始时间为：" + DateUtils.getCurrentSystemTime())
            results.save2Mysql("DM_DISC_TKT_ACT_BRAND_INF_DLY")
            println(s"#### JOB_DM_39 [$today_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
          }else{
            println(s"#### JOB_DM_39 spark sql 清洗[$today_dt]数据无结果集！")
          }
          today_dt=DateUtils.addOneDay(today_dt)
        }
      }

    }
  }

  /**
    * JobName: JOB_DM_40
    * Feature:DM_SWT_PNT_INS_INF_DLY
    *
    * @author tzq
    * @time 2016-12-22
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    */
  def JOB_DM_40 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {
    println("###JOB_DM_40(DM_SWT_PNT_INS_INF_DLY )### "+DateUtils.getCurrentSystemTime())
    DateUtils.timeCost("JOB_DM_40"){
      UPSQL_JDBC.delete(s"DM_SWT_PNT_INS_INF_DLY ","REPORT_DT",start_dt,end_dt)
      println( "#### JOB_DM_40 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())

      println(s"#### JOB_DM_40 spark sql 清洗数据开始时间为:" + DateUtils.getCurrentSystemTime())
      sqlContext.sql(s"use $hive_dbname")
      val results =sqlContext.sql(
        s"""
           |select
           |    trim(if(stat.access_ins_nm is null,'其他',stat.access_ins_nm)) as ins_nm,
           |    to_date(swt.trans_dt)        as report_dt,
           |    count(*)            as trans_cnt,
           |    sum(
           |        case
           |            when substr(swt.trans_st,1,1)='1'
           |            then 1
           |            else 0
           |        end ) as suc_trans_cnt ,
           |    sum(swt.trans_tot_at)           as trans_at,
           |    sum(swt.req_trans_at)           as point_at,
           |    count(distinct swt.usr_id) as trans_usr_cnt,
           |    count(distinct swt.pri_acct_no) as trans_card_cnt
           |from
           |    hive_switch_point_trans swt
           |left join
           |    hive_access_static_inf stat
           |on
           |    swt.rout_ins_id_cd=stat.access_ins_id_cd
           |where
           |    swt.part_trans_dt>='$start_dt'
           |and swt.part_trans_dt<='$end_dt'
           |and swt.rout_ins_id_cd<>'00250002'
           |and swt.trans_tp='S370000000'
           |and swt.rout_ins_id_cd not like '0016%'
           |group by
           |trim(if(stat.access_ins_nm is null,'其他',stat.access_ins_nm)),
           |to_date(swt.trans_dt)
          """.stripMargin)
      println(s"#### JOB_DM_40 spark sql 清洗数据完成时间为:" + DateUtils.getCurrentSystemTime())

      println(s"###JOB_DM_40------ results:"+results.count())
      if(!Option(results).isEmpty){
        results.save2Mysql("DM_SWT_PNT_INS_INF_DLY")
        println(s"#### JOB_DM_40 数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
      }else{
        println(s"#### JOB_DM_40 spark sql 清洗后数据无结果集！")
      }
    }
  }


  /**
    * JobName: JOB_DM_41
    * Feature:DM_SWT_PNT_MCHNT_BRANCH_DLY
    *
    * @author tzq
    * @time 2016-12-23
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    */
  def JOB_DM_41 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {
    println("###JOB_DM_41(DM_SWT_PNT_MCHNT_BRANCH_DLY )### "+DateUtils.getCurrentSystemTime())
    DateUtils.timeCost("JOB_DM_41"){
      UPSQL_JDBC.delete(s"DM_SWT_PNT_MCHNT_BRANCH_DLY ","REPORT_DT",start_dt,end_dt)
      println( "#### JOB_DM_41 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())

      println(s"#### JOB_DM_41 spark sql 清洗数据开始时间为:" + DateUtils.getCurrentSystemTime())
      sqlContext.sql(s"use $hive_dbname")
      val results =sqlContext.sql(
        s"""
           |select
           |    trim(if(acpt_ins.cup_branch_ins_id_nm is null,'总公司',acpt_ins.cup_branch_ins_id_nm)) as cup_branch_ins_id_nm,
           |    to_date(swt.trans_dt)           as report_dt,
           |    count(*)                        as trans_cnt,
           |    sum(
           |        case
           |            when substr(swt.trans_st,1,1)='1'
           |            then 1
           |            else 0
           |        end )                       as suc_trans_cnt,
           |    sum(trans_tot_at)               as trans_at,
           |    sum(req_trans_at)               as point_at,
           |    count(distinct swt.usr_id)      as trans_usr_cnt,
           |    count(distinct swt.pri_acct_no) as trans_card_cnt
           |from
           |    hive_switch_point_trans swt
           |left join
           |    hive_mchnt_inf_wallet mchnt
           |on
           |    (
           |        swt.mchnt_cd=mchnt.mchnt_cd)
           |left join
           |    hive_branch_acpt_ins_inf acpt_ins
           |on
           |    (
           |        acpt_ins.ins_id_cd= concat('000',mchnt.acpt_ins_id_cd))
           |where
           |    swt.part_trans_dt>='$start_dt'
           |and swt.part_trans_dt<='$end_dt'
           |and swt.trans_tp='S370000000'
           |group by
           |    trim(if(acpt_ins.cup_branch_ins_id_nm is null,'总公司',acpt_ins.cup_branch_ins_id_nm)),
           |    to_date(swt.trans_dt)
           |
           |
          """.stripMargin)
      println(s"#### JOB_DM_41 spark sql 清洗数据完成时间为:" + DateUtils.getCurrentSystemTime())

      println(s"###JOB_DM_41------ results:"+results.count())
      if(!Option(results).isEmpty){
        results.save2Mysql("DM_SWT_PNT_MCHNT_BRANCH_DLY")
        println(s"#### JOB_DM_41 数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
      }else{
        println(s"#### JOB_DM_41 spark sql 清洗后数据无结果集！")
      }
    }
  }


  /**
    * JobName: JOB_DM_42
    * Feature:DM_SWT_PNT_MOBILE_LOC_DLY
    *
    * @author tzq
    * @time 2016-12-27
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    */
  def JOB_DM_42 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {
    println("###JOB_DM_42(DM_SWT_PNT_MOBILE_LOC_DLY )### "+DateUtils.getCurrentSystemTime())
    DateUtils.timeCost("JOB_DM_42"){
      UPSQL_JDBC.delete(s"DM_SWT_PNT_MOBILE_LOC_DLY ","REPORT_DT",start_dt,end_dt)
      println( "#### JOB_DM_42 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())

      println(s"#### JOB_DM_42 spark sql 清洗数据开始时间为:" + DateUtils.getCurrentSystemTime())
      sqlContext.sql(s"use $hive_dbname")
      val results =sqlContext.sql(
        s"""
           |select
           |    trim(if(pri_acct.phone_location is null,'总公司',pri_acct.phone_location)) as mobile_loc,
           |    to_date(swt.trans_dt)    as report_dt,
           |    count(*)                 as trans_cnt,
           |    sum(
           |        case
           |            when substr(swt.trans_st,1,1)='1'
           |            then 1
           |            else 0
           |        end )                        as suc_trans_cnt,
           |    sum(trans_tot_at)                as trans_at,
           |    sum(req_trans_at)                as point_at,
           |    count(distinct swt.usr_id)       as trans_usr_cnt,
           |    count(distinct swt.pri_acct_no)  as trans_card_cnt
           |from
           |    hive_switch_point_trans swt
           |left join
           |    hive_pri_acct_inf pri_acct
           |on
           |    (
           |        swt.usr_id = pri_acct.cdhd_usr_id)
           |where
           |    swt.part_trans_dt>='$start_dt'
           |and swt.part_trans_dt<='$end_dt'
           |and swt.trans_tp='S370000000'
           |group by
           |   trim(if(pri_acct.phone_location is null,'总公司',pri_acct.phone_location)),
           |    to_date(swt.trans_dt)
           |
          """.stripMargin)
      println(s"#### JOB_DM_42 spark sql 清洗数据完成时间为:" + DateUtils.getCurrentSystemTime())

      println(s"###JOB_DM_42------ results:"+results.count())
      if(!Option(results).isEmpty){
        results.save2Mysql("DM_SWT_PNT_MOBILE_LOC_DLY")
        println(s"#### JOB_DM_42 数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
      }else{
        println(s"#### JOB_DM_42 spark sql 清洗后数据无结果集！")
      }
    }
  }
  /**
    * JobName: JOB_DM_43
    * Feature:DM_SWT_PNT_ISS_INS_DLY
    *
    * @author tzq
    * @time 2016-12-26
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    */
  def JOB_DM_43(implicit sqlContext: HiveContext, start_dt: String, end_dt: String) = {
    println("###JOB_DM_43(DM_SWT_PNT_ISS_INS_DLY )### " + DateUtils.getCurrentSystemTime())
    DateUtils.timeCost("JOB_DM_43") {
      UPSQL_JDBC.delete(s"DM_SWT_PNT_ISS_INS_DLY ", "REPORT_DT", start_dt, end_dt)
      println("#### JOB_DM_43 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())

      println(s"#### JOB_DM_43 spark sql 清洗数据开始时间为:" + DateUtils.getCurrentSystemTime())
      sqlContext.sql(s"use $hive_dbname")
      val results = sqlContext.sql(
        s"""
           |select
           |    trim(if(cb.iss_ins_cn_nm is null,'其他',cb.iss_ins_cn_nm)) as iss_ins_nm,
           |    to_date(swt.trans_dt)  as   report_dt,
           |    count(*)               as   trans_cnt,
           |    sum(
           |        case
           |            when substr(swt.trans_st,1,1)='1'
           |            then 1
           |            else 0
           |        end )                       as suc_trans_cnt,
           |    sum(trans_tot_at)               as trans_at,
           |    sum(req_trans_at)               as point_at,
           |    count(distinct swt.usr_id)      as trans_usr_cnt,
           |    count(distinct swt.pri_acct_no) as trans_card_cnt
           |from
           |    hive_switch_point_trans swt
           |left join
           |    hive_card_bin cb
           |on
           |    (
           |        swt.card_bin = cb.card_bin)
           |where
           |    swt.part_trans_dt>= '$start_dt'
           |and swt.part_trans_dt<= '$end_dt'
           |and swt.trans_tp='S370000000'
           |group by
           |    trim(if(cb.iss_ins_cn_nm is null,'其他',cb.iss_ins_cn_nm)),
           |    to_date(swt.trans_dt)
           |
          """.stripMargin)
      println(s"#### JOB_DM_43 spark sql 清洗数据完成时间为:" + DateUtils.getCurrentSystemTime())

      println(s"###JOB_DM_43------ results:" + results.count())
      if (!Option(results).isEmpty) {
        results.save2Mysql("DM_SWT_PNT_ISS_INS_DLY")
        println(s"#### JOB_DM_43 数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
      } else {
        println(s"#### JOB_DM_43 spark sql 清洗后数据无结果集！")
      }
    }
  }


  /**
    * JOB_DM_44 20161229
    * dm_buss_dist_pnt_dly->hive_acc_trans,hive_buss_dist
    * Code by Xue
    *
    * @param sqlContext
    * @return
    */
  def JOB_DM_44(implicit sqlContext: HiveContext, start_dt: String, end_dt: String, interval: Int) = {
    println("###JOB_DM_44(dm_buss_dist_pnt_dly->hive_acc_trans)")
    DateUtils.timeCost("JOB_DM_44") {
      UPSQL_JDBC.delete(s"dm_buss_dist_pnt_dly", "report_dt", start_dt, end_dt)
      println("#### JOB_DM_44 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      println(s"#### JOB_DM_44  spark sql 清洗数据开始时间为:" + DateUtils.getCurrentSystemTime())
      sqlContext.sql(s"use $hive_dbname")
      val results = sqlContext.sql(
        s"""
           |select
           |    a.chara_acct_nm as buss_dist_nm,
           |    a.trans_dt as report_dt,
           |    a.transcnt as trans_cnt,
           |    b.suctranscnt as suc_trans_cnt,
           |    b.transat as trans_at,
           |    b.pntat as point_at,
           |    b.transusrcnt as trans_usr_cnt,
           |    b.transcardcnt as trans_card_cnt
           |from
           |    (
           |select
           |if(bd.chara_acct_nm is null,'其他',bd.chara_acct_nm) as chara_acct_nm,
           |            trans.trans_dt,
           |            count(1) as transcnt
           |        from
           |            hive_acc_trans trans
           |        left join
           |            hive_buss_dist bd
           |        on
           |            (
           |                trans.chara_acct_tp=bd.chara_acct_tp )
           |        where
           |            trans.um_trans_id = 'AC02000065'
           |        and trans.buss_tp = '03'
           |        and trans.part_trans_dt >= '$start_dt'
           |        and trans.part_trans_dt <= '$end_dt'
           |        group by
           |if(bd.chara_acct_nm is null,'其他',bd.chara_acct_nm),
           |            trans_dt) a
           |left join
           |    (
           |select
           |if(bd.chara_acct_nm is null,'其他',bd.chara_acct_nm) as chara_acct_nm,
           |            trans.trans_dt,
           |            count(1) as suctranscnt,
           |            sum(trans.trans_at) as transat,
           |            sum(point_at) as pntat,
           |            count(distinct trans.cdhd_usr_id) as transusrcnt,
           |            count(distinct trans.pri_acct_no) as transcardcnt
           |        from
           |            hive_acc_trans trans
           |        left join
           |            hive_buss_dist bd
           |        on
           |            (
           |                trans.chara_acct_tp=bd.chara_acct_tp )
           |        where
           |            trans.um_trans_id = 'AC02000065'
           |        and trans.buss_tp = '03'
           |        and trans.sys_det_cd = 'S'
           |        and trans.part_trans_dt >= '$start_dt'
           |        and trans.part_trans_dt <= '$end_dt'
           |        group by
           |if(bd.chara_acct_nm is null,'其他',bd.chara_acct_nm),
           |            trans_dt)b
           |on
           |    (
           |        a.chara_acct_nm = b.chara_acct_nm
           |    and a.trans_dt = b.trans_dt )
           |where a.chara_acct_nm is not null
           | """.stripMargin)
      println(s"#### JOB_DM_44 spark sql 清洗数据完成时间为:" + DateUtils.getCurrentSystemTime())

      if (!Option(results).isEmpty) {
        results.save2Mysql("dm_buss_dist_pnt_dly")
        println(s"#### JOB_DM_44 数据插入完成时间为：" + DateUtils.getCurrentSystemTime()
        )
      } else {
        println(s"#### JOB_DM_44 spark sql 清洗数据无结果集！")
      }
    }
  }


  /**
    * JOB_DM_45 20161230
    * dm_buss_dist_pnt_mchnt_branch_dly->hive_acc_trans,hive_mchnt_inf_wallet
    * Code by Xue
    *
    * @param sqlContext
    * @return
    */
  def JOB_DM_45(implicit sqlContext: HiveContext, start_dt: String, end_dt: String, interval: Int) = {
    println("###JOB_DM_45(dm_buss_dist_pnt_mchnt_branch_dly->hive_acc_trans,hive_mchnt_inf_wallet)")
    DateUtils.timeCost("JOB_DM_45") {
      UPSQL_JDBC.delete(s"dm_buss_dist_pnt_mchnt_branch_dly", "report_dt", start_dt, end_dt)
      println("#### JOB_DM_45 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      println(s"#### JOB_DM_45  spark sql 清洗数据开始时间为:" + DateUtils.getCurrentSystemTime())
      sqlContext.sql(s"use $hive_dbname")
      val results = sqlContext.sql(
        s"""
           |select
           |    a.cup_branch_ins_id_nm as cup_branch_ins_id_nm,
           |    a.trans_dt as report_dt,
           |    a.transcnt as trans_cnt,
           |    b.suctranscnt as suc_trans_cnt,
           |    b.transat as trans_at,
           |    b.pntat as point_at,
           |    b.transusrcnt as trans_usr_cnt,
           |    b.transcardcnt as trans_card_cnt
           |
           |from
           |    (
           |        select
           |if(acpt_ins.cup_branch_ins_id_nm is null,'总公司',acpt_ins.cup_branch_ins_id_nm) as cup_branch_ins_id_nm,
           |            trans.trans_dt,
           |            count(1) as transcnt
           |        from
           |            hive_acc_trans trans
           |        left join
           |            hive_mchnt_inf_wallet mchnt
           |        on
           |            (
           |                trans.mchnt_cd=mchnt.mchnt_cd)
           |        left join
           |            hive_branch_acpt_ins_inf acpt_ins
           |        on
           |            (
           |                acpt_ins.ins_id_cd=concat('000',mchnt.acpt_ins_id_cd))
           |        left join
           |            hive_buss_dist bd
           |        on
           |            (
           |                trans.chara_acct_tp=bd.chara_acct_tp )
           |        where
           |            trans.um_trans_id = 'AC02000065'
           |        and trans.buss_tp = '03'
           |        and trans.part_trans_dt >= '$start_dt'
           |        and trans.part_trans_dt <= '$end_dt'
           |        group by
           |if(acpt_ins.cup_branch_ins_id_nm is null,'总公司',acpt_ins.cup_branch_ins_id_nm),
           |            trans_dt) a
           |left join
           |    (
           |        select
           |if(acpt_ins.cup_branch_ins_id_nm is null,'总公司',acpt_ins.cup_branch_ins_id_nm) as cup_branch_ins_id_nm,
           |            trans.trans_dt,
           |            count(1) as suctranscnt,
           |            sum(trans.trans_at) as transat,
           |            sum(point_at) as pntat,
           |            count(distinct trans.cdhd_usr_id) as transusrcnt,
           |            count(distinct trans.pri_acct_no) as transcardcnt
           |        from
           |            hive_acc_trans trans
           |        left join
           |            hive_mchnt_inf_wallet mchnt
           |        on
           |            (
           |                trans.mchnt_cd=mchnt.mchnt_cd)
           |        left join
           |            hive_branch_acpt_ins_inf acpt_ins
           |        on
           |            (
           |                acpt_ins.ins_id_cd=concat('000',mchnt.acpt_ins_id_cd))
           |        left join
           |            hive_buss_dist bd
           |        on
           |            (
           |                trans.chara_acct_tp=bd.chara_acct_tp )
           |        where
           |            trans.sys_det_cd = 'S'
           |        and trans.um_trans_id = 'AC02000065'
           |        and trans.buss_tp = '03'
           |        and trans.trans_dt >= '$start_dt'
           |        and trans.trans_dt <= '$end_dt'
           |        group by
           |if(acpt_ins.cup_branch_ins_id_nm is null,'总公司',acpt_ins.cup_branch_ins_id_nm),
           |            trans_dt)b
           |on
           |    (
           |        a.cup_branch_ins_id_nm = b.cup_branch_ins_id_nm
           |    and a.trans_dt = b.trans_dt )
           |
           | """.stripMargin)
      println(s"#### JOB_DM_45 spark sql 清洗数据完成时间为:" + DateUtils.getCurrentSystemTime())

      if (!Option(results).isEmpty) {
        results.save2Mysql("dm_buss_dist_pnt_mchnt_branch_dly")
        println(s"#### JOB_DM_45 数据插入完成时间为：" + DateUtils.getCurrentSystemTime()
        )
      } else {
        println(s"#### JOB_DM_45 spark sql 清洗数据无结果集！")
      }
    }
  }

  /**
    * JOB_DM_46 20161230
    * dm_buss_dist_pnt_mobile_loc_dly->hive_acc_trans,hive_pri_acct_inf
    * Code by Xue
    *
    * @param sqlContext
    * @return
    */
  def JOB_DM_46(implicit sqlContext: HiveContext, start_dt: String, end_dt: String, interval: Int) = {
    println("###JOB_DM_46(dm_buss_dist_pnt_mobile_loc_dly->hive_acc_trans,hive_pri_acct_inf)")
    DateUtils.timeCost("JOB_DM_46") {
      UPSQL_JDBC.delete(s"dm_buss_dist_pnt_mobile_loc_dly", "report_dt", start_dt, end_dt)
      println("#### JOB_DM_46 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      println(s"#### JOB_DM_46  spark sql 清洗数据开始时间为:" + DateUtils.getCurrentSystemTime())
      sqlContext.sql(s"use $hive_dbname")
      val results = sqlContext.sql(
        s"""
           |select
           |    a.phone_location as mobile_loc,
           |    a.trans_dt as report_dt,
           |    a.transcnt as trans_cnt,
           |    b.suctranscnt as suc_trans_cnt,
           |    b.transat as trans_at,
           |    b.pntat as point_at,
           |    b.transusrcnt as trans_usr_cnt,
           |    b.transcardcnt as trans_card_cnt
           |from
           |    (
           |        select
           |if(pri_acct.phone_location is null,'总公司',pri_acct.phone_location) as phone_location,
           |            trans.trans_dt,
           |            count(1) as transcnt
           |        from
           |            hive_acc_trans trans
           |        left join
           |            hive_pri_acct_inf pri_acct
           |        on
           |            (
           |                trans.cdhd_usr_id = pri_acct.cdhd_usr_id)
           |        left join
           |            hive_buss_dist bd
           |        on
           |            (
           |                trans.chara_acct_tp=bd.chara_acct_tp )
           |        where
           |            trans.um_trans_id = 'AC02000065'
           |        and trans.buss_tp = '03'
           |        and trans.part_trans_dt >= '$start_dt'
           |        and trans.part_trans_dt <= '$end_dt'
           |        group by
           |if(pri_acct.phone_location is null,'总公司',pri_acct.phone_location),
           |            trans_dt) a
           |left join
           |    (
           |        select
           |if(pri_acct.phone_location is null,'总公司',pri_acct.phone_location) as phone_location,
           |            trans.trans_dt,
           |            count(1) as suctranscnt,
           |            sum(trans.trans_at) as transat,
           |            sum(point_at) as pntat,
           |            count(distinct trans.cdhd_usr_id) as transusrcnt,
           |            count(distinct trans.pri_acct_no) as transcardcnt
           |        from
           |            hive_acc_trans trans
           |        left join
           |            hive_pri_acct_inf pri_acct
           |        on
           |            (
           |                trans.cdhd_usr_id = pri_acct.cdhd_usr_id)
           |        left join
           |            hive_buss_dist bd
           |        on
           |            (
           |                trans.chara_acct_tp=bd.chara_acct_tp )
           |        where
           |            trans.sys_det_cd = 'S'
           |        and trans.um_trans_id = 'AC02000065'
           |        and trans.buss_tp = '03'
           |        and trans.part_trans_dt >= '$start_dt'
           |        and trans.part_trans_dt <= '$end_dt'
           |        group by
           |if(pri_acct.phone_location is null,'总公司',pri_acct.phone_location),
           |            trans_dt)b
           |on
           |    (
           |        a.phone_location = b.phone_location
           |    and a.trans_dt = b.trans_dt )
           |	where a.phone_location is not null
           | """.stripMargin)
      println(s"#### JOB_DM_46 spark sql 清洗数据完成时间为:" + DateUtils.getCurrentSystemTime())

      if (!Option(results).isEmpty) {
        results.save2Mysql("dm_buss_dist_pnt_mobile_loc_dly")
        println(s"#### JOB_DM_46 数据插入完成时间为：" + DateUtils.getCurrentSystemTime()
        )
      } else {
        println(s"#### JOB_DM_46 spark sql 清洗数据无结果集！")
      }
    }
  }



  /**
    * JOB_DM_47 20170103
    * dm_buss_dist_pnt_mchnt_ind_dly->hive_acc_trans,hive_store_term_relation,hive_preferential_mchnt_inf,hive_mchnt_para,hive_buss_dist
    * Code by Xue
    *
    * @param sqlContext
    * @return
    */
  def JOB_DM_47(implicit sqlContext: HiveContext, start_dt: String, end_dt: String, interval: Int) = {
    println("###JOB_DM_47(dm_buss_dist_pnt_mchnt_ind_dly->hive_acc_trans,hive_store_term_relation,hive_preferential_mchnt_inf,hive_mchnt_para,hive_buss_dist)")
    DateUtils.timeCost("JOB_DM_47") {
      UPSQL_JDBC.delete(s"dm_buss_dist_pnt_mchnt_ind_dly", "report_dt", start_dt, end_dt)
      println("#### JOB_DM_47 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      println(s"#### JOB_DM_47  spark sql 清洗数据开始时间为:" + DateUtils.getCurrentSystemTime())
      sqlContext.sql(s"use $hive_dbname")
      val results = sqlContext.sql(
        s"""
           |select
           |    if(t.first_ind_nm is null,'其他',t.first_ind_nm)     as first_ind_nm,
           |    if(t.second_ind_nm is null,'其他',t.second_ind_nm)   as second_ind_nm,
           |    t.report_dt                                          as report_dt,
           |    sum(t.trans_cnt)                                     as trans_cnt,
           |    sum(t.suc_trans_cnt)                                 as suc_trans_cnt,
           |    sum(t.trans_at)                                      as trans_at,
           |    sum(t.discount_at)                                   as discount_at,
           |    sum(t.trans_usr_cnt)                                 as trans_usr_cnt,
           |    sum(t.trans_card_cnt)                                as trans_card_cnt
           |from
           |    (
           |        select
           |            a.first_para_nm  as first_ind_nm,
           |            a.second_para_nm as second_ind_nm,
           |            a.trans_dt       as report_dt,
           |            a.transcnt       as trans_cnt,
           |            b.suctranscnt    as suc_trans_cnt,
           |            b.transat        as trans_at,
           |            b.pntat          as discount_at,
           |            b.transusrcnt    as trans_usr_cnt,
           |            b.transcardcnt   as trans_card_cnt
           |        from
           |            (
           |                select
           |                    mp.mchnt_para_cn_nm  as first_para_nm,
           |                    mp1.mchnt_para_cn_nm as second_para_nm,
           |                    trans.trans_dt,
           |                    count(1) as transcnt
           |                from
           |                    hive_acc_trans trans
           |                inner join
           |                    hive_store_term_relation str
           |                on
           |                    (
           |                        trans.card_accptr_cd = str.mchnt_cd
           |                    and trans.card_accptr_term_id =str.term_id)
           |                left join
           |                    hive_preferential_mchnt_inf pmi
           |                on
           |                    (
           |                        str.third_party_ins_id = pmi.mchnt_cd)
           |                left join
           |                    hive_mchnt_para mp
           |                on
           |                    (
           |                        pmi.mchnt_first_para = mp.mchnt_para_id)
           |                left join
           |                    hive_mchnt_para mp1
           |                on
           |                    (
           |                        pmi.mchnt_second_para = mp1.mchnt_para_id)
           |                left join
           |                    hive_buss_dist bd
           |                on
           |                    (
           |                        trans.chara_acct_tp=bd.chara_acct_tp )
           |                where
           |                    trans.um_trans_id = 'AC02000065'
           |                and trans.buss_tp = '03'
           |                and str.rec_id is not null
           |                and trans.part_trans_dt >= '$start_dt'
           |                and trans.part_trans_dt <= '$end_dt'
           |                group by
           |                    mp.mchnt_para_cn_nm,
           |                    mp1.mchnt_para_cn_nm,
           |                    trans_dt) a
           |        left join
           |            (
           |                select
           |                    mp.mchnt_para_cn_nm  as first_para_nm,
           |                    mp1.mchnt_para_cn_nm as second_para_nm,
           |                    trans.trans_dt,
           |                    count(1)                          as suctranscnt,
           |                    sum(trans.trans_at)               as transat,
           |                    sum(point_at)                     as pntat,
           |                    count(distinct trans.cdhd_usr_id) as transusrcnt,
           |                    count(distinct trans.pri_acct_no) as transcardcnt
           |                from
           |                    hive_acc_trans trans
           |                inner join
           |                    hive_store_term_relation str
           |                on
           |                    (
           |                        trans.card_accptr_cd = str.mchnt_cd
           |                    and trans.card_accptr_term_id =str.term_id)
           |                left join
           |                    hive_preferential_mchnt_inf pmi
           |                on
           |                    (
           |                        str.third_party_ins_id = pmi.mchnt_cd)
           |                left join
           |                    hive_mchnt_para mp
           |                on
           |                    (
           |                        pmi.mchnt_first_para = mp.mchnt_para_id)
           |                left join
           |                    hive_mchnt_para mp1
           |                on
           |                    (
           |                        pmi.mchnt_second_para = mp1.mchnt_para_id)
           |                left join
           |                    hive_buss_dist bd
           |                on
           |                    (
           |                        trans.chara_acct_tp=bd.chara_acct_tp )
           |                where
           |                    trans.sys_det_cd = 'S'
           |                and trans.um_trans_id = 'AC02000065'
           |                and trans.buss_tp = '03'
           |                and str.rec_id is not null
           |                and trans.part_trans_dt >= '$start_dt'
           |                and trans.part_trans_dt <= '$end_dt'
           |                group by
           |                    mp.mchnt_para_cn_nm,
           |                    mp1.mchnt_para_cn_nm,
           |                    trans_dt)b
           |        on
           |            (
           |                a.first_para_nm = b.first_para_nm
           |            and a.second_para_nm = b.second_para_nm
           |            and a.trans_dt = b.trans_dt )
           |        union all
           |        select
           |            a.first_para_nm,
           |            a.second_para_nm,
           |            a.trans_dt,
           |            a.transcnt,
           |            b.suctranscnt,
           |            b.transat,
           |            b.pntat,
           |            b.transusrcnt,
           |            b.transcardcnt
           |        from
           |            (
           |                select
           |                    mp.mchnt_para_cn_nm  as first_para_nm,
           |                    mp1.mchnt_para_cn_nm as second_para_nm,
           |                    trans.trans_dt,
           |                    count(1) as transcnt
           |                from
           |                    hive_acc_trans trans
           |                left join
           |                    hive_store_term_relation str
           |                on
           |                    (
           |                        trans.card_accptr_cd = str.mchnt_cd
           |                    and trans.card_accptr_term_id = str.term_id)
           |                left join
           |                    (
           |                        select
           |                            mchnt_cd,
           |                            term_id,
           |                            max(third_party_ins_id) over (partition by mchnt_cd)
           |                        from
           |                            hive_store_term_relation) str1
           |                on
           |                    (
           |                        trans.card_accptr_cd = str1.mchnt_cd
           |                    and trans.card_accptr_term_id = str1.term_id)
           |                left join
           |                    hive_preferential_mchnt_inf pmi
           |                on
           |                    (
           |                        str.third_party_ins_id = pmi.mchnt_cd)
           |                left join
           |                    hive_mchnt_para mp
           |                on
           |                    (
           |                        pmi.mchnt_first_para = mp.mchnt_para_id)
           |                left join
           |                    hive_mchnt_para mp1
           |                on
           |                    (
           |                        pmi.mchnt_second_para = mp1.mchnt_para_id)
           |                left join
           |                    hive_buss_dist bd
           |                on
           |                    (
           |                        trans.chara_acct_tp=bd.chara_acct_tp )
           |                where
           |                    trans.um_trans_id = 'AC02000065'
           |                and trans.buss_tp = '03'
           |                and str.rec_id is null
           |                and trans.part_trans_dt >= '$start_dt'
           |                and trans.part_trans_dt <= '$end_dt'
           |                group by
           |                    mp.mchnt_para_cn_nm,
           |                    mp1.mchnt_para_cn_nm,
           |                    trans_dt) a
           |        left join
           |            (
           |                select
           |                    mp.mchnt_para_cn_nm  as first_para_nm,
           |                    mp1.mchnt_para_cn_nm as second_para_nm,
           |                    trans.trans_dt,
           |                    count(1)                          as suctranscnt,
           |                    sum(trans.trans_at)               as transat,
           |                    sum(point_at)                     as pntat,
           |                    count(distinct trans.cdhd_usr_id) as transusrcnt,
           |                    count(distinct trans.pri_acct_no) as transcardcnt
           |                from
           |                    hive_acc_trans trans
           |                left join
           |                    hive_store_term_relation str
           |                on
           |                    (
           |                        trans.card_accptr_cd = str.mchnt_cd
           |                    and trans.card_accptr_term_id =str.term_id)
           |                left join
           |                    (
           |                        select
           |                            mchnt_cd,
           |                            term_id,
           |                            max(third_party_ins_id) over (partition by mchnt_cd)
           |                        from
           |                            hive_store_term_relation) str1
           |                on
           |                    (
           |                        trans.card_accptr_cd = str1.mchnt_cd
           |                    and trans.card_accptr_term_id = str1.term_id)
           |                left join
           |                    hive_preferential_mchnt_inf pmi
           |                on
           |                    (
           |                        str.third_party_ins_id = pmi.mchnt_cd)
           |                left join
           |                    hive_mchnt_para mp
           |                on
           |                    (
           |                        pmi.mchnt_first_para = mp.mchnt_para_id)
           |                left join
           |                    hive_mchnt_para mp1
           |                on
           |                    (
           |                        pmi.mchnt_second_para = mp1.mchnt_para_id)
           |                left join
           |                    hive_buss_dist bd
           |                on
           |                    (
           |                        trans.chara_acct_tp=bd.chara_acct_tp )
           |                where
           |                    trans.sys_det_cd = 'S'
           |                and trans.um_trans_id = 'AC02000065'
           |                and trans.buss_tp = '03'
           |                and str.rec_id is null
           |                and trans.part_trans_dt >= '$start_dt'
           |                and trans.part_trans_dt <= '$end_dt'
           |                group by
           |                    mp.mchnt_para_cn_nm,
           |                    mp1.mchnt_para_cn_nm,
           |                    trans_dt)b
           |        on
           |            (
           |                a.first_para_nm = b.first_para_nm
           |            and a.second_para_nm = b.second_para_nm
           |            and a.trans_dt = b.trans_dt )) t
           |group by
           |    if(t.first_ind_nm is null,'其他',t.first_ind_nm),
           |    if(t.second_ind_nm is null,'其他',t.second_ind_nm),
           |    t.report_dt
           | """.stripMargin)
      println(s"#### JOB_DM_47 spark sql 清洗数据完成时间为:" + DateUtils.getCurrentSystemTime())

      if (!Option(results).isEmpty) {
        results.save2Mysql("dm_buss_dist_pnt_mchnt_ind_dly")
        println(s"#### JOB_DM_47 数据插入完成时间为：" + DateUtils.getCurrentSystemTime()
        )
      } else {
        println(s"#### JOB_DM_47 spark sql 清洗数据无结果集！")
      }
    }
  }


  /**
    * JOB_DM_48 20170103
    * dm_buss_dist_pnt_iss_ins_dly->hive_acc_trans,hive_buss_dist,hive_card_bind_inf
    * Code by Xue
    *
    * @param sqlContext
    * @return
    */
  def JOB_DM_48(implicit sqlContext: HiveContext, start_dt: String, end_dt: String, interval: Int) = {
    println("###JOB_DM_48(dm_buss_dist_pnt_iss_ins_dly->hive_acc_trans,hive_buss_dist,hive_card_bind_inf)")
    DateUtils.timeCost("JOB_DM_48") {
      UPSQL_JDBC.delete(s"dm_buss_dist_pnt_iss_ins_dly", "report_dt", start_dt, end_dt)
      println("#### JOB_DM_48 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      println(s"#### JOB_DM_48  spark sql 清洗数据开始时间为:" + DateUtils.getCurrentSystemTime())
      sqlContext.sql(s"use $hive_dbname")
      val results = sqlContext.sql(
        s"""
           |select
           |    a.iss_ins_cn_nm as iss_ins_nm,
           |    a.trans_dt as report_dt,
           |    a.transcnt as trans_cnt,
           |    b.suctranscnt as suc_trans_cnt,
           |    b.transat as trans_at,
           |    b.pntat as point_at,
           |    b.transusrcnt as trans_usr_cnt,
           |    b.transcardcnt as trans_card_cnt
           |from
           |    (
           |select
           |if(cbi.iss_ins_cn_nm is null,'其他',cbi.iss_ins_cn_nm) as iss_ins_cn_nm,
           |            trans.trans_dt,
           |            count(1) as transcnt
           |        from
           |            hive_acc_trans trans
           |        left join
           |            hive_buss_dist bd
           |        on
           |            (
           |                trans.chara_acct_tp=bd.chara_acct_tp )
           |        left join
           |            hive_card_bind_inf cbi
           |        on
           |            (
           |                trans.card_no = cbi.bind_card_no)
           |        where
           |            trans.um_trans_id = 'AC02000065'
           |        and trans.buss_tp = '03'
           |        and trans.part_trans_dt >= '$start_dt'
           |        and trans.part_trans_dt <= '$end_dt'
           |        group by
           |if(cbi.iss_ins_cn_nm is null,'其他',cbi.iss_ins_cn_nm),
           |            trans_dt) a
           |left join
           |    (
           |select
           |if(cbi.iss_ins_cn_nm is null,'其他',cbi.iss_ins_cn_nm) as iss_ins_cn_nm,
           |            trans.trans_dt,
           |            count(1) as suctranscnt,
           |            sum(trans.trans_at) as transat,
           |            sum(point_at) as pntat,
           |            count(distinct trans.cdhd_usr_id) as transusrcnt,
           |            count(distinct trans.pri_acct_no) as transcardcnt
           |        from
           |            hive_acc_trans trans
           |        left join
           |            hive_buss_dist bd
           |        on
           |            (
           |                trans.chara_acct_tp=bd.chara_acct_tp )
           |        left join
           |            hive_card_bind_inf cbi
           |        on
           |            (
           |                trans.card_no = cbi.bind_card_no)
           |        where
           |            trans.um_trans_id = 'AC02000065'
           |        and trans.buss_tp = '03'
           |        and trans.sys_det_cd = 'S'
           |        and trans.part_trans_dt >= '$start_dt'
           |        and trans.part_trans_dt <= '$end_dt'
           |        group by
           |if(cbi.iss_ins_cn_nm is null,'其他',cbi.iss_ins_cn_nm),
           |            trans_dt)b
           |on
           |    (
           |        a.iss_ins_cn_nm = b.iss_ins_cn_nm
           |    and a.trans_dt = b.trans_dt )
           |	where a.iss_ins_cn_nm is not null
           |
           | """.stripMargin)
      println(s"#### JOB_DM_48 spark sql 清洗数据完成时间为:" + DateUtils.getCurrentSystemTime())

      if (!Option(results).isEmpty) {
        results.save2Mysql("dm_buss_dist_pnt_iss_ins_dly")
        println(s"#### JOB_DM_48 数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
      } else {
        println(s"#### JOB_DM_48 spark sql 清洗数据无结果集！")
      }
    }
  }



  /**
    * JobName: JOB_DM_49
    * Feature:DM_BUSS_DIST_PNT_ONLINE_POINT_DLY
    *
    * @author tzq
    * @time 2016-12-26
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    */
  def JOB_DM_49(implicit sqlContext: HiveContext, start_dt: String, end_dt: String) = {
    println("###JOB_DM_49(DM_BUSS_DIST_PNT_ONLINE_POINT_DLY )### " + DateUtils.getCurrentSystemTime())
    DateUtils.timeCost("JOB_DM_49") {
      UPSQL_JDBC.delete(s"DM_BUSS_DIST_PNT_ONLINE_POINT_DLY ", "REPORT_DT", start_dt, end_dt)
      println("#### JOB_DM_49 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())

      println(s"#### JOB_DM_49 spark sql 清洗数据开始时间为:" + DateUtils.getCurrentSystemTime())
      sqlContext.sql(s"use $hive_dbname")
      val results = sqlContext.sql(
        s"""
           |select
           |    trim(if(bd.chara_acct_nm is null,'其他',bd.chara_acct_nm)) as buss_dist_nm,
           |    to_date(trans.trans_dt)              as report_dt,
           |    count(1)                             as suc_trans_cnt,
           |    sum(trans.trans_at)                  as point_at,
           |    count(distinct trans.cdhd_usr_id)    as trans_usr_cnt
           |from
           |    hive_online_point_trans trans
           |left join
           |    hive_buss_dist bd
           |on
           |    (
           |        trans.chara_acct_tp = bd.chara_acct_tp)
           |where
           |trans.part_trans_dt >= '$start_dt'
           |and trans.part_trans_dt <= '$end_dt'
           |and trans.status = '1'
           |group by
           |    trim(if(bd.chara_acct_nm is null,'其他',bd.chara_acct_nm)),
           |    to_date(trans.trans_dt)
           |
          """.stripMargin)
      println(s"#### JOB_DM_49 spark sql 清洗数据完成时间为:" + DateUtils.getCurrentSystemTime())

      println(s"###JOB_DM_49------ results:" + results.count())
      if (!Option(results).isEmpty) {
        results.save2Mysql("DM_BUSS_DIST_PNT_ONLINE_POINT_DLY")
        println(s"#### JOB_DM_49 数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
      } else {
        println(s"#### JOB_DM_49 spark sql 清洗后数据无结果集！")
      }
    }
  }
  /**
    * JobName: JOB_DM_50
    * Feature:DM_VAL_TKT_ACT_BRANCH_DLY
    *
    * @author tzq
    * @time 2016-12-26
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    */
  def JOB_DM_50(implicit sqlContext: HiveContext, start_dt: String, end_dt: String) = {
    println("###JOB_DM_50(DM_VAL_TKT_ACT_BRANCH_DLY )### " + DateUtils.getCurrentSystemTime())
    DateUtils.timeCost("JOB_DM_50") {
      UPSQL_JDBC.delete(s"DM_VAL_TKT_ACT_BRANCH_DLY ", "REPORT_DT", start_dt, end_dt)
      println("#### JOB_DM_50 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())

      println(s"#### JOB_DM_50 spark sql 清洗数据开始时间为:" + DateUtils.getCurrentSystemTime())
      sqlContext.sql(s"use $hive_dbname")
      val results = sqlContext.sql(
        s"""
           |select
           |a.cup_branch_ins_id_nm as cup_branch_ins_id_nm,
           |a.trans_dt as report_dt,
           |a.transcnt as trans_cnt,
           |c.suctranscnt as suc_trans_cnt,
           |c.bill_original_price as bill_original_price,
           |c.bill_price as bill_price,
           |a.transusrcnt as trans_usr_cnt,
           |b.payusrcnt as pay_usr_cnt,
           |c.paysucusrcnt as pay_suc_usr_cnt
           |from
           |(
           |select
           |trim(if(bill.cup_branch_ins_id_nm is null,'总公司',bill.cup_branch_ins_id_nm)) as cup_branch_ins_id_nm,
           |to_date(trans.trans_dt) as trans_dt,
           |count(1) as transcnt,
           |count(distinct cdhd_usr_id) as transusrcnt
           |from
           |hive_bill_order_trans trans
           |left join
           |hive_bill_sub_order_trans sub_trans
           |on
           |(
           |trans.bill_order_id = sub_trans.bill_order_id)
           |left join
           |hive_ticket_bill_bas_inf bill
           |on
           |(
           |sub_trans.bill_id=bill.bill_id)
           |where
           |bill.bill_sub_tp <> '08'
           |and trans.part_trans_dt >= '$start_dt'
           |and trans.part_trans_dt <= '$end_dt'
           |group by
           |trim(if(bill.cup_branch_ins_id_nm is null,'总公司',bill.cup_branch_ins_id_nm)),
           |to_date(trans.trans_dt)) a
           |left join
           |(
           |select
           |trim(if(bill.cup_branch_ins_id_nm is null,'总公司',bill.cup_branch_ins_id_nm)) as cup_branch_ins_id_nm,
           |to_date(trans.trans_dt) as trans_dt,
           |count(distinct cdhd_usr_id) as payusrcnt
           |from
           |hive_bill_order_trans trans
           |left join
           |hive_bill_sub_order_trans sub_trans
           |on
           |(
           |trans.bill_order_id = sub_trans.bill_order_id)
           |left join
           |hive_ticket_bill_bas_inf bill
           |on
           |(
           |sub_trans.bill_id=bill.bill_id)
           |where
           |bill.bill_sub_tp <> '08'
           |and trans.order_st in ('00',
           |'01',
           |'02',
           |'03',
           |'04')
           |and trans.part_trans_dt >= '$start_dt'
           |and trans.part_trans_dt <= '$end_dt'
           |group by
           |trim(if(bill.cup_branch_ins_id_nm is null,'总公司',bill.cup_branch_ins_id_nm)),
           |to_date(trans.trans_dt)) b
           |on
           |(
           |a.cup_branch_ins_id_nm = b.cup_branch_ins_id_nm
           |and a.trans_dt = b.trans_dt)
           |left join
           |(
           |select
           |trim(if(bill.cup_branch_ins_id_nm is null,'总公司',bill.cup_branch_ins_id_nm)) as cup_branch_ins_id_nm,
           |to_date(trans.trans_dt) as trans_dt,
           |count(1) as suctranscnt,
           |sum(bill.bill_original_price) as bill_original_price,
           |sum(bill.bill_price) as bill_price,
           |count(distinct cdhd_usr_id) as paysucusrcnt
           |from
           |hive_bill_order_trans trans
           |left join
           |hive_bill_sub_order_trans sub_trans
           |on
           |(
           |trans.bill_order_id = sub_trans.bill_order_id)
           |left join
           |hive_ticket_bill_bas_inf bill
           |on
           |(
           |sub_trans.bill_id=bill.bill_id)
           |where
           |bill.bill_sub_tp <> '08'
           |and trans.order_st = '00'
           |and trans.part_trans_dt >= '$start_dt'
           |and trans.part_trans_dt <= '$end_dt'
           |group by
           |trim(if(bill.cup_branch_ins_id_nm is null,'总公司',bill.cup_branch_ins_id_nm)),
           |to_date(trans.trans_dt)) c
           |on
           |(
           |a.cup_branch_ins_id_nm = c.cup_branch_ins_id_nm
           |and a.trans_dt = c.trans_dt)
          """.stripMargin)
      println(s"#### JOB_DM_50 spark sql 清洗数据完成时间为:" + DateUtils.getCurrentSystemTime())

      println(s"###JOB_DM_50------ results:" + results.count())
      if (!Option(results).isEmpty) {
        results.save2Mysql("DM_VAL_TKT_ACT_BRANCH_DLY")
        println(s"#### JOB_DM_50 数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
      } else {
        println(s"#### JOB_DM_50 spark sql 清洗后数据无结果集！")
      }
    }
  }


  /**
    * JobName: JOB_DM_51
    * Feature:DM_VAL_TKT_ACT_MOBILE_LOC_DLY
    * Notice:
    *
    * @author tzq
    * @time 2016-12-26
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    */
  def JOB_DM_51(implicit sqlContext: HiveContext, start_dt: String, end_dt: String) = {
    println("###JOB_DM_51( DM_VAL_TKT_ACT_MOBILE_LOC_DLY---- )### " + DateUtils.getCurrentSystemTime())
    DateUtils.timeCost("JOB_DM_51") {
      UPSQL_JDBC.delete(s"DM_VAL_TKT_ACT_MOBILE_LOC_DLY ", "REPORT_DT", start_dt, end_dt)
      println("#### JOB_DM_51 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())

      println(s"#### JOB_DM_51 spark sql 清洗数据开始时间为:" + DateUtils.getCurrentSystemTime())
      sqlContext.sql(s"use $hive_dbname")
      val results = sqlContext.sql(
        s"""
           |select
           |    a.phone_location  as phone_location,
           |    a.trans_dt  as report_dt,
           |    a.transcnt  as trans_cnt,
           |    c.suctranscnt  as suc_trans_cnt,
           |    c.bill_original_price  as bill_original_price,
           |    c.bill_price  as bill_price,
           |    a.transusrcnt  as trans_usr_cnt,
           |    b.payusrcnt  as pay_usr_cnt,
           |    c.paysucusrcnt    as pay_suc_usr_cnt
           |from
           |    (
           |        select
           |            trim(if(pri_acct.phone_location is null,'总公司',pri_acct.phone_location)) as phone_location,
           |            to_date(trans.trans_dt) as trans_dt,
           |            count(1)                    as transcnt,
           |            count(distinct trans.cdhd_usr_id) as transusrcnt
           |        from
           |            hive_bill_order_trans trans
           |        left join
           |            hive_bill_sub_order_trans sub_trans
           |        on
           |            (
           |                trans.bill_order_id = sub_trans.bill_order_id)
           |        left join
           |            hive_ticket_bill_bas_inf bill
           |        on
           |            (
           |                sub_trans.bill_id=bill.bill_id)
           |        left join
           |            hive_pri_acct_inf pri_acct
           |        on
           |            (
           |                trans.cdhd_usr_id = pri_acct.cdhd_usr_id
           |             )
           |        where
           |        bill.bill_sub_tp <> '08'
           |        and trans.part_trans_dt >= '$start_dt'
           |        and trans.part_trans_dt <= '$end_dt'
           |        group by
           |            trim(if(pri_acct.phone_location is null,'总公司',pri_acct.phone_location)),
           |            to_date(trans.trans_dt)) a
           |left join
           |    (
           |        select
           |            trim(if(pri_acct.phone_location is null,'总公司',pri_acct.phone_location)) as phone_location,
           |            to_date(trans.trans_dt) as trans_dt,
           |            count(distinct trans.cdhd_usr_id) as payusrcnt
           |        from
           |            hive_bill_order_trans trans
           |        left join
           |            hive_bill_sub_order_trans sub_trans
           |        on
           |            (
           |                trans.bill_order_id = sub_trans.bill_order_id)
           |        left join
           |            hive_ticket_bill_bas_inf bill
           |        on
           |            (
           |                sub_trans.bill_id=bill.bill_id)
           |        left join
           |            hive_pri_acct_inf pri_acct
           |        on
           |            (
           |                trans.cdhd_usr_id = pri_acct.cdhd_usr_id)
           |        where
           |        bill.bill_sub_tp <> '08'
           |        and trans.order_st in ('00',
           |                               '01',
           |                               '02',
           |                               '03',
           |                               '04')
           |        and trans.part_trans_dt >= '$start_dt'
           |        and trans.part_trans_dt <= '$end_dt'
           |        group by
           |            trim(if(pri_acct.phone_location is null,'总公司',pri_acct.phone_location)),
           |            to_date(trans.trans_dt)) b
           |on
           |    (
           |        a.phone_location = b.phone_location
           |    and a.trans_dt = b.trans_dt)
           |left join
           |    (
           |        select
           |            trim(if(pri_acct.phone_location is null,'总公司',pri_acct.phone_location)) as phone_location,
           |            to_date(trans.trans_dt) as trans_dt,
           |            count(1)                      as suctranscnt,
           |            sum(bill.bill_original_price) as bill_original_price,
           |            sum(bill.bill_price)          as bill_price,
           |            count(distinct trans.cdhd_usr_id)   as paysucusrcnt
           |        from
           |            hive_bill_order_trans trans
           |        left join
           |            hive_bill_sub_order_trans sub_trans
           |        on
           |            (
           |                trans.bill_order_id = sub_trans.bill_order_id)
           |        left join
           |            hive_ticket_bill_bas_inf bill
           |        on
           |            (
           |                sub_trans.bill_id=bill.bill_id)
           |        left join
           |            hive_pri_acct_inf pri_acct
           |        on
           |            (
           |                trans.cdhd_usr_id = pri_acct.cdhd_usr_id)
           |        where
           |        bill.bill_sub_tp <> '08'
           |        and trans.order_st = '00'
           |        and trans.part_trans_dt >= '$start_dt'
           |        and trans.part_trans_dt <= '$end_dt'
           |        group by
           |            trim(if(pri_acct.phone_location is null,'总公司',pri_acct.phone_location)),
           |            to_date(trans.trans_dt)) c
           |on
           |    (
           |        a.phone_location = c.phone_location
           |    and a.trans_dt = c.trans_dt
           |    )
           |
          """.stripMargin)
      println(s"#### JOB_DM_51 spark sql 清洗数据完成时间为:" + DateUtils.getCurrentSystemTime())

      println(s"###JOB_DM_51------ results:" + results.count())
      if (!Option(results).isEmpty) {
        results.save2Mysql("DM_VAL_TKT_ACT_MOBILE_LOC_DLY")
        println(s"#### JOB_DM_51 数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
      } else {
        println(s"#### JOB_DM_51 spark sql 清洗后数据无结果集！")
      }
    }
  }

  /**
    * JobName: JOB_DM_52
    * Feature: DM_VAL_TKT_ACT_ISS_INS_DLY
    * Notice:
    *
    * @author tzq
    * @time 2016-12-26
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    */
  def JOB_DM_52(implicit sqlContext: HiveContext, start_dt: String, end_dt: String) = {
    println("###JOB_DM_52(DM_VAL_TKT_ACT_ISS_INS_DLY )### " + DateUtils.getCurrentSystemTime())
    DateUtils.timeCost("JOB_DM_52") {
      UPSQL_JDBC.delete(s"DM_VAL_TKT_ACT_ISS_INS_DLY", "REPORT_DT", start_dt, end_dt)
      println("#### JOB_DM_52 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())

      println(s"#### JOB_DM_52 spark sql 清洗数据开始时间为:" + DateUtils.getCurrentSystemTime())
      sqlContext.sql(s"use $hive_dbname")
      val results = sqlContext.sql(
        s"""
           |select
           |    a.iss_ins_cn_nm       as iss_ins_nm,
           |    a.trans_dt            as report_dt,
           |    a.transcnt            as trans_cnt,
           |    c.suctranscnt         as suc_trans_cnt,
           |    c.bill_original_price as bill_original_price,
           |    c.bill_price          as bill_price,
           |    a.transusrcnt         as trans_usr_cnt,
           |    b.payusrcnt           as pay_usr_cnt,
           |    c.paysucusrcnt        as pay_suc_usr_cnt
           |from
           |    (
           |        select
           |            trim(if(cbi.iss_ins_cn_nm is null,'其他',cbi.iss_ins_cn_nm))as iss_ins_cn_nm,
           |            to_date(trans.trans_dt) as trans_dt,
           |            count(1)                          as transcnt,
           |            count(distinct trans.cdhd_usr_id) as transusrcnt
           |        from
           |            hive_bill_order_trans trans
           |        left join
           |            hive_bill_sub_order_trans sub_trans
           |        on
           |            (
           |                trans.bill_order_id = sub_trans.bill_order_id)
           |        left join
           |            hive_ticket_bill_bas_inf bill
           |        on
           |            (
           |                sub_trans.bill_id=bill.bill_id)
           |        left join
           |            hive_card_bind_inf cbi
           |        on
           |            (
           |                concat(length(trans.card_no),trans.card_no) = cbi.bind_card_no)
           |        where
           |            bill.bill_sub_tp <> '08'
           |        and trans.part_trans_dt >= '$start_dt'
           |        and trans.part_trans_dt <= '$end_dt'
           |        group by
           |            trim(if(cbi.iss_ins_cn_nm is null,'其他',cbi.iss_ins_cn_nm)),
           |            to_date(trans.trans_dt)) a
           |left join
           |    (
           |        select
           |            trim(if(cbi.iss_ins_cn_nm is null,'其他',cbi.iss_ins_cn_nm))as iss_ins_cn_nm,
           |            to_date(trans.trans_dt) as trans_dt,
           |            count(distinct trans.cdhd_usr_id) as payusrcnt
           |        from
           |            hive_bill_order_trans trans
           |        left join
           |            hive_bill_sub_order_trans sub_trans
           |        on
           |            (
           |                trans.bill_order_id = sub_trans.bill_order_id)
           |        left join
           |            hive_ticket_bill_bas_inf bill
           |        on
           |            (
           |                sub_trans.bill_id=bill.bill_id)
           |        left join
           |            hive_card_bind_inf cbi
           |        on
           |            (
           |                concat(length(trans.card_no),card_no) = cbi.bind_card_no)
           |        where
           |            bill.bill_sub_tp <> '08'
           |        and trans.order_st in ('00',
           |                               '01',
           |                               '02',
           |                               '03',
           |                               '04')
           |        and trans.part_trans_dt >= '$start_dt'
           |        and trans.part_trans_dt <= '$end_dt'
           |        group by
           |            trim(if(cbi.iss_ins_cn_nm is null,'其他',cbi.iss_ins_cn_nm)),
           |            to_date(trans.trans_dt)) b
           |on
           |    (
           |        a.iss_ins_cn_nm = b.iss_ins_cn_nm
           |    and a.trans_dt = b.trans_dt)
           |left join
           |    (
           |        select
           |            trim(if(cbi.iss_ins_cn_nm is null,'其他',cbi.iss_ins_cn_nm))as iss_ins_cn_nm,
           |            to_date(trans.trans_dt) as trans_dt,
           |            count(1)                          as suctranscnt,
           |            sum(bill.bill_original_price)     as bill_original_price,
           |            sum(bill.bill_price)              as bill_price,
           |            count(distinct trans.cdhd_usr_id) as paysucusrcnt
           |        from
           |            hive_bill_order_trans trans
           |        left join
           |            hive_bill_sub_order_trans sub_trans
           |        on
           |            (
           |                trans.bill_order_id = sub_trans.bill_order_id)
           |        left join
           |            hive_ticket_bill_bas_inf bill
           |        on
           |            (
           |                sub_trans.bill_id=bill.bill_id)
           |        left join
           |            hive_card_bind_inf cbi
           |        on
           |            (
           |                concat(length(trans.card_no),trans.card_no) = cbi.bind_card_no)
           |        where
           |            bill.bill_sub_tp <> '08'
           |        and trans.order_st = '00'
           |        and trans.part_trans_dt >= '$start_dt'
           |        and trans.part_trans_dt <= '$end_dt'
           |        group by
           |            trim(if(cbi.iss_ins_cn_nm is null,'其他',cbi.iss_ins_cn_nm)),
           |            to_date(trans.trans_dt)) c
           |
           |on
           |    (
           |        a.iss_ins_cn_nm = c.iss_ins_cn_nm
           |    and a.trans_dt = c.trans_dt)
           |
          """.stripMargin)

      println(s"#### JOB_DM_52 spark sql 清洗数据完成时间为:" + DateUtils.getCurrentSystemTime())

      println(s"###JOB_DM_52------ results:" + results.count())
      if (!Option(results).isEmpty) {
        results.save2Mysql("DM_VAL_TKT_ACT_ISS_INS_DLY")
        println(s"#### JOB_DM_52 数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
      } else {
        println(s"#### JOB_DM_52 spark sql 清洗后数据无结果集！")
      }
    }
  }


  /**
    * JOB_DM_53 20170105
    * dm_val_tkt_act_mchnt_ind_dly->hive_bill_order_trans,hive_bill_sub_order_trans,hive_ticket_bill_bas_inf,hive_preferential_mchnt_inf
    * Code by Xue
    *
    * @param sqlContext
    * @return
    */
  def JOB_DM_53(implicit sqlContext: HiveContext, start_dt: String, end_dt: String, interval: Int) = {
    println("###JOB_DM_53(dm_val_tkt_act_mchnt_ind_dly->hive_bill_order_trans,hive_bill_sub_order_trans,hive_ticket_bill_bas_inf,hive_preferential_mchnt_inf)")
    DateUtils.timeCost("JOB_DM_53") {
      UPSQL_JDBC.delete(s"dm_val_tkt_act_mchnt_ind_dly", "report_dt", start_dt, end_dt)
      println("#### JOB_DM_53 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      println(s"#### JOB_DM_53  spark sql 清洗数据开始时间为:" + DateUtils.getCurrentSystemTime())
      sqlContext.sql(s"use $hive_dbname")

      println("开始创建临时表Spark_JOB_DM_53_A ")
      val JOB_DM_53_A =sqlContext.sql(
        s"""
           |select
           |if(mp.mchnt_para_cn_nm is null,'其他',mp.mchnt_para_cn_nm) as first_para_nm,
           |if(mp1.mchnt_para_cn_nm is null,'其他',mp1.mchnt_para_cn_nm) as second_para_nm,
           |            trans.trans_dt,
           |            count(1) as transcnt,
           |            count(distinct trans.cdhd_usr_id) as transusrcnt
           |        from
           |            hive_bill_order_trans trans
           |        left join
           |            hive_bill_sub_order_trans sub_trans
           |        on
           |            (
           |                trans.bill_order_id = sub_trans.bill_order_id)
           |        left join
           |            hive_ticket_bill_bas_inf bill
           |        on
           |            (
           |                sub_trans.bill_id=bill.bill_id)
           |        inner join
           |            (
           |                select
           |                    mchnt_cd,
           |                    max(third_party_ins_id) over (partition by mchnt_cd) as third_party_ins_id
           |                from
           |                    hive_store_term_relation) str
           |        on
           |            (
           |                trans.mchnt_cd = str.mchnt_cd)
           |        left join
           |            hive_preferential_mchnt_inf pmi
           |        on
           |            (
           |                str.third_party_ins_id = pmi.mchnt_cd)
           |        left join
           |            hive_mchnt_para mp
           |        on
           |            (
           |                pmi.mchnt_first_para = mp.mchnt_para_id)
           |        left join
           |            hive_mchnt_para mp1
           |        on
           |            (
           |                pmi.mchnt_second_para = mp1.mchnt_para_id)
           |        where
           |            bill.bill_sub_tp <> '08'
           |        and trans.part_trans_dt >= '$start_dt'
           |        and trans.part_trans_dt <= '$end_dt'
           |        group by
           |if(mp.mchnt_para_cn_nm is null,'其他',mp.mchnt_para_cn_nm),
           |if(mp1.mchnt_para_cn_nm is null,'其他',mp1.mchnt_para_cn_nm),
           |            trans.trans_dt
           """.stripMargin)
      JOB_DM_53_A.registerTempTable("spark_job_dm_53_a")
      println("Spark_JOB_DM_53_A 临时表创建成功")

      val JOB_DM_53_B =sqlContext.sql(
        s"""
           |select
           |if(mp.mchnt_para_cn_nm is null,'其他',mp.mchnt_para_cn_nm) as first_para_nm,
           |if(mp1.mchnt_para_cn_nm is null,'其他',mp1.mchnt_para_cn_nm) as second_para_nm,
           |            trans.trans_dt,
           |            count(distinct trans.cdhd_usr_id) as payusrcnt
           |        from
           |            hive_bill_order_trans trans
           |        left join
           |            hive_bill_sub_order_trans sub_trans
           |        on
           |            (
           |                trans.bill_order_id = sub_trans.bill_order_id)
           |        left join
           |            hive_ticket_bill_bas_inf bill
           |        on
           |            (
           |                sub_trans.bill_id=bill.bill_id)
           |        inner join
           |            (
           |                select
           |                    mchnt_cd,
           |                    max(third_party_ins_id) over (partition by mchnt_cd) as third_party_ins_id
           |                from
           |                    hive_store_term_relation) str
           |        on
           |            (
           |                trans.mchnt_cd = str.mchnt_cd)
           |        left join
           |            hive_preferential_mchnt_inf pmi
           |        on
           |            (
           |                str.third_party_ins_id = pmi.mchnt_cd)
           |        left join
           |            hive_mchnt_para mp
           |        on
           |            (
           |                pmi.mchnt_first_para = mp.mchnt_para_id)
           |        left join
           |            hive_mchnt_para mp1
           |        on
           |            (
           |                pmi.mchnt_second_para = mp1.mchnt_para_id)
           |        where
           |            bill.bill_sub_tp <> '08'
           |        and trans.order_st in ('00',
           |                               '01',
           |                               '02',
           |                               '03',
           |                               '04')
           |        and trans.part_trans_dt >= '$start_dt'
           |        and trans.part_trans_dt <= '$end_dt'
           |        group by
           |if(mp.mchnt_para_cn_nm is null,'其他',mp.mchnt_para_cn_nm),
           |if(mp1.mchnt_para_cn_nm is null,'其他',mp1.mchnt_para_cn_nm),
           |            trans.trans_dt
           """.stripMargin)
      JOB_DM_53_B.registerTempTable("spark_job_dm_53_b")

      val JOB_DM_53_C =sqlContext.sql(
        s"""
           |select
           |if(mp.mchnt_para_cn_nm is null,'其他',mp.mchnt_para_cn_nm) as first_para_nm,
           |if(mp1.mchnt_para_cn_nm is null,'其他',mp1.mchnt_para_cn_nm) as second_para_nm,
           |            trans.trans_dt,
           |            count(1)                          as suctranscnt,
           |            sum(bill.bill_original_price)     as bill_original_price,
           |            sum(bill.bill_price)              as bill_price,
           |            count(distinct trans.cdhd_usr_id) as paysucusrcnt
           |        from
           |            hive_bill_order_trans trans
           |        left join
           |            hive_bill_sub_order_trans sub_trans
           |        on
           |            (
           |                trans.bill_order_id = sub_trans.bill_order_id)
           |        left join
           |            hive_ticket_bill_bas_inf bill
           |        on
           |            (
           |                sub_trans.bill_id=bill.bill_id)
           |        inner join
           |            (
           |                select
           |                    mchnt_cd,
           |                    max(third_party_ins_id) over (partition by mchnt_cd) as third_party_ins_id
           |                from
           |                    hive_store_term_relation) str
           |        on
           |            (
           |                trans.mchnt_cd = str.mchnt_cd)
           |        left join
           |            hive_preferential_mchnt_inf pmi
           |        on
           |            (
           |                str.third_party_ins_id = pmi.mchnt_cd)
           |        left join
           |            hive_mchnt_para mp
           |        on
           |            (
           |                pmi.mchnt_first_para = mp.mchnt_para_id)
           |        left join
           |            hive_mchnt_para mp1
           |        on
           |            (
           |                pmi.mchnt_second_para = mp1.mchnt_para_id)
           |        where
           |            bill.bill_sub_tp <> '08'
           |        and trans.order_st = '00'
           |        and trans.part_trans_dt >= '$start_dt'
           |        and trans.part_trans_dt <= '$end_dt'
           |        group by
           |if(mp.mchnt_para_cn_nm is null,'其他',mp.mchnt_para_cn_nm),
           |if(mp1.mchnt_para_cn_nm is null,'其他',mp1.mchnt_para_cn_nm),
           |            trans.trans_dt
           """.stripMargin)
      JOB_DM_53_C.registerTempTable("spark_job_dm_53_c")

      val results = sqlContext.sql(
        s"""
           |select
           |    a.first_para_nm as first_ind_nm,
           |    a.second_para_nm as second_ind_nm,
           |    a.trans_dt as report_dt,
           |    a.transcnt as trans_cnt,
           |    c.suctranscnt as suc_trans_cnt,
           |    c.bill_original_price as bill_original_price,
           |    c.bill_price as bill_price,
           |    a.transusrcnt as trans_usr_cnt,
           |    b.payusrcnt as pay_usr_cnt,
           |    c.paysucusrcnt as pay_suc_usr_cnt
           |from
           |    spark_job_dm_53_a a
           |    left join spark_job_dm_53_b b
           |    on
           |    (
           |        a.first_para_nm = b.first_para_nm
           |    and a.second_para_nm = b.second_para_nm
           |    and a.trans_dt = b.trans_dt )
           |    left join spark_job_dm_53_c c
           |    on
           |    (
           |        a.first_para_nm = c.first_para_nm
           |    and a.second_para_nm = c.second_para_nm
           |    and a.trans_dt = c.trans_dt )
           |	where a.first_para_nm is not null and a.second_para_nm is not null
           | """.stripMargin)
      println(s"#### JOB_DM_53 spark sql 清洗数据完成时间为:" + DateUtils.getCurrentSystemTime())

      if (!Option(results).isEmpty) {
        results.save2Mysql("dm_val_tkt_act_mchnt_ind_dly")
        println(s"#### JOB_DM_53 数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
      } else {
        println(s"#### JOB_DM_53 spark sql 清洗数据无结果集！")
      }
    }
  }


  /**
    * JOB_DM_54/10-14
    * dm_val_tkt_act_mchnt_tp_dly->hive_bill_order_trans,hive_bill_sub_order_trans
    * Code by Xue
    *
    * @param sqlContext
    */
  def JOB_DM_54 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {
    println("###JOB_DM_54(dm_val_tkt_act_mchnt_tp_dly->hive_bill_order_trans,hive_bill_sub_order_trans)")
    DateUtils.timeCost("JOB_DM_54") {
      UPSQL_JDBC.delete(s"dm_val_tkt_act_mchnt_tp_dly","report_dt",s"$start_dt",s"$end_dt")
      println("#### JOB_DM_54 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      sqlContext.sql(s"use $hive_dbname")
      val results = sqlContext.sql(
        s"""
           |select
           |    a.grp_nm as mchnt_tp_grp,
           |    a.tp_nm as mchnt_tp,
           |    a.trans_dt as report_dt,
           |    a.transcnt as trans_cnt,
           |    c.suctranscnt as suc_trans_cnt,
           |    c.bill_original_price as bill_original_price,
           |    c.bill_price as bill_price,
           |    a.transusrcnt as trans_usr_cnt,
           |    b.payusrcnt as pay_usr_cnt,
           |    c.paysucusrcnt as pay_suc_usr_cnt
           |from
           |    (
           |        select
           |            tp_grp.mchnt_tp_grp_desc_cn as grp_nm,
           |            tp.mchnt_tp_desc_cn as tp_nm,
           |            trans.trans_dt as trans_dt,
           |            count(1)                    as transcnt,
           |            count(distinct trans.cdhd_usr_id) as transusrcnt
           |        from
           |            hive_bill_order_trans trans
           |        left join
           |            hive_bill_sub_order_trans sub_trans
           |        on trans.bill_order_id = sub_trans.bill_order_id
           |        inner join
           |             hive_mchnt_inf_wallet mchnt
           |        on
           |            trans.mchnt_cd=mchnt.mchnt_cd
           |        left join
           |            hive_mchnt_tp tp
           |        on
           |            mchnt.mchnt_tp=tp.mchnt_tp
           |        left join
           |            hive_mchnt_tp_grp tp_grp
           |        on
           |            tp.mchnt_tp_grp=tp_grp.mchnt_tp_grp
           |        left join
           |            hive_ticket_bill_bas_inf bill
           |        on  sub_trans.bill_id=bill.bill_id
           |        where
           |            bill.bill_sub_tp <> '08'
           |        and trans.trans_dt >= '$start_dt'
           |        and trans.trans_dt <= '$end_dt'
           |        group by
           |            tp_grp.mchnt_tp_grp_desc_cn,tp.mchnt_tp_desc_cn,trans.trans_dt
           |	) a
           |left join
           |    (
           |        select
           |            tp_grp.mchnt_tp_grp_desc_cn as grp_nm,
           |            tp.mchnt_tp_desc_cn as tp_nm,
           |            trans.trans_dt as trans_dt,
           |            count(distinct trans.cdhd_usr_id) as payusrcnt
           |        from
           |            hive_bill_order_trans trans
           |        left join
           |            hive_bill_sub_order_trans sub_trans
           |        on
           |            (
           |                trans.bill_order_id = sub_trans.bill_order_id)
           |        inner join
           |             hive_mchnt_inf_wallet mchnt
           |        on
           |            trans.mchnt_cd=mchnt.mchnt_cd
           |        left join
           |            hive_mchnt_tp tp
           |        on
           |            mchnt.mchnt_tp=tp.mchnt_tp
           |        left join
           |            hive_mchnt_tp_grp tp_grp
           |        on
           |            tp.mchnt_tp_grp=tp_grp.mchnt_tp_grp
           |        left join
           |            hive_ticket_bill_bas_inf bill
           |        on
           |            (
           |                sub_trans.bill_id=bill.bill_id)
           |        where
           |            bill.bill_sub_tp <> '08'
           |        and trans.order_st in ('00',
           |                               '01',
           |                               '02',
           |                               '03',
           |                               '04')
           |        and trans.trans_dt >='$start_dt'
           |        and trans.trans_dt <= '$end_dt'
           |        group by
           |            tp_grp.mchnt_tp_grp_desc_cn,
           |            tp.mchnt_tp_desc_cn,
           |            trans.trans_dt
           |	) b
           |on
           |    (a.grp_nm = b.grp_nm
           |    and a.tp_nm = b.tp_nm
           |    and a.trans_dt = b.trans_dt)
           |left join
           |    (
           |        select
           |            tp_grp.mchnt_tp_grp_desc_cn as grp_nm,
           |            tp.mchnt_tp_desc_cn as tp_nm,
           |            trans.trans_dt as trans_dt,
           |            count(1)                      as suctranscnt,
           |            sum(bill.bill_original_price) as bill_original_price,
           |            sum(bill.bill_price)          as bill_price,
           |            count(distinct trans.cdhd_usr_id)   as paysucusrcnt
           |        from
           |            hive_bill_order_trans trans
           |        left join
           |            hive_bill_sub_order_trans sub_trans
           |        on
           |            (trans.bill_order_id = sub_trans.bill_order_id)
           |        inner join
           |             hive_mchnt_inf_wallet mchnt
           |        on
           |            trans.mchnt_cd=mchnt.mchnt_cd
           |        left join
           |            hive_mchnt_tp tp
           |        on
           |            mchnt.mchnt_tp=tp.mchnt_tp
           |        left join
           |            hive_mchnt_tp_grp tp_grp
           |        on
           |            tp.mchnt_tp_grp=tp_grp.mchnt_tp_grp
           |        left join
           |            hive_ticket_bill_bas_inf bill
           |        on
           |            (sub_trans.bill_id=bill.bill_id)
           |        where
           |            bill.bill_sub_tp <> '08'
           |        and trans.order_st = '00'
           |        and trans.trans_dt >= '$start_dt'
           |        and trans.trans_dt <= '$end_dt'
           |        group by
           |            tp_grp.mchnt_tp_grp_desc_cn,tp.mchnt_tp_desc_cn,trans.trans_dt
           |	) c
           |on
           |    (a.grp_nm = c.grp_nm
           |    and a.tp_nm = c.tp_nm
           |    and a.trans_dt = c.trans_dt)
           | """.stripMargin)
      println(s"#### JOB_DM_54 spark sql 清洗[$start_dt -- $end_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())

      if (!Option(results).isEmpty) {
        results.save2Mysql("dm_val_tkt_act_mchnt_tp_dly")
        println(s"#### JOB_DM_54 [$start_dt -- $end_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
      } else {
        println(s"#### JOB_DM_54 spark sql 清洗[$start_dt -- $end_dt]数据无结果集！")
      }
    }
  }


  /**
    * JobName: JOB_DM_55
    * Feature: hive_prize_discount_result->dm_disc_act_branch_dly
    *
    * @author tzq
    * @time 2016-9-6
    * @param sqlContext ,start_dt,end_dt
    */
  def JOB_DM_55(implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {

    println("###JOB_DM_55-----JOB_DM_55(dm_disc_act_branch_dly->hive_prize_discount_result)")
    DateUtils.timeCost("JOB_DM_55"){
      UPSQL_JDBC.delete("dm_disc_act_branch_dly","report_dt",start_dt,end_dt);
      println("##### JOB_DM_55 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())

      sqlContext.sql(s"use $hive_dbname")
      println(s"#### JOB_DM_55 spark sql 清洗数据开始时间为:" + DateUtils.getCurrentSystemTime())

      val results=sqlContext.sql(
        s"""
           |select
           |    dbi.cup_branch_ins_id_nm as cup_branch_ins_id_nm,
           |    to_date(trans.settle_dt) as report_dt,
           |    count(1)                                 as trans_cnt,
           |    sum(trans.trans_pos_at)                  as trans_at,
           |    sum(trans.trans_pos_at - trans.trans_at) as discount_at
           |from
           |    hive_prize_discount_result trans
           |left join
           |    hive_discount_bas_inf dbi
           |on
           |    (
           |        trans.agio_app_id=dbi.loc_activity_id)
           |where
           |trans.prod_in = '0'
           |and trans.trans_id='S22'
           |and trans.part_settle_dt >= '$start_dt'
           |and trans.part_settle_dt <= '$end_dt'
           |group by
           |    dbi.cup_branch_ins_id_nm,
           |    to_date(trans.settle_dt)
        """.stripMargin)
      println(s"#### JOB_DM_55 spark sql 清洗数据完成时间为:" + DateUtils.getCurrentSystemTime())

      //println("###JOB_DM_55------results:"+results.count())
      if(!Option(results).isEmpty){
        results.save2Mysql("dm_disc_act_branch_dly")
        println(s"#### JOB_DM_55 数据插入完成时间为：" + DateUtils.getCurrentSystemTime())

      }else{
        println(s"#### JOB_DM_55 spark sql 清洗数据无结果集！")
      }
    }


  }



  /**
    * JOB_DM_56 20170106
    * dm_disc_act_mchnt_branch_dly->hive_prize_discount_result,hive_mchnt_inf_wallet,hive_branch_acpt_ins_inf
    * Code by Xue
    *
    * @param sqlContext
    * @return
    */
  def JOB_DM_56(implicit sqlContext: HiveContext, start_dt: String, end_dt: String, interval: Int) = {
    println("###JOB_DM_56(dm_disc_act_mchnt_branch_dly->hive_prize_discount_result,hive_mchnt_inf_wallet,hive_branch_acpt_ins_inf)")
    DateUtils.timeCost("JOB_DM_56") {
      UPSQL_JDBC.delete(s"dm_disc_act_mchnt_branch_dly", "report_dt", start_dt, end_dt)
      println("#### JOB_DM_56 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      println(s"#### JOB_DM_56  spark sql 清洗数据开始时间为:" + DateUtils.getCurrentSystemTime())
      sqlContext.sql(s"use $hive_dbname")
      val results = sqlContext.sql(
        s"""
           |select
           |if(acpt_ins.cup_branch_ins_id_nm is null,'总公司',acpt_ins.cup_branch_ins_id_nm) as cup_branch_ins_id_nm,
           |    trans.settle_dt as report_dt,
           |    count(1)                                 as trans_cnt,
           |    sum(trans.trans_pos_at)                  as trans_at,
           |    sum(trans.trans_pos_at - trans.trans_at) as discount_at
           |from
           |    hive_prize_discount_result trans
           |left join
           |    hive_mchnt_inf_wallet mchnt
           |on
           |    (
           |        trans.mchnt_cd=mchnt.mchnt_cd)
           |left join
           |    hive_branch_acpt_ins_inf acpt_ins
           |on
           |    (
           |        acpt_ins.ins_id_cd = concat('000',mchnt.acpt_ins_id_cd))
           |where
           |    trans.prod_in = '0'
           |and trans.part_settle_dt >= '$start_dt'
           |and trans.part_settle_dt <= '$end_dt'
           |group by
           |if(acpt_ins.cup_branch_ins_id_nm is null,'总公司',acpt_ins.cup_branch_ins_id_nm),
           |    trans.settle_dt
           | """.stripMargin)
      println(s"#### JOB_DM_56 spark sql 清洗数据完成时间为:" + DateUtils.getCurrentSystemTime())
      if (!Option(results).isEmpty) {
        results.save2Mysql("dm_disc_act_mchnt_branch_dly")
        println(s"#### JOB_DM_56 数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
      } else {
        println(s"#### JOB_DM_56 spark sql 清洗数据无结果集！")
      }
    }
  }




  /**
    * JOB_DM_57 20170106
    * dm_disc_act_mchnt_tp_dly->hive_prize_discount_result,hive_mchnt_inf_wallet,hive_mchnt_tp,hive_mchnt_tp_grp
    * Code by Xue
    *
    * @param sqlContext
    * @return
    */
  def JOB_DM_57(implicit sqlContext: HiveContext, start_dt: String, end_dt: String, interval: Int) = {
    println("###JOB_DM_57(dm_disc_act_mchnt_tp_dly->hive_prize_discount_result,hive_mchnt_inf_wallet,hive_mchnt_tp,hive_mchnt_tp_grp)")
    DateUtils.timeCost("JOB_DM_57") {
      UPSQL_JDBC.delete(s"dm_disc_act_mchnt_tp_dly", "report_dt", start_dt, end_dt)
      println("#### JOB_DM_57 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      println(s"#### JOB_DM_57  spark sql 清洗数据开始时间为:" + DateUtils.getCurrentSystemTime())
      sqlContext.sql(s"use $hive_dbname")
      val results = sqlContext.sql(
        s"""
           |select
           |if(tp_grp.mchnt_tp_grp_desc_cn is null,'其他',tp_grp.mchnt_tp_grp_desc_cn) as mchnt_tp_grp,
           |if(tp.mchnt_tp_desc_cn is null,'其他',tp.mchnt_tp_desc_cn) as mchnt_tp,
           |    trans.settle_dt as 	report_dt,
           |    count(1)                                 as trans_cnt,
           |    sum(trans.trans_pos_at)                  as trans_at,
           |    sum(trans.trans_pos_at - trans.trans_at) as discount_at
           |from
           |    hive_prize_discount_result trans
           |left join
           |    hive_mchnt_inf_wallet mchnt
           |on
           |    (
           |        trans.mchnt_cd=mchnt.mchnt_cd)
           |left join
           |    hive_mchnt_tp tp
           |on
           |    mchnt.mchnt_tp=tp.mchnt_tp
           |left join
           |    hive_mchnt_tp_grp tp_grp
           |on
           |    tp.mchnt_tp_grp=tp_grp.mchnt_tp_grp
           |where
           |    trans.prod_in = '0'
           |and trans.settle_dt >= '$start_dt'
           |and trans.settle_dt <= '$end_dt'
           |group by
           |if(tp_grp.mchnt_tp_grp_desc_cn is null,'其他',tp_grp.mchnt_tp_grp_desc_cn),
           |if(tp.mchnt_tp_desc_cn is null,'其他',tp.mchnt_tp_desc_cn),
           |    trans.settle_dt
           | """.stripMargin)
      println(s"#### JOB_DM_57 spark sql 清洗数据完成时间为:" + DateUtils.getCurrentSystemTime())
      if (!Option(results).isEmpty) {
        results.save2Mysql("dm_disc_act_mchnt_tp_dly")
        println(s"#### JOB_DM_57 数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
      } else {
        println(s"#### JOB_DM_57 spark sql 清洗数据无结果集！")
      }
    }
  }


  /**
    * JOB_DM_58 20170110
    * dm_disc_act_iss_ins_dly->hive_prize_discount_result,hive_card_bind_inf
    * Code by Xue
    *
    * @param sqlContext
    * @return
    */
  def JOB_DM_58(implicit sqlContext: HiveContext, start_dt: String, end_dt: String, interval: Int) = {
    println("###JOB_DM_58(dm_disc_act_iss_ins_dly->hive_prize_discount_result,hive_card_bind_inf)")
    DateUtils.timeCost("JOB_DM_58") {
      UPSQL_JDBC.delete(s"dm_disc_act_iss_ins_dly", "report_dt", start_dt, end_dt)
      println("#### JOB_DM_58 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      println(s"#### JOB_DM_58  spark sql 清洗数据开始时间为:" + DateUtils.getCurrentSystemTime())
      sqlContext.sql(s"use $hive_dbname")
      val results = sqlContext.sql(
        s"""
           |select
           |if(trim(cbi.iss_ins_cn_nm) is null,'其他',trim(cbi.iss_ins_cn_nm)) as iss_ins_cn_nm,
           |    trans.settle_dt as report_dt,
           |    count(1)                                 as trans_cnt,
           |    sum(trans.trans_pos_at)                  as trans_at,
           |    sum(trans.trans_pos_at - trans.trans_at) as discount_at
           |from
           |    hive_prize_discount_result trans
           |left join
           |    hive_card_bind_inf cbi
           |on
           |    (
           |        trans.pri_acct_no = cbi.bind_card_no)
           |where
           |    trans.prod_in = '0'
           |and trans.part_settle_dt >= '$start_dt'
           |and trans.part_settle_dt <= '$end_dt'
           |group by
           |if(trim(cbi.iss_ins_cn_nm) is null,'其他',trim(cbi.iss_ins_cn_nm)),
           |trans.settle_dt
           | """.stripMargin)
      println(s"#### JOB_DM_58 spark sql 清洗数据完成时间为:" + DateUtils.getCurrentSystemTime())
      if (!Option(results).isEmpty) {
        results.save2Mysql("dm_disc_act_iss_ins_dly")
        println(s"#### JOB_DM_58 数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
      } else {
        println(s"#### JOB_DM_58 spark sql 清洗数据无结果集！")
      }
    }
  }



  /**
    * JOB_DM_59 20170110
    * dm_disc_act_quick_pass_dly->hive_prize_discount_result,hive_discount_bas_inf,hive_filter_app_det,hive_filter_rule_det
    * Code by Xue
    *
    * @param sqlContext
    * @return
    */
  def JOB_DM_59(implicit sqlContext: HiveContext, start_dt: String, end_dt: String, interval: Int) = {
    println("###JOB_DM_59(dm_disc_act_quick_pass_dly->hive_prize_discount_result,hive_discount_bas_inf,hive_filter_app_det,hive_filter_rule_det)")
    DateUtils.timeCost("JOB_DM_59") {
      UPSQL_JDBC.delete(s"dm_disc_act_quick_pass_dly", "report_dt", start_dt, end_dt)
      println("#### JOB_DM_59 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      println(s"#### JOB_DM_59  spark sql 清洗数据开始时间为:" + DateUtils.getCurrentSystemTime())
      sqlContext.sql(s"use $hive_dbname")
      val results = sqlContext.sql(
        s"""
           |select
           |    case
           |        when dbi.loc_activity_id is not null
           |        then '仅限云闪付'
           |        else '非仅限云闪付'
           |    end as quick_pass_tp,
           |    trans.settle_dt as report_dt,
           |    count(1)                                 as trans_cnt,
           |    sum(trans.trans_pos_at)                  as trans_at,
           |    sum(trans.trans_pos_at - trans.trans_at) as discount_at
           |from
           |    hive_prize_discount_result trans
           |left join
           |    (
           |        select distinct
           |            a.loc_activity_id
           |        from
           |            (
           |                select distinct
           |                    bas.loc_activity_id
           |                from
           |                    hive_discount_bas_inf bas
           |                inner join
           |                    hive_filter_app_det app
           |                on
           |                    bas.loc_activity_id=app.loc_activity_id
           |                inner join
           |                    hive_filter_rule_det rule
           |                on
           |                    rule.rule_grp_id=app.rule_grp_id
           |                and rule.rule_grp_cata=app.rule_grp_cata
           |                where
           |                    app.rule_grp_cata='12'
           |                and rule_min_val in ('06',
           |                                     '07',
           |                                     '08') ) a
           |        left join
           |            (
           |                select distinct
           |                    bas.loc_activity_id
           |                from
           |                    hive_discount_bas_inf bas
           |                inner join
           |                    hive_filter_app_det app
           |                on
           |                    bas.loc_activity_id=app.loc_activity_id
           |                inner join
           |                    hive_filter_rule_det rule
           |                on
           |                    rule.rule_grp_id=app.rule_grp_id
           |                and rule.rule_grp_cata=app.rule_grp_cata
           |                where
           |                    app.rule_grp_cata='12'
           |                and rule_min_val in ('04',
           |                                     '05',
           |                                     '09') ) b
           |        on
           |            a.loc_activity_id=b.loc_activity_id
           |        where
           |            b.loc_activity_id is null) dbi
           |on
           |    (
           |        trans.agio_app_id=dbi.loc_activity_id)
           |where
           |    trans.prod_in = '0'
           |and trans.part_settle_dt >= '$start_dt'
           |and trans.part_settle_dt <= '$end_dt'
           |group by
           |    case
           |        when dbi.loc_activity_id is not null
           |        then '仅限云闪付'
           |        else '非仅限云闪付'
           |    end,
           |    trans.settle_dt
           | """.stripMargin)
      println(s"#### JOB_DM_59 spark sql 清洗数据完成时间为:" + DateUtils.getCurrentSystemTime())
      if (!Option(results).isEmpty) {
        results.save2Mysql("dm_disc_act_quick_pass_dly")
        println(s"#### JOB_DM_59 数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
      } else {
        println(s"#### JOB_DM_59 spark sql 清洗数据无结果集！")
      }
    }
  }


  /**
    * JOB_DM_60 20170113
    * dm_disc_tkt_act_acpt_ins_dly->hive_acc_trans,hive_ins_inf
    * Code by Xue
    *
    * @param sqlContext
    * @return
    */
  def JOB_DM_60(implicit sqlContext: HiveContext, start_dt: String, end_dt: String, interval: Int) = {
    println("###JOB_DM_60(dm_disc_tkt_act_acpt_ins_dly->hive_acc_trans,hive_ins_inf)")
    DateUtils.timeCost("JOB_DM_60") {
      UPSQL_JDBC.delete(s"dm_disc_tkt_act_acpt_ins_dly", "report_dt", start_dt, end_dt)
      println("#### JOB_DM_60 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      println(s"#### JOB_DM_60  spark sql 清洗数据开始时间为:" + DateUtils.getCurrentSystemTime())
      sqlContext.sql(s"use $hive_dbname")
      val results = sqlContext.sql(
        s"""
           |
           |select
           |    a.ins_cn_nm as acpt_ins_cn_nm,
           |    a.sys_settle_dt as report_dt,
           |    a.ins_id_cd as ins_id_cd,
           |    a.term_upgrade_cd as term_upgrade_cd,
           |    a.trans_norm_cd as norm_ver_cd,
           |    sum(a.transcnt) as trans_cnt,
           |    sum(b.suctranscnt) as suc_trans_cnt,
           |    sum(b.transat) as trans_at,
           |    sum(b.discountat) as discount_at,
           |    sum(b.transusrcnt) as trans_usr_cnt,
           |    sum(b.cardcnt) as trans_card_cnt
           |from
           |    (
           |        select
           |if(ins.ins_cn_nm is null,'其他',ins.ins_cn_nm) as ins_cn_nm,
           |            trans.sys_settle_dt,
           |            case
           |                when trans.fwd_ins_id_cd in ('00097310',
           |                                             '00093600',
           |                                             '00095210',
           |                                             '00098700',
           |                                             '00098500',
           |                                             '00097700',
           |                                             '00096400',
           |                                             '00096500',
           |                                             '00155800',
           |                                             '00095840',
           |                                             '00097000',
           |                                             '00085500',
           |                                             '00096900',
           |                                             '00093930',
           |                                             '00094200',
           |                                             '00093900',
           |                                             '00096100',
           |                                             '00092210',
           |                                             '00092220',
           |                                             '00092900',
           |                                             '00091600',
           |                                             '00092400',
           |                                             '00098800',
           |                                             '00098200',
           |                                             '00097900',
           |                                             '00091900',
           |                                             '00092600',
           |                                             '00091200',
           |                                             '00093320',
           |                                             '00031000',
           |                                             '00094500',
           |                                             '00094900',
           |                                             '00091100',
           |                                             '00094520',
           |                                             '00093000',
           |                                             '00093310')
           |                then '直连'
           |                else '间连'
           |            end ins_id_cd,
           |            case
           |                when trans.internal_trans_tp='C00022'
           |                or  trans.internal_trans_tp='C20022'
           |                then '改造'
           |                else '终端不改造'
           |            end term_upgrade_cd,
           |            case
           |                when trans.internal_trans_tp='C00022'
           |                then '1.0规范'
           |                when trans.internal_trans_tp='C20022'
           |                then '2.0规范'
           |                else ''
           |            end         trans_norm_cd,
           |            count(*) as transcnt
           |        from
           |            hive_acc_trans trans
           |        left join
           |            hive_ins_inf ins
           |        on
           |            trans.acpt_ins_id_cd=ins.ins_id_cd
           |        where
           |            trans.internal_trans_tp in ('C00022' ,
           |                                        'C20022',
           |                                        'C00023')
           |        and trans.sys_settle_dt >= '$start_dt'
           |        and trans.sys_settle_dt <= '$end_dt'
           |        group by
           |if(ins.ins_cn_nm is null,'其他',ins.ins_cn_nm),
           |            trans.sys_settle_dt,
           |            case
           |                when trans.fwd_ins_id_cd in ('00097310',
           |                                             '00093600',
           |                                             '00095210',
           |                                             '00098700',
           |                                             '00098500',
           |                                             '00097700',
           |                                             '00096400',
           |                                             '00096500',
           |                                             '00155800',
           |                                             '00095840',
           |                                             '00097000',
           |                                             '00085500',
           |                                             '00096900',
           |                                             '00093930',
           |                                             '00094200',
           |                                             '00093900',
           |                                             '00096100',
           |                                             '00092210',
           |                                             '00092220',
           |                                             '00092900',
           |                                             '00091600',
           |                                             '00092400',
           |                                             '00098800',
           |                                             '00098200',
           |                                             '00097900',
           |                                             '00091900',
           |                                             '00092600',
           |                                             '00091200',
           |                                             '00093320',
           |                                             '00031000',
           |                                             '00094500',
           |                                             '00094900',
           |                                             '00091100',
           |                                             '00094520',
           |                                             '00093000',
           |                                             '00093310')
           |                then '直连'
           |                else '间连'
           |            end,
           |            case
           |                when trans.internal_trans_tp='C00022'
           |                or  trans.internal_trans_tp='C20022'
           |                then '改造'
           |                else '终端不改造'
           |            end ,
           |            case
           |                when trans.internal_trans_tp='C00022'
           |                then '1.0规范'
           |                when trans.internal_trans_tp='C20022'
           |                then '2.0规范'
           |                else ''
           |            end ) a
           |left join
           |    (
           |        select
           |if(ins.ins_cn_nm is null,'其他',ins.ins_cn_nm) as ins_cn_nm,
           |            trans.sys_settle_dt,
           |            case
           |                when trans.fwd_ins_id_cd in ('00097310',
           |                                             '00093600',
           |                                             '00095210',
           |                                             '00098700',
           |                                             '00098500',
           |                                             '00097700',
           |                                             '00096400',
           |                                             '00096500',
           |                                             '00155800',
           |                                             '00095840',
           |                                             '00097000',
           |                                             '00085500',
           |                                             '00096900',
           |                                             '00093930',
           |                                             '00094200',
           |                                             '00093900',
           |                                             '00096100',
           |                                             '00092210',
           |                                             '00092220',
           |                                             '00092900',
           |                                             '00091600',
           |                                             '00092400',
           |                                             '00098800',
           |                                             '00098200',
           |                                             '00097900',
           |                                             '00091900',
           |                                             '00092600',
           |                                             '00091200',
           |                                             '00093320',
           |                                             '00031000',
           |                                             '00094500',
           |                                             '00094900',
           |                                             '00091100',
           |                                             '00094520',
           |                                             '00093000',
           |                                             '00093310')
           |                then '直连'
           |                else '间连'
           |            end ins_id_cd,
           |            case
           |                when trans.internal_trans_tp='C00022'
           |                or  trans.internal_trans_tp='C20022'
           |                then '改造'
           |                else '终端不改造'
           |            end term_upgrade_cd,
           |            case
           |                when trans.internal_trans_tp='C00022'
           |                then '1.0规范'
           |                when trans.internal_trans_tp='C20022'
           |                then '2.0规范'
           |                else ''
           |            end               trans_norm_cd,
           |            count(*)          as suctranscnt,
           |            sum(trans_tot_at) as transat,
           |            sum(trans.discount_at)          as discountat,
           |            count(distinct trans.cdhd_usr_id) as transusrcnt,
           |            count(distinct pri_acct_no)       as cardcnt
           |        from
           |            hive_acc_trans trans
           |        left join
           |            hive_ins_inf ins
           |        on
           |            trans.acpt_ins_id_cd=ins.ins_id_cd
           |        where
           |            trans.chswt_resp_cd='00'
           |        and trans.internal_trans_tp in ('C00022' ,
           |                                        'C20022',
           |                                        'C00023')
           |        and trans.sys_settle_dt >= '$start_dt'
           |        and trans.sys_settle_dt <= '$end_dt'
           |        group by
           |if(ins.ins_cn_nm is null,'其他',ins.ins_cn_nm),
           |            trans.sys_settle_dt,
           |            case
           |                when trans.fwd_ins_id_cd in ('00097310',
           |                                             '00093600',
           |                                             '00095210',
           |                                             '00098700',
           |                                             '00098500',
           |                                             '00097700',
           |                                             '00096400',
           |                                             '00096500',
           |                                             '00155800',
           |                                             '00095840',
           |                                             '00097000',
           |                                             '00085500',
           |                                             '00096900',
           |                                             '00093930',
           |                                             '00094200',
           |                                             '00093900',
           |                                             '00096100',
           |                                             '00092210',
           |                                             '00092220',
           |                                             '00092900',
           |                                             '00091600',
           |                                             '00092400',
           |                                             '00098800',
           |                                             '00098200',
           |                                             '00097900',
           |                                             '00091900',
           |                                             '00092600',
           |                                             '00091200',
           |                                             '00093320',
           |                                             '00031000',
           |                                             '00094500',
           |                                             '00094900',
           |                                             '00091100',
           |                                             '00094520',
           |                                             '00093000',
           |                                             '00093310')
           |                then '直连'
           |                else '间连'
           |            end,
           |            case
           |                when trans.internal_trans_tp='C00022'
           |                or  trans.internal_trans_tp='C20022'
           |                then '改造'
           |                else '终端不改造'
           |            end ,
           |            case
           |                when trans.internal_trans_tp='C00022'
           |                then '1.0规范'
           |                when trans.internal_trans_tp='C20022'
           |                then '2.0规范'
           |                else ''
           |            end ) b
           |on a.ins_cn_nm=b.ins_cn_nm
           |and a.sys_settle_dt = b.sys_settle_dt
           |and a.ins_id_cd = b.ins_id_cd
           |and a.term_upgrade_cd = b.term_upgrade_cd
           |and a.trans_norm_cd = b.trans_norm_cd
           |group by
           |    a.ins_cn_nm,
           |    a.sys_settle_dt,
           |    a.ins_id_cd ,
           |    a.term_upgrade_cd ,
           |    a.trans_norm_cd
           | """.stripMargin)
      println(s"#### JOB_DM_60 spark sql 清洗数据完成时间为:" + DateUtils.getCurrentSystemTime())
      if (!Option(results).isEmpty) {
        results.save2Mysql("dm_disc_tkt_act_acpt_ins_dly")
        println(s"#### JOB_DM_60 数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
      } else {
        println(s"#### JOB_DM_60 spark sql 清洗数据无结果集！")
      }
    }
  }


  /**
    * JobName: JOB_DM_61
    * Feature: hive_cdhd_cashier_maktg_reward_dtl -> dm_cashier_cup_red_branch
    *
    * @author YangXue
    * @time 2016-09-05
    * @param sqlContext,start_dt,end_dt
    */
  def JOB_DM_61(implicit sqlContext: HiveContext, start_dt: String, end_dt: String, interval: Int) = {
    println("#### JOB_DM_61(hive_cdhd_cashier_maktg_reward_dtl -> dm_cashier_cup_red_branch)")

    DateUtils.timeCost("JOB_DM_61") {
      UPSQL_JDBC.delete("dm_cashier_cup_red_branch", "report_dt", start_dt, end_dt)
      println("#### JOB_DM_61 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())

      var today_dt = start_dt
      if (interval >= 0) {
        sqlContext.sql(s"use $hive_dbname")
        for (i <- 0 to interval) {
          println(s"#### JOB_DM_61 spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())
          val results = sqlContext.sql(
            s"""
               |select cup_branch_ins_id_nm as branch_nm,
               |'$today_dt' as report_dt,
               |count(case when to_date(settle_dt)>=trunc('$today_dt','YYYY') and to_date(settle_dt)<='$today_dt' then 1 end) as years_cnt,
               |sum(case when to_date(settle_dt)>=trunc('$today_dt','YYYY') and  to_date(settle_dt)<='$today_dt' then reward_point_at else 0 end) as years_at,
               |count(case when to_date(settle_dt)='$today_dt' then 1  end)  as today_cnt,
               |sum(case when to_date(settle_dt)='$today_dt' then reward_point_at else 0 end) as today_at
               |from hive_cdhd_cashier_maktg_reward_dtl
               |where rec_st='2' and activity_tp='004'
               |group by cup_branch_ins_id_nm
             """.stripMargin)
          println(s"#### JOB_DM_61 spark sql 清洗[$today_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())
          //println(s"#### JOB_DM_61 spark sql 清洗[$today_dt]数据 results:"+results.count())

          if (!Option(results).isEmpty) {
            results.save2Mysql("dm_cashier_cup_red_branch")
            println(s"#### JOB_DM_61 [$today_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
          } else {
            println(s"#### JOB_DM_61 spark sql 清洗[$today_dt]数据无结果集！")
          }
          today_dt = DateUtils.addOneDay(today_dt)
        }
      }
    }
  }

  /**
    * JobName: JOB_DM_62
    * Feature: dm_usr_auther_nature_tie_card --> hive_card_bind_inf
    *
    * @author tzq
    * @time 2016-9-6
    * @param sqlContext,start_dt,end_dt,interval
    */
  def JOB_DM_62(implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("###JOB_DM_62-----JOB_DM_62(dm_usr_auther_nature_tie_card --> hive_card_bind_inf)")

    DateUtils.timeCost("JOB_DM_62"){
      UPSQL_JDBC.delete("dm_usr_auther_nature_tie_card","report_dt",start_dt,end_dt)
      println("##### JOB_DM_62 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())

      var today_dt=start_dt
      if(interval>=0 ){
        sqlContext.sql(s"use $hive_dbname")
        for(i <- 0 to interval){
          println(s"#### JOB_DM_62 spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())
          val results=sqlContext.sql(
            s"""
               |select
               |(case when card_auth_st='0' then   '未认证'
               |  when card_auth_st='1' then   '支付认证'
               |  when card_auth_st='2' then   '可信认证'
               |  when card_auth_st='3' then   '可信+支付认证'
               | else '--' end) as card_auth_nm,
               |card_attr as card_attr ,
               |'$today_dt' as report_dt ,
               |count(case when to_date(bind_ts) = '$today_dt'  then 1 end)  as tpre,
               |count(case when to_date(bind_ts) <= '$today_dt'  then 1 end)  as total
               |
               |from  (
               |select  card_auth_st,bind_ts,substr(bind_card_no,1,8) as card_bin
               |from hive_card_bind_inf where card_bind_st='0') a
               |left join
               |(select card_attr,card_bin from hive_card_bin ) b
               |on a.card_bin=b.card_bin
               |group by (case when card_auth_st='0' then   '未认证'
               |  when card_auth_st='1' then   '支付认证'
               |  when card_auth_st='2' then   '可信认证'
               |  when card_auth_st='3' then   '可信+支付认证'
               | else '--' end),card_attr
               |
            """.stripMargin)
          println(s"#### JOB_DM_62 spark sql 清洗[$today_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())
          println(s"###JOB_DM_62------$today_dt results:"+results.count())

          if(!Option(results).isEmpty){
            results.save2Mysql("dm_usr_auther_nature_tie_card")
            println(s"#### JOB_DM_62 [$today_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
          }else{
            println(s"#### JOB_DM_62 spark sql 清洗[$today_dt]数据无结果集！")
          }
          today_dt=DateUtils.addOneDay(today_dt)
        }
      }
    }
  }

  /**
    * JOB_DM_63/10-14
    * DM_LIFE_SERVE_BUSINESS_TRANS->HIVE_LIFE_TRANS
    * Code by Xue
    *
    * @param sqlContext
    * @return
    */
  def JOB_DM_63 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("###JOB_DM_63(dm_life_serve_business_trans->hive_life_trans)")
    DateUtils.timeCost("JOB_DM_63") {
      UPSQL_JDBC.delete(s"dm_life_serve_business_trans", "report_dt", start_dt, end_dt)
      println("#### JOB_DM_63 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      var today_dt = start_dt
      if (interval >= 0) {
        sqlContext.sql(s"use $hive_dbname")
        for (i <- 0 to interval) {
          println(s"#### JOB_DM_63 spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())
          val results = sqlContext.sql(
            s"""
               |select
               |nvl(a.buss_tp_nm,'--') as buss_tp_nm,
               |nvl(a.chnl_tp_nm,'--') as chnl_tp_nm,
               |a.trans_dt as report_dt,
               |sum(b.tran_all_cnt) as tran_all_cnt,
               |sum(a.tran_succ_cnt) as tran_succ_cnt,
               |sum(a.trans_succ_at) as trans_succ_at
               |from(
               |select
               |buss_tp_nm,
               |chnl_tp_nm,
               |to_date(trans_dt) as trans_dt,
               |count(trans_no) as tran_succ_cnt,
               |sum(trans_at) as trans_succ_at
               |from hive_life_trans
               |where proc_st ='00'
               |and to_date(trans_dt)>='$today_dt' and   to_date(trans_dt)<='$today_dt'
               |group by buss_tp_nm,chnl_tp_nm,to_date(trans_dt)) a
               |left join
               |(
               |select
               |buss_tp_nm,
               |chnl_tp_nm,
               |to_date(trans_dt) as trans_dt,
               |count(trans_no) as tran_all_cnt
               |from hive_life_trans
               |where  to_date(trans_dt)>='$today_dt' and   to_date(trans_dt)<='$today_dt'
               |group by buss_tp_nm,chnl_tp_nm,to_date(trans_dt)) b
               |on a.buss_tp_nm=b.buss_tp_nm and a.buss_tp_nm=b.buss_tp_nm and a.trans_dt=b.trans_dt
               |group by a.buss_tp_nm,
               |a.chnl_tp_nm,
               |a.trans_dt
               | """.stripMargin)
          println(s"#### JOB_DM_63 spark sql 清洗[$today_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())

          if (!Option(results).isEmpty) {
            results.save2Mysql("dm_life_serve_business_trans")
            println(s"#### JOB_DM_63 [$today_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime()
            )
          } else {
            println(s"#### JOB_DM_63 spark sql 清洗[$today_dt]数据无结果集！")
          }
          today_dt = DateUtils.addOneDay(today_dt)
        }
      }
    }
  }


  /**
    * JOB_DM_64 20161219
    * dm_coupon_pub_down_if_hce->hive_download_trans,hive_ticket_bill_bas_inf
    * Code by Xue
    *
    * @param sqlContext
    * @return
    */
  def JOB_DM_64(implicit sqlContext: HiveContext, start_dt: String, end_dt: String, interval: Int) = {
    println("###JOB_DM_64(dm_coupon_pub_down_if_hce->hive_download_trans,hive_ticket_bill_bas_inf)")
    DateUtils.timeCost("JOB_DM_64") {
      UPSQL_JDBC.delete(s"dm_coupon_pub_down_if_hce", "report_dt", start_dt, end_dt)
      println("#### JOB_DM_64 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      var today_dt = start_dt
      if (interval >= 0) {
        println(s"#### JOB_DM_64  spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())
        sqlContext.sql(s"use $hive_dbname")
        for (i <- 0 to interval) {
          val results = sqlContext.sql(
            s"""
               |select
               |a.if_hce as if_hce,
               |'$today_dt' as report_dt,
               |sum(a.coupon_class) as class_tpre_add_num,
               |sum(a.coupon_publish) as amt_tpre_add_num ,
               |sum(a.dwn_num) as dowm_tpre_add_num ,
               |sum(b.batch) as batch_tpre_add_num
               |from
               |(select
               |case when substr(udf_fld,31,2) in ('01','02','03','04','05') then '仅限云闪付' else '非仅限云闪付' end as if_hce,
               |count(*) as coupon_class ,
               |sum(case when dwn_total_num = -1 then dwn_num else dwn_total_num end) as coupon_publish ,
               |sum(dwn_num) as dwn_num
               |from hive_download_trans as dtl,
               |hive_ticket_bill_bas_inf as bill
               |where dtl.bill_id=bill.bill_id
               |and dtl.um_trans_id in ('12','17')
               |and dtl.trans_st='1' and part_trans_dt='$today_dt' and dtl.bill_nm not like '%机场%' and dtl.bill_nm not like '%住两晚送一晚%'
               |and bill.bill_sub_tp in ('01','03') and bill.bill_nm not like '%测试%' and bill.bill_nm not like '%验证%' and bill.bill_id <>'Z00000000020415'
               |and bill.bill_id<>'Z00000000020878' and bill.bill_nm not like '%满2元减1%' and bill.bill_nm not like '%满2分减1分%'
               |and bill.bill_nm not like '%满2减1%' and bill.bill_nm not like '%满2抵1%' and bill.bill_nm not like '测%' and dwn_total_num-dwn_num<100000
               |group by case when substr(udf_fld,31,2) in ('01','02','03','04','05') then '仅限云闪付' else '非仅限云闪付' end )a
               |left join
               |(
               |select
               |case when substr(udf_fld,31,2) in ('01','02','03','04','05') then '仅限云闪付' else '非仅限云闪付' end as if_hce,
               |sum(adj.adj_ticket_bill) as batch
               |from
               |hive_ticket_bill_acct_adj_task adj
               |inner join
               |(select cup_branch_ins_id_cd,bill.bill_id,udf_fld from hive_download_trans as dtl,
               |hive_ticket_bill_bas_inf as bill
               |where dtl.bill_id=bill.bill_id
               |and dtl.um_trans_id in ('12','17')
               |and dtl.trans_st='1' and part_trans_dt='$today_dt' and dtl.bill_nm not like '%机场%' and dtl.bill_nm not like '%住两晚送一晚%'
               |and bill.bill_sub_tp in ('01','03') and bill.bill_nm not like '%测试%' and bill.bill_nm not like '%验证%' and bill.bill_id <>'Z00000000020415'
               |and bill.bill_id<>'Z00000000020878' and bill.bill_nm not like '%满2元减1%' and bill.bill_nm not like '%满2分减1分%'
               |and bill.bill_nm not like '%满2减1%' and bill.bill_nm not like '%满2抵1%' and bill.bill_nm not like '测%' and dwn_total_num-dwn_num<100000
               |) b
               |on adj.bill_id=b.bill_id
               |group by case when substr(udf_fld,31,2) in ('01','02','03','04','05') then '仅限云闪付' else '非仅限云闪付' end )b
               |on a.if_hce=b.if_hce
               |group by a.if_hce,'$today_dt'
               | """.stripMargin)
          println(s"#### JOB_DM_64 spark sql 清洗[$today_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())

          if (!Option(results).isEmpty) {
            results.save2Mysql("dm_coupon_pub_down_if_hce")
            println(s"#### JOB_DM_64 [$today_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
          } else {
            println(s"#### JOB_DM_64 spark sql 清洗[$today_dt]数据无结果集！")
          }
          today_dt = DateUtils.addOneDay(today_dt)
        }
      }
    }
  }


  /**
    * JOB_DM_65/10-14
    * DM_HCE_COUPON_TRAN->HIVE_TICKET_BILL_BAS_INF
    * Code by Xue
    *
    * @param sqlContext
    * @return
    */
  def JOB_DM_65 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("###JOB_DM_65(dm_hce_coupon_tran->hive_ticket_bill_bas_inf)")

    DateUtils.timeCost("JOB_DM_65") {
      UPSQL_JDBC.delete(s"dm_hce_coupon_tran","report_dt",start_dt,end_dt)
      println("#### JOB_DM_65 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())

      var today_dt=start_dt
      if(interval>=0 ){
        sqlContext.sql(s"use $hive_dbname")
        for(i <- 0 to interval){
          println(s"#### JOB_DM_65 spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())
          val results = sqlContext.sql(
            s"""
               |select
               |tempe.cup_branch_ins_id_nm as branch_nm,
               |'$today_dt' as report_dt,
               |tempe.dwn_total_num as year_release_num,
               |tempe.dwn_num as year_down_num,
               |tempc.accept_year_num as year_vote_against_num,
               |tempc.accept_today_num as today_vote_against_num
               |from
               |(
               |select
               |tempa.cup_branch_ins_id_nm,
               |count(*),
               |sum(dwn_total_num) as dwn_total_num,
               |sum(dwn_num)  as  dwn_num
               |from
               |(
               |select
               |bill_id,
               |bill_nm,
               |cup_branch_ins_id_nm,
               |(case when dwn_total_num=-1 then dwn_num else dwn_total_num end) as dwn_total_num,dwn_num
               |from hive_ticket_bill_bas_inf
               |where to_date(valid_begin_dt)<='$today_dt' and to_date(valid_end_dt) >= trunc('$today_dt','YYYY')
               |and  exclusive_in ='1' and  bill_nm not like '%机场%' and bill_nm not like '%住两晚送一晚%' and
               |      bill_nm not like '%测试%'         and
               |      bill_nm not like '%验证%'         and
               |      bill_nm not like '%满2元减1%'     and
               |      bill_nm not like '%满2分减1分%'   and
               |      bill_nm not like '%满2减1%'       and
               |      bill_nm not like '%满2抵1%'       and
               |      bill_nm not like '测%'            and
               |      bill_nm not like '%test%'         and
               |      bill_id <>'Z00000000020415'       and
               |      bill_id<>'Z00000000020878'        and
               |      length(trim(cup_branch_ins_id_cd))<>0         and
               |      dwn_total_num<>0                  and
               |      dwn_num>=0                        and
               |      length(trim(translate(trim(bill_nm),'-0123456789','')))<>0
               |      ) tempa
               |      group by tempa.cup_branch_ins_id_nm
               |	  ) tempe
               |left join
               |
               |(
               |select
               |tempb.cup_branch_ins_id_nm as cup_branch_ins_id_nm,
               |count(case when to_date(tempd.trans_dt)>=trunc('$today_dt','YYYY') and to_date(tempd.trans_dt)<='$today_dt' then tempd.bill_id end) as accept_year_num,
               |count(case when  to_date(tempd.trans_dt) ='$today_dt' then tempd.bill_id end) as accept_today_num
               |from
               |(
               |select
               |bill_id,
               |trans_dt,
               |substr(udf_fld,31,2) as cfp_sign
               |from  hive_acc_trans
               |where substr(udf_fld,31,2) not in ('',' ','  ','00') and
               |      um_trans_id in ('AC02000065','AC02000063') and
               |      buss_tp in ('04','05','06')
               |      and sys_det_cd='S' and
               |       bill_nm not like '%机场%'         and
               |       bill_nm not like '%住两晚送一晚%' and
               |       bill_nm not like '%测试%'         and
               |       bill_nm not like '%验证%'         and
               |       bill_nm not like '%满2元减1%'     and
               |       bill_nm not like '%满2分减1分%'   and
               |       bill_nm not like '%满2减1%'       and
               |       bill_nm not like '%满2抵1%'       and
               |       bill_nm not like '测%'            and
               |       bill_nm not like '%test%'
               |      ) tempd
               |left join
               |(
               |select
               |bill_id,
               |bill_nm,
               |cup_branch_ins_id_nm
               |from hive_ticket_bill_bas_inf
               |)
               |tempb
               |on tempd.bill_id = tempb.bill_id
               |group by
               |tempb.cup_branch_ins_id_nm
               |)
               |tempc
               |on tempe.cup_branch_ins_id_nm=tempc.cup_branch_ins_id_nm
               | """.stripMargin)
          println(s"#### JOB_DM_65 spark sql 清洗[$today_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())

          if (!Option(results).isEmpty) {
            results.save2Mysql("dm_hce_coupon_tran")
            println(s"#### JOB_DM_65 [$today_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime()
            )
          } else {
            println(s"#### JOB_DM_65 spark sql 清洗[$today_dt]数据无结果集！")
          }
          today_dt=DateUtils.addOneDay(today_dt)
        }
      }
    }
  }



  /**
    * JobName: JOB_DM_66
    * Feature: hive_acc_trans+hive_ticket_bill_bas_inf->dm_coupon_cfp_tran
    *
    * @author tzq
    * @time 2016-9-7
    * @param sqlContext,start_dt,end_dt,interval
    */
  def JOB_DM_66(implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("#### JOB_DM_66(dm_coupon_cfp_tran->hive_acc_trans+hive_ticket_bill_bas_inf)")

    DateUtils.timeCost("JOB_DM_66"){
      UPSQL_JDBC.delete("dm_coupon_cfp_tran","report_dt",start_dt,end_dt)
      println("##### JOB_DM_66 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      var today_dt=start_dt
      if(interval>=0 ){
        sqlContext.sql(s"use $hive_dbname")
        for(i <- 0 to interval){
          println(s"#### JOB_DM_66 spark sql 清洗数据开始时间为:" + DateUtils.getCurrentSystemTime())
          val results=sqlContext.sql(
            s"""
               |select
               |cup_branch_ins_id_nm as branch_nm,
               |'$today_dt' as report_dt,
               |cfp_sign as  cfp_sign ,
               |count(case when to_date(trans_dt) >=trunc('$today_dt','YYYY') and to_date(trans_dt) <='$today_dt' then a.bill_id end) as year_tran_num,
               |count(case when to_date(trans_dt) ='$today_dt' then a.bill_id end) as today_tran_num
               |from
               |(select
               | bill_id, trans_dt,
               | case when substr(udf_fld,31,2)='01'  then 'HCE'
               |      when substr(udf_fld,31,2)='02'  then 'Apple Pay'
               |      when substr(udf_fld,31,2)in ('03','04')  then '三星pay'
               |      when substr(udf_fld,31,2)='05'  then 'IC卡挥卡'
               |      when substr(udf_fld,31,2)='06'  then '华为pay'
               |      when substr(udf_fld,31,2)='07'  then '小米pay'
               | else '其它' end  as cfp_sign
               |from hive_acc_trans
               | where substr(udf_fld,31,2) not in ('',' ','  ','00') and
               |       um_trans_id in ('AC02000065','AC02000063') and
               |       buss_tp in ('04','05','06')
               |       and sys_det_cd='S' and
               |        bill_nm not like '%机场%'         and
               |        bill_nm not like '%住两晚送一晚%' and
               |        bill_nm not like '%测试%'         and
               |        bill_nm not like '%验证%'         and
               |        bill_nm not like '%满2元减1%'     and
               |        bill_nm not like '%满2分减1分%'   and
               |        bill_nm not like '%满2减1%'       and
               |        bill_nm not like '%满2抵1%'       and
               |        bill_nm not like '测%'            and
               |        bill_nm not like '%test%'
               |       ) a
               | left join
               |(
               |   select bill_id, bill_nm,cup_branch_ins_id_nm from hive_ticket_bill_bas_inf
               | ) b
               | on a.bill_id = b.bill_id
               | group by  cup_branch_ins_id_nm,cfp_sign
               |
      """.stripMargin)
          println(s"#### JOB_DM_66 spark sql 清洗数据完成时间为:" + DateUtils.getCurrentSystemTime())
          println(s"###JOB_DM_66------$today_dt results:"+results.count())

          if(!Option(results).isEmpty){
            results.save2Mysql("dm_coupon_cfp_tran")
            println(s"#### JOB_DM_66 数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
          }else{
            println(s"#### JOB_DM_66 spark sql 清洗数据无结果集！")
          }
          today_dt=DateUtils.addOneDay(today_dt)
        }
      }
    }

  }


  /**
    * JobName: JOB_DM_67
    * Feature: hive_acc_trans,hive_offline_point_trans,hive_passive_code_pay_trans,hive_download_trans,
    *          hive_switch_point_trans,hive_prize_discount_result,hive_discount_bas_inf
    *          -> dm_o2o_trans_dly
    *
    * @author YangXue
    * @time 2016-09-12
    * @param sqlContext,start_dt,end_dt
    */
  def JOB_DM_67(implicit sqlContext: HiveContext, start_dt: String, end_dt: String) = {
    println("#### JOB_DM_67(hive_acc_trans,hive_offline_point_trans,hive_passive_code_pay_trans,hive_download_trans,hive_switch_point_trans,hive_prize_discount_result,hive_discount_bas_inf -> dm_user_card_nature)")

    DateUtils.timeCost("JOB_DM_67") {
      UPSQL_JDBC.delete("dm_o2o_trans_dly", "report_dt", start_dt, end_dt)
      println("#### JOB_DM_67 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())

      sqlContext.sql(s"use $hive_dbname")
      val results = sqlContext.sql(
        s"""
           |select
           |    t.cup_branch_ins_id_nm,
           |    t.report_dt,
           |    sum(t.coupon_cnt)        as coupon_cnt,
           |    sum(t.up_point_cnt)      as up_point_cnt,
           |    sum(t.offline_point_cnt) as offline_point_cnt,
           |    sum(t.online_point_cnt)  as online_point_cnt,
           |    sum(code_pay_cnt)        as code_pay_cnt,
           |    sum(t.swt_point_cnt)     as swt_point_cnt,
           |    sum(t.disc_cnt)          as disc_cnt,
           |    sum(o2o_cnt)             as o2o_cnt,
           |    sum(coupon_down_num)     as coupon_down_num,
           |    sum(coupon_acpt_num)     as coupon_acpt_num,
           |    sum(coupon_orig_at)      as coupon_orig_at,
           |    sum(coupon_disc_at)      as coupon_disc_at
           |from
           |    (
           |        select
           |            case
           |                when a.cup_branch_ins_id_nm is null
           |                then
           |                    case
           |                        when b.cup_branch_ins_id_nm is null
           |                        then
           |                            case
           |                                when c.cup_branch_ins_id_nm is null
           |                                then
           |                                    case
           |                                        when d.cup_branch_ins_id_nm is null
           |                                        then
           |                                            case
           |                                                when e.cup_branch_ins_id_nm is null
           |                                                then
           |                                                    case
           |                                                        when f.cup_branch_ins_id_nm is null
           |                                                        then
           |                                                            case
           |                                                                when g.cup_branch_ins_id_nm is null
           |                                                                then
           |                                                                    case
           |                                                                        when h.cup_branch_ins_id_nm
           |                                                                            is null
           |                                                                        then '总公司'
           |                                                                        else h.cup_branch_ins_id_nm
           |                                                                    end
           |                                                                else g.cup_branch_ins_id_nm
           |                                                            end
           |                                                        else f.cup_branch_ins_id_nm
           |                                                    end
           |                                                else e.cup_branch_ins_id_nm
           |                                            end
           |                                        else d.cup_branch_ins_id_nm
           |                                    end
           |                                else c.cup_branch_ins_id_nm
           |                            end
           |                        else b.cup_branch_ins_id_nm
           |                    end
           |                else a.cup_branch_ins_id_nm
           |            end as cup_branch_ins_id_nm,
           |            case
           |                when a.trans_dt is null
           |                then
           |                    case
           |                        when b.trans_dt is null
           |                        then
           |                            case
           |                                when c.trans_dt is null
           |                                then
           |                                    case
           |                                        when d.trans_dt is null
           |                                        then
           |                                            case
           |                                                when e.trans_dt is null
           |                                                then
           |                                                    case
           |                                                        when f.trans_dt is null
           |                                                        then
           |                                                            case
           |                                                                when g.trans_dt is null
           |                                                                then
           |                                                                    case
           |                                                                        when h.trans_dt is not null
           |                                                                        then h.trans_dt
           |                                                                    end
           |                                                                else g.trans_dt
           |                                                            end
           |                                                        else f.trans_dt
           |                                                    end
           |                                                else e.trans_dt
           |                                            end
           |                                        else d.trans_dt
           |                                    end
           |                                else c.trans_dt
           |                            end
           |                        else b.trans_dt
           |                    end
           |                else a.trans_dt
           |            end                                   as report_dt,
           |            if(a.trans_cnt is null,0,a.trans_cnt) as coupon_cnt,
           |            if(b.trans_cnt is null,0,b.trans_cnt) as up_point_cnt,
           |            if(c.trans_cnt is null,0,c.trans_cnt) as offline_point_cnt,
           |            if(d.trans_cnt is null,0,d.trans_cnt) as online_point_cnt,
           |            if(e.trans_cnt is null,0,e.trans_cnt) as code_pay_cnt,
           |            if(g.trans_cnt is null,0,g.trans_cnt) as swt_point_cnt,
           |            if(h.trans_cnt is null,0,h.trans_cnt) as disc_cnt,
           |            if(a.trans_cnt is null,0,a.trans_cnt) + if(b.trans_cnt is null,0,b.trans_cnt) + if
           |            (c.trans_cnt is null,0,c.trans_cnt) + if(d.trans_cnt is null,0,d.trans_cnt) + if
           |            (e.trans_cnt is null,0, e.trans_cnt) + if(g.trans_cnt is null,0,g.trans_cnt) + if
           |            (h.trans_cnt is null,0,h.trans_cnt)         as o2o_cnt,
           |            if(f.download_num is null,0,f.download_num) as coupon_down_num,
           |            if(a.accept_num is null,0, a.accept_num)    as coupon_acpt_num,
           |            if(a.orig_at is null,0, a.orig_at)          as coupon_orig_at,
           |            if(a.discount_at is null,0, a.discount_at)  as coupon_disc_at
           |        from
           |            (
           |                select
           |                    bill.cup_branch_ins_id_nm,
           |                    to_date(trans.trans_dt) as trans_dt,
           |                    count(1) as trans_cnt,
           |                    count(1) as accept_num,
           |                    sum((
           |                            case
           |                                when discount_at is null
           |                                then 0
           |                                else discount_at
           |                            end) + (
           |                            case
           |                                when trans_at is null
           |                                then 0
           |                                else trans_at
           |                            end )) as orig_at,
           |                    sum(
           |                        case
           |                            when discount_at is null
           |                            then 0
           |                            else discount_at
           |                        end) as discount_at
           |                from
           |                    hive_acc_trans trans
           |                left join
           |                    hive_ticket_bill_bas_inf bill
           |                on
           |                    trans.bill_id=bill.bill_id
           |                where
           |                    trans.um_trans_id in ('AC02000065',
           |                                          'AC02000063')
           |                and trans.buss_tp in ('04',
           |                                      '05',
           |                                      '06')
           |                and trans.sys_det_cd='S'
           |                and trans.part_trans_dt >= '$start_dt'
           |                and trans.part_trans_dt <= '$end_dt'
           |                group by
           |                    bill.cup_branch_ins_id_nm,
           |                    to_date(trans.trans_dt)) a
           |        full outer join
           |            (
           |                select
           |                    ins.cup_branch_ins_id_nm,
           |                    to_date(trans.trans_dt) as trans_dt,
           |                    count(*) as trans_cnt
           |                from
           |                    hive_acc_trans trans
           |                left join
           |                    hive_ins_inf ins
           |                on
           |                    trans.acpt_ins_id_cd=concat('000',ins.ins_id_cd)
           |                where
           |                    trans.um_trans_id in ('AC02000065',
           |                                          'AC02000063')
           |                and trans.buss_tp = '02'
           |                and trans.sys_det_cd='S'
           |                and trans.part_trans_dt >= '$start_dt'
           |                and trans.part_trans_dt <= '$end_dt'
           |                group by
           |                    ins.cup_branch_ins_id_nm,
           |                    to_date(trans.trans_dt)) b
           |        on
           |            (
           |                a.cup_branch_ins_id_nm = b.cup_branch_ins_id_nm
           |            and a.trans_dt = b.trans_dt)
           |        full outer join
           |            (
           |                select
           |                    trans.cup_branch_ins_id_nm as cup_branch_ins_id_nm,
           |                    to_date(trans.trans_dt) as trans_dt,
           |                    count(*)                as trans_cnt
           |                from
           |                    hive_offline_point_trans trans
           |                where
           |                    trans.part_trans_dt >= '$start_dt'
           |                and trans.part_trans_dt <= '$end_dt'
           |                and trans.oper_st in('0' ,
           |                                     '3')
           |                and trans.point_at>0
           |                and UM_TRANS_ID in('AD00000002','AD00000003','AD00000007')
           |                group by
           |                    trans.cup_branch_ins_id_nm,
           |                    to_date(trans.trans_dt)) c
           |        on
           |            (
           |                a.cup_branch_ins_id_nm = c.cup_branch_ins_id_nm
           |            and a.trans_dt = c.trans_dt)
           |        full outer join
           |            (
           |                select
           |                    trans.cup_branch_ins_id_nm,
           |                    to_date(trans.trans_dt) as trans_dt,
           |                    count(*) as trans_cnt
           |                from
           |                    hive_online_point_trans trans
           |                where
           |                    trans.status = '1'
           |                and trans.part_trans_dt >= '$start_dt'
           |                and trans.part_trans_dt <= '$end_dt'
           |                group by
           |                    trans.cup_branch_ins_id_nm,
           |                    to_date(trans.trans_dt)) d
           |        on
           |            (
           |                a.cup_branch_ins_id_nm = d.cup_branch_ins_id_nm
           |            and a.trans_dt = d.trans_dt)
           |        full outer join
           |            (
           |                select
           |                    mchnt.cup_branch_ins_id_nm,
           |                    to_date(trans.trans_dt) as trans_dt,
           |                    count(*) as trans_cnt
           |                from
           |                    hive_passive_code_pay_trans trans
           |                left join
           |                    hive_mchnt_inf_wallet mchnt
           |                on
           |                    trans.mchnt_cd=mchnt.mchnt_cd
           |                where
           |                    trans.mchnt_cd is not null
           |                and trans.mchnt_cd<>''
           |                and trans_st='04'
           |                and trans.part_trans_dt >= '$start_dt'
           |                and trans.part_trans_dt <= '$end_dt'
           |                group by
           |                    mchnt.cup_branch_ins_id_nm,
           |                    to_date(trans.trans_dt)) e
           |        on
           |            (
           |                a.cup_branch_ins_id_nm = e.cup_branch_ins_id_nm
           |            and a.trans_dt = e.trans_dt)
           |        full outer join
           |            (
           |                select
           |                    bill.cup_branch_ins_id_nm,
           |                    to_date(a.trans_dt) as trans_dt,
           |                    count(1) as download_num
           |                from
           |                    hive_download_trans a
           |                inner join
           |                    hive_ticket_bill_bas_inf bill
           |                on
           |                    (
           |                        a.bill_id = bill.bill_id)
           |                where
           |                    a.buss_tp in ('04',
           |                                  '05',
           |                                  '06')
           |                and a.trans_st='1'
           |                and a.part_trans_dt >= '$start_dt'
           |                and a.part_trans_dt <= '$end_dt'
           |                group by
           |                    bill.cup_branch_ins_id_nm,
           |                    to_date(a.trans_dt)) f
           |        on
           |            (
           |                a.cup_branch_ins_id_nm = f.cup_branch_ins_id_nm
           |            and a.trans_dt = f.trans_dt)
           |        full outer join
           |            (
           |                select
           |                    ins.cup_branch_ins_id_nm as cup_branch_ins_id_nm,
           |                    to_date(swt.trans_dt)             as trans_dt,
           |                    sum(
           |                        case
           |                            when substr(swt.trans_st,1,1)='1'
           |                            then 1
           |                            else 0
           |                        end ) as trans_cnt
           |                from
           |                    hive_switch_point_trans swt
           |                left join
           |                    hive_ins_inf ins
           |                on
           |                    swt.rout_ins_id_cd=ins.ins_id_cd
           |                where
           |                    swt.part_trans_dt >= '$start_dt'
           |                and swt.part_trans_dt <= '$end_dt'
           |                and swt.rout_ins_id_cd<>'00250002'
           |                and swt.rout_ins_id_cd not like '0016%'
           |                group by
           |                    ins.cup_branch_ins_id_nm,
           |                    to_date(swt.trans_dt))g
           |        on
           |            (
           |                a.cup_branch_ins_id_nm = g.cup_branch_ins_id_nm
           |            and a.trans_dt = g.trans_dt)
           |        full outer join
           |            (
           |                select
           |                    dbi.cup_branch_ins_id_nm as cup_branch_ins_id_nm,
           |                    to_date(trans.settle_dt)          as trans_dt,
           |                    sum (
           |                        case
           |                            when trans.trans_id in ('V52',
           |                                                    'R22',
           |                                                    'V50',
           |                                                    'R20',
           |                                                    'S30')
           |                            then -1
           |                            else 1
           |                        end) as trans_cnt
           |                from
           |                    hive_prize_discount_result trans,
           |                    hive_discount_bas_inf dbi
           |                where
           |                    trans.agio_app_id=dbi.loc_activity_id
           |                and trans.agio_app_id is not null
           |                and trans.trans_id='S22'
           |                and trans.part_settle_dt >= '$start_dt'
           |                and trans.part_settle_dt <= '$end_dt'
           |                group by
           |                    dbi.cup_branch_ins_id_nm,
           |                    to_date(trans.settle_dt)) h
           |        on
           |            (
           |                a.cup_branch_ins_id_nm = h.cup_branch_ins_id_nm
           |            and a.trans_dt = h.trans_dt)) t
           |group by
           |    t.cup_branch_ins_id_nm,
           |    t.report_dt
           |
      """.stripMargin)

      println(s"#### JOB_DM_67 spark sql 清洗[$start_dt -- $end_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())
      //println(s"#### JOB_DM_67 spark sql 清洗[$start_dt -- $end_dt]数据 results:"+results.count())

      if (!Option(results).isEmpty) {
        results.save2Mysql("dm_o2o_trans_dly")
        println(s"#### JOB_DM_67 [$start_dt -- $end_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
      } else {
        println(s"#### JOB_DM_67 spark sql 清洗[$start_dt -- $end_dt]数据无结果集！")
      }
    }
  }


  /**
    * JobName: JOB_DM_68
    * Feature: hive_offline_point_trans -> dm_offline_point_trans_dly
    *
    * @author YangXue
    * @time 2016-09-05
    * @param sqlContext,start_dt,end_dt
    */
  def JOB_DM_68(implicit sqlContext: HiveContext, start_dt: String, end_dt: String) = {
    println("#### JOB_DM_68(hive_offline_point_trans -> dm_offline_point_trans_dly)")

    DateUtils.timeCost("JOB_DM_68") {
      UPSQL_JDBC.delete("dm_offline_point_trans_dly", "report_dt", start_dt, end_dt)
      println("#### JOB_DM_68 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())

      sqlContext.sql(s"use $hive_dbname")
      val results = sqlContext.sql(
        s"""
           |select
           |    cup_branch_ins_id_nm,
           |    to_date(trans_dt) as report_dt,
           |    count(distinct(plan_id)) as plan_cnt,
           |    count(*)                as trans_cnt,
           |    sum(
           |        case
           |            when um_trans_id in('AD00000002','AD00000003','AD00000007')
           |            then point_at
           |            else 0
           |        end) as point_at,
           |    sum(
           |        case
           |            when um_trans_id in('AD00000004','AD00000005','AD00000006')
           |            then bill_num
           |            else 0
           |        end) as bill_num
           |from
           |    hive_offline_point_trans  trans
           |where
           |  trans.part_trans_dt >='$start_dt' and trans.part_trans_dt <='$end_dt' and
           |   oper_st in('0','3')
           |group by
           |    cup_branch_ins_id_nm,
           |    to_date(trans_dt)
      """.stripMargin)
      println(s"#### JOB_DM_68 spark sql 清洗[$start_dt -- $end_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())
      //println(s"#### JOB_DM_68 spark sql 清洗[$start_dt -- $end_dt]数据 results:"+results.count())

      if (!Option(results).isEmpty) {
        results.save2Mysql("dm_offline_point_trans_dly")
        println(s"#### JOB_DM_68 [$start_dt -- $end_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
      } else {
        println(s"#### JOB_DM_68 spark sql 清洗[$start_dt -- $end_dt]数据无结果集！")
      }
    }
  }

  /**
    * JobName: JOB_DM_69
    * Feature: hive_ticket_bill_bas_inf+hive_acc_trans->dm_disc_tkt_act_dly
    *
    * @author tzq
    * @time 2016-9-1
    * @param sqlContext,start_dt,end_dt
    */
  def JOB_DM_69(implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {
    println("###JOB_DM_69(hive_ticket_bill_bas_inf+hive_acc_trans->dm_disc_tkt_act_dly)")

    DateUtils.timeCost("JOB_DM_69"){

      UPSQL_JDBC.delete("dm_disc_tkt_act_dly","report_dt",start_dt,end_dt);
      println( "#### JOB_DM_69 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())

      sqlContext.sql(s"use $hive_dbname")

      println(s"#### JOB_DM_69 spark sql 清洗数据开始时间为:" + DateUtils.getCurrentSystemTime())
      val results=sqlContext.sql(
        s"""
           |select
           |    a.bill_id as bill_id,
           |    a.bill_nm as bill_nm,
           |    a.trans_dt as report_dt,
           |    a.plh_num  as rlse_num,
           |    a.dwn_num  as dnld_num,
           |    a.acpt_cnt as acct_cnt,
           |    a.disc_cnt as disc_at,
           |    a.acpt_rate as acct_rate
           |from
           |    (
           |        select
           |            trans_data.bill_id            as bill_id,
           |            bill_data.bill_nm             as bill_nm,
           |            trans_data.trans_dt           as trans_dt,
           |            bill_data.dwn_total_num       as plh_num,
           |            bill_data.dwn_num             as dwn_num,
           |            trans_data.concnt             as acpt_cnt,
           |            trans_data.discount_at        as disc_cnt,
           |            case when bill_data.dwn_num is null or bill_data.dwn_num = 0
           |            then 0
           |            else
           |            round(trans_data.concnt/bill_data.dwn_num * 100,2)
           |            end  as acpt_rate,
           |            row_number() over(partition by trans_data.trans_dt order by trans_data.concnt desc) as
           |            rn
           |        from
           |            (
           |                select
           |                    bill_id,
           |                    bill_nm,
           |                    dwn_total_num,
           |                    dwn_num,
           |                    to_date(valid_begin_dt) as valid_begin_dt,
           |                    to_date(valid_end_dt) as valid_end_dt
           |                from
           |                    hive_ticket_bill_bas_inf
           |                where
           |                    bill_st in('1', '2')
           |            ) bill_data
           |        left join
           |            (
           |                select
           |                    bill_id,
           |                    to_date(trans_dt) as trans_dt,
           |                    count(*) as concnt,
           |                    sum(
           |                        case
           |                            when discount_at is null
           |                            then 0
           |                            else discount_at
           |                        end) as discount_at
           |                from
           |                    hive_acc_trans
           |                where
           |                    bill_id like 'D%'
           |                and um_trans_id in ('AC02000065','AC02000063')
           |                and buss_tp in ('04', '05', '06')
           |               and part_trans_dt >= '$start_dt'
           |               and part_trans_dt <= '$end_dt'
           |
         |                and sys_det_cd='S'
           |                group by
           |                    bill_id,to_date(trans_dt))  trans_data
           |        on
           |            (
           |                bill_data.bill_id = trans_data.bill_id)
           |        where
           |            trans_data.concnt is not null
           |            and bill_data.valid_begin_dt<= trans_data.trans_dt
           |            and bill_data.valid_end_dt>= trans_data.trans_dt
           |       ) a
           |where
           |    a.rn <= 10
           |
      """.stripMargin)
      println(s"#### JOB_DM_69 spark sql 清洗数据完成时间为:" + DateUtils.getCurrentSystemTime())
      println("###JOB_DM_69------results:"+results.count())
      if(!Option(results).isEmpty){
        results.save2Mysql("dm_disc_tkt_act_dly")
        println(s"#### JOB_DM_69 数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
      }else{
        println(s"#### JOB_DM_69 spark sql 清洗数据无结果集！")
      }
    }
  }

  /**
    * JobName: JOB_DM_70
    * Feature: hive_ticket_bill_bas_inf+hive_acc_trans->dm_elec_tkt_act_dly
    *
    * @author tzq
    * @time 2016-8-30
    * @param sqlContext,start_dt,end_dt
    */
  def JOB_DM_70(implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {
    println("###JOB_DM_70(hive_ticket_bill_bas_inf+hive_acc_trans->dm_elec_tkt_act_dly)")
    DateUtils.timeCost("JOB_DM_70"){
      UPSQL_JDBC.delete("dm_elec_tkt_act_dly","report_dt",start_dt,end_dt);
      println( "#### JOB_DM_70 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      sqlContext.sql(s"use $hive_dbname")

      println(s"#### JOB_DM_70 spark sql 清洗数据开始时间为:" + DateUtils.getCurrentSystemTime())
      val results=sqlContext.sql(
        s"""
           |
         |select
           |    a.bill_id  as bill_id,
           |    a.bill_nm  as bill_nm,
           |    a.trans_dt as report_dt,
           |    a.plh_num  as rlse_num,
           |    a.dwn_num  as dnld_num,
           |    a.acpt_cnt as acct_cnt,
           |    a.disc_cnt as disc_at,
           |    a.acpt_rate as acct_rate
           |from
           |    (
           |        select
           |            trans_data.bill_id                                 as bill_id,
           |            bill_data.bill_nm                                  as bill_nm,
           |            trans_data.trans_dt                                as trans_dt,
           |            bill_data.dwn_total_num                            as plh_num,
           |            bill_data.dwn_num                                  as dwn_num,
           |            trans_data.concnt                                  as acpt_cnt,
           |            trans_data.discount_at                             as disc_cnt,
           |            case when bill_data.dwn_num is null or bill_data.dwn_num = 0
           |            then 0
           |            else
           |            round(trans_data.concnt/bill_data.dwn_num * 100,2)
           |            end  as acpt_rate,
           |            row_number() over(partition by trans_data.trans_dt order by trans_data.concnt desc) as rn
           |        from
           |            (
           |                select
           |                    bill_id,
           |                    bill_nm,
           |                    dwn_total_num,
           |                    dwn_num,
           |                    to_date(valid_begin_dt) as valid_begin_dt,
           |                    to_date(valid_end_dt) as valid_end_dt
           |                from
           |                    hive_ticket_bill_bas_inf
           |                where
           |                    bill_st in('1', '2')
           |
         |            ) bill_data
           |        left join
           |            (
           |                select
           |                    bill_id,
           |                    to_date(trans_dt) as trans_dt,
           |                    count(*) as concnt,
           |                    sum(
           |                        case
           |                            when discount_at is null
           |                            then 0
           |                            else discount_at
           |                        end) as discount_at
           |                from
           |                    hive_acc_trans
           |                where
           |                    bill_id like 'E%'
           |                and um_trans_id in ('AC02000065', 'AC02000063')
           |                and buss_tp in ('04', '05','06')
           |               and part_trans_dt >= '$start_dt'
           |               and part_trans_dt <= '$end_dt'
           |                and sys_det_cd='S'
           |                group by bill_id, to_date(trans_dt)
           |            ) as trans_data
           |        on
           |            (
           |            bill_data.bill_id = trans_data.bill_id
           |            )
           |        where
           |            trans_data.concnt is not null
           |            and bill_data.valid_begin_dt<= trans_data.trans_dt
           |            and bill_data.valid_end_dt>= trans_data.trans_dt
           |    ) a
           |where
           |    a.rn <= 10
           |
      """.stripMargin)
      println(s"#### JOB_DM_70 spark sql 清洗数据完成时间为:" + DateUtils.getCurrentSystemTime())
      println("###JOB_DM_70------results:"+results.count())
      if(!Option(results).isEmpty){
        results.save2Mysql("dm_elec_tkt_act_dly")
        println(s"#### JOB_DM_70 数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
      }else{
        println(s"#### JOB_DM_70 spark sql 清洗数据无结果集！")
      }
    }

  }

  /**
    * JobName:JOB_DM_71
    * Feature:hive_ticket_bill_bas_inf+hive_acc_trans->dm_vchr_tkt_act_dly
    *
    * @author tzq
    * @time 2016-8-31
    * @param sqlContext,start_dt,end_dt
    */
  def JOB_DM_71(implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {

    println("###JOB_DM_71(hive_ticket_bill_bas_inf+hive_acc_trans->dm_vchr_tkt_act_dly)")

    DateUtils.timeCost("JOB_DM_71"){

      UPSQL_JDBC.delete("dm_vchr_tkt_act_dly","report_dt",start_dt,end_dt);
      println( "#### JOB_DM_71 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())

      sqlContext.sql(s"use $hive_dbname")
      println(s"#### JOB_DM_71 spark sql 清洗数据开始时间为:" + DateUtils.getCurrentSystemTime())
      val results=sqlContext.sql(
        s"""
           |select
           |    a.bill_id  as bill_id,
           |    a.bill_nm  as bill_nm,
           |    a.trans_dt  as report_dt,
           |    a.plh_num  as rlse_num,
           |    a.dwn_num  as dnld_num,
           |    a.acpt_cnt  as acct_cnt,
           |    a.disc_cnt  as disc_at,
           |    a.acpt_rate   as acct_rate
           |from
           |    (
           |        select
           |            trans_data.bill_id            as bill_id,
           |            bill_data.bill_nm             as bill_nm,
           |            trans_data.trans_dt           as trans_dt,
           |            bill_data.dwn_total_num       as plh_num,
           |            bill_data.dwn_num             as dwn_num,
           |            trans_data.concnt             as acpt_cnt,
           |            trans_data.discount_at        as disc_cnt,
           |            case when bill_data.dwn_num is null or bill_data.dwn_num = 0
           |            then 0
           |            else
           |            round(trans_data.concnt/bill_data.dwn_num * 100,2)
           |            end  as acpt_rate,
           |            row_number() over(partition by trans_data.trans_dt order by trans_data.concnt desc) as
           |            rn
           |        from
           |            (
           |                select
           |                    bill_id,
           |                    bill_nm,
           |                    dwn_total_num,
           |                    dwn_num,
           |                   to_date(valid_begin_dt) as valid_begin_dt,
           |                    to_date(valid_end_dt) as valid_end_dt
           |                from
           |                    hive_ticket_bill_bas_inf
           |                where
           |                    bill_st in('1','2')
           |
         |            ) bill_data
           |        left join
           |            (
           |                select
           |                    bill_id,
           |                    to_date(trans_dt) as trans_dt,
           |                    count(*) as concnt,
           |                    sum(
           |                        case
           |                            when discount_at is null
           |                            then 0
           |                            else discount_at
           |                        end) as discount_at
           |                from
           |                    hive_acc_trans
           |                where
           |                    bill_id like 'Z%'
           |               and um_trans_id in ('AC02000065','AC02000063')
           |               and buss_tp in ('04', '05', '06')
           |               and part_trans_dt >= '$start_dt'
           |               and part_trans_dt <= '$end_dt'
           |               and sys_det_cd='S'
           |                group by bill_id,to_date(trans_dt)) as trans_data
           |        on
           |            (
           |                bill_data.bill_id = trans_data.bill_id)
           |        where
           |            trans_data.concnt is not null
           |        and bill_data.valid_begin_dt<= trans_data.trans_dt
           |        and bill_data.valid_end_dt>= trans_data.trans_dt) a
           |where
           |    a.rn <= 10
           |
      """.stripMargin)
      println(s"#### JOB_DM_71 spark sql 清洗数据完成时间为:" + DateUtils.getCurrentSystemTime())

      println("###JOB_DM_71------results:"+results.count())
      if(!Option(results).isEmpty){
        results.save2Mysql("dm_vchr_tkt_act_dly")
        println(s"#### JOB_DM_71 数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
      }else{
        println(s"#### JOB_DM_71 spark sql 清洗数据无结果集！")
      }
    }
  }


  /**
    * JobName: JOB_DM_72
    * Feature: hive_offline_point_trans -> dm_offline_point_act_dly
    *
    * @author YangXue
    * @time 2016-09-01
    * @param sqlContext,start_dt,end_dt
    */
  def JOB_DM_72(implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {
    println("#### JOB_DM_72(hive_offline_point_trans -> dm_offline_point_act_dly)")

    DateUtils.timeCost("JOB_DM_72"){
      UPSQL_JDBC.delete("dm_offline_point_act_dly","report_dt",start_dt,end_dt)
      println("#### JOB_DM_72 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())

      sqlContext.sql(s"use $hive_dbname")
      val results = sqlContext.sql(
        s"""
           |select
           |    b.plan_id,
           |    b.plan_nm,
           |    b.acpt_addup_bat_dt as report_dt,
           |    b.trans_cnt,
           |    b.point_at,
           |    b.bill_num
           |from
           |    (
           |        select
           |            a.plan_id,
           |            a.plan_nm,
           |            a.acpt_addup_bat_dt,
           |            a.trans_cnt,
           |            a.point_at,
           |            a.bill_num,
           |            row_number() over(partition by to_date(a.acpt_addup_bat_dt) order by a.bill_num desc) as rn
           |        from
           |            (
           |                select
           |                    trans.plan_id,
           |                    regexp_replace(trans.plan_nm,' ','') as plan_nm,
           |                    to_date(trans.acct_addup_bat_dt) as acpt_addup_bat_dt,
           |                    count(*) as trans_cnt,
           |                    sum(
           |                        case
           |                            when trans.um_trans_id in('AD00000002',
           |                                                'AD00000003',
           |                                                'AD00000007')
           |                            then trans.point_at
           |                            else 0
           |                        end) as point_at,
           |                    sum(
           |                        case
           |                            when trans.um_trans_id in('AD00000004',
           |                                                'AD00000005',
           |                                                'AD00000006')
           |                            then trans.bill_num
           |                            else 0
           |                        end) as bill_num
           |                from
           |                    hive_offline_point_trans trans
           |                where
           |                		trans.part_trans_dt >= '$start_dt' and trans.part_trans_dt <= '$end_dt' and
           |                		trans.oper_st in('0', '3') and
           |                		to_date(trans.acct_addup_bat_dt) >= '$start_dt' and to_date(trans.acct_addup_bat_dt) <= '$end_dt'
           |                    and trans.plan_id is not null
           |                group by
           |                    trans.plan_id,
           |                    regexp_replace(trans.plan_nm,' ',''),to_date(trans.acct_addup_bat_dt))a) b
           |        where
           |            b.rn <= 10
      """.stripMargin)
      println(s"#### JOB_DM_72 spark sql 清洗[$start_dt -- $end_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())
      //println(s"#### JOB_DM_72 spark sql 清洗[$start_dt -- $end_dt]数据 results:"+results.count())

      if(!Option(results).isEmpty){
        results.save2Mysql("dm_offline_point_act_dly")
        println(s"#### JOB_DM_72 [$start_dt -- $end_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
      }else{
        println(s"#### JOB_DM_72 spark sql 清洗[$start_dt -- $end_dt]数据无结果集！")
      }
    }
  }


  /**
    * JOB_DM_73/10-14
    * dm_prize_act_branch_dly->hive_prize_activity_bas_inf,hive_prize_lvl,hive_prize_discount_result
    * Code by Xue
    *
    * @param sqlContext
    * @return
    */
  def JOB_DM_73 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {
    println("#### JOB_DM_73(dm_prize_act_branch_dly->hive_prize_activity_bas_inf,hive_prize_lvl,hive_prize_discount_result)")

    DateUtils.timeCost("JOB_DM_73") {
      UPSQL_JDBC.delete(s"dm_prize_act_branch_dly","report_dt",s"$start_dt",s"$end_dt")
      println("#### JOB_DM_73 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      sqlContext.sql(s"use $hive_dbname")
      val results = sqlContext.sql(
        s"""
           |select
           |    ta.cup_branch_ins_id_nm as cup_branch_ins_id_nm,
           |    ta.settle_dt as report_dt,
           |    max(ta.activity_num) as activity_num,
           |    max(ta.plan_num)     as plan_num,
           |    max(ta.actual_num)   as actual_num
           |from
           |(
           |        select
           |            a.cup_branch_ins_id_nm as cup_branch_ins_id_nm,
           |            a.settle_dt as settle_dt,
           |            c.activity_num as activity_num,
           |            a.plan_num as plan_num,
           |            b.actual_num as actual_num
           |        from
           |            (
           |                select
           |                    if(prize.cup_branch_ins_id_nm is null,'总公司',prize.cup_branch_ins_id_nm)  as cup_branch_ins_id_nm,
           |                    rslt.settle_dt as settle_dt,
           |                    sum(lvl.lvl_prize_num) as plan_num
           |                from
           |                    hive_prize_activity_bas_inf prize,
           |                    hive_prize_lvl lvl,
           |                    (
           |                        select distinct
           |                            settle_dt
           |                        from
           |                            hive_prize_discount_result
           |                        where
           |                            part_settle_dt >= '$start_dt'
           |                        and part_settle_dt <= '$end_dt') rslt
           |                where
           |                    prize.loc_activity_id = lvl.loc_activity_id
           |                and prize.activity_begin_dt<= rslt.settle_dt
           |                and prize.activity_end_dt>=rslt.settle_dt
           |                and prize.run_st!='3'
           |                group by
           |                    if(prize.cup_branch_ins_id_nm is null,'总公司',prize.cup_branch_ins_id_nm),
           |                    rslt.settle_dt
           |   ) a,
           |            (
           |                select
           |                    prize.cup_branch_ins_id_nm as cup_branch_ins_id_nm,
           |                    pr.settle_dt as settle_dt,
           |                    count(pr.sys_tra_no_conv) as actual_num
           |                from
           |                    hive_prize_activity_bas_inf prize,
           |                    hive_prize_bas bas,
           |                    hive_prize_discount_result pr
           |                where
           |                    prize.loc_activity_id = bas.loc_activity_id
           |                and bas.prize_id = pr.prize_id
           |                and prize.activity_begin_dt<= pr.settle_dt
           |                and prize.activity_end_dt>= pr.settle_dt
           |                and pr.trans_id='S22'
           |                and pr.part_settle_dt >= '$start_dt'
           |                and pr.part_settle_dt <= '$end_dt'
           |                and pr.trans_id not like 'V%'
           |                and prize.run_st!='3'
           |                group by
           |                    prize.cup_branch_ins_id_nm,pr.settle_dt
           |   ) b,
           |            (
           |                select
           |                    prize.cup_branch_ins_id_nm as cup_branch_ins_id_nm,
           |                    rslt.settle_dt as settle_dt,
           |                    count(*) as activity_num
           |                from
           |                    hive_prize_activity_bas_inf prize,
           |                    (
           |                        select distinct
           |                            settle_dt
           |                        from
           |                            hive_prize_discount_result
           |                        where
           |                            part_settle_dt >= '$start_dt'
           |                        and part_settle_dt <= '$end_dt') rslt
           |                where
           |                    prize.activity_begin_dt<= rslt.settle_dt
           |                and prize.activity_end_dt>=rslt.settle_dt
           |                and prize.run_st!='3'
           |                group by
           |                    prize.cup_branch_ins_id_nm,
           |                    rslt.settle_dt
           |   ) c
           |        where
           |            a.cup_branch_ins_id_nm=b.cup_branch_ins_id_nm
           |        and b.cup_branch_ins_id_nm=c.cup_branch_ins_id_nm
           |        and a.settle_dt = b.settle_dt
           |        and b.settle_dt = c.settle_dt
           |
           |union all
           |
           |select
           |distinct
           |d.ins_cn_nm as cup_branch_ins_id_nm,
           |rslt.settle_dt as settle_dt,
           |0 as activity_num,
           |0 as plan_num,
           |0 as actual_num
           |
           |from
           |hive_ins_inf d,
           |(
           |select distinct
           |settle_dt
           |from
           |hive_prize_discount_result
           |where
           |part_settle_dt >='$start_dt'and part_settle_dt <='$end_dt') rslt
           |where trim(d.ins_cn_nm) like '%中国银联股份有限公司%分公司'   or trim(d.ins_cn_nm) like '%信息中心'
           |)
           |ta
           |group by
           |ta.cup_branch_ins_id_nm,ta.settle_dt
           |
         | """.stripMargin)

      println(s"#### JOB_DM_73 spark sql 清洗[$start_dt -- $end_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())
      if (!Option(results).isEmpty) {
        results.save2Mysql("dm_prize_act_branch_dly")
        println(s"#### JOB_DM_73 [$start_dt -- $end_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
      } else {
        println(s"#### JOB_DM_73 spark sql 清洗[$start_dt -- $end_dt]数据无结果集！")
      }
    }
  }



  /**
    * JOB_DM_74/10-14
    * dm_prize_act_dly->hive_prize_activity_bas_inf,hive_prize_lvl,hive_prize_bas,hive_prize_discount_result
    * Code by Xue
    *
    * @param sqlContext
    * @return
    */
  def JOB_DM_74 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {
    println("#### JOB_DM_74(dm_prize_act_dly->hive_prize_activity_bas_inf,hive_prize_lvl,hive_prize_bas,hive_prize_discount_result)")

    DateUtils.timeCost("JOB_DM_74") {
      UPSQL_JDBC.delete(s"DM_PRIZE_ACT_DLY","REPORT_DT",s"$start_dt",s"$end_dt")
      println("#### JOB_DM_74 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      sqlContext.sql(s"use $hive_dbname")
      val results = sqlContext.sql(
        s"""
           |select
           |    c.activity_id as activity_id,
           |    c.activity_nm as activity_nm,
           |    c.settle_dt as report_dt,
           |    c.plan_num as plan_num,
           |    c.actual_num as actual_num
           |from
           |    (
           |        select
           |            a.loc_activity_id                                                  as activity_id,
           |            a.loc_activity_nm                                                  as activity_nm,
           |            a.settle_dt                                                        as settle_dt,
           |            a.plan_num                                                         as plan_num,
           |            b.actual_num                                                       as actual_num,
           |            row_number() over(partition by a.settle_dt order by b.actual_num desc) as rn
           |        from
           |            (
           |                select
           |                    prize.loc_activity_id,
           |                    translate(prize.loc_activity_nm,' ','') as loc_activity_nm,
           |                    rslt.settle_dt,
           |                    sum(lvl.lvl_prize_num) as plan_num
           |                from
           |                    hive_prize_activity_bas_inf prize,
           |                    hive_prize_lvl lvl,
           |                    (
           |                        select distinct
           |                            settle_dt
           |                        from
           |                            hive_prize_discount_result
           |                        where
           |                            part_settle_dt >= '$start_dt'
           |                        and part_settle_dt <= '$end_dt') rslt
           |                where
           |                    prize.loc_activity_id = lvl.loc_activity_id
           |                and prize.activity_begin_dt<= rslt.settle_dt
           |                and prize.activity_end_dt>=rslt.settle_dt
           |                and prize.run_st!='3'
           |                and prize.loc_activity_id is not null
           |                group by
           |                    prize.loc_activity_id,
           |                    translate(prize.loc_activity_nm,' ',''),
           |                    rslt.settle_dt ) a
           |        left join
           |            (
           |                select
           |                    prize.loc_activity_id,
           |                    pr.settle_dt,
           |                    count(pr.sys_tra_no_conv) as actual_num
           |                from
           |                    hive_prize_activity_bas_inf prize,
           |                    hive_prize_bas bas,
           |                    hive_prize_discount_result pr
           |                where
           |                    prize.loc_activity_id = bas.loc_activity_id
           |                and bas.prize_id = pr.prize_id
           |                and prize.activity_begin_dt<= pr.settle_dt
           |                and prize.activity_end_dt>= pr.settle_dt
           |                and pr.trans_id='S22'
           |                and pr.part_settle_dt >= '$start_dt'
           |                and pr.part_settle_dt <= '$end_dt'
           |                and pr.trans_id not like 'V%'
           |                and prize.run_st!='3'
           |                and prize.loc_activity_id is not null
           |                group by
           |                    prize.loc_activity_id,
           |                    settle_dt ) b
           |        on
           |            (
           |                a.loc_activity_id = b.loc_activity_id
           |            and a.settle_dt = b.settle_dt)
           |        where
           |            b.actual_num is not null
           |) c
           |where
           |    c.rn <= 10
           |
         | """.stripMargin)

      println(s"#### JOB_DM_74 spark sql 清洗[$start_dt -- $end_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())

      if (!Option(results).isEmpty) {
        results.save2Mysql("dm_prize_act_dly")
        println(s"#### JOB_DM_74 [$start_dt -- $end_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
      } else {
        println(s"#### JOB_DM_74 spark sql 清洗[$start_dt -- $end_dt]数据无结果集！")
      }
    }
  }

  /**
    * JOB_DM_75/10-14
    * dm_disc_act_dly->hive_prize_discount_result,hive_discount_bas_inf
    * Code by Xue
    *
    * @param sqlContext
    * @return
    */
  def JOB_DM_75 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {
    println("#### JOB_DM_75(dm_disc_act_dly->hive_prize_discount_result,hive_discount_bas_inf)")

    DateUtils.timeCost("JOB_DM_75") {
      UPSQL_JDBC.delete(s"dm_disc_act_dly","report_dt",s"$start_dt",s"$end_dt")
      println("#### JOB_DM_75 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())

      sqlContext.sql(s"use $hive_dbname")
      val results = sqlContext.sql(
        s"""
           |select
           |  b.activity_id as activity_id,
           |  b.activity_nm as activity_nm,
           |  b.settle_dt as report_dt,
           |  b.trans_cnt as trans_cnt,
           |  b.trans_at as trans_at,
           |  b.discount_at as discount_at
           | from
           | (
           | select
           | a.activity_id                         as activity_id,
           | a.activity_nm                        as activity_nm,
           | a.settle_dt                           as settle_dt,
           | a.trans_cnt                           as trans_cnt,
           | a.trans_pos_at_ttl                    as trans_at,
           | (a.trans_pos_at_ttl - a.trans_at_ttl) as discount_at,
           | row_number() over(partition by a.settle_dt order by a.trans_cnt desc) as rn
           | from
           |  (
           |   select
           |    dbi.loc_activity_id                 as activity_id,
           |    translate(dbi.loc_activity_nm,' ','') as activity_nm,
           |    trans.settle_dt,
           |    sum (
           |     case
           |      when trans.trans_id in ('V52','R22','V50','R20','S30')
           |      then -1
           |      else 1
           |     end) as trans_cnt,
           |    sum (
           |     case
           |      when trans.trans_id in ('V52','R22','V50','R20','S30')
           |      then -trans.trans_pos_at
           |      else trans.trans_pos_at
           |     end) as trans_pos_at_ttl,
           |    sum (
           |     case
           |      when trans.trans_id in ('V52','R22','V50','R20','S30')
           |      then -trans.trans_at
           |      else trans.trans_at
           |     end) as trans_at_ttl
           |   from
           |    hive_prize_discount_result trans,
           |    hive_discount_bas_inf dbi
           |   where
           |    trans.agio_app_id=dbi.loc_activity_id
           |   and trans.agio_app_id is not null
           |   and trans.trans_id='S22'
           |   and trans.part_settle_dt >='$start_dt'
           |   and trans.part_settle_dt <='$end_dt'
           |   group by
           |    dbi.loc_activity_id,
           |    translate(dbi.loc_activity_nm,' ',''),
           |    trans.settle_dt
           |  ) a
           | ) b
           | where
           |  b.rn <= 10
           |
         | """.stripMargin)

      println(s"#### JOB_DM_75 spark sql 清洗[$start_dt -- $end_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())

      if (!Option(results).isEmpty) {
        results.save2Mysql("dm_disc_act_dly")
        println(s"#### JOB_DM_75 [$start_dt -- $end_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
      } else {
        println(s"#### JOB_DM_75 spark sql 清洗[$start_dt -- $end_dt]数据无结果集！")
      }
    }
  }


  /**
    * JobName:JOB_DM_76
    * Feature:hive_prize_discount_result->dm_auto_disc_cfp_tran
    *
    * @author tzq
    * @time 2016-8-31
    * @param sqlContext start_dt,end_dt,interval
    */
  def JOB_DM_76(implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("###JOB_DM_76(hive_prize_discount_result->dm_auto_disc_cfp_tran)")
    DateUtils.timeCost("JOB_DM_76"){
      UPSQL_JDBC.delete("dm_auto_disc_cfp_tran","report_dt",start_dt,end_dt)
      println( "#### JOB_DM_76 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())

      var today_dt=start_dt
      if(interval>=0 ){
        sqlContext.sql(s"use $hive_dbname")
        for(i <- 0 to interval){
          println(s"#### JOB_DM_76 spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())
          val results=sqlContext.sql(
            s"""
               |select
               | tmp.cup_branch_nm as branch_nm,
               | tmp.cfp_sign as cfp_sign ,
               | tmp.settle_dt as report_dt,
               | count(case when tmp.settle_dt >= trunc('$today_dt','YYYY') and
               |           tmp.settle_dt <='$today_dt' then tmp.pri_acct_no end) as year_tran_num,
               | count(case when tmp.settle_dt = '$today_dt' then tmp.pri_acct_no end) as today_tran_num
               | from (
               | select (case when t.cloud_pay_in='0' then 'apple pay'
               |      when t.cloud_pay_in='1' then 'hce'
               |      when t.cloud_pay_in in ('2','3') then '三星pay'
               |      when t.cloud_pay_in='4' then 'ic卡挥卡'
               |      when t.cloud_pay_in='5' then '华为pay'
               |      when t.cloud_pay_in='6' then '小米pay'
               |    else '其它' end ) as cfp_sign ,t.cup_branch_ins_id_nm as cup_branch_nm,to_date(t.settle_dt) as settle_dt,t.pri_acct_no
               | from hive_prize_discount_result  t
               | where  t.prod_in='0'  and  t.trans_id='S22'
               | ) tmp
               | group by tmp.cup_branch_nm, tmp.cfp_sign, tmp.settle_dt
      """.stripMargin)
          println(s"#### JOB_DM_76 spark sql 清洗[$today_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())

          println(s"###JOB_DM_76------$today_dt results:"+results.count())

          if(!Option(results).isEmpty){
            results.save2Mysql("dm_auto_disc_cfp_tran")
            println(s"#### JOB_DM_76 [$today_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
          }else{
            println(s"#### JOB_DM_76 spark sql 清洗[$today_dt]数据无结果集！")
          }

          today_dt=DateUtils.addOneDay(today_dt)
        }
      }
    }

  }



  /**
    * JOB_DM_78/10-14
    * dm_iss_disc_cfp_tran->hive_card_bin,hive_card_bind_inf,hive_active_card_acq_branch_mon
    * Code by Xue
    *
    * @param sqlContext
    * @return
    */
  def JOB_DM_78 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("#### JOB_DM_78(dm_iss_disc_cfp_tran->hive_card_bin,hive_card_bind_inf,hive_active_card_acq_branch_mon)")
    DateUtils.timeCost("JOB_DM_78") {
      UPSQL_JDBC.delete(s"dm_iss_disc_cfp_tran","report_dt",start_dt,end_dt)
      println("#### JOB_DM_78 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      var today_dt=start_dt
      if(interval>=0 ){
        println(s"#### JOB_DM_78 spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())
        sqlContext.sql(s"use $hive_dbname")
        for(i <- 0 to interval){
          val results = sqlContext.sql(
            s"""
               |select
               |a.bank_nm as iss_nm,
               |'$today_dt' as report_dt,
               |a.card_attr as card_attr,
               |a.total_bind_cnt as total_bind_cnt,
               |b.last_quarter_active_cnt as last_quarter_active,
               |a.today_cnt  as today_tran_num
               |from
               |(
               |select
               |tempa.bank_nm as bank_nm,
               |tempa.card_attr as card_attr,
               |count(case when to_date(tempa.bind_dt)<='$today_dt' then 1 end ) as total_bind_cnt,
               |count(case when to_date(tempa.bind_dt)='$today_dt' then 1 end ) as today_cnt
               |from
               |(
               |select
               |tempc.bank_nm as bank_nm,
               |tempb.bind_dt as bind_dt,
               |(case when tempc.card_attr in ('01') then '借记卡'
               |      when tempc.card_attr in ('02', '03') then '贷记卡'
               |      else null end) as card_attr
               |from
               |(
               |select
               |to_date(bind_ts) as bind_dt,
               |substr(trim(bind_card_no),1,8) as card_bin
               |from hive_card_bind_inf where card_bind_st='0'
               |) tempb
               |inner join
               |(
               |select
               |distinct card_bin,
               |(case
               |when iss_ins_id_cd in ('01020000') then '工商银行'
               |when iss_ins_id_cd in ('01030000') then '农业银行'
               |when iss_ins_id_cd in ('01040000', '01040003') then '中国银行'
               |when iss_ins_id_cd in ('01050000', '01050001') then '建设银行'
               |when iss_ins_id_cd in ('01000000', '01009999', '61000000') then '邮储银行'
               |when iss_ins_id_cd in ('03010000') then '交通银行'
               |when iss_ins_id_cd in ('03020000', '63020000') then '中信银行'
               |when iss_ins_id_cd in ('03030000', '63030000') then '光大银行'
               |when iss_ins_id_cd in ('03040000', '03040001', '63040001') then '华夏银行'
               |when iss_ins_id_cd in ('03050000', '03050001') then '民生银行'
               |when iss_ins_id_cd in ('03060000') then '广发银行'
               |when iss_ins_id_cd in ('03080000') then '招商银行'
               |when iss_ins_id_cd in ('03090000', '03090002', '03090010') then '兴业银行'
               |when iss_ins_id_cd in ('03100000') then '浦发银行'
               |when iss_ins_id_cd in ('04031000', '64031000') then '北京银行'
               |when iss_ins_id_cd in ('04010000', '04012902', '04012900') then '上海银行'
               |when iss_ins_id_cd in ('04100000', '05105840', '06105840', '03070000', '03070010') then '平安银行'
               |else null end ) as bank_nm,
               |card_attr
               |from hive_card_bin
               |where iss_ins_id_cd in ('01020000','01030000','01040000','01040003','03070010',
               |   '01050000','01050001','61000000','01009999','01000000',
               |   '03010000','03020000','63020000','03030000','63030000',
               |   '03040000','03040001','63040001','03050000','03050001',
               |   '03060000','03080000','03090000','03090002','03090010',
               |   '03100000','04031000','64031000','04010000','04012902',
               |   '04012900','04100000','05105840','06105840','03070000')
               |
               |)tempc
               | on tempb.card_bin=tempc.card_bin
               |  ) tempa
               |where tempa.card_attr is not null
               |group by tempa.bank_nm, tempa.card_attr
               |) a
               |
               |left join
               |(
               |select
               |iss_root_ins_id_cd,
               |(case when iss_root_ins_id_cd in ('0801020000') then '工商银行'
               |when iss_root_ins_id_cd in ('0801030000') then '农业银行'
               |when iss_root_ins_id_cd in ('0801040000', '0801040003') then '中国银行'
               |when iss_root_ins_id_cd in ('0801050000', '0801050001') then '建设银行'
               |when iss_root_ins_id_cd in ('0801000000', '0801009999', '0861000000') then '邮储银行'
               |when iss_root_ins_id_cd in ('0803010000') then '交通银行'
               |when iss_root_ins_id_cd in ('0803020000', '0863020000') then '中信银行'
               |when iss_root_ins_id_cd in ('0803030000', '0863030000') then '光大银行'
               |when iss_root_ins_id_cd in ('0803040000', '0803040001', '0863040001') then '华夏银行'
               |when iss_root_ins_id_cd in ('0803050000', '0803050001') then '民生银行'
               |when iss_root_ins_id_cd in ('0803060000') then '广发银行'
               |when iss_root_ins_id_cd in ('0803080000') then '招商银行'
               |when iss_root_ins_id_cd in ('0803090000', '0803090002', '0803090010') then '兴业银行'
               |when iss_root_ins_id_cd in ('03100000') then '浦发银行'
               |when iss_root_ins_id_cd in ('0804031000', '0864031000') then '北京银行'
               |when iss_root_ins_id_cd in ('0804010000', '0804012902', '0804012900') then '上海银行'
               |when iss_root_ins_id_cd in ('0804100000', '0805105840', '0806105840', '0803070000', '0803070010') then '平安银行'
               |else null end ) as bank_nm,
               |(case when card_attr_id in ('01') then '借记卡'
               |      when card_attr_id in ('02', '03') then '贷记卡'
               |      else null end) as card_attr,
               |sum( case when month('$today_dt') in (01,02,03) and  trans_month>= concat(year('$today_dt')-1,'10') and trans_month<= concat(year('$today_dt')-1,'12')  then active_card_num
               |     when month('$today_dt') in (04,05,06) and  trans_month>= concat(year('$today_dt'),'01')  and trans_month<=concat(year('$today_dt'),'01')  then active_card_num
               |     when month('$today_dt') in (07,08,09) and  trans_month>= concat(year('$today_dt'),'04')  and trans_month<=concat(year('$today_dt'),'06')  then active_card_num
               |     when month('$today_dt') in (10,11,12) and  trans_month>= concat(year('$today_dt'),'07')  and trans_month<=concat(year('$today_dt'),'09')  then active_card_num
               |	 end) as last_quarter_active_cnt
               |
               |from hive_active_card_acq_branch_mon
               |where trans_class ='4' and
               |iss_root_ins_id_cd in ('0801020000','0801030000','0801040000','0801040003','0803070010',
               |   '0801050000','0801050001','0861000000','0801009999','0801000000',
               |   '0803010000','0803020000','0863020000','0803030000','0863030000',
               |   '0803040000','0803040001','0863040001','0803050000','0803050001',
               |   '0803060000','0803080000','0803090000','0803090002','0803090010',
               |   '0803100000','0804031000','0864031000','0804010000','0804012902',
               |   '0804012900','0804100000','0805105840','0806105840','0803070000')
               |group by iss_root_ins_id_cd,card_attr_id
               |   ) b
               |on a.bank_nm=b.bank_nm and a.card_attr=b.card_attr
               | """.stripMargin)

          println(s"#### JOB_DM_78 spark sql 清洗[$today_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())

          if (!Option(results).isEmpty) {
            results.save2Mysql("dm_iss_disc_cfp_tran")
            println(s"#### JOB_DM_78 [$today_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime()
            )
          } else {
            println(s"#### JOB_DM_78 spark sql 清洗[$today_dt]数据无结果集！")
          }
          today_dt=DateUtils.addOneDay(today_dt)
        }
      }
    }
  }


  /**
    * JOB_DM_79 20161220
    * dm_main_sweep_mchnt_type->hive_mchnt_tp,hive_mchnt_tp_grp,hive_active_code_pay_trans
    * Code by Xue
    *
    * @param sqlContext
    * @return
    */
  def JOB_DM_79(implicit sqlContext: HiveContext, start_dt: String, end_dt: String, interval: Int) = {
    println("###JOB_DM_79(dm_main_sweep_mchnt_type->hive_mchnt_tp,hive_mchnt_tp_grp,hive_active_code_pay_trans)")
    DateUtils.timeCost("JOB_DM_79") {
      UPSQL_JDBC.delete(s"dm_main_sweep_mchnt_type", "report_dt", start_dt, end_dt)
      println("#### JOB_DM_79 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      var today_dt = start_dt
      if (interval >= 0) {
        println(s"#### JOB_DM_79  spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())
        sqlContext.sql(s"use $hive_dbname")
        for (i <- 0 to interval) {
          val results = sqlContext.sql(
            s"""
               |select
               |e.grp_nm as mchnt_type,
               |'$today_dt'  as report_dt,
               |sum(c.quick_cnt),
               |sum(c.quick_cardcnt),
               |sum(d.quick_succ_cnt),
               |sum(d.quick_succ_cardcnt),
               |sum(d.quick_succ_trans_at),
               |sum(d.quick_succ_points_at),
               |sum(a.tran_cnt),
               |sum(a.cardcnt),
               |sum(b.succ_cnt),
               |sum(b.succ_cardcnt),
               |sum(b.succ_trans_at),
               |sum(c.swp_quick_cnt),
               |sum(c.swp_quick_usrcnt),
               |sum(d.swp_quick_succ_cnt),
               |sum(d.swp_quick_succ_usrcnt) ,
               |sum(d.swp_quick_succ_trans_at),
               |sum(d.swp_quick_succ_points_at),
               |sum(a.swp_verify_cnt),
               |sum(a.swp_verify_usrcnt),
               |sum(b.swp_verify_succ_cnt),
               |sum(b.swp_verify_succ_usrcnt ),
               |sum(b.swp_verify_succ_trans_at)
               |from
               |(select
               |tp_grp.mchnt_tp_grp_desc_cn as grp_nm ,
               |tp.mchnt_tp
               |from hive_mchnt_tp tp
               |left join hive_mchnt_tp_grp tp_grp
               |on tp.mchnt_tp_grp=tp_grp.mchnt_tp_grp) e
               |left join
               |(
               |select
               |mchnt_tp,
               |rec_crt,
               |sum(cnt) as tran_cnt,
               |sum(cardcnt) as cardcnt,
               |sum(case when trans_source in('0001','9001') then cnt end ) as swp_verify_cnt,
               |sum(case when trans_source in('0001','9001') then usrcnt end ) as swp_verify_usrcnt
               |from (select mchnt_cd,mchnt_tp,trans_source,to_date(rec_crt_ts) as rec_crt,count(*) as cnt,
               |count(distinct pri_acct_no) as cardcnt,
               |count(distinct usr_id) as usrcnt
               |from hive_active_code_pay_trans
               |where usr_id=0 and to_date(rec_crt_ts)='$today_dt'
               |group by mchnt_cd,mchnt_tp,trans_source,to_date(rec_crt_ts)
               |) t1
               |group by mchnt_tp,rec_crt) a
               |on a.mchnt_tp=e.mchnt_tp
               |left join
               |(
               |select
               |mchnt_tp,
               |rec_crt,
               |sum(cnt) as succ_cnt,
               |sum(cardcnt) as succ_cardcnt,
               |sum(trans_at) as succ_trans_at,
               |sum(case when trans_source in('0001','9001') then cnt end ) as swp_verify_succ_cnt,
               |sum(case when trans_source in('0001','9001') then usrcnt end ) as swp_verify_succ_usrcnt ,
               |sum(case when trans_source in('0001','9001') then trans_at end) as swp_verify_succ_trans_at
               |
               |from (select mchnt_tp,trans_source,to_date(rec_crt_ts) as rec_crt,count(*) as cnt,
               |count(distinct pri_acct_no) as cardcnt,sum(trans_at) as trans_at,
               |count(distinct usr_id) as usrcnt
               |from hive_active_code_pay_trans
               |where trans_st like '%000' and usr_id=0 and to_date(rec_crt_ts)='$today_dt'
               |group by mchnt_tp,trans_source,to_date(rec_crt_ts)) t2
               |group by mchnt_tp, rec_crt
               |) b
               |on e.mchnt_tp=b.mchnt_tp and a.rec_crt=b.rec_crt
               |left join
               |(
               |select
               |mchnt_tp,
               |rec_crt,
               |sum(cnt) as quick_cnt,
               |sum(cardcnt) as quick_cardcnt,
               |sum(case when trans_source in('0001','9001') then cnt end ) as swp_quick_cnt,
               |sum(case when trans_source in('0001','9001') then usrcnt end ) as swp_quick_usrcnt
               |from
               |(select mchnt_cd,mchnt_tp,trans_source,to_date(rec_crt_ts) as rec_crt,count(*) as cnt,
               |count(distinct pri_acct_no) as cardcnt,count(distinct usr_id) as usrcnt
               |from hive_active_code_pay_trans
               |where usr_id<>0 and to_date(rec_crt_ts)='$today_dt'
               |group by mchnt_cd,mchnt_tp,trans_source,to_date(rec_crt_ts)) t3
               |group by mchnt_tp,rec_crt
               |) c
               |on e.mchnt_tp=c.mchnt_tp and a.rec_crt=c.rec_crt
               |left join
               |(
               |select mchnt_tp,
               |rec_crt,
               |sum(cnt) as quick_succ_cnt,
               |sum(cardcnt) as quick_succ_cardcnt ,
               |sum(trans_at) as quick_succ_trans_at,
               |sum(points_at) as quick_succ_points_at,
               |sum(case when trans_source in('0001','9001') then cnt end ) as swp_quick_succ_cnt,
               |sum(case when trans_source in('0001','9001') then usrcnt end ) as swp_quick_succ_usrcnt ,
               |sum(case when trans_source in('0001','9001') then trans_at end) as swp_quick_succ_trans_at,
               |sum(case when trans_source in('0001','9001') then points_at end ) as swp_quick_succ_points_at
               |from
               |(select
               |mchnt_tp,
               |trans_source,
               |to_date(rec_crt_ts) as rec_crt,
               |count(*) as cnt,
               |count(distinct pri_acct_no) as cardcnt,
               |sum(points_at) as points_at,
               |sum(trans_at) as trans_at,
               |count(distinct usr_id) as usrcnt
               |from hive_active_code_pay_trans
               |where trans_st like '%000' and usr_id<>0 and to_date(rec_crt_ts)='$today_dt'
               |group by mchnt_tp,trans_source,to_date(rec_crt_ts) ) t4
               |group by mchnt_tp,rec_crt) d
               |on e.mchnt_tp=d.mchnt_tp and a.rec_crt=d.rec_crt
               |where a.mchnt_tp is not null and b.mchnt_tp is not null and c.mchnt_tp is not null and d.mchnt_tp is not null
               |group by e.grp_nm
               | """.stripMargin)
          println(s"#### JOB_DM_79 spark sql 清洗[$today_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())

          if (!Option(results).isEmpty) {
            results.save2Mysql("dm_main_sweep_mchnt_type")
            println(s"#### JOB_DM_79 [$today_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
          } else {
            println(s"#### JOB_DM_79 spark sql 清洗[$today_dt]数据无结果集！")
          }
          today_dt = DateUtils.addOneDay(today_dt)
        }
      }
    }
  }


  /**
    * JOB_DM_80 20161220
    * dm_main_sweep_mchnt_top10->hive_mchnt_inf_wallet,hive_active_code_pay_trans
    * Code by Xue
    *
    * @param sqlContext
    * @return
    */
  def JOB_DM_80(implicit sqlContext: HiveContext, start_dt: String, end_dt: String, interval: Int) = {
    println("###JOB_DM_80(dm_main_sweep_mchnt_top10->hive_mchnt_inf_wallet,hive_active_code_pay_trans)")
    DateUtils.timeCost("JOB_DM_80") {
      UPSQL_JDBC.delete(s"dm_main_sweep_mchnt_top10", "report_dt", start_dt, end_dt)
      println("#### JOB_DM_80 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      var today_dt = start_dt
      if (interval >= 0) {
        println(s"#### JOB_DM_80  spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())
        sqlContext.sql(s"use $hive_dbname")
        for (i <- 0 to interval) {
          val results = sqlContext.sql(
            s"""
               |select
               |e.mchnt_cn_nm as mchnt_nm,
               |'$today_dt'  as report_dt,
               |sum(c.quick_cnt),
               |sum(c.quick_cardcnt),
               |sum(d.quick_succ_cnt),
               |sum(d.quick_succ_cardcnt ),
               |sum(d.quick_succ_trans_at),
               |sum(d.quick_succ_points_at),
               |sum(a.tran_cnt),
               |sum(a.cardcnt),
               |sum(b.succ_cnt),
               |sum(b.succ_cardcnt),
               |sum(b.succ_trans_at),
               |sum(c.swp_quick_cardcnt),
               |sum(c.swp_quick_usrcnt),
               |sum(d.swp_quick_succ_cnt),
               |sum(d.swp_quick_succ_usrcnt ),
               |sum(d.swp_quick_succ_trans_at),
               |sum(d.swp_quick_succ_points_at),
               |sum(a.swp_verify_cnt),
               |sum(a.swp_verify_cardcnt),
               |sum(b.swp_succ_verify_cnt),
               |sum(b.swp_succ_verify_cardcnt),
               |sum(b.swp_succ_verify_trans_at)
               |
               |from
               |hive_mchnt_inf_wallet e
               |left join
               |(
               |select
               |mchnt_tp,
               |rec_crt,
               |cnt as tran_cnt,
               |cardcnt,
               |sum(case when trans_source in('0001','9001') then cnt end ) as swp_verify_cnt,
               |sum(case when trans_source in('0001','9001') then cardcnt end) as swp_verify_cardcnt
               |from
               |(
               |select
               |mchnt_tp,
               |trans_source,
               |to_date(rec_crt_ts) as rec_crt,
               |count(*) as cnt,
               |count(distinct pri_acct_no) as cardcnt
               |from hive_active_code_pay_trans
               |where usr_id=0 and to_date(rec_crt_ts)='$today_dt'
               |group by mchnt_tp,trans_source,to_date(rec_crt_ts)) t1
               |group by mchnt_tp,rec_crt,cnt,cardcnt
               |) a
               |on a.mchnt_tp=e.mchnt_tp
               |left join
               |(
               |select
               |mchnt_tp,
               |rec_crt,
               |sum(cnt) as succ_cnt,
               |sum(cardcnt) as succ_cardcnt,
               |sum(trans_at) as succ_trans_at,
               |sum(case when trans_source in('0001','9001') then cnt end ) as swp_succ_verify_cnt,
               |sum(case when trans_source in('0001','9001') then cardcnt end) as swp_succ_verify_cardcnt,
               |sum(case when trans_source in('0001','9001') then trans_at end) as swp_succ_verify_trans_at
               |from (select mchnt_tp,trans_source,to_date(rec_crt_ts) as rec_crt ,count(*) as cnt,
               |count(distinct pri_acct_no) as cardcnt,sum(trans_at) as trans_at
               |from hive_active_code_pay_trans
               |where trans_st like '%000' and usr_id=0 and to_date(rec_crt_ts)='$today_dt'
               |group by mchnt_tp,trans_source,to_date(rec_crt_ts)) t2
               |group by mchnt_tp,rec_crt
               |) b
               |on e.mchnt_tp=b.mchnt_tp
               |left join
               |(
               |select
               |mchnt_tp,
               |to_date(rec_crt_ts) as rec_crt,
               |count(*) as quick_cnt,
               |count(distinct pri_acct_no) as quick_cardcnt,
               |count(distinct(case when trans_source in('0001','9001') then pri_acct_no end)) as swp_quick_cardcnt,
               |count(distinct(case when trans_source in('0001','9001') then usr_id end )) as swp_quick_usrcnt
               |from hive_active_code_pay_trans
               |where usr_id<>0 and to_date(rec_crt_ts)='$today_dt'
               |group by mchnt_tp,to_date(rec_crt_ts) ) c
               |on e.mchnt_tp=c.mchnt_tp
               |left join
               |(
               |select
               |mchnt_tp,
               |rec_crt,
               |sum(cnt) as quick_succ_cnt,
               |sum(cardcnt) as quick_succ_cardcnt ,
               |sum(trans_at) as quick_succ_trans_at,
               |sum(points_at) as quick_succ_points_at,
               |sum(case when trans_source in('0001','9001') then cnt end ) as swp_quick_succ_cnt,
               |sum(case when trans_source in('0001','9001') then usrcnt end ) as swp_quick_succ_usrcnt ,
               |sum(case when trans_source in('0001','9001') then trans_at end) as swp_quick_succ_trans_at,
               |sum(case when trans_source in('0001','9001') then points_at end ) as swp_quick_succ_points_at
               |from
               |(select
               |mchnt_tp,
               |to_date(rec_crt_ts) as rec_crt,
               |trans_source,
               |count(*) as cnt,
               |count(distinct pri_acct_no) as cardcnt,
               |sum(points_at) as points_at,
               |sum(trans_at) as trans_at,
               |count(distinct usr_id) as usrcnt
               |from hive_active_code_pay_trans
               |where trans_st like '%000' and usr_id<>0 and to_date(rec_crt_ts)='$today_dt'
               |group by mchnt_tp,to_date(rec_crt_ts),trans_source ) t3
               |group by mchnt_tp,rec_crt ) d
               |on e.mchnt_tp=d.mchnt_tp
               |where a.mchnt_tp is not null and b.mchnt_tp is not null and c.mchnt_tp is not null and d.mchnt_tp is not null
               |group by e.mchnt_cn_nm
               |
               | """.stripMargin)
          println(s"#### JOB_DM_80 spark sql 清洗[$today_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())

          if (!Option(results).isEmpty) {
            results.save2Mysql("dm_main_sweep_mchnt_top10")
            println(s"#### JOB_DM_80 [$today_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
          } else {
            println(s"#### JOB_DM_80 spark sql 清洗[$today_dt]数据无结果集！")
          }
          today_dt = DateUtils.addOneDay(today_dt)
        }
      }
    }
  }


  /**
    * JOB_DM_81 20161221
    * dm_main_sweep_mchnt_iss->hive_card_bind_inf,hive_active_code_pay_trans
    * Code by Xue
    *
    * @param sqlContext
    * @return
    */
  def JOB_DM_81(implicit sqlContext: HiveContext, start_dt: String, end_dt: String, interval: Int) = {
    println("###JOB_DM_81(dm_main_sweep_mchnt_iss->hive_card_bind_inf,hive_active_code_pay_trans)")
    DateUtils.timeCost("JOB_DM_81") {
      UPSQL_JDBC.delete(s"dm_main_sweep_mchnt_iss", "report_dt", start_dt, end_dt)
      println("#### JOB_DM_81 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      var today_dt = start_dt
      if (interval >= 0) {
        println(s"#### JOB_DM_81  spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())
        sqlContext.sql(s"use $hive_dbname")
        for (i <- 0 to interval) {
          val results = sqlContext.sql(
            s"""
               |select
               |trim(e.iss_ins_cn_nm) as iss_nm,
               |'$today_dt' as report_dt,
               |sum(c.swp_quick_cnt) as swp_quick_cnt,
               |sum(c.swp_quick_usrcnt) as swp_quick_usrcnt,
               |sum(d.swp_quick_succ_cnt) as swp_quick_succ_cnt,
               |sum(d.swp_quick_succ_usrcnt) as swp_quick_succ_usrcnt,
               |sum(d.swp_quick_succ_trans_at) as swp_quick_succ_trans_at,
               |sum(d.swp_quick_succ_points_at) as swp_quick_succ_points_at,
               |sum(a.swp_verify_cnt) as swp_verify_cnt,
               |sum(a.swp_verify_cardcnt) as swp_verify_cardcnt,
               |sum(b.swp_verify_cnt) as swp_verify_succ_cnt,
               |sum(b.swp_verify_cardcnt) as swp_verify_succ_cardcnt,
               |sum(b.swp_verify_trans_at) as swp_verify_succ_trans_at
               |from
               |(select iss_ins_cn_nm,substr(iss_ins_id_cd,3,8) as iss_ins_id_cd
               |from hive_card_bind_inf )e
               |left join
               |(
               |select
               |iss_ins_id_cd,
               |to_date(rec_crt_ts) as rec_crt,
               |count(*) as swp_verify_cnt,
               |count(distinct pri_acct_no) as swp_verify_cardcnt
               |from hive_active_code_pay_trans
               |where usr_id=0 and trans_source in('0001','9001') and to_date(rec_crt_ts)='$today_dt'
               |group by iss_ins_id_cd,to_date(rec_crt_ts)) a
               |on a.iss_ins_id_cd=e.iss_ins_id_cd
               |left join
               |(
               |select
               |iss_ins_id_cd,
               |to_date(rec_crt_ts) as rec_crt,
               |count(*) as swp_verify_cnt,
               |count(distinct pri_acct_no) as swp_verify_cardcnt,
               |sum(trans_at) as swp_verify_trans_at
               |from hive_active_code_pay_trans
               |where trans_st like '%000' and usr_id=0
               |and trans_source in('0001','9001') and to_date(rec_crt_ts)='$today_dt'
               |group by iss_ins_id_cd,to_date(rec_crt_ts)
               |) b
               |on e.iss_ins_id_cd=b.iss_ins_id_cd
               |full outer join
               |(
               |select
               |iss_ins_id_cd,
               |to_date(rec_crt_ts) as rec_crt,
               |count(*) as swp_quick_cnt,
               |count(distinct usr_id) as swp_quick_usrcnt
               |from hive_active_code_pay_trans
               |where usr_id<>0 and trans_source in('0001','9001') and to_date(rec_crt_ts)='$today_dt'
               |group by iss_ins_id_cd,to_date(rec_crt_ts) ) c
               |on e.iss_ins_id_cd=c.iss_ins_id_cd
               |full outer join
               |(select
               |iss_ins_id_cd,
               |to_date(rec_crt_ts) as rec_crt,
               |count(*) as swp_quick_succ_cnt,
               |count(distinct usr_id) as swp_quick_succ_usrcnt ,
               |sum(trans_at) as swp_quick_succ_trans_at,
               |sum(points_at) as swp_quick_succ_points_at
               |from hive_active_code_pay_trans
               |where trans_st like '%000' and usr_id<>0 and to_date(rec_crt_ts)='$today_dt'
               |group by iss_ins_id_cd,to_date(rec_crt_ts) ) d
               |on e.iss_ins_id_cd=d.iss_ins_id_cd
               |group by trim(e.iss_ins_cn_nm),'$today_dt'
               | """.stripMargin)
          println(s"#### JOB_DM_81 spark sql 清洗[$today_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())

          if (!Option(results).isEmpty) {
            results.save2Mysql("dm_main_sweep_mchnt_iss")
            println(s"#### JOB_DM_81 [$today_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
          } else {
            println(s"#### JOB_DM_81 spark sql 清洗[$today_dt]数据无结果集！")
          }
          today_dt = DateUtils.addOneDay(today_dt)
        }
      }
    }
  }


  /**
    * JOB_DM_82 20161221
    * dm_main_sweep_card_attr->hive_active_code_pay_trans
    * Code by Xue
    *
    * @param sqlContext
    * @return
    */
  def JOB_DM_82(implicit sqlContext: HiveContext, start_dt: String, end_dt: String, interval: Int) = {
    println("###JOB_DM_82(dm_main_sweep_card_attr->hive_active_code_pay_trans)")
    DateUtils.timeCost("JOB_DM_82") {
      UPSQL_JDBC.delete(s"dm_main_sweep_card_attr", "report_dt", start_dt, end_dt)
      println("#### JOB_DM_82 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      var today_dt = start_dt
      if (interval >= 0) {
        println(s"#### JOB_DM_82  spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())
        sqlContext.sql(s"use $hive_dbname")
        for (i <- 0 to interval) {
          val results = sqlContext.sql(
            s"""
               |select
               |case when a.card_attr is null then nvl(b.card_attr,nvl(c.card_attr,nvl(d.card_attr,'其它'))) else a.card_attr end as card_attr_nm,
               |'$today_dt' as report_dt,
               |sum(c.swp_quick_cnt),
               |sum(c.swp_quick_usrcnt),
               |sum(d.swp_quick_succ_cnt),
               |sum(d.swp_quick_succ_usrcnt),
               |sum(d.swp_quick_succ_trans_at),
               |sum(d.swp_quick_succ_points_at),
               |sum(a.swp_verify_cnt),
               |sum(a.swp_verify_usrcnt),
               |sum(b.swp_succ_verify_cnt),
               |sum(b.swp_succ_verify_usrcnt),
               |sum(b.swp_succ_verify_trans_at)
               |from
               |(
               |select
               |(case when card_attr in ('01') then '借记卡'
               |when card_attr in ('02', '03') then '贷记卡' end
               |) as card_attr,
               |count(*) as swp_verify_cnt,
               |count(distinct usr_id) as swp_verify_usrcnt
               |from hive_active_code_pay_trans
               |where usr_id=0 and trans_source in('0001','9001') and to_date(rec_crt_ts)='$today_dt'
               |group by (case when card_attr in ('01') then '借记卡'
               |when card_attr in ('02', '03') then '贷记卡' end)) a
               |full outer join
               |(
               |select
               |(case when card_attr in ('01') then '借记卡'
               |when card_attr in ('02', '03') then '贷记卡' end
               |) as card_attr,
               |count(*) as swp_succ_verify_cnt,
               |count(distinct usr_id) as swp_succ_verify_usrcnt,
               |sum(trans_at) as swp_succ_verify_trans_at
               |from hive_active_code_pay_trans
               |where trans_st like '%000' and usr_id=0
               |and trans_source in('0001','9001') and to_date(rec_crt_ts)='$today_dt'
               |group by (case when card_attr in ('01') then '借记卡'
               |when card_attr in ('02', '03') then '贷记卡' end)
               |) b
               |on a.card_attr=b.card_attr
               |full outer join
               |(
               |select
               |(case when card_attr in ('01') then '借记卡'
               |when card_attr in ('02', '03') then '贷记卡' end) as card_attr,
               |count(*) as swp_quick_cnt,
               |count(distinct usr_id) as swp_quick_usrcnt
               |from hive_active_code_pay_trans
               |where usr_id<>0 and trans_source in('0001','9001') and to_date(rec_crt_ts)='$today_dt'
               |group by (case when card_attr in ('01') then '借记卡'
               |when card_attr in ('02', '03') then '贷记卡' end) ) c
               |on a.card_attr=c.card_attr
               |full outer join
               |(select
               |(case when card_attr in ('01') then '借记卡'
               |when card_attr in ('02', '03') then '贷记卡' end
               |) as card_attr,
               |count(*) as swp_quick_succ_cnt,
               |count(distinct usr_id) as swp_quick_succ_usrcnt ,
               |sum(trans_at) as swp_quick_succ_trans_at,
               |sum(points_at) as swp_quick_succ_points_at
               |from hive_active_code_pay_trans
               |where trans_st like '%000' and usr_id<>0 and to_date(rec_crt_ts)='$today_dt'
               |group by (case when card_attr in ('01') then '借记卡'
               |when card_attr in ('02', '03') then '贷记卡' end) ) d
               |on a.card_attr=d.card_attr
               |group by case when a.card_attr is null then nvl(b.card_attr,nvl(c.card_attr,nvl(d.card_attr,'其它'))) else a.card_attr end
               | """.stripMargin)
          println(s"#### JOB_DM_82 spark sql 清洗[$today_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())

          if (!Option(results).isEmpty) {
            results.save2Mysql("dm_main_sweep_card_attr")
            println(s"#### JOB_DM_82 [$today_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
          } else {
            println(s"#### JOB_DM_82 spark sql 清洗[$today_dt]数据无结果集！")
          }
          today_dt = DateUtils.addOneDay(today_dt)
        }
      }
    }
  }


  /**
    * JOB_DM_83 20161221
    * dm_main_sweep_card_lvl->hive_card_bin,hive_active_code_pay_trans
    * Code by Xue
    *
    * @param sqlContext
    * @return
    */
  def JOB_DM_83(implicit sqlContext: HiveContext, start_dt: String, end_dt: String, interval: Int) = {
    println("###JOB_DM_83(dm_main_sweep_card_lvl->hive_card_bin,hive_active_code_pay_trans)")
    DateUtils.timeCost("JOB_DM_83") {
      UPSQL_JDBC.delete(s"dm_main_sweep_card_lvl", "report_dt", start_dt, end_dt)
      println("#### JOB_DM_83 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      var today_dt = start_dt
      if (interval >= 0) {
        println(s"#### JOB_DM_83  spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())
        sqlContext.sql(s"use $hive_dbname")
        for (i <- 0 to interval) {
          val results = sqlContext.sql(
            s"""
               |select
               |trim(e.card_lvl_nm) as  card_lvl_nm,
               |'$today_dt' as report_dt,
               |sum(c.swp_quick_cnt) as swp_quick_cnt,
               |sum(c.swp_quick_usrcnt) as swp_quick_usrcnt,
               |sum(d.swp_quick_succ_cnt) as swp_quick_succ_cnt,
               |sum(d.swp_quick_succ_usrcnt) as swp_quick_succ_usrcnt,
               |sum(d.swp_quick_succ_trans_at) as swp_quick_succ_trans_at,
               |sum(d.swp_quick_succ_points_at) as swp_quick_succ_points_at,
               |sum(a.swp_verify_cnt) as swp_verify_cnt,
               |sum(a.swp_verify_usrcnt) as swp_verify_usrcnt,
               |sum(b.swp_succ_verify_cnt) as swp_succ_verify_cnt,
               |sum(b.swp_succ_verify_usrcnt) as swp_succ_verify_usrcnt,
               |sum(b.swp_succ_verify_trans_at) as swp_succ_verify_trans_at
               |from
               |(
               |select
               |(case when card_attr in ('0') then '未知'
               |when card_attr in ('1') then '普卡'
               |when card_attr in ('2') then '银卡'
               |when card_attr in ('3') then '金卡'
               |when card_attr in ('4') then '白金卡'
               |when card_attr in ('5') then '钻石卡'
               |when card_attr in ('6') then '无限卡'
               |else '其它' end
               |) as card_lvl_nm,
               |substr(card_bin,1,8) as card_bin
               |from hive_card_bin ) e
               |left join
               |(
               |select
               |substr(card_bin,1,8) as card_bin,
               |count(*) as swp_verify_cnt,
               |count(distinct usr_id) as swp_verify_usrcnt
               |from hive_active_code_pay_trans
               |where usr_id=0 and trans_source in('0001','9001') and to_date(rec_crt_ts)='$today_dt'
               |group by substr(card_bin,1,8)) a
               |on a.card_bin=e.card_bin
               |left join
               |(
               |select
               |substr(card_bin,1,8) as card_bin,
               |count(*) as swp_succ_verify_cnt,
               |count(distinct usr_id) as swp_succ_verify_usrcnt,
               |sum(trans_at) as swp_succ_verify_trans_at
               |from hive_active_code_pay_trans
               |where trans_st like '%000' and usr_id=0
               |and trans_source in('0001','9001') and to_date(rec_crt_ts)='$today_dt'
               |group by substr(card_bin,1,8)
               |) b
               |on e.card_bin=b.card_bin
               |left join
               |(
               |select
               |substr(card_bin,1,8) as card_bin,
               |count(*) as swp_quick_cnt,
               |count(distinct usr_id) as swp_quick_usrcnt
               |from hive_active_code_pay_trans
               |where usr_id<>0 and trans_source in('0001','9001') and to_date(rec_crt_ts)='$today_dt'
               |group by substr(card_bin,1,8) ) c
               |on e.card_bin=c.card_bin
               |left join
               |(select
               |substr(card_bin,1,8) as card_bin,
               |count(*) as swp_quick_succ_cnt,
               |count(distinct usr_id) as swp_quick_succ_usrcnt ,
               |sum(trans_at) as swp_quick_succ_trans_at,
               |sum(points_at) as swp_quick_succ_points_at
               |from hive_active_code_pay_trans
               |where trans_st like '%000' and usr_id<>0 and to_date(rec_crt_ts)='$today_dt'
               |group by substr(card_bin,1,8)) d
               |on e.card_bin=d.card_bin
               |group by trim(e.card_lvl_nm)
               | """.stripMargin)
          println(s"#### JOB_DM_83 spark sql 清洗[$today_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())

          if (!Option(results).isEmpty) {
            results.save2Mysql("dm_main_sweep_card_lvl")
            println(s"#### JOB_DM_83 [$today_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
          } else {
            println(s"#### JOB_DM_83 spark sql 清洗[$today_dt]数据无结果集！")
          }
          today_dt = DateUtils.addOneDay(today_dt)
        }
      }
    }
  }


  /**
    * JOB_DM_84 20161219
    * dm_main_sweep_area->hive_mchnt_inf_wallet,hive_active_code_pay_trans
    * Code by Xue
    *
    * @param sqlContext
    * @return
    */
  def JOB_DM_84(implicit sqlContext: HiveContext, start_dt: String, end_dt: String, interval: Int) = {
    println("###JOB_DM_84(dm_main_sweep_area->hive_mchnt_inf_wallet,hive_active_code_pay_trans)")
    DateUtils.timeCost("JOB_DM_84") {
      UPSQL_JDBC.delete(s"dm_main_sweep_area", "report_dt", start_dt, end_dt)
      println("#### JOB_DM_84 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      var today_dt = start_dt
      if (interval >= 0) {
        println(s"#### JOB_DM_84  spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())
        sqlContext.sql(s"use $hive_dbname")
        for (i <- 0 to interval) {
          val results = sqlContext.sql(
            s"""
               |select
               |e.cup_branch_ins_id_nm as area_nm,
               |'$today_dt' as report_dt,
               |sum(c.quick_cnt) as quick_cnt,
               |sum(c.quick_usrcnt) as quick_usrcnt,
               |sum(d.quick_succ_cnt) as quick_succ_cnt,
               |sum(d.quick_succ_usrcnt) as quick_succ_usrcnt,
               |sum(d.quick_succ_trans_at) as quick_succ_trans_at,
               |sum(d.quick_succ_points_at) as quick_succ_points_at,
               |sum(a.tran_cnt) as tran_cnt,
               |sum(a.usrcnt) as usrcnt,
               |sum(b.succ_cnt) as succ_cnt,
               |sum(b.succ_usrcnt) as succ_usrcnt,
               |sum(b.succ_trans_at) as succ_trans_at,
               |sum(c.swp_quick_cnt) as swp_quick_cnt,
               |sum(c.swp_quick_usrcnt) as swp_quick_usrcnt,
               |sum(d.swp_quick_succ_cnt) as swp_quick_succ_cnt,
               |sum(d.swp_quick_succ_usrcnt) as swp_quick_succ_usrcnt,
               |sum(d.swp_quick_succ_trans_at) as swp_quick_succ_trans_at,
               |sum(d.swp_quick_succ_points_at) as swp_quick_succ_points_at,
               |sum(a.swp_verify_cnt) as swp_verify_cnt,
               |sum(a.swp_verify_usrcnt) as swp_verify_usrcnt,
               |sum(b.swp_verify_succ_cnt) as swp_verify_succ_cnt,
               |sum(b.swp_verify_succ_usrcnt) as swp_verify_succ_usrcnt,
               |sum(b.swp_verify_succ_trans_at) as swp_verify_succ_trans_at,
               |sum(b.swp_verify_succ_points_at) as swp_verify_succ_points_at
               |from
               |hive_mchnt_inf_wallet e
               |left join
               |(
               |select
               |mchnt_tp ,
               |sum(cnt) as tran_cnt,
               |sum(usrcnt) as usrcnt,
               |sum(case when trans_source in('0001','9001') then cnt end ) as swp_verify_cnt,
               |sum(case when trans_source in('0001','9001') then usrcnt end ) as swp_verify_usrcnt
               |from (select mchnt_tp,trans_source,count(*) as cnt,
               |count(distinct usr_id) as usrcnt
               |from hive_active_code_pay_trans
               |where usr_id=0 and to_date(rec_crt_ts)='$today_dt'
               |group by mchnt_tp,trans_source
               |) t1
               |group by mchnt_tp) a
               |on a.mchnt_tp=e.mchnt_tp
               |left join
               |(
               |select
               |mchnt_tp,
               |sum(cnt) as succ_cnt,
               |sum(usrcnt) as succ_usrcnt,
               |sum(trans_at) as succ_trans_at,
               |sum(case when trans_source in('0001','9001') then cnt end ) as swp_verify_succ_cnt,
               |sum(case when trans_source in('0001','9001') then usrcnt end ) as swp_verify_succ_usrcnt ,
               |sum(case when trans_source in('0001','9001') then trans_at end) as swp_verify_succ_trans_at,
               |sum(case when trans_source in('0001','9001') then points_at end ) as swp_verify_succ_points_at
               |from (select mchnt_tp,trans_source,count(*) as cnt,
               |sum(trans_at) as trans_at,
               |sum(points_at) as points_at,
               |count(distinct usr_id) as usrcnt
               |from hive_active_code_pay_trans
               |where trans_st like '%000' and usr_id=0 and to_date(rec_crt_ts)=='$today_dt'
               |group by mchnt_tp,trans_source) t2
               |group by mchnt_tp
               |) b
               |on e.mchnt_tp=b.mchnt_tp
               |left join
               |(
               |select
               |mchnt_tp,
               |sum(cnt) as quick_cnt,
               |sum(usrcnt) as quick_usrcnt,
               |sum(case when trans_source in('0001','9001') then cnt end ) as swp_quick_cnt,
               |sum(case when trans_source in('0001','9001') then usrcnt end ) as swp_quick_usrcnt
               |from
               |(select mchnt_tp,trans_source,count(*) as cnt,
               |count(distinct usr_id) as usrcnt
               |from hive_active_code_pay_trans
               |where usr_id<>0 and to_date(rec_crt_ts)='$today_dt'
               |group by mchnt_tp,trans_source) t3
               |group by mchnt_tp
               |) c
               |on e.mchnt_tp=c.mchnt_tp
               |left join
               |(
               |select mchnt_tp,
               |sum(cnt) as quick_succ_cnt,
               |sum(usrcnt) as quick_succ_usrcnt ,
               |sum(trans_at) as quick_succ_trans_at,
               |sum(points_at) as quick_succ_points_at,
               |sum(case when trans_source in('0001','9001') then cnt end ) as swp_quick_succ_cnt,
               |sum(case when trans_source in('0001','9001') then usrcnt end ) as swp_quick_succ_usrcnt ,
               |sum(case when trans_source in('0001','9001') then trans_at end) as swp_quick_succ_trans_at,
               |sum(case when trans_source in('0001','9001') then points_at end ) as swp_quick_succ_points_at
               |from
               |(select
               |mchnt_tp,
               |trans_source,
               |count(*) as cnt,
               |sum(points_at) as points_at,
               |sum(trans_at) as trans_at,
               |count(distinct usr_id) as usrcnt
               |from hive_active_code_pay_trans
               |where trans_st like '%000' and usr_id<>0 and to_date(rec_crt_ts)='$today_dt'
               |group by mchnt_tp,trans_source) t4
               |group by mchnt_tp
               |) d
               |on e.mchnt_tp=d.mchnt_tp
               |where a.mchnt_tp is not null and b.mchnt_tp is not null and c.mchnt_tp is not null and d.mchnt_tp is not null
               |group by e.cup_branch_ins_id_nm
               | """.stripMargin)
          println(s"#### JOB_DM_84 spark sql 清洗[$today_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())

          if (!Option(results).isEmpty) {
            results.save2Mysql("dm_main_sweep_area")
            println(s"#### JOB_DM_84 [$today_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
          } else {
            println(s"#### JOB_DM_84 spark sql 清洗[$today_dt]数据无结果集！")
          }
          today_dt = DateUtils.addOneDay(today_dt)
        }
      }
    }
  }


  /**
    * JOB_DM_85 20161226
    * dm_main_sweep_resp_code->hive_active_code_pay_trans
    * Code by Xue
    *
    * @param sqlContext
    * @return
    */
  def JOB_DM_85(implicit sqlContext: HiveContext, start_dt: String, end_dt: String, interval: Int) = {
    println("###JOB_DM_85(dm_main_sweep_resp_code->hive_active_code_pay_trans)")
    DateUtils.timeCost("JOB_DM_85") {
      UPSQL_JDBC.delete(s"dm_main_sweep_resp_code", "report_dt", start_dt, end_dt)
      println("#### JOB_DM_85 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      var today_dt = start_dt
      if (interval >= 0) {
        println(s"#### JOB_DM_85  spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())
        sqlContext.sql(s"use $hive_dbname")
        for (i <- 0 to interval) {
          val results = sqlContext.sql(
            s"""
               |select
               |resp_cd as resp_cd,
               |to_date(rec_crt_ts) as report_dt,
               |count(*) as  tran_cnt,
               |sum(trans_at) as trans_at
               |from hive_active_code_pay_trans
               |where  length(resp_cd)=2  and  to_date(rec_crt_ts)>='$today_dt'  and  to_date(rec_crt_ts)<= '$today_dt'
               |group by resp_cd,to_date(rec_crt_ts)
               | """.stripMargin)
          println(s"#### JOB_DM_85 spark sql 清洗[$today_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())

          if (!Option(results).isEmpty) {
            results.save2Mysql("dm_main_sweep_resp_code")
            println(s"#### JOB_DM_85 [$today_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
          } else {
            println(s"#### JOB_DM_85 spark sql 清洗[$today_dt]数据无结果集！")
          }
          today_dt = DateUtils.addOneDay(today_dt)
        }
      }
    }
  }

  /**
    * JOB_DM_86/10-14
    * dm_user_real_name->hive_pri_acct_inf
    * Code by Xue
    *
    * @param sqlContext
    * @return
    */
  def JOB_DM_86 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("#### JOB_DM_86(dm_user_real_name->hive_pri_acct_inf)")
    DateUtils.timeCost("JOB_DM_86") {
      UPSQL_JDBC.delete(s"dm_user_real_name","report_dt",start_dt,end_dt)
      println("#### JOB_DM_86 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      var today_dt=start_dt
      if(interval>=0 ){
        println(s"#### JOB_DM_86 spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())
        sqlContext.sql(s"use $hive_dbname")
        for(i <- 0 to interval){
          val results = sqlContext.sql(
            s"""
               |select
               |tempb.phone_location as branch_nm,
               |'$today_dt' as report_dt,
               |tempb.stock_num as stock_num,
               |tempb.today_num as today_num,
               |tempb.total_num as total_num
               |from
               |(
               |select
               |nvl(a.phone_location,a.phone_location) as phone_location,
               |count(distinct (case when to_date(a.rec_upd_ts) > to_date(a.rec_crt_ts) then a.cdhd_usr_id end)) as stock_num,
               |count(distinct (case when to_date(a.rec_upd_ts) = to_date(a.rec_crt_ts) then a.cdhd_usr_id end)) as today_num,
               |b.total_num as total_num
               |from
               |(
               |select distinct
               |cdhd_usr_id,
               |phone_location,
               |rec_upd_ts,
               |rec_crt_ts
               |from hive_pri_acct_inf
               |where
               |to_date(rec_upd_ts)>='$today_dt'
               |and to_date(rec_upd_ts)<='$today_dt'
               |and (usr_st='1' or (usr_st='2' and note='bdyx_freeze')) and  realnm_in='01'
               |) a
               |full outer join
               |(
               |select
               |tempa.phone_location as phone_location,
               |count(distinct tempa.cdhd_usr_id) as total_num
               |from
               |(
               |select
               |cdhd_usr_id,
               |phone_location
               |from
               |hive_pri_acct_inf
               |where  to_date(rec_upd_ts)<='$today_dt' and
               |(usr_st='1' or (usr_st='2' and note='BDYX_FREEZE'))
               |and  realnm_in='01'
               |)tempa
               |group by tempa.phone_location
               |) b
               |on a.phone_location=b.phone_location
               |group by nvl(a.phone_location,a.phone_location),b.total_num
               |)tempb
               | """.stripMargin)

          println(s"#### JOB_DM_86 spark sql 清洗[$today_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())

          if (!Option(results).isEmpty) {
            results.save2Mysql("dm_user_real_name")
            println(s"#### JOB_DM_86 [$today_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime()
            )
          } else {
            println(s"#### JOB_DM_86 spark sql 清洗[$today_dt]数据无结果集！")
          }
          today_dt=DateUtils.addOneDay(today_dt)
        }
      }
    }
  }


  /**
    * JobName:JOB_DM_87
    * Feature:dm_cashier_stat_dly ->
    * FROM :
    * cup_branch_ins_id_nm
    * hive_cashier_point_acct_oper_dtl
    * hive_cashier_bas_inf
    * hive_cdhd_cashier_maktg_reward_dtl
    * hive_signer_log
    *
    * @author tzq
    * @time 2016-9-7
    * @param sqlContext,start_dt,end_dt,interval
    */
  def JOB_DM_87(implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {

    println("###JOB_DM_87(dm_cashier_stat_dly->hive_cashier_bas_inf+cup_branch_ins_id_nm+hive_cashier_point_acct_oper_dtl+hive_cdhd_cashier_maktg_reward_dtl+hive_signer_log)")
    DateUtils.timeCost("JOB_DM_87"){
      UPSQL_JDBC.delete("dm_cashier_stat_dly","report_dt",start_dt,end_dt);
      println( "#### JOB_DM_87 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())

      var today_dt=start_dt
      if(interval>=0 ){
        sqlContext.sql(s"use $hive_dbname")
        for(i <- 0 to interval)

        println(s"#### JOB_DM_87 spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())
        val results=sqlContext.sql(
          s"""
             |select
             |    t.cup_branch_ins_id_nm as cup_branch_ins_id_nm,
             |    t.report_dt as report_dt,
             |    sum(t.cashier_cnt_tot) as cashier_cnt_tot,
             |    sum(t.act_cashier_cnt_tot) as act_cashier_cnt_tot,
             |    sum(t.non_act_cashier_cnt_tot) as non_act_cashier_cnt_tot,
             |    sum(t.cashier_cnt_year) as cashier_cnt_year,
             |    sum(t.act_cashier_cnt_year) as act_cashier_cnt_year,
             |    sum(t.non_act_cashier_cnt_year)as non_act_cashier_cnt_year,
             |    sum(t.cashier_cnt_mth) as cashier_cnt_mth,
             |    sum(t.act_cashier_cnt_mth) as act_cashier_cnt_mth,
             |    sum(t.non_act_cashier_cnt_mth) as non_act_cashier_cnt_mth,
             |    sum(t.pnt_acct_cashier_cnt_tot) as pnt_acct_cashier_cnt_tot,
             |    sum(t.reward_cashier_cnt_tot) as reward_cashier_cnt_tot,
             |    sum(t.reward_cdhd_cashier_cnt_tot) as reward_cdhd_cashier_cnt_tot,
             |    sum(t.sign_cashier_cnt_dly) as sign_cashier_cnt_dly,
             |    sum(t.cashier_cnt_dly) as cashier_cnt_dly,
             |    sum(t.act_cashier_cnt_dly) as act_cashier_cnt_dly,
             |    sum(t.non_act_cashier_cnt_dly) as non_act_cashier_cnt_dly
             |from
             |    (
             |        select
             |            case
             |                when a11.cup_branch_ins_id_nm is null
             |                then
             |                    case
             |                        when a12.cup_branch_ins_id_nm is null
             |                        then
             |                            case
             |                                when a13.cup_branch_ins_id_nm is null
             |                                then
             |                                    case
             |                                        when a21.cup_branch_ins_id_nm is null
             |                                        then
             |                                            case
             |                                                when a22.cup_branch_ins_id_nm is null
             |                                                then
             |                                                    case
             |                                                        when a23.cup_branch_ins_id_nm is null
             |                                                        then
             |                                                            case
             |                                                                when a31.cup_branch_ins_id_nm
             |                                                                    is null
             |                                                                then
             |                                                                    case
             |                                                                        when
             |                                                                            a32.cup_branch_ins_id_nm
             |                                                                            is null
             |                                                                        then
             |                                                                            case
             |                                                                                when
             |                                                                                    a33.cup_branch_ins_id_nm
             |                                                                                    is null
             |                                                                                then
             |                                                                                    case
             |                                                                                        when
             |                                                                                            a4.cup_branch_ins_id_nm
             |                                                                                            is null
             |                                                                                        then
             |                                                                                            case
             |                                                                                                when
             |                                                                                                    a5.cup_branch_ins_id_nm
             |                                                                                                    is null
             |                                                                                                then
             |                                                                                                    case
             |                                                                                                        when
             |                                                                                                            a6.cup_branch_ins_id_nm
             |                                                                                                            is null
             |                                                                                                        then
             |                                                                                                            case
             |                                                                                                                when
             |                                                                                                                    a7.cup_branch_ins_id_nm
             |                                                                                                                    is null
             |                                                                                                                then
             |                                                                                                                    case
             |                                                                                                                        when
             |                                                                                                                            a81.cup_branch_ins_id_nm
             |                                                                                                                            is null
             |                                                                                                                        then
             |                                                                                                                            case
             |                                                                                                                                when
             |                                                                                                                                    a82.cup_branch_ins_id_nm
             |                                                                                                                                    is null
             |                                                                                                                                then
             |                                                                                                                                    case
             |                                                                                                                                        when
             |                                                                                                                                            a83.cup_branch_ins_id_nm
             |                                                                                                                                            is null
             |                                                                                                                                        then
             |                                                                                                                                        		'????'
             |                                                                                                                                        else
             |                                                                                                                                            a83.cup_branch_ins_id_nm
             |                                                                                                                                    end
             |                                                                                                                                else
             |                                                                                                                                    a82.cup_branch_ins_id_nm
             |                                                                                                                            end
             |                                                                                                                        else
             |                                                                                                                            a81.cup_branch_ins_id_nm
             |                                                                                                                    end
             |                                                                                                                else
             |                                                                                                                    a7.cup_branch_ins_id_nm
             |                                                                                                            end
             |                                                                                                        else
             |                                                                                                            a6.cup_branch_ins_id_nm
             |                                                                                                    end
             |                                                                                                else
             |                                                                                                    a5.cup_branch_ins_id_nm
             |                                                                                            end
             |                                                                                        else
             |                                                                                            a4.cup_branch_ins_id_nm
             |                                                                                    end
             |                                                                                else
             |                                                                                    a33.cup_branch_ins_id_nm
             |                                                                            end
             |                                                                        else
             |                                                                            a32.cup_branch_ins_id_nm
             |                                                                    end
             |                                                                else a31.cup_branch_ins_id_nm
             |                                                            end
             |                                                        else a23.cup_branch_ins_id_nm
             |                                                    end
             |                                                else a22.cup_branch_ins_id_nm
             |                                            end
             |                                        else a21.cup_branch_ins_id_nm
             |                                    end
             |                                else a13.cup_branch_ins_id_nm
             |                            end
             |                        else a12.cup_branch_ins_id_nm
             |                    end
             |                else a11.cup_branch_ins_id_nm
             |            end                                                   as cup_branch_ins_id_nm,
             |            '$today_dt'                                           as report_dt,
             |            if(a11.cashier_cnt_tot is null,0,a11.cashier_cnt_tot)         as cashier_cnt_tot,
             |            if(a12.act_cashier_cnt_tot is null,0,a12.act_cashier_cnt_tot)    as act_cashier_cnt_tot,
             |            if(a13.non_act_cashier_cnt_tot is null,0,a13.non_act_cashier_cnt_tot) as
             |                                                                       non_act_cashier_cnt_tot,
             |            if(a21.cashier_cnt_year is null,0,a21.cashier_cnt_year)         as cashier_cnt_year,
             |            if(a22.act_cashier_cnt_year is null,0,a22.act_cashier_cnt_year) as act_cashier_cnt_year
             |            ,
             |            if(a23.non_act_cashier_cnt_year is null,0,a23.non_act_cashier_cnt_year) as
             |                                                                     non_act_cashier_cnt_year,
             |            if(a31.cashier_cnt_mth is null,0,a31.cashier_cnt_mth)         as cashier_cnt_mth,
             |            if(a32.act_cashier_cnt_mth is null,0,a32.act_cashier_cnt_mth)    as act_cashier_cnt_mth,
             |            if(a33.non_act_cashier_cnt_mth is null,0,a33.non_act_cashier_cnt_mth) as
             |            non_act_cashier_cnt_mth,
             |            if(a4.pnt_acct_cashier_cnt is null,0,a4.pnt_acct_cashier_cnt) as
             |            pnt_acct_cashier_cnt_tot,
             |            if(a5.reward_cashier_cnt_tot is null,0,a5.reward_cashier_cnt_tot) as
             |            reward_cashier_cnt_tot,
             |            if(a6.reward_cdhd_cashier_cnt_tot is null,0,a6.reward_cdhd_cashier_cnt_tot) as
             |                                                                        reward_cdhd_cashier_cnt_tot,
             |            if(a7.sign_cashier_cnt_dly is null,0,a7.sign_cashier_cnt_dly)   as sign_cashier_cnt_dly,
             |            if(a81.cashier_cnt_dly is null,0,a81.cashier_cnt_dly)                as cashier_cnt_dly,
             |            if(a82.act_cashier_cnt_dly is null,0,a82.act_cashier_cnt_dly)    as act_cashier_cnt_dly,
             |            if(a83.non_act_cashier_cnt_dly is null,0,a83.non_act_cashier_cnt_dly) as
             |            non_act_cashier_cnt_dly
             |        from
             |            (
             |                select
             |                    cup_branch_ins_id_nm,
             |                    count(distinct cashier_usr_id) as cashier_cnt_tot
             |                from
             |                    hive_cashier_bas_inf
             |                where
             |                    to_date(reg_dt)<= '$today_dt'
             |                and usr_st not in ('4',
             |                                   '9')
             |                group by
             |                    cup_branch_ins_id_nm) a11
             |        full outer join
             |            (
             |                select
             |                    cup_branch_ins_id_nm,
             |                    count(distinct cashier_usr_id) as act_cashier_cnt_tot
             |                from
             |                    hive_cashier_bas_inf
             |                where
             |                    to_date(activate_ts) <= '$today_dt'
             |                and usr_st in ('1')
             |                group by
             |                    cup_branch_ins_id_nm) a12
             |        on
             |            (
             |                a11.cup_branch_ins_id_nm = a12.cup_branch_ins_id_nm)
             |        full outer join
             |            (
             |                select
             |                    cup_branch_ins_id_nm,
             |                    count(distinct cashier_usr_id) as non_act_cashier_cnt_tot
             |                from
             |                    hive_cashier_bas_inf
             |                where
             |                    to_date(activate_ts)<= '$today_dt'
             |                and usr_st in ('0')
             |                group by
             |                    cup_branch_ins_id_nm) a13
             |        on
             |            (
             |                a11.cup_branch_ins_id_nm = a13.cup_branch_ins_id_nm)
             |        full outer join
             |            (
             |                select
             |                    cup_branch_ins_id_nm,
             |                    count(distinct cashier_usr_id) as cashier_cnt_year
             |                from
             |                    hive_cashier_bas_inf
             |                where
             |                    to_date(reg_dt) <= '$today_dt'
             |                and to_date(reg_dt) >= concat(substring('$today_dt',1,5),'01-01')
             |                and usr_st not in ('4',
             |                                   '9')
             |                group by
             |                    cup_branch_ins_id_nm) a21
             |        on
             |            (
             |                a11.cup_branch_ins_id_nm = a21.cup_branch_ins_id_nm)
             |        full outer join
             |            (
             |                select
             |                    cup_branch_ins_id_nm,
             |                    count(distinct cashier_usr_id) as act_cashier_cnt_year
             |                from
             |                    hive_cashier_bas_inf
             |                where
             |                    to_date(activate_ts) <= '$today_dt'
             |                and to_date(activate_ts) >= concat(substring('$today_dt',1,5),'01-01')
             |                and usr_st in ('1')
             |                group by
             |                    cup_branch_ins_id_nm) a22
             |        on
             |            (
             |                a11.cup_branch_ins_id_nm = a22.cup_branch_ins_id_nm)
             |        full outer join
             |            (
             |                select
             |                    cup_branch_ins_id_nm,
             |                    count(distinct cashier_usr_id) as non_act_cashier_cnt_year
             |                from
             |                    hive_cashier_bas_inf
             |                where
             |                    to_date(activate_ts) <= '$today_dt'
             |                and to_date(activate_ts) >= concat(substring('$today_dt',1,5),'01-01')
             |                and usr_st in ('0')
             |                group by
             |                    cup_branch_ins_id_nm) a23
             |        on
             |            (
             |                a11.cup_branch_ins_id_nm = a23.cup_branch_ins_id_nm)
             |        full outer join
             |            (
             |                select
             |                    cup_branch_ins_id_nm,
             |                    count(distinct cashier_usr_id) as cashier_cnt_mth
             |                from
             |                    hive_cashier_bas_inf
             |                where
             |                    to_date(reg_dt) <= '$today_dt'
             |                and to_date(reg_dt) >= concat(substring('$today_dt',1,8),'01')
             |                and usr_st not in ('4',
             |                                   '9')
             |                group by
             |                    cup_branch_ins_id_nm) a31
             |        on
             |            (
             |                a11.cup_branch_ins_id_nm = a31.cup_branch_ins_id_nm)
             |        full outer join
             |            (
             |                select
             |                    cup_branch_ins_id_nm,
             |                    count(distinct cashier_usr_id) as act_cashier_cnt_mth
             |                from
             |                    hive_cashier_bas_inf
             |                where
             |                    to_date(activate_ts) <= '$today_dt'
             |                and to_date(activate_ts) >= concat(substring('$today_dt',1,8),'01')
             |                and usr_st in ('1')
             |                group by
             |                    cup_branch_ins_id_nm) a32
             |        on
             |            (
             |                a11.cup_branch_ins_id_nm = a32.cup_branch_ins_id_nm)
             |        full outer join
             |            (
             |                select
             |                    cup_branch_ins_id_nm,
             |                    count(distinct cashier_usr_id) as non_act_cashier_cnt_mth
             |                from
             |                    hive_cashier_bas_inf
             |                where
             |                    to_date(activate_ts) <= '$today_dt'
             |                and to_date(activate_ts) >= concat(substring('$today_dt',1,8),'01')
             |                and usr_st in ('0')
             |                group by
             |                    cup_branch_ins_id_nm) a33
             |        on
             |            (
             |                a11.cup_branch_ins_id_nm = a33.cup_branch_ins_id_nm)
             |        full outer join
             |            (
             |                select
             |                    b.cup_branch_ins_id_nm,
             |                    count(distinct a.cashier_usr_id) as pnt_acct_cashier_cnt
             |                from
             |                    (
             |                        select distinct
             |                            cashier_usr_id
             |                        from
             |                            hive_cashier_point_acct_oper_dtl
             |                        where
             |                            to_date(acct_oper_ts) <= '$today_dt'
             |                        and to_date(acct_oper_ts) >= trunc('$today_dt','yyyy') )a
             |                inner join
             |                    (
             |                        select
             |                            cup_branch_ins_id_nm,
             |                            cashier_usr_id
             |                        from
             |                            hive_cashier_bas_inf
             |                        where
             |                            to_date(reg_dt)<= '$today_dt'
             |                        and usr_st not in ('4',
             |                                           '9') )b
             |                on
             |                    a.cashier_usr_id=b.cashier_usr_id
             |                group by
             |                    b. cup_branch_ins_id_nm) a4
             |        on
             |            (
             |                a11.cup_branch_ins_id_nm = a4.cup_branch_ins_id_nm)
             |        full outer join
             |            (
             |                select
             |                    b.cup_branch_ins_id_nm,
             |                    count(distinct b.cashier_usr_id) as reward_cashier_cnt_tot
             |                from
             |                    (
             |                        select
             |                            mobile
             |                        from
             |                            hive_cdhd_cashier_maktg_reward_dtl
             |                        where
             |                            to_date(settle_dt) <= '$today_dt'
             |                        and rec_st='2'
             |                        and activity_tp='004'
             |                        group by
             |                            mobile ) a
             |                inner join
             |                    hive_cashier_bas_inf b
             |                on
             |                    a.mobile=b.mobile
             |                group by
             |                    b.cup_branch_ins_id_nm) a5
             |        on
             |            (
             |                a11.cup_branch_ins_id_nm = a5.cup_branch_ins_id_nm)
             |        full outer join
             |            (
             |                select
             |                    b.cup_branch_ins_id_nm,
             |                    count(distinct b.cashier_usr_id) reward_cdhd_cashier_cnt_tot
             |                from
             |                    (
             |                        select
             |                            mobile
             |                        from
             |                            hive_cdhd_cashier_maktg_reward_dtl
             |                        where
             |                            to_date(settle_dt) <= '$today_dt'
             |                        and rec_st='2'
             |                        and activity_tp='004'
             |                        group by
             |                            mobile ) a
             |                inner join
             |                    hive_cashier_bas_inf b
             |                on
             |                    a.mobile=b.mobile
             |                inner join
             |                    hive_pri_acct_inf c
             |                on
             |                    a.mobile=c.mobile
             |                where
             |                    c.usr_st='1'
             |                group by
             |                    b.cup_branch_ins_id_nm) a6
             |        on
             |            (
             |                a11.cup_branch_ins_id_nm = a6.cup_branch_ins_id_nm)
             |        full outer join
             |            (
             |                select
             |                    b.cup_branch_ins_id_nm,
             |                    count(distinct b.cashier_usr_id) as sign_cashier_cnt_dly
             |                from
             |                    (
             |                        select
             |                            pri_acct_no
             |                        from
             |                            hive_signer_log
             |                        where
             |                            concat_ws('-',substr(cashier_trans_tm,1,4),substr(cashier_trans_tm,5,2)
             |                            ,substr (cashier_trans_tm,7,2)) = '$today_dt'
             |                        group by
             |                            pri_acct_no ) a
             |                inner join
             |                    (
             |                        select
             |                            cup_branch_ins_id_nm,
             |                            cashier_usr_id,
             |                            bind_card_no
             |                        from
             |                            hive_cashier_bas_inf
             |                        where
             |                            to_date(reg_dt) <= '$today_dt'
             |                        and usr_st not in ('4',
             |                                           '9') ) b
             |                on
             |                    a.pri_acct_no=b.bind_card_no
             |                group by
             |                    b.cup_branch_ins_id_nm) a7
             |        on
             |            (
             |                a11.cup_branch_ins_id_nm = a7.cup_branch_ins_id_nm)
             |        full outer join
             |            (
             |                select
             |                    cup_branch_ins_id_nm,
             |                    count(distinct cashier_usr_id) as cashier_cnt_dly
             |                from
             |                    hive_cashier_bas_inf
             |                where
             |                    to_date(reg_dt) = '$today_dt'
             |                and usr_st not in ('4',
             |                                   '9')
             |                group by
             |                    cup_branch_ins_id_nm) a81
             |        on
             |            (
             |                a11.cup_branch_ins_id_nm = a81.cup_branch_ins_id_nm)
             |        full outer join
             |            (
             |                select
             |                    cup_branch_ins_id_nm,
             |                    count(distinct cashier_usr_id) as act_cashier_cnt_dly
             |                from
             |                    hive_cashier_bas_inf
             |                where
             |                    to_date(activate_ts) = '$today_dt'
             |                and usr_st in ('1')
             |                group by
             |                    cup_branch_ins_id_nm) a82
             |        on
             |            (
             |                a11.cup_branch_ins_id_nm = a82.cup_branch_ins_id_nm)
             |        full outer join
             |            (
             |                select
             |                    cup_branch_ins_id_nm,
             |                    count(distinct cashier_usr_id) as non_act_cashier_cnt_dly
             |                from
             |                    hive_cashier_bas_inf
             |                where
             |                    to_date(activate_ts) = '$today_dt'
             |                and usr_st in ('0')
             |                group by
             |                    cup_branch_ins_id_nm) a83
             |        on
             |            (
             |                a11.cup_branch_ins_id_nm = a83.cup_branch_ins_id_nm)) t
             |where
             |    t.cup_branch_ins_id_nm is not null
             |group by
             |    t.cup_branch_ins_id_nm,
             |    t.report_dt
      """.stripMargin)
        println(s"#### JOB_DM_87 spark sql 清洗[$today_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())

        println(s"###JOB_DM_87------$today_dt results:"+results.count())
        if(!Option(results).isEmpty){
          results.save2Mysql("dm_cashier_stat_dly")
          println(s"#### JOB_DM_87 [$today_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
        }else{
          println(s"#### JOB_DM_87 spark sql 清洗[$today_dt]数据无结果集！")
        }

        today_dt=DateUtils.addOneDay(today_dt)
      }
    }

  }

  /**
    * JobName: JOB_DM_88
    * Feature: DM_UNIONPAY_RED_DOMAIN_BRANCH
    *
    * @author tzq
    * @time 2016-12-26
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    * @param interval
    */
  def JOB_DM_88 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("###JOB_DM_88(DM_UNIONPAY_RED_DOMAIN_BRANCH)### "+DateUtils.getCurrentSystemTime())
    DateUtils.timeCost("JOB_DM_88"){
      UPSQL_JDBC.delete(s"DM_UNIONPAY_RED_DOMAIN_BRANCH","REPORT_DT",start_dt,end_dt)
      println( "#### JOB_DM_88 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      var today_dt=start_dt
      if(interval>=0 ){
        sqlContext.sql(s"use $hive_dbname")
        for(i <- 0 to interval){
          println(s"#### JOB_DM_88 spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())
          val results =sqlContext.sql(
            s"""
               |select
               |a.access_nm as chan_nm,
               |a.starttime_day as report_dt,
               |count(case when eventid='registersubmit' then a.tduserid end) as acc_num,
               |count(distinct(case when eventid='registersubmit' then a.tduserid end)) as acc_devc_num,
               |count(case when eventid='registersubmit' then a.relate_id end) as submit_num,
               |count(distinct(case when eventid='registersubmit' then a.relate_id end)) as submit_devc_num,
               |count(case when eventid='registersuccess' then a.tduserid end) as reg_scc_num,
               |count(distinct(case when eventid='registersuccess' then a.tduserid end)) as reg_scc_submit_usr_num,
               |count(distinct(case when eventid='registersuccess' then a.relate_id end)) as reg_scc_usr_num
               |from(
               |select
               |tdapp.tduserid,
               |tdapp.eventid,
               |td.relate_id,
               |tdapp.starttime_day,
               |sour.access_nm
               |from
               |(select
               |tduserid, eventid,eventcount,partnerid,starttime_day,
               |from_unixtime(starttime,'yyyy-MM-dd HH:mm:ss') as starttime
               |from hive_org_tdapp_tappevent
               |where starttime_day>='$today_dt' and starttime_day<='$today_dt'
               |and eventid in ('registerapply','registersubmit','registersuccess') ) tdapp
               |left join
               |(select
               |tduser_id ,
               |relate_id ,
               |start_dt ,
               |end_dt
               |from hive_use_td_d
               |) td
               |on tdapp.tduserid=td.tduser_id
               |left join hive_inf_source_dtl sour
               |on tdapp.partnerid=sour.access_id
               |where tdapp.starttime between start_dt and end_dt
               |) a
               |group by a.access_nm,a.starttime_day
               |
          """.stripMargin)
          println(s"#### JOB_DM_88 spark sql 清洗[$today_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())

          println(s"###JOB_DM_88------$today_dt results:"+results.count())
          if(!Option(results).isEmpty){
            results.save2Mysql("DM_UNIONPAY_RED_DOMAIN_BRANCH")
            println(s"#### JOB_DM_88 [$today_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
          }else{
            println(s"#### JOB_DM_88 spark sql 清洗[$today_dt]数据无结果集！")
          }
          today_dt=DateUtils.addOneDay(today_dt)
        }
      }
    }
  }

  /**
    * JobName: JOB_DM_89
    * Feature: DM_USR_REGISTER_PHOEN_AREA
    *
    * @author tzq
    * @time 2016-12-26
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    * @param interval
    */
  def JOB_DM_89 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("###JOB_DM_89(DM_USR_REGISTER_PHOEN_AREA)### "+DateUtils.getCurrentSystemTime())
    DateUtils.timeCost("JOB_DM_89"){
      UPSQL_JDBC.delete(s"DM_USR_REGISTER_PHOEN_AREA","REPORT_DT",start_dt,end_dt)
      println( "#### JOB_DM_89 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      var today_dt=start_dt
      if(interval>=0 ){
        sqlContext.sql(s"use $hive_dbname")
        for(i <- 0 to interval){
          println(s"#### JOB_DM_89 spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())
          val results =sqlContext.sql(
            s"""
               |select
               |a.phone_location as area_nm,
               |a.starttime_day as report_dt,
               |count(case when eventid='registersubmit' then a.tduserid end) as acc_num,
               |count(distinct(case when eventid='registersubmit' then a.tduserid end)) as acc_devc_num,
               |count(case when eventid='registersubmit' then a.relate_id end) as submit_num,
               |count(distinct(case when eventid='registersubmit' then a.relate_id end)) as submit_devc_num,
               |count(case when eventid='registersuccess' then a.tduserid end) as reg_scc_num,
               |count(distinct(case when eventid='registersuccess' then a.tduserid end)) as reg_scc_submit_usr_num,
               |count(distinct(case when eventid='registersuccess' then a.relate_id end)) as reg_scc_usr_num
               |from(
               |select
               |tdapp.tduserid,
               |tdapp.eventid,
               |td.relate_id,
               |tdapp.starttime_day,
               |pri.phone_location
               |from
               |(select
               |tduserid, eventid,eventcount,partnerid,starttime_day,
               |from_unixtime(starttime,'yyyy-MM-dd HH:mm:ss') as starttime
               |from hive_org_tdapp_tappevent
               |where starttime_day>='$today_dt' and starttime_day<='$today_dt'
               |and eventid in ('registerapply','registersubmit','registersuccess') ) tdapp
               |left join
               |(select
               |tduser_id ,
               |relate_id ,
               |start_dt ,
               |end_dt
               |from hive_use_td_d
               |) td
               |on tdapp.tduserid=td.tduser_id
               |left join hive_pri_acct_inf pri
               |on td.relate_id=pri.relate_id
               |where tdapp.starttime between start_dt and end_dt
               |) a
               |group by a.phone_location,a.starttime_day
               |
          """.stripMargin)
          println(s"#### JOB_DM_89 spark sql 清洗[$today_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())

          println(s"###JOB_DM_89------$today_dt results:"+results.count())
          if(!Option(results).isEmpty){
            results.save2Mysql("DM_USR_REGISTER_PHOEN_AREA")
            println(s"#### JOB_DM_89 [$today_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
          }else{
            println(s"#### JOB_DM_89 spark sql 清洗[$today_dt]数据无结果集！")
          }
          today_dt=DateUtils.addOneDay(today_dt)
        }
      }
    }
  }

  /**
    * JOB_DM_90 20161227
    * dm_disc_tkt_act_link_tp_dly->hive_acc_trans
    * Code by Xue
    *
    * @param sqlContext
    * @return
    */
  def JOB_DM_90(implicit sqlContext: HiveContext, start_dt: String, end_dt: String, interval: Int) = {
    println("###JOB_DM_90(dm_usr_register_device_loc->hive_acc_trans)")
    DateUtils.timeCost("JOB_DM_90") {
      UPSQL_JDBC.delete(s"dm_usr_register_device_loc", "report_dt", start_dt, end_dt)
      println("#### JOB_DM_90 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      println(s"#### JOB_DM_90  spark sql 清洗数据开始时间为:" + DateUtils.getCurrentSystemTime())
      sqlContext.sql(s"use $hive_dbname")
      val results = sqlContext.sql(
        s"""
           |select
           |a.gb_region_nm as device_loc_nm,
           |a.starttime_day as report_dt,
           |count(case when a.eventid='registersubmit' then  a.tduser_id end) as acc_num,
           |count(distinct(case when a.eventid='registersubmit' then  a.tduser_id end)) as acc_devc_num,
           |count(case when a.eventid='registersubmit' then  a.relate_id end) as  submit_num,
           |count(distinct(case when a.eventid='registersubmit' then  a.relate_id end)) as submit_devc_num,
           |count(case when a.eventid='registersuccess' then  a.tduser_id end) as reg_scc_num,
           |count(distinct(case when a.eventid='registersuccess' then  a.tduser_id end)) as reg_scc_submit_usr_num,
           |count(distinct(case when a.eventid='registersuccess' then  a.relate_id end)) as reg_scc_usr_num
           |from
           |(
           |select
           |tdapp.tduser_id,
           |tdapp.eventid,
           |td.relate_id,
           |tdapp.starttime_day,
           |pri.gb_region_nm
           |from
           |(
           |select
           |tape.tduserid as tduser_id,
           |tape.eventid as eventid,
           |tape.eventcount as eventcount,
           |tape.partnerid as partnerid,
           |tape.starttime_day as starttime_day,
           |from_unixtime(tape.starttime,'yyyy-mm-dd hh:mm:ss')  as starttime
           |from hive_org_tdapp_tappevent tape
           |where tape.starttime_day>='$start_dt'  and tape.starttime_day<='$end_dt'
           |and tape.eventid in ('registerapply','registersubmit','registersuccess')
           |) tdapp
           |left join
           |(select
           |tduser_id ,
           |relate_id ,
           |start_dt ,
           |end_dt
           |from hive_use_td_d
           |) td
           |on tdapp.tduser_id=td.tduser_id
           |left join
           |(
           |select
           |relate_id,
           |(case
           |when gb_region_cd like '2102' then '大连'
           |when gb_region_cd like '3302' then '宁波'
           |when gb_region_cd like '3502' then '厦门'
           |when gb_region_cd like '3702' then '青岛'
           |when gb_region_cd like '4403' then '深圳'
           |when gb_region_cd like '11%' then '北京'
           |when gb_region_cd like '12%' then '天津'
           |when gb_region_cd like '13%' then '河北'
           |when gb_region_cd like '14%' then '山西'
           |when gb_region_cd like '15%' then '内蒙古'
           |when gb_region_cd like '21%' then '辽宁'
           |when gb_region_cd like '22%' then '吉林'
           |when gb_region_cd like '23%' then '黑龙江'
           |when gb_region_cd like '31%' then '上海'
           |when gb_region_cd like '32%' then '江苏'
           |when gb_region_cd like '33%' then '浙江'
           |when gb_region_cd like '34%' then '安徽'
           |when gb_region_cd like '35%' then '福建'
           |when gb_region_cd like '36%' then '江西'
           |when gb_region_cd like '37%' then '山东'
           |when gb_region_cd like '41%' then '河南'
           |when gb_region_cd like '42%' then '湖北'
           |when gb_region_cd like '43%' then '湖南'
           |when gb_region_cd like '44%' then '广东'
           |when gb_region_cd like '45%' then '广西'
           |when gb_region_cd like '46%' then '海南'
           |when gb_region_cd like '50%' then '重庆'
           |when gb_region_cd like '51%' then '四川'
           |when gb_region_cd like '52%' then '贵州'
           |when gb_region_cd like '53%' then '云南'
           |when gb_region_cd like '54%' then '西藏'
           |when gb_region_cd like '61%' then '陕西'
           |when gb_region_cd like '62%' then '甘肃'
           |when gb_region_cd like '63%' then '青海'
           |when gb_region_cd like '64%' then '宁夏'
           |when gb_region_cd like '65%' then '新疆'
           |else '其他' end) as gb_region_nm
           |from
           |hive_pri_acct_inf )pri
           |on td.relate_id=pri.relate_id
           |where tdapp.starttime>=td.start_dt and tdapp.starttime<=td.end_dt
           |) a
           |group by a.gb_region_nm,a.starttime_day
           | """.stripMargin)
      println(s"#### JOB_DM_90 spark sql 清洗数据完成时间为:" + DateUtils.getCurrentSystemTime())

      if (!Option(results).isEmpty) {
        results.save2Mysql("dm_usr_register_device_loc")
        println(s"#### JOB_DM_90 数据插入完成时间为：" + DateUtils.getCurrentSystemTime()
        )
      } else {
        println(s"#### JOB_DM_90 spark sql 清洗数据无结果集！")
      }
    }
  }


  /**
    * JOB_DM_91 20161229
    *
    * dm_coupon_pub_down_stat->hive_download_trans,hive_ticket_bill_bas_inf
    * Code by Xue
    *
    * @param sqlContext
    * @return
    */
  def JOB_DM_91(implicit sqlContext: HiveContext, start_dt: String, end_dt: String, interval: Int) = {
    println("###JOB_DM_91(dm_coupon_pub_down_stat->hive_download_trans,hive_ticket_bill_bas_inf)")
    DateUtils.timeCost("JOB_DM_91") {
      UPSQL_JDBC.delete(s"dm_coupon_pub_down_stat", "report_dt", start_dt, end_dt)
      println("#### JOB_DM_91 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      var today_dt = start_dt
      if (interval >= 0) {
        println(s"#### JOB_DM_91  spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())
        sqlContext.sql(s"use $hive_dbname")
        for (i <- 0 to interval) {
          val results = sqlContext.sql(
            s"""
               |select
               |case when a.cup_branch_ins_id_nm is null then b.cup_branch_ins_id_nm else a.cup_branch_ins_id_nm end as branch_nm,
               |case when a.if_hce is null then b.if_hce else a.if_hce end as if_hce,
               |'$today_dt' as report_dt,
               |sum(a.coupon_class) as coupon_class,
               |sum(a.coupon_pub_num) as coupon_pub_num,
               |sum(a.coupon_dow_num) as coupon_dow_num,
               |sum(b.batch_num) as batch_num,
               |sum(a.dow_usr_num) as dow_usr_num,
               |sum(b.batch_sur_num) as batch_sur_num
               |from
               |(select bill.cup_branch_ins_id_nm,
               |case when substr(udf_fld,31,2) in ('01','02','03','04','05') then '仅限云闪付' else '非仅限云闪付' end as if_hce,
               |count(*) as coupon_class ,
               |sum(case when bill.dwn_total_num = -1 then bill.dwn_num else bill.dwn_total_num end) as coupon_pub_num ,
               |sum(bill.dwn_num) as coupon_dow_num,
               |sum(dtl.cdhd_usr_id) as dow_usr_num
               |from hive_download_trans as dtl,
               |hive_ticket_bill_bas_inf as bill
               |where dtl.bill_id=bill.bill_id
               |and dtl.um_trans_id in ('12','17')
               |and dtl.trans_st='1' and to_date(dtl.trans_dt)>='$today_dt' and to_date(dtl.trans_dt)<='$today_dt' and dtl.bill_nm not like '%机场%' and dtl.bill_nm not like '%住两晚送一晚%'
               |and to_date(bill.rec_crt_ts)>='$today_dt' and to_date(bill.rec_crt_ts)<='$today_dt'
               |and bill.bill_sub_tp in ('01','03') and bill.bill_nm not like '%测试%' and bill.bill_nm not like '%验证%' and bill.bill_id <>'Z00000000020415'
               |and bill.bill_id<>'Z00000000020878' and bill.bill_nm not like '%满2元减1%' and bill.bill_nm not like '%满2分减1分%'
               |and bill.bill_nm not like '%满2减1%' and bill.bill_nm not like '%满2抵1%' and bill.bill_nm not like '测%' and dwn_total_num-dwn_num<100000
               |group by bill.cup_branch_ins_id_nm, case when substr(udf_fld,31,2) in ('01','02','03','04','05') then '仅限云闪付' else '非仅限云闪付' end) a
               |full outer join
               |(
               |select b.cup_branch_ins_id_nm,
               |case when substr(b.udf_fld,31,2) in ('01','02','03','04','05') then '仅限云闪付' else '非仅限云闪付' end as if_hce,
               |sum(adj.adj_ticket_bill) as batch_num ,
               |sum(distinct b.cdhd_usr_id) as batch_sur_num
               |from
               |hive_ticket_bill_acct_adj_task adj
               |inner join
               |(select cup_branch_ins_id_nm,cdhd_usr_id,bill.bill_id,udf_fld
               |from hive_download_trans as dtl,
               |hive_ticket_bill_bas_inf as bill
               |where dtl.bill_id=bill.bill_id
               |and dtl.um_trans_id in ('12','17')
               |and dtl.trans_st='1' and to_date(dtl.trans_dt)>='$today_dt' and to_date(dtl.trans_dt)<='$today_dt' and dtl.bill_nm not like '%机场%' and dtl.bill_nm not like '%住两晚送一晚%'
               |and to_date(bill.rec_crt_ts)>='$today_dt' and to_date(bill.rec_crt_ts)<='$today_dt'
               |and bill.bill_sub_tp in ('01','03') and bill.bill_nm not like '%测试%' and bill.bill_nm not like '%验证%' and bill.bill_id <>'Z00000000020415'
               |and bill.bill_id<>'Z00000000020878' and bill.bill_nm not like '%满2元减1%' and bill.bill_nm not like '%满2分减1分%'
               |and bill.bill_nm not like '%满2减1%' and bill.bill_nm not like '%满2抵1%' and bill.bill_nm not like '测%' and dwn_total_num-dwn_num<100000
               |) b
               |on adj.bill_id=b.bill_id
               |group by b.cup_branch_ins_id_nm, case when substr(b.udf_fld,31,2) in ('01','02','03','04','05') then '仅限云闪付' else '非仅限云闪付' end )b
               |on a.cup_branch_ins_id_nm=b.cup_branch_ins_id_nm and a.if_hce=b.if_hce
               |
               |group by case when a.cup_branch_ins_id_nm is null then b.cup_branch_ins_id_nm else a.cup_branch_ins_id_nm end,
               |case when a.if_hce is null then b.if_hce else a.if_hce end
               | """.stripMargin)
          println(s"#### JOB_DM_91 spark sql 清洗[$today_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())

          if (!Option(results).isEmpty) {
            results.save2Mysql("dm_coupon_pub_down_stat")
            println(s"#### JOB_DM_91 [$today_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
          } else {
            println(s"#### JOB_DM_91 spark sql 清洗[$today_dt]数据无结果集！")
          }
          today_dt = DateUtils.addOneDay(today_dt)
        }
      }
    }
  }

  /**
    * JobName: JOB_DM_92
    * Feature: DM_ACPT_DIRECT_TRAN_STANDARD
    *
    * @author tzq
    * @time 2016-12-28
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    * @param interval
    */
  def JOB_DM_92 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("###JOB_DM_92(DM_ACPT_DIRECT_TRAN_STANDARD)### "+DateUtils.getCurrentSystemTime())
    DateUtils.timeCost("JOB_DM_92"){
      UPSQL_JDBC.delete(s"DM_ACPT_DIRECT_TRAN_STANDARD","REPORT_DT",start_dt,end_dt)
      println( "#### JOB_DM_92 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      var today_dt=start_dt
      if(interval>=0 ){
        sqlContext.sql(s"use $hive_dbname")
        for(i <- 0 to interval){
          println(s"#### JOB_DM_92 spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())
          val results =sqlContext.sql(
            s"""
               |select
               |project_name         as project_name,
               |if_hce               as if_hce,
               |report_dt            as report_dt,
               |sum(tran_cnt)        as tran_cnt,
               |sum(tran_succ_cnt)   as tran_succ_cnt,
               |sum(tran_succ_at)    as tran_succ_at,
               |sum(discount_at)     as discount_at,
               |sum(tran_usr_num )   as tran_usr_num,
               |sum(tran_card_num)   as tran_card_num
               |from
               |(select
               |a.project_name,
               |a.if_hce,
               |a.report_dt,
               |a.tran_cnt ,
               |b.tran_succ_cnt,
               |b.tran_succ_at,
               |b.discount_at,
               |b.tran_usr_num ,
               |b.tran_card_num
               |from
               |(select
               |case when fwd_ins_id_cd in ('00097310','00093600','00095210','00098700','00098500','00097700',
               |'00096400','00096500','00155800','00095840','00097000','00085500','00096900','00093930',
               |'00094200','00093900','00096100','00092210','00092220','00092900','00091600','00092400',
               |'00098800','00098200','00097900','00091900','00092600','00091200','00093320','00031000',
               |'00094500','00094900','00091100','00094520','00093000','00093310') then '直联' else '间联' end as project_name,
               |case when substr(udf_fld,31,2) in ('01','02','03','04','05') then '仅限云闪付' else '非仅限云闪付' end as if_hce,
               |to_date(trans_dt) as report_dt,
               |count(*) as tran_cnt
               |from hive_acc_trans
               |where to_date(trans_dt)>='$today_dt' and to_date(trans_dt)<='$today_dt'
               |and bill_nm not like '%机场%' and bill_nm not like '%住两晚送一晚%'
               |and bill_nm not like '%测试%' and bill_nm not like '%验证%' and bill_id <>'Z00000000020415'
               |and bill_id<>'Z00000000020878' and bill_nm not like '%满2元减1%' and bill_nm not like '%满2分减1分%'
               |and bill_nm not like '%满2减1%' and bill_nm not like '%满2抵1%' and bill_nm not like '测%'
               |group by case when fwd_ins_id_cd in ('00097310','00093600','00095210','00098700','00098500','00097700',
               |'00096400','00096500','00155800','00095840','00097000','00085500','00096900','00093930',
               |'00094200','00093900','00096100','00092210','00092220','00092900','00091600','00092400',
               |'00098800','00098200','00097900','00091900','00092600','00091200','00093320','00031000',
               |'00094500','00094900','00091100','00094520','00093000','00093310') then '直联' else '间联' end ,
               |case when substr(udf_fld,31,2) in ('01','02','03','04','05') then '仅限云闪付' else '非仅限云闪付' end,
               |to_date(trans_dt) ) a
               |left join
               |(select
               |case when fwd_ins_id_cd in ('00097310','00093600','00095210','00098700','00098500','00097700',
               |'00096400','00096500','00155800','00095840','00097000','00085500','00096900','00093930',
               |'00094200','00093900','00096100','00092210','00092220','00092900','00091600','00092400',
               |'00098800','00098200','00097900','00091900','00092600','00091200','00093320','00031000',
               |'00094500','00094900','00091100','00094520','00093000','00093310') then '直联' else '间联' end as project_name,
               |case when substr(udf_fld,31,2) in ('01','02','03','04','05') then '仅限云闪付' else '非仅限云闪付' end as if_hce,
               |to_date(trans_dt) as report_dt,
               |count(*) as tran_succ_cnt,
               |sum(trans_at) as tran_succ_at,
               |sum(discount_at) as discount_at,
               |count(cdhd_usr_id) as tran_usr_num ,
               |count(card_no) as tran_card_num
               |from hive_acc_trans
               |where to_date(trans_dt)>='$today_dt' and to_date(trans_dt)<='$today_dt'
               |and sys_det_cd='S'
               |and bill_nm not like '%机场%' and bill_nm not like '%住两晚送一晚%'
               |and bill_nm not like '%测试%' and bill_nm not like '%验证%' and bill_id <>'Z00000000020415'
               |and bill_id<>'Z00000000020878' and bill_nm not like '%满2元减1%' and bill_nm not like '%满2分减1分%'
               |and bill_nm not like '%满2减1%' and bill_nm not like '%满2抵1%' and bill_nm not like '测%'
               |group by case when fwd_ins_id_cd in ('00097310','00093600','00095210','00098700','00098500','00097700',
               |'00096400','00096500','00155800','00095840','00097000','00085500','00096900','00093930',
               |'00094200','00093900','00096100','00092210','00092220','00092900','00091600','00092400',
               |'00098800','00098200','00097900','00091900','00092600','00091200','00093320','00031000',
               |'00094500','00094900','00091100','00094520','00093000','00093310') then '直联' else '间联' end ,
               |case when substr(udf_fld,31,2) in ('01','02','03','04','05') then '仅限云闪付' else '非仅限云闪付' end,
               |to_date(trans_dt) ) b
               |on a.project_name=b.project_name and a.if_hce=b.if_hce and a.report_dt=b.report_dt
               |union all
               |select
               |a.project_name,
               |a.if_hce,
               |a.report_dt,
               |a.tran_cnt ,
               |b.tran_succ_cnt,
               |b.tran_succ_at,
               |b.discount_at,
               |b.tran_usr_num ,
               |b.tran_card_num
               |from
               |(select
               |case when internal_trans_tp='c00022' then '1.0 规范'
               |when internal_trans_tp='C20022' then '2.0 规范' else '--' end as project_name ,
               |case when substr(udf_fld,31,2) in ('01','02','03','04','05') then '仅限云闪付' else '非仅限云闪付' end as if_hce,
               |to_date(trans_dt) as report_dt,
               |count(*) as tran_cnt
               |from hive_acc_trans
               |where to_date(trans_dt)>='$today_dt' and to_date(trans_dt)<='$today_dt'
               |and bill_nm not like '%机场%' and bill_nm not like '%住两晚送一晚%'
               |and bill_nm not like '%测试%' and bill_nm not like '%验证%' and bill_id <>'Z00000000020415'
               |and bill_id<>'Z00000000020878' and bill_nm not like '%满2元减1%' and bill_nm not like '%满2分减1分%'
               |and bill_nm not like '%满2减1%' and bill_nm not like '%满2抵1%' and bill_nm not like '测%'
               |group by case when internal_trans_tp='c00022' then '1.0 规范'
               |when internal_trans_tp='C20022' then '2.0 规范' else '--' end,
               |case when substr(udf_fld,31,2) in ('01','02','03','04','05') then '仅限云闪付' else '非仅限云闪付' end,
               |to_date(trans_dt) ) a
               |left join
               |(select
               |case when internal_trans_tp='c00022' then '1.0 规范'
               |when internal_trans_tp='C20022' then '2.0 规范' else '--' end as project_name,
               |case when substr(udf_fld,31,2) in ('01','02','03','04','05') then '仅限云闪付' else '非仅限云闪付' end as if_hce,
               |to_date(trans_dt) as report_dt,
               |count(*) as tran_succ_cnt,
               |sum(trans_at) as tran_succ_at,
               |sum(discount_at) as discount_at,
               |count(cdhd_usr_id) as tran_usr_num ,
               |count(card_no) as tran_card_num
               |from hive_acc_trans
               |where to_date(trans_dt)>='$today_dt' and to_date(trans_dt)<='$today_dt'
               |and sys_det_cd='S'
               |and bill_nm not like '%机场%' and bill_nm not like '%住两晚送一晚%'
               |and bill_nm not like '%测试%' and bill_nm not like '%验证%' and bill_id <>'Z00000000020415'
               |and bill_id<>'Z00000000020878' and bill_nm not like '%满2元减1%' and bill_nm not like '%满2分减1分%'
               |and bill_nm not like '%满2减1%' and bill_nm not like '%满2抵1%' and bill_nm not like '测%'
               |group by case when internal_trans_tp='C00022' then '1.0 规范'
               |when internal_trans_tp='C20022' then '2.0 规范' else '--' end ,
               |case when substr(udf_fld,31,2) in ('01','02','03','04','05') then '仅限云闪付' else '非仅限云闪付' end,
               |to_date(trans_dt)) b
               |on a.project_name=b.project_name and a.if_hce=b.if_hce and a.report_dt=b.report_dt
               |
               |union all
               |select
               |a.project_name,
               |a.if_hce,
               |a.report_dt,
               |a.tran_cnt ,
               |b.tran_succ_cnt,
               |b.tran_succ_at,
               |b.discount_at,
               |b.tran_usr_num ,
               |b.tran_card_num
               |from
               |(select
               |case when internal_trans_tp='C00023' then '终端不改造' else '终端改造' end as project_name ,
               |case when substr(udf_fld,31,2) in ('01','02','03','04','05') then '仅限云闪付' else '非仅限云闪付' end as if_hce,
               |to_date(trans_dt) as report_dt,
               |count(*) as tran_cnt
               |from hive_acc_trans
               |where to_date(trans_dt)>='$today_dt' and to_date(trans_dt)<='$today_dt'
               |and bill_nm not like '%机场%' and bill_nm not like '%住两晚送一晚%'
               |and bill_nm not like '%测试%' and bill_nm not like '%验证%' and bill_id <>'Z00000000020415'
               |and bill_id<>'Z00000000020878' and bill_nm not like '%满2元减1%' and bill_nm not like '%满2分减1分%'
               |and bill_nm not like '%满2减1%' and bill_nm not like '%满2抵1%' and bill_nm not like '测%'
               |group by case when internal_trans_tp='C00023' then '终端不改造' else '终端改造' end ,
               |case when substr(udf_fld,31,2) in ('01','02','03','04','05') then '仅限云闪付' else '非仅限云闪付' end,
               |to_date(trans_dt) ) a
               |left join
               |(select
               |case when internal_trans_tp='C00023' then '终端不改造' else '终端改造' end as project_name,
               |case when substr(udf_fld,31,2) in ('01','02','03','04','05') then '仅限云闪付' else '非仅限云闪付' end as if_hce,
               |to_date(trans_dt) as report_dt,
               |count(*) as tran_succ_cnt,
               |sum(trans_at) as tran_succ_at,
               |sum(discount_at) as discount_at,
               |count(cdhd_usr_id) as tran_usr_num ,
               |count(card_no) as tran_card_num
               |from hive_acc_trans
               |where to_date(trans_dt)>='$today_dt' and to_date(trans_dt)<='$today_dt'
               |and sys_det_cd='S'
               |and bill_nm not like '%机场%' and bill_nm not like '%住两晚送一晚%'
               |and bill_nm not like '%测试%' and bill_nm not like '%验证%' and bill_id <>'Z00000000020415'
               |and bill_id<>'Z00000000020878' and bill_nm not like '%满2元减1%' and bill_nm not like '%满2分减1分%'
               |and bill_nm not like '%满2减1%' and bill_nm not like '%满2抵1%' and bill_nm not like '测%'
               |group by case when internal_trans_tp='C00023' then '终端不改造' else '终端改造' end ,
               |case when substr(udf_fld,31,2) in ('01','02','03','04','05') then '仅限云闪付' else '非仅限云闪付' end,
               |to_date(trans_dt)) b
               |on a.project_name=b.project_name and a.if_hce=b.if_hce and a.report_dt=b.report_dt
               |) t1
               |group by project_name, if_hce,report_dt
               |
               |
          """.stripMargin)
          println(s"#### JOB_DM_92 spark sql 清洗[$today_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())

          println(s"###JOB_DM_92------$today_dt results:"+results.count())
          if(!Option(results).isEmpty){
            results.save2Mysql("DM_ACPT_DIRECT_TRAN_STANDARD")
            println(s"#### JOB_DM_92 [$today_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
          }else{
            println(s"#### JOB_DM_92 spark sql 清洗[$today_dt]数据无结果集！")
          }
          today_dt=DateUtils.addOneDay(today_dt)
        }
      }
    }
  }
  /**
    * JobName: JOB_DM_93
    * Feature: DM_COUPON_ACPT_PHONE_AREA
    *
    * @author tzq
    * @time 2016-12-28
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    * @param interval
    */
  def JOB_DM_93 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("###JOB_DM_93(DM_COUPON_ACPT_PHONE_AREA)### "+DateUtils.getCurrentSystemTime())
    DateUtils.timeCost("JOB_DM_93"){
      UPSQL_JDBC.delete(s"DM_COUPON_ACPT_PHONE_AREA","REPORT_DT",start_dt,end_dt)
      println( "#### JOB_DM_93 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      var today_dt=start_dt
      if(interval>=0 ){
        sqlContext.sql(s"use $hive_dbname")
        for(i <- 0 to interval){
          println(s"#### JOB_DM_93 spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())
          val results =sqlContext.sql(
            s"""
               |select
               |c.phone_location as area_nm,
               |case when a.if_hce is null then b.if_hce else a.if_hce end as if_hce,
               |'$today_dt' as report_dt,
               |sum(a.tran_cnt) as tran_cnt,
               |sum(b.tran_succ_cnt) as tran_succ_cnt,
               |sum(b.tran_succ_at) as tran_succ_at,
               |sum(b.discount_at) as discount_at,
               |sum(b.tran_usr_num) as tran_usr_num,
               |sum(b.tran_card_num) as tran_card_num
               |from
               |(select phone_location,cdhd_usr_id from hive_pri_acct_inf ) c
               |left join
               |(select
               |cdhd_usr_id,
               |case when substr(udf_fld,31,2) in ('01','02','03','04','05') then '仅限云闪付' else '非仅限云闪付' end as if_hce,
               |count(*) as tran_cnt
               |from hive_acc_trans
               |where to_date(trans_dt)='$today_dt'
               |and bill_nm not like '%机场%' and bill_nm not like '%住两晚送一晚%'
               |and bill_nm not like '%测试%' and bill_nm not like '%验证%' and bill_id <>'Z00000000020415'
               |and bill_id<>'Z00000000020878' and bill_nm not like '%满2元减1%' and bill_nm not like '%满2分减1分%'
               |and bill_nm not like '%满2减1%' and bill_nm not like '%满2抵1%' and bill_nm not like '测%'
               |group by cdhd_usr_id,
               |case when substr(udf_fld,31,2) in ('01','02','03','04','05') then '仅限云闪付' else '非仅限云闪付' end ) a
               |on a.cdhd_usr_id=c.cdhd_usr_id
               |left join
               |(select
               |cdhd_usr_id,
               |case when substr(udf_fld,31,2) in ('01','02','03','04','05') then '仅限云闪付' else '非仅限云闪付' end as if_hce,
               |count(*) as tran_succ_cnt,
               |sum(trans_at) as tran_succ_at,
               |sum(discount_at) as discount_at,
               |count(cdhd_usr_id) as tran_usr_num ,
               |count(card_no) as tran_card_num
               |from hive_acc_trans
               |where to_date(trans_dt)='$today_dt'
               |and sys_det_cd='S'
               |and bill_nm not like '%机场%' and bill_nm not like '%住两晚送一晚%'
               |and bill_nm not like '%测试%' and bill_nm not like '%验证%' and bill_id <>'Z00000000020415'
               |and bill_id<>'Z00000000020878' and bill_nm not like '%满2元减1%' and bill_nm not like '%满2分减1分%'
               |and bill_nm not like '%满2减1%' and bill_nm not like '%满2抵1%' and bill_nm not like '测%'
               |group by cdhd_usr_id,
               |case when substr(udf_fld,31,2) in ('01','02','03','04','05') then '仅限云闪付' else '非仅限云闪付' end) b
               |on c.cdhd_usr_id=b.cdhd_usr_id and a.if_hce=b.if_hce
               |group by c.phone_location,(case when a.if_hce is null then b.if_hce else a.if_hce end)
               |
               |
          """.stripMargin)
          println(s"#### JOB_DM_93 spark sql 清洗[$today_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())

          println(s"###JOB_DM_93------$today_dt results:"+results.count())
          if(!Option(results).isEmpty){
            results.save2Mysql("DM_COUPON_ACPT_PHONE_AREA")
            println(s"#### JOB_DM_93 [$today_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
          }else{
            println(s"#### JOB_DM_93 spark sql 清洗[$today_dt]数据无结果集！")
          }
          today_dt=DateUtils.addOneDay(today_dt)
        }
      }
    }
  }

  /**
    * JobName: JOB_DM_94
    * Feature: DM_COUPON_TKT_MCHNT_BRANCH_DLY
    * Notice: SQL 注释部分：--and udf_fld in ('01','02','03','04','05')
    *
    * @author tzq
    * @time 2017-1-5
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    * @param interval
    */
  def JOB_DM_94 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("###JOB_DM_94(DM_COUPON_TKT_MCHNT_BRANCH_DLY)### "+DateUtils.getCurrentSystemTime())
    DateUtils.timeCost("JOB_DM_94"){
      UPSQL_JDBC.delete(s"DM_COUPON_TKT_MCHNT_BRANCH_DLY","REPORT_DT",start_dt,end_dt)
      println( "#### JOB_DM_94 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      var today_dt=start_dt
      if(interval>=0 ){
        sqlContext.sql(s"use $hive_dbname")
        for(i <- 0 to interval){
          println(s"#### JOB_DM_94 spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())
          val results =sqlContext.sql(
            s"""
               |select
               |    a.cup_branch_ins_id_nm as cup_branch_ins_id_nm,
               |    a.trans_dt  as report_dt,
               |    a.transcnt as trans_cnt,
               |    b.suctranscnt as suc_trans_cnt,
               |    b.transat as trans_at,
               |    b.discountat as discount_at,
               |    b.transusrcnt as trans_usr_cnt,
               |    b.transcardcnt as trans_card_cnt
               |from
               |    (
               |        select
               |            trim(if(acpt_ins.cup_branch_ins_id_nm is null,'总公司',acpt_ins.cup_branch_ins_id_nm)) as cup_branch_ins_id_nm,
               |            to_date(trans.trans_dt) as trans_dt,
               |            count(1) as transcnt
               |        from
               |            hive_acc_trans trans
               |        left join
               |            hive_mchnt_inf_wallet mchnt
               |        on
               |            (
               |                trans.mchnt_cd=mchnt.mchnt_cd)
               |        left join
               |            hive_branch_acpt_ins_inf acpt_ins
               |        on
               |            ( acpt_ins.ins_id_cd=concat('000',mchnt.acpt_ins_id_cd))
               |        where
               |            trans.um_trans_id in ('AC02000065', 'AC02000063')
               |        and trans.buss_tp in ('04','05','06')
               |        and trans.part_trans_dt >= '$today_dt'
               |        and trans.part_trans_dt <= '$today_dt'
               |        group by
               |            trim(if(acpt_ins.cup_branch_ins_id_nm is null,'总公司',acpt_ins.cup_branch_ins_id_nm)),
               |            to_date(trans.trans_dt)) a
               |left join
               |    (
               |        select
               |            trim(if(acpt_ins.cup_branch_ins_id_nm is null,'总公司',acpt_ins.cup_branch_ins_id_nm)) as cup_branch_ins_id_nm,
               |            to_date(trans.trans_dt) as trans_dt,
               |            count(1) as suctranscnt,
               |            sum(trans.trans_at) as transat,
               |            sum(trans.discount_at)          as discountat,
               |            count(distinct trans.cdhd_usr_id) as transusrcnt,
               |            count(distinct trans.pri_acct_no) as transcardcnt
               |        from
               |            hive_acc_trans trans
               |        left join
               |            hive_mchnt_inf_wallet mchnt
               |        on
               |            (
               |                trans.mchnt_cd=mchnt.mchnt_cd)
               |        left join
               |            hive_branch_acpt_ins_inf acpt_ins
               |        on
               |            (
               |                acpt_ins.ins_id_cd=concat('000',mchnt.acpt_ins_id_cd))
               |        inner join
               |            hive_ticket_bill_bas_inf bill
               |        on
               |            trans.bill_id=bill.bill_id
               |        where
               |            trans.sys_det_cd = 'S'
               |        and trans.um_trans_id in ('AC02000065',
               |                                  'AC02000063')
               |        and trans.buss_tp in ('04','05','06')
               |
               |        and trans.part_trans_dt >= '$today_dt'
               |        and trans.part_trans_dt <= '$today_dt'
               |        group by
               |            trim(if(acpt_ins.cup_branch_ins_id_nm is null,'总公司',acpt_ins.cup_branch_ins_id_nm)),
               |            to_date(trans.trans_dt))b
               |on
               |    (
               |        a.cup_branch_ins_id_nm = b.cup_branch_ins_id_nm
               |    and a.trans_dt = b.trans_dt )
          """.stripMargin)
          println(s"#### JOB_DM_94 spark sql 清洗[$today_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())

          println(s"###JOB_DM_94------$today_dt results:"+results.count())
          if(!Option(results).isEmpty){
            results.save2Mysql("DM_COUPON_TKT_MCHNT_BRANCH_DLY")
            println(s"#### JOB_DM_94 [$today_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
          }else{
            println(s"#### JOB_DM_94 spark sql 清洗[$today_dt]数据无结果集！")
          }
          today_dt=DateUtils.addOneDay(today_dt)
        }
      }
    }
  }




  /**
    * JobName: JOB_DM_95
    * Feature: DM_COUPON_TKT_MCHNT_TP_DLY
    *
    * @author tzq
    * @time 2017-1-13
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    * @param interval
    */
  def JOB_DM_95 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("###JOB_DM_95(DM_COUPON_TKT_MCHNT_TP_DLY)### "+DateUtils.getCurrentSystemTime())
    DateUtils.timeCost("JOB_DM_95"){
      UPSQL_JDBC.delete(s"DM_COUPON_TKT_MCHNT_TP_DLY","REPORT_DT",start_dt,end_dt)
      println( "#### JOB_DM_95 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      var today_dt=start_dt
      if(interval>=0 ){
        sqlContext.sql(s"use $hive_dbname")
        for(i <- 0 to interval){
          println(s"#### JOB_DM_95 spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())
          val results =sqlContext.sql(
            s"""
               |select
               |    a.grp_nm   as mchnt_tp_grp,
               |    a.tp_nm   as mchnt_tp,
               |    a.trans_dt   as report_dt,
               |    a.transcnt   as trans_cnt,
               |    b.suctranscnt   as suc_trans_cnt,
               |    b.transat   as trans_at,
               |    b.discountat   as discount_at,
               |    b.usrcnt   as trans_usr_cnt,
               |    b.cardcnt    as trans_card_cnt
               |from
               |    (
               |        select
               |            a1.grp_nm   as grp_nm,
               |            a1.tp_nm    as tp_nm,
               |            a1.trans_dt as trans_dt,
               |            count(*)    as transcnt
               |        from
               |            (
               |                select
               |                    trim(if(tp_grp.mchnt_tp_grp_desc_cn is null,'其他',tp_grp.mchnt_tp_grp_desc_cn)) as grp_nm,
               |                    trim(if(tp.mchnt_tp_desc_cn is null,'其他',tp.mchnt_tp_desc_cn)) as tp_nm,
               |                    to_date(trans.trans_dt)              as trans_dt,
               |                    trans.trans_at,
               |                    trans.discount_at,
               |                    trans.cdhd_usr_id,
               |                    trans.pri_acct_no
               |                from
               |                    hive_acc_trans trans
               |                inner join
               |                    hive_mchnt_inf_wallet mchnt
               |                on
               |                    trans.mchnt_cd=mchnt.mchnt_cd
               |                left join
               |                    hive_mchnt_tp tp
               |                on
               |                    mchnt.mchnt_tp=tp.mchnt_tp
               |                left join
               |                    hive_mchnt_tp_grp tp_grp
               |                on
               |                    tp.mchnt_tp_grp=tp_grp.mchnt_tp_grp
               |                where
               |                    trans.um_trans_id in ('AC02000065',
               |                                          'AC02000063')
               |                and trans.buss_tp in ('04',
               |                                      '05',
               |                                      '06')
               |                and trans.part_trans_dt= '$today_dt'
               |                 ) a1
               |        group by
               |            a1.grp_nm,
               |            a1.tp_nm,
               |            a1.trans_dt ) a
               |left join
               |    (
               |        select
               |            b1.grp_nm                      as grp_nm,
               |            b1.tp_nm                       as tp_nm,
               |            b1.trans_dt                    as trans_dt,
               |            count(*)                       as suctranscnt,
               |            sum(b1.trans_at)               as transat,
               |            sum(b1.discount_at)            as discountat,
               |            count(distinct b1.cdhd_usr_id) as usrcnt,
               |            count(distinct b1.pri_acct_no) as cardcnt
               |        from
               |            (
               |                select
               |                    trim(if(tp_grp.mchnt_tp_grp_desc_cn is null,'其他',tp_grp.mchnt_tp_grp_desc_cn)) as grp_nm ,
               |                    trim(if(tp.mchnt_tp_desc_cn is null,'其他',tp.mchnt_tp_desc_cn)) as tp_nm,
               |                    to_date(trans.trans_dt) as trans_dt,
               |                    trans.trans_at,
               |                    trans.discount_at,
               |                    trans.cdhd_usr_id,
               |                    trans.pri_acct_no
               |                from
               |                    hive_acc_trans trans
               |                inner join
               |                    hive_mchnt_inf_wallet mchnt
               |                on
               |                    trans.mchnt_cd=mchnt.mchnt_cd
               |                left join
               |                    hive_mchnt_tp tp
               |                on
               |                    mchnt.mchnt_tp=tp.mchnt_tp
               |                left join
               |                    hive_mchnt_tp_grp tp_grp
               |                on
               |                    tp.mchnt_tp_grp=tp_grp.mchnt_tp_grp
               |                where
               |                    trans.sys_det_cd='S'
               |                and trans.um_trans_id in ('AC02000065',
               |                                          'AC02000063')
               |                and trans.buss_tp in ('04', '05','06')
               |                and trans.part_trans_dt = '$today_dt' ) b1
               |        group by
               |            b1.grp_nm,
               |            b1.tp_nm,
               |            b1.trans_dt) b
               |on
               |    a.grp_nm=b.grp_nm
               |and a.tp_nm=b.tp_nm
               |and a.trans_dt=b.trans_dt
          """.stripMargin)
          println(s"#### JOB_DM_95 spark sql 清洗[$today_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())

          println(s"###JOB_DM_95------$today_dt results:"+results.count())
          if(!Option(results).isEmpty){
            results.save2Mysql("DM_COUPON_TKT_MCHNT_TP_DLY")
            println(s"#### JOB_DM_95 [$today_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
          }else{
            println(s"#### JOB_DM_95 spark sql 清洗[$today_dt]数据无结果集！")
          }
          today_dt=DateUtils.addOneDay(today_dt)
        }
      }
    }
  }


  /**
    * JobName: JOB_DM_96
    * Feature: DM_COUPON_TKT_ISS_INS_DLY
    *
    * @author tzq
    * @time 2017-1-13
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    * @param interval
    */
  def JOB_DM_96 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("###JOB_DM_96(DM_COUPON_TKT_ISS_INS_DLY)### "+DateUtils.getCurrentSystemTime())
    DateUtils.timeCost("JOB_DM_96"){
      UPSQL_JDBC.delete(s"DM_COUPON_TKT_ISS_INS_DLY","REPORT_DT",start_dt,end_dt)
      println( "#### JOB_DM_96 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      var today_dt=start_dt
      if(interval>=0 ){
        sqlContext.sql(s"use $hive_dbname")
        for(i <- 0 to interval){
          println(s"#### JOB_DM_96 spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())
          val results =sqlContext.sql(
            s"""
               |select
               |    a.iss_ins_cn_nm          as iss_ins_nm,
               |    a.trans_dt          as report_dt,
               |    a.transcnt          as trans_cnt,
               |    b.suctranscnt          as suc_trans_cnt,
               |    b.transat          as trans_at,
               |    b.discountat          as discount_at,
               |    b.transusrcnt          as trans_usr_cnt,
               |    b.transcardcnt          as trans_card_cnt
               |from
               |    (
               |        select
               |            trim(if(cbi.iss_ins_cn_nm is null,'其他',cbi.iss_ins_cn_nm)) as iss_ins_cn_nm,
               |            to_date(trans.trans_dt) as trans_dt,
               |            count(1) as transcnt
               |        from
               |            hive_acc_trans trans
               |        left join
               |            hive_card_bind_inf cbi
               |        on
               |            (
               |                trans.card_no = cbi.bind_card_no)
               |        where
               |            trans.um_trans_id in ('AC02000065',
               |                                  'AC02000063')
               |        and trans.buss_tp in ('04',
               |                              '05',
               |                              '06')
               |        and trans.part_trans_dt = '$today_dt'
               |        group by
               |            trim(if(cbi.iss_ins_cn_nm is null,'其他',cbi.iss_ins_cn_nm)),
               |            to_date(trans_dt)) a
               |left join
               |    (
               |        select
               |            trim(if(cbi.iss_ins_cn_nm is null,'其他',cbi.iss_ins_cn_nm)) as iss_ins_cn_nm,
               |            to_date(trans.trans_dt) as trans_dt,
               |            count(1)                          as suctranscnt,
               |            sum(trans.trans_at)               as transat,
               |            sum(trans.discount_at)            as discountat,
               |            count(distinct trans.cdhd_usr_id) as transusrcnt,
               |            count(distinct trans.pri_acct_no) as transcardcnt
               |        from
               |            hive_acc_trans trans
               |        left join
               |            hive_card_bind_inf cbi
               |        on
               |            (
               |                trans.card_no = cbi.bind_card_no)
               |        where
               |            trans.sys_det_cd = 's'
               |        and trans.um_trans_id in ('AC02000065',
               |                                  'AC02000063')
               |        and trans.buss_tp in ('04',
               |                              '05',
               |                              '06')
               |        and trans.part_trans_dt = '$today_dt'
               |        group by
               |            trim(if(cbi.iss_ins_cn_nm is null,'其他',cbi.iss_ins_cn_nm)) ,
               |            to_date(trans_dt))b
               |on
               |    (
               |        a.iss_ins_cn_nm = b.iss_ins_cn_nm
               |    and a.trans_dt = b.trans_dt )
               |
               |
          """.stripMargin)
          println(s"#### JOB_DM_96 spark sql 清洗[$today_dt]数据完成时间为:" + DateUtils.getCurrentSystemTime())

          println(s"###JOB_DM_96------$today_dt results:"+results.count())
          if(!Option(results).isEmpty){
            results.save2Mysql("DM_COUPON_TKT_ISS_INS_DLY")
            println(s"#### JOB_DM_96 [$today_dt]数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
          }else{
            println(s"#### JOB_DM_96 spark sql 清洗[$today_dt]数据无结果集！")
          }
          today_dt=DateUtils.addOneDay(today_dt)
        }
      }
    }
  }

  /**
    * JOB_DM_97 20170116
    * dm_coupon_tkt_branch_dly->hive_acc_trans,hive_ticket_bill_bas_inf
    * Code by Xue
    *
    * @param sqlContext
    * @return
    */
  def JOB_DM_97(implicit sqlContext: HiveContext, start_dt: String, end_dt: String, interval: Int) = {
    println("###JOB_DM_97(dm_coupon_tkt_branch_dly->hive_acc_trans,hive_ticket_bill_bas_inf)")
    DateUtils.timeCost("JOB_DM_97") {
      UPSQL_JDBC.delete(s"dm_disc_act_quick_pass_dly", "report_dt", start_dt, end_dt)
      println("#### JOB_DM_97 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      println(s"#### JOB_DM_97  spark sql 清洗数据开始时间为:" + DateUtils.getCurrentSystemTime())
      sqlContext.sql(s"use $hive_dbname")
      val results = sqlContext.sql(
        s"""
           |select
           |    a.cup_branch_ins_id_nm as cup_branch_ins_id_nm,
           |    a.trans_dt as report_dt,
           |    a.transcnt as trans_cnt,
           |    b.suctranscnt  as suc_trans_cnt,
           |    b.transat as trans_at,
           |    b.discountat as discount_at,
           |    b.transusrcnt as trans_usr_cnt,
           |    b.transcardcnt as trans_card_cnt
           |from
           |    (
           |        select
           |if(bill.cup_branch_ins_id_nm is null,'总公司',bill.cup_branch_ins_id_nm) as cup_branch_ins_id_nm,
           |            trans.trans_dt,
           |            count(1) as transcnt
           |        from
           |            hive_acc_trans trans
           |        left join
           |            hive_ticket_bill_bas_inf bill
           |        on
           |            (
           |                trans.bill_id=bill.bill_id)
           |        where
           |            trans.um_trans_id in ('AC02000065',
           |                                  'AC02000063')
           |        and trans.buss_tp in ('04','05','06')
           |        and trans.part_trans_dt >= '$start_dt'
           |        and trans.part_trans_dt <= '$end_dt'
           |        group by
           |if(bill.cup_branch_ins_id_nm is null,'总公司',bill.cup_branch_ins_id_nm),
           |            trans_dt) a
           |left join
           |    (
           |        select
           |if(bill.cup_branch_ins_id_nm is null,'总公司',bill.cup_branch_ins_id_nm) as cup_branch_ins_id_nm,
           |            trans.trans_dt,
           |            count(1) as suctranscnt,
           |            sum(trans.trans_at) as transat,
           |            sum(trans.discount_at)          as discountat,
           |            count(distinct trans.cdhd_usr_id) as transusrcnt,
           |            count(distinct trans.pri_acct_no) as transcardcnt
           |        from
           |            hive_acc_trans trans
           |        left join
           |            hive_ticket_bill_bas_inf bill
           |        on
           |            (
           |                trans.bill_id=bill.bill_id)
           |        where
           |            trans.sys_det_cd = 'S'
           |        and trans.um_trans_id in ('AC02000065',
           |                                  'AC02000063')
           |        and trans.buss_tp in ('04','05','06')
           |        and trans.part_trans_dt >= '$start_dt'
           |        and trans.part_trans_dt <= '$end_dt'
           |        group by
           |if(bill.cup_branch_ins_id_nm is null,'总公司',bill.cup_branch_ins_id_nm),
           |            trans_dt)b
           |on
           |    (
           |        a.cup_branch_ins_id_nm = b.cup_branch_ins_id_nm
           |    and a.trans_dt = b.trans_dt )
           | """.stripMargin)
      println(s"#### JOB_DM_97 spark sql 清洗数据完成时间为:" + DateUtils.getCurrentSystemTime())
      if (!Option(results).isEmpty) {
        results.save2Mysql("dm_coupon_tkt_branch_dly")
        println(s"#### JOB_DM_97 数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
      } else {
        println(s"#### JOB_DM_97 spark sql 清洗数据无结果集！")
      }
    }
  }


  /**
    * JOB_DM_98 20170116
    * dm_coupon_tkt_mchnt_ind_dly->hive_acc_trans,hive_store_term_relation,hive_preferential_mchnt_inf,hive_mchnt_para
    * Code by Xue
    *
    * @param sqlContext
    * @return
    */
  def JOB_DM_98(implicit sqlContext: HiveContext, start_dt: String, end_dt: String, interval: Int) = {
    println("###JOB_DM_98(dm_coupon_tkt_mchnt_ind_dly->hive_acc_trans,hive_store_term_relation,hive_preferential_mchnt_inf,hive_mchnt_para)")
    DateUtils.timeCost("JOB_DM_98") {
      UPSQL_JDBC.delete(s"dm_coupon_tkt_mchnt_ind_dly", "report_dt", start_dt, end_dt)
      println("#### JOB_DM_98 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      println(s"#### JOB_DM_98  spark sql 清洗数据开始时间为:" + DateUtils.getCurrentSystemTime())
      sqlContext.sql(s"use $hive_dbname")
      val results = sqlContext.sql(
        s"""
           |select
           |    if(t.first_ind_nm is null,'其他',t.first_ind_nm)     as first_ind_nm,
           |    if(t.second_ind_nm is null,'其他',t.second_ind_nm)   as second_ind_nm,
           |    t.report_dt                                          as report_dt,
           |    sum(t.trans_cnt)                                     as trans_cnt,
           |    sum(t.suc_trans_cnt)                                 as suc_trans_cnt,
           |    sum(t.trans_at)                                      as trans_at,
           |    sum(t.discount_at)                                   as discount_at,
           |    sum(t.trans_usr_cnt)                                 as trans_usr_cnt,
           |    sum(t.trans_card_cnt)                                as trans_card_cnt
           |from
           |    (
           |        select
           |            a.first_para_nm  as first_ind_nm,
           |            a.second_para_nm as second_ind_nm,
           |            a.trans_dt       as report_dt,
           |            a.transcnt       as trans_cnt,
           |            b.suctranscnt    as suc_trans_cnt,
           |            b.transat        as trans_at,
           |            b.discountat     as discount_at,
           |            b.transusrcnt    as trans_usr_cnt,
           |            b.transcardcnt   as trans_card_cnt
           |        from
           |            (
           |                select
           |                    mp.mchnt_para_cn_nm  as first_para_nm,
           |                    mp1.mchnt_para_cn_nm as second_para_nm,
           |                    trans.trans_dt,
           |                    count(1) as transcnt
           |                from
           |                    hive_acc_trans trans
           |                inner join
           |                    hive_store_term_relation str
           |                on
           |                    (
           |                        trans.card_accptr_cd = str.mchnt_cd
           |                    and trans.card_accptr_term_id = str.term_id)
           |                left join
           |                    hive_preferential_mchnt_inf pmi
           |                on
           |                    (
           |                        str.third_party_ins_id = pmi.mchnt_cd)
           |                left join
           |                    hive_mchnt_para mp
           |                on
           |                    (
           |                        pmi.mchnt_first_para = mp.mchnt_para_id)
           |                left join
           |                    hive_mchnt_para mp1
           |                on
           |                    (
           |                        pmi.mchnt_second_para = mp1.mchnt_para_id)
           |                where
           |                    trans.um_trans_id in ('AC02000065',
           |                                          'AC02000063')
           |                and str.rec_id is not null
           |                and trans.buss_tp in ('04',
           |                                      '05',
           |                                      '06')
           |                and trans.part_trans_dt >= '$start_dt'
           |                and trans.part_trans_dt <= '$end_dt'
           |                group by
           |                    mp.mchnt_para_cn_nm,
           |                    mp1.mchnt_para_cn_nm,
           |                    trans_dt) a
           |        left join
           |            (
           |                select
           |                    mp.mchnt_para_cn_nm  as first_para_nm,
           |                    mp1.mchnt_para_cn_nm as second_para_nm,
           |                    trans.trans_dt,
           |                    count(1)                          as suctranscnt,
           |                    sum(trans.trans_at)               as transat,
           |                    sum(trans.discount_at)            as discountat,
           |                    count(distinct trans.cdhd_usr_id) as transusrcnt,
           |                    count(distinct trans.pri_acct_no) as transcardcnt
           |                from
           |                    hive_acc_trans trans
           |                inner join
           |                    hive_store_term_relation str
           |                on
           |                    (
           |                        trans.card_accptr_cd = str.mchnt_cd
           |                    and trans.card_accptr_term_id = str.term_id)
           |                left join
           |                    hive_preferential_mchnt_inf pmi
           |                on
           |                    (
           |                        str.third_party_ins_id = pmi.mchnt_cd)
           |                left join
           |                    hive_mchnt_para mp
           |                on
           |                    (
           |                        pmi.mchnt_first_para = mp.mchnt_para_id)
           |                left join
           |                    hive_mchnt_para mp1
           |                on
           |                    (
           |                        pmi.mchnt_second_para = mp1.mchnt_para_id)
           |                where
           |                    trans.sys_det_cd = 'S'
           |                and trans.um_trans_id in ('AC02000065',
           |                                          'AC02000063')
           |                and str.rec_id is not null
           |                and trans.buss_tp in ('04',
           |                                      '05',
           |                                      '06')
           |                and trans.part_trans_dt >= '$start_dt'
           |                and trans.part_trans_dt <= '$end_dt'
           |                group by
           |                    mp.mchnt_para_cn_nm,
           |                    mp1.mchnt_para_cn_nm,
           |                    trans_dt)b
           |        on
           |            (
           |                a.first_para_nm = b.first_para_nm
           |            and a.second_para_nm = b.second_para_nm
           |            and a.trans_dt = b.trans_dt )
           |        union all
           |        select
           |            a.first_para_nm  as first_ind_nm,
           |            a.second_para_nm as second_ind_nm,
           |            a.trans_dt       as report_dt,
           |            a.transcnt       as trans_cnt,
           |            b.suctranscnt    as suc_trans_cnt,
           |            b.transat        as trans_at,
           |            b.discountat     as discount_at,
           |            b.transusrcnt    as trans_usr_cnt,
           |            b.transcardcnt   as trans_card_cnt
           |        from
           |            (
           |                select
           |                    mp.mchnt_para_cn_nm  as first_para_nm,
           |                    mp1.mchnt_para_cn_nm as second_para_nm,
           |                    trans.trans_dt,
           |                    count(1) as transcnt
           |                from
           |                    hive_acc_trans trans
           |                left join
           |                    hive_store_term_relation str
           |                on
           |                    (
           |                        trans.card_accptr_cd = str.mchnt_cd
           |                    and trans.card_accptr_term_id = str.term_id)
           |                left join
           |                    (
           |                        select
           |                            mchnt_cd,
           |                            max(third_party_ins_id) over (partition by mchnt_cd) as
           |                            third_party_ins_id
           |                        from
           |                            hive_store_term_relation) str1
           |                on
           |                    (
           |                        trans.card_accptr_cd = str1.mchnt_cd)
           |                left join
           |                    hive_preferential_mchnt_inf pmi
           |                on
           |                    (
           |                        str1.third_party_ins_id = pmi.mchnt_cd)
           |                left join
           |                    hive_mchnt_para mp
           |                on
           |                    (
           |                        pmi.mchnt_first_para = mp.mchnt_para_id)
           |                left join
           |                    hive_mchnt_para mp1
           |                on
           |                    (
           |                        pmi.mchnt_second_para = mp1.mchnt_para_id)
           |                where
           |                    trans.um_trans_id in ('AC02000065',
           |                                          'AC02000063')
           |                and trans.buss_tp in ('04',
           |                                      '05',
           |                                      '06')
           |                and str.rec_id is null
           |                and trans.part_trans_dt >= '$start_dt'
           |                and trans.part_trans_dt <= '$end_dt'
           |                group by
           |                    mp.mchnt_para_cn_nm,
           |                    mp1.mchnt_para_cn_nm,
           |                    trans_dt) a
           |        left join
           |            (
           |                select
           |                    mp.mchnt_para_cn_nm  as first_para_nm,
           |                    mp1.mchnt_para_cn_nm as second_para_nm,
           |                    trans.trans_dt,
           |                    count(1)                          as suctranscnt,
           |                    sum(trans.trans_at)               as transat,
           |                    sum(trans.discount_at)            as discountat,
           |                    count(distinct trans.cdhd_usr_id) as transusrcnt,
           |                    count(distinct trans.pri_acct_no) as transcardcnt
           |                from
           |                    hive_acc_trans trans
           |                left join
           |                    hive_store_term_relation str
           |                on
           |                    (
           |                        trans.card_accptr_cd = str.mchnt_cd
           |                    and trans.card_accptr_term_id = str.term_id)
           |                left join
           |                    (
           |                        select
           |                            mchnt_cd,
           |                            max(third_party_ins_id) over (partition by mchnt_cd) as
           |                            third_party_ins_id
           |                        from
           |                            hive_store_term_relation) str1
           |                on
           |                    (
           |                        trans.card_accptr_cd = str1.mchnt_cd)
           |                left join
           |                    hive_preferential_mchnt_inf pmi
           |                on
           |                    (
           |                        str1.third_party_ins_id = pmi.mchnt_cd)
           |                left join
           |                    hive_mchnt_para mp
           |                on
           |                    (
           |                        pmi.mchnt_first_para = mp.mchnt_para_id)
           |                left join
           |                    hive_mchnt_para mp1
           |                on
           |                    (
           |                        pmi.mchnt_second_para = mp1.mchnt_para_id)
           |                where
           |                    trans.sys_det_cd = 'S'
           |                and trans.um_trans_id in ('AC02000065',
           |                                          'AC02000063')
           |                and trans.buss_tp in ('04',
           |                                      '05',
           |                                      '06')
           |                and str.rec_id is null
           |                and trans.part_trans_dt >= '$start_dt'
           |                and trans.part_trans_dt <= '$end_dt'
           |                group by
           |                    mp.mchnt_para_cn_nm,
           |                    mp1.mchnt_para_cn_nm,
           |                    trans_dt)b
           |        on
           |            (
           |                a.first_para_nm = b.first_para_nm
           |            and a.second_para_nm = b.second_para_nm
           |            and a.trans_dt = b.trans_dt )) t
           |group by
           |    if(t.first_ind_nm is null,'其他',t.first_ind_nm),
           |    if(t.second_ind_nm is null,'其他',t.second_ind_nm),
           |    t.report_dt
           | """.stripMargin)
      println(s"#### JOB_DM_98 spark sql 清洗数据完成时间为:" + DateUtils.getCurrentSystemTime())
      if (!Option(results).isEmpty) {
        results.save2Mysql("dm_coupon_tkt_mchnt_ind_dly")
        println(s"#### JOB_DM_98 数据插入完成时间为：" + DateUtils.getCurrentSystemTime())
      } else {
        println(s"#### JOB_DM_98 spark sql 清洗数据无结果集！")
      }
    }
  }

}
// ## END LINE ##