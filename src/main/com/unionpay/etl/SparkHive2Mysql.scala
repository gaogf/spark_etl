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
  //指定HIVE数据库名
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
      case "JOB_DM_54" =>JOB_DM_54(sqlContext,start_dt,end_dt)              //CODE BY XTP 无数据
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
               |NVL(a.ID_AREA_NM,'其它') as IDCARD_HOME,
               |'$today_dt' as report_dt,
               |sum(a.tpre)   as   REG_TPRE_ADD_NUM    ,
               |sum(a.years)  as   REG_YEAR_ADD_NUM    ,
               |sum(a.total)  as   REG_TOTLE_ADD_NUM   ,
               |sum(b.tpre)   as   EFFECT_TPRE_ADD_NUM ,
               |sum(b.years)  as   EFFECT_YEAR_ADD_NUM ,
               |sum(b.total)  as   EFFECT_TOTLE_ADD_NUM,
               |sum(c.tpre)   as   DEAL_TPRE_ADD_NUM   ,
               |sum(c.years)  as   DEAL_YEAR_ADD_NUM   ,
               |sum(c.total)  as   DEAL_TOTLE_ADD_NUM
               |from
               |(
               |select
               |case when tempe.CITY_CARD in ('大连','宁波','厦门','青岛','深圳') then tempe.CITY_CARD else tempe.PROVINCE_CARD end as ID_AREA_NM,
               |count(distinct(case when tempe.rec_crt_ts='$today_dt'  then tempe.cdhd_usr_id end)) as tpre,
               |count(distinct(case when tempe.rec_crt_ts>=trunc('$today_dt','YYYY') and tempe.rec_crt_ts<='$today_dt'  then tempe.cdhd_usr_id end)) as years,
               |count(distinct(case when tempe.rec_crt_ts<='$today_dt' then  tempe.cdhd_usr_id end)) as total
               |from
               |(select cdhd_usr_id,to_date(rec_crt_ts) as rec_crt_ts, CITY_CARD,PROVINCE_CARD from HIVE_PRI_ACCT_INF where usr_st='1' ) tempe
               |group by (case when CITY_CARD in ('大连','宁波','厦门','青岛','深圳') then CITY_CARD else PROVINCE_CARD end)
               |) a
               |left join
               |(
               |select
               |case when tempa.CITY_CARD in ('大连','宁波','厦门','青岛','深圳') then tempa.CITY_CARD else tempa.PROVINCE_CARD end as ID_AREA_NM,
               |count(distinct(case when tempa.rec_crt_ts='$today_dt'  and tempb.bind_dt='$today_dt'  then  tempa.cdhd_usr_id end)) as tpre,
               |count(distinct(case when tempa.rec_crt_ts>=trunc('$today_dt','YYYY') and tempa.rec_crt_ts<='$today_dt'
               |and tempb.bind_dt>=trunc('$today_dt','YYYY') and  tempb.bind_dt<='$today_dt' then  tempa.cdhd_usr_id end)) as years,
               |count(distinct(case when tempa.rec_crt_ts<='$today_dt' and  tempb.bind_dt<='$today_dt'  then  tempa.cdhd_usr_id end)) as total
               |from
               |(
               |select to_date(rec_crt_ts) as rec_crt_ts,CITY_CARD,PROVINCE_CARD,cdhd_usr_id from HIVE_PRI_ACCT_INF
               |where usr_st='1' ) tempa
               |inner join (select distinct cdhd_usr_id , to_date(rec_crt_ts) as  bind_dt  from HIVE_CARD_BIND_INF where card_auth_st in ('1','2','3') ) tempb
               |on tempa.cdhd_usr_id=tempb.cdhd_usr_id
               |group by (case when tempa.CITY_CARD in ('大连','宁波','厦门','青岛','深圳') then tempa.CITY_CARD else tempa.PROVINCE_CARD end) ) b
               |on a.ID_AREA_NM =b.ID_AREA_NM
               |left join
               |(
               |select
               |case when tempc.CITY_CARD in ('大连','宁波','厦门','青岛','深圳') then tempc.CITY_CARD else tempc.PROVINCE_CARD end as ID_AREA_NM,
               |count(distinct(case when tempc.rec_crt_ts='$today_dt'  and tempd.trans_dt='$today_dt'  then tempc.cdhd_usr_id end)) as tpre,
               |count(distinct(case when tempc.rec_crt_ts>=trunc('$today_dt','YYYY') and tempc.rec_crt_ts<='$today_dt'
               |and tempd.trans_dt>=trunc('$today_dt','YYYY') and  tempd.trans_dt<='$today_dt' then  tempc.cdhd_usr_id end)) as years,
               |count(distinct(case when tempc.rec_crt_ts<='$today_dt' and  tempd.trans_dt<='$today_dt'  then  tempc.cdhd_usr_id end)) as total
               |from
               |(select CITY_CARD,CITY_CARD,PROVINCE_CARD,cdhd_usr_id,to_date(rec_crt_ts) as rec_crt_ts  from HIVE_PRI_ACCT_INF
               |where usr_st='1') tempc
               |inner join (select distinct cdhd_usr_id,trans_dt from HIVE_ACC_TRANS ) tempd
               |on tempc.cdhd_usr_id=tempd.cdhd_usr_id
               |group by (case when tempc.CITY_CARD in ('大连','宁波','厦门','青岛','深圳') then tempc.CITY_CARD else tempc.PROVINCE_CARD end) ) c
               |on a.ID_AREA_NM=c.ID_AREA_NM
               |group by NVL(a.ID_AREA_NM,'其它'),'$today_dt'
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
            |when to_date(a.rec_crt_ts)>=trunc('$today_dt','yyyy')
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
            |when to_date(a.rec_crt_ts)>=trunc('$today_dt','yyyy')
            |and to_date(a.rec_crt_ts)<='$today_dt'
            |and to_date(b.card_dt)>=trunc('$today_dt','yyyy')
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
            |when to_date(a.rec_crt_ts)>=trunc('$today_dt','yyyy')
            |and to_date(a.rec_crt_ts)<='$today_dt'
            |and to_date(b.trans_dt)>=trunc('$today_dt','yyyy')
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
      UPSQL_JDBC.delete("DM_USER_CARD_AUTH","REPORT_DT",start_dt,end_dt)
      println("#### JOB_DM_4 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      var today_dt=start_dt
      if(interval>=0){
        sqlContext.sql(s"use $hive_dbname")
        for(i <- 0 to interval){
          println(s"#### JOB_DM_4 spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())
          val results = sqlContext.sql(
            s"""
               |select
               |a.card_auth_nm as CARD_AUTH,
               |a.realnm_in as DIFF_NAME,
               |'$today_dt' as REPORT_DT,
               |sum(a.tpre)   as   EFFECT_TPRE_ADD_NUM ,
               |sum(a.years)  as   EFFECT_YEAR_ADD_NUM ,
               |sum(a.total)  as   EFFECT_TOTLE_ADD_NUM,
               |sum(b.tpre)   as   DEAL_TPRE_ADD_NUM   ,
               |sum(b.years)  as   DEAL_YEAR_ADD_NUM   ,
               |sum(b.total)  as   DEAL_TOTLE_ADD_NUM
               |
             |from (
               |select
               |(case when tempb.card_auth_st='0' then   '未认证'
               | when tempb.card_auth_st='1' then   '支付认证'
               | when tempb.card_auth_st='2' then   '可信认证'
               | when tempb.card_auth_st='3' then   '可信+支付认证'
               |else '未认证' end) as card_auth_nm,
               |tempa.realnm_in as realnm_in,
               |count(distinct(case when tempa.rec_crt_ts='$today_dt'  and tempb.CARD_DT='$today_dt'  then tempa.cdhd_usr_id end)) as tpre,
               |count(distinct(case when tempa.rec_crt_ts>=trunc('$today_dt','YYYY') and tempa.rec_crt_ts<='$today_dt'
               |and tempb.CARD_DT>=trunc('$today_dt','YYYY')  and tempb.CARD_DT<='$today_dt' then  tempa.cdhd_usr_id end)) as years,
               |count(distinct(case when tempa.rec_crt_ts<='$today_dt' and tempb.CARD_DT<='$today_dt'  then tempa.cdhd_usr_id end)) as total
               |from
               |(select cdhd_usr_id,to_date(rec_crt_ts) as rec_crt_ts,realnm_in from HIVE_PRI_ACCT_INF
               |where usr_st='1' ) tempa
               |inner join
               |(select distinct tempe.cdhd_usr_id as cdhd_usr_id,
               |tempe.card_auth_st as card_auth_st,
               |to_date(tempe.rec_crt_ts) as CARD_DT
               |from HIVE_CARD_BIND_INF tempe) tempb
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
               |(select distinct cdhd_usr_id,card_auth_st,to_date(rec_crt_ts) as rec_crt_ts from HIVE_CARD_BIND_INF) tempc
               |inner join (select distinct cdhd_usr_id,trans_dt from HIVE_ACC_TRANS ) tempd
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
    println("JOB_DM_6------->JOB_DM_6(dm_user_card_nature->hive_pri_acct_inf+hive_card_bind_inf+hive_acc_trans)")
    DateUtils.timeCost("JOB_DM_6"){
      UPSQL_JDBC.delete("dm_user_card_nature","report_dt",start_dt,end_dt)
      println( "#### JOB_DM_6 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())

      var today_dt=start_dt
      if(interval>0 ){
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
    println("###JOB_DM_7(dm_user_card_level->HIVE_CARD_BIN,HIVE_CARD_BIND_INF,HIVE_PRI_ACCT_INF)")
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
               |NVL(NVL(tempa.card_lvl,tempb.card_lvl),'其它') as card_level,
               |'$today_dt' as report_dt,
               |sum(tempa.tpre)   as   effect_tpre_add_num  ,
               |sum(tempa.years)  as   effect_year_add_num  ,
               |sum(tempa.total)  as   effect_totle_add_num ,
               |sum(tempb.tpre)   as   deal_tpre_add_num    ,
               |sum(tempb.years)  as   deal_year_add_num    ,
               |sum(tempb.total)  as   deal_totle_add_num
               |FROM
               |(
               |select a.card_lvl as card_lvl,
               |count(distinct(case when to_date(c.rec_crt_ts)='$today_dt'  and to_date(b.CARD_DT)='$today_dt'  then b.cdhd_usr_id end)) as tpre,
               |count(distinct(case when to_date(c.rec_crt_ts)>=trunc('$today_dt','YYYY-MM-DD') and to_date(c.rec_crt_ts)<='$today_dt'
               |     and to_date(b.CARD_DT)>=trunc('$today_dt','YYYY-MM-DD') and  to_date(b.CARD_DT)<='$today_dt' then  b.cdhd_usr_id end)) as years,
               |count(distinct(case when to_date(c.rec_crt_ts)<='$today_dt' and  to_date(b.CARD_DT)<='$today_dt'  then  b.cdhd_usr_id end)) as total
               |from
               |(select card_lvl,card_bin from HIVE_CARD_BIN ) a
               |inner join
               |(select distinct cdhd_usr_id, rec_crt_ts as CARD_DT ,substr(bind_card_no,1,8) as card_bin from HIVE_CARD_BIND_INF where card_auth_st in ('1','2','3')  ) b
               |on a.card_bin=b.card_bin
               |inner join
               |(select cdhd_usr_id,rec_crt_ts from HIVE_PRI_ACCT_INF where usr_st='1'  ) c on b.cdhd_usr_id=c.cdhd_usr_id
               |group by a.card_lvl ) tempa
               |
             |left join
               |
             |(
               |select a.card_lvl as card_lvl,
               |count(distinct(case when to_date(b.trans_dt)='$today_dt'    then b.cdhd_usr_id end)) as tpre,
               |count(distinct(case when to_date(b.trans_dt)>=trunc('$today_dt','YYYY-MM-DD') and to_date(b.trans_dt)<='$today_dt' then  b.cdhd_usr_id end)) as years,
               |count(distinct(case when to_date(b.trans_dt)='$today_dt'  then  b.cdhd_usr_id end)) as total
               |from
               |(select card_lvl,card_bin from HIVE_CARD_BIN ) a
               |inner join
               |(select distinct cdhd_usr_id,trans_dt,substr(card_no,1,8) as card_bin  from VIW_CHACC_ACC_TRANS_DTL ) b on a.card_bin=b.card_bin
               |group by a.card_lvl ) tempb
               |on tempa.card_lvl=tempb.card_lvl
               |group by NVL(NVL(tempa.card_lvl,tempb.card_lvl),'其它'),'$today_dt'
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
               |SELECT
               |NVL(NVL(NVL(a.CUP_BRANCH_INS_ID_NM,c.BRANCH_DIVISION_CD),d.CUP_BRANCH_INS_ID_NM),'其它') as INPUT_BRANCH,
               |'$today_dt' as REPORT_DT,
               |sum(a.tpre)   as   STORE_TPRE_ADD_NUM  ,
               |sum(a.years)  as   STORE_YEAR_ADD_NUM  ,
               |sum(a.total)  as   STORE_TOTLE_ADD_NUM ,
               |sum(c.tpre)   as   ACTIVE_TPRE_ADD_NUM ,
               |sum(c.years)  as   ACTIVE_YEAR_ADD_NUM ,
               |sum(c.total)  as   ACTIVE_TOTLE_ADD_NUM,
               |sum(d.tpre)   as   COUPON_TPRE_ADD_NUM ,
               |sum(d.years)  as   COUPON_YEAR_ADD_NUM ,
               |sum(d.total)  as   COUPON_TOTLE_ADD_NUM
               |from(
               |select CUP_BRANCH_INS_ID_NM,
               |count(distinct(case when to_date(rec_crt_ts)='$today_dt'  then MCHNT_CD end)) as tpre,
               |count(distinct(case when to_date(rec_crt_ts)>=trunc('$today_dt','YYYY-MM-DD') and to_date(rec_crt_ts)<='$today_dt' then  MCHNT_CD end)) as years,
               |count(distinct(case when to_date(rec_crt_ts)<='$today_dt'  then MCHNT_CD end)) as total
               |from HIVE_MCHNT_INF_WALLET where substr(OPEN_BUSS_BMP,1,2)<>00
               |group by  CUP_BRANCH_INS_ID_NM) a
               |
             |left join
               |
             |(
               |select
               |tempa.BRANCH_DIVISION_CD as BRANCH_DIVISION_CD,
               |count(distinct(case when to_date(tempa.rec_crt_ts)='$today_dt'  and tempa.valid_begin_dt='$today_dt' AND tempa.valid_end_dt='$today_dt'  then tempa.MCHNT_CD end)) as tpre,
               |count(distinct(case when to_date(tempa.rec_crt_ts)>=trunc('$today_dt','YYYY-MM-DD') and to_date(tempa.rec_crt_ts)<='$today_dt'
               |     and valid_begin_dt>=trunc('$today_dt','YYYY-MM-DD') and  tempa.valid_end_dt<='$today_dt' then  tempa.MCHNT_CD end)) as years,
               |count(distinct(case when to_date(tempa.rec_crt_ts)<='$today_dt' and  tempa.valid_begin_dt='$today_dt' AND tempa.valid_end_dt='$today_dt'  then  tempa.MCHNT_CD end)) as total  --累计新增用户
               |from (
               |select distinct access.CUP_BRANCH_INS_ID_NM as BRANCH_DIVISION_CD,b.rec_crt_ts as rec_crt_ts ,bill.valid_begin_dt as valid_begin_dt, bill.valid_end_dt as valid_end_dt,b.MCHNT_CD as MCHNT_CD
               |from (select distinct mchnt_cd,rec_crt_ts from HIVE_PREFERENTIAL_MCHNT_INF
               |where mchnt_cd like 'T%' and mchnt_st='2' and mchnt_nm not like '%验证%' and mchnt_nm not like '%测试%'
               |and brand_id<>68988 ) b
               |inner join HIVE_CHARA_GRP_DEF_BAT grp on b.mchnt_cd=grp.chara_data
               |inner join HIVE_access_bas_inf access on access.ch_ins_id_cd=b.mchnt_cd
               |inner join (select distinct(chara_grp_cd),valid_begin_dt,valid_end_dt from HIVE_TICKET_BILL_BAS_INF  ) bill
               |on bill.chara_grp_cd=grp.chara_grp_cd
               |)tempa
               |group by tempa.BRANCH_DIVISION_CD )c
               |on a.CUP_BRANCH_INS_ID_NM=c.BRANCH_DIVISION_CD
               |
             |left join
               |
             |(select
               |CUP_BRANCH_INS_ID_NM,
               |count(distinct(case when to_date(rec_crt_ts)='$today_dt'  then MCHNT_CD end)) as tpre,
               |count(distinct(case when to_date(rec_crt_ts)>=trunc('$today_dt','YYYY-MM-DD') and to_date(rec_crt_ts)<='$today_dt' then MCHNT_CD end)) as years,
               |count(distinct(case when to_date(rec_crt_ts)<='$today_dt'  then MCHNT_CD end)) as total
               |from HIVE_MCHNT_INF_WALLET  where substr(OPEN_BUSS_BMP,1,2) in (10,11)
               |group by CUP_BRANCH_INS_ID_NM) d
               |on a.CUP_BRANCH_INS_ID_NM=d.CUP_BRANCH_INS_ID_NM
               |group by NVL(NVL(NVL(a.CUP_BRANCH_INS_ID_NM,c.BRANCH_DIVISION_CD),d.CUP_BRANCH_INS_ID_NM),'其它'),'$today_dt'
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
               |case when a.gb_region_nm is null then nvl(b.cup_branch_ins_id_nm,nvl(c.gb_region_nm,'其它')) else a.gb_region_nm end as BRANCH_AREA,
               |'$today_dt' as report_dt,
               |sum(a.tpre)   as   STORE_TPRE_ADD_NUM  ,
               |sum(a.years)  as   STORE_YEAR_ADD_NUM  ,
               |sum(a.total)  as   STORE_TOTLE_ADD_NUM ,
               |sum(b.tpre)   as   ACTIVE_TPRE_ADD_NUM ,
               |sum(b.years)  as   ACTIVE_YEAR_ADD_NUM ,
               |sum(b.total)  as   ACTIVE_TOTLE_ADD_NUM,
               |sum(c.tpre)   as   COUPON_TPRE_ADD_NUM ,
               |sum(c.years)  as   COUPON_YEAR_ADD_NUM ,
               |sum(c.total)  as   COUPON_TOTLE_ADD_NUM
               |FROM
               |(
               |select
               |tempe.PROV_DIVISION_CD as gb_region_nm,
               |count(case when to_date(tempe.rec_crt_ts)='$today_dt'  then 1 end) as tpre,
               |count(case when to_date(tempe.rec_crt_ts)>=trunc('$today_dt','YYYY') and to_date(tempe.rec_crt_ts)<='$today_dt' then  1 end) as years,
               |count(case when to_date(tempe.rec_crt_ts)<='$today_dt' then 1 end) as total
               |from
               |(
               |select distinct  PROV_DIVISION_CD,to_date(rec_crt_ts) as rec_crt_ts,mchnt_prov, mchnt_city_cd, mchnt_county_cd, mchnt_addr
               |from HIVE_PREFERENTIAL_MCHNT_INF
               |where  mchnt_st='2' and mchnt_nm not like '%验证%' and mchnt_nm not like '%测试%' and  brand_id<>68988
               |union all
               |select PROV_DIVISION_CD, to_date(rec_crt_ts) as rec_crt_ts,mchnt_prov, mchnt_city_cd, mchnt_county_cd, mchnt_addr
               |from HIVE_PREFERENTIAL_MCHNT_INF
               |where mchnt_st='2' and mchnt_nm not like '%验证%' and mchnt_nm not like '%测试%' and brand_id=68988
               |)  tempe
               |group by  tempe.PROV_DIVISION_CD
               |) a
               |
               |full outer join
               |(select
               |tempb.PROV_DIVISION_CD as cup_branch_ins_id_nm,
               |sum(case when to_date(tempb.trans_dt)='$today_dt'  then tempb.cnt end) as tpre,
               |sum(case when to_date(tempb.trans_dt)>=trunc('$today_dt','YYYY') and to_date(tempb.trans_dt)<='$today_dt' then  tempb.cnt end) as years,
               |sum(case when to_date(tempb.trans_dt)<='$today_dt'  then  tempb.cnt end) as total
               |from
               |(
               |select nvl(tp.PROV_DIVISION_CD,'总公司') as PROV_DIVISION_CD,tp.trans_dt, sum(cnt) as cnt
               |from(
               |select
               |t1.PROV_DIVISION_CD,
               |t1.trans_dt,
               |count(*) as cnt
               |from (
               |select distinct mchnt.PROV_DIVISION_CD , mchnt_prov,mchnt_city_cd, mchnt_addr , a.trans_dt
               |from (
               |select distinct card_accptr_cd,card_accptr_term_id,trans_dt
               |from HIVE_ACC_TRANS
               |where UM_TRANS_ID in ('AC02000065','AC02000063') and
               |buss_tp in ('02','04','05','06') and sys_det_cd='S'
               |) a
               |left join HIVE_STORE_TERM_RELATION b
               |on a.card_accptr_cd=b.mchnt_cd and a.card_accptr_term_id=b.term_id
               |left join HIVE_PREFERENTIAL_MCHNT_INF mchnt
               |on b.THIRD_PARTY_INS_ID=mchnt.mchnt_cd
               |where b.THIRD_PARTY_INS_ID is not null
               |) t1
               |group by t1.PROV_DIVISION_CD,t1.trans_dt
               |
               |union all
               |
               |select
               |t2.PROV_DIVISION_CD,
               |t2.trans_dt,
               |count(*) as cnt
               |from (
               |select distinct mcf.PROV_DIVISION_CD, a.card_accptr_cd,a.card_accptr_term_id, a.trans_dt
               |from (
               |select distinct card_accptr_cd,card_accptr_term_id, trans_dt,acpt_ins_id_cd
               |from HIVE_ACC_TRANS
               |where UM_TRANS_ID in ('AC02000065','AC02000063') and
               |buss_tp in ('02','04','05','06') and sys_det_cd='S'
               |) a
               |left join HIVE_STORE_TERM_RELATION b
               |on a.card_accptr_cd=b.mchnt_cd and a.card_accptr_term_id=b.term_id
               |left join  (select distinct ins_id_cd,CUP_BRANCH_INS_ID_NM as PROV_DIVISION_CD from HIVE_INS_INF where length(trim(cup_branch_ins_id_cd))<>0
               |union all
               |select distinct ins_id_cd, cup_branch_ins_id_nm as PROV_DIVISION_CD from HIVE_ACONL_INS_BAS where length(trim(cup_branch_ins_id_cd))<>0  )mcf
               |on a.acpt_ins_id_cd=concat('000',mcf.ins_id_cd)
               |
               |where b.THIRD_PARTY_INS_ID is null
               |) t2
               |group by t2.PROV_DIVISION_CD,t2.trans_dt
               |
               |union all
               |select
               |dis.CUP_BRANCH_INS_ID_NM as PROV_DIVISION_CD,
               |dis.settle_dt as trans_dt,
               |count(distinct dis.term_id) as cnt
               |from
               |(select CUP_BRANCH_INS_ID_NM,term_id,settle_dt
               |from HIVE_PRIZE_DISCOUNT_RESULT ) dis
               |left join
               |(select term_id from HIVE_STORE_TERM_RELATION ) rt
               |on dis.term_id=rt.term_id
               |where rt.term_id is null
               |group by dis.CUP_BRANCH_INS_ID_NM,dis.settle_dt
               |) tp
               |group by tp.PROV_DIVISION_CD ,tp.trans_dt
               |
               |) tempb
               |group by tempb.PROV_DIVISION_CD) b
               |on a.gb_region_nm=b.cup_branch_ins_id_nm
               |
               |full  outer join
               |(
               |select
               |tempd.gb_region_nm as gb_region_nm,
               |count(distinct(case when to_date(tempd.rec_crt_ts)='$today_dt'  then tempd.MCHNT_CD end)) as tpre,
               |count(distinct(case when to_date(tempd.rec_crt_ts)>=trunc('$today_dt','YYYY') and to_date(tempd.rec_crt_ts)<='$today_dt' then  tempd.MCHNT_CD end)) as years,
               |count(distinct(case when to_date(tempd.rec_crt_ts)<='$today_dt'  then tempd.MCHNT_CD end)) as total
               |from HIVE_MCHNT_INF_WALLET tempd
               |WHERE substr(tempd.OPEN_BUSS_BMP,1,2) in (10,11)
               |GROUP BY tempd.gb_region_nm) c
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
               |nvl(tempa.PHONE_LOCATION,nvl(tempb.PHONE_LOCATION,nvl(tempc.PHONE_LOCATION,nvl(tempd.PHONE_LOCATION,
               |nvl(tempe.PHONE_LOCATION,nvl(tempf.PHONE_LOCATION,nvl(tempg.PHONE_LOCATION,temph.PHONE_LOCATION)))))))
               |as USR_DEVICE_AREA,
               |'$today_dt' as REPORT_DT,
               |SUM(tempa.count)   AS GEN_QRCODE_NUM,
               |SUM(tempb.usr_cnt) AS GEN_QRCODE_USR_NUM,
               |SUM(tempc.count)   AS SWEEP_NUM,
               |SUM(tempd.usr_cnt) AS SWEEP_USR_NUM,
               |SUM(tempe.count)   AS PAY_NUM,
               |SUM(tempf.usr_cnt) AS PAY_USR_NUM,
               |SUM(tempg.count)   AS PAY_SUCC_NUM,
               |SUM(tempg.amt)     AS PAY_AMT,
               |SUM(temph.usr_cnt) AS PAY_SUCC_USR_NUM
               |from
               |(select
               |c.PHONE_LOCATION,
               |sum(c.cnt) as count
               |from
               |(select distinct b.PHONE_LOCATION,a.rec_crt_ts, a.cdhd_usr_id,a.cnt
               |from
               |(select cdhd_usr_id,rec_crt_ts,count(*) as cnt
               |from HIVE_PASSIVE_CODE_PAY_TRANS
               |where to_date(rec_crt_ts)='$today_dt'
               |and TRAN_CERTI like '10%'
               |group by cdhd_usr_id,rec_crt_ts) a
               |inner join HIVE_PRI_ACCT_INF b
               |on a.cdhd_usr_id=b.cdhd_usr_id) c
               |group by c.PHONE_LOCATION ) tempa
               |
               |FULL OUTER JOIN
               |
               |(SELECT
               |c.PHONE_LOCATION,
               |COUNT(distinct(c.cdhd_usr_id)) AS usr_cnt
               |FROM
               |( SELECT DISTINCT b.PHONE_LOCATION, a.cdhd_usr_id,  a.rec_crt_ts
               |FROM
               |( SELECT DISTINCT (cdhd_usr_id), rec_crt_ts
               |FROM  HIVE_PASSIVE_CODE_PAY_TRANS
               |WHERE  to_date(rec_crt_ts)='$today_dt'
               |AND TRAN_CERTI LIKE '10%') a
               |INNER JOIN  HIVE_PRI_ACCT_INF b
               |ON  a.cdhd_usr_id=b.cdhd_usr_id) c
               |GROUP BY  c.PHONE_LOCATION) tempb
               |on tempa.PHONE_LOCATION=tempb.PHONE_LOCATION
               |
               |FULL OUTER JOIN
               |
               |(select
               |c.PHONE_LOCATION,
               |sum(c.cnt) as count
               |from
               |(select distinct (b.PHONE_LOCATION) as PHONE_LOCATION, a.cdhd_usr_id,a.cnt
               |from
               |(select cdhd_usr_id,rec_crt_ts,count(*) as cnt
               |from HIVE_PASSIVE_CODE_PAY_TRANS
               |where to_date(rec_crt_ts)='$today_dt' and TRAN_CERTI like '10%'
               | group by cdhd_usr_id,rec_crt_ts) a
               |inner join HIVE_PRI_ACCT_INF b on a.cdhd_usr_id=b.cdhd_usr_id)c
               |group by c.PHONE_LOCATION ) tempc
               |on tempa.PHONE_LOCATION=tempc.PHONE_LOCATION
               |
               |FULL OUTER JOIN
               |
               |(select
               |c.PHONE_LOCATION,
               |count(distinct(c.cdhd_usr_id)) as usr_cnt
               |from
               |(select distinct (b.PHONE_LOCATION) as PHONE_LOCATION, a.cdhd_usr_id
               |from
               |(select distinct(cdhd_usr_id),rec_crt_ts
               |from HIVE_PASSIVE_CODE_PAY_TRANS
               |where to_date(rec_crt_ts)='$today_dt'
               |and TRAN_CERTI like '10%' ) a
               |inner join HIVE_PRI_ACCT_INF b
               |on a.cdhd_usr_id=b.cdhd_usr_id)c
               |group by c.PHONE_LOCATION ) tempd
               |on tempa.PHONE_LOCATION=tempd.PHONE_LOCATION
               |
               |FULL OUTER JOIN
               |
               |(select
               |c.PHONE_LOCATION,
               |sum(c.cnt) as count
               |from
               |(select distinct b.PHONE_LOCATION, a.cdhd_usr_id,a.cnt
               |from
               |(select cdhd_usr_id,rec_crt_ts,count(*) as cnt
               |from HIVE_PASSIVE_CODE_PAY_TRANS
               |where to_date(rec_crt_ts)='$today_dt'
               |and TRAN_CERTI like '10%'
               |and trans_tp='00' and trans_st in ('03','04','05','06','08','09','10','11')
               |group by cdhd_usr_id,rec_crt_ts) a
               |inner join HIVE_PRI_ACCT_INF b on a.cdhd_usr_id=b.cdhd_usr_id) c
               |group by c.PHONE_LOCATION ) tempe
               |on tempa.PHONE_LOCATION=tempe.PHONE_LOCATION
               |
               |FULL OUTER JOIN
               |
               |(select
               |c.PHONE_LOCATION,
               |count(distinct(c.cdhd_usr_id)) as usr_cnt
               |from
               |(select distinct  b.PHONE_LOCATION, a.cdhd_usr_id
               |from
               |(select distinct(cdhd_usr_id),rec_crt_ts
               |from HIVE_PASSIVE_CODE_PAY_TRANS
               |where to_date(rec_crt_ts)='$today_dt'
               | and TRAN_CERTI like '10%'
               |and trans_tp='00' and trans_st in ('03','04','05','06','08','09','10','11')) a
               |inner join HIVE_PRI_ACCT_INF b
               |on a.cdhd_usr_id=b.cdhd_usr_id) c
               |group by c.PHONE_LOCATION ) tempf
               |on tempa.PHONE_LOCATION=tempf.PHONE_LOCATION
               |
               |FULL OUTER JOIN
               |
               |(select
               |c.PHONE_LOCATION,
               |sum(c.cnt) as count,
               |sum(c.trans_at) as amt
               |from
               |(select distinct b.PHONE_LOCATION, a.cdhd_usr_id,a.cnt,a.trans_at
               |from
               |(select cdhd_usr_id,rec_crt_ts,sum(trans_at) as trans_at ,count(*) as cnt
               |from HIVE_PASSIVE_CODE_PAY_TRANS
               |where to_date(rec_crt_ts)='$today_dt'
               |and TRAN_CERTI like '10%'
               |and trans_tp='00' and trans_st in ('04','10')
               | group by cdhd_usr_id,rec_crt_ts) a
               |inner join HIVE_PRI_ACCT_INF b
               |on a.cdhd_usr_id=b.cdhd_usr_id) c
               |group by c.PHONE_LOCATION ) tempg
               |on tempa.PHONE_LOCATION=tempg.PHONE_LOCATION
               |
               |FULL OUTER JOIN
               |
               |(select
               |c.PHONE_LOCATION,
               |count(distinct(c.cdhd_usr_id)) as usr_cnt
               |from
               |(select distinct b.PHONE_LOCATION, a.cdhd_usr_id
               |from
               |(select distinct (cdhd_usr_id),rec_crt_ts
               |from HIVE_PASSIVE_CODE_PAY_TRANS
               |where to_date(rec_crt_ts)='$today_dt'
               |and TRAN_CERTI like '10%'
               |and trans_tp='00' and trans_st in ('04','10')) a
               |inner join HIVE_PRI_ACCT_INF b
               |on a.cdhd_usr_id=b.cdhd_usr_id) c
               |group by c.PHONE_LOCATION ) temph
               |on tempa.PHONE_LOCATION=temph.PHONE_LOCATION
               |GROUP BY nvl(tempa.PHONE_LOCATION,nvl(tempb.PHONE_LOCATION,nvl(tempc.PHONE_LOCATION,nvl(tempd.PHONE_LOCATION,
               |nvl(tempe.PHONE_LOCATION,nvl(tempf.PHONE_LOCATION,nvl(tempg.PHONE_LOCATION,temph.PHONE_LOCATION)))))))
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
               |SUM(tempa.count)   as gen_qrcode_num,
               |SUM(tempb.usr_cnt) as gen_qrcode_usr_num,
               |SUM(tempc.count)   as sweep_num,
               |SUM(tempd.usr_cnt) as sweep_usr_num,
               |SUM(tempe.count)   as pay_num,
               |SUM(tempf.usr_cnt) as pay_usr_num,
               |SUM(tempg.count)   as pay_succ_num,
               |SUM(tempg.amt)     as pay_amt,
               |SUM(temph.usr_cnt) as pay_succ_usr_num
               |
               |from
               |(select
               |c.GB_REGION_NM as GB_REGION_NM,
               |sum(c.cnt) as count
               |from
               |(select distinct b.GB_REGION_NM,a.rec_crt_ts, a.cdhd_usr_id,a.cnt
               |from
               |(select cdhd_usr_id,rec_crt_ts,mchnt_cd,count(*) as cnt
               |from hive_passive_code_pay_trans
               |where to_date(rec_crt_ts)='$today_dt'
               |and TRAN_CERTI like '10%'
               |group by cdhd_usr_id,rec_crt_ts,mchnt_cd) a
               |inner join hive_mchnt_inf_wallet b
               |on  a.mchnt_cd=b.mchnt_cd) c
               |group by c.GB_REGION_NM ) tempa
               |
               |FULL OUTER JOIN
               |
               |(SELECT
               |c.GB_REGION_NM as GB_REGION_NM,
               |COUNT(distinct(c.cdhd_usr_id)) AS usr_cnt
               |FROM
               |( SELECT DISTINCT b.GB_REGION_NM, a.cdhd_usr_id, a.rec_crt_ts
               |FROM
               |( SELECT DISTINCT (cdhd_usr_id), rec_crt_ts,mchnt_cd
               |FROM  hive_passive_code_pay_trans
               |WHERE  to_date(rec_crt_ts)='$today_dt'
               |AND TRAN_CERTI LIKE '10%') a
               |inner join HIVE_MCHNT_INF_WALLET b
               |on  a.mchnt_cd=b.mchnt_cd) c
               |GROUP BY  c.GB_REGION_NM) tempb
               |on tempa.GB_REGION_NM=tempb.GB_REGION_NM
               |
               |FULL OUTER JOIN
               |
               |(select c.GB_REGION_NM as GB_REGION_NM,
               |sum(c.cnt) as count
               |from
               |(select distinct b.GB_REGION_NM, a.cdhd_usr_id,a.cnt
               |from
               |(select cdhd_usr_id,rec_crt_ts,mchnt_cd,count(*) as cnt
               |from HIVE_PASSIVE_CODE_PAY_TRANS
               |where to_date(rec_crt_ts)='$today_dt' and TRAN_CERTI like '10%'
               |and mchnt_cd is not null and mchnt_cd<>'' group by cdhd_usr_id,rec_crt_ts,mchnt_cd) a
               |inner join HIVE_MCHNT_INF_WALLET b
               |on  a.mchnt_cd=b.mchnt_cd)c
               |group by c.GB_REGION_NM ) tempc
               |on tempa.GB_REGION_NM=tempc.GB_REGION_NM
               |
               |FULL OUTER JOIN
               |
               |(select c.GB_REGION_NM as GB_REGION_NM,
               |count(distinct(c.cdhd_usr_id)) as usr_cnt
               |from
               |(select distinct b.GB_REGION_NM, a.cdhd_usr_id
               |from
               |(select distinct(cdhd_usr_id),rec_crt_ts,mchnt_cd
               |from HIVE_PASSIVE_CODE_PAY_TRANS
               |where to_date(rec_crt_ts)='$today_dt'
               |and TRAN_CERTI like '10%') a
               |inner join HIVE_MCHNT_INF_WALLET b
               |on  a.mchnt_cd=b.mchnt_cd) c
               |group by c.GB_REGION_NM ) tempd
               |on tempa.GB_REGION_NM=tempd.GB_REGION_NM
               |
               |FULL OUTER JOIN
               |
               |(select c.GB_REGION_NM as GB_REGION_NM,
               |sum(c.cnt) as count
               |from
               |(select distinct b.GB_REGION_NM, a.cdhd_usr_id,a.cnt
               |from
               |(select cdhd_usr_id,rec_crt_ts,mchnt_cd,count(*) as cnt
               |from HIVE_PASSIVE_CODE_PAY_TRANS
               |where to_date(rec_crt_ts)='$today_dt'
               |and TRAN_CERTI like '10%'
               |and trans_tp='00' and trans_st in ('03','04','05','06','08','09','10','11')
               |group by cdhd_usr_id,rec_crt_ts,mchnt_cd) a
               |inner join HIVE_MCHNT_INF_WALLET b
               |on  a.mchnt_cd=b.mchnt_cd) c
               |group by c.GB_REGION_NM ) tempe
               |on tempa.GB_REGION_NM=tempe.GB_REGION_NM
               |
               |FULL OUTER JOIN
               |
               |(select c.GB_REGION_NM as GB_REGION_NM,
               |count(distinct(c.cdhd_usr_id)) as usr_cnt
               |from
               |(select distinct  b.GB_REGION_NM, a.cdhd_usr_id
               |from
               |(select distinct(cdhd_usr_id),rec_crt_ts ,mchnt_cd
               |from HIVE_PASSIVE_CODE_PAY_TRANS
               |where to_date(rec_crt_ts)='$today_dt'
               | and TRAN_CERTI like '10%'
               |and trans_tp='00' and trans_st in ('03','04','05','06','08','09','10','11')) a
               |inner join HIVE_MCHNT_INF_WALLET b
               |on  a.mchnt_cd=b.mchnt_cd) c
               |group by GB_REGION_NM ) tempf
               |on tempa.GB_REGION_NM=tempf.GB_REGION_NM
               |
               |FULL OUTER JOIN
               |
               |(select
               |c.GB_REGION_NM as GB_REGION_NM,
               |sum(c.cnt) as count,
               |sum(c.trans_at) as amt
               |from
               |(select distinct b.GB_REGION_NM, a.cdhd_usr_id,a.cnt,a.trans_at
               |from
               |(select cdhd_usr_id,rec_crt_ts,mchnt_cd,sum(trans_at) as trans_at ,count(*) as cnt
               |from HIVE_PASSIVE_CODE_PAY_TRANS
               |where to_date(rec_crt_ts)='$today_dt'
               |and TRAN_CERTI like '10%'
               |and trans_tp='00' and trans_st in ('04','10')
               | group by cdhd_usr_id,rec_crt_ts,mchnt_cd) a
               |inner join HIVE_MCHNT_INF_WALLET b
               |on  a.mchnt_cd=b.mchnt_cd) c
               |group by c.GB_REGION_NM ) tempg
               |on tempa.GB_REGION_NM=tempg.GB_REGION_NM
               |
               |FULL OUTER JOIN
               |
               |(select c.GB_REGION_NM as GB_REGION_NM,
               |count(distinct(c.cdhd_usr_id)) as usr_cnt
               |from (select distinct b.GB_REGION_NM, a.cdhd_usr_id
               |from (select distinct (cdhd_usr_id),rec_crt_ts ,mchnt_cd
               |from HIVE_PASSIVE_CODE_PAY_TRANS
               |where to_date(rec_crt_ts)='$today_dt'
               |and TRAN_CERTI like '10%'
               |and trans_tp='00' and trans_st in ('04','10')) a
               |inner join HIVE_MCHNT_INF_WALLET b
               |on  a.mchnt_cd=b.mchnt_cd) c
               |group by c.GB_REGION_NM ) temph
               |on tempa.GB_REGION_NM=temph.GB_REGION_NM
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
               |tempg.grp_nm as STORE_FRIST_NM,
               |tempg.tp_nm as STORE_SECOND_NM,
               |'$today_dt' as REPORT_DT,
               |sum(tempa.count)   AS SWEEP_NUM,
               |sum(tempb.usr_cnt) AS SWEEP_USR_NUM,
               |sum(tempc.count)   AS PAY_NUM,
               |sum(tempd.usr_cnt) AS PAY_USR_NUM,
               |sum(tempe.count)   AS PAY_SUCC_NUM,
               |sum(tempe.amt)     AS PAY_AMT,
               |sum(tempf.usr_cnt) AS PAY_SUCC_USR_NUM
               |
               |from
               |(SELECT
               |tp_grp.MCHNT_TP_GRP_DESC_CN AS grp_nm ,
               |tp.MCHNT_TP_DESC_CN         AS tp_nm,
               |tp.MCHNT_TP
               |FROM  HIVE_MCHNT_TP tp
               |LEFT JOIN HIVE_MCHNT_TP_GRP tp_grp
               |ON tp.MCHNT_TP_GRP=tp_grp.MCHNT_TP_GRP) tempg
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
               |from HIVE_PASSIVE_CODE_PAY_TRANS
               |where to_date(rec_crt_ts)='$today_dt' and TRAN_CERTI like '10%'
               |group by cdhd_usr_id,rec_crt_ts,mchnt_cd) a
               |inner join HIVE_MCHNT_INF_WALLET b
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
               |from HIVE_PASSIVE_CODE_PAY_TRANS
               |where to_date(rec_crt_ts)='$today_dt'
               | and TRAN_CERTI like '10%') a
               |inner join HIVE_MCHNT_INF_WALLET b
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
               |from HIVE_PASSIVE_CODE_PAY_TRANS
               |where to_date(rec_crt_ts)='$today_dt'
               |and TRAN_CERTI like '10%' and mchnt_cd is not null and mchnt_cd<>''
               |and trans_tp='00' and trans_st in ('03','04','05','06','08','09','10','11')
               |group by cdhd_usr_id,rec_crt_ts,mchnt_cd) a
               |inner join HIVE_MCHNT_INF_WALLET b
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
               |from HIVE_PASSIVE_CODE_PAY_TRANS
               |where to_date(rec_crt_ts)='$today_dt'
               |and TRAN_CERTI like '10%'  and mchnt_cd is not null and mchnt_cd<>''
               |and trans_tp='00' and trans_st in ('03','04','05','06','08','09','10','11')) a
               |inner join HIVE_MCHNT_INF_WALLET b
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
               |from HIVE_PASSIVE_CODE_PAY_TRANS
               |where to_date(rec_crt_ts)='$today_dt'
               |and TRAN_CERTI like '10%' and mchnt_cd is not null and mchnt_cd<>''
               |and trans_tp='00' and trans_st in ('04','10')
               | group by cdhd_usr_id,rec_crt_ts,mchnt_cd) a
               |inner join HIVE_MCHNT_INF_WALLET b
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
               |from HIVE_PASSIVE_CODE_PAY_TRANS
               |where to_date(rec_crt_ts)='$today_dt'
               |and TRAN_CERTI like '10%'  and mchnt_cd is not null and mchnt_cd<>''
               |and trans_tp='00' and trans_st in ('04','10')) a
               |inner join HIVE_MCHNT_INF_WALLET b
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
               |final.mchnt_cn_nm as STORE_FRIST_NM,
               |'$today_dt' as REPORT_DT,
               |sum(final.type) as TYPE,
               |sum(final.Sweep_num) as SWEEP_NUM,
               |sum(final.sweep_usr_num) as SWEEP_USR_NUM,
               |sum(final.pay_num) as PAY_NUM,
               |sum(final.pay_usr_num) as PAY_USR_NUM,
               |sum(final.pay_succ_num) as PAY_SUCC_NUM,
               |sum(final.pay_amt) as PAY_AMT,
               |sum(final.pay_succ_usr_num) as PAY_SUCC_USR_NUM
               |
               |from
               |
               |(select
               |nvl(tempa.mchnt_cn_nm,nvl(tempb.mchnt_cn_nm,nvl(tempc.mchnt_cn_nm,nvl(tempd.mchnt_cn_nm,
               |nvl(tempe.mchnt_cn_nm,tempf.mchnt_cn_nm)))))
               |AS mchnt_cn_nm ,
               |'0' as type,
               |SUM(tempa.count) as Sweep_num,
               |SUM(tempb.usr_cnt) as sweep_usr_num,
               |SUM(tempc.count) as pay_num,
               |SUM(tempd.usr_cnt) as pay_usr_num,
               |SUM(tempe.count) as pay_succ_num,
               |SUM(tempe.amt) as pay_amt,
               |SUM(tempf.usr_cnt) as pay_succ_usr_num
               |from
               |(
               |select c.mchnt_cn_nm,
               |sum(c.cnt) as count
               |from
               |(select distinct b.mchnt_cn_nm, a.cdhd_usr_id,a.cnt
               |from
               |(select cdhd_usr_id,rec_crt_ts,mchnt_cd,count(*) as cnt
               |from HIVE_PASSIVE_CODE_PAY_TRANS
               |where to_date(rec_crt_ts)='$today_dt' and TRAN_CERTI like '10%'
               |group by cdhd_usr_id,rec_crt_ts,mchnt_cd) a
               |inner join HIVE_MCHNT_INF_WALLET b
               |on a.mchnt_cd=b.mchnt_cd) c
               |group by c.mchnt_cn_nm ) tempa
               |
               |FULL OUTER JOIN
               |
               |(
               |select c.mchnt_cn_nm,
               |count(distinct(c.cdhd_usr_id)) as usr_cnt
               |from
               |(select distinct b.mchnt_cn_nm, a.cdhd_usr_id
               |from
               |(select distinct(cdhd_usr_id),rec_crt_ts,mchnt_cd
               |from HIVE_PASSIVE_CODE_PAY_TRANS
               |where to_date(rec_crt_ts)='$today_dt'
               |and TRAN_CERTI like '10%') a
               |inner join HIVE_MCHNT_INF_WALLET b
               |on a.mchnt_cd=b.mchnt_cd) c
               |group by c.mchnt_cn_nm ) tempb
               |on tempa.mchnt_cn_nm=tempb.mchnt_cn_nm
               |
               |FULL OUTER JOIN
               |
               |(
               |select c.mchnt_cn_nm,
               |sum(c.cnt) as count
               |from
               |(select distinct b.mchnt_cn_nm, a.cdhd_usr_id,a.cnt
               |from
               |(select cdhd_usr_id,rec_crt_ts,mchnt_cd,count(*) as cnt
               |from HIVE_PASSIVE_CODE_PAY_TRANS
               |where to_date(rec_crt_ts)='$today_dt'
               |and TRAN_CERTI like '10%'
               |and trans_tp='00' and trans_st in ('03','04','05','06','08','09','10','11')
               |group by cdhd_usr_id,rec_crt_ts,mchnt_cd) a
               |inner join HIVE_MCHNT_INF_WALLET b
               |on a.mchnt_cd=b.mchnt_cd) c
               |group by c.mchnt_cn_nm ) tempc
               |on tempa.mchnt_cn_nm=tempc.mchnt_cn_nm
               |
               |FULL OUTER JOIN
               |
               |(select c.mchnt_cn_nm,
               |count(distinct(c.cdhd_usr_id)) as usr_cnt
               |from
               |(select distinct b.mchnt_cn_nm, a.cdhd_usr_id
               |from
               |(select distinct(cdhd_usr_id),rec_crt_ts ,mchnt_cd
               |from HIVE_PASSIVE_CODE_PAY_TRANS
               |where to_date(rec_crt_ts)='$today_dt'
               |and TRAN_CERTI like '10%'
               |and trans_tp='00' and trans_st in ('03','04','05','06','08','09','10','11')) a
               |inner join HIVE_MCHNT_INF_WALLET b
               |on a.mchnt_cd=b.mchnt_cd) c
               |group by c.mchnt_cn_nm ) tempd
               |on tempa.mchnt_cn_nm=tempd.mchnt_cn_nm
               |
               |FULL OUTER JOIN
               |
               |(select
               |c.mchnt_cn_nm,
               |sum(c.cnt) as count,
               |sum(c.trans_at) as amt
               |from
               |(select distinct b.mchnt_cn_nm, a.cdhd_usr_id,a.cnt,a.trans_at
               |from
               |(select cdhd_usr_id,rec_crt_ts,mchnt_cd,sum(trans_at) as trans_at ,count(*) as cnt
               |from HIVE_PASSIVE_CODE_PAY_TRANS
               |where to_date(rec_crt_ts)='$today_dt'
               |and TRAN_CERTI like '10%'
               |and trans_tp='00' and trans_st in ('04','10')
               |group by cdhd_usr_id,rec_crt_ts,mchnt_cd) a
               |inner join HIVE_MCHNT_INF_WALLET b
               |on a.mchnt_cd=b.mchnt_cd) c
               |group by c.mchnt_cn_nm ) tempe
               |on tempa.mchnt_cn_nm=tempe.mchnt_cn_nm
               |
               |FULL OUTER JOIN
               |
               |(select c.mchnt_cn_nm,
               |count(distinct(c.cdhd_usr_id)) as usr_cnt
               |from
               |(select distinct b.mchnt_cn_nm, a.cdhd_usr_id
               |from
               |(select distinct (cdhd_usr_id),rec_crt_ts ,mchnt_cd
               |from HIVE_PASSIVE_CODE_PAY_TRANS
               |where to_date(rec_crt_ts)='$today_dt'
               |and TRAN_CERTI like '10%'
               |and trans_tp='00' and trans_st in ('04','10')) a
               |inner join HIVE_MCHNT_INF_WALLET b
               |on a.mchnt_cd=b.mchnt_cd) c
               |group by c.mchnt_cn_nm ) tempf
               |on tempa.mchnt_cn_nm=tempf.mchnt_cn_nm
               |GROUP BY nvl(tempa.mchnt_cn_nm,nvl(tempb.mchnt_cn_nm,nvl(tempc.mchnt_cn_nm,nvl(tempd.mchnt_cn_nm,
               |nvl(tempe.mchnt_cn_nm,tempf.mchnt_cn_nm)))))
               |order by Sweep_num
               |desc limit 20
               |
               |union all
               |
               |select
               |nvl(ta.mchnt_cn_nm,nvl(tb.mchnt_cn_nm,nvl(tc.mchnt_cn_nm,nvl(td.mchnt_cn_nm,
               |nvl(te.mchnt_cn_nm,tf.mchnt_cn_nm)))))
               |AS mchnt_cn_nm ,
               |'1' as type,
               |sum(ta.count) as Sweep_num,
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
               |from HIVE_PASSIVE_CODE_PAY_TRANS
               |where to_date(rec_crt_ts)='$today_dt' and TRAN_CERTI like '10%' and trans_tp='01'
               |group by cdhd_usr_id,rec_crt_ts,mchnt_cd) a
               |inner join HIVE_MCHNT_INF_WALLET b
               |on a.mchnt_cd=b.mchnt_cd) c
               |group by c.mchnt_cn_nm ) ta
               |
               |FULL OUTER JOIN
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
               |from HIVE_PASSIVE_CODE_PAY_TRANS
               |where to_date(rec_crt_ts)='$today_dt'
               |and TRAN_CERTI like '10%'
               |and trans_tp='01') a
               |inner join HIVE_MCHNT_INF_WALLET b
               |on a.mchnt_cd=b.mchnt_cd) c
               |group by c.mchnt_cn_nm
               |) tb
               |on ta.mchnt_cn_nm=tb.mchnt_cn_nm
               |
               |FULL OUTER JOIN
               |
               |(select c.mchnt_cn_nm,
               |sum(c.cnt) as count
               |from
               |(select distinct b.mchnt_cn_nm, a.cdhd_usr_id,a.cnt
               |from
               |(select cdhd_usr_id,rec_crt_ts,mchnt_cd,count(*) as cnt
               |from HIVE_PASSIVE_CODE_PAY_TRANS
               |where to_date(rec_crt_ts)='$today_dt'
               |and TRAN_CERTI like '10%'
               |and trans_tp='01' and trans_st in ('03','04','05','06','08','09','10','11')
               |group by cdhd_usr_id,rec_crt_ts,mchnt_cd) a
               |inner join HIVE_MCHNT_INF_WALLET b
               |on a.mchnt_cd=b.mchnt_cd) c
               |group by c.mchnt_cn_nm ) tc
               |on ta.mchnt_cn_nm=tc.mchnt_cn_nm
               |
               |FULL OUTER JOIN
               |
               |(
               |select c.mchnt_cn_nm,
               |count(distinct(c.cdhd_usr_id)) as usr_cnt
               |from
               |(select distinct b.mchnt_cn_nm, a.cdhd_usr_id
               |from
               |(select distinct(cdhd_usr_id),rec_crt_ts ,mchnt_cd
               |from HIVE_PASSIVE_CODE_PAY_TRANS
               |where to_date(rec_crt_ts)='$today_dt'
               |and TRAN_CERTI like '10%'
               |and trans_tp='01' and trans_st in ('03','04','05','06','08','09','10','11')) a
               |inner join HIVE_MCHNT_INF_WALLET b
               |on a.mchnt_cd=b.mchnt_cd)c
               |group by c.mchnt_cn_nm ) td
               |on ta.mchnt_cn_nm=td.mchnt_cn_nm
               |
               |FULL OUTER JOIN
               |
               |(select
               |c.mchnt_cn_nm,
               |sum(c.cnt) as count,
               |sum(c.trans_at) as amt
               |from
               |(select distinct b.mchnt_cn_nm, a.cdhd_usr_id,a.cnt,a.trans_at
               |from
               |(select cdhd_usr_id,rec_crt_ts,mchnt_cd,sum(trans_at) as trans_at ,count(*) as cnt
               |from HIVE_PASSIVE_CODE_PAY_TRANS
               |where to_date(rec_crt_ts)='$today_dt'
               |and TRAN_CERTI like '10%'
               |and trans_tp='01' and trans_st in ('04','10')
               |group by cdhd_usr_id,rec_crt_ts,mchnt_cd) a
               |inner join HIVE_MCHNT_INF_WALLET b
               |on a.mchnt_cd=b.mchnt_cd) c
               |group by c.mchnt_cn_nm ) te
               |on ta.mchnt_cn_nm=te.mchnt_cn_nm
               |
               |FULL OUTER JOIN
               |
               |(select c.mchnt_cn_nm,
               |count(distinct(c.cdhd_usr_id)) as usr_cnt
               |from
               |(select distinct b.mchnt_cn_nm, a.cdhd_usr_id
               |from
               |(select distinct (cdhd_usr_id),rec_crt_ts ,mchnt_cd
               |from HIVE_PASSIVE_CODE_PAY_TRANS
               |where to_date(rec_crt_ts)='$today_dt'
               |and TRAN_CERTI like '10%'
               |and trans_tp='01' and trans_st in ('04','10')) a
               |inner join HIVE_MCHNT_INF_WALLET b
               |on a.mchnt_cd=b.mchnt_cd)c
               |group by c.mchnt_cn_nm ) tf
               |on ta.mchnt_cn_nm=tf.mchnt_cn_nm
               |group by nvl(ta.mchnt_cn_nm,nvl(tb.mchnt_cn_nm,nvl(tc.mchnt_cn_nm,nvl(td.mchnt_cn_nm,
               |nvl(te.mchnt_cn_nm,tf.mchnt_cn_nm)))))
               |order by Sweep_num
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
               |ta.resp_code as RESP_CODE,
               |'$today_dt' as REPORT_DT,
               |count(distinct ta.trans_seq) as SWEEP_USR_NUM,
               |sum(ta.trans_at) as SWEEP_NUM
               |from HIVE_PASSIVE_CODE_PAY_TRANS ta
               |where to_date(ta.rec_crt_ts)='$today_dt'
               |and ta.TRAN_CERTI like '10%'
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
               |        and um_trans_id='ac02000065'
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
               |    a.cup_branch_ins_id_nm  as cup_branch_ins_id_nm,
               |    a.trans_dt              as report_dt           ,
               |    a.transcnt              as trans_cnt           ,
               |    b.suctranscnt           as suc_trans_cnt       ,
               |    b.transat               as trans_at            ,
               |    b.discountat            as discount_at         ,
               |    b.transusrcnt           as trans_usr_cnt       ,
               |    b.transcardcnt          as trans_card_cnt
               |from
               |    (
               |        select
               |            bill.cup_branch_ins_id_nm,
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
               |        and bill.bill_sub_tp in ('01', '03')
               |        and trans.part_trans_dt >= '$start_dt'
               |        and trans.part_trans_dt <= '$end_dt'
               |        group by
               |            bill.cup_branch_ins_id_nm,
               |            trans.trans_dt) a
               |left join
               |    (
               |        select
               |            bill.cup_branch_ins_id_nm,
               |            trans.trans_dt,
               |            count(1) as suctranscnt,
               |            sum(trans.trans_at) as transat,
               |            sum(trans.discount_at) as discountat,
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
               |        and trans.um_trans_id iN ('AC02000065','AC02000063')
               |        and bill.bill_sub_tp in ('01','03')
               |        and trans.part_trans_dt >= '$start_dt'
               |        and trans.part_trans_dt <= '$end_dt'
               |
               |        group by
               |            bill.cup_branch_ins_id_nm,
               |            trans.trans_dt)b
               |on
               |a.cup_branch_ins_id_nm = b.cup_branch_ins_id_nm and a.trans_dt = b.trans_dt
               |
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
           |            acpt_ins.cup_branch_ins_id_nm,
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
           |            acpt_ins.cup_branch_ins_id_nm,
           |            trans.trans_dt) a
           |left join
           |    (
           |        select
           |            acpt_ins.cup_branch_ins_id_nm,
           |            trans.trans_dt,
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
           |                                  'ac02000063')
           |        and bill.bill_sub_tp in ('01',
           |                                 '03')
           |        and trans.part_trans_dt >= '$start_dt'
           |        and trans.part_trans_dt <= '$end_dt'
           |        group by
           |            acpt_ins.cup_branch_ins_id_nm,
           |            trans.trans_dt)b
           |on
           |a.cup_branch_ins_id_nm = b.cup_branch_ins_id_nm and a.trans_dt = b.trans_dt
           |where  a.cup_branch_ins_id_nm is not null and  a.trans_dt is not null
           |
           |
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
           |SELECT
           |    A.PHONE_LOCATION as MOBILE_LOC,
           |    A.TRANS_DT as REPORT_DT,
           |    A.TRANSCNT as TRANS_CNT,
           |    B.SUCTRANSCNT as SUC_TRANS_CNT,
           |    B.TRANSAT as TRANS_AT,
           |    B.DISCOUNTAT as DISCOUNT_AT,
           |    B.transUsrCnt as TRANS_USR_CNT,
           |    B.TRANSCARDCNT as TRANS_CARD_CNT
           |FROM
           |    (
           |        SELECT
           |            PRI_ACCT.PHONE_LOCATION,
           |            TRANS.TRANS_DT,
           |            COUNT(*) AS TRANSCNT
           |        FROM
           |            HIVE_ACC_TRANS TRANS
           |        LEFT JOIN
           |            HIVE_TICKET_BILL_BAS_INF BILL
           |        ON
           |            (
           |                TRANS.BILL_ID=BILL.BILL_ID)
           |        LEFT JOIN
           |            HIVE_PRI_ACCT_INF PRI_ACCT
           |        ON
           |            (
           |                TRANS.CDHD_USR_ID = PRI_ACCT.CDHD_USR_ID)
           |        WHERE
           |            TRANS.UM_TRANS_ID IN ('AC02000065','AC02000063')
           |        AND BILL.BILL_SUB_TP IN ('01','03')
           |        AND TRANS.PART_TRANS_DT >= '$start_dt'
           |        AND TRANS.PART_TRANS_DT <= '$end_dt'
           |        GROUP BY
           |            PRI_ACCT.PHONE_LOCATION,
           |            TRANS.TRANS_DT) A
           |LEFT JOIN
           |    (
           |        SELECT
           |            PRI_ACCT.PHONE_LOCATION,
           |            TRANS.TRANS_DT,
           |            COUNT(*) AS SUCTRANSCNT,
           |            SUM(TRANS.TRANS_AT) AS TRANSAT,
           |            SUM(TRANS.DISCOUNT_AT)          AS DISCOUNTAT,
           |            COUNT(DISTINCT TRANS.CDHD_USR_ID) AS TRANSUSRCNT,
           |            COUNT(DISTINCT TRANS.PRI_ACCT_NO) AS TRANSCARDCNT
           |        FROM
           |            HIVE_ACC_TRANS TRANS
           |        LEFT JOIN
           |            HIVE_TICKET_BILL_BAS_INF BILL
           |        ON
           |            (
           |                TRANS.BILL_ID=BILL.BILL_ID)
           |        LEFT JOIN
           |            HIVE_PRI_ACCT_INF PRI_ACCT
           |        ON
           |            (
           |                TRANS.CDHD_USR_ID = PRI_ACCT.CDHD_USR_ID)
           |        WHERE
           |            TRANS.SYS_DET_CD = 'S'
           |        AND TRANS.UM_TRANS_ID IN ('AC02000065','AC02000063')
           |        AND BILL.BILL_SUB_TP IN ('01','03')
           |        AND TRANS.PART_TRANS_DT >= '$start_dt'
           |        AND TRANS.PART_TRANS_DT <= '$end_dt'
           |        GROUP BY PRI_ACCT.PHONE_LOCATION, TRANS.TRANS_DT
           |		)B
           |ON
           |    (
           |        A.PHONE_LOCATION = B.PHONE_LOCATION
           |    AND A.TRANS_DT = B.TRANS_DT )
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
           |SELECT
           |    A.INS_ID_CD as LINK_TP_NM,
           |    A.SYS_SETTLE_DT as REPORT_DT,
           |    A.TRANSCNT AS TRANS_CNT,
           |    B.SUCTRANSCNT as SUC_TRANS_CNT,
           |    B.TRANSAT  as TRANS_AT,
           |    B.DISCOUNTAT as DISCOUNT_AT,
           |    B.TRANSUSRCNT AS TRANS_USR_CNT,
           |    B.CARDCNT as TRANS_CARD_CNT
           |
               |FROM
           |    (
           |        SELECT
           |            CASE
           |                WHEN TRANS.FWD_INS_ID_CD IN ('00097310',
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
           |                THEN '直连'
           |                ELSE '间连'
           |            END AS INS_ID_CD,
           |            TRANS.SYS_SETTLE_DT,
           |            COUNT(*) AS TRANSCNT
           |        FROM
           |            HIVE_ACC_TRANS TRANS
           |        WHERE
           |            TRANS.SYS_SETTLE_DT >= '$start_dt'
           |        AND TRANS.SYS_SETTLE_DT <= '$end_dt'
           |        GROUP BY
           |            CASE
           |                WHEN TRANS.FWD_INS_ID_CD IN ('00097310',
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
           |                THEN '直连'
           |                ELSE '间连'
           |            END,
           |            TRANS.SYS_SETTLE_DT) A
           |LEFT JOIN
           |    (
           |        SELECT
           |            CASE
           |                WHEN TRANS.FWD_INS_ID_CD IN ('00097310',
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
           |                THEN '直连'
           |                ELSE '间连'
           |            END AS INS_ID_CD,
           |            TRANS.SYS_SETTLE_DT,
           |            COUNT(*)          AS SUCTRANSCNT,
           |            SUM(TRANS.TRANS_TOT_AT) AS TRANSAT,
           |            SUM(CAST((
           |                    CASE TRIM(TRANS.DISCOUNT_AT)
           |                        WHEN ''
           |                        THEN '000000000000'
           |                        ELSE TRIM(TRANS.DISCOUNT_AT)
           |                    END) AS BIGINT))          AS DISCOUNTAT,
           |            COUNT(DISTINCT TRANS.CDHD_USR_ID) AS TRANSUSRCNT,
           |            COUNT(DISTINCT TRANS.PRI_ACCT_NO)       AS CARDCNT
           |        FROM
           |            HIVE_ACC_TRANS TRANS
           |        WHERE
           |            TRANS.CHSWT_RESP_CD='00'
           |        AND TRANS.SYS_SETTLE_DT >= '$start_dt'
           |        AND TRANS.SYS_SETTLE_DT <= '$end_dt'
           |        GROUP BY
           |            CASE
           |                WHEN TRANS.FWD_INS_ID_CD IN ('00097310',
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
           |                THEN '直连'
           |                ELSE '间连'
           |            END,
           |            TRANS.SYS_SETTLE_DT) B
           |ON
           |    A.INS_ID_CD=B.INS_ID_CD
           |AND A.SYS_SETTLE_DT = B.SYS_SETTLE_DT
           |ORDER BY
           |    A.TRANSCNT DESC
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
    * NOTE: 添加过滤条件 WHERE A.FIRST_PARA_NM IS NOT NULL
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
               |    a.first_para_nm    as first_ind_nm,
               |    a.second_para_nm   as second_ind_nm,
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
               |            mp.mchnt_para_cn_nm  as first_para_nm,
               |            mp1.mchnt_para_cn_nm as second_para_nm,
               |            trans.trans_dt,
               |            count(1) as transcnt
               |        from
               |            hive_acc_trans trans
               |        inner join
               |            hive_store_term_relation str
               |        on
               |            (
               |                trans.card_accptr_cd = str.mchnt_cd
               |            and trans.card_accptr_term_id = str.term_id)
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
               |        left join
               |            hive_ticket_bill_bas_inf bill
               |        on
               |            (
               |                trans.bill_id=bill.bill_id)
               |        where
               |            trans.um_trans_id in ('AC02000065',
               |                                  'AC02000063')
               |        and str.rec_id is not null
               |        and bill.bill_sub_tp in ('01',
               |                                 '03')
               |        and trans.part_trans_dt = '$today_dt'
               |        group by
               |            mp.mchnt_para_cn_nm,
               |            mp1.mchnt_para_cn_nm,
               |            trans_dt) a
               |left join
               |    (
               |        select
               |            mp.mchnt_para_cn_nm  as first_para_nm,
               |            mp1.mchnt_para_cn_nm as second_para_nm,
               |            trans.trans_dt,
               |            count(1)                          as suctranscnt,
               |            sum(trans.trans_at)               as transat,
               |            sum(trans.discount_at)            as discountat,
               |            count(distinct trans.cdhd_usr_id) as transusrcnt,
               |            count(distinct trans.pri_acct_no) as transcardcnt
               |        from
               |            hive_acc_trans trans
               |        inner join
               |            hive_store_term_relation str
               |        on
               |            (
               |                trans.card_accptr_cd = str.mchnt_cd
               |            and trans.card_accptr_term_id = str.term_id)
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
               |        left join
               |            hive_ticket_bill_bas_inf bill
               |        on
               |            (
               |                trans.bill_id=bill.bill_id)
               |        where
               |            trans.sys_det_cd = 's'
               |        and trans.um_trans_id in ('AC02000065',
               |                                  'AC02000063')
               |        and str.rec_id is not null
               |        and bill.bill_sub_tp in ('01',
               |                                 '03')
               |        and trans.part_trans_dt = '$today_dt'
               |        group by
               |            mp.mchnt_para_cn_nm,
               |            mp1.mchnt_para_cn_nm,
               |            trans_dt)b
               |on
               |    (
               |        a.first_para_nm = b.first_para_nm
               |    and a.second_para_nm = b.second_para_nm
               |    and a.trans_dt = b.trans_dt )
               |union all
               |select
               |    a.first_para_nm,
               |    a.second_para_nm,
               |    a.trans_dt,
               |    a.transcnt,
               |    b.suctranscnt,
               |    b.transat,
               |    b.discountat,
               |    b.transusrcnt,
               |    b.transcardcnt
               |from
               |    (
               |        select
               |            mp.mchnt_para_cn_nm  as first_para_nm,
               |            mp1.mchnt_para_cn_nm as second_para_nm,
               |            trans.trans_dt,
               |            count(1) as transcnt
               |        from
               |            hive_acc_trans trans
               |        left join
               |            hive_store_term_relation str
               |        on
               |            (
               |                trans.card_accptr_cd = str.mchnt_cd
               |            and trans.card_accptr_term_id = str.term_id)
               |        left join
               |            (
               |                select
               |                    mchnt_cd,
               |                    max(third_party_ins_id) over (partition by mchnt_cd) as third_party_ins_id
               |                from
               |                    hive_store_term_relation) str1
               |        on
               |            (
               |                trans.card_accptr_cd = str1.mchnt_cd)
               |        left join
               |            hive_preferential_mchnt_inf pmi
               |        on
               |            (
               |                str1.third_party_ins_id = pmi.mchnt_cd)
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
               |        left join
               |            hive_ticket_bill_bas_inf bill
               |        on
               |            (
               |                trans.bill_id=bill.bill_id)
               |        where
               |            trans.um_trans_id in ('AC02000065',
               |                                  'AC02000063')
               |        and bill.bill_sub_tp in ('01',
               |                                 '03')
               |        and str.rec_id is null
               |        and trans.part_trans_dt = '$today_dt'
               |        group by
               |            mp.mchnt_para_cn_nm,
               |            mp1.mchnt_para_cn_nm,
               |            trans_dt) a
               |left join
               |    (
               |        select
               |            mp.mchnt_para_cn_nm  as first_para_nm,
               |            mp1.mchnt_para_cn_nm as second_para_nm,
               |            trans.trans_dt,
               |            count(1)                          as suctranscnt,
               |            sum(trans.trans_at)               as transat,
               |            sum(trans.discount_at)            as discountat,
               |            count(distinct trans.cdhd_usr_id) as transusrcnt,
               |            count(distinct trans.pri_acct_no) as transcardcnt
               |        from
               |            hive_acc_trans trans
               |        left join
               |            hive_store_term_relation str
               |        on
               |            (
               |                trans.card_accptr_cd = str.mchnt_cd
               |            and trans.card_accptr_term_id = str.term_id)
               |        left join
               |            (
               |                select
               |                    mchnt_cd,
               |                    max(third_party_ins_id) over (partition by mchnt_cd) as third_party_ins_id
               |                from
               |                    hive_store_term_relation) str1
               |        on
               |            (
               |                trans.card_accptr_cd = str1.mchnt_cd)
               |        left join
               |            hive_preferential_mchnt_inf pmi
               |        on
               |            (
               |                str1.third_party_ins_id = pmi.mchnt_cd)
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
               |        left join
               |            hive_ticket_bill_bas_inf bill
               |        on
               |            (
               |                trans.bill_id=bill.bill_id)
               |        where
               |            trans.sys_det_cd = 'S'
               |        and trans.um_trans_id in ('AC02000065',
               |                                  'AC02000063')
               |        and bill.bill_sub_tp in ('01',
               |                                 '03')
               |        and str.rec_id is null
               |        and trans.part_trans_dt = '$today_dt'
               |        group by
               |            mp.mchnt_para_cn_nm,
               |            mp1.mchnt_para_cn_nm,
               |            trans_dt)b
               |on
               |    (
               |        a.first_para_nm = b.first_para_nm
               |    and a.second_para_nm = b.second_para_nm
               |    and a.trans_dt = b.trans_dt )
               |where a.first_para_nm is not null
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
           |                    tp_grp.mchnt_tp_grp_desc_cn as grp_nm ,
           |                    tp.mchnt_tp_desc_cn         as tp_nm,
           |                    trans_dt,
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
           |            a1.trans_dt ) a
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
           |                    tp_grp.mchnt_tp_grp_desc_cn as grp_nm ,
           |                    tp.mchnt_tp_desc_cn         as tp_nm,
           |                    trans_dt,
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
           |                and bill_sub_tp in ('01',
           |                                    '03')
           |                and trans.part_trans_dt>='$start_dt'
           |                and trans.part_trans_dt<='$end_dt' ) b1
           |        group by
           |            b1.grp_nm,
           |            b1.tp_nm,
           |            b1.trans_dt) b
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
    * notice: 主键空值已经过滤 WHERE A.ISS_INS_CN_NM IS NOT NULL
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
           |            cbi.iss_ins_cn_nm,
           |            trans.trans_dt,
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
           |            trans.um_trans_id in ('AC02000065',
           |                                  'AC02000063')
           |        and bill.bill_sub_tp in ('01',
           |                                 '03')
           |        and trans.part_trans_dt >= '$start_dt'
           |        and trans.part_trans_dt <= '$end_dt'
           |        group by
           |            cbi.iss_ins_cn_nm,
           |            trans_dt) a
           |left join
           |    (
           |        select
           |            cbi.iss_ins_cn_nm,
           |            trans.trans_dt,
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
           |        and trans.um_trans_id in ('AC02000065',
           |                                  'AC02000063')
           |        and bill.bill_sub_tp in ('01',
           |                                 '03')
           |        and trans.part_trans_dt >= '$start_dt'
           |        and trans.part_trans_dt <= '$end_dt'
           |        group by
           |            cbi.iss_ins_cn_nm,
           |            trans_dt)b
           |on
           |a.iss_ins_cn_nm = b.iss_ins_cn_nm and a.trans_dt = b.trans_dt
           |where a.iss_ins_cn_nm is not null
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
               |            substr(brand.brand_nm,1,42) as brand_nm,
               |            trans.trans_dt,
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
               |        and bill.bill_sub_tp in ('01',
               |                                 '03')
               |        and trans.part_trans_dt ='$today_dt'
               |        group by
               |            substr(brand.brand_nm,1,42),
               |            trans_dt) a
               |left join
               |    (
               |        select
               |            substr(brand.brand_nm,1,42) as brand_nm,
               |            trans.trans_dt,
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
               |        and trans.um_trans_id in ('AC02000065',
               |                                  'AC02000063')
               |        and bill.bill_sub_tp in ('01',
               |                                 '03')
               |        and trans.part_trans_dt = '$today_dt'
               |        group by
               |            substr(brand.brand_nm,1,42),
               |            trans_dt)b
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
    * NOTICE: 数据不全，在where中过滤 AND STAT.ACCESS_INS_NM IS NOT NULL
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
           |    trim(stat.access_ins_nm)  as ins_nm,
           |    swt.trans_dt        as report_dt,
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
           |and stat.access_ins_nm is not null
           |group by
           |    stat.access_ins_nm,
           |    swt.trans_dt
           |
           |
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
    * NOTICE: 过滤了 ACPT_INS.CUP_BRANCH_INS_ID_NM IS NOT NULL
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
           |    trim(acpt_ins.cup_branch_ins_id_nm)   as cup_branch_ins_id_nm,
           |    swt.trans_dt                    as report_dt,
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
           |and acpt_ins.cup_branch_ins_id_nm is not null
           |group by
           |    acpt_ins.cup_branch_ins_id_nm,
           |    swt.trans_dt
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
    * NOTICE: WHERE条件中进行了手动空值过滤
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
           |    trim(pri_acct.phone_location)  as mobile_loc,
           |    swt.trans_dt             as report_dt,
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
           |and pri_acct.phone_location is not null
           |group by
           |    pri_acct.phone_location,
           |    swt.trans_dt
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
    * Notice:where中过滤条件待有数据时候去除
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
           |    trim(cb.iss_ins_cn_nm)      as   iss_ins_nm,
           |    swt.trans_dt           as   report_dt,
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
           |and cb.iss_ins_cn_nm is not null
           |group by
           |    cb.iss_ins_cn_nm,
           |    swt.trans_dt
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
           |SELECT
           |    A.CHARA_ACCT_NM as BUSS_DIST_NM,
           |    A.TRANS_DT as REPORT_DT,
           |    A.TRANSCNT as TRANS_CNT,
           |    B.SUCTRANSCNT as SUC_TRANS_CNT,
           |    B.TRANSAT as TRANS_AT,
           |    B.PNTAT as POINT_AT,
           |    B.TRANSUSRCNT as TRANS_USR_CNT,
           |    B.TRANSCARDCNT as TRANS_CARD_CNT
           |FROM
           |    (
           |        SELECT
           |            BD.CHARA_ACCT_NM,
           |            TRANS.TRANS_DT,
           |            COUNT(1) AS TRANSCNT
           |        FROM
           |            HIVE_ACC_TRANS TRANS
           |        LEFT JOIN
           |            HIVE_BUSS_DIST BD
           |        ON
           |            (
           |                TRANS.CHARA_ACCT_TP=BD.CHARA_ACCT_TP )
           |        WHERE
           |            TRANS.UM_TRANS_ID = 'AC02000065'
           |        AND TRANS.BUSS_TP = '03'
           |        AND TRANS.PART_TRANS_DT >= '$start_dt'
           |        AND TRANS.PART_TRANS_DT <= '$end_dt'
           |        GROUP BY
           |            BD.CHARA_ACCT_NM,
           |            TRANS_DT) A
           |LEFT JOIN
           |    (
           |        SELECT
           |            BD.CHARA_ACCT_NM,
           |            TRANS.TRANS_DT,
           |            COUNT(1) AS SUCTRANSCNT,
           |            SUM(TRANS.TRANS_AT) AS TRANSAT,
           |            SUM(POINT_AT) AS PNTAT,
           |            COUNT(DISTINCT TRANS.CDHD_USR_ID) AS TRANSUSRCNT,
           |            COUNT(DISTINCT TRANS.PRI_ACCT_NO) AS TRANSCARDCNT
           |        FROM
           |            HIVE_ACC_TRANS TRANS
           |        LEFT JOIN
           |            HIVE_BUSS_DIST BD
           |        ON
           |            (
           |                TRANS.CHARA_ACCT_TP=BD.CHARA_ACCT_TP )
           |        WHERE
           |            TRANS.UM_TRANS_ID = 'AC02000065'
           |        AND TRANS.BUSS_TP = '03'
           |        AND TRANS.SYS_DET_CD = 'S'
           |        AND TRANS.PART_TRANS_DT >= '$start_dt'
           |        AND TRANS.PART_TRANS_DT <= '$end_dt'
           |        GROUP BY
           |            BD.CHARA_ACCT_NM,
           |            TRANS_DT)B
           |ON
           |    (
           |        A.CHARA_ACCT_NM = B.CHARA_ACCT_NM
           |    AND A.TRANS_DT = B.TRANS_DT )
           |WHERE A.CHARA_ACCT_NM IS NOT NULL
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
           |SELECT
           |    A.CUP_BRANCH_INS_ID_NM as CUP_BRANCH_INS_ID_NM,
           |    A.TRANS_DT as REPORT_DT,
           |    A.TRANSCNT as TRANS_CNT,
           |    B.SUCTRANSCNT as SUC_TRANS_CNT,
           |    B.TRANSAT as TRANS_AT,
           |    B.PNTAT as POINT_AT,
           |    B.TRANSUSRCNT AS TRANS_USR_CNT,
           |    B.TRANSCARDCNT AS TRANS_CARD_CNT
           |
           |FROM
           |    (
           |        SELECT
           |            ACPT_INS.CUP_BRANCH_INS_ID_NM,
           |            TRANS.TRANS_DT,
           |            COUNT(1) AS TRANSCNT
           |        FROM
           |            HIVE_ACC_TRANS TRANS
           |        LEFT JOIN
           |            HIVE_MCHNT_INF_WALLET MCHNT
           |        ON
           |            (
           |                TRANS.MCHNT_CD=MCHNT.MCHNT_CD)
           |        LEFT JOIN
           |            HIVE_BRANCH_ACPT_INS_INF ACPT_INS
           |        ON
           |            (
           |                ACPT_INS.INS_ID_CD=CONCAT('000',MCHNT.ACPT_INS_ID_CD))
           |        LEFT JOIN
           |            HIVE_BUSS_DIST BD
           |        ON
           |            (
           |                TRANS.CHARA_ACCT_TP=BD.CHARA_ACCT_TP )
           |        WHERE
           |            TRANS.UM_TRANS_ID = 'AC02000065'
           |        AND TRANS.BUSS_TP = '03'
           |        AND TRANS.PART_TRANS_DT >= '$start_dt'
           |        AND TRANS.PART_TRANS_DT <= '$end_dt'
           |        GROUP BY
           |            ACPT_INS.CUP_BRANCH_INS_ID_NM,
           |            TRANS_DT) A
           |LEFT JOIN
           |    (
           |        SELECT
           |            ACPT_INS.CUP_BRANCH_INS_ID_NM,
           |            TRANS.TRANS_DT,
           |            COUNT(1) AS SUCTRANSCNT,
           |            SUM(TRANS.TRANS_AT) AS TRANSAT,
           |            SUM(POINT_AT) AS PNTAT,
           |            COUNT(DISTINCT TRANS.CDHD_USR_ID) AS TRANSUSRCNT,
           |            COUNT(DISTINCT TRANS.PRI_ACCT_NO) AS TRANSCARDCNT
           |        FROM
           |            HIVE_ACC_TRANS TRANS
           |        LEFT JOIN
           |            HIVE_MCHNT_INF_WALLET MCHNT
           |        ON
           |            (
           |                TRANS.MCHNT_CD=MCHNT.MCHNT_CD)
           |        LEFT JOIN
           |            HIVE_BRANCH_ACPT_INS_INF ACPT_INS
           |        ON
           |            (
           |                ACPT_INS.INS_ID_CD=CONCAT('000',MCHNT.ACPT_INS_ID_CD))
           |        LEFT JOIN
           |            HIVE_BUSS_DIST BD
           |        ON
           |            (
           |                TRANS.CHARA_ACCT_TP=BD.CHARA_ACCT_TP )
           |        WHERE
           |            TRANS.SYS_DET_CD = 'S'
           |        AND TRANS.UM_TRANS_ID = 'AC02000065'
           |        AND TRANS.BUSS_TP = '03'
           |        AND TRANS.TRANS_DT >= '$start_dt'
           |        AND TRANS.TRANS_DT <= '$end_dt'
           |        GROUP BY
           |            ACPT_INS.CUP_BRANCH_INS_ID_NM,
           |            TRANS_DT)B
           |ON
           |    (
           |        A.CUP_BRANCH_INS_ID_NM = B.CUP_BRANCH_INS_ID_NM
           |    AND A.TRANS_DT = B.TRANS_DT )
           |	WHERE A.CUP_BRANCH_INS_ID_NM IS NOT NULL
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
           |SELECT
           |    A.PHONE_LOCATION as MOBILE_LOC,
           |    A.TRANS_DT as REPORT_DT,
           |    A.TRANSCNT as TRANS_CNT,
           |    B.SUCTRANSCNT as SUC_TRANS_CNT,
           |    B.TRANSAT as TRANS_AT,
           |    B.PNTAT as POINT_AT,
           |    B.TRANSUSRCNT as TRANS_USR_CNT,
           |    B.TRANSCARDCNT as TRANS_CARD_CNT
           |FROM
           |    (
           |        SELECT
           |            PRI_ACCT.PHONE_LOCATION,
           |            TRANS.TRANS_DT,
           |            COUNT(1) AS TRANSCNT
           |        FROM
           |            HIVE_ACC_TRANS TRANS
           |        LEFT JOIN
           |            HIVE_PRI_ACCT_INF PRI_ACCT
           |        ON
           |            (
           |                TRANS.CDHD_USR_ID = PRI_ACCT.CDHD_USR_ID)
           |        LEFT JOIN
           |            HIVE_BUSS_DIST BD
           |        ON
           |            (
           |                TRANS.CHARA_ACCT_TP=BD.CHARA_ACCT_TP )
           |        WHERE
           |            TRANS.UM_TRANS_ID = 'AC02000065'
           |        AND TRANS.BUSS_TP = '03'
           |        AND TRANS.PART_TRANS_DT >= '$start_dt'
           |        AND TRANS.PART_TRANS_DT <= '$end_dt'
           |        GROUP BY
           |            PRI_ACCT.PHONE_LOCATION,
           |            TRANS_DT) A
           |LEFT JOIN
           |    (
           |        SELECT
           |            PRI_ACCT.PHONE_LOCATION,
           |            TRANS.TRANS_DT,
           |            COUNT(1) AS SUCTRANSCNT,
           |            SUM(TRANS.TRANS_AT) AS TRANSAT,
           |            SUM(POINT_AT) AS PNTAT,
           |            COUNT(DISTINCT TRANS.CDHD_USR_ID) AS TRANSUSRCNT,
           |            COUNT(DISTINCT TRANS.PRI_ACCT_NO) AS TRANSCARDCNT
           |        FROM
           |            HIVE_ACC_TRANS TRANS
           |        LEFT JOIN
           |            HIVE_PRI_ACCT_INF PRI_ACCT
           |        ON
           |            (
           |                TRANS.CDHD_USR_ID = PRI_ACCT.CDHD_USR_ID)
           |        LEFT JOIN
           |            HIVE_BUSS_DIST BD
           |        ON
           |            (
           |                TRANS.CHARA_ACCT_TP=BD.CHARA_ACCT_TP )
           |        WHERE
           |            TRANS.SYS_DET_CD = 'S'
           |        AND TRANS.UM_TRANS_ID = 'AC02000065'
           |        AND TRANS.BUSS_TP = '03'
           |        AND TRANS.PART_TRANS_DT >= '$start_dt'
           |        AND TRANS.PART_TRANS_DT <= '$end_dt'
           |        GROUP BY
           |            PRI_ACCT.PHONE_LOCATION,
           |            TRANS_DT)B
           |ON
           |    (
           |        A.PHONE_LOCATION = B.PHONE_LOCATION
           |    AND A.TRANS_DT = B.TRANS_DT )
           |	WHERE A.PHONE_LOCATION IS NOT NULL
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
           |SELECT
           |    A.FIRST_PARA_NM as FIRST_IND_NM,
           |    A.SECOND_PARA_NM as SECOND_IND_NM,
           |    A.TRANS_DT as REPORT_DT,
           |    A.TRANSCNT as TRANS_CNT,
           |    B.SUCTRANSCNT as SUC_TRANS_CNT,
           |    B.TRANSAT as TRANS_AT,
           |    B.PNTAT as DISCOUNT_AT,
           |    B.TRANSUSRCNT as TRANS_USR_CNT,
           |    B.TRANSCARDCNT as TRANS_CARD_CNT
           |FROM
           |    (
           |        SELECT
           |            MP.MCHNT_PARA_CN_NM  AS FIRST_PARA_NM,
           |            MP1.MCHNT_PARA_CN_NM AS SECOND_PARA_NM,
           |            TRANS.TRANS_DT,
           |            COUNT(1) AS TRANSCNT
           |        FROM
           |            HIVE_ACC_TRANS TRANS
           |        INNER JOIN
           |            HIVE_STORE_TERM_RELATION STR
           |        ON
           |            (
           |                TRANS.CARD_ACCPTR_CD = STR.MCHNT_CD
           |            AND TRANS.CARD_ACCPTR_TERM_ID =STR.TERM_ID)
           |        LEFT JOIN
           |            HIVE_PREFERENTIAL_MCHNT_INF PMI
           |        ON
           |            (
           |                STR.THIRD_PARTY_INS_ID = PMI.MCHNT_CD)
           |        LEFT JOIN
           |            HIVE_MCHNT_PARA MP
           |        ON
           |            (
           |                PMI.MCHNT_FIRST_PARA = MP.MCHNT_PARA_ID)
           |        LEFT JOIN
           |            HIVE_MCHNT_PARA MP1
           |        ON
           |            (
           |                PMI.MCHNT_SECOND_PARA = MP1.MCHNT_PARA_ID)
           |        LEFT JOIN
           |            HIVE_BUSS_DIST BD
           |        ON
           |            (
           |                TRANS.CHARA_ACCT_TP=BD.CHARA_ACCT_TP )
           |        WHERE
           |            TRANS.UM_TRANS_ID = 'AC02000065'
           |        AND TRANS.BUSS_TP = '03'
           |        AND STR.REC_ID IS NOT NULL
           |        AND TRANS.PART_TRANS_DT >= '$start_dt'
           |        AND TRANS.PART_TRANS_DT <= '$end_dt'
           |        GROUP BY
           |            MP.MCHNT_PARA_CN_NM,
           |            MP1.MCHNT_PARA_CN_NM,
           |            TRANS_DT) A
           |LEFT JOIN
           |    (
           |        SELECT
           |            MP.MCHNT_PARA_CN_NM  AS FIRST_PARA_NM,
           |            MP1.MCHNT_PARA_CN_NM AS SECOND_PARA_NM,
           |            TRANS.TRANS_DT,
           |            COUNT(1)                          AS SUCTRANSCNT,
           |            SUM(TRANS.TRANS_AT)               AS TRANSAT,
           |            SUM(POINT_AT)                     AS PNTAT,
           |            COUNT(DISTINCT TRANS.CDHD_USR_ID) AS TRANSUSRCNT,
           |            COUNT(DISTINCT TRANS.PRI_ACCT_NO) AS TRANSCARDCNT
           |        FROM
           |            HIVE_ACC_TRANS TRANS
           |        INNER JOIN
           |            HIVE_STORE_TERM_RELATION STR
           |        ON
           |            (
           |                TRANS.CARD_ACCPTR_CD = STR.MCHNT_CD
           |            AND TRANS.CARD_ACCPTR_TERM_ID =STR.TERM_ID)
           |        LEFT JOIN
           |            HIVE_PREFERENTIAL_MCHNT_INF PMI
           |        ON
           |            (
           |                STR.THIRD_PARTY_INS_ID = PMI.MCHNT_CD)
           |        LEFT JOIN
           |            HIVE_MCHNT_PARA MP
           |        ON
           |            (
           |                PMI.MCHNT_FIRST_PARA = MP.MCHNT_PARA_ID)
           |        LEFT JOIN
           |            HIVE_MCHNT_PARA MP1
           |        ON
           |            (
           |                PMI.MCHNT_SECOND_PARA = MP1.MCHNT_PARA_ID)
           |        LEFT JOIN
           |            HIVE_BUSS_DIST BD
           |        ON
           |            (
           |                TRANS.CHARA_ACCT_TP=BD.CHARA_ACCT_TP )
           |        WHERE
           |            TRANS.SYS_DET_CD = 'S'
           |        AND TRANS.UM_TRANS_ID = 'AC02000065'
           |        AND TRANS.BUSS_TP = '03'
           |        AND STR.REC_ID IS NOT NULL
           |        AND TRANS.PART_TRANS_DT >= '$start_dt'
           |        AND TRANS.PART_TRANS_DT <= '$end_dt'
           |        GROUP BY
           |            MP.MCHNT_PARA_CN_NM,
           |            MP1.MCHNT_PARA_CN_NM,
           |            TRANS_DT)B
           |ON
           |    (
           |        A.FIRST_PARA_NM = B.FIRST_PARA_NM
           |    AND A.SECOND_PARA_NM = B.SECOND_PARA_NM
           |    AND A.TRANS_DT = B.TRANS_DT )
           |UNION ALL
           |SELECT
           |    A.FIRST_PARA_NM,
           |    A.SECOND_PARA_NM,
           |    A.TRANS_DT,
           |    A.TRANSCNT,
           |    B.SUCTRANSCNT,
           |    B.TRANSAT,
           |    B.PNTAT,
           |    B.TRANSUSRCNT,
           |    B.TRANSCARDCNT
           |FROM
           |    (
           |        SELECT
           |            MP.MCHNT_PARA_CN_NM  AS FIRST_PARA_NM,
           |            MP1.MCHNT_PARA_CN_NM AS SECOND_PARA_NM,
           |            TRANS.TRANS_DT,
           |            COUNT(1) AS TRANSCNT
           |        FROM
           |            HIVE_ACC_TRANS TRANS
           |        LEFT JOIN
           |            HIVE_STORE_TERM_RELATION STR
           |        ON
           |            (
           |                TRANS.CARD_ACCPTR_CD = STR.MCHNT_CD
           |            AND TRANS.CARD_ACCPTR_TERM_ID = STR.TERM_ID)
           |        LEFT JOIN
           |            (
           |                SELECT
           |                    MCHNT_CD,
           |                    TERM_ID,
           |                    MAX(THIRD_PARTY_INS_ID) OVER (PARTITION BY MCHNT_CD)
           |                FROM
           |                    HIVE_STORE_TERM_RELATION) STR1
           |        ON
           |            (
           |                TRANS.CARD_ACCPTR_CD = STR1.MCHNT_CD
           |            AND TRANS.CARD_ACCPTR_TERM_ID = STR1.TERM_ID)
           |        LEFT JOIN
           |            HIVE_PREFERENTIAL_MCHNT_INF PMI
           |        ON
           |            (
           |                STR.THIRD_PARTY_INS_ID = PMI.MCHNT_CD)
           |        LEFT JOIN
           |            HIVE_MCHNT_PARA MP
           |        ON
           |            (
           |                PMI.MCHNT_FIRST_PARA = MP.MCHNT_PARA_ID)
           |        LEFT JOIN
           |            HIVE_MCHNT_PARA MP1
           |        ON
           |            (
           |                PMI.MCHNT_SECOND_PARA = MP1.MCHNT_PARA_ID)
           |        LEFT JOIN
           |            HIVE_BUSS_DIST BD
           |        ON
           |            (
           |                TRANS.CHARA_ACCT_TP=BD.CHARA_ACCT_TP )
           |        WHERE
           |            TRANS.UM_TRANS_ID = 'AC02000065'
           |        AND TRANS.BUSS_TP = '03'
           |        AND STR.REC_ID IS NULL
           |        AND TRANS.PART_TRANS_DT >= '$start_dt'
           |        AND TRANS.PART_TRANS_DT <= '$end_dt'
           |        GROUP BY
           |            MP.MCHNT_PARA_CN_NM,
           |            MP1.MCHNT_PARA_CN_NM,
           |            TRANS_DT) A
           |LEFT JOIN
           |    (
           |        SELECT
           |            MP.MCHNT_PARA_CN_NM  AS FIRST_PARA_NM,
           |            MP1.MCHNT_PARA_CN_NM AS SECOND_PARA_NM,
           |            TRANS.TRANS_DT,
           |            COUNT(1)                          AS SUCTRANSCNT,
           |            SUM(TRANS.TRANS_AT)               AS TRANSAT,
           |            SUM(POINT_AT)                     AS PNTAT,
           |            COUNT(DISTINCT TRANS.CDHD_USR_ID) AS TRANSUSRCNT,
           |            COUNT(DISTINCT TRANS.PRI_ACCT_NO) AS TRANSCARDCNT
           |        FROM
           |            HIVE_ACC_TRANS TRANS
           |        LEFT JOIN
           |            HIVE_STORE_TERM_RELATION STR
           |        ON
           |            (
           |                TRANS.CARD_ACCPTR_CD = STR.MCHNT_CD
           |            AND TRANS.CARD_ACCPTR_TERM_ID =STR.TERM_ID)
           |        LEFT JOIN
           |            (
           |                SELECT
           |                    MCHNT_CD,
           |                    TERM_ID,
           |                    MAX(THIRD_PARTY_INS_ID) OVER (PARTITION BY MCHNT_CD)
           |                FROM
           |                    HIVE_STORE_TERM_RELATION) STR1
           |        ON
           |            (
           |                TRANS.CARD_ACCPTR_CD = STR1.MCHNT_CD
           |            AND TRANS.CARD_ACCPTR_TERM_ID = STR1.TERM_ID)
           |        LEFT JOIN
           |            HIVE_PREFERENTIAL_MCHNT_INF PMI
           |        ON
           |            (
           |                STR.THIRD_PARTY_INS_ID = PMI.MCHNT_CD)
           |        LEFT JOIN
           |            HIVE_MCHNT_PARA MP
           |        ON
           |            (
           |                PMI.MCHNT_FIRST_PARA = MP.MCHNT_PARA_ID)
           |        LEFT JOIN
           |            HIVE_MCHNT_PARA MP1
           |        ON
           |            (
           |                PMI.MCHNT_SECOND_PARA = MP1.MCHNT_PARA_ID)
           |        LEFT JOIN
           |            HIVE_BUSS_DIST BD
           |        ON
           |            (
           |                TRANS.CHARA_ACCT_TP=BD.CHARA_ACCT_TP )
           |        WHERE
           |            TRANS.SYS_DET_CD = 'S'
           |        AND TRANS.UM_TRANS_ID = 'AC02000065'
           |        AND TRANS.BUSS_TP = '03'
           |        AND STR.REC_ID IS NULL
           |        AND TRANS.PART_TRANS_DT >= '$start_dt'
           |        AND TRANS.PART_TRANS_DT <= '$end_dt'
           |        GROUP BY
           |            MP.MCHNT_PARA_CN_NM,
           |            MP1.MCHNT_PARA_CN_NM,
           |            TRANS_DT)B
           |ON
           |    (
           |        A.FIRST_PARA_NM = B.FIRST_PARA_NM
           |    AND A.SECOND_PARA_NM = B.SECOND_PARA_NM
           |    AND A.TRANS_DT = B.TRANS_DT )
           |	where A.FIRST_PARA_NM is not null and A.SECOND_PARA_NM is not null
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
           |SELECT
           |    A.ISS_INS_CN_NM as ISS_INS_NM,
           |    A.TRANS_DT as REPORT_DT,
           |    A.TRANSCNT as TRANS_CNT,
           |    B.SUCTRANSCNT as SUC_TRANS_CNT,
           |    B.TRANSAT as TRANS_AT,
           |    B.PNTAT as POINT_AT,
           |    B.TRANSUSRCNT as TRANS_USR_CNT,
           |    B.TRANSCARDCNT as TRANS_CARD_CNT
           |FROM
           |    (
           |        SELECT
           |            CBI.ISS_INS_CN_NM,
           |            TRANS.TRANS_DT,
           |            COUNT(1) AS TRANSCNT
           |        FROM
           |            HIVE_ACC_TRANS TRANS
           |        LEFT JOIN
           |            HIVE_BUSS_DIST BD
           |        ON
           |            (
           |                TRANS.CHARA_ACCT_TP=BD.CHARA_ACCT_TP )
           |        LEFT JOIN
           |            HIVE_CARD_BIND_INF CBI
           |        ON
           |            (
           |                TRANS.CARD_NO = CBI.BIND_CARD_NO)
           |        WHERE
           |            TRANS.UM_TRANS_ID = 'AC02000065'
           |        AND TRANS.BUSS_TP = '03'
           |        AND TRANS.PART_TRANS_DT >= '$start_dt'
           |        AND TRANS.PART_TRANS_DT <= '$end_dt'
           |        GROUP BY
           |            CBI.ISS_INS_CN_NM,
           |            TRANS_DT) A
           |LEFT JOIN
           |    (
           |        SELECT
           |            CBI.ISS_INS_CN_NM,
           |            TRANS.TRANS_DT,
           |            COUNT(1) AS SUCTRANSCNT,
           |            SUM(TRANS.TRANS_AT) AS TRANSAT,
           |            SUM(POINT_AT) AS PNTAT,
           |            COUNT(DISTINCT TRANS.CDHD_USR_ID) AS TRANSUSRCNT,
           |            COUNT(DISTINCT TRANS.PRI_ACCT_NO) AS TRANSCARDCNT
           |        FROM
           |            HIVE_ACC_TRANS TRANS
           |        LEFT JOIN
           |            HIVE_BUSS_DIST BD
           |        ON
           |            (
           |                TRANS.CHARA_ACCT_TP=BD.CHARA_ACCT_TP )
           |        LEFT JOIN
           |            HIVE_CARD_BIND_INF CBI
           |        ON
           |            (
           |                TRANS.CARD_NO = CBI.BIND_CARD_NO)
           |        WHERE
           |            TRANS.UM_TRANS_ID = 'AC02000065'
           |        AND TRANS.BUSS_TP = '03'
           |        AND TRANS.SYS_DET_CD = 'S'
           |        AND TRANS.PART_TRANS_DT >= '$start_dt'
           |        AND TRANS.PART_TRANS_DT <= '$end_dt'
           |        GROUP BY
           |            CBI.ISS_INS_CN_NM,
           |            TRANS_DT)B
           |ON
           |    (
           |        A.ISS_INS_CN_NM = B.ISS_INS_CN_NM
           |    AND A.TRANS_DT = B.TRANS_DT )
           |	where A.ISS_INS_CN_NM is not null
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
    * Notice:
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
           |    trim(bd.chara_acct_nm)               as buss_dist_nm,
           |    trans.trans_dt                       as report_dt,
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
           |    trans.chara_acct_tp is not null
           |and trans.part_trans_dt >= '$start_dt'
           |and trans.part_trans_dt <= '$end_dt'
           |and trans.status = '1'
           |group by
           |    bd.chara_acct_nm,
           |    trans.trans_dt
           |
           |
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
    * Notice:
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
           |    trim(a.cup_branch_ins_id_nm)   as cup_branch_ins_id_nm,
           |    a.trans_dt               as report_dt,
           |    a.transcnt               as trans_cnt,
           |    c.suctranscnt            as suc_trans_cnt,
           |    c.bill_original_price    as bill_original_price,
           |    c.bill_price             as bill_price,
           |    a.transusrcnt            as trans_usr_cnt,
           |    b.payusrcnt              as pay_usr_cnt,
           |    c.paysucusrcnt           as pay_suc_usr_cnt
           |from
           |    (
           |        select
           |            bill.cup_branch_ins_id_nm,
           |            trans.trans_dt,
           |            count(1)                    as transcnt,
           |            count(distinct cdhd_usr_id) as transusrcnt
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
           |        where
           |            bill.bill_sub_tp <> '08'
           |        and trans.part_trans_dt >= '$start_dt'
           |        and trans.part_trans_dt <= '$end_dt'
           |        group by
           |            bill.cup_branch_ins_id_nm,
           |            trans.trans_dt) a
           |left join
           |    (
           |        select
           |            bill.cup_branch_ins_id_nm,
           |            trans.trans_dt,
           |            count(distinct cdhd_usr_id) as payusrcnt
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
           |            bill.cup_branch_ins_id_nm,
           |            trans.trans_dt) b
           |on
           |    (
           |        a.cup_branch_ins_id_nm = b.cup_branch_ins_id_nm
           |    and a.trans_dt = b.trans_dt)
           |left join
           |    (
           |        select
           |            bill.cup_branch_ins_id_nm,
           |            trans.trans_dt,
           |            count(1)                      as suctranscnt,
           |            sum(bill.bill_original_price) as bill_original_price,
           |            sum(bill.bill_price)          as bill_price,
           |            count(distinct cdhd_usr_id)   as paysucusrcnt
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
           |        where
           |            bill.bill_sub_tp <> '08'
           |        and trans.order_st = '00'
           |        and trans.part_trans_dt >= '$start_dt'
           |        and trans.part_trans_dt <= '$end_dt'
           |        group by
           |            bill.cup_branch_ins_id_nm,
           |            trans.trans_dt) c
           |on
           |    (
           |        a.cup_branch_ins_id_nm = c.cup_branch_ins_id_nm
           |    and a.trans_dt = c.trans_dt)
           |
           |
           |
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
           |            pri_acct.phone_location,
           |            trans.trans_dt,
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
           |        pri_acct.phone_location is not null
           |        and bill.bill_sub_tp <> '08'
           |        and trans.part_trans_dt >= '$start_dt'
           |        and trans.part_trans_dt <= '$end_dt'
           |        group by
           |            pri_acct.phone_location,
           |            trans.trans_dt) a
           |left join
           |    (
           |        select
           |            pri_acct.phone_location,
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
           |        left join
           |            hive_pri_acct_inf pri_acct
           |        on
           |            (
           |                trans.cdhd_usr_id = pri_acct.cdhd_usr_id)
           |        where
           |        pri_acct.phone_location is not null
           |        and    bill.bill_sub_tp <> '08'
           |        and trans.order_st in ('00',
           |                               '01',
           |                               '02',
           |                               '03',
           |                               '04')
           |        and trans.part_trans_dt >= '$start_dt'
           |        and trans.part_trans_dt <= '$end_dt'
           |        group by
           |            pri_acct.phone_location,
           |            trans.trans_dt) b
           |on
           |    (
           |        a.phone_location = b.phone_location
           |    and a.trans_dt = b.trans_dt)
           |left join
           |    (
           |        select
           |            pri_acct.phone_location,
           |            trans.trans_dt,
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
           |        pri_acct.phone_location is not null
           |        and    bill.bill_sub_tp <> '08'
           |        and trans.order_st = '00'
           |        and trans.part_trans_dt >= '$start_dt'
           |        and trans.part_trans_dt <= '$end_dt'
           |        group by
           |            pri_acct.phone_location,
           |            trans.trans_dt) c
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
           |            cbi.iss_ins_cn_nm,
           |            trans.trans_dt,
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
           |            cbi.iss_ins_cn_nm,
           |            trans.trans_dt) a
           |left join
           |    (
           |        select
           |            cbi.iss_ins_cn_nm,
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
           |            cbi.iss_ins_cn_nm,
           |            trans.trans_dt) b
           |on
           |    (
           |        a.iss_ins_cn_nm = b.iss_ins_cn_nm
           |    and a.trans_dt = b.trans_dt)
           |left join
           |    (
           |        select
           |            cbi.iss_ins_cn_nm,
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
           |            cbi.iss_ins_cn_nm,
           |            trans.trans_dt) c
           |
           |on
           |    (
           |        a.iss_ins_cn_nm = c.iss_ins_cn_nm
           |    and a.trans_dt = c.trans_dt)
           |
           |where  a.iss_ins_cn_nm is not null
           |
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
           |        SELECT
           |            MP.MCHNT_PARA_CN_NM  AS FIRST_PARA_NM,
           |            MP1.MCHNT_PARA_CN_NM AS SECOND_PARA_NM,
           |            TRANS.TRANS_DT,
           |            COUNT(1) AS TRANSCNT,
           |            COUNT(DISTINCT TRANS.CDHD_USR_ID) AS TRANSUSRCNT
           |        FROM
           |            HIVE_BILL_ORDER_TRANS TRANS
           |        LEFT JOIN
           |            HIVE_BILL_SUB_ORDER_TRANS SUB_TRANS
           |        ON
           |            (
           |                TRANS.BILL_ORDER_ID = SUB_TRANS.BILL_ORDER_ID)
           |        LEFT JOIN
           |            HIVE_TICKET_BILL_BAS_INF BILL
           |        ON
           |            (
           |                SUB_TRANS.BILL_ID=BILL.BILL_ID)
           |        INNER JOIN
           |            (
           |                SELECT
           |                    MCHNT_CD,
           |                    MAX(THIRD_PARTY_INS_ID) OVER (PARTITION BY MCHNT_CD) AS THIRD_PARTY_INS_ID
           |                FROM
           |                    HIVE_STORE_TERM_RELATION) STR
           |        ON
           |            (
           |                TRANS.MCHNT_CD = STR.MCHNT_CD)
           |        LEFT JOIN
           |            HIVE_PREFERENTIAL_MCHNT_INF PMI
           |        ON
           |            (
           |                STR.THIRD_PARTY_INS_ID = PMI.MCHNT_CD)
           |        LEFT JOIN
           |            HIVE_MCHNT_PARA MP
           |        ON
           |            (
           |                PMI.MCHNT_FIRST_PARA = MP.MCHNT_PARA_ID)
           |        LEFT JOIN
           |            HIVE_MCHNT_PARA MP1
           |        ON
           |            (
           |                PMI.MCHNT_SECOND_PARA = MP1.MCHNT_PARA_ID)
           |        WHERE
           |            BILL.BILL_SUB_TP <> '08'
           |        AND TRANS.PART_TRANS_DT >= '$start_dt'
           |        AND TRANS.PART_TRANS_DT <= '$end_dt'
           |        GROUP BY
           |            MP.MCHNT_PARA_CN_NM,
           |            MP1.MCHNT_PARA_CN_NM,
           |            TRANS.TRANS_DT
           """.stripMargin)
      JOB_DM_53_A.registerTempTable("Spark_JOB_DM_53_A")
      println("Spark_JOB_DM_53_A 临时表创建成功")

      val JOB_DM_53_B =sqlContext.sql(
        s"""
           |SELECT
           |            MP.MCHNT_PARA_CN_NM  AS FIRST_PARA_NM,
           |            MP1.MCHNT_PARA_CN_NM AS SECOND_PARA_NM,
           |            TRANS.TRANS_DT,
           |            COUNT(DISTINCT TRANS.CDHD_USR_ID) AS PAYUSRCNT
           |        FROM
           |            HIVE_BILL_ORDER_TRANS TRANS
           |        LEFT JOIN
           |            HIVE_BILL_SUB_ORDER_TRANS SUB_TRANS
           |        ON
           |            (
           |                TRANS.BILL_ORDER_ID = SUB_TRANS.BILL_ORDER_ID)
           |        LEFT JOIN
           |            HIVE_TICKET_BILL_BAS_INF BILL
           |        ON
           |            (
           |                SUB_TRANS.BILL_ID=BILL.BILL_ID)
           |        INNER JOIN
           |            (
           |                SELECT
           |                    MCHNT_CD,
           |                    MAX(THIRD_PARTY_INS_ID) OVER (PARTITION BY MCHNT_CD) AS THIRD_PARTY_INS_ID
           |                FROM
           |                    HIVE_STORE_TERM_RELATION) STR
           |        ON
           |            (
           |                TRANS.MCHNT_CD = STR.MCHNT_CD)
           |        LEFT JOIN
           |            HIVE_PREFERENTIAL_MCHNT_INF PMI
           |        ON
           |            (
           |                STR.THIRD_PARTY_INS_ID = PMI.MCHNT_CD)
           |        LEFT JOIN
           |            HIVE_MCHNT_PARA MP
           |        ON
           |            (
           |                PMI.MCHNT_FIRST_PARA = MP.MCHNT_PARA_ID)
           |        LEFT JOIN
           |            HIVE_MCHNT_PARA MP1
           |        ON
           |            (
           |                PMI.MCHNT_SECOND_PARA = MP1.MCHNT_PARA_ID)
           |        WHERE
           |            BILL.BILL_SUB_TP <> '08'
           |        AND TRANS.ORDER_ST IN ('00',
           |                               '01',
           |                               '02',
           |                               '03',
           |                               '04')
           |        AND TRANS.PART_TRANS_DT >= '$start_dt'
           |        AND TRANS.PART_TRANS_DT <= '$end_dt'
           |        GROUP BY
           |            MP.MCHNT_PARA_CN_NM,
           |            MP1.MCHNT_PARA_CN_NM,
           |            TRANS.TRANS_DT
           """.stripMargin)
      JOB_DM_53_B.registerTempTable("Spark_JOB_DM_53_B")

      val JOB_DM_53_C =sqlContext.sql(
        s"""
           |SELECT
           |            MP.MCHNT_PARA_CN_NM  AS FIRST_PARA_NM,
           |            MP1.MCHNT_PARA_CN_NM AS SECOND_PARA_NM,
           |            TRANS.TRANS_DT,
           |            COUNT(1)                          AS SUCTRANSCNT,
           |            SUM(BILL.BILL_ORIGINAL_PRICE)     AS BILL_ORIGINAL_PRICE,
           |            SUM(BILL.BILL_PRICE)              AS BILL_PRICE,
           |            COUNT(DISTINCT TRANS.CDHD_USR_ID) AS PAYSUCUSRCNT
           |        FROM
           |            HIVE_BILL_ORDER_TRANS TRANS
           |        LEFT JOIN
           |            HIVE_BILL_SUB_ORDER_TRANS SUB_TRANS
           |        ON
           |            (
           |                TRANS.BILL_ORDER_ID = SUB_TRANS.BILL_ORDER_ID)
           |        LEFT JOIN
           |            HIVE_TICKET_BILL_BAS_INF BILL
           |        ON
           |            (
           |                SUB_TRANS.BILL_ID=BILL.BILL_ID)
           |        INNER JOIN
           |            (
           |                SELECT
           |                    MCHNT_CD,
           |                    MAX(THIRD_PARTY_INS_ID) OVER (PARTITION BY MCHNT_CD) AS THIRD_PARTY_INS_ID
           |                FROM
           |                    HIVE_STORE_TERM_RELATION) STR
           |        ON
           |            (
           |                TRANS.MCHNT_CD = STR.MCHNT_CD)
           |        LEFT JOIN
           |            HIVE_PREFERENTIAL_MCHNT_INF PMI
           |        ON
           |            (
           |                STR.THIRD_PARTY_INS_ID = PMI.MCHNT_CD)
           |        LEFT JOIN
           |            HIVE_MCHNT_PARA MP
           |        ON
           |            (
           |                PMI.MCHNT_FIRST_PARA = MP.MCHNT_PARA_ID)
           |        LEFT JOIN
           |            HIVE_MCHNT_PARA MP1
           |        ON
           |            (
           |                PMI.MCHNT_SECOND_PARA = MP1.MCHNT_PARA_ID)
           |        WHERE
           |            BILL.BILL_SUB_TP <> '08'
           |        AND TRANS.ORDER_ST = '00'
           |        AND TRANS.PART_TRANS_DT >= '$start_dt'
           |        AND TRANS.PART_TRANS_DT <= '$end_dt'
           |        GROUP BY
           |            MP.MCHNT_PARA_CN_NM,
           |            MP1.MCHNT_PARA_CN_NM,
           |            TRANS.TRANS_DT
           """.stripMargin)
      JOB_DM_53_C.registerTempTable("Spark_JOB_DM_53_C")

      val results = sqlContext.sql(
        s"""
           |SELECT
           |    A.FIRST_PARA_NM as FIRST_IND_NM,
           |    A.SECOND_PARA_NM as SECOND_IND_NM,
           |    A.TRANS_DT as REPORT_DT,
           |    A.TRANSCNT as TRANS_CNT,
           |    C.SUCTRANSCNT as SUC_TRANS_CNT,
           |    C.BILL_ORIGINAL_PRICE as BILL_ORIGINAL_PRICE,
           |    C.BILL_PRICE as BILL_PRICE,
           |    A.TRANSUSRCNT as TRANS_USR_CNT,
           |    B.PAYUSRCNT as PAY_USR_CNT,
           |    C.PAYSUCUSRCNT as PAY_SUC_USR_CNT
           |FROM
           |    Spark_JOB_DM_53_A A
           |    LEFT JOIN Spark_JOB_DM_53_B B
           |    ON
           |    (
           |        A.FIRST_PARA_NM = B.FIRST_PARA_NM
           |    AND A.SECOND_PARA_NM = B.SECOND_PARA_NM
           |    AND A.TRANS_DT = B.TRANS_DT )
           |    LEFT JOIN Spark_JOB_DM_53_C C
           |    ON
           |    (
           |        A.FIRST_PARA_NM = C.FIRST_PARA_NM
           |    AND A.SECOND_PARA_NM = C.SECOND_PARA_NM
           |    AND A.TRANS_DT = C.TRANS_DT )
           |	where A.FIRST_PARA_NM is not null and A.SECOND_PARA_NM is not null
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
           |SELECT
           |    A.GRP_NM AS MCHNT_TP_GRP,
           |    A.TP_NM AS MCHNT_TP,
           |    A.TRANS_DT AS REPORT_DT,
           |    A.TRANSCNT AS TRANS_CNT,
           |    C.SUCTRANSCNT AS SUC_TRANS_CNT,
           |    C.BILL_ORIGINAL_PRICE AS BILL_ORIGINAL_PRICE,
           |    C.BILL_PRICE AS BILL_PRICE,
           |    A.TRANSUSRCNT AS TRANS_USR_CNT,
           |    B.PAYUSRCNT AS PAY_USR_CNT,
           |    C.PAYSUCUSRCNT AS PAY_SUC_USR_CNT
           |FROM
           |    (
           |        SELECT
           |            TP_GRP.MCHNT_TP_GRP_DESC_CN AS GRP_NM,
           |            TP.MCHNT_TP_DESC_CN AS TP_NM,
           |            TRANS.TRANS_DT AS TRANS_DT,
           |            COUNT(1)                    AS TRANSCNT,
           |            COUNT(DISTINCT TRANS.CDHD_USR_ID) AS TRANSUSRCNT
           |        FROM
           |            HIVE_BILL_ORDER_TRANS TRANS
           |        LEFT JOIN
           |            HIVE_BILL_SUB_ORDER_TRANS SUB_TRANS
           |        ON TRANS.BILL_ORDER_ID = SUB_TRANS.BILL_ORDER_ID
           |        INNER JOIN
           |             HIVE_MCHNT_INF_WALLET MCHNT
           |        ON
           |            TRANS.MCHNT_CD=MCHNT.MCHNT_CD
           |        LEFT JOIN
           |            HIVE_MCHNT_TP TP
           |        ON
           |            MCHNT.MCHNT_TP=TP.MCHNT_TP
           |        LEFT JOIN
           |            HIVE_MCHNT_TP_GRP TP_GRP
           |        ON
           |            TP.MCHNT_TP_GRP=TP_GRP.MCHNT_TP_GRP
           |        LEFT JOIN
           |            HIVE_TICKET_BILL_BAS_INF BILL
           |        ON  SUB_TRANS.BILL_ID=BILL.BILL_ID
           |        WHERE
           |            BILL.BILL_SUB_TP <> '08'
           |        AND TRANS.TRANS_DT >= '$start_dt'
           |        AND TRANS.TRANS_DT <= '$end_dt'
           |        GROUP BY
           |            TP_GRP.MCHNT_TP_GRP_DESC_CN,TP.MCHNT_TP_DESC_CN,TRANS.TRANS_DT
           |	) A
           |LEFT JOIN
           |    (
           |        SELECT
           |            TP_GRP.MCHNT_TP_GRP_DESC_CN AS GRP_NM,
           |            TP.MCHNT_TP_DESC_CN AS TP_NM,
           |            TRANS.TRANS_DT AS TRANS_DT,
           |            COUNT(DISTINCT TRANS.CDHD_USR_ID) AS PAYUSRCNT
           |        FROM
           |            HIVE_BILL_ORDER_TRANS TRANS
           |        LEFT JOIN
           |            HIVE_BILL_SUB_ORDER_TRANS SUB_TRANS
           |        ON
           |            (
           |                TRANS.BILL_ORDER_ID = SUB_TRANS.BILL_ORDER_ID)
           |        INNER JOIN
           |             HIVE_MCHNT_INF_WALLET MCHNT
           |        ON
           |            TRANS.MCHNT_CD=MCHNT.MCHNT_CD
           |        LEFT JOIN
           |            HIVE_MCHNT_TP TP
           |        ON
           |            MCHNT.MCHNT_TP=TP.MCHNT_TP
           |        LEFT JOIN
           |            HIVE_MCHNT_TP_GRP TP_GRP
           |        ON
           |            TP.MCHNT_TP_GRP=TP_GRP.MCHNT_TP_GRP
           |        LEFT JOIN
           |            HIVE_TICKET_BILL_BAS_INF BILL
           |        ON
           |            (
           |                SUB_TRANS.BILL_ID=BILL.BILL_ID)
           |        WHERE
           |            BILL.BILL_SUB_TP <> '08'
           |        AND TRANS.ORDER_ST IN ('00',
           |                               '01',
           |                               '02',
           |                               '03',
           |                               '04')
           |        AND TRANS.TRANS_DT >='$start_dt'
           |        AND TRANS.TRANS_DT <= '$end_dt'
           |        GROUP BY
           |            TP_GRP.MCHNT_TP_GRP_DESC_CN,
           |            TP.MCHNT_TP_DESC_CN,
           |            TRANS.TRANS_DT
           |	) B
           |ON
           |    (A.GRP_NM = B.GRP_NM
           |    AND A.TP_NM = B.TP_NM
           |    AND A.TRANS_DT = B.TRANS_DT)
           |LEFT JOIN
           |    (
           |        SELECT
           |            TP_GRP.MCHNT_TP_GRP_DESC_CN AS GRP_NM,
           |            TP.MCHNT_TP_DESC_CN AS TP_NM,
           |            TRANS.TRANS_DT AS TRANS_DT,
           |            COUNT(1)                      AS SUCTRANSCNT,
           |            SUM(BILL.BILL_ORIGINAL_PRICE) AS BILL_ORIGINAL_PRICE,
           |            SUM(BILL.BILL_PRICE)          AS BILL_PRICE,
           |            COUNT(DISTINCT TRANS.CDHD_USR_ID)   AS PAYSUCUSRCNT
           |        FROM
           |            HIVE_BILL_ORDER_TRANS TRANS
           |        LEFT JOIN
           |            HIVE_BILL_SUB_ORDER_TRANS SUB_TRANS
           |        ON
           |            (TRANS.BILL_ORDER_ID = SUB_TRANS.BILL_ORDER_ID)
           |        INNER JOIN
           |             HIVE_MCHNT_INF_WALLET MCHNT
           |        ON
           |            TRANS.MCHNT_CD=MCHNT.MCHNT_CD
           |        LEFT JOIN
           |            HIVE_MCHNT_TP TP
           |        ON
           |            MCHNT.MCHNT_TP=TP.MCHNT_TP
           |        LEFT JOIN
           |            HIVE_MCHNT_TP_GRP TP_GRP
           |        ON
           |            TP.MCHNT_TP_GRP=TP_GRP.MCHNT_TP_GRP
           |        LEFT JOIN
           |            HIVE_TICKET_BILL_BAS_INF BILL
           |        ON
           |            (SUB_TRANS.BILL_ID=BILL.BILL_ID)
           |        WHERE
           |            BILL.BILL_SUB_TP <> '08'
           |        AND TRANS.ORDER_ST = '00'
           |        AND TRANS.TRANS_DT >= '$start_dt'
           |        AND TRANS.TRANS_DT <= '$end_dt'
           |        GROUP BY
           |            TP_GRP.MCHNT_TP_GRP_DESC_CN,TP.MCHNT_TP_DESC_CN,TRANS.TRANS_DT
           |	) C
           |ON
           |    (A.GRP_NM = C.GRP_NM
           |    AND A.TP_NM = C.TP_NM
           |    AND A.TRANS_DT = C.TRANS_DT)
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
           |SELECT
           |    ACPT_INS.CUP_BRANCH_INS_ID_NM as CUP_BRANCH_INS_ID_NM,
           |    TRANS.SETTLE_DT as ROPORT_DT,
           |    COUNT(1)                                 AS TRANS_CNT,
           |    SUM(TRANS.TRANS_POS_AT)                  AS TRANS_AT,
           |    SUM(TRANS.TRANS_POS_AT - TRANS.TRANS_AT) AS DISCOUNT_AT
           |FROM
           |    HIVE_PRIZE_DISCOUNT_RESULT TRANS
           |LEFT JOIN
           |    HIVE_MCHNT_INF_WALLET MCHNT
           |ON
           |    (
           |        TRANS.MCHNT_CD=MCHNT.MCHNT_CD)
           |LEFT JOIN
           |    HIVE_BRANCH_ACPT_INS_INF ACPT_INS
           |ON
           |    (
           |        ACPT_INS.INS_ID_CD = CONCAT('000',MCHNT.ACPT_INS_ID_CD))
           |WHERE
           |    TRANS.PROD_IN = '0'
           |AND TRANS.PART_SETTLE_DT >= '$start_dt'
           |AND TRANS.PART_SETTLE_DT <= '$end_dt'
           |AND ACPT_INS.CUP_BRANCH_INS_ID_NM is not null
           |GROUP BY
           |    ACPT_INS.CUP_BRANCH_INS_ID_NM,
           |    TRANS.SETTLE_DT
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
           |SELECT
           |    TP_GRP.MCHNT_TP_GRP_DESC_CN AS MCHNT_TP_GRP ,
           |    TP.MCHNT_TP_DESC_CN         AS MCHNT_TP,
           |    TRANS.SETTLE_DT as 	REPORT_DT,
           |    COUNT(1)                                 AS TRANS_CNT,
           |    SUM(TRANS.TRANS_POS_AT)                  AS TRANS_AT,
           |    SUM(TRANS.TRANS_POS_AT - TRANS.TRANS_AT) AS DISCOUNT_AT
           |FROM
           |    HIVE_PRIZE_DISCOUNT_RESULT TRANS
           |LEFT JOIN
           |    HIVE_MCHNT_INF_WALLET MCHNT
           |ON
           |    (
           |        TRANS.MCHNT_CD=MCHNT.MCHNT_CD)
           |LEFT JOIN
           |    HIVE_MCHNT_TP TP
           |ON
           |    MCHNT.MCHNT_TP=TP.MCHNT_TP
           |LEFT JOIN
           |    HIVE_MCHNT_TP_GRP TP_GRP
           |ON
           |    TP.MCHNT_TP_GRP=TP_GRP.MCHNT_TP_GRP
           |WHERE
           |    TRANS.PROD_IN = '0'
           |AND TRANS.SETTLE_DT >= '$start_dt'
           |AND TRANS.SETTLE_DT <= '$end_dt'
           |AND TP_GRP.MCHNT_TP_GRP_DESC_CN is not null  and TP.MCHNT_TP_DESC_CN is not null
           |GROUP BY
           |    TP_GRP.MCHNT_TP_GRP_DESC_CN,
           |    TP.MCHNT_TP_DESC_CN,
           |    TRANS.SETTLE_DT
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
           |SELECT
           |    CBI.ISS_INS_CN_NM as ISS_INS_CN_NM,
           |    TRANS.SETTLE_DT as REPORT_DT,
           |    COUNT(1)                                 AS TRANS_CNT,
           |    SUM(TRANS.TRANS_POS_AT)                  AS TRANS_AT,
           |    SUM(TRANS.TRANS_POS_AT - TRANS.TRANS_AT) AS DISCOUNT_AT
           |FROM
           |    HIVE_PRIZE_DISCOUNT_RESULT TRANS
           |LEFT JOIN
           |    HIVE_CARD_BIND_INF CBI
           |ON
           |    (
           |        TRANS.PRI_ACCT_NO = CBI.BIND_CARD_NO)
           |WHERE
           |    TRANS.PROD_IN = '0'
           |AND TRANS.PART_SETTLE_DT >= '$start_dt'
           |AND TRANS.PART_SETTLE_DT <= '$end_dt'
           |AND CBI.ISS_INS_CN_NM is not null
           |GROUP BY
           |    CBI.ISS_INS_CN_NM,
           |    TRANS.SETTLE_DT
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
           |SELECT
           |    CASE
           |        WHEN DBI.LOC_ACTIVITY_ID IS NOT NULL
           |        THEN '仅限云闪付'
           |        ELSE '非仅限云闪付'
           |    END AS QUICK_PASS_TP,
           |    TRANS.SETTLE_DT as REPORT_DT,
           |    COUNT(1)                                 AS TRANS_CNT,
           |    SUM(TRANS.TRANS_POS_AT)                  AS TRANS_AT,
           |    SUM(TRANS.TRANS_POS_AT - TRANS.TRANS_AT) AS DISCOUNT_AT
           |FROM
           |    HIVE_PRIZE_DISCOUNT_RESULT TRANS
           |LEFT JOIN
           |    (
           |        SELECT DISTINCT
           |            A.LOC_ACTIVITY_ID
           |        FROM
           |            (
           |                SELECT DISTINCT
           |                    BAS.LOC_ACTIVITY_ID
           |                FROM
           |                    HIVE_DISCOUNT_BAS_INF BAS
           |                INNER JOIN
           |                    HIVE_FILTER_APP_DET APP
           |                ON
           |                    BAS.LOC_ACTIVITY_ID=APP.LOC_ACTIVITY_ID
           |                INNER JOIN
           |                    HIVE_FILTER_RULE_DET RULE
           |                ON
           |                    RULE.RULE_GRP_ID=APP.RULE_GRP_ID
           |                AND RULE.RULE_GRP_CATA=APP.RULE_GRP_CATA
           |                WHERE
           |                    APP.RULE_GRP_CATA='12'
           |                AND RULE_MIN_VAL IN ('06',
           |                                     '07',
           |                                     '08') ) A
           |        LEFT JOIN
           |            (
           |                SELECT DISTINCT
           |                    BAS.LOC_ACTIVITY_ID
           |                FROM
           |                    HIVE_DISCOUNT_BAS_INF BAS
           |                INNER JOIN
           |                    HIVE_FILTER_APP_DET APP
           |                ON
           |                    BAS.LOC_ACTIVITY_ID=APP.LOC_ACTIVITY_ID
           |                INNER JOIN
           |                    HIVE_FILTER_RULE_DET RULE
           |                ON
           |                    RULE.RULE_GRP_ID=APP.RULE_GRP_ID
           |                AND RULE.RULE_GRP_CATA=APP.RULE_GRP_CATA
           |                WHERE
           |                    APP.RULE_GRP_CATA='12'
           |                AND RULE_MIN_VAL IN ('04',
           |                                     '05',
           |                                     '09') ) B
           |        ON
           |            A.LOC_ACTIVITY_ID=B.LOC_ACTIVITY_ID
           |        WHERE
           |            B.LOC_ACTIVITY_ID IS NULL) DBI
           |ON
           |    (
           |        TRANS.AGIO_APP_ID=DBI.LOC_ACTIVITY_ID)
           |WHERE
           |    TRANS.PROD_IN = '0'
           |AND TRANS.PART_SETTLE_DT >= '$start_dt'
           |AND TRANS.PART_SETTLE_DT <= '$end_dt'
           |GROUP BY
           |    CASE
           |        WHEN DBI.LOC_ACTIVITY_ID IS NOT NULL
           |        THEN '仅限云闪付'
           |        ELSE '非仅限云闪付'
           |    END,
           |    TRANS.SETTLE_DT
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
           |SELECT
           |    A.ACPT_INS_ID_CD as ACPT_INS_ID_CD,
           |    A.INS_CN_NM as ACPT_INS_CN_NM,
           |    A.SYS_SETTLE_DT as REPORT_DT,
           |    A.INS_ID_CD as INS_ID_CD,
           |    A.TERM_UPGRADE_CD as TERM_UPGRADE_CD,
           |    A.TRANS_NORM_CD as NORM_VER_CD,
           |    A.TRANSCNT as TRANS_CNT,
           |    B.SUCTRANSCNT as SUC_TRANS_CNT,
           |    B.TRANSAT as TRANS_AT,
           |    B.DISCOUNTAT as DISCOUNT_AT,
           |    B.TRANSUSRCNT as TRANS_USR_CNT,
           |    B.CARDCNT as TRANS_CARD_CNT
           |FROM
           |    (
           |        SELECT
           |            TRANS.ACPT_INS_ID_CD,
           |            INS.INS_CN_NM,
           |            TRANS.SYS_SETTLE_DT,
           |            CASE
           |                WHEN TRANS.FWD_INS_ID_CD IN ('00097310',
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
           |                THEN '直连'
           |                ELSE '间连'
           |            END INS_ID_CD,
           |            CASE
           |                WHEN TRANS.INTERNAL_TRANS_TP='C00022'
           |                OR  TRANS.INTERNAL_TRANS_TP='C20022'
           |                THEN '改造'
           |                ELSE '终端不改造'
           |            END TERM_UPGRADE_CD,
           |            CASE
           |                WHEN TRANS.INTERNAL_TRANS_TP='C00022'
           |                THEN '1.0规范'
           |                WHEN TRANS.INTERNAL_TRANS_TP='C20022'
           |                THEN '2.0规范'
           |                ELSE ''
           |            END         TRANS_NORM_CD,
           |            COUNT(*) AS TRANSCNT
           |        FROM
           |            HIVE_ACC_TRANS TRANS
           |        LEFT JOIN
           |            HIVE_INS_INF INS
           |        ON
           |            TRANS.ACPT_INS_ID_CD=INS.INS_ID_CD
           |        WHERE
           |            TRANS.INTERNAL_TRANS_TP IN ('C00022' ,
           |                                        'C20022',
           |                                        'C00023')
           |        AND TRANS.SYS_SETTLE_DT >= '$start_dt'
           |        AND TRANS.SYS_SETTLE_DT <= '$end_dt'
           |        GROUP BY
           |            TRANS.ACPT_INS_ID_CD ,
           |            INS.INS_CN_NM,
           |            TRANS.SYS_SETTLE_DT,
           |            CASE
           |                WHEN TRANS.FWD_INS_ID_CD IN ('00097310',
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
           |                THEN '直连'
           |                ELSE '间连'
           |            END,
           |            CASE
           |                WHEN TRANS.INTERNAL_TRANS_TP='C00022'
           |                OR  TRANS.INTERNAL_TRANS_TP='C20022'
           |                THEN '改造'
           |                ELSE '终端不改造'
           |            END ,
           |            CASE
           |                WHEN TRANS.INTERNAL_TRANS_TP='C00022'
           |                THEN '1.0规范'
           |                WHEN TRANS.INTERNAL_TRANS_TP='C20022'
           |                THEN '2.0规范'
           |                ELSE ''
           |            END ) A
           |LEFT JOIN
           |    (
           |        SELECT
           |            TRANS.ACPT_INS_ID_CD ,
           |            INS.INS_CN_NM,
           |            TRANS.SYS_SETTLE_DT,
           |            CASE
           |                WHEN TRANS.FWD_INS_ID_CD IN ('00097310',
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
           |                THEN '直连'
           |                ELSE '间连'
           |            END INS_ID_CD,
           |            CASE
           |                WHEN TRANS.INTERNAL_TRANS_TP='C00022'
           |                OR  TRANS.INTERNAL_TRANS_TP='C20022'
           |                THEN '改造'
           |                ELSE '终端不改造'
           |            END TERM_UPGRADE_CD,
           |            CASE
           |                WHEN TRANS.INTERNAL_TRANS_TP='C00022'
           |                THEN '1.0规范'
           |                WHEN TRANS.INTERNAL_TRANS_TP='C20022'
           |                THEN '2.0规范'
           |                ELSE ''
           |            END               TRANS_NORM_CD,
           |            COUNT(*)          AS SUCTRANSCNT,
           |            SUM(TRANS_TOT_AT) AS TRANSAT,
           |            SUM(TRANS.DISCOUNT_AT)          AS DISCOUNTAT,
           |            COUNT(DISTINCT TRANS.CDHD_USR_ID) AS TRANSUSRCNT,
           |            COUNT(DISTINCT PRI_ACCT_NO)       AS CARDCNT
           |        FROM
           |            HIVE_ACC_TRANS TRANS
           |        LEFT JOIN
           |            HIVE_INS_INF INS
           |        ON
           |            TRANS.ACPT_INS_ID_CD=INS.INS_ID_CD
           |        WHERE
           |            TRANS.CHSWT_RESP_CD='00'
           |        AND TRANS.INTERNAL_TRANS_TP IN ('C00022' ,
           |                                        'C20022',
           |                                        'C00023')
           |        AND TRANS.SYS_SETTLE_DT >= '$start_dt'
           |        AND TRANS.SYS_SETTLE_DT <= '$end_dt'
           |        GROUP BY
           |            TRANS.ACPT_INS_ID_CD ,
           |            INS.INS_CN_NM,
           |            TRANS.SYS_SETTLE_DT,
           |            CASE
           |                WHEN TRANS.FWD_INS_ID_CD IN ('00097310',
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
           |                THEN '直连'
           |                ELSE '间连'
           |            END,
           |            CASE
           |                WHEN TRANS.INTERNAL_TRANS_TP='C00022'
           |                OR  TRANS.INTERNAL_TRANS_TP='C20022'
           |                THEN '改造'
           |                ELSE '终端不改造'
           |            END ,
           |            CASE
           |                WHEN TRANS.INTERNAL_TRANS_TP='C00022'
           |                THEN '1.0规范'
           |                WHEN TRANS.INTERNAL_TRANS_TP='C20022'
           |                THEN '2.0规范'
           |                ELSE ''
           |            END ) B
           |ON
           |    A.ACPT_INS_ID_CD=B.ACPT_INS_ID_CD
           |AND A.INS_CN_NM=B.INS_CN_NM
           |AND A.SYS_SETTLE_DT = B.SYS_SETTLE_DT
           |AND A.INS_ID_CD = B.INS_ID_CD
           |AND A.TERM_UPGRADE_CD = B.TERM_UPGRADE_CD
           |AND A.TRANS_NORM_CD = B.TRANS_NORM_CD
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
               |SELECT
               |NVL(A.BUSS_TP_NM,'--') as BUSS_TP_NM,
               |NVL(A.CHNL_TP_NM,'--') as CHNL_TP_NM,
               |A.TRANS_DT as REPORT_DT,
               |SUM(B.TRAN_ALL_CNT) as TRAN_ALL_CNT,
               |SUM(A.TRAN_SUCC_CNT) as TRAN_SUCC_CNT,
               |SUM(A.TRANS_SUCC_AT) as TRANS_SUCC_AT
               |FROM(
               |select
               |BUSS_TP_NM,
               |CHNL_TP_NM,
               |TO_DATE(TRANS_DT) as TRANS_DT,
               |COUNT( TRANS_NO) AS TRAN_SUCC_CNT,
               |SUM(TRANS_AT) AS TRANS_SUCC_AT
               |from HIVE_LIFE_TRANS
               |where PROC_ST ='00'
               |and TO_DATE(TRANS_DT)>='$today_dt' AND   TO_DATE(TRANS_DT)<='$today_dt'
               |GROUP BY BUSS_TP_NM,CHNL_TP_NM,TO_DATE(TRANS_DT)) A
               |LEFT JOIN
               |(
               |select
               |BUSS_TP_NM,
               |CHNL_TP_NM,
               |TO_DATE(TRANS_DT) as TRANS_DT,
               |COUNT(TRANS_NO) AS TRAN_ALL_CNT
               |from HIVE_LIFE_TRANS
               |where  TO_DATE(TRANS_DT)>='$today_dt' AND   TO_DATE(TRANS_DT)<='$today_dt'
               |GROUP BY BUSS_TP_NM,CHNL_TP_NM,TO_DATE(TRANS_DT)) B
               |ON A.BUSS_TP_NM=B.BUSS_TP_NM AND A.BUSS_TP_NM=B.BUSS_TP_NM and A.TRANS_DT=B.TRANS_DT
               |GROUP BY A.BUSS_TP_NM,
               |A.CHNL_TP_NM,
               |A.TRANS_DT
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
               |a.IF_HCE as IF_HCE,
               |'$today_dt' as REPORT_DT,
               |SUM(a.coupon_class) as CLASS_TPRE_ADD_NUM,
               |SUM(a.coupon_publish) as AMT_TPRE_ADD_NUM ,
               |SUM(a.dwn_num) as DOWM_TPRE_ADD_NUM ,
               |SUM(b.batch) as BATCH_TPRE_ADD_NUM
               |from
               |(select
               |CASE WHEN substr(UDF_FLD,31,2) in ('01','02','03','04','05') THEN '仅限云闪付' ELSE '非仅限云闪付' END AS IF_HCE,
               |count(*) as coupon_class ,
               |sum(case when dwn_total_num = -1 then dwn_num else dwn_total_num end) as coupon_publish ,
               |sum(dwn_num) as dwn_num
               |from HIVE_DOWNLOAD_TRANS as dtl,
               |HIVE_TICKET_BILL_BAS_INF as bill
               |where dtl.bill_id=bill.bill_id
               |and dtl.um_trans_id in ('12','17')
               |and dtl.trans_st='1' and part_trans_dt='$today_dt' and dtl.bill_nm not like '%机场%' and dtl.bill_nm not like '%住两晚送一晚%'
               |and bill.bill_sub_tp in ('01','03') and bill.bill_nm not like '%测试%' and bill.bill_nm not like '%验证%' and bill.bill_id <>'Z00000000020415'
               |and bill.bill_id<>'Z00000000020878' and bill.bill_nm not like '%满2元减1%' and bill.bill_nm not like '%满2分减1分%'
               |and bill.bill_nm not like '%满2减1%' and bill.bill_nm not like '%满2抵1%' and bill.bill_nm not like '测%' and dwn_total_num-dwn_num<100000
               |group by CASE WHEN substr(UDF_FLD,31,2) in ('01','02','03','04','05') THEN '仅限云闪付' ELSE '非仅限云闪付' END )a
               |left join
               |(
               |select
               |CASE WHEN substr(UDF_FLD,31,2) in ('01','02','03','04','05') THEN '仅限云闪付' ELSE '非仅限云闪付' END AS IF_HCE,
               |sum(adj.ADJ_TICKET_BILL) as batch
               |from
               |HIVE_TICKET_BILL_ACCT_ADJ_TASK adj
               |inner join
               |(select cup_branch_ins_id_cd,bill.bill_id,UDF_FLD from HIVE_DOWNLOAD_TRANS as dtl,
               |HIVE_TICKET_BILL_BAS_INF as bill
               |where dtl.bill_id=bill.bill_id
               |and dtl.um_trans_id in ('12','17')
               |and dtl.trans_st='1' and part_trans_dt='$today_dt' and dtl.bill_nm not like '%机场%' and dtl.bill_nm not like '%住两晚送一晚%'
               |and bill.bill_sub_tp in ('01','03') and bill.bill_nm not like '%测试%' and bill.bill_nm not like '%验证%' and bill.bill_id <>'Z00000000020415'
               |and bill.bill_id<>'Z00000000020878' and bill.bill_nm not like '%满2元减1%' and bill.bill_nm not like '%满2分减1分%'
               |and bill.bill_nm not like '%满2减1%' and bill.bill_nm not like '%满2抵1%' and bill.bill_nm not like '测%' and dwn_total_num-dwn_num<100000
               |) b
               |on adj.bill_id=b.bill_id
               |group by CASE WHEN substr(UDF_FLD,31,2) in ('01','02','03','04','05') THEN '仅限云闪付' ELSE '非仅限云闪付' END )b
               |on a.IF_HCE=b.IF_HCE
               |group by a.IF_HCE,'$today_dt'
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
      UPSQL_JDBC.delete(s"DM_HCE_COUPON_TRAN","REPORT_DT",start_dt,end_dt)
      println("#### JOB_DM_65 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())

      var today_dt=start_dt
      if(interval>=0 ){
        sqlContext.sql(s"use $hive_dbname")
        for(i <- 0 to interval){
          println(s"#### JOB_DM_65 spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())
          val results = sqlContext.sql(
            s"""
               |select
               |tempe.CUP_BRANCH_INS_ID_NM as BRANCH_NM,
               |'$today_dt' as REPORT_DT,
               |tempe.dwn_total_num as YEAR_RELEASE_NUM,
               |tempe.dwn_num as YEAR_DOWN_NUM,
               |tempc.accept_year_num as YEAR_VOTE_AGAINST_NUM,
               |tempc.accept_today_num as TODAY_VOTE_AGAINST_NUM
               |from
               |(
               |select
               |tempa.CUP_BRANCH_INS_ID_NM,
               |count(*),
               |sum(dwn_total_num) as dwn_total_num,
               |sum(dwn_num)  as  dwn_num
               |from
               |(
               |select
               |bill_id,
               |bill_nm,
               |CUP_BRANCH_INS_ID_NM,
               |(case when dwn_total_num=-1 then dwn_num else dwn_total_num end) as dwn_total_num,dwn_num
               |from HIVE_TICKET_BILL_BAS_INF
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
               |      group by tempa.CUP_BRANCH_INS_ID_NM
               |	  ) tempe
               |left join
               |
               |(
               |select
               |tempb.CUP_BRANCH_INS_ID_NM as CUP_BRANCH_INS_ID_NM,
               |count(case when to_date(tempd.trans_dt)>=trunc('$today_dt','YYYY') and to_date(tempd.trans_dt)<='$today_dt' then tempd.bill_id end) as accept_year_num,
               |count(case when  to_date(tempd.trans_dt) ='$today_dt' then tempd.bill_id end) as accept_today_num
               |from
               |(
               |select
               |bill_id,
               |trans_dt,
               |substr(udf_fld,31,2) as CFP_SIGN
               |from  HIVE_ACC_TRANS
               |where substr(udf_fld,31,2) not in ('',' ','  ','00') and
               |      UM_TRANS_ID in ('AC02000065','AC02000063') and
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
               |CUP_BRANCH_INS_ID_NM
               |from HIVE_TICKET_BILL_BAS_INF
               |)
               |tempb
               |on tempd.bill_id = tempb.bill_id
               |group by
               |tempb.CUP_BRANCH_INS_ID_NM
               |)
               |tempc
               |on tempe.CUP_BRANCH_INS_ID_NM=tempc.CUP_BRANCH_INS_ID_NM
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
    println("###JOB_DM_66(dm_coupon_cfp_tran->hive_acc_trans+hive_ticket_bill_bas_inf)")

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
           |SELECT
           |    ta.CUP_BRANCH_INS_ID_NM as CUP_BRANCH_INS_ID_NM,
           |    ta.SETTLE_DT as REPORT_DT,
           |    MAX(ta.ACTIVITY_NUM) AS ACTIVITY_NUM,
           |    MAX(ta.PLAN_NUM)     AS PLAN_NUM,
           |    MAX(ta.ACTUAL_NUM)   AS ACTUAL_NUM
           |FROM
           |(
           |        SELECT
           |            A.CUP_BRANCH_INS_ID_NM as CUP_BRANCH_INS_ID_NM,
           |            A.SETTLE_DT as SETTLE_DT,
           |            C.ACTIVITY_NUM as ACTIVITY_NUM,
           |            A.PLAN_NUM as PLAN_NUM,
           |            B.ACTUAL_NUM as ACTUAL_NUM
           |        FROM
           |            (
           |                SELECT
           |                    PRIZE.CUP_BRANCH_INS_ID_NM AS CUP_BRANCH_INS_ID_NM,
           |                    RSLT.SETTLE_DT AS SETTLE_DT,
           |                    SUM(LVL.LVL_PRIZE_NUM) AS PLAN_NUM
           |                FROM
           |                    HIVE_PRIZE_ACTIVITY_BAS_INF PRIZE,
           |                    HIVE_PRIZE_LVL LVL,
           |                    (
           |                        SELECT DISTINCT
           |                            SETTLE_DT
           |                        FROM
           |                            HIVE_PRIZE_DISCOUNT_RESULT
           |                        WHERE
           |                            PART_SETTLE_DT >= '$start_dt'
           |                        AND PART_SETTLE_DT <= '$end_dt') RSLT
           |                WHERE
           |                    PRIZE.LOC_ACTIVITY_ID = LVL.LOC_ACTIVITY_ID
           |                AND PRIZE.ACTIVITY_BEGIN_DT<= RSLT.SETTLE_DT
           |                AND PRIZE.ACTIVITY_END_DT>=RSLT.SETTLE_DT
           |                AND PRIZE.RUN_ST!='3'
           |                GROUP BY
           |                    PRIZE.CUP_BRANCH_INS_ID_NM,
           |                    RSLT.SETTLE_DT
           |   ) A,
           |            (
           |                SELECT
           |                    PRIZE.CUP_BRANCH_INS_ID_NM AS CUP_BRANCH_INS_ID_NM,
           |                    PR.SETTLE_DT AS SETTLE_DT,
           |                    COUNT(PR.SYS_TRA_NO_CONV) AS ACTUAL_NUM
           |                FROM
           |                    HIVE_PRIZE_ACTIVITY_BAS_INF PRIZE,
           |                    HIVE_PRIZE_BAS BAS,
           |                    HIVE_PRIZE_DISCOUNT_RESULT PR
           |                WHERE
           |                    PRIZE.LOC_ACTIVITY_ID = BAS.LOC_ACTIVITY_ID
           |                AND BAS.PRIZE_ID = PR.PRIZE_ID
           |                AND PRIZE.ACTIVITY_BEGIN_DT<= PR.SETTLE_DT
           |                AND PRIZE.ACTIVITY_END_DT>= PR.SETTLE_DT
           |                AND PR.trans_id='S22'
           |                AND PR.PART_SETTLE_DT >= '$start_dt'
           |                AND PR.PART_SETTLE_DT <= '$end_dt'
           |                AND PR.TRANS_ID NOT LIKE 'V%'
           |                AND PRIZE.RUN_ST!='3'
           |                GROUP BY
           |                    PRIZE.CUP_BRANCH_INS_ID_NM,PR.SETTLE_DT
           |   ) B,
           |            (
           |                SELECT
           |                    PRIZE.CUP_BRANCH_INS_ID_NM AS CUP_BRANCH_INS_ID_NM,
           |                    RSLT.SETTLE_DT AS SETTLE_DT,
           |                    COUNT(*) AS ACTIVITY_NUM
           |                FROM
           |                    HIVE_PRIZE_ACTIVITY_BAS_INF PRIZE,
           |                    (
           |                        SELECT DISTINCT
           |                            SETTLE_DT
           |                        FROM
           |                            HIVE_PRIZE_DISCOUNT_RESULT
           |                        WHERE
           |                            PART_SETTLE_DT >= '$start_dt'
           |                        AND PART_SETTLE_DT <= '$end_dt') RSLT
           |                WHERE
           |                    PRIZE.ACTIVITY_BEGIN_DT<= RSLT.SETTLE_DT
           |                AND PRIZE.ACTIVITY_END_DT>=RSLT.SETTLE_DT
           |                AND PRIZE.RUN_ST!='3'
           |                GROUP BY
           |                    PRIZE.CUP_BRANCH_INS_ID_NM,
           |                    RSLT.SETTLE_DT
           |   ) C
           |        WHERE
           |            A.CUP_BRANCH_INS_ID_NM=B.CUP_BRANCH_INS_ID_NM
           |        AND B.CUP_BRANCH_INS_ID_NM=C.CUP_BRANCH_INS_ID_NM
           |        AND A.SETTLE_DT = B.SETTLE_DT
           |        AND B.SETTLE_DT = C.SETTLE_DT
           |
           |UNION ALL
           |
           |SELECT
           |DISTINCT D.INS_CN_NM AS CUP_BRANCH_INS_ID_NM,RSLT.SETTLE_DT as SETTLE_DT,0 AS ACTIVITY_NUM,0 AS PLAN_NUM,0 AS ACTUAL_NUM
           |FROM
           |HIVE_INS_INF D,
           |(
           |SELECT DISTINCT
           |SETTLE_DT
           |FROM
           |HIVE_PRIZE_DISCOUNT_RESULT
           |WHERE
           |PART_SETTLE_DT >='$start_dt'AND PART_SETTLE_DT <='$end_dt') RSLT
           |WHERE TRIM(D.INS_CN_NM) LIKE '%中国银联股份有限公司%分公司' OR  TRIM(D.INS_CN_NM) LIKE '%信息中心'
           |)  ta
           |GROUP BY
           |ta.CUP_BRANCH_INS_ID_NM,ta.SETTLE_DT
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
           |SELECT
           |    C.ACTIVITY_ID AS ACTIVITY_ID,
           |    C.ACTIVITY_NM AS ACTIVITY_NM,
           |    C.SETTLE_DT AS REPORT_DT,
           |    C.PLAN_NUM AS PLAN_NUM,
           |    C.ACTUAL_NUM AS ACTUAL_NUM
           |FROM
           |    (
           |        SELECT
           |            A.LOC_ACTIVITY_ID                                                  AS ACTIVITY_ID,
           |            A.LOC_ACTIVITY_NM                                                  AS ACTIVITY_NM,
           |            A.SETTLE_DT                                                        AS SETTLE_DT,
           |            A.PLAN_NUM                                                         AS PLAN_NUM,
           |            B.ACTUAL_NUM                                                       AS ACTUAL_NUM,
           |            ROW_NUMBER() OVER(PARTITION BY A.SETTLE_DT ORDER BY B.ACTUAL_NUM DESC) AS RN
           |        FROM
           |            (
           |                SELECT
           |                    PRIZE.LOC_ACTIVITY_ID,
           |                    TRANSLATE(PRIZE.LOC_ACTIVITY_NM,' ','') AS LOC_ACTIVITY_NM,
           |                    RSLT.SETTLE_DT,
           |                    SUM(LVL.LVL_PRIZE_NUM) AS PLAN_NUM
           |                FROM
           |                    HIVE_PRIZE_ACTIVITY_BAS_INF PRIZE,
           |                    HIVE_PRIZE_LVL LVL,
           |                    (
           |                        SELECT DISTINCT
           |                            SETTLE_DT
           |                        FROM
           |                            HIVE_PRIZE_DISCOUNT_RESULT
           |                        WHERE
           |                            PART_SETTLE_DT >= '$start_dt'
           |                        AND PART_SETTLE_DT <= '$end_dt') RSLT
           |                WHERE
           |                    PRIZE.LOC_ACTIVITY_ID = LVL.LOC_ACTIVITY_ID
           |                AND PRIZE.ACTIVITY_BEGIN_DT<= RSLT.SETTLE_DT
           |                AND PRIZE.ACTIVITY_END_DT>=RSLT.SETTLE_DT
           |                AND PRIZE.RUN_ST!='3'
           |                GROUP BY
           |                    PRIZE.LOC_ACTIVITY_ID,
           |                    TRANSLATE(PRIZE.LOC_ACTIVITY_NM,' ',''),
           |                    RSLT.SETTLE_DT ) A
           |        LEFT JOIN
           |            (
           |                SELECT
           |                    PRIZE.LOC_ACTIVITY_ID,
           |                    PR.SETTLE_DT,
           |                    COUNT(PR.SYS_TRA_NO_CONV) AS ACTUAL_NUM
           |                FROM
           |                    HIVE_PRIZE_ACTIVITY_BAS_INF PRIZE,
           |                    HIVE_PRIZE_BAS BAS,
           |                    HIVE_PRIZE_DISCOUNT_RESULT PR
           |                WHERE
           |                    PRIZE.LOC_ACTIVITY_ID = BAS.LOC_ACTIVITY_ID
           |                AND BAS.PRIZE_ID = PR.PRIZE_ID
           |                AND PRIZE.ACTIVITY_BEGIN_DT<= PR.SETTLE_DT
           |                AND PRIZE.ACTIVITY_END_DT>= PR.SETTLE_DT
           |                AND PR.trans_id='S22'
           |                AND PR.PART_SETTLE_DT >= '$start_dt'
           |                AND PR.PART_SETTLE_DT <= '$end_dt'
           |                AND PR.TRANS_ID NOT LIKE 'V%'
           |                AND PRIZE.RUN_ST!='3'
           |                GROUP BY
           |                    PRIZE.LOC_ACTIVITY_ID,
           |                    SETTLE_DT ) B
           |        ON
           |            (
           |                A.LOC_ACTIVITY_ID = B.LOC_ACTIVITY_ID
           |            AND A.SETTLE_DT = B.SETTLE_DT)
           |        WHERE
           |            B.ACTUAL_NUM IS NOT NULL
           |) C
           |WHERE
           |    C.RN <= 10
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
           |SELECT
           |  B.ACTIVITY_ID as ACTIVITY_ID,
           |  B.ACTIVITY_NM as ACTIVITY_NM,
           |  B.SETTLE_DT as REPORT_DT,
           |  B.TRANS_CNT as TRANS_CNT,
           |  B.TRANS_AT as TRANS_AT,
           |  B.DISCOUNT_AT as DISCOUNT_AT
           | FROM
           | (
           | SELECT
           | A.ACTIVITY_ID                         AS ACTIVITY_ID,
           | A.ACTIVITY_NM                        AS ACTIVITY_NM,
           | A.SETTLE_DT                           AS SETTLE_DT,
           | A.TRANS_CNT                           AS TRANS_CNT,
           | A.TRANS_POS_AT_TTL                    AS TRANS_AT,
           | (A.TRANS_POS_AT_TTL - A.TRANS_AT_TTL) AS DISCOUNT_AT,
           | ROW_NUMBER() OVER(PARTITION BY A.SETTLE_DT ORDER BY A.TRANS_CNT DESC) AS RN
           | FROM
           |  (
           |   SELECT
           |    DBI.LOC_ACTIVITY_ID                 AS ACTIVITY_ID,
           |    TRANSLATE(DBI.LOC_ACTIVITY_NM,' ','') AS ACTIVITY_NM,
           |    TRANS.SETTLE_DT,
           |    SUM (
           |     CASE
           |      WHEN TRANS.TRANS_ID IN ('V52','R22','V50','R20','S30')
           |      THEN -1
           |      ELSE 1
           |     END) AS TRANS_CNT,
           |    SUM (
           |     CASE
           |      WHEN TRANS.TRANS_ID IN ('V52','R22','V50','R20','S30')
           |      THEN -TRANS.TRANS_POS_AT
           |      ELSE TRANS.TRANS_POS_AT
           |     END) AS TRANS_POS_AT_TTL,
           |    SUM (
           |     CASE
           |      WHEN TRANS.TRANS_ID IN ('V52','R22','V50','R20','S30')
           |      THEN -TRANS.TRANS_AT
           |      ELSE TRANS.TRANS_AT
           |     END) AS TRANS_AT_TTL
           |   FROM
           |    HIVE_PRIZE_DISCOUNT_RESULT TRANS,
           |    HIVE_DISCOUNT_BAS_INF DBI
           |   WHERE
           |    TRANS.AGIO_APP_ID=DBI.LOC_ACTIVITY_ID
           |   AND TRANS.AGIO_APP_ID IS NOT NULL
           |   AND TRANS.trans_id='S22'
           |   AND TRANS.PART_SETTLE_DT >='$start_dt'
           |   AND TRANS.PART_SETTLE_DT <='$end_dt'
           |   GROUP BY
           |    DBI.LOC_ACTIVITY_ID,
           |    TRANSLATE(DBI.LOC_ACTIVITY_NM,' ',''),
           |    TRANS.SETTLE_DT
           |  ) A
           | ) B
           | WHERE
           |  B.RN <= 10
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
    DateUtils.timeCost("JOB_DM_9") {
      UPSQL_JDBC.delete(s"dm_iss_disc_cfp_tran","report_dt",start_dt,end_dt)
      println("#### JOB_DM_78 删除重复数据完成的时间为：" + DateUtils.getCurrentSystemTime())
      var today_dt=start_dt
      if(interval>=0 ){
        println(s"#### JOB_DM_78 spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())
        sqlContext.sql(s"use $hive_dbname")
        for(i <- 0 to interval){
          val results = sqlContext.sql(
            s"""
               |SELECT
               |A.bank_nm AS ISS_NM,
               |'$today_dt' AS REPORT_DT,
               |A.card_attr AS CARD_ATTR,
               |A.TOTAL_BIND_CNT AS TOTAL_BIND_CNT,
               |B.LAST_QUARTER_ACTIVE_CNT AS LAST_QUARTER_ACTIVE,
               |A.TODAY_CNT  AS TODAY_TRAN_NUM
               |from
               |(
               |select
               |tempa.bank_nm as bank_nm,
               |tempa.card_attr as card_attr,
               |count(case when tempa.bind_dt<='$today_dt' then 1 end ) as TOTAL_BIND_CNT,
               |count(case when tempa.bind_dt='$today_dt' then 1 end ) as TODAY_CNT
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
               |from HIVE_CARD_BIND_INF where card_bind_st='0'
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
               |from HIVE_CARD_BIN
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
               |) A
               |
             |LEFT JOIN
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
               |	 end) as LAST_QUARTER_ACTIVE_CNT
               |
             |from HIVE_ACTIVE_CARD_ACQ_BRANCH_MON
               |where trans_class ='4' and
               |iss_root_ins_id_cd in ('0801020000','0801030000','0801040000','0801040003','0803070010',
               |   '0801050000','0801050001','0861000000','0801009999','0801000000',
               |   '0803010000','0803020000','0863020000','0803030000','0863030000',
               |   '0803040000','0803040001','0863040001','0803050000','0803050001',
               |   '0803060000','0803080000','0803090000','0803090002','0803090010',
               |   '0803100000','0804031000','0864031000','0804010000','0804012902',
               |   '0804012900','0804100000','0805105840','0806105840','0803070000')
               |GROUP BY iss_root_ins_id_cd,card_attr_id
               |   ) B
               |ON A.bank_nm=B.bank_nm AND A.card_attr=B.card_attr
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
               |SELECT
               |E.grp_nm as MCHNT_TYPE,
               |'$today_dt'  AS REPORT_DT,
               |SUM(C.quick_cnt),
               |SUM(C.quick_cardcnt),
               |SUM(D.quick_succ_cnt),
               |SUM(D.quick_succ_cardcnt),
               |SUM(D.quick_succ_trans_at),
               |SUM(D.quick_succ_points_at),
               |SUM(A.TRAN_CNT),
               |SUM(A.CARDCNT),
               |SUM(B.SUCC_CNT),
               |SUM(B.SUCC_CARDCNT),
               |SUM(B.SUCC_TRANS_AT),
               |SUM(C.swp_quick_cnt),
               |SUM(C.swp_quick_usrcnt),
               |SUM(D.swp_quick_succ_cnt),
               |SUM(D.swp_quick_succ_usrcnt) ,
               |SUM(D.swp_quick_succ_trans_at),
               |SUM(D.swp_quick_succ_points_at),
               |SUM(A.swp_verify_cnt),
               |SUM(A.swp_verify_usrcnt),
               |SUM(B.swp_verify_succ_cnt),
               |SUM(B.swp_verify_succ_usrcnt ),
               |SUM(B.swp_verify_succ_trans_at)
               |FROM
               |(SELECT
               |tp_grp.MCHNT_TP_GRP_DESC_CN AS grp_nm ,
               |tp.MCHNT_TP
               |FROM HIVE_MCHNT_TP tp
               |LEFT JOIN HIVE_MCHNT_TP_GRP tp_grp
               |ON tp.MCHNT_TP_GRP=tp_grp.MCHNT_TP_GRP) E
               |LEFT JOIN
               |(
               |SELECT
               |mchnt_tp,
               |rec_crt,
               |SUM(CNT) AS TRAN_CNT,
               |SUM(CARDCNT) AS CARDCNT,
               |sum(case when TRANS_SOURCE in('0001','9001') then cnt end ) as swp_verify_cnt,
               |sum(case when TRANS_SOURCE in('0001','9001') then usrcnt end ) as swp_verify_usrcnt
               |FROM (SELECT MCHNT_CD,mchnt_tp,TRANS_SOURCE,to_date(rec_crt_ts) as rec_crt,COUNT(*) AS CNT,
               |COUNT(DISTINCT PRI_ACCT_NO) AS CARDCNT,
               |count(distinct usr_id) as usrcnt
               |FROM HIVE_ACTIVE_CODE_PAY_TRANS
               |WHERE USR_ID=0 AND TO_DATE(REC_CRT_TS)='$today_dt'
               |GROUP BY MCHNT_CD,mchnt_tp,TRANS_SOURCE,to_date(rec_crt_ts)
               |) t1
               |GROUP BY mchnt_tp,rec_crt) A
               |on a.mchnt_tp=E.mchnt_tp
               |LEFT JOIN
               |(
               |select
               |mchnt_tp,
               |rec_crt,
               |sum(cnt) as SUCC_CNT,
               |sum(cardcnt) as SUCC_CARDCNT,
               |sum(trans_at) as SUCC_TRANS_AT,
               |sum(case when TRANS_SOURCE in('0001','9001') then cnt end ) as swp_verify_succ_cnt,
               |sum(case when TRANS_SOURCE in('0001','9001') then usrcnt end ) as swp_verify_succ_usrcnt ,
               |sum(case when TRANS_SOURCE in('0001','9001') then trans_at end) as swp_verify_succ_trans_at
               |
               |from (select mchnt_tp,TRANS_SOURCE,to_date(rec_crt_ts) as rec_crt,count(*) as cnt,
               |count(distinct pri_acct_no) as cardcnt,sum(trans_at) as trans_at,
               |count(distinct usr_id) as usrcnt
               |from HIVE_ACTIVE_CODE_PAY_TRANS
               |where trans_st like '%000' and usr_id=0 and TO_DATE(REC_CRT_TS)='$today_dt'
               |group by mchnt_tp,TRANS_SOURCE,to_date(rec_crt_ts)) t2
               |group by mchnt_tp, rec_crt
               |) B
               |ON E.mchnt_tp=B.mchnt_tp and A.rec_crt=b.rec_crt
               |LEFT JOIN
               |(
               |select
               |mchnt_tp,
               |rec_crt,
               |sum(cnt) as quick_cnt,
               |sum(cardcnt) as quick_cardcnt,
               |sum(case when TRANS_SOURCE in('0001','9001') then cnt end ) as swp_quick_cnt,
               |sum(case when TRANS_SOURCE in('0001','9001') then usrcnt end ) as swp_quick_usrcnt
               |from
               |(select mchnt_cd,mchnt_tp,TRANS_SOURCE,to_date(rec_crt_ts) as rec_crt,count(*) as cnt,
               |count(distinct pri_acct_no) as cardcnt,count(distinct usr_id) as usrcnt
               |from HIVE_ACTIVE_CODE_PAY_TRANS
               |where usr_id<>0 and TO_DATE(REC_CRT_TS)='$today_dt'
               |group by mchnt_cd,mchnt_tp,TRANS_SOURCE,to_date(rec_crt_ts)) t3
               |group by mchnt_tp,rec_crt
               |) C
               |ON E.mchnt_tp=C.mchnt_tp and A.rec_crt=c.rec_crt
               |LEFT JOIN
               |(
               |select mchnt_tp,
               |rec_crt,
               |sum(cnt) as quick_succ_cnt,
               |sum(cardcnt) as quick_succ_cardcnt ,
               |sum(trans_at) as quick_succ_trans_at,
               |sum(POINTS_AT) as quick_succ_points_at,
               |sum(case when TRANS_SOURCE in('0001','9001') then cnt end ) as swp_quick_succ_cnt,
               |sum(case when TRANS_SOURCE in('0001','9001') then usrcnt end ) as swp_quick_succ_usrcnt ,
               |sum(case when TRANS_SOURCE in('0001','9001') then trans_at end) as swp_quick_succ_trans_at,
               |sum(case when TRANS_SOURCE in('0001','9001') then POINTS_AT end ) as swp_quick_succ_points_at
               |from
               |(select
               |mchnt_tp,
               |TRANS_SOURCE,
               |to_date(rec_crt_ts) as rec_crt,
               |count(*) as cnt,
               |count(distinct pri_acct_no) as cardcnt,
               |sum(POINTS_AT) as POINTS_AT,
               |sum(trans_at) as trans_at,
               |count(distinct usr_id) as usrcnt
               |from HIVE_ACTIVE_CODE_PAY_TRANS
               |where trans_st like '%000' and usr_id<>0 and TO_DATE(REC_CRT_TS)='$today_dt'
               |group by mchnt_tp,TRANS_SOURCE,to_date(rec_crt_ts) ) t4
               |group by mchnt_tp,rec_crt) D
               |ON E.mchnt_tp=D.mchnt_tp and A.rec_crt=D.rec_crt
               |WHERE A.mchnt_tp IS NOT NULL AND B.mchnt_tp IS NOT NULL AND C.mchnt_tp IS NOT NULL AND D.mchnt_tp IS NOT NULL
               |GROUP BY E.grp_nm
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
               |SELECT
               |E.MCHNT_CN_NM as MCHNT_NM,
               |'$today_dt'  AS REPORT_DT,
               |sum(C.quick_cnt),
               |sum(C.quick_cardcnt),
               |sum(D.quick_succ_cnt),
               |sum(D.quick_succ_cardcnt ),
               |sum(D.quick_succ_trans_at),
               |sum(D.quick_succ_points_at),
               |sum(A.TRAN_CNT),
               |sum(A.CARDCNT),
               |sum(B.SUCC_CNT),
               |sum(B.SUCC_CARDCNT),
               |sum(B.SUCC_TRANS_AT),
               |sum(C.swp_quick_cardcnt),
               |sum(C.swp_quick_usrcnt),
               |sum(D.swp_quick_succ_cnt),
               |sum(D.swp_quick_succ_usrcnt ),
               |sum(D.swp_quick_succ_trans_at),
               |sum(D.swp_quick_succ_points_at),
               |sum(A.swp_verify_cnt),
               |sum(A.swp_verify_cardcnt),
               |sum(B.swp_succ_verify_cnt),
               |sum(B.swp_succ_verify_cardcnt),
               |sum(B.swp_succ_verify_trans_at)
               |
               |FROM
               |HIVE_MCHNT_INF_WALLET E
               |LEFT JOIN
               |(
               |SELECT
               |mchnt_tp,
               |rec_crt,
               |CNT AS TRAN_CNT,
               |CARDCNT,
               |sum(case when TRANS_SOURCE in('0001','9001') then CNT end ) as swp_verify_cnt,
               |sum(case when TRANS_SOURCE in('0001','9001') then CARDCNT end) as swp_verify_cardcnt
               |FROM
               |(
               |SELECT
               |mchnt_tp,
               |TRANS_SOURCE,
               |to_date(rec_crt_ts) as rec_crt,
               |COUNT(*) AS CNT,
               |COUNT(DISTINCT PRI_ACCT_NO) AS CARDCNT
               |FROM HIVE_ACTIVE_CODE_PAY_TRANS
               |WHERE USR_ID=0 AND TO_DATE(REC_CRT_TS)='$today_dt'
               |GROUP BY mchnt_tp,TRANS_SOURCE,to_date(rec_crt_ts)) t1
               |GROUP BY mchnt_tp,rec_crt,CNT,CARDCNT
               |) A
               |on a.mchnt_tp=E.mchnt_tp
               |LEFT JOIN
               |(
               |select
               |mchnt_tp,
               |rec_crt,
               |sum(cnt) as SUCC_CNT,
               |sum(cardcnt) as SUCC_CARDCNT,
               |sum(trans_at) as SUCC_TRANS_AT,
               |sum(case when TRANS_SOURCE in('0001','9001') then cnt end ) as swp_succ_verify_cnt,
               |sum(case when TRANS_SOURCE in('0001','9001') then cardcnt end) as swp_succ_verify_cardcnt,
               |sum(case when TRANS_SOURCE in('0001','9001') then trans_at end) as swp_succ_verify_trans_at
               |from (select mchnt_tp,TRANS_SOURCE,to_date(rec_crt_ts) as rec_crt ,count(*) as cnt,
               |count(distinct pri_acct_no) as cardcnt,sum(trans_at) as trans_at
               |from HIVE_ACTIVE_CODE_PAY_TRANS
               |where trans_st like '%000' and usr_id=0 and TO_DATE(REC_CRT_TS)='$today_dt'
               |group by mchnt_tp,TRANS_SOURCE,to_date(rec_crt_ts)) t2
               |group by mchnt_tp,rec_crt
               |) B
               |ON E.mchnt_tp=B.mchnt_tp
               |LEFT JOIN
               |(
               |select
               |mchnt_tp,
               |to_date(rec_crt_ts) as rec_crt,
               |count(*) as quick_cnt,
               |count(distinct pri_acct_no) as quick_cardcnt,
               |count(distinct(case when TRANS_SOURCE in('0001','9001') then pri_acct_no end)) as swp_quick_cardcnt,
               |count(distinct(case when TRANS_SOURCE in('0001','9001') then usr_id end )) as swp_quick_usrcnt
               |from HIVE_ACTIVE_CODE_PAY_TRANS
               |where usr_id<>0 and TO_DATE(REC_CRT_TS)='$today_dt'
               |group by mchnt_tp,to_date(rec_crt_ts) ) C
               |ON E.mchnt_tp=C.mchnt_tp
               |LEFT JOIN
               |(
               |select
               |mchnt_tp,
               |rec_crt,
               |sum(cnt) as quick_succ_cnt,
               |sum(cardcnt) as quick_succ_cardcnt ,
               |sum(trans_at) as quick_succ_trans_at,
               |sum(POINTS_AT) as quick_succ_points_at,
               |sum(case when TRANS_SOURCE in('0001','9001') then cnt end ) as swp_quick_succ_cnt,
               |sum(case when TRANS_SOURCE in('0001','9001') then usrcnt end ) as swp_quick_succ_usrcnt ,
               |sum(case when TRANS_SOURCE in('0001','9001') then trans_at end) as swp_quick_succ_trans_at,
               |sum(case when TRANS_SOURCE in('0001','9001') then POINTS_AT end ) as swp_quick_succ_points_at
               |from
               |(select
               |mchnt_tp,
               |to_date(rec_crt_ts) as rec_crt,
               |TRANS_SOURCE,
               |count(*) as cnt,
               |count(distinct pri_acct_no) as cardcnt,
               |sum(POINTS_AT) as POINTS_AT,
               |sum(trans_at) as trans_at,
               |count(distinct usr_id) as usrcnt
               |from HIVE_ACTIVE_CODE_PAY_TRANS
               |where trans_st like '%000' and usr_id<>0 and TO_DATE(REC_CRT_TS)='$today_dt'
               |group by mchnt_tp,to_date(rec_crt_ts),TRANS_SOURCE ) t3
               |group by mchnt_tp,rec_crt ) D
               |ON E.mchnt_tp=D.mchnt_tp
               |where A.mchnt_tp IS NOT NULL and B.mchnt_tp IS NOT NULL and C.mchnt_tp IS NOT NULL and d.mchnt_tp IS NOT NULL
               |group by E.MCHNT_CN_NM
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
               |SELECT
               |trim(E.ISS_INS_CN_NM) as ISS_NM,
               |'$today_dt' as REPORT_DT,
               |sum(C.swp_quick_cnt) as SWP_QUICK_CNT,
               |sum(C.swp_quick_usrcnt) as SWP_QUICK_USRCNT,
               |sum(D.swp_quick_succ_cnt) as SWP_QUICK_SUCC_CNT,
               |sum(D.swp_quick_succ_usrcnt) as SWP_QUICK_SUCC_USRCNT,
               |sum(D.swp_quick_succ_trans_at) as SWP_QUICK_SUCC_TRANS_AT,
               |sum(D.swp_quick_succ_points_at) as SWP_QUICK_SUCC_POINTS_AT,
               |sum(A.swp_verify_cnt) as SWP_VERIFY_CNT,
               |sum(A.swp_verify_cardcnt) as SWP_VERIFY_CARDCNT,
               |sum(B.swp_verify_cnt) as SWP_VERIFY_SUCC_CNT,
               |sum(B.swp_verify_cardcnt) as SWP_VERIFY_SUCC_CARDCNT,
               |sum(B.swp_verify_trans_at) as SWP_VERIFY_SUCC_TRANS_AT
               |FROM
               |(SELECT ISS_INS_CN_NM,SUBSTR(ISS_INS_ID_CD,3,8) as ISS_INS_ID_CD
               |FROM HIVE_CARD_BIND_INF )E
               |LEFT JOIN
               |(
               |SELECT
               |ISS_INS_ID_CD,
               |TO_DATE(REC_CRT_TS) AS REC_CRT,
               |COUNT(*) AS swp_verify_cnt,
               |COUNT(DISTINCT PRI_ACCT_NO) as swp_verify_cardcnt
               |FROM HIVE_ACTIVE_CODE_PAY_TRANS
               |WHERE USR_ID=0 AND TRANS_SOURCE in('0001','9001') AND TO_DATE(REC_CRT_TS)='$today_dt'
               |GROUP BY ISS_INS_ID_CD,TO_DATE(REC_CRT_TS)) A
               |ON A.ISS_INS_ID_CD=E.ISS_INS_ID_CD
               |LEFT JOIN
               |(
               |select
               |ISS_INS_ID_CD,
               |TO_DATE(REC_CRT_TS) AS REC_CRT,
               |count(*) as swp_verify_cnt,
               |count(distinct pri_acct_no) as swp_verify_cardcnt,
               |sum(trans_at) as swp_verify_trans_at
               |from HIVE_ACTIVE_CODE_PAY_TRANS
               |where trans_st like '%000' and usr_id=0
               |AND TRANS_SOURCE in('0001','9001') and TO_DATE(REC_CRT_TS)='$today_dt'
               |group by ISS_INS_ID_CD,TO_DATE(REC_CRT_TS)
               |) B
               |ON E.ISS_INS_ID_CD=B.ISS_INS_ID_CD
               |full outer join
               |(
               |select
               |ISS_INS_ID_CD,
               |TO_DATE(REC_CRT_TS) AS REC_CRT,
               |count(*) as swp_quick_cnt,
               |count(distinct usr_id) as swp_quick_usrcnt
               |from HIVE_ACTIVE_CODE_PAY_TRANS
               |where usr_id<>0 and TRANS_SOURCE in('0001','9001') AND TO_DATE(REC_CRT_TS)='$today_dt'
               |group by ISS_INS_ID_CD,TO_DATE(REC_CRT_TS) ) C
               |ON E.ISS_INS_ID_CD=C.ISS_INS_ID_CD
               |full outer join
               |(select
               |ISS_INS_ID_CD,
               |TO_DATE(REC_CRT_TS) as REC_CRT,
               |count(*) as swp_quick_succ_cnt,
               |count(distinct usr_id) as swp_quick_succ_usrcnt ,
               |sum(trans_at) as swp_quick_succ_trans_at,
               |sum(POINTS_AT) as swp_quick_succ_points_at
               |from HIVE_ACTIVE_CODE_PAY_TRANS
               |where trans_st like '%000' and usr_id<>0 and TO_DATE(REC_CRT_TS)='$today_dt'
               |group by ISS_INS_ID_CD,TO_DATE(REC_CRT_TS) ) D
               |ON E.ISS_INS_ID_CD=D.ISS_INS_ID_CD
               |group by trim(E.ISS_INS_CN_NM),'$today_dt'
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
               |SELECT
               |CASE WHEN A.CARD_ATTR IS NULL THEN NVL(B.CARD_ATTR,NVL(C.CARD_ATTR,NVL(D.CARD_ATTR,'其它'))) ELSE A.CARD_ATTR END AS CARD_ATTR,
               |'$today_dt' AS REPORT_DT,
               |sum(C.swp_quick_cnt),
               |sum(C.swp_quick_usrcnt),
               |sum(D.swp_quick_succ_cnt),
               |sum(D.swp_quick_succ_usrcnt),
               |sum(D.swp_quick_succ_trans_at),
               |sum(D.swp_quick_succ_points_at),
               |sum(A.swp_verify_cnt),
               |sum(A.swp_verify_usrcnt),
               |sum(B.swp_succ_verify_cnt),
               |sum(B.swp_succ_verify_usrcnt),
               |sum(B.swp_succ_verify_trans_at)
               |FROM
               |(
               |SELECT
               |(case when card_attr in ('01') then '借记卡'
               |when card_attr in ('02', '03') then '贷记卡' end
               |) as CARD_ATTR,
               |COUNT(*) AS swp_verify_cnt,
               |COUNT(distinct usr_id) as swp_verify_usrcnt
               |FROM HIVE_ACTIVE_CODE_PAY_TRANS
               |WHERE USR_ID=0 AND TRANS_SOURCE in('0001','9001') AND TO_DATE(REC_CRT_TS)='$today_dt'
               |GROUP BY (case when card_attr in ('01') then '借记卡'
               |when card_attr in ('02', '03') then '贷记卡' end)) A
               |FULL OUTER JOIN
               |(
               |select
               |(case when card_attr in ('01') then '借记卡'
               |when card_attr in ('02', '03') then '贷记卡' end
               |) as CARD_ATTR,
               |count(*) as swp_succ_verify_cnt,
               |count(distinct usr_id) as swp_succ_verify_usrcnt,
               |sum(trans_at) as swp_succ_verify_trans_at
               |from HIVE_ACTIVE_CODE_PAY_TRANS
               |where trans_st like '%000' and usr_id=0
               |AND TRANS_SOURCE in('0001','9001') and TO_DATE(REC_CRT_TS)='$today_dt'
               |group by (case when card_attr in ('01') then '借记卡'
               |when card_attr in ('02', '03') then '贷记卡' end)
               |) B
               |ON A.CARD_ATTR=B.CARD_ATTR
               |FULL OUTER JOIN
               |(
               |select
               |(case when card_attr in ('01') then '借记卡'
               |when card_attr in ('02', '03') then '贷记卡' end) as CARD_ATTR,
               |count(*) as swp_quick_cnt,
               |count(distinct usr_id) as swp_quick_usrcnt
               |from HIVE_ACTIVE_CODE_PAY_TRANS
               |where usr_id<>0 and TRANS_SOURCE in('0001','9001') AND TO_DATE(REC_CRT_TS)='$today_dt'
               |group by (case when card_attr in ('01') then '借记卡'
               |when card_attr in ('02', '03') then '贷记卡' end) ) C
               |ON A.CARD_ATTR=C.CARD_ATTR
               |FULL OUTER JOIN
               |(select
               |(case when card_attr in ('01') then '借记卡'
               |when card_attr in ('02', '03') then '贷记卡' end
               |) as CARD_ATTR,
               |count(*) as swp_quick_succ_cnt,
               |count(distinct usr_id) as swp_quick_succ_usrcnt ,
               |sum(trans_at) as swp_quick_succ_trans_at,
               |sum(POINTS_AT) as swp_quick_succ_points_at
               |from HIVE_ACTIVE_CODE_PAY_TRANS
               |where trans_st like '%000' and usr_id<>0 and TO_DATE(REC_CRT_TS)='$today_dt'
               |group by (case when card_attr in ('01') then '借记卡'
               |when card_attr in ('02', '03') then '贷记卡' end) ) D
               |ON A.CARD_ATTR=D.CARD_ATTR
               |group by CASE WHEN A.CARD_ATTR IS NULL THEN NVL(B.CARD_ATTR,NVL(C.CARD_ATTR,NVL(D.CARD_ATTR,'其它'))) ELSE A.CARD_ATTR END
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
               |SELECT
               |trim(E.CARD_LVL_NM) as  CARD_LVL_NM,
               |'$today_dt' AS REPORT_DT,
               |sum(C.swp_quick_cnt) as swp_quick_cnt,
               |sum(C.swp_quick_usrcnt) as swp_quick_usrcnt,
               |sum(D.swp_quick_succ_cnt) as swp_quick_succ_cnt,
               |sum(D.swp_quick_succ_usrcnt) as swp_quick_succ_usrcnt,
               |sum(D.swp_quick_succ_trans_at) as swp_quick_succ_trans_at,
               |sum(D.swp_quick_succ_points_at) as swp_quick_succ_points_at,
               |sum(A.swp_verify_cnt) as swp_verify_cnt,
               |sum(A.swp_verify_usrcnt) as swp_verify_usrcnt,
               |sum(B.swp_succ_verify_cnt) as swp_succ_verify_cnt,
               |sum(B.swp_succ_verify_usrcnt) as swp_succ_verify_usrcnt,
               |sum(B.swp_succ_verify_trans_at) as swp_succ_verify_trans_at
               |FROM
               |(
               |SELECT
               |(case when card_attr in ('0') then '未知'
               |when card_attr in ('1') then '普卡'
               |when card_attr in ('2') then '银卡'
               |when card_attr in ('3') then '金卡'
               |when card_attr in ('4') then '白金卡'
               |when card_attr in ('5') then '钻石卡'
               |when card_attr in ('6') then '无限卡'
               |else '其它' end
               |) as CARD_LVL_NM,
               |substr(CARD_BIN,1,8) AS CARD_BIN
               |FROM HIVE_CARD_BIN ) E
               |LEFT JOIN
               |(
               |SELECT
               |substr(CARD_BIN,1,8) as CARD_BIN,
               |COUNT(*) AS swp_verify_cnt,
               |COUNT(distinct usr_id) as swp_verify_usrcnt
               |FROM HIVE_ACTIVE_CODE_PAY_TRANS
               |WHERE USR_ID=0 AND TRANS_SOURCE in('0001','9001') AND TO_DATE(REC_CRT_TS)='$today_dt'
               |GROUP BY substr(CARD_BIN,1,8)) A
               |ON A.CARD_BIN=E.CARD_BIN
               |LEFT JOIN
               |(
               |select
               |substr(CARD_BIN,1,8) AS CARD_BIN,
               |count(*) as swp_succ_verify_cnt,
               |count(distinct usr_id) as swp_succ_verify_usrcnt,
               |sum(trans_at) as swp_succ_verify_trans_at
               |from HIVE_ACTIVE_CODE_PAY_TRANS
               |where trans_st like '%000' and usr_id=0
               |AND TRANS_SOURCE in('0001','9001') and TO_DATE(REC_CRT_TS)='$today_dt'
               |group by substr(CARD_BIN,1,8)
               |) B
               |ON E.CARD_BIN=B.CARD_BIN
               |LEFT JOIN
               |(
               |select
               |substr(CARD_BIN,1,8) AS CARD_BIN,
               |count(*) as swp_quick_cnt,
               |count(distinct usr_id) as swp_quick_usrcnt
               |from HIVE_ACTIVE_CODE_PAY_TRANS
               |where usr_id<>0 and TRANS_SOURCE in('0001','9001') AND TO_DATE(REC_CRT_TS)='$today_dt'
               |group by substr(CARD_BIN,1,8) ) C
               |ON E.CARD_BIN=C.CARD_BIN
               |LEFT JOIN
               |(select
               |substr(CARD_BIN,1,8) AS CARD_BIN,
               |count(*) as swp_quick_succ_cnt,
               |count(distinct usr_id) as swp_quick_succ_usrcnt ,
               |sum(trans_at) as swp_quick_succ_trans_at,
               |sum(POINTS_AT) as swp_quick_succ_points_at
               |from HIVE_ACTIVE_CODE_PAY_TRANS
               |where trans_st like '%000' and usr_id<>0 and TO_DATE(REC_CRT_TS)='$today_dt'
               |group by substr(CARD_BIN,1,8)) D
               |ON E.CARD_BIN=D.CARD_BIN
               |group by trim(E.CARD_LVL_NM)
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
               |SELECT
               |E.CUP_BRANCH_INS_ID_NM as AREA_NM,
               |'$today_dt' AS REPORT_DT,
               |sum(C.quick_cnt) as quick_cnt,
               |sum(C.quick_usrcnt) as quick_usrcnt,
               |sum(D.quick_succ_cnt) as quick_succ_cnt,
               |sum(D.quick_succ_usrcnt) as quick_succ_usrcnt,
               |sum(D.quick_succ_trans_at) as quick_succ_trans_at,
               |sum(D.quick_succ_points_at) as quick_succ_points_at,
               |sum(A.TRAN_CNT) as TRAN_CNT,
               |sum(A.usrcnt) as usrcnt,
               |sum(B.SUCC_CNT) as SUCC_CNT,
               |sum(B.SUCC_USRCNT) as SUCC_USRCNT,
               |sum(B.SUCC_TRANS_AT) as SUCC_TRANS_AT,
               |sum(C.swp_quick_cnt) as swp_quick_cnt,
               |sum(C.swp_quick_usrcnt) as swp_quick_usrcnt,
               |sum(D.swp_quick_succ_cnt) as swp_quick_succ_cnt,
               |sum(D.swp_quick_succ_usrcnt) as swp_quick_succ_usrcnt,
               |sum(D.swp_quick_succ_trans_at) as swp_quick_succ_trans_at,
               |sum(D.swp_quick_succ_points_at) as swp_quick_succ_points_at,
               |sum(A.swp_verify_cnt) as swp_verify_cnt,
               |sum(A.swp_verify_usrcnt) as swp_verify_usrcnt,
               |sum(B.swp_verify_succ_cnt) as swp_verify_succ_cnt,
               |sum(B.swp_verify_succ_usrcnt) as swp_verify_succ_usrcnt,
               |sum(B.swp_verify_succ_trans_at) as swp_verify_succ_trans_at,
               |sum(B.swp_verify_succ_points_at) as swp_verify_succ_points_at
               |FROM
               |HIVE_MCHNT_INF_WALLET E
               |LEFT JOIN
               |(
               |SELECT
               |mchnt_tp ,
               |SUM(CNT) AS TRAN_CNT,
               |SUM(usrcnt) AS usrcnt,
               |sum(case when TRANS_SOURCE in('0001','9001') then cnt end ) as swp_verify_cnt,
               |sum(case when TRANS_SOURCE in('0001','9001') then usrcnt end ) as swp_verify_usrcnt
               |FROM (SELECT mchnt_tp,TRANS_SOURCE,COUNT(*) AS CNT,
               |count(distinct usr_id) as usrcnt
               |FROM HIVE_ACTIVE_CODE_PAY_TRANS
               |WHERE USR_ID=0 AND TO_DATE(REC_CRT_TS)='$today_dt'
               |GROUP BY mchnt_tp,TRANS_SOURCE
               |) T1
               |GROUP BY mchnt_tp) A
               |ON A.mchnt_tp=E.mchnt_tp
               |LEFT JOIN
               |(
               |select
               |mchnt_tp,
               |sum(cnt) as SUCC_CNT,
               |sum(usrcnt) as SUCC_usrcnt,
               |sum(trans_at) as SUCC_TRANS_AT,
               |sum(case when TRANS_SOURCE in('0001','9001') then cnt end ) as swp_verify_succ_cnt,
               |sum(case when TRANS_SOURCE in('0001','9001') then usrcnt end ) as swp_verify_succ_usrcnt ,
               |sum(case when TRANS_SOURCE in('0001','9001') then trans_at end) as swp_verify_succ_trans_at,
               |sum(case when TRANS_SOURCE in('0001','9001') then POINTS_AT end ) as swp_verify_succ_points_at
               |from (select mchnt_tp,TRANS_SOURCE,count(*) as cnt,
               |sum(trans_at) as trans_at,
               |sum(points_at) as POINTS_AT,
               |count(distinct usr_id) as usrcnt
               |from HIVE_ACTIVE_CODE_PAY_TRANS
               |where trans_st like '%000' and usr_id=0 and TO_DATE(REC_CRT_TS)=='$today_dt'
               |group by mchnt_tp,TRANS_SOURCE) T2
               |GROUP BY mchnt_tp
               |) B
               |ON E.mchnt_tp=B.mchnt_tp
               |LEFT JOIN
               |(
               |select
               |mchnt_tp,
               |sum(cnt) as quick_cnt,
               |sum(usrcnt) as quick_usrcnt,
               |sum(case when TRANS_SOURCE in('0001','9001') then cnt end ) as swp_quick_cnt,
               |sum(case when TRANS_SOURCE in('0001','9001') then usrcnt end ) as swp_quick_usrcnt
               |from
               |(select mchnt_tp,TRANS_SOURCE,count(*) as cnt,
               |count(distinct usr_id) as usrcnt
               |from HIVE_ACTIVE_CODE_PAY_TRANS
               |where usr_id<>0 and TO_DATE(REC_CRT_TS)='$today_dt'
               |group by mchnt_tp,TRANS_SOURCE) t3
               |GROUP BY mchnt_tp
               |) C
               |ON E.mchnt_tp=C.mchnt_tp
               |LEFT JOIN
               |(
               |select mchnt_tp,
               |sum(cnt) as quick_succ_cnt,
               |sum(usrcnt) as quick_succ_usrcnt ,
               |sum(trans_at) as quick_succ_trans_at,
               |sum(POINTS_AT) as quick_succ_points_at,
               |sum(case when TRANS_SOURCE in('0001','9001') then cnt end ) as swp_quick_succ_cnt,
               |sum(case when TRANS_SOURCE in('0001','9001') then usrcnt end ) as swp_quick_succ_usrcnt ,
               |sum(case when TRANS_SOURCE in('0001','9001') then trans_at end) as swp_quick_succ_trans_at,
               |sum(case when TRANS_SOURCE in('0001','9001') then POINTS_AT end ) as swp_quick_succ_points_at
               |from
               |(select
               |mchnt_tp,
               |TRANS_SOURCE,
               |count(*) as cnt,
               |sum(POINTS_AT) as POINTS_AT,
               |sum(trans_at) as trans_at,
               |count(distinct usr_id) as usrcnt
               |from HIVE_ACTIVE_CODE_PAY_TRANS
               |where trans_st like '%000' and usr_id<>0 and TO_DATE(REC_CRT_TS)='$today_dt'
               |group by mchnt_tp,TRANS_SOURCE) t4
               |GROUP BY mchnt_tp
               |) D
               |ON E.mchnt_tp=D.mchnt_tp
               |WHERE A.mchnt_tp IS NOT NULL AND B.mchnt_tp IS NOT NULL AND C.mchnt_tp IS NOT NULL AND D.mchnt_tp IS NOT NULL
               |GROUP BY E.CUP_BRANCH_INS_ID_NM
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
               |SELECT
               |RESP_CD as RESP_CD,
               |TO_DATE(REC_CRT_TS) as report_dt,
               |COUNT(*) AS  tran_cnt,
               |sum(trans_at) as trans_at
               |FROM HIVE_ACTIVE_CODE_PAY_TRANS
               |WHERE  length(RESP_CD)=2  and  TO_DATE(REC_CRT_TS)>='$today_dt'  and  TO_DATE(REC_CRT_TS)<= '$today_dt'
               |GROUP BY RESP_CD,TO_DATE(REC_CRT_TS)
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
               |SELECT
               |tempb.PHONE_LOCATION AS BRANCH_NM,
               |'$today_dt' as REPORT_DT,
               |tempb.STOCK_NUM AS STOCK_NUM,
               |tempb.TODAY_NUM AS TODAY_NUM,
               |tempb.TOTAL_NUM AS TOTAL_NUM
               |FROM
               |(
               |SELECT
               |NVL(A.PHONE_LOCATION,A.PHONE_LOCATION) AS PHONE_LOCATION,
               |COUNT(distinct (CASE WHEN to_date(A.rec_upd_ts) > to_date(A.rec_crt_ts) THEN A.cdhd_usr_id END)) AS STOCK_NUM,
               |COUNT(distinct (CASE WHEN to_date(A.rec_upd_ts) = to_date(A.rec_crt_ts) THEN A.cdhd_usr_id END)) AS TODAY_NUM,
               |B.TOTAL_NUM AS TOTAL_NUM
               |FROM
               |(
               |select distinct
               |cdhd_usr_id,
               |PHONE_LOCATION,
               |rec_upd_ts,
               |rec_crt_ts
               |FROM HIVE_PRI_ACCT_INF
               |where
               |to_date(rec_upd_ts)>='$today_dt'
               |and to_date(rec_upd_ts)<='$today_dt'
               |and (usr_st='1' or (usr_st='2' and note='BDYX_FREEZE')) and  realnm_in='01'
               |) A
               |FULL OUTER JOIN
               |(
               |SELECT
               |tempa.PHONE_LOCATION AS PHONE_LOCATION,
               |count(distinct tempa.cdhd_usr_id) AS TOTAL_NUM
               |FROM
               |(
               |select
               |cdhd_usr_id,
               |PHONE_LOCATION
               |FROM
               |HIVE_PRI_ACCT_INF
               |where  to_date(rec_upd_ts)<='$today_dt' and
               |(usr_st='1' or (usr_st='2' and note='BDYX_FREEZE'))
               |and  realnm_in='01'
               |)tempa
               |GROUP BY tempa.PHONE_LOCATION
               |) B
               |ON A.PHONE_LOCATION=B.PHONE_LOCATION
               |GROUP BY NVL(A.PHONE_LOCATION,A.PHONE_LOCATION),B.TOTAL_NUM
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
             |SELECT
             |    t.cup_branch_ins_id_nm as cup_branch_ins_id_nm,
             |    t.report_dt as report_dt,
             |    SUM(t.cashier_cnt_tot) as cashier_cnt_tot,
             |    SUM(t.act_cashier_cnt_tot) as act_cashier_cnt_tot,
             |    SUM(t.non_act_cashier_cnt_tot) as non_act_cashier_cnt_tot,
             |    SUM(t.cashier_cnt_year) as cashier_cnt_year,
             |    SUM(t.act_cashier_cnt_year) as act_cashier_cnt_year,
             |    SUM(t.non_act_cashier_cnt_year)as non_act_cashier_cnt_year,
             |    SUM(t.cashier_cnt_mth) as cashier_cnt_mth,
             |    SUM(t.act_cashier_cnt_mth) as act_cashier_cnt_mth,
             |    SUM(t.non_act_cashier_cnt_mth) as non_act_cashier_cnt_mth,
             |    SUM(t.pnt_acct_cashier_cnt_tot) as pnt_acct_cashier_cnt_tot,
             |    SUM(t.reward_cashier_cnt_tot) as reward_cashier_cnt_tot,
             |    SUM(t.reward_cdhd_cashier_cnt_tot) as reward_cdhd_cashier_cnt_tot,
             |    sum(t.sign_cashier_cnt_dly) as sign_cashier_cnt_dly,
             |    SUM(t.cashier_cnt_dly) as cashier_cnt_dly,
             |    SUM(t.act_cashier_cnt_dly) as act_cashier_cnt_dly,
             |    SUM(t.non_act_cashier_cnt_dly) as non_act_cashier_cnt_dly
             |FROM
             |    (
             |        SELECT
             |            CASE
             |                WHEN a11.cup_branch_ins_id_nm IS NULL
             |                THEN
             |                    CASE
             |                        WHEN a12.cup_branch_ins_id_nm IS NULL
             |                        THEN
             |                            CASE
             |                                WHEN a13.cup_branch_ins_id_nm IS NULL
             |                                THEN
             |                                    CASE
             |                                        WHEN a21.cup_branch_ins_id_nm IS NULL
             |                                        THEN
             |                                            CASE
             |                                                WHEN a22.cup_branch_ins_id_nm IS NULL
             |                                                THEN
             |                                                    CASE
             |                                                        WHEN a23.cup_branch_ins_id_nm IS NULL
             |                                                        THEN
             |                                                            CASE
             |                                                                WHEN a31.cup_branch_ins_id_nm
             |                                                                    IS NULL
             |                                                                THEN
             |                                                                    CASE
             |                                                                        WHEN
             |                                                                            a32.cup_branch_ins_id_nm
             |                                                                            IS NULL
             |                                                                        THEN
             |                                                                            CASE
             |                                                                                WHEN
             |                                                                                    a33.cup_branch_ins_id_nm
             |                                                                                    IS NULL
             |                                                                                THEN
             |                                                                                    CASE
             |                                                                                        WHEN
             |                                                                                            a4.cup_branch_ins_id_nm
             |                                                                                            IS NULL
             |                                                                                        THEN
             |                                                                                            CASE
             |                                                                                                WHEN
             |                                                                                                    a5.cup_branch_ins_id_nm
             |                                                                                                    IS NULL
             |                                                                                                THEN
             |                                                                                                    CASE
             |                                                                                                        WHEN
             |                                                                                                            a6.cup_branch_ins_id_nm
             |                                                                                                            IS NULL
             |                                                                                                        THEN
             |                                                                                                            CASE
             |                                                                                                                WHEN
             |                                                                                                                    a7.cup_branch_ins_id_nm
             |                                                                                                                    IS NULL
             |                                                                                                                THEN
             |                                                                                                                    CASE
             |                                                                                                                        WHEN
             |                                                                                                                            a81.cup_branch_ins_id_nm
             |                                                                                                                            IS NULL
             |                                                                                                                        THEN
             |                                                                                                                            CASE
             |                                                                                                                                WHEN
             |                                                                                                                                    a82.cup_branch_ins_id_nm
             |                                                                                                                                    IS NULL
             |                                                                                                                                THEN
             |                                                                                                                                    CASE
             |                                                                                                                                        WHEN
             |                                                                                                                                            a83.cup_branch_ins_id_nm
             |                                                                                                                                            IS NOT NULL
             |                                                                                                                                        THEN
             |                                                                                                                                            a83.cup_branch_ins_id_nm
             |                                                                                                                                    END
             |                                                                                                                                ELSE
             |                                                                                                                                    a82.cup_branch_ins_id_nm
             |                                                                                                                            END
             |                                                                                                                        ELSE
             |                                                                                                                            a81.cup_branch_ins_id_nm
             |                                                                                                                    END
             |                                                                                                                ELSE
             |                                                                                                                    a7.cup_branch_ins_id_nm
             |                                                                                                            END
             |                                                                                                        ELSE
             |                                                                                                            a6.cup_branch_ins_id_nm
             |                                                                                                    END
             |                                                                                                ELSE
             |                                                                                                    a5.cup_branch_ins_id_nm
             |                                                                                            END
             |                                                                                        ELSE
             |                                                                                            a4.cup_branch_ins_id_nm
             |                                                                                    END
             |                                                                                ELSE
             |                                                                                    a33.cup_branch_ins_id_nm
             |                                                                            END
             |                                                                        ELSE
             |                                                                            a32.cup_branch_ins_id_nm
             |                                                                    END
             |                                                                ELSE a31.cup_branch_ins_id_nm
             |                                                            END
             |                                                        ELSE a23.cup_branch_ins_id_nm
             |                                                    END
             |                                                ELSE a22.cup_branch_ins_id_nm
             |                                            END
             |                                        ELSE a21.cup_branch_ins_id_nm
             |                                    END
             |                                ELSE a13.cup_branch_ins_id_nm
             |                            END
             |                        ELSE a12.cup_branch_ins_id_nm
             |                    END
             |                ELSE a11.cup_branch_ins_id_nm
             |            END                                                   AS cup_branch_ins_id_nm,
             |            '$today_dt'                                           AS report_dt,
             |            IF(a11.cashier_cnt_tot IS NULL,0,a11.cashier_cnt_tot)         AS cashier_cnt_tot,
             |            IF(a12.act_cashier_cnt_tot IS NULL,0,a12.act_cashier_cnt_tot)    AS act_cashier_cnt_tot,
             |            IF(a13.non_act_cashier_cnt_tot IS NULL,0,a13.non_act_cashier_cnt_tot) AS
             |                                                                       non_act_cashier_cnt_tot,
             |            IF(a21.cashier_cnt_year IS NULL,0,a21.cashier_cnt_year)         AS cashier_cnt_year,
             |            IF(a22.act_cashier_cnt_year IS NULL,0,a22.act_cashier_cnt_year) AS act_cashier_cnt_year
             |            ,
             |            IF(a23.non_act_cashier_cnt_year IS NULL,0,a23.non_act_cashier_cnt_year) AS
             |                                                                     non_act_cashier_cnt_year,
             |            IF(a31.cashier_cnt_mth IS NULL,0,a31.cashier_cnt_mth)         AS cashier_cnt_mth,
             |            IF(a32.act_cashier_cnt_mth IS NULL,0,a32.act_cashier_cnt_mth)    AS act_cashier_cnt_mth,
             |            IF(a33.non_act_cashier_cnt_mth IS NULL,0,a33.non_act_cashier_cnt_mth) AS
             |            non_act_cashier_cnt_mth,
             |            IF(a4.pnt_acct_cashier_cnt IS NULL,0,a4.pnt_acct_cashier_cnt) AS
             |            pnt_acct_cashier_cnt_tot,
             |            IF(a5.reward_cashier_cnt_tot IS NULL,0,a5.reward_cashier_cnt_tot) AS
             |            reward_cashier_cnt_tot,
             |            IF(a6.reward_cdhd_cashier_cnt_tot IS NULL,0,a6.reward_cdhd_cashier_cnt_tot) AS
             |                                                                        reward_cdhd_cashier_cnt_tot,
             |            IF(a7.sign_cashier_cnt_dly IS NULL,0,a7.sign_cashier_cnt_dly)   AS sign_cashier_cnt_dly,
             |            IF(a81.cashier_cnt_dly IS NULL,0,a81.cashier_cnt_dly)                AS cashier_cnt_dly,
             |            IF(a82.act_cashier_cnt_dly IS NULL,0,a82.act_cashier_cnt_dly)    AS act_cashier_cnt_dly,
             |            IF(a83.non_act_cashier_cnt_dly IS NULL,0,a83.non_act_cashier_cnt_dly) AS
             |            non_act_cashier_cnt_dly
             |        FROM
             |            (
             |                SELECT
             |                    cup_branch_ins_id_nm,
             |                    COUNT(DISTINCT cashier_usr_id) AS cashier_cnt_tot
             |                FROM
             |                    hive_cashier_bas_inf
             |                WHERE
             |                    to_date(reg_dt)<= '$today_dt'
             |                AND usr_st NOT IN ('4',
             |                                   '9')
             |                GROUP BY
             |                    cup_branch_ins_id_nm) a11
             |        FULL OUTER JOIN
             |            (
             |                SELECT
             |                    cup_branch_ins_id_nm,
             |                    COUNT(DISTINCT cashier_usr_id) AS act_cashier_cnt_tot
             |                FROM
             |                    hive_cashier_bas_inf
             |                WHERE
             |                    to_date(activate_ts) <= '$today_dt'
             |                AND usr_st IN ('1')
             |                GROUP BY
             |                    cup_branch_ins_id_nm) a12
             |        ON
             |            (
             |                a11.cup_branch_ins_id_nm = a12.cup_branch_ins_id_nm)
             |        FULL OUTER JOIN
             |            (
             |                SELECT
             |                    cup_branch_ins_id_nm,
             |                    COUNT(DISTINCT cashier_usr_id) AS non_act_cashier_cnt_tot
             |                FROM
             |                    hive_cashier_bas_inf
             |                WHERE
             |                    to_date(activate_ts)<= '$today_dt'
             |                AND usr_st IN ('0')
             |                GROUP BY
             |                    cup_branch_ins_id_nm) a13
             |        ON
             |            (
             |                a11.cup_branch_ins_id_nm = a13.cup_branch_ins_id_nm)
             |        FULL OUTER JOIN
             |            (
             |                SELECT
             |                    cup_branch_ins_id_nm,
             |                    COUNT(DISTINCT cashier_usr_id) AS cashier_cnt_year
             |                FROM
             |                    hive_cashier_bas_inf
             |                WHERE
             |                    to_date(reg_dt) <= '$today_dt'
             |                AND to_date(reg_dt) >= concat(substring('$today_dt',1,5),'01-01')
             |                AND usr_st NOT IN ('4',
             |                                   '9')
             |                GROUP BY
             |                    cup_branch_ins_id_nm) a21
             |        ON
             |            (
             |                a11.cup_branch_ins_id_nm = a21.cup_branch_ins_id_nm)
             |        FULL OUTER JOIN
             |            (
             |                SELECT
             |                    cup_branch_ins_id_nm,
             |                    COUNT(DISTINCT cashier_usr_id) AS act_cashier_cnt_year
             |                FROM
             |                    hive_cashier_bas_inf
             |                WHERE
             |                    to_date(activate_ts) <= '$today_dt'
             |                AND to_date(activate_ts) >= concat(substring('$today_dt',1,5),'01-01')
             |                AND usr_st IN ('1')
             |                GROUP BY
             |                    cup_branch_ins_id_nm) a22
             |        ON
             |            (
             |                a11.cup_branch_ins_id_nm = a22.cup_branch_ins_id_nm)
             |        FULL OUTER JOIN
             |            (
             |                SELECT
             |                    cup_branch_ins_id_nm,
             |                    COUNT(DISTINCT cashier_usr_id) AS non_act_cashier_cnt_year
             |                FROM
             |                    hive_cashier_bas_inf
             |                WHERE
             |                    to_date(activate_ts) <= '$today_dt'
             |                AND to_date(activate_ts) >= concat(substring('$today_dt',1,5),'01-01')
             |                AND usr_st IN ('0')
             |                GROUP BY
             |                    cup_branch_ins_id_nm) a23
             |        ON
             |            (
             |                a11.cup_branch_ins_id_nm = a23.cup_branch_ins_id_nm)
             |        FULL OUTER JOIN
             |            (
             |                SELECT
             |                    cup_branch_ins_id_nm,
             |                    COUNT(DISTINCT cashier_usr_id) AS cashier_cnt_mth
             |                FROM
             |                    hive_cashier_bas_inf
             |                WHERE
             |                    to_date(reg_dt) <= '$today_dt'
             |                AND to_date(reg_dt) >= concat(substring('$today_dt',1,8),'01')
             |                AND usr_st NOT IN ('4',
             |                                   '9')
             |                GROUP BY
             |                    cup_branch_ins_id_nm) a31
             |        ON
             |            (
             |                a11.cup_branch_ins_id_nm = a31.cup_branch_ins_id_nm)
             |        FULL OUTER JOIN
             |            (
             |                SELECT
             |                    cup_branch_ins_id_nm,
             |                    COUNT(DISTINCT cashier_usr_id) AS act_cashier_cnt_mth
             |                FROM
             |                    hive_cashier_bas_inf
             |                WHERE
             |                    to_date(activate_ts) <= '$today_dt'
             |                AND to_date(activate_ts) >= concat(substring('$today_dt',1,8),'01')
             |                AND usr_st IN ('1')
             |                GROUP BY
             |                    cup_branch_ins_id_nm) a32
             |        ON
             |            (
             |                a11.cup_branch_ins_id_nm = a32.cup_branch_ins_id_nm)
             |        FULL OUTER JOIN
             |            (
             |                SELECT
             |                    cup_branch_ins_id_nm,
             |                    COUNT(DISTINCT cashier_usr_id) AS non_act_cashier_cnt_mth
             |                FROM
             |                    hive_cashier_bas_inf
             |                WHERE
             |                    to_date(activate_ts) <= '$today_dt'
             |                AND to_date(activate_ts) >= concat(substring('$today_dt',1,8),'01')
             |                AND usr_st IN ('0')
             |                GROUP BY
             |                    cup_branch_ins_id_nm) a33
             |        ON
             |            (
             |                a11.cup_branch_ins_id_nm = a33.cup_branch_ins_id_nm)
             |        FULL OUTER JOIN
             |            (
             |                SELECT
             |                    b.cup_branch_ins_id_nm,
             |                    COUNT(DISTINCT a.cashier_usr_id) AS pnt_acct_cashier_cnt
             |                FROM
             |                    (
             |                        SELECT DISTINCT
             |                            cashier_usr_id
             |                        FROM
             |                            hive_cashier_point_acct_oper_dtl
             |                        WHERE
             |                            to_date(acct_oper_ts) <= '$today_dt'
             |                        AND to_date(acct_oper_ts) >= trunc('$today_dt','YYYY') )a
             |                INNER JOIN
             |                    (
             |                        SELECT
             |                            cup_branch_ins_id_nm,
             |                            cashier_usr_id
             |                        FROM
             |                            hive_cashier_bas_inf
             |                        WHERE
             |                            to_date(reg_dt)<= '$today_dt'
             |                        AND usr_st NOT IN ('4',
             |                                           '9') )b
             |                ON
             |                    a.cashier_usr_id=b.cashier_usr_id
             |                GROUP BY
             |                    b. cup_branch_ins_id_nm) a4
             |        ON
             |            (
             |                a11.cup_branch_ins_id_nm = a4.cup_branch_ins_id_nm)
             |        FULL OUTER JOIN
             |            (
             |                SELECT
             |                    b.cup_branch_ins_id_nm,
             |                    COUNT(DISTINCT b.cashier_usr_id) AS reward_cashier_cnt_tot
             |                FROM
             |                    (
             |                        SELECT
             |                            mobile
             |                        FROM
             |                            hive_cdhd_cashier_maktg_reward_dtl
             |                        WHERE
             |                            to_date(settle_dt) <= '$today_dt'
             |                        AND rec_st='2'
             |                        AND activity_tp='004'
             |                        GROUP BY
             |                            mobile ) a
             |                INNER JOIN
             |                    hive_cashier_bas_inf b
             |                ON
             |                    a.mobile=b.mobile
             |                GROUP BY
             |                    b.cup_branch_ins_id_nm) a5
             |        ON
             |            (
             |                a11.cup_branch_ins_id_nm = a5.cup_branch_ins_id_nm)
             |        FULL OUTER JOIN
             |            (
             |                SELECT
             |                    b.cup_branch_ins_id_nm,
             |                    COUNT(DISTINCT b.cashier_usr_id) reward_cdhd_cashier_cnt_tot
             |                FROM
             |                    (
             |                        SELECT
             |                            mobile
             |                        FROM
             |                            hive_cdhd_cashier_maktg_reward_dtl
             |                        WHERE
             |                            to_date(settle_dt) <= '$today_dt'
             |                        AND rec_st='2'
             |                        AND activity_tp='004'
             |                        GROUP BY
             |                            mobile ) a
             |                INNER JOIN
             |                    hive_cashier_bas_inf b
             |                ON
             |                    a.mobile=b.mobile
             |                INNER JOIN
             |                    hive_pri_acct_inf c
             |                ON
             |                    a.mobile=c.mobile
             |                WHERE
             |                    c.usr_st='1'
             |                GROUP BY
             |                    b.cup_branch_ins_id_nm) a6
             |        ON
             |            (
             |                a11.cup_branch_ins_id_nm = a6.cup_branch_ins_id_nm)
             |        FULL OUTER JOIN
             |            (
             |                SELECT
             |                    b.cup_branch_ins_id_nm,
             |                    COUNT(DISTINCT b.cashier_usr_id) AS sign_cashier_cnt_dly
             |                FROM
             |                    (
             |                        SELECT
             |                            pri_acct_no
             |                        FROM
             |                            hive_signer_log
             |                        WHERE
             |                            concat_ws('-',SUBSTR(cashier_trans_tm,1,4),SUBSTR(cashier_trans_tm,5,2)
             |                            ,SUBSTR (cashier_trans_tm,7,2)) = '$today_dt'
             |                        GROUP BY
             |                            pri_acct_no ) a
             |                INNER JOIN
             |                    (
             |                        SELECT
             |                            cup_branch_ins_id_nm,
             |                            cashier_usr_id,
             |                            bind_card_no
             |                        FROM
             |                            hive_cashier_bas_inf
             |                        WHERE
             |                            to_date(reg_dt) <= '$today_dt'
             |                        AND usr_st NOT IN ('4',
             |                                           '9') ) b
             |                ON
             |                    a.pri_acct_no=b.bind_card_no
             |                GROUP BY
             |                    b.cup_branch_ins_id_nm) a7
             |        ON
             |            (
             |                a11.cup_branch_ins_id_nm = a7.cup_branch_ins_id_nm)
             |        FULL OUTER JOIN
             |            (
             |                SELECT
             |                    cup_branch_ins_id_nm,
             |                    COUNT(DISTINCT cashier_usr_id) AS cashier_cnt_dly
             |                FROM
             |                    hive_cashier_bas_inf
             |                WHERE
             |                    to_date(reg_dt) = '$today_dt'
             |                AND usr_st NOT IN ('4',
             |                                   '9')
             |                GROUP BY
             |                    cup_branch_ins_id_nm) a81
             |        ON
             |            (
             |                a11.cup_branch_ins_id_nm = a81.cup_branch_ins_id_nm)
             |        FULL OUTER JOIN
             |            (
             |                SELECT
             |                    cup_branch_ins_id_nm,
             |                    COUNT(DISTINCT cashier_usr_id) AS act_cashier_cnt_dly
             |                FROM
             |                    hive_cashier_bas_inf
             |                WHERE
             |                    to_date(activate_ts) = '$today_dt'
             |                AND usr_st IN ('1')
             |                GROUP BY
             |                    cup_branch_ins_id_nm) a82
             |        ON
             |            (
             |                a11.cup_branch_ins_id_nm = a82.cup_branch_ins_id_nm)
             |        FULL OUTER JOIN
             |            (
             |                SELECT
             |                    cup_branch_ins_id_nm,
             |                    COUNT(DISTINCT cashier_usr_id) AS non_act_cashier_cnt_dly
             |                FROM
             |                    hive_cashier_bas_inf
             |                WHERE
             |                    to_date(activate_ts) = '$today_dt'
             |                AND usr_st IN ('0')
             |                GROUP BY
             |                    cup_branch_ins_id_nm) a83
             |        ON
             |            (
             |                a11.cup_branch_ins_id_nm = a83.cup_branch_ins_id_nm)) t
             |WHERE
             |    t.cup_branch_ins_id_nm IS NOT NULL
             |GROUP BY
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
               |case when a.CUP_BRANCH_INS_ID_NM is null then b.CUP_BRANCH_INS_ID_NM else a.CUP_BRANCH_INS_ID_NM end AS BRANCH_NM,
               |case when a.IF_HCE is null then b.IF_HCE else a.IF_HCE end AS IF_HCE,
               |'$today_dt' as report_dt,
               |sum(a.COUPON_CLASS),
               |sum(a.COUPON_PUB_NUM),
               |sum(a.COUPON_DOW_NUM),
               |sum(b.BATCH_NUM),
               |sum(a.DOW_USR_NUM),
               |sum(b.BATCH_SUR_NUM)
               |from
               |(select bill.CUP_BRANCH_INS_ID_NM,
               |CASE WHEN substr(UDF_FLD,31,2) in ('01','02','03','04','05') THEN '仅限云闪付' ELSE '非仅限云闪付' END AS IF_HCE,
               |count(*) as COUPON_CLASS ,
               |sum(case when bill.dwn_total_num = -1 then bill.dwn_num else bill.dwn_total_num end) as COUPON_PUB_NUM ,
               |sum(bill.dwn_num) as COUPON_DOW_NUM,
               |sum(dtl.CDHD_USR_ID) as DOW_USR_NUM
               |from HIVE_DOWNLOAD_TRANS as dtl,
               |HIVE_TICKET_BILL_BAS_INF as bill
               |where dtl.bill_id=bill.bill_id
               |and dtl.um_trans_id in ('12','17')
               |and dtl.trans_st='1' and dtl.trans_dt>='$today_dt' and dtl.trans_dt<='$today_dt' and dtl.bill_nm not like '%机场%' and dtl.bill_nm not like '%住两晚送一晚%'
               |and bill.rec_crt_ts>='$today_dt' and bill.rec_crt_ts<='$today_dt'
               |and bill.bill_sub_tp in ('01','03') and bill.bill_nm not like '%测试%' and bill.bill_nm not like '%验证%' and bill.bill_id <>'Z00000000020415'
               |and bill.bill_id<>'Z00000000020878' and bill.bill_nm not like '%满2元减1%' and bill.bill_nm not like '%满2分减1分%'
               |and bill.bill_nm not like '%满2减1%' and bill.bill_nm not like '%满2抵1%' and bill.bill_nm not like '测%' and dwn_total_num-dwn_num<100000
               |group by bill.CUP_BRANCH_INS_ID_NM, CASE WHEN substr(UDF_FLD,31,2) in ('01','02','03','04','05') THEN '仅限云闪付' ELSE '非仅限云闪付' END) A
               |full outer join
               |(
               |select b.CUP_BRANCH_INS_ID_NM,
               |CASE WHEN substr(b.UDF_FLD,31,2) in ('01','02','03','04','05') THEN '仅限云闪付' ELSE '非仅限云闪付' END AS IF_HCE,
               |sum(adj.ADJ_TICKET_BILL) as BATCH_NUM ,
               |sum(distinct b.CDHD_USR_ID) as BATCH_SUR_NUM
               |from
               |HIVE_TICKET_BILL_ACCT_ADJ_TASK adj
               |inner join
               |(select CUP_BRANCH_INS_ID_NM,CDHD_USR_ID,bill.bill_id,UDF_FLD
               |from HIVE_DOWNLOAD_TRANS as dtl,
               |HIVE_TICKET_BILL_BAS_INF as bill
               |where dtl.bill_id=bill.bill_id
               |and dtl.um_trans_id in ('12','17')
               |and dtl.trans_st='1' and dtl.trans_dt>='$today_dt' and dtl.trans_dt<='$today_dt' and dtl.bill_nm not like '%机场%' and dtl.bill_nm not like '%住两晚送一晚%'
               |and bill.rec_crt_ts>='$today_dt' and bill.rec_crt_ts<='$today_dt'
               |and bill.bill_sub_tp in ('01','03') and bill.bill_nm not like '%测试%' and bill.bill_nm not like '%验证%' and bill.bill_id <>'Z00000000020415'
               |and bill.bill_id<>'Z00000000020878' and bill.bill_nm not like '%满2元减1%' and bill.bill_nm not like '%满2分减1分%'
               |and bill.bill_nm not like '%满2减1%' and bill.bill_nm not like '%满2抵1%' and bill.bill_nm not like '测%' and dwn_total_num-dwn_num<100000
               |) b
               |on adj.bill_id=b.bill_id
               |group by b.CUP_BRANCH_INS_ID_NM, CASE WHEN substr(b.UDF_FLD,31,2) in ('01','02','03','04','05') THEN '仅限云闪付' ELSE '非仅限云闪付' END )B
               |on a.CUP_BRANCH_INS_ID_NM=b.CUP_BRANCH_INS_ID_NM and a.IF_HCE=b.IF_HCE
               |
               |group by case when a.CUP_BRANCH_INS_ID_NM is null then b.CUP_BRANCH_INS_ID_NM else a.CUP_BRANCH_INS_ID_NM end,
               |case when a.IF_HCE is null then b.IF_HCE else a.IF_HCE end
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
               |            acpt_ins.cup_branch_ins_id_nm,
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
               |            ( acpt_ins.ins_id_cd=concat('000',mchnt.acpt_ins_id_cd))
               |        where
               |            trans.um_trans_id in ('AC02000065', 'AC02000063')
               |        and trans.buss_tp in ('04','05','06')
               |        and trans.part_trans_dt >= '$today_dt'
               |        and trans.part_trans_dt <= '$today_dt'
               |        group by
               |            acpt_ins.cup_branch_ins_id_nm,
               |            trans.trans_dt) a
               |left join
               |    (
               |        select
               |            acpt_ins.cup_branch_ins_id_nm,
               |            trans.trans_dt,
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
               |            trans.sys_det_cd = 's'
               |        and trans.um_trans_id in ('AC02000065',
               |                                  'AC02000063')
               |        and trans.buss_tp in ('04','05','06')
               |
               |        and trans.part_trans_dt >= '$today_dt'
               |        and trans.part_trans_dt <= '$today_dt'
               |        group by
               |            acpt_ins.cup_branch_ins_id_nm,
               |            trans.trans_dt)b
               |on
               |    (
               |        a.cup_branch_ins_id_nm = b.cup_branch_ins_id_nm
               |    and a.trans_dt = b.trans_dt )
               |
               |
               |
               |
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
               |                    tp_grp.mchnt_tp_grp_desc_cn as grp_nm,
               |                    tp.mchnt_tp_desc_cn         as tp_nm,
               |                    trans.trans_dt              as trans_dt,
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
               |                    tp_grp.mchnt_tp_grp_desc_cn as grp_nm ,
               |                    tp.mchnt_tp_desc_cn         as tp_nm,
               |                    trans.trans_dt,
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
               |                    trans.sys_det_cd='s'
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
               |            cbi.iss_ins_cn_nm,
               |            trans.trans_dt,
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
               |            cbi.iss_ins_cn_nm, trans_dt) a
               |left join
               |    (
               |        select
               |            cbi.iss_ins_cn_nm,
               |            trans.trans_dt,
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
               |            cbi.iss_ins_cn_nm,
               |            trans_dt)b
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
           |SELECT
           |    A.CUP_BRANCH_INS_ID_NM as CUP_BRANCH_INS_ID_NM,
           |    A.TRANS_DT as REPORT_DT,
           |    A.TRANSCNT as TRANS_CNT,
           |    B.SUCTRANSCNT  as SUC_TRANS_CNT,
           |    B.TRANSAT as TRANS_AT,
           |    B.DISCOUNTAT as DISCOUNT_AT,
           |    B.TRANSUSRCNT as TRANS_USR_CNT,
           |    B.TRANSCARDCNT as TRANS_CARD_CNT
           |FROM
           |    (
           |        SELECT
           |            BILL.CUP_BRANCH_INS_ID_NM,
           |            TRANS.TRANS_DT,
           |            COUNT(1) AS TRANSCNT
           |        FROM
           |            HIVE_ACC_TRANS TRANS
           |        LEFT JOIN
           |            HIVE_TICKET_BILL_BAS_INF BILL
           |        ON
           |            (
           |                TRANS.BILL_ID=BILL.BILL_ID)
           |        WHERE
           |            TRANS.UM_TRANS_ID IN ('AC02000065',
           |                                  'AC02000063')
           |        AND TRANS.BUSS_TP IN ('04','05','06')
           |        AND TRANS.TRANS_DT >= '$start_dt'
           |        AND TRANS.TRANS_DT <= '$end_dt'
           |        GROUP BY
           |            BILL.CUP_BRANCH_INS_ID_NM,
           |            TRANS_DT) A
           |LEFT JOIN
           |    (
           |        SELECT
           |            BILL.CUP_BRANCH_INS_ID_NM,
           |            TRANS.TRANS_DT,
           |            COUNT(1) AS SUCTRANSCNT,
           |            SUM(TRANS.TRANS_AT) AS TRANSAT,
           |            SUM(TRANS.DISCOUNT_AT)          AS DISCOUNTAT,
           |            COUNT(DISTINCT TRANS.CDHD_USR_ID) AS TRANSUSRCNT,
           |            COUNT(DISTINCT TRANS.PRI_ACCT_NO) AS TRANSCARDCNT
           |        FROM
           |            HIVE_ACC_TRANS TRANS
           |        LEFT JOIN
           |            HIVE_TICKET_BILL_BAS_INF BILL
           |        ON
           |            (
           |                TRANS.BILL_ID=BILL.BILL_ID)
           |        WHERE
           |            TRANS.SYS_DET_CD = 'S'
           |        AND TRANS.UM_TRANS_ID IN ('AC02000065',
           |                                  'AC02000063')
           |        AND TRANS.BUSS_TP IN ('04','05','06')
           |        AND TRANS.TRANS_DT >= '$start_dt'
           |        AND TRANS.TRANS_DT <= '$end_dt'
           |        GROUP BY
           |            BILL.CUP_BRANCH_INS_ID_NM,
           |            TRANS_DT)B
           |ON
           |    (
           |        A.CUP_BRANCH_INS_ID_NM = B.CUP_BRANCH_INS_ID_NM
           |    AND A.TRANS_DT = B.TRANS_DT )
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
           |SELECT
           |    A.FIRST_PARA_NM as FIRST_IND_NM,
           |    A.SECOND_PARA_NM as SECOND_IND_NM,
           |    A.TRANS_DT as REPORT_DT,
           |    A.TRANSCNT as TRANS_CNT,
           |    B.SUCTRANSCNT as SUC_TRANS_CNT,
           |    B.TRANSAT as TRANS_AT,
           |    B.DISCOUNTAT as DISCOUNT_AT,
           |    B.TRANSUSRCNT as TRANS_USR_CNT,
           |    B.TRANSCARDCNT as TRANS_CARD_CNT
           |FROM
           |    (
           |        SELECT
           |            MP.MCHNT_PARA_CN_NM  AS FIRST_PARA_NM,
           |            MP1.MCHNT_PARA_CN_NM AS SECOND_PARA_NM,
           |            TRANS.TRANS_DT,
           |            COUNT(1) AS TRANSCNT
           |        FROM
           |            HIVE_ACC_TRANS TRANS
           |        INNER JOIN
           |            HIVE_STORE_TERM_RELATION STR
           |        ON
           |            (
           |                TRANS.CARD_ACCPTR_CD = STR.MCHNT_CD
           |            AND TRANS.CARD_ACCPTR_TERM_ID = STR.TERM_ID)
           |        LEFT JOIN
           |            HIVE_PREFERENTIAL_MCHNT_INF PMI
           |        ON
           |            (
           |                STR.THIRD_PARTY_INS_ID = PMI.MCHNT_CD)
           |        LEFT JOIN
           |            HIVE_MCHNT_PARA MP
           |        ON
           |            (
           |                PMI.MCHNT_FIRST_PARA = MP.MCHNT_PARA_ID)
           |        LEFT JOIN
           |            HIVE_MCHNT_PARA MP1
           |        ON
           |            (
           |                PMI.MCHNT_SECOND_PARA = MP1.MCHNT_PARA_ID)
           |        WHERE
           |            TRANS.UM_TRANS_ID IN ('AC02000065',
           |                                  'AC02000063')
           |		AND STR.REC_ID IS NOT NULL
           |        AND TRANS.BUSS_TP IN ('04',
           |                              '05',
           |                              '06')
           |        AND TRANS.TRANS_DT >= '$start_dt'
           |        AND TRANS.TRANS_DT <= '$end_dt'
           |        GROUP BY
           |            MP.MCHNT_PARA_CN_NM,
           |            MP1.MCHNT_PARA_CN_NM,
           |            TRANS_DT) A
           |LEFT JOIN
           |    (
           |        SELECT
           |            MP.MCHNT_PARA_CN_NM  AS FIRST_PARA_NM,
           |            MP1.MCHNT_PARA_CN_NM AS SECOND_PARA_NM,
           |            TRANS.TRANS_DT,
           |            COUNT(1)                          AS SUCTRANSCNT,
           |            SUM(TRANS.TRANS_AT)               AS TRANSAT,
           |            SUM(TRANS.DISCOUNT_AT)            AS DISCOUNTAT,
           |            COUNT(DISTINCT TRANS.CDHD_USR_ID) AS TRANSUSRCNT,
           |            COUNT(DISTINCT TRANS.PRI_ACCT_NO) AS TRANSCARDCNT
           |        FROM
           |            HIVE_ACC_TRANS TRANS
           |        INNER JOIN
           |            HIVE_STORE_TERM_RELATION STR
           |        ON
           |            (
           |                TRANS.CARD_ACCPTR_CD = STR.MCHNT_CD
           |            AND TRANS.CARD_ACCPTR_TERM_ID = STR.TERM_ID)
           |        LEFT JOIN
           |            HIVE_PREFERENTIAL_MCHNT_INF PMI
           |        ON
           |            (
           |                STR.THIRD_PARTY_INS_ID = PMI.MCHNT_CD)
           |        LEFT JOIN
           |            HIVE_MCHNT_PARA MP
           |        ON
           |            (
           |                PMI.MCHNT_FIRST_PARA = MP.MCHNT_PARA_ID)
           |        LEFT JOIN
           |            HIVE_MCHNT_PARA MP1
           |        ON
           |            (
           |                PMI.MCHNT_SECOND_PARA = MP1.MCHNT_PARA_ID)
           |        WHERE
           |            TRANS.SYS_DET_CD = 'S'
           |        AND TRANS.UM_TRANS_ID IN ('AC02000065',
           |                                  'AC02000063')
           |		AND STR.REC_ID IS NOT NULL
           |        AND TRANS.BUSS_TP IN ('04',
           |                              '05',
           |                              '06')
           |        AND TRANS.TRANS_DT >= '$start_dt'
           |        AND TRANS.TRANS_DT <= '$end_dt'
           |        GROUP BY
           |            MP.MCHNT_PARA_CN_NM,
           |            MP1.MCHNT_PARA_CN_NM,
           |            TRANS_DT)B
           |ON
           |    (
           |        A.FIRST_PARA_NM = B.FIRST_PARA_NM
           |    AND A.SECOND_PARA_NM = B.SECOND_PARA_NM
           |    AND A.TRANS_DT = B.TRANS_DT )
           |UNION ALL
           |SELECT
           |    A.FIRST_PARA_NM as FIRST_IND_NM,
           |    A.SECOND_PARA_NM as SECOND_IND_NM,
           |    A.TRANS_DT as REPORT_DT,
           |    A.TRANSCNT as TRANS_CNT,
           |    B.SUCTRANSCNT as SUC_TRANS_CNT,
           |    B.TRANSAT as TRANS_AT,
           |    B.DISCOUNTAT as DISCOUNT_AT,
           |    B.TRANSUSRCNT as TRANS_USR_CNT,
           |    B.TRANSCARDCNT as TRANS_CARD_CNT
           |FROM
           |    (
           |        SELECT
           |            MP.MCHNT_PARA_CN_NM  AS FIRST_PARA_NM,
           |            MP1.MCHNT_PARA_CN_NM AS SECOND_PARA_NM,
           |            TRANS.TRANS_DT,
           |            COUNT(1) AS TRANSCNT
           |        FROM
           |            HIVE_ACC_TRANS TRANS
           |        LEFT JOIN
           |            HIVE_STORE_TERM_RELATION STR
           |        ON
           |            (
           |                TRANS.CARD_ACCPTR_CD = STR.MCHNT_CD
           |            AND TRANS.CARD_ACCPTR_TERM_ID = STR.TERM_ID)
           |        LEFT JOIN
           |            (
           |                SELECT
           |                    MCHNT_CD,
           |                    MAX(THIRD_PARTY_INS_ID) OVER (PARTITION BY MCHNT_CD) AS THIRD_PARTY_INS_ID
           |                FROM
           |                    HIVE_STORE_TERM_RELATION) STR1
           |        ON
           |            (
           |                TRANS.CARD_ACCPTR_CD = STR1.MCHNT_CD)
           |        LEFT JOIN
           |            HIVE_PREFERENTIAL_MCHNT_INF PMI
           |        ON
           |            (
           |                STR1.THIRD_PARTY_INS_ID = PMI.MCHNT_CD)
           |        LEFT JOIN
           |            HIVE_MCHNT_PARA MP
           |        ON
           |            (
           |                PMI.MCHNT_FIRST_PARA = MP.MCHNT_PARA_ID)
           |        LEFT JOIN
           |            HIVE_MCHNT_PARA MP1
           |        ON
           |            (
           |                PMI.MCHNT_SECOND_PARA = MP1.MCHNT_PARA_ID)
           |        WHERE
           |            TRANS.UM_TRANS_ID IN ('AC02000065',
           |                                  'AC02000063')
           |        AND TRANS.BUSS_TP IN ('04',
           |                              '05',
           |                              '06')
           |        AND STR.REC_ID IS NULL
           |        AND TRANS.TRANS_DT >= '$start_dt'
           |        AND TRANS.TRANS_DT <= '$end_dt'
           |        GROUP BY
           |            MP.MCHNT_PARA_CN_NM,
           |            MP1.MCHNT_PARA_CN_NM,
           |            TRANS_DT) A
           |LEFT JOIN
           |    (
           |        SELECT
           |            MP.MCHNT_PARA_CN_NM  AS FIRST_PARA_NM,
           |            MP1.MCHNT_PARA_CN_NM AS SECOND_PARA_NM,
           |            TRANS.TRANS_DT,
           |            COUNT(1)                          AS SUCTRANSCNT,
           |            SUM(TRANS.TRANS_AT)               AS TRANSAT,
           |            SUM(TRANS.DISCOUNT_AT)            AS DISCOUNTAT,
           |            COUNT(DISTINCT TRANS.CDHD_USR_ID) AS TRANSUSRCNT,
           |            COUNT(DISTINCT TRANS.PRI_ACCT_NO) AS TRANSCARDCNT
           |        FROM
           |            HIVE_ACC_TRANS TRANS
           |        LEFT JOIN
           |            HIVE_STORE_TERM_RELATION STR
           |        ON
           |            (
           |                TRANS.CARD_ACCPTR_CD = STR.MCHNT_CD
           |            AND TRANS.CARD_ACCPTR_TERM_ID = STR.TERM_ID)
           |        LEFT JOIN
           |            (
           |                SELECT
           |                    MCHNT_CD,
           |                    MAX(THIRD_PARTY_INS_ID) OVER (PARTITION BY MCHNT_CD) AS THIRD_PARTY_INS_ID
           |                FROM
           |                    HIVE_STORE_TERM_RELATION) STR1
           |        ON
           |            (
           |                TRANS.CARD_ACCPTR_CD = STR1.MCHNT_CD)
           |        LEFT JOIN
           |            HIVE_PREFERENTIAL_MCHNT_INF PMI
           |        ON
           |            (
           |                STR1.THIRD_PARTY_INS_ID = PMI.MCHNT_CD)
           |        LEFT JOIN
           |            HIVE_MCHNT_PARA MP
           |        ON
           |            (
           |                PMI.MCHNT_FIRST_PARA = MP.MCHNT_PARA_ID)
           |        LEFT JOIN
           |            HIVE_MCHNT_PARA MP1
           |        ON
           |            (
           |                PMI.MCHNT_SECOND_PARA = MP1.MCHNT_PARA_ID)
           |        WHERE
           |            TRANS.SYS_DET_CD = 'S'
           |        AND TRANS.UM_TRANS_ID IN ('AC02000065',
           |                                  'AC02000063')
           |        AND TRANS.BUSS_TP IN ('04',
           |                              '05',
           |                              '06')
           |        AND STR.REC_ID IS NULL
           |        AND TRANS.TRANS_DT >= '$start_dt'
           |        AND TRANS.TRANS_DT <= '$end_dt'
           |        GROUP BY
           |            MP.MCHNT_PARA_CN_NM,
           |            MP1.MCHNT_PARA_CN_NM,
           |            TRANS_DT)B
           |ON
           |    (
           |        A.FIRST_PARA_NM = B.FIRST_PARA_NM
           |    AND A.SECOND_PARA_NM = B.SECOND_PARA_NM
           |    AND A.TRANS_DT = B.TRANS_DT )
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