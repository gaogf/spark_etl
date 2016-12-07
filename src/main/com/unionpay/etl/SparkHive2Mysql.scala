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
      .set("spark.yarn.executor.memoryOverhead","2000")
      .set("spark.newwork.buffer.timeout","300s")
      .set("spark.executor.heartbeatInterval","30s")
      .set("spark.driver.extraJavaOptions","-XX:+UseG1GC -XX:+UseCompressedOops")
      .set("spark.executor.extraJavaOptions","-XX:+UseG1GC -XX:+UseCompressedOops")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new HiveContext(sc)

    val rowParams=UPSQL_TIMEPARAMS_JDBC.readTimeParams(sqlContext)
    val start_dt=DateUtils.getYesterdayByJob(rowParams.getString(0))//获取开始日期：start_dt-1
    val end_dt=rowParams.getString(1)//结束日期
    val interval=DateUtils.getIntervalDays(start_dt,end_dt).toInt

    println(s"#### SparkHive2Mysql 数据清洗的起始日期为: $start_dt --  $end_dt")

    val JobName = if(args.length>0) args(0) else None
    println(s"#### The Current Job Name is ： [$JobName]")
    JobName match {
      /**
        * 每日模板job
        */
      case "JOB_DM_1"  => JOB_DM_1(sqlContext,start_dt,end_dt,interval)    //CODE BY YX
      case "JOB_DM_3"  => JOB_DM_3(sqlContext,start_dt,end_dt,interval)    //CODE BY YX
      case "JOB_DM_9"  => JOB_DM_9(sqlContext,start_dt,end_dt,interval)    //CODE BY XTP
      case "JOB_DM_55"  => JOB_DM_55(sqlContext,start_dt,end_dt)   //CODE BY TZQ
      case "JOB_DM_61"  => JOB_DM_61(sqlContext,start_dt,end_dt,interval)   //CODE BY YX
      case "JOB_DM_62"  => JOB_DM_62(sqlContext,start_dt,end_dt,interval)   //CODE BY TZQ
      case "JOB_DM_63"  => JOB_DM_63(sqlContext,start_dt,end_dt,interval)   //CODE BY XTP
      case "JOB_DM_65"  => JOB_DM_65(sqlContext,start_dt,end_dt,interval)   //CODE BY XTP
      case "JOB_DM_66"  => JOB_DM_66(sqlContext,start_dt,end_dt,interval)   //CODE BY TZQ
      case "JOB_DM_67"  => JOB_DM_67(sqlContext,start_dt,end_dt)   //CODE BY YX
      case "JOB_DM_68"  => JOB_DM_68(sqlContext,start_dt,end_dt)   //CODE BY YX
      case "JOB_DM_69"  => JOB_DM_69(sqlContext,start_dt,end_dt)   //CODE BY TZQ
      case "JOB_DM_70"  => JOB_DM_70(sqlContext,start_dt,end_dt)   //CODE BY TZQ
      case "JOB_DM_71"  => JOB_DM_71(sqlContext,start_dt,end_dt)   //CODE BY TZQ
      case "JOB_DM_72"  => JOB_DM_72(sqlContext,start_dt,end_dt)   //CODE BY YX
      case "JOB_DM_73"  => JOB_DM_73(sqlContext,start_dt,end_dt)   //CODE BY XTP
      case "JOB_DM_74"  => JOB_DM_74(sqlContext,start_dt,end_dt)   //CODE BY XTP
      case "JOB_DM_75"  => JOB_DM_75(sqlContext,start_dt,end_dt)   //CODE BY XTP
      case "JOB_DM_76"  => JOB_DM_76(sqlContext,start_dt,end_dt,interval)   //CODE BY TZQ
      case "JOB_DM_78"  => JOB_DM_78(sqlContext,start_dt,end_dt,interval)   //CODE BY XTP
      case "JOB_DM_86"  => JOB_DM_86(sqlContext,start_dt,end_dt,interval)   //CODE BY XTP
      case "JOB_DM_87"  => JOB_DM_87(sqlContext,start_dt,end_dt,interval)   //CODE BY TZQ

      /**
        * 指标套表job
        */
      case "JOB_DM_4"  => JOB_DM_4(sqlContext,start_dt,end_dt,interval)    //CODE BY XTP
      case "JOB_DM_2"  => JOB_DM_2(sqlContext,start_dt,end_dt,interval)    //CODE BY XTP
      case "JOB_DM_5"  => JOB_DM_5(sqlContext,start_dt,end_dt,interval)    //CODE BY TZQ
      case "JOB_DM_6"  => JOB_DM_6(sqlContext,start_dt,end_dt,interval)    //CODE BY TZQ
      case "JOB_DM_7"  => JOB_DM_7(sqlContext,start_dt,end_dt,interval)    //CODE BY TZQ
      case "JOB_DM_8"  => JOB_DM_8(sqlContext,start_dt,end_dt,interval)    //CODE BY TZQ
      case "JOB_DM_54" =>JOB_DM_54(sqlContext,start_dt,end_dt)   //CODE BY XTP 无数据
      case "JOB_DM_10" =>JOB_DM_10(sqlContext,start_dt,end_dt,interval)   //CODE BY TZQ
      case "JOB_DM_11" =>JOB_DM_11(sqlContext,start_dt,end_dt,interval)   //CODE BY TZQ
      case "JOB_DM_12" =>JOB_DM_12(sqlContext,start_dt,end_dt,interval)   //CODE BY TZQ
      case "JOB_DM_13" =>JOB_DM_13(sqlContext,start_dt,end_dt,interval)   //CODE BY TZQ
      case "JOB_DM_14" =>JOB_DM_14(sqlContext,start_dt,end_dt,interval)   //CODE BY TZQ
      case "JOB_DM_15" =>JOB_DM_15(sqlContext,start_dt,end_dt,interval)   //CODE BY TZQ
      case "JOB_DM_16" =>JOB_DM_16(sqlContext,start_dt,end_dt,interval)   //CODE BY TZQ
      case "JOB_DM_17" =>JOB_DM_17(sqlContext,start_dt,end_dt,interval)   //CODE BY TZQ

      case _ => println("#### No Case Job,Please Input JobName")
    }

    sc.stop()

  }


  /**
    * JobName: JOB_DM_1
    * Feature: hive_pri_acct_inf,hive_card_bind_inf,hive_acc_trans -> dm_user_card_nature
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
      if (interval > 0) {
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
               |and to_date(rec_crt_ts)='$today_dt '
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
    UPSQL_JDBC.delete("dm_user_idcard_home","report_dt",start_dt,end_dt)

    var today_dt=start_dt
    if(interval>0 ){
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
        println(s"###JOB_DM_2------$today_dt results:"+results.count())
        if(!Option(results).isEmpty){
          results.save2Mysql("DM_USER_IDCARD_HOME")
        }else{
          println("指定的时间范围无数据插入！")
        }

        today_dt=DateUtils.addOneDay(today_dt)
      }
    }
  }


  /**
    * JobName: JOB_DM_3
    * Feature: hive_pri_acct_inf,hive_inf_source_dtl,hive_acc_trans,hive_card_bind_inf,hive_inf_source_class-> dm_user_card_nature
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
      if (interval > 0) {
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
    UPSQL_JDBC.delete("DM_USER_CARD_AUTH","REPORT_DT",start_dt,end_dt)
    var today_dt=start_dt
    if(interval>0){
      sqlContext.sql(s"use $hive_dbname")
      for(i <- 0 to interval){
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
        println(s"###JOB_DM_4------$today_dt results:"+results.count())
        if(!Option(results).isEmpty){
          results.save2Mysql("DM_USER_CARD_AUTH")
        }else{
          println("指定的时间范围无数据插入！")
        }

        today_dt=DateUtils.addOneDay(today_dt)
      }
    }

  }

  /**
    * JOB_DM_5  2016年9月27日 星期二
    * dm_user_card_iss->hive_pri_acct_inf+hive_acc_trans+hive_card_bind_inf+hive_card_bin
    *
    * @author tzq
    * @param sqlContext
    */
  def JOB_DM_5(implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {

    println("###JOB_DM_5(dm_user_card_iss->hive_pri_acct_inf+hive_acc_trans+hive_card_bind_inf+hive_card_bin)")

    //1.先删除作业指定开始日期和结束日期间的数据
    UPSQL_JDBC.delete("dm_user_card_iss","report_dt",start_dt,end_dt);
    var today_dt=start_dt
    //2.循环从指定的日期范围内抽取数据（单位：天）
    if(interval>0 ){
      sqlContext.sql(s"use $hive_dbname")
      for(i <- 0 to interval){
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

        println(s"###JOB_DM_5------$today_dt results:"+results.count())
        if(!Option(results).isEmpty){
          results.save2Mysql("dm_user_card_iss")
        }else{
          println("指定的时间范围无数据插入！")
        }
        //日期加1天
        today_dt=DateUtils.addOneDay(today_dt)
      }
    }

  }

  /**
    * JOB_DM_6  2016年9月27日 星期二
    * dm_user_card_nature
    *
    * @author tzq
    * @param sqlContext
    */
  def JOB_DM_6(implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {

    println("JOB_DM_6------->JOB_DM_6(dm_user_card_nature->hive_pri_acct_inf+hive_card_bind_inf+hive_acc_trans)")

    UPSQL_JDBC.delete("dm_user_card_nature","report_dt",start_dt,end_dt)

    var today_dt=start_dt
    if(interval>0 ){
      sqlContext.sql(s"use $hive_dbname")
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

        println(s"###JOB_DM_6------$today_dt results:"+results.count())

        if(!Option(results).isEmpty){
          results.save2Mysql("dm_user_card_nature")

        }else{
          println("指定的时间范围无数据插入！")
        }
        //日期加1天
        today_dt=DateUtils.addOneDay(today_dt)
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

    UPSQL_JDBC.delete("dm_user_card_level","report_dt",start_dt,end_dt)
    var today_dt=start_dt

    if(interval>0 ){
      sqlContext.sql(s"use $hive_dbname")
      for(i <- 0 to interval){

        val results = sqlContext.sql(
          s"""
             |select
             |tempa.card_lvl as card_level,
             |'$today_dt' as report_dt,
             |tempa.tpre   as   effect_tpre_add_num  ,
             |tempa.years  as   effect_year_add_num  ,
             |tempa.total  as   effect_totle_add_num ,
             |tempb.tpre   as   deal_tpre_add_num    ,
             |tempb.years  as   deal_year_add_num    ,
             |tempb.total  as   deal_totle_add_num
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
             |
      """.stripMargin)

        println(s"###JOB_DM_7------$today_dt results:"+results.count())
        if(!Option(results).isEmpty){
          results.save2Mysql("dm_user_card_level")
        }else{
          println("指定的时间范围无数据插入！")
        }
        today_dt=DateUtils.addOneDay(today_dt)
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

    UPSQL_JDBC.delete("dm_store_input_branch","report_dt",start_dt,end_dt)
    var today_dt=start_dt

    if(interval>0 ){
      sqlContext.sql(s"use $hive_dbname")
      for(i <- 0 to interval){

        val results = sqlContext.sql(
          s"""
             |SELECT
             |a.CUP_BRANCH_INS_ID_NM as INPUT_BRANCH,
             |'$today_dt' as REPORT_DT,
             |a.tpre   as   STORE_TPRE_ADD_NUM  ,
             |a.years  as   STORE_YEAR_ADD_NUM  ,
             |a.total  as   STORE_TOTLE_ADD_NUM ,
             |c.tpre   as   ACTIVE_TPRE_ADD_NUM ,
             |c.years  as   ACTIVE_YEAR_ADD_NUM ,
             |c.total  as   ACTIVE_TOTLE_ADD_NUM,
             |d.tpre   as   COUPON_TPRE_ADD_NUM ,
             |d.years  as   COUPON_YEAR_ADD_NUM ,
             |d.total  as   COUPON_TOTLE_ADD_NUM
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
             |
      """.stripMargin)

        println(s"###JOB_DM_8------$today_dt results:"+results.count())
        if(!Option(results).isEmpty){
          results.save2Mysql("dm_store_input_branch")
        }else{
          println("指定的时间范围无数据插入！")
        }
        today_dt=DateUtils.addOneDay(today_dt)
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
    UPSQL_JDBC.delete(s"DM_STORE_DOMAIN_BRANCH_COMPANY","REPORT_DT",start_dt,end_dt)
    var today_dt=start_dt
    if(interval>0 ){
      sqlContext.sql(s"use $hive_dbname")
      for(i <- 0 to interval){
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
        println(s"###JOB_DM_9------$today_dt results:"+results.count())
        if(!Option(results).isEmpty){
          results.save2Mysql("DM_STORE_DOMAIN_BRANCH_COMPANY")
        }else{
          println("指定的时间范围无数据插入！")
        }
        today_dt=DateUtils.addOneDay(today_dt)
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
    println("###JOB_DM_10###")
    UPSQL_JDBC.delete(s"DM_STORE_DIRECT_CONTACT_TRAN","REPORT_DT",start_dt,end_dt)
    var today_dt=start_dt
    if(interval>0 ){
      sqlContext.sql(s"use $hive_dbname")
      for(i <- 0 to interval){
        val results = sqlContext.sql(
          s"""
             |select
             |tmp.project_name,
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
             |HIVE_ACC_TRANS trans
             |
             |left join
             |
             |HIVE_INS_INF ins
             |on trans.acpt_ins_id_cd=ins.ins_id_cd
             |where trans.FWD_INS_ID_CD in ('00097310','00093600','00095210','00098700','00098500','00097700',
             |'00096400','00096500','00155800','00095840','00097000','00085500','00096900','00093930',
             |'00094200','00093900','00096100','00092210','00092220','00092900','00091600','00092400',
             |'00098800','00098200','00097900','00091900','00092600','00091200','00093320','00031000',
             |'00094500','00094900','00091100','00094520','00093000','00093310')
             |
             |group by
             |case when trans.FWD_INS_ID_CD in ('00097310','00093600','00095210','00098700','00098500','00097700',
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
             |when trans.internal_trans_tp='C20022' then '2.0 规范' else '--' end as PROJECT_NAME ,
             |count(distinct(case when to_date(trans.MSG_SETTLE_DT)='$today_dt' then MCHNT_CD end)) as tpre,
             |count(distinct(case when to_date(trans.MSG_SETTLE_DT)>=trunc(to_date(trans.MSG_SETTLE_DT),"YYYY") and to_date(trans.MSG_SETTLE_DT)<='$today_dt' then MCHNT_CD end)) as years,
             |count(distinct(case when to_date(trans.MSG_SETTLE_DT)<='$today_dt' then MCHNT_CD end)) as total
             |from
             |HIVE_ACC_TRANS trans
             |left join
             |HIVE_INS_INF ins
             |on trans.acpt_ins_id_cd=ins.ins_id_cd
             |where internal_trans_tp in ('C00022','C20022')
             |group by case when trans.internal_trans_tp='C00022' then '1.0 规范'
             |when trans.internal_trans_tp='C20022' then '2.0 规范' else '--' end
             |
             |
             |union all
             |
             |select
             |case when trans.internal_trans_tp='C00023' then '终端不改造' else '终端改造' end as PROJECT_NAME,
             |count(distinct(case when to_date(trans.MSG_SETTLE_DT)='$today_dt' then MCHNT_CD end)) as tpre,
             |count(distinct(case when to_date(trans.MSG_SETTLE_DT)>=trunc(to_date(trans.MSG_SETTLE_DT),"YYYY") and to_date(trans.MSG_SETTLE_DT)<='$today_dt' then MCHNT_CD end)) as years,
             |count(distinct(case when to_date(trans.MSG_SETTLE_DT)<='$today_dt' then MCHNT_CD end)) as total
             |from
             |HIVE_ACC_TRANS trans
             |left join
             |HIVE_INS_INF ins
             |on trans.acpt_ins_id_cd=ins.ins_id_cd
             |where internal_trans_tp in ('C00022','C20022','C00023')
             |group by case when trans.internal_trans_tp='C00023' then '终端不改造' else '终端改造' end
             |) tmp
             |
             |
             |
             | """.stripMargin)
        println(s"###JOB_DM_10------$today_dt results:"+results.count())
        if(!Option(results).isEmpty){
          results.save2Mysql("DM_STORE_DOMAIN_BRANCH_COMPANY")
        }else{
          println("指定的时间范围无数据插入！")
        }
        today_dt=DateUtils.addOneDay(today_dt)
      }
    }
  }


  /**
    * JOB_DM_11/11-7
    * DM_DEVELOPMENT_ORG_CLASS
    * @author tzq
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    * @param interval
    */
  def JOB_DM_11 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("###JOB_DM_11###")
    UPSQL_JDBC.delete(s"DM_DEVELOPMENT_ORG_CLASS","REPORT_DT",start_dt,end_dt)
    var today_dt=start_dt
    if(interval>0 ){
      sqlContext.sql(s"use $hive_dbname")
      for(i <- 0 to interval){
        val results = sqlContext.sql(
          s"""
             |select
             |(case when a.extend_ins_id_cd_class is null then nvl(b.extend_ins_id_cd_class,c.extend_ins_id_cd_class) else  nvl(a.extend_ins_id_cd_class,'其它') end) as ORG_CLASS,
             |'$today_dt'              as    REPORT_DT         ,
             |sum(c.extend_ins_tpre)    as    EXTEND_INS_TPRE   ,
             |sum(c.extend_ins_years)   as    EXTEND_INS_YEARS  ,
             |sum(c.extend_ins_total)   as    EXTEND_INS_TOTAL  ,
             |sum(a.brand_tpre)         as    BRAND_TPRE        ,
             |sum(a.brand_years)        as    BRAND_YEARS       ,
             |sum(a.brand_total)        as    BRAND_TOTAL       ,
             |sum(a.store_tpre )        as    STORE_TPRE        ,
             |sum(a.store_years)        as    STORE_YEARS       ,
             |sum(a.store_total)        as    STORE_TOTAL       ,
             |sum(b.active_store_tpre)  as    ACTIVE_STORE_TPRE ,
             |sum(b.active_store_years) as    ACTIVE_STORE_YEARS,
             |sum(b.active_store_total) as    ACTIVE_STORE_TOTAL,
             |sum(b.tran_store_tpre)    as    TRAN_STORE_TPRE   ,
             |sum(b.tran_store_years)   as    TRAN_STORE_YEARS  ,
             |sum(b.tran_store_total)   as    TRAN_STORE_TOTAL
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
             |when substr(t0.EXTEND_INS_ID_CD,1,4)>='0100' and substr(t0.EXTEND_INS_ID_CD,1,4)<='0599' then '银行'
             |when substr(t0.EXTEND_INS_ID_CD,1,4)>='1400' and substr(t0.EXTEND_INS_ID_CD,1,4)<='1699' or (substr(t0.EXTEND_INS_ID_CD,1,4)>='4800' and substr(t0.EXTEND_INS_ID_CD,1,4)<='4999' and substr(t0.EXTEND_INS_ID_CD,1,4)<>'4802') then '非金机构'
             |when substr(t0.EXTEND_INS_ID_CD,1,4) = '4802' then '银商收单'
             |when substr(t0.EXTEND_INS_ID_CD,1,4) in ('4990','4991','8804') or substr(t0.EXTEND_INS_ID_CD,1,1) in ('c','C') then '第三方机构'
             |else t0.EXTEND_INS_ID_CD end ) as extend_ins_id_cd_class,
             |t0.sto_num,t0.brand_num,t0.brand_ts ,t0.pre_ts
             |from
             |(select bas.EXTEND_INS_ID_CD,count(distinct pre.mchnt_cd) as sto_num,count(distinct pre.brand_id) as brand_num ,brand.rec_crt_ts as brand_ts ,pre.rec_crt_ts as pre_ts
             |from HIVE_ACCESS_BAS_INF bas
             |left join HIVE_PREFERENTIAL_MCHNT_INF pre
             |on bas.ch_ins_id_cd=pre.mchnt_cd
             |left join HIVE_BRAND_INF brand on brand.brand_id=pre.brand_id
             |where
             |( substr(bas.EXTEND_INS_ID_CD,1,4) <'0000' or substr(bas.EXTEND_INS_ID_CD,1,4) >'0099' )
             |and extend_ins_id_cd<>''
             |and pre.mchnt_st='2' and
             |pre.rec_crt_ts>='2015-01-01-00.00.00.000000'
             |and brand.rec_crt_ts>='2015-01-01-00.00.00.000000'
             |group by bas.EXTEND_INS_ID_CD,brand.rec_crt_ts,pre.rec_crt_ts) t0
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
             |when substr(t1.EXTEND_INS_ID_CD,1,4) >= '0100' and substr(t1.EXTEND_INS_ID_CD,1,4) <='0599' then '银行'
             |when substr(t1.EXTEND_INS_ID_CD,1,4) >= '1400' and substr(t1.EXTEND_INS_ID_CD,1,4) <= '1699' or (substr(t1.EXTEND_INS_ID_CD,1,4) >= '4800' and substr(t1.EXTEND_INS_ID_CD,1,4) <='4999' and substr(t1.EXTEND_INS_ID_CD,1,4)<>'4802') then '非金机构'
             |when substr(t1.EXTEND_INS_ID_CD,1,4) = '4802' then '银商收单'
             |when substr(t1.EXTEND_INS_ID_CD,1,4) in ('4990','4991','8804') or substr(t1.EXTEND_INS_ID_CD,1,1) in ('c','C') then '第三方机构'
             |else t1.EXTEND_INS_ID_CD end ) as extend_ins_id_cd_class,
             |t1.sto_num,t1.brand_num,t1.brand_ts ,t1.pre_ts
             |from
             |(select
             |bas.EXTEND_INS_ID_CD,
             |count(distinct pre.mchnt_cd) as sto_num,
             |count(distinct pre.brand_id) as brand_num ,
             |brand.rec_crt_ts as brand_ts ,
             |pre.rec_crt_ts as pre_ts
             |from HIVE_ACCESS_BAS_INF bas
             |left join HIVE_PREFERENTIAL_MCHNT_INF pre
             |on bas.ch_ins_id_cd=pre.mchnt_cd
             |left join HIVE_BRAND_INF brand
             |on brand.brand_id=pre.brand_id
             |where
             |( substr(bas.EXTEND_INS_ID_CD,1,4) <'0000' or substr(bas.EXTEND_INS_ID_CD,1,4) >'0099' )
             |and length(trim(extend_ins_id_cd))<>0
             |and pre.mchnt_st='2'
             |group by bas.EXTEND_INS_ID_CD,brand.rec_crt_ts,pre.rec_crt_ts) t1
             |) tmp1 group by extend_ins_id_cd_class ) b
             |on a.extend_ins_id_cd_class=b.extend_ins_id_cd_class
             |full outer join
             |(
             |select extend_ins_id_cd_class,
             |count(case when to_date(ENTRY_TS)='$today_dt' then extend_ins_id_cd_class end) as extend_ins_tpre,
             |count(case when to_date(ENTRY_TS)>=trunc('$today_dt','YYYY') and to_date(ENTRY_TS)<='$today_dt' then extend_ins_id_cd_class end) as extend_ins_years,
             |count(case when to_date(ENTRY_TS)<='$today_dt' then extend_ins_id_cd_class end) as extend_ins_total
             |from (
             |select
             |(case
             |when substr(EXTEND_INS_ID_CD,1,4) >= '0100' and substr(EXTEND_INS_ID_CD,1,4) <='0599' then '银行'
             |when substr(EXTEND_INS_ID_CD,1,4) >= '1400' and substr(EXTEND_INS_ID_CD,1,4) <= '1699' or (substr(EXTEND_INS_ID_CD,1,4) >= '4800' and substr(EXTEND_INS_ID_CD,1,4) <='4999' and substr(EXTEND_INS_ID_CD,1,4)<>'4802') then '非金机构'
             |when substr(EXTEND_INS_ID_CD,1,4) = '4802' then '银商收单'
             |when substr(EXTEND_INS_ID_CD,1,4) in ('4990','4991','8804') or substr(EXTEND_INS_ID_CD,1,1) in ('c','C') then '第三方机构'
             |else EXTEND_INS_ID_CD end ) as extend_ins_id_cd_class,EXTEND_INS_ID_CD,min(ENTRY_TS) as ENTRY_TS
             |from HIVE_ACCESS_BAS_INF
             |group by (case
             |when substr(EXTEND_INS_ID_CD,1,4) >= '0100' and substr(EXTEND_INS_ID_CD,1,4) <='0599' then '银行'
             |when substr(EXTEND_INS_ID_CD,1,4) >= '1400' and substr(EXTEND_INS_ID_CD,1,4) <= '1699' or (substr(EXTEND_INS_ID_CD,1,4) >= '4800' and substr(EXTEND_INS_ID_CD,1,4) <='4999' and substr(EXTEND_INS_ID_CD,1,4)<>'4802') then '非金机构'
             |when substr(EXTEND_INS_ID_CD,1,4) = '4802' then '银商收单'
             |when substr(EXTEND_INS_ID_CD,1,4) in ('4990','4991','8804') or substr(EXTEND_INS_ID_CD,1,1) in ('c','C') then '第三方机构'
             |else EXTEND_INS_ID_CD end ),EXTEND_INS_ID_CD
             |having min(ENTRY_TS) >='2015-01-01-00.00.00.000000'
             |) tmp3
             |group by extend_ins_id_cd_class) c
             |on a.extend_ins_id_cd_class=c.extend_ins_id_cd_class
             |group by (case when a.extend_ins_id_cd_class is null then nvl(b.extend_ins_id_cd_class,c.extend_ins_id_cd_class) else  nvl(a.extend_ins_id_cd_class,'其它') end)
             |
             | """.stripMargin)
        println(s"###JOB_DM_11------$today_dt results:"+results.count())
        if(!Option(results).isEmpty){
          results.save2Mysql("dm_development_org_class")
        }else{
          println("指定的时间范围无数据插入！")
        }
        today_dt=DateUtils.addOneDay(today_dt)
      }
    }
  }


  /**
    * JOB_DM_12/12-6
    * DM_COUPON_PUB_DOWN_BRANCH
    * @author tzq
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    * @param interval
    */
  def JOB_DM_12 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("###JOB_DM_12(DM_COUPON_PUB_DOWN_BRANCH)### "+DateUtils.getCurrentSystemTime())
    DateUtils.timeCost("JOB_DM_12"){
      UPSQL_JDBC.delete(s"DM_COUPON_PUB_DOWN_BRANCH","REPORT_DT",start_dt,end_dt)
      var today_dt=start_dt
      if(interval>=0 ){
        sqlContext.sql(s"use $hive_dbname")
        for(i <- 0 to interval){
          val results =sqlContext.sql(
            s"""
               |select
               |a.CUP_BRANCH_INS_ID_NM  as COUPON_BRANCH,
               |'$today_dt'             as REPORT_DT,
               |a.coupon_class          as CLASS_TPRE_ADD_NUM,
               |a.coupon_publish        as AMT_TPRE_ADD_NUM,
               |a.coupon_down           as DOWM_TPRE_ADD_NUM,
               |b.batch                 as BATCH_TPRE_ADD_NUM
               |from
               |(
               |select CUP_BRANCH_INS_ID_NM,
               |count(*) as coupon_class ,
               |sum(case when dwn_total_num = -1 then dwn_num else dwn_total_num end) as coupon_publish ,
               |sum(dwn_num) as coupon_down
               |FROM HIVE_TICKET_BILL_BAS_INF bill
               |where bill_nm not like '%测试%' and bill_nm not like '%验证%' and bill_id <>'Z00000000020415'
               |and bill_id<>'Z00000000020878' and bill_nm not like '%满2元减1%' and bill_nm not like '%满2分减1分%'
               |and bill_nm not like '%满2减1%' and bill_nm not like '%满2抵1%' and bill_nm not like '测%' and dwn_total_num-dwn_num<100000
               |and to_date(rec_crt_ts)='$today_dt'
               |group by CUP_BRANCH_INS_ID_NM
               |) a
               |
               |left join
               |(
               |select CUP_BRANCH_INS_ID_NM,
               |sum(adj.ADJ_TICKET_BILL) as batch
               |from
               |HIVE_TICKET_BILL_ACCT_ADJ_TASK adj
               |inner join
               |HIVE_TICKET_BILL_BAS_INF bill
               |on adj.bill_id=bill.bill_id
               |where bill_nm not like '%测试%' and bill_nm not like '%验证%' and bill.bill_id <>'Z00000000020415'
               |and bill.bill_id<>'Z00000000020878' and bill_nm not like '%满2元减1%' and bill_nm not like '%满2分减1分%'
               |and bill_nm not like '%满2减1%' and bill_nm not like '%满2抵1%' and bill_nm not like '测%' and dwn_total_num-dwn_num<100000
               |and usr_tp='1' and to_date(adj.rec_crt_ts)='$today_dt'
               |and current_st='1'
               |group by CUP_BRANCH_INS_ID_NM
               |) b
               |on a.CUP_BRANCH_INS_ID_NM=b.CUP_BRANCH_INS_ID_NM
               |
          """.stripMargin)
          println(s"###JOB_DM_12------$today_dt results:"+results.count())
          if(!Option(results).isEmpty){
            results.save2Mysql("DM_COUPON_PUB_DOWN_BRANCH")
          }else{
            println("No data insert!")
          }
          today_dt=DateUtils.addOneDay(today_dt)
        }
      }
    }
  }
  /**
    * JOB_DM_13/12-6
    * DM_COUPON_PUB_DOWN_IF_ICCARD
    * @author tzq
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    * @param interval
    */
  def JOB_DM_13 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("###JOB_DM_13(DM_COUPON_PUB_DOWN_IF_ICCARD)### "+DateUtils.getCurrentSystemTime())
    DateUtils.timeCost("JOB_DM_13"){
      UPSQL_JDBC.delete(s"DM_COUPON_PUB_DOWN_IF_ICCARD","REPORT_DT",start_dt,end_dt)
      var today_dt=start_dt
      if(interval>=0 ){
        sqlContext.sql(s"use $hive_dbname")
        for(i <- 0 to interval){
          val results =sqlContext.sql(
            s"""
               |select
               |a.IF_ICCARD       as IF_ICCARD,
               |'$today_dt'       as REPORT_DT,
               |a.coupon_class    as CLASS_TPRE_ADD_NUM,
               |a.coupon_publish  as AMT_TPRE_ADD_NUM,
               |a.dwn_num         as DOWM_TPRE_ADD_NUM,
               |b.batch           as BATCH_TPRE_ADD_NUM
               |from
               |(
               |select
               |CASE WHEN pos_entry_md_cd in ('01','05','07','95','98') THEN '仅限IC卡' ELSE '非仅限IC卡' END AS IF_ICCARD,
               |count(*) as coupon_class ,
               |sum(case when dwn_total_num = -1 then dwn_num else dwn_total_num end) as coupon_publish ,
               |sum(dwn_num) as dwn_num
               |from HIVE_DOWNLOAD_TRANS as dtl,HIVE_TICKET_BILL_BAS_INF as bill
               |where dtl.bill_id=bill.bill_id
               |and dtl.um_trans_id in ('12','17')
               |and dtl.trans_st='1' and trans_dt='$today_dt' and dtl.bill_nm not like '%机场%' and dtl.bill_nm not like '%住两晚送一晚%'
               |and bill.bill_sub_tp in ('01','03') and bill.bill_nm not like '%测试%' and bill.bill_nm not like '%验证%' and bill.bill_id <>'Z00000000020415'
               |and bill.bill_id<>'Z00000000020878' and bill.bill_nm not like '%满2元减1%' and bill.bill_nm not like '%满2分减1分%'
               |and bill.bill_nm not like '%满2减1%' and bill.bill_nm not like '%满2抵1%' and bill.bill_nm not like '测%' and dwn_total_num-dwn_num<100000
               |group by CASE WHEN pos_entry_md_cd in ('01','05','07','95','98') THEN '仅限IC卡' ELSE '非仅限IC卡' END
               |) a
               |left join
               |(
               |select
               |CASE WHEN pos_entry_md_cd in ('01','05','07','95','98') THEN '仅限IC卡' ELSE '非仅限IC卡' END AS IF_ICCARD,
               |sum(adj.ADJ_TICKET_BILL) as batch
               |from
               |HIVE_TICKET_BILL_ACCT_ADJ_TASK adj
               |inner join
               |(
               |select bill.CUP_BRANCH_INS_ID_CD,dtl.bill_id,pos_entry_md_cd
               |from HIVE_DOWNLOAD_TRANS as dtl,HIVE_TICKET_BILL_BAS_INF as bill
               |where dtl.bill_id=bill.bill_id
               |and dtl.um_trans_id in ('12','17')
               |and dtl.trans_st='1' and trans_dt='$today_dt' and dtl.bill_nm not like '%机场%' and dtl.bill_nm not like '%住两晚送一晚%'
               |and bill.bill_sub_tp in ('01','03') and bill.bill_nm not like '%测试%' and bill.bill_nm not like '%验证%' and bill.bill_id <>'Z00000000020415'
               |and bill.bill_id<>'Z00000000020878' and bill.bill_nm not like '%满2元减1%' and bill.bill_nm not like '%满2分减1分%'
               |and bill.bill_nm not like '%满2减1%' and bill.bill_nm not like '%满2抵1%' and bill.bill_nm not like '测%' and dwn_total_num-dwn_num<100000
               |) b
               |on adj.bill_id=b.bill_id
               |group by CASE WHEN pos_entry_md_cd in ('01','05','07','95','98') THEN '仅限IC卡' ELSE '非仅限IC卡' END )b
               |on a.IF_ICCARD=b.IF_ICCARD
               |
          """.stripMargin)
          println(s"###JOB_DM_13------$today_dt results:"+results.count())
          if(!Option(results).isEmpty){
            results.save2Mysql("DM_COUPON_PUB_DOWN_IF_ICCARD")
          }else{
            println("No data insert!")
          }
          today_dt=DateUtils.addOneDay(today_dt)
        }
      }
    }
  }

  /**
    * JOB_DM_14/12-6
    * DM_COUPON_SHIPP_DELIVER_MERCHANT
    * @author tzq
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    * @param interval
    */
  def JOB_DM_14 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("###JOB_DM_14(DM_COUPON_SHIPP_DELIVER_MERCHANT)### "+DateUtils.getCurrentSystemTime())
    DateUtils.timeCost("JOB_DM_14"){
      UPSQL_JDBC.delete(s"DM_COUPON_SHIPP_DELIVER_MERCHANT","REPORT_DT",start_dt,end_dt)
      var today_dt=start_dt
      if(interval>=0 ){
        sqlContext.sql(s"use $hive_dbname")
        for(i <- 0 to interval){
          val results =sqlContext.sql(
            s"""
               |select
               |mchnt_nm                                   as MERCHANT_NM,
               |bill_sub_tp                                as BILL_TP,
               |'$today_dt'                                as REPORT_DT,
               |sum(trans_num)                             as DEAL_NUM,
               |sum(trans_succ_num)                        as SUCC_DEAL_NUM,
               |sum(trans_succ_num)/sum(trans_num)*100     as DEAL_SUCC_RATE,
               |sum(trans_amt)                             as DEAL_AMT,
               |sum(del_usr_num)                           as DEAL_USR_NUM,
               |sum(card_num)                              as DEAL_CARD_NUM
               |from(
               |select
               |dtl.mchnt_nm,
               |bill.bill_sub_tp,
               |0 as trans_num,
               |count(distinct dtl.trans_seq) as trans_succ_num,
               |sum(dtl.trans_at) as trans_amt,
               |count(distinct dtl.cdhd_usr_id) as del_usr_num,
               |count(distinct dtl.card_no) as card_num
               |from HIVE_BILL_ORDER_TRANS as dtl,
               |HIVE_BILL_SUB_ORDER_TRANS as sub_dtl,
               |HIVE_TICKET_BILL_BAS_INF as bill
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
               |from HIVE_BILL_ORDER_TRANS as dtl,
               |HIVE_BILL_SUB_ORDER_TRANS as sub_dtl,
               |HIVE_TICKET_BILL_BAS_INF as bill
               |where dtl.bill_order_id=sub_dtl.bill_order_id
               |and sub_dtl.bill_id=bill.bill_id
               |and dtl.order_st<>'00' and dtl.trans_dt='$today_dt'
               |and bill.bill_sub_tp in ('04','07','08')
               |group by dtl.mchnt_nm,bill.bill_sub_tp
               |) a
               |group by mchnt_nm,bill_sub_tp
               |
          """.stripMargin)
          println(s"###JOB_DM_14------$today_dt results:"+results.count())
          if(!Option(results).isEmpty){
            results.save2Mysql("DM_COUPON_SHIPP_DELIVER_MERCHANT")
          }else{
            println("No data insert!")
          }
          today_dt=DateUtils.addOneDay(today_dt)
        }
      }
    }
  }

  /**
    * JOB_DM_15/12-6
    * DM_COUPON_SHIPP_DELIVER_ISS
    * @author tzq
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    * @param interval
    */
  def JOB_DM_15 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("###JOB_DM_15(DM_COUPON_SHIPP_DELIVER_ISS)### "+DateUtils.getCurrentSystemTime())
    DateUtils.timeCost("JOB_DM_15"){
      UPSQL_JDBC.delete(s"DM_COUPON_SHIPP_DELIVER_ISS","REPORT_DT",start_dt,end_dt)
      var today_dt=start_dt
      if(interval>=0 ){
        sqlContext.sql(s"use $hive_dbname")
        for(i <- 0 to interval){
          val results =sqlContext.sql(
            s"""
               |select
               |iss_ins_cn_nm                          as  CARD_ISS,
               |bill_sub_tp                            as  BILL_TP,
               |'$today_dt'                            as  REPORT_DT,
               |sum(trans_num)                         as  DEAL_NUM,
               |sum(trans_succ_num)                    as  SUCC_DEAL_NUM,
               |sum(trans_succ_num)/sum(trans_num)*100 as  DEAL_SUCC_RATE,
               |sum(trans_amt)                         as  DEAL_AMT,
               |sum(del_usr_num)                       as  DEAL_USR_NUM,
               |sum(card_num)                          as  DEAL_CARD_NUM
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
               |HIVE_BILL_ORDER_TRANS as dtl,
               |HIVE_BILL_SUB_ORDER_TRANS as  sub_dtl,
               |HIVE_TICKET_BILL_BAS_INF as bill,
               |HIVE_CARD_BIND_INF as card
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
               |from HIVE_BILL_ORDER_TRANS as dtl,
               |HIVE_BILL_SUB_ORDER_TRANS as sub_dtl,
               |HIVE_TICKET_BILL_BAS_INF as bill,
               |HIVE_CARD_BIND_INF as card
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
          println(s"###JOB_DM_15------$today_dt results:"+results.count())
          if(!Option(results).isEmpty){
            results.save2Mysql("DM_COUPON_SHIPP_DELIVER_ISS")
          }else{
            println("No data insert!")
          }
          today_dt=DateUtils.addOneDay(today_dt)
        }
      }
    }
  }

  /**
    * JOB_DM_16/12-7
    * DM_COUPON_SHIPP_DELIVER_BRANCH
    * @author tzq
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    * @param interval
    */
  def JOB_DM_16 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("###JOB_DM_16(DM_COUPON_SHIPP_DELIVER_BRANCH)### "+DateUtils.getCurrentSystemTime())
    DateUtils.timeCost("JOB_DM_16"){
      UPSQL_JDBC.delete(s"DM_COUPON_SHIPP_DELIVER_BRANCH","REPORT_DT",start_dt,end_dt)
      var today_dt=start_dt
      if(interval>=0 ){
        sqlContext.sql(s"use $hive_dbname")
        for(i <- 0 to interval){
          val results =sqlContext.sql(
            s"""
               |select
               |CUP_BRANCH_INS_ID_NM                     AS BRANCH_NM     ,
               |bill_sub_tp                              AS BILL_TP       ,
               |'$today_dt'                              AS REPORT_DT     ,
               |sum(trans_num)                           AS DEAL_NUM      ,
               |sum(trans_succ_num)                      AS SUCC_DEAL_NUM ,
               |sum(trans_succ_num)/sum(trans_num)*100   AS DEAL_SUCC_RATE,
               |sum(trans_amt)                           AS DEAL_AMT      ,
               |sum(del_usr_num)                         AS DEAL_USR_NUM  ,
               |sum(card_num)                            AS DEAL_CARD_NUM
               |from(
               |select
               |bill.CUP_BRANCH_INS_ID_NM,
               |bill.bill_sub_tp,
               |0  as trans_num,
               |count(distinct dtl.trans_seq) as trans_succ_num,
               |sum(dtl.trans_at) as trans_amt,
               |count(distinct dtl.cdhd_usr_id)  as del_usr_num,
               |count(distinct dtl.card_no) as card_num
               |from HIVE_BILL_ORDER_TRANS as dtl,
               |HIVE_BILL_SUB_ORDER_TRANS as sub_dtl,
               |HIVE_TICKET_BILL_BAS_INF as bill
               |where dtl.bill_order_id=sub_dtl.bill_order_id
               |and sub_dtl.bill_id=bill.bill_id
               |and dtl.order_st='00'and dtl.trans_dt='$today_dt'
               |and  bill.bill_sub_tp in ('04','07','08')
               |group by bill.CUP_BRANCH_INS_ID_NM,bill.bill_sub_tp
               |
               |union all
               |select
               |bill.CUP_BRANCH_INS_ID_NM,
               |bill.bill_sub_tp,
               |count(distinct dtl.trans_seq) as trans_num,
               |0 as trans_succ_num ,
               |0 as trans_amt ,
               |0 as del_usr_num ,
               |0 as card_num
               |from HIVE_BILL_ORDER_TRANS as dtl,
               |HIVE_BILL_SUB_ORDER_TRANS as sub_dtl,
               |HIVE_TICKET_BILL_BAS_INF as bill
               |where dtl.bill_order_id=sub_dtl.bill_order_id
               |and sub_dtl.bill_id=bill.bill_id
               |and dtl.order_st<>'00'and dtl.trans_dt='$today_dt'
               |and  bill.bill_sub_tp in ('04','07','08')
               |group by  bill.CUP_BRANCH_INS_ID_NM,bill.bill_sub_tp
               |) a
               |group by CUP_BRANCH_INS_ID_NM,bill_sub_tp
               |
          """.stripMargin)
          println(s"###JOB_DM_16------$today_dt results:"+results.count())
          if(!Option(results).isEmpty){
            results.save2Mysql("DM_COUPON_SHIPP_DELIVER_BRANCH")
          }else{
            println("No data insert!")
          }
          today_dt=DateUtils.addOneDay(today_dt)
        }
      }
    }
  }

  /**
    * JOB_DM_17/12-9
    * DM_COUPON_SHIPP_DELIVER_PHOME_AREA
    * @author tzq
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    * @param interval
    */
  def JOB_DM_17 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("###JOB_DM_17(DM_COUPON_SHIPP_DELIVER_PHOME_AREA)### "+DateUtils.getCurrentSystemTime())
    DateUtils.timeCost("JOB_DM_17"){
      UPSQL_JDBC.delete(s"DM_COUPON_SHIPP_DELIVER_PHOME_AREA","REPORT_DT",start_dt,end_dt)
      var today_dt=start_dt
      if(interval>=0 ){
        sqlContext.sql(s"use $hive_dbname")
        for(i <- 0 to interval){
          val results =sqlContext.sql(
            s"""
               |select
               |PHONE_LOCATION as PHONE_NM ,
               |bill_sub_tp as BILL_TP ,
               |'$today_dt' as REPORT_DT ,
               |sum(trans_num) as DEAL_NUM ,
               |sum(trans_succ_num) as SUCC_DEAL_NUM ,
               |sum(trans_succ_num)/sum(trans_num)*100 as DEAL_SUCC_RATE,
               |sum(trans_amt) as DEAL_AMT ,
               |sum(del_usr_num) as DEAL_USR_NUM ,
               |sum(card_num) as DEAL_CARD_NUM
               |from(
               |select
               |acc.PHONE_LOCATION,
               |bill.bill_sub_tp,
               |0 as trans_num,
               |count(distinct dtl.trans_seq) as trans_succ_num,
               |sum(dtl.trans_at) as trans_amt,
               |count(distinct dtl.cdhd_usr_id) as del_usr_num,
               |count(distinct dtl.card_no) as card_num
               |from HIVE_BILL_ORDER_TRANS as dtl,
               |HIVE_BILL_SUB_ORDER_TRANS as sub_dtl,
               |HIVE_TICKET_BILL_BAS_INF as bill,
               |HIVE_PRI_ACCT_INF as acc
               |where dtl.bill_order_id=sub_dtl.bill_order_id
               |and sub_dtl.bill_id=bill.bill_id
               |and dtl.cdhd_usr_id=acc.cdhd_usr_id
               |and dtl.order_st='00'and dtl.trans_dt='$today_dt'
               |and bill.bill_sub_tp in ('04','07','08')
               |group by acc.PHONE_LOCATION ,bill.bill_sub_tp
               |
               |union all
               |
               |select
               |acc.PHONE_LOCATION,
               |bill.bill_sub_tp,
               |count(distinct dtl.trans_seq) as trans_num,
               |0 as trans_succ_num ,
               |0 as trans_amt ,
               |0 as del_usr_num ,
               |0 as card_num
               |from HIVE_BILL_ORDER_TRANS as dtl,
               |HIVE_BILL_SUB_ORDER_TRANS as sub_dtl,
               |HIVE_TICKET_BILL_BAS_INF as bill,
               |HIVE_PRI_ACCT_INF as acc
               |where dtl.bill_order_id=sub_dtl.bill_order_id
               |and sub_dtl.bill_id=bill.bill_id
               |and dtl.cdhd_usr_id=acc.cdhd_usr_id
               |and dtl.order_st<>'00'and dtl.trans_dt='$today_dt'
               |and bill.bill_sub_tp in ('04','07','08')
               |group by acc.PHONE_LOCATION ,bill.bill_sub_tp
               |) a
               |group by PHONE_LOCATION,bill_sub_tp
               |
          """.stripMargin)
          println(s"###JOB_DM_17------$today_dt results:"+results.count())
          if(!Option(results).isEmpty){
            results.save2Mysql("DM_COUPON_SHIPP_DELIVER_PHOME_AREA")
          }else{
            println("No data insert!")
          }
          today_dt=DateUtils.addOneDay(today_dt)
        }
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
    UPSQL_JDBC.delete(s"DM_VAL_TKT_ACT_MCHNT_TP_DLY","REPORT_DT",s"$start_dt",s"$end_dt")
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

    println(s"###JOB_DM_54------($start_dt-$end_dt) results:"+results.count())
    if(!Option(results).isEmpty){
      results.save2Mysql("DM_VAL_TKT_ACT_MCHNT_TP_DLY")
    }else{
      println("指定的时间范围无数据插入！")
    }
  }


  /**
    *
    * JOB_DM_55  2016-9-6
    *
    * DM_DISC_ACT_BRANCH_DLY->hive_prize_discount_result
    *
    * @author tzq
    * @param sqlContext
    */
  def JOB_DM_55(implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {

    println("###JOB_DM_55-----JOB_DM_55(dm_disc_act_branch_dly->hive_prize_discount_result)")

    UPSQL_JDBC.delete("dm_disc_act_branch_dly","report_dt",start_dt,end_dt);

    sqlContext.sql(s"use $hive_dbname")

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

    if(!Option(results).isEmpty){
      println("###JOB_DM_55------results:"+results.count())
      results.save2Mysql("dm_disc_act_branch_dly")

    }else{
      println("指定的时间范围无数据插入！")
    }
  }


  /**
    * JobName: JOB_DM_61
    * Feature: hive_cdhd_cashier_maktg_reward_dtl -> dm_cashier_cup_red_branch
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
      if (interval > 0) {
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
    * JOB_DM_62  2016-9-6
    * dm_usr_auther_nature_tie_card --> hive_card_bind_inf
    *
    * @author tzq
    * @param sqlContext
    */
  def JOB_DM_62(implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("###JOB_DM_62-----JOB_DM_62(dm_usr_auther_nature_tie_card->hive_card_bind_inf)")

    UPSQL_JDBC.delete("dm_usr_auther_nature_tie_card","report_dt",start_dt,end_dt)

    var today_dt=start_dt
    if(interval>0 ){
      sqlContext.sql(s"use $hive_dbname")
      for(i <- 0 to interval){

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

        println(s"###JOB_DM_62------$today_dt results:"+results.count())

        if(!Option(results).isEmpty){
          results.save2Mysql("dm_usr_auther_nature_tie_card")
        }else{
          println("指定的时间范围无数据插入！")
        }

        //日期加1天
        today_dt=DateUtils.addOneDay(today_dt)
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
    UPSQL_JDBC.delete(s"DM_LIFE_SERVE_BUSINESS_TRANS","REPORT_DT",start_dt,end_dt)
    var today_dt=start_dt
    if(interval>0 ){
      sqlContext.sql(s"use $hive_dbname")
      for(i <- 0 to interval){
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
        println(s"###JOB_DM_63------$today_dt results:"+results.count())
        if(!Option(results).isEmpty){
          results.save2Mysql("DM_LIFE_SERVE_BUSINESS_TRANS")
        }else{
          println("指定的时间范围无数据插入！")
        }
        today_dt=DateUtils.addOneDay(today_dt)
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
    UPSQL_JDBC.delete(s"DM_HCE_COUPON_TRAN","REPORT_DT",start_dt,end_dt)
    var today_dt=start_dt
    if(interval>0 ){
      sqlContext.sql(s"use $hive_dbname")
      for(i <- 0 to interval){
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
        println(s"###JOB_DM_65------$today_dt results:"+results.count())
        if(!Option(results).isEmpty){
          results.save2Mysql("DM_HCE_COUPON_TRAN")
        }else{
          println("指定的时间范围无数据插入！")
        }
        today_dt=DateUtils.addOneDay(today_dt)
      }
    }
  }

  /**
    * JOB_DM_66 2016-09-07
    *
    * dm_coupon_cfp_tran->hive_acc_trans+hive_ticket_bill_bas_inf
    *
    * @author tzq
    * @param sqlContext
    */
  def JOB_DM_66(implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("###JOB_DM_66(dm_coupon_cfp_tran->hive_acc_trans+hive_ticket_bill_bas_inf)")

    UPSQL_JDBC.delete("dm_coupon_cfp_tran","report_dt",start_dt,end_dt)

    var today_dt=start_dt
    if(interval>0 ){
      sqlContext.sql(s"use $hive_dbname")
      for(i <- 0 to interval){

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

        println(s"###JOB_DM_66------$today_dt results:"+results.count())

        if(!Option(results).isEmpty){
          results.save2Mysql("dm_coupon_cfp_tran")
        }else{
          println("指定的日期无数据插入！")
        }
        today_dt=DateUtils.addOneDay(today_dt)
      }
    }

  }


  /**
    * JobName: JOB_DM_67
    * Feature: hive_acc_trans,hive_offline_point_trans,hive_passive_code_pay_trans,hive_download_trans,
    *          hive_switch_point_trans,hive_prize_discount_result,hive_discount_bas_inf
    *          -> dm_o2o_trans_dly
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
           |    trans_dt as report_dt,
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
           |  trans.part_trans_dt >='$start_dt' and trans.part_trans_dt >='$end_dt' and
           |   oper_st in('0','3')
           |group by
           |    cup_branch_ins_id_nm,
           |    trans_dt
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
    * JOB_DM_69  2016-9-1
    * dm_disc_tkt_act_dly->hive_ticket_bill_bas_inf+hive_acc_trans
    * (使用分区part_trans_dt 中的数据)
    *
    * @author tzq
    * @param sqlContext
    */
  def JOB_DM_69(implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {
    println("###JOB_DM_69-----JOB_DM_69(dm_disc_tkt_act_dly->hive_ticket_bill_bas_inf+hive_acc_trans)")

    UPSQL_JDBC.delete("dm_disc_tkt_act_dly","report_dt",start_dt,end_dt);

    sqlContext.sql(s"use $hive_dbname")

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
    println("###JOB_DM_69------results:"+results.count())
    if(!Option(results).isEmpty){
      results.save2Mysql("dm_disc_tkt_act_dly")
    }else{
      println("指定的时间范围无数据插入！")
    }
  }

  /**
    * JOB_DM_70  2016-8-30
    * dm_elec_tkt_act_dly->hive_ticket_bill_bas_inf+hive_acc_trans
    *
    * @author tzq
    * @param sqlContext
    */
  def JOB_DM_70(implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {
    println("###JOB_DM_70-----JOB_DM_70(dm_elec_tkt_act_dly->hive_ticket_bill_bas_inf+hive_acc_trans)")

    UPSQL_JDBC.delete("dm_elec_tkt_act_dly","report_dt",start_dt,end_dt);

    sqlContext.sql(s"use $hive_dbname")

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
    println("###JOB_DM_70------results:"+results.count())
    if(!Option(results).isEmpty){
      results.save2Mysql("dm_elec_tkt_act_dly")
    }else{
      println("指定的时间范围无数据插入！")
    }

  }

  /**
    * JOB_DM_71  2016-8-31
    * dm_vchr_tkt_act_dly->hive_ticket_bill_bas_inf+hive_acc_trans
    *
    * @author tzq
    * @param sqlContext
    */
  def JOB_DM_71(implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {

    println("###JOB_DM_71-----JOB_DM_71(dm_vchr_tkt_act_dly->hive_ticket_bill_bas_inf+hive_acc_trans)")

    UPSQL_JDBC.delete("dm_vchr_tkt_act_dly","report_dt",start_dt,end_dt);

    sqlContext.sql(s"use $hive_dbname")
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
    println("###JOB_DM_71------results:"+results.count())
    if(!Option(results).isEmpty){
      results.save2Mysql("dm_vchr_tkt_act_dly")
    }else{
      println("指定的时间范围无数据插入！")
    }
  }


  /**
    * JobName: JOB_DM_72
    * Feature: hive_offline_point_trans -> dm_offline_point_act_dly
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
    * DM_PRIZE_ACT_BRANCH_DLY->HIVE_PRIZE_ACTIVITY_BAS_INF,HIVE_PRIZE_LVL,HIVE_PRIZE_DISCOUNT_RESULT
    * Code by Xue
    *
    * @param sqlContext
    * @return
    */
  def JOB_DM_73 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {
    UPSQL_JDBC.delete(s"DM_PRIZE_ACT_BRANCH_DLY","REPORT_DT",s"$start_dt",s"$end_dt")
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

    println(s"###JOB_DM_73----($start_dt-$end_dt)--results:"+results.count())
    if(!Option(results).isEmpty){
      results.save2Mysql("DM_PRIZE_ACT_BRANCH_DLY")
    }else{
      println("指定的时间范围无数据插入！")
    }
  }

  /**
    * JOB_DM_74/10-14
    * DM_PRIZE_ACT_DLY->HIVE_PRIZE_ACTIVITY_BAS_INF,HIVE_PRIZE_LVL,HIVE_PRIZE_BAS,HIVE_PRIZE_DISCOUNT_RESULT
    * Code by Xue
    *
    * @param sqlContext
    * @return
    */
  def JOB_DM_74 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {
    UPSQL_JDBC.delete(s"DM_PRIZE_ACT_DLY","REPORT_DT",s"$start_dt",s"$end_dt")
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

    println(s"###JOB_DM_74-----($start_dt-$end_dt)--results:"+results.count())
    if(!Option(results).isEmpty){
      results.save2Mysql("DM_PRIZE_ACT_DLY")
    }else{
      println("指定的时间范围无数据插入！")
    }
  }

  /**
    * JOB_DM_75/10-14
    * DM_DISC_ACT_DLY->HIVE_PRIZE_DISCOUNT_RESULT,HIVE_DISCOUNT_BAS_INF
    * Code by Xue
    *
    * @param sqlContext
    * @return
    */
  def JOB_DM_75 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {
    UPSQL_JDBC.delete(s"DM_DISC_ACT_DLY","REPORT_DT",s"$start_dt",s"$end_dt")
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

    println(s"###JOB_DM_75----($start_dt-$end_dt)--results:"+results.count())
    if(!Option(results).isEmpty){
      results.save2Mysql("DM_DISC_ACT_DLY")
    }else{
      println("指定的时间范围无数据插入！")
    }
  }


  /**
    * JOB_DM_76  2016-8-31
    * dm_auto_disc_cfp_tran->hive_prize_discount_result
    * code by tzq
    *
    * @param sqlContext
    */
  def JOB_DM_76(implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {

    println("###JOB_DM_76(dm_auto_disc_cfp_tran->hive_prize_discount_result)")

    UPSQL_JDBC.delete("dm_auto_disc_cfp_tran","report_dt",start_dt,end_dt)

    var today_dt=start_dt
    if(interval>0 ){
      sqlContext.sql(s"use $hive_dbname")
      for(i <- 0 to interval){
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

        println(s"###JOB_DM_76------$today_dt results:"+results.count())

        if(!Option(results).isEmpty){
          results.save2Mysql("dm_auto_disc_cfp_tran")
        }else{
          println("指定的时间范围无数据插入！")
        }

        today_dt=DateUtils.addOneDay(today_dt)
      }
    }
  }



  /**
    * JOB_DM_78/10-14
    * DM_ISS_DISC_CFP_TRAN->HIVE_CARD_BIN,HIVE_CARD_BIND_INF,HIVE_ACTIVE_CARD_ACQ_BRANCH_MON
    * Code by Xue
    *
    * @param sqlContext
    * @return
    */
  def JOB_DM_78 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    UPSQL_JDBC.delete(s"DM_ISS_DISC_CFP_TRAN","REPORT_DT",start_dt,end_dt)
    var today_dt=start_dt
    if(interval>0 ){
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
        println(s"###JOB_DM_78------$today_dt results:"+results.count())
        if(!Option(results).isEmpty){
          results.save2Mysql("DM_ISS_DISC_CFP_TRAN")
        }else{
          println("指定的时间范围无数据插入！")
        }

        today_dt=DateUtils.addOneDay(today_dt)
      }
    }
  }

  /**
    * JOB_DM_86/10-14
    * DM_USER_REAL_NAME->HIVE_PRI_ACCT_INF
    * Code by Xue
    *
    * @param sqlContext
    * @return
    */
  def JOB_DM_86 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    UPSQL_JDBC.delete(s"DM_USER_REAL_NAME","REPORT_DT",start_dt,end_dt)
    var today_dt=start_dt
    if(interval>0 ){
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
        println(s"###JOB_DM_86------$today_dt results:"+results.count())
        if(!Option(results).isEmpty){
          results.save2Mysql("DM_USER_REAL_NAME")
        }else{
          println("指定的时间范围无数据插入！")
        }
        today_dt=DateUtils.addOneDay(today_dt)
      }
    }

  }


  /**
    * JOB_DM_87  2016年9月7日
    * dm_cashier_stat_dly ->
    * FROM :
    * cup_branch_ins_id_nm
    * hive_cashier_point_acct_oper_dtl
    * hive_cashier_bas_inf
    * hive_cdhd_cashier_maktg_reward_dtl
    * hive_signer_log
    *
    * @author tzq
    * @param sqlContext
    */
  def JOB_DM_87(implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {

    println("###JOB_DM_87(dm_cashier_stat_dly->hive_cashier_bas_inf+cup_branch_ins_id_nm+hive_cashier_point_acct_oper_dtl+hive_cdhd_cashier_maktg_reward_dtl+hive_signer_log)")

    UPSQL_JDBC.delete("dm_cashier_stat_dly","report_dt",start_dt,end_dt);

    var today_dt=start_dt
    if(interval>0 ){
      sqlContext.sql(s"use $hive_dbname")
      for(i <- 0 to interval){
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
        println(s"###JOB_DM_87------$today_dt results:"+results.count())
        if(!Option(results).isEmpty){
          results.save2Mysql("dm_cashier_stat_dly")
        }else{
          println("指定的时间范围无数据插入！")
        }

        today_dt=DateUtils.addOneDay(today_dt)
      }
    }




  }




}

// ## END LINE ##