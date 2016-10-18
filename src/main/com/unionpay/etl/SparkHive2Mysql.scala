package com.unionpay.etl

import com.unionpay.conf.ConfigurationManager
import com.unionpay.constant.Constants
import com.unionpay.jdbc.UPSQL_JDBC
import com.unionpay.jdbc.UPSQL_JDBC.DataFrame2Mysql
import com.unionpay.utils.DateUtils
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
/**
  * 作业：抽取hive数据仓库中的数据到UPSQL数据库
  * Created by tzq on 2016/10/13.
  */
object SparkHive2Mysql {

  //计算开始日期：start_dt-1
  private lazy val start_dt=DateUtils.getYesterdayByJob(ConfigurationManager.getProperty(Constants.START_DT))
  //结束日期
  private lazy val end_dt=ConfigurationManager.getProperty(Constants.END_DT)
  //计算间隔天数
  private lazy val interval=DateUtils.getIntervalDays(start_dt,end_dt).toInt
  //指定HIVE数据库名
  private lazy val hive_dbname =ConfigurationManager.getProperty(Constants.HIVE_DBNAME)

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("SparkHive2Mysql")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    implicit val sqlContext = new HiveContext(sc)

    JOB_DM_1    //CODE BY YX
    JOB_DM_2    //CODE BY XTP
    JOB_DM_3    //CODE BY YX
    JOB_DM_4    //CODE BY XTP
    JOB_DM_5    //CODE BY TZQ
    JOB_DM_6    //CODE BY TZQ
    JOB_DM_9    //CODE BY XTP
    JOB_DM_54   //CODE BY XTP  //Error:Kryo serialization failed: Buffer overflow
    JOB_DM_55   //CODE BY TZQ
    JOB_DM_61   //CODE BY YX
    JOB_DM_62   //CODE BY TZQ
    JOB_DM_63   //CODE BY XTP
    JOB_DM_65   //CODE BY XTP
    JOB_DM_66   //CODE BY TZQ
    JOB_DM_67   //CODE BY YX
    JOB_DM_68   //CODE BY YX
    JOB_DM_69   //CODE BY TZQ
    JOB_DM_70   //CODE BY TZQ
    JOB_DM_71   //CODE BY TZQ
    JOB_DM_72   //CODE BY YX
    JOB_DM_73   //CODE BY XTP
    JOB_DM_74   //CODE BY XTP
    JOB_DM_75   //CODE BY XTP
    JOB_DM_76   //CODE BY TZQ
    JOB_DM_78   //CODE BY XTP
    JOB_DM_86   //CODE BY XTP
    JOB_DM_87   //CODE BY TZQ

    sc.stop()

  }

  /**
    * dm-job-01 20160901
    * dm_user_mobile_home->hive_pri_acct_inf
    * @author winslow yang
    * @param sqlContext
    */
  def JOB_DM_1(implicit sqlContext: HiveContext) = {
    println("###JOB_DM_1(dm_user_card_nature->hive_pri_acct_inf)")

    UPSQL_JDBC.delete("dm_user_mobile_home","report_dt",start_dt,end_dt)
    var today_dt=start_dt

    if(interval>0 ){
      sqlContext.sql(s"use $hive_dbname")
      for(i <- 0 to interval){

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
             |sum(case when to_date(t.rec_crt_ts)>=trunc('$today_dt','YYYY') and to_date(t.rec_crt_ts)<='$today_dt'  then  1 else 0 end) as months,
             |sum(case when to_date(t.rec_crt_ts)>=trunc('$today_dt','YYYY') and to_date(t.rec_crt_ts)<='$today_dt'  then  1 else 0 end) as years,
             |sum(case when to_date(t.rec_crt_ts)<='$today_dt' then  1 else 0 end) as total
             |from
             |(select cdhd_usr_id,rec_crt_ts, phone_location,realnm_in from hive_pri_acct_inf where usr_st='1' ) t
             |group by t.phone_location  ) a
             |left join
             |(
             |select
             |phone_location,
             |sum(case when to_date(rec_crt_ts)='$today_dt'  and to_date(bind_dt)='$today_dt'  then  1  else 0 end) as tpre,
             |sum(case when to_date(rec_crt_ts)>=trunc('$today_dt','YYYY') and to_date(rec_crt_ts)<='$today_dt'
             |          and to_date(bind_dt)>=trunc('$today_dt','YYYY') and  to_date(bind_dt)<='$today_dt' then   1 else 0 end) as months,
             |sum(case when to_date(rec_crt_ts)>=trunc('$today_dt','YYYY') and to_date(rec_crt_ts)<='$today_dt'
             |          and to_date(bind_dt)>=trunc('$today_dt','YYYY') and  to_date(bind_dt)<='$today_dt' then  1 else 0 end) as years,
             |sum(case when to_date(rec_crt_ts)<='$today_dt' and  to_date(bind_dt)<='$today_dt'  then  1 else 0 end) as total
             |from (select rec_crt_ts,phone_location,cdhd_usr_id from hive_pri_acct_inf
             |where usr_st='1' ) a
             |inner join (select distinct(cdhd_usr_id) , rec_crt_ts as bind_dt from hive_card_bind_inf where card_auth_st in ('1','2','3') ) b
             |on a.cdhd_usr_id=b.cdhd_usr_id
             |group by phone_location ) b
             |on a.phone_location =b.phone_location
             |left join
             |(
             |select
             |phone_location,
             |sum(case when to_date(rec_crt_ts)='$today_dt'  and to_date(trans_dt)='$today_dt'  then  1  else 0 end) as tpre,
             |sum(case when to_date(rec_crt_ts)>=trunc('$today_dt','YYYY') and to_date(rec_crt_ts)<='$today_dt'
             |          and to_date(trans_dt)>=trunc('$today_dt','YYYY') and  to_date(trans_dt)<='$today_dt' then  1 else 0 end) as years,
             |sum(case when to_date(rec_crt_ts)<='$today_dt' and  to_date(trans_dt)<='$today_dt'  then  1 else 0 end) as total
             |from (select phone_location,cdhd_usr_id,rec_crt_ts from hive_pri_acct_inf
             |where usr_st='1') a
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

        println(s"###JOB_DM_1------$today_dt results:"+results.count())
        if(!Option(results).isEmpty){
          results.save2Mysql("dm_user_mobile_home")
        }else{
          println("指定的时间范围无数据插入！")
        }
        today_dt=DateUtils.addOneDay(today_dt)
      }
    }
  }

  /**
    * JOB_DM_2/10-14
    * dm_user_idcard_home->hive_pri_acct_inf,hive_acc_trans
    * Code by Xue
    * @param sqlContext
    * @return
    */
  def JOB_DM_2 (implicit sqlContext: HiveContext) = {
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
             |a.tpre   as   REG_TPRE_ADD_NUM    ,
             |a.years  as   REG_YEAR_ADD_NUM    ,
             |a.total  as   REG_TOTLE_ADD_NUM   ,
             |b.tpre   as   EFFECT_TPRE_ADD_NUM ,
             |b.years  as   EFFECT_YEAR_ADD_NUM ,
             |b.total  as   EFFECT_TOTLE_ADD_NUM,
             |c.tpre   as   DEAL_TPRE_ADD_NUM   ,
             |c.years  as   DEAL_YEAR_ADD_NUM   ,
             |c.total  as   DEAL_TOTLE_ADD_NUM
             |from
             |(
             |select
             |case when tempe.CITY_CARD in ('大连','宁波','厦门','青岛','深圳') then tempe.CITY_CARD else tempe.PROVINCE_CARD end as ID_AREA_NM,
             |count(distinct(case when substr(tempe.rec_crt_ts,1,10)='$today_dt'  then tempe.cdhd_usr_id end)) as tpre,
             |count(distinct(case when substr(tempe.rec_crt_ts,1,10)>=trunc('$today_dt','YYYY') and substr(tempe.rec_crt_ts,1,10)<='$today_dt'  then tempe.cdhd_usr_id end)) as years,
             |count(distinct(case when substr(tempe.rec_crt_ts,1,10)<='$today_dt' then  tempe.cdhd_usr_id end)) as total
             |from
             |(select cdhd_usr_id,rec_crt_ts, CITY_CARD,PROVINCE_CARD from HIVE_PRI_ACCT_INF where usr_st='1' ) tempe
             |group by (case when CITY_CARD in ('大连','宁波','厦门','青岛','深圳') then CITY_CARD else PROVINCE_CARD end)
             |) a
             |left join
             |(
             |select
             |case when tempa.CITY_CARD in ('大连','宁波','厦门','青岛','深圳') then tempa.CITY_CARD else tempa.PROVINCE_CARD end as ID_AREA_NM,
             |count(distinct(case when substr(tempa.rec_crt_ts,1,10)='$today_dt'  and substr(tempb.bind_dt,1,10)='$today_dt'  then  tempa.cdhd_usr_id end)) as tpre,
             |count(distinct(case when substr(tempa.rec_crt_ts,1,10)>=trunc('$today_dt','YYYY') and substr(tempa.rec_crt_ts,1,10)<='$today_dt'
             |and substr(tempb.bind_dt,1,10)>=trunc('$today_dt','YYYY') and  substr(tempb.bind_dt,1,10)<='$today_dt' then  tempa.cdhd_usr_id end)) as years,
             |count(distinct(case when substr(tempa.rec_crt_ts,1,10)<='$today_dt' and  substr(tempb.bind_dt,1,10)<='$today_dt'  then  tempa.cdhd_usr_id end)) as total
             |from
             |(
             |select rec_crt_ts,CITY_CARD,PROVINCE_CARD,cdhd_usr_id from HIVE_PRI_ACCT_INF
             |where usr_st='1' ) tempa
             |inner join (select distinct cdhd_usr_id , rec_crt_ts as  bind_dt  from HIVE_CARD_BIND_INF where card_auth_st in ('1','2','3') ) tempb
             |on tempa.cdhd_usr_id=tempb.cdhd_usr_id
             |group by (case when tempa.CITY_CARD in ('大连','宁波','厦门','青岛','深圳') then tempa.CITY_CARD else tempa.PROVINCE_CARD end) ) b
             |on a.ID_AREA_NM =b.ID_AREA_NM
             |left join
             |(
             |select
             |case when tempc.CITY_CARD in ('大连','宁波','厦门','青岛','深圳') then tempc.CITY_CARD else tempc.PROVINCE_CARD end as ID_AREA_NM,
             |count(distinct(case when substr(tempc.rec_crt_ts,1,10)='$today_dt'  and substr(tempd.trans_dt,1,10)='$today_dt'  then tempc.cdhd_usr_id end)) as tpre,
             |count(distinct(case when substr(tempc.rec_crt_ts,1,10)>=trunc('$today_dt','YYYY') and substr(tempc.rec_crt_ts,1,10)<='$today_dt'
             |and substr(tempd.trans_dt,1,10)>=trunc('$today_dt','YYYY') and  substr(tempd.trans_dt,1,10)<='$today_dt' then  tempc.cdhd_usr_id end)) as years,
             |count(distinct(case when substr(tempc.rec_crt_ts,1,10)<='$today_dt' and  substr(tempd.trans_dt,1,10)<='$today_dt'  then  tempc.cdhd_usr_id end)) as total
             |from
             |(select CITY_CARD,CITY_CARD,PROVINCE_CARD,cdhd_usr_id,rec_crt_ts from HIVE_PRI_ACCT_INF
             |where usr_st='1') tempc
             |inner join (select distinct cdhd_usr_id,trans_dt from HIVE_ACC_TRANS ) tempd
             |on tempc.cdhd_usr_id=tempd.cdhd_usr_id
             |group by (case when tempc.CITY_CARD in ('大连','宁波','厦门','青岛','深圳') then tempc.CITY_CARD else tempc.PROVINCE_CARD end) ) c
             |on a.ID_AREA_NM=c.ID_AREA_NM
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
    *  dm-job-3 20160912
    *  dm_user_regist_channel->hive_pri_acct_inf+hive_inf_source_dtl+hive_acc_trans+hive_card_bind_inf+hive_inf_source_class
    * @author winslow yang
    * @param sqlContext
    */
  def JOB_DM_3(implicit sqlContext: HiveContext) = {
    println("###JOB_DM_3(dm_user_regist_channel->hive_pri_acct_inf+hive_inf_source_dtl+hive_acc_trans+hive_card_bind_inf+hive_inf_source_class)")

    UPSQL_JDBC.delete("dm_user_regist_channel","report_dt",start_dt,end_dt)

    var today_dt=start_dt
    if(interval>0 ){
      sqlContext.sql(s"use $hive_dbname")
      for(i <- 0 to interval){
        val results = sqlContext.sql(
          s"""
             |select
             | d.class as regist_channel ,
             | d.access_nm as reg_son_chn ,
             | '$today_dt' as report_dt ,
             | sum(a.tpre) as reg_tpre_add_num ,
             | sum(a.years) as reg_year_add_num ,
             | sum(a.total) as reg_totle_add_num ,
             | sum(b.tpre) as effect_tpre_add_num ,
             | sum(b.years) as effect_year_add_num ,
             | sum(b.total) as effect_totle_add_num ,
             | 0 as batch_tpre_add_num ,
             | 0 as batch_year_add_num ,
             | 0 as batch_totle_add_num ,
             | 0 as client_tpre_add_num ,
             | 0 as client_year_add_num ,
             | 0 as client_totle_add_num ,
             | sum(c.tpre) as deal_tpre_add_num ,
             | sum(c.years) as deal_year_add_num ,
             | sum(c.total) as deal_totle_add_num
             |from
             | (
             | select
             | a.inf_source,
             | count(distinct(
             | case
             | when to_date(a.rec_crt_ts)='$today_dt'
             | then a.cdhd_usr_id
             | end)) as tpre,
             | count(distinct(
             | case
             | when to_date(a.rec_crt_ts)>=trunc('$today_dt','yyyy')
             | and to_date(a.rec_crt_ts)<='$today_dt'
             | then a.cdhd_usr_id
             | end)) as years,
             | count(distinct(
             | case
             | when to_date(a.rec_crt_ts)<='$today_dt'
             | then a.cdhd_usr_id
             | end)) as total
             | from
             | (
             | select
             | inf_source,
             | cdhd_usr_id,
             | rec_crt_ts
             | from
             | hive_pri_acct_inf
             | where
             | usr_st='1') a
             | group by
             | a.inf_source ) a
             |left join
             | (
             | select
             | a.inf_source,
             | count(distinct(
             | case
             | when to_date(a.rec_crt_ts)='$today_dt'
             | and to_date(b.card_dt)='$today_dt'
             | then a.cdhd_usr_id
             | end)) as tpre,
             | count(distinct(
             | case
             | when to_date(a.rec_crt_ts)>=trunc('$today_dt','yyyy')
             | and to_date(a.rec_crt_ts)<='$today_dt'
             | and to_date(b.card_dt)>=trunc('$today_dt','yyyy')
             | and to_date(b.card_dt)<='$today_dt'
             | then a.cdhd_usr_id
             | end)) as years,
             | count(distinct(
             | case
             | when to_date(a.rec_crt_ts)<='$today_dt'
             | and to_date(b.card_dt)<='$today_dt'
             | then a.cdhd_usr_id
             | end)) as total
             | from
             | (
             | select
             | inf_source,
             | cdhd_usr_id,
             | rec_crt_ts
             | from
             | hive_pri_acct_inf
             | where
             | usr_st='1' ) a
             | inner join
             | (
             | select distinct
             | (cdhd_usr_id),
             | rec_crt_ts as card_dt
             | from
             | hive_card_bind_inf
             | where
             | card_auth_st in ('1',
             | '2',
             | '3') ) b
             | on
             | a.cdhd_usr_id=b.cdhd_usr_id
             | group by
             | a.inf_source) b
             |on
             | trim(a.inf_source)=trim(b.inf_source)
             |left join
             | (
             | select
             | a.inf_source,
             | count(distinct (a.cdhd_usr_id)),
             | count(distinct(
             | case
             | when to_date(a.rec_crt_ts)='$today_dt'
             | and to_date(b.trans_dt)='$today_dt'
             | then a.cdhd_usr_id
             | end)) as tpre,
             | count(distinct(
             | case
             | when to_date(a.rec_crt_ts)>=trunc('$today_dt','yyyy')
             | and to_date(a.rec_crt_ts)<='$today_dt'
             | and to_date(b.trans_dt)>=trunc('$today_dt','yyyy')
             | and to_date(b.trans_dt)<='$today_dt'
             | then a.cdhd_usr_id
             | end)) as years,
             | count(distinct(
             | case
             | when to_date(a.rec_crt_ts)<='$today_dt'
             | and to_date(b.trans_dt)<='$today_dt'
             | then a.cdhd_usr_id
             | end)) as total
             | from
             | (
             | select
             | inf_source,
             | cdhd_usr_id,
             | rec_crt_ts
             | from
             | hive_pri_acct_inf
             | where
             | usr_st='1' ) a
             | inner join
             | (
             | select distinct
             | (cdhd_usr_id),
             | trans_dt
             | from
             | hive_acc_trans) b
             | on
             | a.cdhd_usr_id=b.cdhd_usr_id
             | group by
             | a.inf_source ) c
             |on
             | trim(a.inf_source)=trim(c.inf_source)
             |left join
             | (
             | select
             | dtl.access_id,
             | dtl.access_nm,
             | cla.class
             | from
             | hive_inf_source_dtl dtl
             | left join
             | hive_inf_source_class cla
             | on
             | trim(cla.access_nm)=trim(dtl.access_nm) ) d
             |on
             | trim(a.inf_source)=trim(d.access_id)
             |group by
             |	 d.class, d.access_nm
             |
      """.stripMargin)

        println(s"###JOB_DM_3------$today_dt results:"+results.count())
        if(!Option(results).isEmpty){
          results.save2Mysql("dm_user_regist_channel")
        }else{
          println("指定的时间范围无数据插入！")
        }
        today_dt=DateUtils.addOneDay(today_dt)
      }
    }
  }


  /**
    * JOB_DM_4/10-14
    * DM_USER_CARD_AUTH->HIVE_PRI_ACCT_INF,HIVE_CARD_BIND_INF,HIVE_ACC_TRANS
    * Code by Xue
    * @param sqlContext
    * @return
    */
  def JOB_DM_4 (implicit sqlContext: HiveContext) = {
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
             |a.tpre   as   EFFECT_TPRE_ADD_NUM ,
             |a.years  as   EFFECT_YEAR_ADD_NUM ,
             |a.total  as   EFFECT_TOTLE_ADD_NUM,
             |b.tpre   as   DEAL_TPRE_ADD_NUM   ,
             |b.years  as   DEAL_YEAR_ADD_NUM   ,
             |b.total  as   DEAL_TOTLE_ADD_NUM
             |
             |from (
             |select
             |(case when tempb.card_auth_st='0' then   '默认'
             | when tempb.card_auth_st='1' then   '支付认证'
             | when tempb.card_auth_st='2' then   '可信认证'
             | when tempb.card_auth_st='3' then   '可信+支付认证'
             |else '--' end) as card_auth_nm,
             |tempa.realnm_in as realnm_in,
             |count(distinct(case when substr(tempa.rec_crt_ts,1,10)='$today_dt'  and substr(tempb.CARD_DT,1,10)='$today_dt'  then tempa.cdhd_usr_id end)) as tpre,
             |count(distinct(case when substr(tempa.rec_crt_ts,1,10)>=trunc('$today_dt','YYYY') and substr(tempa.rec_crt_ts,1,10)<='$today_dt'
             |and substr(tempb.CARD_DT,1,10)>=trunc('$today_dt','YYYY')  and  substr(tempb.CARD_DT,1,10)<='$today_dt' then  tempa.cdhd_usr_id end)) as years,
             |count(distinct(case when substr(tempa.rec_crt_ts,1,10)<='$today_dt' and  substr(tempb.CARD_DT,1,10)<='$today_dt'  then tempa.cdhd_usr_id end)) as total
             |from
             |(select cdhd_usr_id,rec_crt_ts,realnm_in from HIVE_PRI_ACCT_INF
             |where usr_st='1' ) tempa
             |inner join
             |(select distinct tempe.cdhd_usr_id as cdhd_usr_id,
             |tempe.card_auth_st as card_auth_st,
             |tempe.rec_crt_ts as CARD_DT
             |from HIVE_CARD_BIND_INF tempe) tempb
             |on tempa.cdhd_usr_id=tempb.cdhd_usr_id
             |group by
             |case when tempb.card_auth_st='0' then   '默认'
             | when tempb.card_auth_st='1' then   '支付认证'
             | when tempb.card_auth_st='2' then   '可信认证'
             | when tempb.card_auth_st='3' then   '可信+支付认证'
             |else '--' end,tempa.realnm_in
             |) a
             |
             |left join
             |
             |(
             |select
             |(case when tempc.card_auth_st='0' then   '默认'
             | when tempc.card_auth_st='1' then   '支付认证'
             | when tempc.card_auth_st='2' then   '可信认证'
             | when tempc.card_auth_st='3' then   '可信+支付认证'
             |else '--' end) as card_auth_nm,
             |count(distinct(case when substr(tempc.rec_crt_ts,1,10)='$today_dt'  and substr(tempd.trans_dt,1,10)='$today_dt' then  tempc.cdhd_usr_id end)) as tpre,
             |count(distinct(case when substr(tempc.rec_crt_ts,1,10)>=trunc('$today_dt','YYYY') and substr(tempc.rec_crt_ts,1,10)<='$today_dt'
             |and substr(tempd.trans_dt,1,10)>=trunc('$today_dt','YYYY') and  substr(tempd.trans_dt,1,10)<='$today_dt' then  tempc.cdhd_usr_id end)) as years,
             |count(distinct(case when substr(tempc.rec_crt_ts,1,10)<='$today_dt' and  substr(tempd.trans_dt,1,10)<='$today_dt'  then  tempc.cdhd_usr_id end)) as total
             |from
             |(select distinct cdhd_usr_id,card_auth_st,rec_crt_ts from HIVE_CARD_BIND_INF) tempc
             |inner join (select distinct cdhd_usr_id,trans_dt from HIVE_ACC_TRANS ) tempd
             |on tempc.cdhd_usr_id=tempd.cdhd_usr_id
             |group by
             |case when tempc.card_auth_st='0' then   '默认'
             | when tempc.card_auth_st='1' then   '支付认证'
             | when tempc.card_auth_st='2' then   '可信认证'
             | when tempc.card_auth_st='3' then   '可信+支付认证'
             |else '--' end
             |) b
             |on a.card_auth_nm=b.card_auth_nm
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
    * @author tzq
    * @param sqlContext
    */
  def JOB_DM_5(implicit sqlContext: HiveContext) = {

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
    * @author tzq
    * @param sqlContext
    */
  def JOB_DM_6(implicit sqlContext: HiveContext) = {

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
    * JOB_DM_9/10-14
    * dm_store_domain_branch_company->hive_mchnt_inf_wallet,hive_preferential_mchnt_inf,hive_mchnt_tp,hive_mchnt_tp_grp
    * Code by Xue
    * @param sqlContext
    * @return
    */
  def JOB_DM_9 (implicit sqlContext: HiveContext) = {
    println("###JOB_DM_9(dm_store_domain_branch_company->hive_mchnt_inf_wallet,hive_preferential_mchnt_inf,hive_mchnt_tp,hive_mchnt_tp_grp)")
    UPSQL_JDBC.delete(s"DM_STORE_DOMAIN_BRANCH_COMPANY","REPORT_DT",start_dt,end_dt)
    var today_dt=start_dt
    if(interval>0 ){
      sqlContext.sql(s"use $hive_dbname")
      for(i <- 0 to interval){
        val results = sqlContext.sql(
          s"""
             |SELECT
             |a.gb_region_nm as BRANCH_AREA,
             |'$today_dt' as report_dt,
             |a.tpre   as   STORE_TPRE_ADD_NUM  ,
             |a.years  as   STORE_YEAR_ADD_NUM  ,
             |a.total  as   STORE_TOTLE_ADD_NUM ,
             |b.tpre   as   ACTIVE_TPRE_ADD_NUM ,
             |b.years  as   ACTIVE_YEAR_ADD_NUM ,
             |b.total  as   ACTIVE_TOTLE_ADD_NUM,
             |c.tpre   as   COUPON_TPRE_ADD_NUM ,
             |c.years  as   COUPON_YEAR_ADD_NUM ,
             |c.total  as   COUPON_TOTLE_ADD_NUM
             |FROM
             |(
             |select
             |tempe.gb_region_nm as gb_region_nm,
             |count(distinct(case when to_date(tempe.rec_crt_ts)='$today_dt'  then tempe.MCHNT_CD end)) as tpre,
             |count(distinct(case when to_date(tempe.rec_crt_ts)>=trunc('$today_dt','YYYY') and to_date(tempe.rec_crt_ts)<='$today_dt' then  tempe.MCHNT_CD end)) as years,
             |count(distinct(case when to_date(tempe.rec_crt_ts)<='$today_dt' then tempe.MCHNT_CD end)) as total
             |from HIVE_MCHNT_INF_WALLET tempe where substr(tempe.OPEN_BUSS_BMP,1,2)<>00
             |GROUP BY gb_region_nm) a
             |left join
             |
         |(
             |select
             |tempb.cup_branch_ins_id_nm as cup_branch_ins_id_nm,
             |count(distinct(case when to_date(tempb.rec_crt_ts)='$today_dt'  and tempb.valid_begin_dt='$today_dt' AND tempb.valid_end_dt='$today_dt'  then tempb.MCHNT_CD end)) as tpre,
             |count(distinct(case when to_date(tempb.rec_crt_ts)>=trunc('$today_dt','YYYY') and to_date(tempb.rec_crt_ts)='$today_dt'
             |and tempb.valid_begin_dt>=trunc('$today_dt','YYYY') and  tempb.valid_end_dt<='$today_dt' then  tempb.MCHNT_CD end)) as years,
             |count(distinct(case when to_date(tempb.rec_crt_ts)<='$today_dt' and  tempb.valid_begin_dt='$today_dt' AND tempb.valid_end_dt='$today_dt'  then  tempb.MCHNT_CD end)) as total
             |from
             |(
             |select distinct
             |tempc.mchnt_prov as mchnt_prov,
             |tempc.mchnt_city_cd as mchnt_city_cd,
             |tempc.mchnt_county_cd as mchnt_county_cd,
             |tempc.mchnt_addr as mchnt_addr,
             |access.cup_branch_ins_id_nm as cup_branch_ins_id_nm,
             |bill.valid_begin_dt as valid_begin_dt,
             |bill.valid_end_dt as valid_end_dt,
             |tempc.rec_crt_ts as rec_crt_ts,
             |tempc.MCHNT_CD as MCHNT_CD
             |from
             |(select *
             |from HIVE_PREFERENTIAL_MCHNT_INF tempf
             |where tempf.mchnt_cd like 'T%' and tempf.mchnt_st='2' and tempf.mchnt_nm not like '%验证%' and tempf.mchnt_nm not like '%测试%'
             |and tempf.brand_id<>68988) tempc
             |inner join HIVE_CHARA_GRP_DEF_BAT grp on tempc.mchnt_cd=grp.chara_data
             |inner join HIVE_ACCESS_BAS_INF access on access.ch_ins_id_cd=tempc.mchnt_cd
             |inner join (select distinct(chara_grp_cd),valid_begin_dt,valid_end_dt from HIVE_TICKET_BILL_BAS_INF ) bill
             |on bill.chara_grp_cd=grp.chara_grp_cd
             |) tempb
             |group by tempb.cup_branch_ins_id_nm) b
             |on a.gb_region_nm=b.cup_branch_ins_id_nm
             |left join
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
    * JOB_DM_54/10-14
    * dm_val_tkt_act_mchnt_tp_dly->hive_bill_order_trans,hive_bill_sub_order_trans
    * @param sqlContext
    */
  def JOB_DM_54 (implicit sqlContext: HiveContext) = {
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
         |            COUNT(DISTINCT CDHD_USR_ID) AS TRANSUSRCNT
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
         |            COUNT(DISTINCT CDHD_USR_ID) AS PAYUSRCNT
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
         |            COUNT(DISTINCT CDHD_USR_ID)   AS PAYSUCUSRCNT
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
    * @author tzq
    * @param sqlContext
    */
  def JOB_DM_55(implicit sqlContext: HiveContext) = {

    println("###JOB_DM_55-----JOB_DM_55(dm_disc_act_branch_dly->hive_prize_discount_result)")

    UPSQL_JDBC.delete("dm_disc_act_branch_dly","report_dt",start_dt,end_dt);

    sqlContext.sql(s"use $hive_dbname")

    val results=sqlContext.sql(
      s"""
         |select
         |    dbi.cup_branch_ins_id_nm as cup_branch_ins_id_nm,
         |    trans.settle_dt as report_dt,
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
         |and trans.part_settle_dt >= '$start_dt'
         |and trans.part_settle_dt <= '$end_dt'
         |group by
         |    dbi.cup_branch_ins_id_nm,
         |    trans.settle_dt
         |
      """.stripMargin)

    if(!Option(results).isEmpty){
      println("###JOB_DM_55------results:"+results.count())
      results.save2Mysql("dm_disc_act_branch_dly")

    }else{
      println("指定的时间范围无数据插入！")
    }


  }

  /**
    * dm-job-61 20160905
    * dm_cashier_cup_red_branch->hive_cdhd_cashier_maktg_reward_dtl
    * @author winslow yang
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    */
  def JOB_DM_61(implicit sqlContext: HiveContext) = {
    println("###JOB_DM_61(dm_cashier_cup_red_branch->hive_cdhd_cashier_maktg_reward_dtl)")

    UPSQL_JDBC.delete("dm_cashier_cup_red_branch","report_dt",start_dt,end_dt)
    var today_dt=start_dt
    if(interval>0 ){
      sqlContext.sql(s"use $hive_dbname")
      for(i <- 0 to interval){
        val results = sqlContext.sql(
          s"""
             |select cup_branch_ins_id_cd as branch_nm,
             |'$today_dt' as report_dt,
             |count(distinct(case when to_date(settle_dt)>=trunc('$today_dt','YYYY') and to_date(settle_dt)>='$today_dt' then bill_id end)) as years_cnt,
             |sum(case when to_date(settle_dt)>=trunc('$today_dt','YYYY') and  to_date(settle_dt)>='$today_dt' then reward_point_at else 0 end) as years_at,
             |count(distinct(case when to_date(settle_dt)='$today_dt' then bill_id  end))  as today_cnt,
             |sum(case when to_date(settle_dt)='$today_dt' then reward_point_at else 0 end) as today_at
             |from hive_cdhd_cashier_maktg_reward_dtl
             |where rec_st='2' and activity_tp='004'
             |group by cup_branch_ins_id_cd
      """.stripMargin)

        println(s"###JOB_DM_61------$today_dt results:"+results.count())
        if(!Option(results).isEmpty){
          results.save2Mysql("dm_cashier_cup_red_branch")
        }else{
          println("指定的时间范围无数据插入！")
        }

        today_dt=DateUtils.addOneDay(today_dt)
      }
    }
  }

  /**
    * JOB_DM_62  2016-9-6
    * dm_usr_auther_nature_tie_card --> hive_card_bind_inf
    * @author tzq
    * @param sqlContext
    */
  def JOB_DM_62(implicit sqlContext: HiveContext) = {
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
             | else '未认证' end) as card_auth_nm,
             |card_attr as card_attr ,
             |'$today_dt' as report_dt ,
             |count(distinct(case when rec_crt_ts = '$today_dt'  then cdhd_usr_id end))  as tpre,
             |count(distinct(case when rec_crt_ts <= '$today_dt'  then cdhd_usr_id end))  as total
             |
             |from  (
             |select distinct cdhd_usr_id,card_auth_st,rec_crt_ts,substr(bind_card_no,1,8) as card_bin
             |from hive_card_bind_inf where card_bind_st='0') a
             |left join
             |(select card_attr,card_bin from hive_card_bin ) b
             |on a.card_bin=b.card_bin
             |group by (case when card_auth_st='0' then   '未认证'
             |  when card_auth_st='1' then   '支付认证'
             |  when card_auth_st='2' then   '可信认证'
             |  when card_auth_st='3' then   '可信+支付认证'
             | else '未认证' end),card_attr
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
    * @param sqlContext
    * @return
    */
  def JOB_DM_63 (implicit sqlContext: HiveContext) = {
    println("###JOB_DM_63(dm_life_serve_business_trans->hive_life_trans)")
    UPSQL_JDBC.delete(s"DM_LIFE_SERVE_BUSINESS_TRANS","REPORT_DT",start_dt,end_dt)
    var today_dt=start_dt
    if(interval>0 ){
      sqlContext.sql(s"use $hive_dbname")
      for(i <- 0 to interval){
        val results = sqlContext.sql(
          s"""
             |SELECT
             |A.BUSS_TP_NM as BUSS_TP_NM,
             |A.CHNL_TP_NM as CHNL_TP_NM,
             |'$today_dt' as REPORT_DT,
             |B.TRAN_ALL_CNT as TRAN_ALL_CNT,
             |A.TRAN_SUCC_CNT as TRAN_SUCC_CNT,
             |A.TRANS_SUCC_AT as TRANS_SUCC_AT
             |FROM
             |(
             |select
             |BUSS_TP_NM,
             |CHNL_TP_NM,
             |COUNT(TRANS_NO) AS TRAN_SUCC_CNT,
             |SUM(TRANS_AT) AS TRANS_SUCC_AT
             |from HIVE_LIFE_TRANS
             |where PROC_ST ='00'
             |and substr(TRANS_DT,1,10)='$today_dt'
             |GROUP BY BUSS_TP_NM,CHNL_TP_NM
             |) A
             |LEFT JOIN
             |(
             |select
             |BUSS_TP_NM,
             |CHNL_TP_NM,
             |COUNT(TRANS_NO) AS TRAN_ALL_CNT
             |from HIVE_LIFE_TRANS
             |where PROC_ST <>'00'
             |and substr(TRANS_DT,1,10)='$today_dt'
             |GROUP BY BUSS_TP_NM,CHNL_TP_NM
             |) B
             |ON A.BUSS_TP_NM=B.BUSS_TP_NM AND A.BUSS_TP_NM=B.BUSS_TP_NM
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
    * @param sqlContext
    * @return
    */
  def JOB_DM_65 (implicit sqlContext: HiveContext) = {
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
             |where valid_begin_dt>=trunc('$today_dt','YYYY') and valid_end_dt<='$today_dt'
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
             |      cup_branch_ins_id_cd<> ''         and
             |      dwn_total_num<>0                  and
             |      dwn_num>=0                        and
             |      length(trim(translate(trim(bill_nm),'','-0123456789')))<>0
             |      ) tempa
             |      group by tempa.CUP_BRANCH_INS_ID_NM
             |	  ) tempe
             |left join
             |
         |(
             |select
             |tempb.CUP_BRANCH_INS_ID_NM as CUP_BRANCH_INS_ID_NM,
             |count(case when tempd.trans_dt >=trunc('$today_dt','YYYY') and tempd.trans_dt <='$today_dt' then tempd.bill_id end) as accept_year_num,
             |count(case when  tempd.trans_dt ='$today_dt' then tempd.bill_id end) as accept_today_num
             |from
             |(
             |select
             |bill_id,
             |trans_dt,
             |substr(udf_fld,31,2) as CFP_SIGN
             |from  HIVE_ACC_TRANS
             |where substr(udf_fld,31,2) not in ('',' ', '00') and
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
    * @author tzq
    * @param sqlContext
    */
  def JOB_DM_66(implicit sqlContext: HiveContext) = {
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
             | else '其它' end  as cfp_sign
             |from hive_acc_trans
             | where substr(udf_fld,31,2) not in ('',' ', '00') and
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
    *  dm-job-67 20160912
    *  dm_o2o_trans_dly->
    *  hive_acc_trans
    *  hive_offline_point_trans
    *  hive_passive_code_pay_trans
    *  hive_download_trans
    *  hive_switch_point_trans
    *  hive_prize_discount_result
    *  hive_discount_bas_inf
    * @author winslow yang
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    */
  def JOB_DM_67(implicit sqlContext: HiveContext) = {
    println("###JOB_DM_67(dm_o2o_trans_dly)")

    UPSQL_JDBC.delete("dm_o2o_trans_dly","report_dt",start_dt,end_dt)

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
         |                    trans.trans_dt,
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
         |                    trans.trans_dt) a
         |        full outer join
         |            (
         |                select
         |                    ins.cup_branch_ins_id_nm,
         |                    trans.trans_dt,
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
         |                    trans.trans_dt) b
         |        on
         |            (
         |                a.cup_branch_ins_id_nm = b.cup_branch_ins_id_nm
         |            and a.trans_dt = b.trans_dt)
         |        full outer join
         |            (
         |                select
         |                    trans.cup_branch_ins_id_nm as cup_branch_ins_id_nm,
         |                    trans.trans_dt as trans_dt,
         |                    count(*)                as trans_cnt
         |                from
         |                    hive_offline_point_trans trans
         |                where
         |                    trans.part_trans_dt >= '$start_dt'
         |                and trans.part_trans_dt <= '$end_dt'
         |                and trans.oper_st in('0' ,
         |                                     '3')
         |                and trans.point_at>0
         |                group by
         |                    trans.cup_branch_ins_id_nm,
         |                    trans.trans_dt) c
         |        on
         |            (
         |                a.cup_branch_ins_id_nm = c.cup_branch_ins_id_nm
         |            and a.trans_dt = c.trans_dt)
         |        full outer join
         |            (
         |                select
         |                    trans.cup_branch_ins_id_nm,
         |                    trans.trans_dt,
         |                    count(*) as trans_cnt
         |                from
         |                    hive_online_point_trans trans
         |                where
         |                    trans.status = '1'
         |                and trans.part_trans_dt >= '$start_dt'
         |                and trans.part_trans_dt <= '$end_dt'
         |                group by
         |                    trans.cup_branch_ins_id_nm,
         |                    trans.trans_dt) d
         |        on
         |            (
         |                a.cup_branch_ins_id_nm = d.cup_branch_ins_id_nm
         |            and a.trans_dt = d.trans_dt)
         |        full outer join
         |            (
         |                select
         |                    mchnt.cup_branch_ins_id_nm,
         |                    trans.trans_dt,
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
         |                and trans.tran_certi like '10%'
         |                and trans.part_trans_dt >= '$start_dt'
         |                and trans.part_trans_dt <= '$end_dt'
         |                group by
         |                    mchnt.cup_branch_ins_id_nm,
         |                    trans.trans_dt) e
         |        on
         |            (
         |                a.cup_branch_ins_id_nm = e.cup_branch_ins_id_nm
         |            and a.trans_dt = e.trans_dt)
         |        full outer join
         |            (
         |                select
         |                    bill.cup_branch_ins_id_nm,
         |                    a.trans_dt,
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
         |                    a.trans_dt) f
         |        on
         |            (
         |                a.cup_branch_ins_id_nm = f.cup_branch_ins_id_nm
         |            and a.trans_dt = f.trans_dt)
         |        full outer join
         |            (
         |                select
         |                    ins.cup_branch_ins_id_nm as cup_branch_ins_id_nm,
         |                    swt.trans_dt             as trans_dt,
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
         |                    swt.trans_dt)g
         |        on
         |            (
         |                a.cup_branch_ins_id_nm = g.cup_branch_ins_id_nm
         |            and a.trans_dt = g.trans_dt)
         |        full outer join
         |            (
         |                select
         |                    dbi.cup_branch_ins_id_nm as cup_branch_ins_id_nm,
         |                    trans.settle_dt          as trans_dt,
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
         |                and trans.part_settle_dt >= '$start_dt'
         |                and trans.part_settle_dt <= '$end_dt'
         |                group by
         |                    dbi.cup_branch_ins_id_nm,
         |                    trans.settle_dt) h
         |        on
         |            (
         |                a.cup_branch_ins_id_nm = g.cup_branch_ins_id_nm
         |            and a.trans_dt = h.trans_dt)) t
         |group by
         |    t.cup_branch_ins_id_nm,
         |    t.report_dt
      """.stripMargin)

    println(s"###JOB_DM_67------ ( $start_dt-$end_dt ) results:"+results.count())

    if(!Option(results).isEmpty){
      results.save2Mysql("dm_o2o_trans_dly")
    }else{
      println("指定的时间范围无数据插入！")
    }

  }

  /**
    *  dm-job-68 20160905
    *  DM_OFFLINE_POINT_TRANS_DLY
    * @author winslow yang
    * @param sqlContext
    */
  def JOB_DM_68(implicit sqlContext: HiveContext) = {
    println("###JOB_DM_68(dm_offline_point_trans_dly)")


    UPSQL_JDBC.delete("dm_offline_point_trans_dly","report_dt",start_dt,end_dt)

    sqlContext.sql(s"use $hive_dbname")
    val results = sqlContext.sql(
      s"""
         |select
         |    cup_branch_ins_id_nm,
         |    acct_addup_bat_dt as report_dt,
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
         |		trans.part_trans_dt >='$start_dt' and trans.part_trans_dt >='$end_dt' and
         |   oper_st in('0','3') and
         |   to_date(acct_addup_bat_dt) >= '$start_dt' and to_date(acct_addup_bat_dt) <= '$end_dt'
         |group by
         |    cup_branch_ins_id_nm,
         |    acct_addup_bat_dt
      """.stripMargin)

    println(s"###JOB_DM_68------ ( $start_dt-$end_dt ) results:"+results.count())
    if(!Option(results).isEmpty){
      results.save2Mysql("dm_offline_point_trans_dly")
    }else{
      println("指定的时间范围无数据插入！")
    }


  }

  /**
    * JOB_DM_69  2016-9-1
    * dm_disc_tkt_act_dly->hive_ticket_bill_bas_inf+hive_acc_trans
    * (使用分区part_trans_dt 中的数据)
    * @author tzq
    * @param sqlContext
    */
  def JOB_DM_69(implicit sqlContext: HiveContext) = {
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
         |                    valid_begin_dt,
         |                    valid_end_dt
         |                from
         |                    hive_ticket_bill_bas_inf
         |                where
         |                    bill_st in('1', '2')
         |            ) bill_data
         |        left join
         |            (
         |                select
         |                    bill_id,
         |                    trans_dt,
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
         |                    bill_id,trans_dt)  trans_data
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
    * @author tzq
    * @param sqlContext
    */
  def JOB_DM_70(implicit sqlContext: HiveContext) = {
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
         |                    valid_begin_dt,
         |                    valid_end_dt
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
         |                    trans_dt,
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
         |                group by bill_id, trans_dt
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
    * @author tzq
    * @param sqlContext
    */
  def JOB_DM_71(implicit sqlContext: HiveContext) = {

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
         |                    valid_begin_dt,
         |                    valid_end_dt
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
         |                    trans_dt,
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
         |                group by bill_id,trans_dt) as trans_data
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
    * dm-job-72 20160901
    * dm_offline_point_act_dly->hive_offline_point_trans
    * @author winslow yang
    * @param sqlContext
    */
  def JOB_DM_72(implicit sqlContext: HiveContext) = {
    println("###JOB_DM_72(dm_offline_point_act_dly->hive_offline_point_trans)")

    UPSQL_JDBC.delete("dm_offline_point_act_dly","report_dt",start_dt,end_dt)

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
         |            row_number() over(partition by a.acpt_addup_bat_dt order by a.bill_num desc) as rn
         |        from
         |            (
         |                select
         |                    trans.plan_id,
         |                    regexp_replace(trans.plan_nm,' ','') as plan_nm,
         |                    trans.acct_addup_bat_dt as acpt_addup_bat_dt,
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
         |                    regexp_replace(trans.plan_nm,' ',''),trans.acct_addup_bat_dt)a) b
         |        where
         |            b.rn <= 10
      """.stripMargin)

    println(s"###JOB_DM_72------ ( $start_dt-$end_dt ) results:"+results.count())
    if(!Option(results).isEmpty){
      results.save2Mysql("dm_offline_point_act_dly")
    }else{
      println("指定的时间范围无数据插入！")
    }

  }




  /**
    * JOB_DM_73/10-14
    * DM_PRIZE_ACT_BRANCH_DLY->HIVE_PRIZE_ACTIVITY_BAS_INF,HIVE_PRIZE_LVL,HIVE_PRIZE_DISCOUNT_RESULT
    * Code by Xue
    * @param sqlContext
    * @return
    */
  def JOB_DM_73 (implicit sqlContext: HiveContext) = {
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
         |                            SETTLE_DT >= '$start_dt'
         |                        AND SETTLE_DT <= '$end_dt') RSLT
         |                WHERE
         |                    PRIZE.LOC_ACTIVITY_ID = LVL.LOC_ACTIVITY_ID
         |                AND PRIZE.ACTIVITY_BEGIN_DT<= RSLT.SETTLE_DT
         |                AND PRIZE.ACTIVITY_END_DT>=RSLT.SETTLE_DT
         |                AND PRIZE.RUN_ST!='3'
         |                GROUP BY
         |                    PRIZE.CUP_BRANCH_INS_ID_NM,
         |                    RSLT.SETTLE_DT
         |			) A,
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
         |                AND PR.SETTLE_DT >= '$start_dt'
         |                AND PR.SETTLE_DT <= '$end_dt'
         |                AND PR.TRANS_ID NOT LIKE 'V%'
         |                AND PRIZE.RUN_ST!='3'
         |                GROUP BY
         |                    PRIZE.CUP_BRANCH_INS_ID_NM,PR.SETTLE_DT
         |			) B,
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
         |                            SETTLE_DT >= '$start_dt'
         |                        AND SETTLE_DT <= '$end_dt') RSLT
         |                WHERE
         |                    PRIZE.ACTIVITY_BEGIN_DT<= RSLT.SETTLE_DT
         |                AND PRIZE.ACTIVITY_END_DT>=RSLT.SETTLE_DT
         |                AND PRIZE.RUN_ST!='3'
         |                GROUP BY
         |                    PRIZE.CUP_BRANCH_INS_ID_NM,
         |                    RSLT.SETTLE_DT
         |			) C
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
         |SETTLE_DT >='$start_dt'AND SETTLE_DT <='$end_dt') RSLT
         |WHERE TRIM(D.INS_CN_NM) LIKE '%中国银联股份有限公司%分公司' OR  TRIM(D.INS_CN_NM) LIKE '%信息中心'
         |)  ta
         |GROUP BY
         |ta.CUP_BRANCH_INS_ID_NM,ta.SETTLE_DT
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
    * @param sqlContext
    * @return
    */
  def JOB_DM_74 (implicit sqlContext: HiveContext) = {
    UPSQL_JDBC.delete(s"DM_PRIZE_ACT_DLY","REPORT_DT",s"$start_dt",s"$end_dt")
    sqlContext.sql(s"use $hive_dbname")
    val results = sqlContext.sql(
      s"""
         | SELECT
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
         |                            SETTLE_DT >= '$start_dt'
         |                        AND SETTLE_DT <= '$end_dt') RSLT
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
         |                AND PR.SETTLE_DT >= '$start_dt'
         |                AND PR.SETTLE_DT <= '$end_dt'
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
    * @param sqlContext
    * @return
    */
  def JOB_DM_75 (implicit sqlContext: HiveContext) = {
    UPSQL_JDBC.delete(s"DM_DISC_ACT_DLY","REPORT_DT",s"$start_dt",s"$end_dt")
    sqlContext.sql(s"use $hive_dbname")
    val results = sqlContext.sql(
      s"""
         |SELECT
         |		B.ACTIVITY_ID as ACTIVITY_ID,
         |		B.ACTIVITY_NM as ACTIVITY_NM,
         |		B.SETTLE_DT as REPORT_DT,
         |		B.TRANS_CNT as TRANS_CNT,
         |		B.TRANS_AT as TRANS_AT,
         |		B.DISCOUNT_AT as DISCOUNT_AT
         |	FROM
         |	(
         |	SELECT
         |	A.ACTIVITY_ID                         AS ACTIVITY_ID,
         |	A.ACTIVITY_NM                        AS ACTIVITY_NM,
         |	A.SETTLE_DT                           AS SETTLE_DT,
         |	A.TRANS_CNT                           AS TRANS_CNT,
         |	A.TRANS_POS_AT_TTL                    AS TRANS_AT,
         |	(A.TRANS_POS_AT_TTL - A.TRANS_AT_TTL) AS DISCOUNT_AT,
         |	ROW_NUMBER() OVER(PARTITION BY A.SETTLE_DT ORDER BY A.TRANS_CNT DESC) AS RN
         |	FROM
         |		(
         |			SELECT
         |				DBI.LOC_ACTIVITY_ID                 AS ACTIVITY_ID,
         |				TRANSLATE(DBI.LOC_ACTIVITY_NM,' ','') AS ACTIVITY_NM,
         |				TRANS.SETTLE_DT,
         |				SUM (
         |					CASE
         |						WHEN TRANS.TRANS_ID IN ('V52','R22','V50','R20','S30')
         |						THEN -1
         |						ELSE 1
         |					END) AS TRANS_CNT,
         |				SUM (
         |					CASE
         |						WHEN TRANS.TRANS_ID IN ('V52','R22','V50','R20','S30')
         |						THEN -TRANS.TRANS_POS_AT
         |						ELSE TRANS.TRANS_POS_AT
         |					END) AS TRANS_POS_AT_TTL,
         |				SUM (
         |					CASE
         |						WHEN TRANS.TRANS_ID IN ('V52','R22','V50','R20','S30')
         |						THEN -TRANS.TRANS_AT
         |						ELSE TRANS.TRANS_AT
         |					END) AS TRANS_AT_TTL
         |			FROM
         |				HIVE_PRIZE_DISCOUNT_RESULT TRANS,
         |				HIVE_DISCOUNT_BAS_INF DBI
         |			WHERE
         |				TRANS.AGIO_APP_ID=DBI.LOC_ACTIVITY_ID
         |			AND TRANS.AGIO_APP_ID IS NOT NULL
         |			AND TRANS.SETTLE_DT >='$start_dt'
         |			AND TRANS.SETTLE_DT <='$end_dt'
         |			GROUP BY
         |				DBI.LOC_ACTIVITY_ID,
         |				TRANSLATE(DBI.LOC_ACTIVITY_NM,' ',''),
         |				TRANS.SETTLE_DT
         |		) A
         |	) B
         |	WHERE
         |		B.RN <= 10
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
    * @param sqlContext
    */
  def JOB_DM_76(implicit sqlContext: HiveContext) = {

    println("###JOB_DM_76(dm_auto_disc_cfp_tran->hive_prize_discount_result)")

    UPSQL_JDBC.delete("dm_auto_disc_cfp_tran","report_dt",start_dt,end_dt)

    var today_dt=start_dt
    if(interval>0 ){
      sqlContext.sql(s"use $hive_dbname")
      for(i <- 0 to interval){
        val results=sqlContext.sql(
          s"""
             |select
             |cup_branch_ins_id_nm as branch_nm,
             |max(case when cloud_pay_in='0' then 'apple pay'
             |     when cloud_pay_in='1' then 'hce'
             |     when cloud_pay_in in ('2','3') then '三星pay'
             |     when cloud_pay_in='4' then 'ic卡挥卡'
             |   else '--' end ) as cfp_sign,
             |settle_dt as  report_dt,
             |count(case when to_date(settle_dt) >= trunc('$today_dt','YYYY') and
             |       to_date(settle_dt) <='$today_dt' then pri_acct_no end) as year_tran_num,
             |count(case when to_date(settle_dt) = '$today_dt' then pri_acct_no end) as today_tran_num
             |
         |from hive_prize_discount_result
             |where  prod_in='0'
             |group by cup_branch_ins_id_nm,
             |case when cloud_pay_in='0' then 'Apple Pay'
             |     when cloud_pay_in='1' then 'HCE'
             |     when cloud_pay_in in ('2','3') then '三星pay'
             |     when cloud_pay_in='4' then 'IC卡挥卡'
             |   else '--' end , settle_dt
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
    * @param sqlContext
    * @return
    */
  def JOB_DM_78 (implicit sqlContext: HiveContext) = {
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
             |count(case when tempa.bind_dt<='$today_dt' then tempa.bind_card_no end ) as TOTAL_BIND_CNT,
             |count(case when tempa.bind_dt='$today_dt' then tempa.bind_card_no end ) as TODAY_CNT
             |from
             |(
             |select
             |tempc.bank_nm as bank_nm,
             |tempb.bind_dt as bind_dt,
             |(case when tempc.card_attr in ('01') then '借记卡'
             |      when tempc.card_attr in ('02', '03') then '贷记卡'
             |      else null end) as card_attr,
             |tempb.bind_card_no as bind_card_no
             |from
             |(
             |select
             |distinct(bind_card_no),
             |date(bind_ts) as bind_dt,
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
    * @param sqlContext
    * @return
    */
  def JOB_DM_86 (implicit sqlContext: HiveContext) = {
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
             |A.PHONE_LOCATION,
             |COUNT(distinct (CASE WHEN date(A.rec_upd_ts) > date(A.rec_crt_ts) THEN A.cdhd_usr_id END)) AS STOCK_NUM,
             |COUNT(distinct (CASE WHEN date(A.rec_upd_ts) = date(A.rec_crt_ts) THEN A.cdhd_usr_id END)) AS TODAY_NUM,
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
             |substr(rec_upd_ts,1,10)>='$today_dt'
             |and substr(rec_upd_ts,1,10)<='$today_dt'
             |and date(rec_upd_ts) > date(rec_crt_ts) and
             |(usr_st='1' or (usr_st='2' and note='BDYX_FREEZE')) and  realnm_in='01'
             |) A
             |LEFT JOIN
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
             |where  substr(rec_upd_ts,1,10)<'$today_dt' and
             |(usr_st='1' or (usr_st='2' and note='BDYX_FREEZE'))
             |and  realnm_in='01'
             |)tempa
             |GROUP BY tempa.PHONE_LOCATION
             |) B
             |ON A.PHONE_LOCATION=B.PHONE_LOCATION
             |GROUP BY A.PHONE_LOCATION,B.TOTAL_NUM
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
      hive_cashier_point_acct_oper_dtl
      hive_cashier_bas_inf
      hive_cdhd_cashier_maktg_reward_dtl
      hive_signer_log
    * @author tzq
    * @param sqlContext
    */
  def JOB_DM_87(implicit sqlContext: HiveContext) = {

    println("###JOB_DM_87(dm_cashier_stat_dly->hive_cashier_bas_inf+cup_branch_ins_id_nm+hive_cashier_point_acct_oper_dtl+hive_cdhd_cashier_maktg_reward_dtl+hive_signer_log)")

    UPSQL_JDBC.delete("dm_cashier_stat_dly","report_dt",start_dt,end_dt);

    var today_dt=start_dt
    if(interval>0 ){
      sqlContext.sql(s"use $hive_dbname")
      for(i <- 0 to interval){
        val results=sqlContext.sql(
          s"""
             |select
             |    a11.cup_branch_ins_id_nm                                      as cup_branch_ins_id_nm,
             |    '$today_dt'                                                   as report_dt,
             |    a11.cashier_cnt_tot                                           as cashier_cnt_tot,
             |    if(a12.act_cashier_cnt_tot is null,0,a12.act_cashier_cnt_tot)         as act_cashier_cnt_tot,
             |    if(a13.non_act_cashier_cnt_tot is null,0,a13.non_act_cashier_cnt_tot) as non_act_cashier_cnt_tot,
             |    if(a21.cashier_cnt_year is null,0,a21.cashier_cnt_year)              as cashier_cnt_year,
             |    if(a22.act_cashier_cnt_year is null,0,a22.act_cashier_cnt_year)         as act_cashier_cnt_year,
             |    if(a23.non_act_cashier_cnt_year is null,0,a23.non_act_cashier_cnt_year) as non_act_cashier_cnt_year,
             |    if(a31.cashier_cnt_mth is null,0,a31.cashier_cnt_mth)                as cashier_cnt_mth,
             |    if(a32.act_cashier_cnt_mth is null,0,a32.act_cashier_cnt_mth)         as act_cashier_cnt_mth,
             |    if(a33.non_act_cashier_cnt_mth is null,0,a33.non_act_cashier_cnt_mth) as non_act_cashier_cnt_mth,
             |    if(a4.pnt_acct_cashier_cnt is null,0,a4.pnt_acct_cashier_cnt)       as pnt_acct_cashier_cnt_tot,
             |    if(a5.reward_cashier_cnt_tot is null,0,a5.reward_cashier_cnt_tot)           as reward_cashier_cnt_tot,
             |    if(a6.reward_cdhd_cashier_cnt_tot is null,0,a6.reward_cdhd_cashier_cnt_tot) as reward_cdhd_cashier_cnt_tot,
             |    if(a7.sign_cashier_cnt_dly is null,0,a7.sign_cashier_cnt_dly)         as sign_cashier_cnt_dly,
             |    if(a81.cashier_cnt_dly is null,0,a81.cashier_cnt_dly)                 as cashier_cnt_dly,
             |    if(a82.act_cashier_cnt_dly is null,0,a82.act_cashier_cnt_dly)         as act_cashier_cnt_dly,
             |    if(a83.non_act_cashier_cnt_dly is null,0,a83.non_act_cashier_cnt_dly) as non_act_cashier_cnt_dly
             |from
             |
             |    (
             |        select
             |            cup_branch_ins_id_nm,
             |            count(distinct cashier_usr_id) as cashier_cnt_tot
             |        from
             |            hive_cashier_bas_inf
             |        where
             |            reg_dt<= '$today_dt'
             |        and usr_st not in ('4', '9')
             |        group by
             |            cup_branch_ins_id_nm) a11
             |left join
             |
             |    (
             |        select
             |            cup_branch_ins_id_nm,
             |            count(distinct cashier_usr_id) as act_cashier_cnt_tot
             |        from
             |            hive_cashier_bas_inf
             |        where
             |            reg_dt<= '$today_dt'
             |        and usr_st in ('1')
             |        group by
             |            cup_branch_ins_id_nm) a12
             |on
             |    (
             |        a11.cup_branch_ins_id_nm = a12.cup_branch_ins_id_nm)
             |left join
             |
             |    (
             |        select
             |            cup_branch_ins_id_nm,
             |            count(distinct cashier_usr_id) as non_act_cashier_cnt_tot
             |        from
             |            hive_cashier_bas_inf
             |        where
             |            reg_dt<= '$today_dt'
             |        and usr_st in ('0')
             |        group by
             |            cup_branch_ins_id_nm) a13
             |on
             |    (
             |        a11.cup_branch_ins_id_nm = a13.cup_branch_ins_id_nm)
             |left join
             |
             |    (
             |        select
             |            cup_branch_ins_id_nm,
             |            count(distinct cashier_usr_id) as cashier_cnt_year
             |        from
             |            hive_cashier_bas_inf
             |        where
             |            reg_dt <= '$today_dt'
             |        and reg_dt >= concat(substring('$today_dt',1,5),'01-01')
             |        and usr_st not in ('4', '9')
             |        group by
             |            cup_branch_ins_id_nm) a21
             |on
             |    (
             |        a11.cup_branch_ins_id_nm = a21.cup_branch_ins_id_nm)
             |left join
             |
             |    (
             |        select
             |            cup_branch_ins_id_nm,
             |            count(distinct cashier_usr_id) as act_cashier_cnt_year
             |        from
             |            hive_cashier_bas_inf
             |        where
             |            reg_dt <= '$today_dt'
             |        and reg_dt >= concat(substring('$today_dt',1,5),'01-01')
             |        and usr_st in ('1')
             |        group by
             |            cup_branch_ins_id_nm) a22
             |on
             |    (
             |        a11.cup_branch_ins_id_nm = a22.cup_branch_ins_id_nm)
             |left join
             |    (
             |        select
             |            cup_branch_ins_id_nm,
             |            count(distinct cashier_usr_id) as non_act_cashier_cnt_year
             |        from
             |            hive_cashier_bas_inf
             |        where
             |            reg_dt <= '$today_dt'
             |        and reg_dt >= concat(substring('$today_dt',1,5),'01-01')
             |        and usr_st in ('0')
             |        group by
             |            cup_branch_ins_id_nm) a23
             |on
             |    (
             |        a11.cup_branch_ins_id_nm = a23.cup_branch_ins_id_nm)
             |left join
             |    (
             |        select
             |            cup_branch_ins_id_nm,
             |            count(distinct cashier_usr_id) as cashier_cnt_mth
             |        from
             |            hive_cashier_bas_inf
             |        where
             |            reg_dt <= '$today_dt'
             |        and reg_dt >= concat(substring('$today_dt',1,8),'01')
             |        and usr_st not in ('4',
             |                           '9')
             |        group by
             |            cup_branch_ins_id_nm) a31
             |on
             |    (
             |        a11.cup_branch_ins_id_nm = a31.cup_branch_ins_id_nm)
             |left join
             |    (
             |        select
             |            cup_branch_ins_id_nm,
             |            count(distinct cashier_usr_id) as act_cashier_cnt_mth
             |        from
             |            hive_cashier_bas_inf
             |        where
             |            reg_dt <= '$today_dt'
             |        and reg_dt >= concat(substring('$today_dt',1,8),'01')
             |        and usr_st in ('1')
             |        group by
             |            cup_branch_ins_id_nm) a32
             |on
             |    (
             |        a11.cup_branch_ins_id_nm = a32.cup_branch_ins_id_nm)
             |left join
             |    (
             |        select
             |            cup_branch_ins_id_nm,
             |            count(distinct cashier_usr_id) as non_act_cashier_cnt_mth
             |        from
             |            hive_cashier_bas_inf
             |        where
             |            reg_dt <= '$today_dt'
             |        and reg_dt >= concat(substring('$today_dt',1,8),'01')
             |        and usr_st in ('0')
             |        group by
             |            cup_branch_ins_id_nm) a33
             |on
             |    (
             |        a11.cup_branch_ins_id_nm = a33.cup_branch_ins_id_nm)
             |left join
             |    (
             |        select
             |            b.cup_branch_ins_id_nm,
             |            count(distinct a.cashier_usr_id) as pnt_acct_cashier_cnt
             |        from
             |            (
             |                select distinct cashier_usr_id
             |                from
             |                    hive_cashier_point_acct_oper_dtl
             |                where
             |                    acct_oper_ts <= '$today_dt'
             |                and acct_oper_ts>= concat(substring('$today_dt',1,8),'01'))a
             |        inner join
             |            (
             |                select
             |                    cup_branch_ins_id_nm,
             |                    cashier_usr_id
             |                from
             |                    hive_cashier_bas_inf
             |                where
             |                    reg_dt<= '$today_dt'
             |                and usr_st not in ('4','9') )b
             |        on
             |            a.cashier_usr_id=b.cashier_usr_id
             |        group by
             |            b. cup_branch_ins_id_nm) a4
             |on
             |    (
             |        a11.cup_branch_ins_id_nm = a4.cup_branch_ins_id_nm)
             |left join
             |    (
             |        select
             |            b.cup_branch_ins_id_nm,
             |            count(distinct b.cashier_usr_id) as reward_cashier_cnt_tot
             |        from
             |            (
             |                select
             |                    mobile
             |                from
             |                    hive_cdhd_cashier_maktg_reward_dtl
             |                where
             |                    settle_dt<= '$today_dt'
             |                and rec_st='2'
             |                and activity_tp='004'
             |                group by
             |                    mobile ) a
             |        inner join
             |            hive_cashier_bas_inf b
             |        on
             |            a.mobile=b.mobile
             |        group by
             |            b.cup_branch_ins_id_nm) a5
             |on
             |    (
             |        a11.cup_branch_ins_id_nm = a5.cup_branch_ins_id_nm)
             |left join
             |    (
             |        select
             |            b.cup_branch_ins_id_nm,
             |            count(distinct b.cashier_usr_id) reward_cdhd_cashier_cnt_tot
             |        from
             |            (
             |                select distinct
             |                    mobile
             |                from
             |                    hive_cdhd_cashier_maktg_reward_dtl
             |                where
             |                    settle_dt<= '$today_dt'
             |                and rec_st='2'
             |                and activity_tp='004'
             |                group by mobile ) a
             |        inner join
             |            hive_cashier_bas_inf b
             |        on
             |            a.mobile=b.mobile
             |        inner join
             |            hive_pri_acct_inf c
             |        on
             |            a.mobile=c.mobile
             |        where
             |            c.usr_st='1'
             |        group by
             |            b.cup_branch_ins_id_nm) a6
             |on
             |    (
             |        a11.cup_branch_ins_id_nm = a6.cup_branch_ins_id_nm)
             |left join
             |    (
             |        select
             |            b.cup_branch_ins_id_nm,
             |            count(distinct b.cashier_usr_id) as sign_cashier_cnt_dly
             |        from
             |            (
             |                select
             |                    pri_acct_no
             |                from
             |                    hive_signer_log
             |                where
             |                    substr(cashier_trans_tm,1,8)= '$today_dt'
             |                group by
             |                    pri_acct_no ) a
             |        inner join
             |            (
             |                select
             |                    cup_branch_ins_id_nm,
             |                    cashier_usr_id,
             |                    bind_card_no
             |                from
             |                    hive_cashier_bas_inf
             |                where
             |                    reg_dt <= '$today_dt'
             |                and usr_st not in ('4', '9') ) b
             |        on
             |            a.pri_acct_no=b.bind_card_no
             |        group by
             |            b.cup_branch_ins_id_nm) a7
             |on
             |    (
             |        a11.cup_branch_ins_id_nm = a7.cup_branch_ins_id_nm)
             |left join
             |    (
             |        select
             |            cup_branch_ins_id_nm,
             |            count(distinct cashier_usr_id) as cashier_cnt_dly
             |        from
             |            hive_cashier_bas_inf
             |        where
             |            reg_dt= '$today_dt'
             |        and usr_st not in ('4','9')
             |        group by
             |            cup_branch_ins_id_nm) a81
             |on
             |    (
             |        a11.cup_branch_ins_id_nm = a81.cup_branch_ins_id_nm)
             |left join
             |    (
             |        select
             |            cup_branch_ins_id_nm,
             |            count(distinct cashier_usr_id) as act_cashier_cnt_dly
             |        from
             |            hive_cashier_bas_inf
             |        where
             |            reg_dt= '$today_dt'
             |        and usr_st in ('1')
             |        group by
             |            cup_branch_ins_id_nm) a82
             |on
             |    (
             |        a11.cup_branch_ins_id_nm = a82.cup_branch_ins_id_nm)
             |left join
             |    (
             |        select
             |            cup_branch_ins_id_nm,
             |            count(distinct cashier_usr_id) as non_act_cashier_cnt_dly
             |        from
             |            hive_cashier_bas_inf
             |        where
             |            reg_dt= '$today_dt'
             |        and usr_st in ('0')
             |        group by
             |            cup_branch_ins_id_nm) a83
             |on
             |    (
             |        a11.cup_branch_ins_id_nm = a83.cup_branch_ins_id_nm)
             |
         |
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