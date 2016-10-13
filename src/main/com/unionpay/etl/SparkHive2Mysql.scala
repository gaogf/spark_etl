package com.unionpay.etl

import com.unionpay.conf.ConfigurationManager
import com.unionpay.constant.Constants
import com.unionpay.jdbc.UPSQL_JDBC
import com.unionpay.jdbc.UPSQL_JDBC.DataFrame2Mysql
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
/**
  * 作业：抽取hive数据仓库中的数据到UPSQL数据库
  * Created by tzq on 2016/10/13.
  */
object SparkHive2Mysql {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("SparkHive2Mysql")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new HiveContext(sc)

    lazy val today_dt=ConfigurationManager.getProperty(Constants.TODAY_DT)
    lazy val start_dt=ConfigurationManager.getProperty(Constants.START_DT)
    lazy val end_dt=ConfigurationManager.getProperty(Constants.END_DT)

//--------TAN ZHENG QIANG---------------------------------------------------------
    JOB_DM_5(sqlContext,today_dt)
    JOB_DM_6(sqlContext,today_dt)
    JOB_DM_55(sqlContext,start_dt,end_dt)

    JOB_DM_62(sqlContext,today_dt)
//    JOB_DM_66   //未添加
//    JOB_DM_69   //未添加
//    JOB_DM_70   //未添加
//    JOB_DM_71   //未添加
//    JOB_DM_76   //未添加
//    JOB_DM_87   //未添加

//--------XUE TAI PING---------------------------------------------------------
//    JOB_DM_2   //未添加
//    JOB_DM_4   //未添加
//    JOB_DM_9   //未添加
//    JOB_DM_54  //未添加
//    JOB_DM_63  //未添加
//    JOB_DM_65  //未添加
//    JOB_DM_73  //未添加
//    JOB_DM_74  //未添加
//    JOB_DM_75  //未添加
//    JOB_DM_78  //未添加
//    JOB_DM_86  //未添加

//--------YANG XUE---------------------------------------------------------
//    JOB_DM_1   //未添加
//    JOB_DM_3   //未添加
//    JOB_DM_61  //未添加
//    JOB_DM_67  //未添加
//    JOB_DM_68  //未添加
//    JOB_DM_72  //未添加


  }

  //=========Created by tanzhengqiang====================================================================
  /**
    * JOB_DM_5  2016年9月27日 星期二
    * DM_USER_CARD_ISS
    * @param sqlContext
    */
  def JOB_DM_5(implicit sqlContext: HiveContext,today_dt:String) = {

    println("JOB_DM_5------->JOB_DM_5(dm_user_card_iss)")

    UPSQL_JDBC.delete("DM_USER_CARD_ISS","report_dt",today_dt,today_dt);

    sqlContext.sql("use upw_hive")

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

    println("###JOB_DM_5------results:"+results.count())
    if(!Option(results).isEmpty){
      results.save2Mysql("dm_user_card_iss")
    }else{
      println("指定的时间范围无数据插入！")
    }
  }



  /**
    * JOB_DM_6  2016年9月27日 星期二
    * dm_user_card_nature
    * @param sqlContext
    */
  def JOB_DM_6(implicit sqlContext: HiveContext,today_dt:String) = {

    println("JOB_DM_6------->JOB_DM_6(dm_user_card_nature)")

    UPSQL_JDBC.delete("dm_user_card_nature","report_dt",today_dt,today_dt)

    sqlContext.sql("use upw_hive")
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
         |(select cdhd_usr_id,rec_crt_ts from HIVE_PRI_ACCT_INF
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


    println("###JOB_DM_6------results:"+results.count())

    if(!Option(results).isEmpty){
      results.save2Mysql("dm_user_card_nature")

    }else{
      println("指定的时间范围无数据插入！")
    }

  }


  /**
    *
    * JOB_DM_55  2016-9-6
    *
    * DM_DISC_ACT_BRANCH_DLY
    * @param sqlContext
    */
  def JOB_DM_55(implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {

    println("###JOB_DM_55-----JOB_DM_55(dm_disc_act_branch_dly)")

    UPSQL_JDBC.delete("dm_disc_act_branch_dly","report_dt",start_dt,end_dt);

    sqlContext.sql("use upw_hive")

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
    * JOB_DM_62  2016-9-6
    * dm_usr_auther_nature_tie_card --> hive_card_bind_inf
    * @param sqlContext
    */
  def JOB_DM_62(implicit sqlContext: HiveContext,today_dt:String) = {
    println("###JOB_DM_62-----JOB_DM_62(dm_usr_auther_nature_tie_card->hive_card_bind_inf)")

    UPSQL_JDBC.delete("dm_usr_auther_nature_tie_card","report_dt",today_dt,today_dt)

    sqlContext.sql("use upw_hive")

    val results=sqlContext.sql(
      s"""
         |select
         |(case when card_auth_st='0' then   '默认'
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
         |group by (case when card_auth_st='0' then   '默认'
         |  when card_auth_st='1' then   '支付认证'
         |  when card_auth_st='2' then   '可信认证'
         |  when card_auth_st='3' then   '可信+支付认证'
         | else '未认证' end),card_attr
         |
      """.stripMargin)

    println("###JOB_DM_62------results:"+results.count())

    if(!Option(results).isEmpty){
      results.save2Mysql("dm_usr_auther_nature_tie_card")
    }else{
      println("指定的时间范围无数据插入！")
    }


  }




  //=========Created by xuetaiping====================================================================













  //=========Created by yangxue=======================================================================







}

// ## END LINE ##