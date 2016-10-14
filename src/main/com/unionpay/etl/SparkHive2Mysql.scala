package com.unionpay.etl

import com.unionpay.conf.ConfigurationManager
import com.unionpay.constant.Constants
import com.unionpay.jdbc.UPSQL_JDBC
import com.unionpay.jdbc.UPSQL_JDBC.DataFrame2Mysql
import com.unionpay.utils.DateUtils

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.log4j.Logger

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
   private lazy val interval=DateUtils.getIntervalDays(start_dt,end_dt)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkHive2Mysql").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new HiveContext(sc)


//--------TAN ZHENG QIANG---------------------------------------------------------

    JOB_DM_5(sqlContext,start_dt,end_dt)
    JOB_DM_6(sqlContext,start_dt,end_dt)
//    JOB_DM_55(sqlContext,start_dt,end_dt)
//
//    JOB_DM_62(sqlContext,today_dt)
//    JOB_DM_66(sqlContext,today_dt)
//    JOB_DM_69(sqlContext,start_dt,end_dt)
//    JOB_DM_70(sqlContext,start_dt,end_dt)
//    JOB_DM_71(sqlContext,start_dt,end_dt)
//    JOB_DM_76(sqlContext,today_dt)
//    JOB_DM_87(sqlContext,today_dt)

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

  /**
    * JOB_DM_5  2016年9月27日 星期二
    * dm_user_card_iss->hive_pri_acct_inf+hive_acc_trans+hive_card_bind_inf+hive_card_bin
    * @param sqlContext
    */
  def JOB_DM_5(implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {

    println("###JOB_DM_5(dm_user_card_iss->hive_pri_acct_inf+hive_acc_trans+hive_card_bind_inf+hive_card_bin)")

    //1.先删除作业指定开始日期和结束日期间的数据
    UPSQL_JDBC.delete("dm_user_card_iss","report_dt",start_dt,end_dt);


    var today_dt=start_dt
    //2.循环从指定的日期范围内抽取数据（单位：天）
    if(interval>0 ){
      sqlContext.sql("use upw_hive")
      for(i <- 0 to interval.toInt){
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
    * @param sqlContext
    */
  def JOB_DM_6(implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {

    println("###JOB_DM_6------->JOB_DM_6(dm_user_card_nature)")

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




  /**
    * JOB_DM_66 2016-09-07
    *
    * dm_coupon_cfp_tran
    * @param sqlContext
    */
  def JOB_DM_66(implicit sqlContext: HiveContext,today_dt:String) = {
    println("###JOB_DM_66-----JOB_DM_66(dm_coupon_cfp_tran)")

    // 删除一天的数据
    UPSQL_JDBC.delete("dm_coupon_cfp_tran","report_dt",today_dt,today_dt);

    sqlContext.sql("use upw_hive")

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

    println("###JOB_DM_66------results:"+results.count())

    if(!Option(results).isEmpty){
      results.save2Mysql("dm_coupon_cfp_tran")
    }else{
      println("指定的日期无数据插入！")
    }
  }

  /**
    * JOB_DM_69  2016-9-1
    * dm_disc_tkt_act_dly->hive_ticket_bill_bas_inf+hive_acc_trans
    * (使用分区part_trans_dt 中的数据)
    * @param sqlContext
    */
  def JOB_DM_69(implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {
    println("###JOB_DM_69-----JOB_DM_69(dm_disc_tkt_act_dly->hive_ticket_bill_bas_inf+hive_acc_trans)")

    UPSQL_JDBC.delete("dm_disc_tkt_act_dly","report_dt",start_dt,end_dt);

    sqlContext.sql("use upw_hive")

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
    * @param sqlContext
    */
  def JOB_DM_70(implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {
    println("###JOB_DM_70-----JOB_DM_70(dm_elec_tkt_act_dly->hive_ticket_bill_bas_inf+hive_acc_trans)")

    UPSQL_JDBC.delete("dm_elec_tkt_act_dly","report_dt",start_dt,end_dt);

    sqlContext.sql("use upw_hive")

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
    * @param sqlContext
    */
  def JOB_DM_71(implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {

    println("###JOB_DM_71-----JOB_DM_71(dm_vchr_tkt_act_dly->hive_ticket_bill_bas_inf+hive_acc_trans)")

    UPSQL_JDBC.delete("dm_vchr_tkt_act_dly","report_dt",start_dt,end_dt);

    sqlContext.sql("use upw_hive")
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
    * JOB_DM_76  2016-8-31
    * dm_auto_disc_cfp_tran->hive_prize_discount_result
    * @param sqlContext
    */
  def JOB_DM_76(implicit sqlContext: HiveContext,today_dt:String) = {

    println("###JOB_DM_76(dm_auto_disc_cfp_tran->hive_prize_discount_result)")

    UPSQL_JDBC.delete("dm_auto_disc_cfp_tran","report_dt",today_dt,today_dt);

    sqlContext.sql("use upw_hive")

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

    println("###JOB_DM_76------results:"+results.count())

    if(!Option(results).isEmpty){
      results.save2Mysql("dm_auto_disc_cfp_tran")
    }else{
      println("指定的时间范围无数据插入！")
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
    *
    * @param sqlContext
    */
  def JOB_DM_87(implicit sqlContext: HiveContext,today_dt:String) = {

    println("###JOB_DM_87(dm_cashier_stat_dly->hive_cashier_bas_inf+cup_branch_ins_id_nm+hive_cashier_point_acct_oper_dtl+hive_cdhd_cashier_maktg_reward_dtl+hive_signer_log)")

    UPSQL_JDBC.delete("dm_cashier_stat_dly","report_dt",today_dt,today_dt);

    sqlContext.sql("use upw_hive")

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
    println("###JOB_DM_87------results:"+results.count())
    if(!Option(results).isEmpty){
      results.save2Mysql("dm_cashier_stat_dly")
    }else{
      println("指定的时间范围无数据插入！")
    }

  }



}// ## END LINE ##