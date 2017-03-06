package com.unionpay.etl

import com.unionpay.conf.ConfigurationManager
import com.unionpay.constant.Constants
import com.unionpay.jdbc.UPSQL_TIMEPARAMS_JDBC
import com.unionpay.utils.DateUtils
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 作业：抽取钱包Hive数据到钱包Hive数据仓库
  */
object SparkUPWH2H {
  //指定HIVE数据库名
  private lazy val hive_dbname =ConfigurationManager.getProperty(Constants.HIVE_DBNAME)


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkUPWH2H")
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
//    start_dt=DateUtils.getYesterdayByJob(rowParams.getString(0))//获取开始日期：start_dt-1
//    end_dt=rowParams.getString(1)//结束日期

    /**
      * 从命令行获取当前JOB的执行起始和结束日期。
      * 无规则日期的增量数据抽取，主要用于数据初始化和调试。
      */
        if (args.length > 1) {
          start_dt = args(1)
          end_dt = args(2)
        } else {
          println("#### 缺少参数输入")
          println("#### 请指定 SparkUPWH2H 数据抽取的起始日期")
        }




    println(s"#### SparkUPWH2H 数据抽取的起始日期为: $start_dt --  $end_dt")

    val JobName = if(args.length>0) args(0) else None
    println(s"#### 当前执行JobName为： $JobName ####")
    JobName match {
      /**
        * 每日模板job
        */
      case "JOB_HV_4" => JOB_HV_4(sqlContext, start_dt, end_dt)  //CODE BY XTP
      case "JOB_HV_40"  => JOB_HV_40(sqlContext,start_dt,end_dt) //CODE BY TZQ
      case "JOB_HV_42"  => JOB_HV_42(sqlContext,start_dt,end_dt) //CODE BY TZQ
      case "JOB_HV_43"  => JOB_HV_43(sqlContext,start_dt,end_dt)  //CODE BY YX


      /**
        *  指标套表job
        */
      case "JOB_HV_77"  => JOB_HV_77(sqlContext,start_dt,end_dt) //CODE BY TZQ
      case "JOB_HV_78"  => JOB_HV_78(sqlContext,start_dt,end_dt) //CODE BY TZQ

      case _ => println("#### No Case Job,Please Input JobName")
    }

    sc.stop()

  }

  /**
    * JOB_HV_4/03-06  数据清洗
    * hive_acc_trans->hive_trans_dtl,hive_trans_log,hive_swt_log,hive_cups_trans
    * Code by Xue
    *
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    */
  def JOB_HV_4(implicit sqlContext: HiveContext, start_dt: String, end_dt: String) = {
    println("hive_acc_trans->hive_trans_dtl,hive_trans_log,hive_swt_log,hive_cups_trans")
    DateUtils.timeCost("JOB_HV_4") {
      println("#### JOB_HV_4 Extract data start at: " + start_dt + " and end :" + end_dt)

      sqlContext.sql(s"use $hive_dbname")

      val df2_1 = sqlContext.sql(s"select * from hive_trans_dtl where part_trans_dt>='$start_dt' and part_trans_dt<='$end_dt'")
      df2_1.registerTempTable("viw_chacc_acc_trans_dtl")
      println("临时表 viw_chacc_acc_trans_dtl 创建成功")

      val df2_2 = sqlContext.sql(s"select * from hive_trans_log  where part_msg_settle_dt>='$start_dt'  and part_msg_settle_dt <='$end_dt'")
      df2_2.registerTempTable("viw_chacc_acc_trans_log")
      println("临时表 viw_chacc_acc_trans_log 创建成功")

      val df2_3 = sqlContext.sql(s"select * from hive_swt_log where part_trans_dt>='$start_dt' and part_trans_dt<='$end_dt'")
      df2_3.registerTempTable("viw_chmgm_swt_log")
      println("临时表 viw_chmgm_swt_log 创建成功")


      val results = sqlContext.sql(
        s"""
           |select
           |ta.seq_id as seq_id,
           |trim(ta.cdhd_usr_id) as cdhd_usr_id,
           |trim(ta.card_no) as card_no,
           |trim(ta.trans_tfr_tm) as trans_tfr_tm,
           |trim(ta.sys_tra_no) as sys_tra_no,
           |trim(ta.acpt_ins_id_cd) as acpt_ins_id_cd,
           |trim(ta.fwd_ins_id_cd) as fwd_ins_id_cd,
           |trim(ta.rcv_ins_id_cd) as rcv_ins_id_cd,
           |trim(ta.oper_module) as oper_module,
           |case
           |	when
           |		substr(ta.trans_dt,1,4) between '0001' and '9999' and substr(ta.trans_dt,5,2) between '01' and '12' and
           |		substr(ta.trans_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(ta.trans_dt,1,4),substr(ta.trans_dt,5,2),substr(ta.trans_dt,7,2))),9,2)
           |	then concat_ws('-',substr(ta.trans_dt,1,4),substr(ta.trans_dt,5,2),substr(ta.trans_dt,7,2))
           |	else null
           |end as trans_dt,
           |case
           |	when
           |		substr(ta.trans_dt,1,4) between '0001' and '9999' and substr(ta.trans_dt,5,2) between '01' and '12' and
           |		substr(ta.trans_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(ta.trans_dt,1,4),substr(ta.trans_dt,5,2),substr(ta.trans_dt,7,2))),9,2) and
           |		substr(ta.trans_tm,1,2) between '00' and '24' and
           |		substr(ta.trans_tm,3,2) between '00' and '59' and
           |		substr(ta.trans_tm,5,2) between '00' and '59'
           |	then concat(concat_ws('-',substr(ta.trans_dt,1,4),substr(ta.trans_dt,5,2),substr(ta.trans_dt,7,2)), ' ', concat_ws(':',substr(ta.trans_tm,1,2),substr(ta.trans_tm,3,2),substr(ta.trans_tm,5,2)))
           |	else null
           |end as trans_tm,
           |trim(ta.buss_tp) as buss_tp,
           |trim(ta.um_trans_id) as um_trans_id,
           |trim(ta.swt_right_tp) as swt_right_tp,
           |trim(ta.bill_id) as bill_id,
           |ta.bill_nm as bill_nm,
           |trim(ta.chara_acct_tp) as chara_acct_tp,
           |case
           |	when length(trim(translate(trim(ta.trans_at),'-0123456789',' ')))=0 then trim(ta.trans_at)
           |	else null
           |end as trans_at,
           |ta.point_at as point_at,
           |trim(ta.mchnt_tp) as mchnt_tp,
           |trim(ta.resp_cd) as resp_cd,
           |trim(ta.card_accptr_term_id) as card_accptr_term_id,
           |trim(ta.card_accptr_cd) as card_accptr_cd,
           |ta.frist_trans_proc_start_ts  as frist_trans_proc_start_ts,
           |ta.second_trans_proc_start_ts as second_trans_proc_start_ts,
           |ta.third_trans_proc_start_ts as third_trans_proc_start_ts,
           |ta.trans_proc_end_ts as trans_proc_end_ts,
           |trim(ta.sys_det_cd) as sys_det_cd,
           |trim(ta.sys_err_cd) as sys_err_cd,
           |ta.rec_upd_ts as rec_upd_ts,
           |trim(ta.chara_acct_nm) as chara_acct_nm,
           |trim(ta.void_trans_tfr_tm) as void_trans_tfr_tm,
           |trim(ta.void_sys_tra_no) as void_sys_tra_no,
           |trim(ta.void_acpt_ins_id_cd) as void_acpt_ins_id_cd,
           |trim(ta.void_fwd_ins_id_cd) as void_fwd_ins_id_cd,
           |ta.orig_data_elemnt as orig_data_elemnt,
           |ta.rec_crt_ts as rec_crt_ts,
           |case
           |	when length(trim(translate(trim(ta.discount_at),'-0123456789',' ')))=0 then trim(ta.discount_at)
           |	else null
           |end as discount_at,
           |trim(ta.bill_item_id) as bill_item_id,
           |ta.chnl_inf_index as chnl_inf_index,
           |ta.bill_num as bill_num,
           |case
           |	when length(trim(translate(trim(ta.addn_discount_at),'-0123456789',' ')))=0 then trim(ta.addn_discount_at)
           |	else null
           |end as addn_discount_at,
           |trim(ta.pos_entry_md_cd) as pos_entry_md_cd,
           |ta.udf_fld as udf_fld,
           |trim(ta.card_accptr_nm_addr) as card_accptr_nm_addr,
           |ta.part_trans_dt as p_trans_dt,
           |trim(ta.msg_tp) as msg_tp,
           |trim(ta.cdhd_fk) as cdhd_fk,
           |trim(ta.bill_tp) as bill_tp,
           |trim(ta.bill_bat_no) as bill_bat_no,
           |ta.bill_inf as bill_inf,
           |trim(ta.proc_cd) as proc_cd,
           |trim(ta.trans_curr_cd) as trans_curr_cd,
           |case
           |	when length(trim(translate(trim(ta.settle_at),'-0123456789',' ')))=0 then trim(ta.settle_at)
           |	else null
           |end as settle_at,
           |trim(ta.settle_curr_cd) as settle_curr_cd,
           |trim(ta.card_accptr_local_tm) as card_accptr_local_tm,
           |trim(ta.card_accptr_local_dt) as card_accptr_local_dt,
           |trim(ta.expire_dt) as expire_dt,
           |case
           |	when
           |		substr(ta.msg_settle_dt,1,4) between '0001' and '9999' and substr(ta.msg_settle_dt,5,2) between '01' and '12' and
           |		substr(ta.msg_settle_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(ta.msg_settle_dt,1,4),substr(ta.msg_settle_dt,5,2),substr(ta.msg_settle_dt,7,2))),9,2)
           |	then concat_ws('-',substr(ta.msg_settle_dt,1,4),substr(ta.msg_settle_dt,5,2),substr(ta.msg_settle_dt,7,2))
           |	else null
           |end as msg_settle_dt,
           |trim(ta.pos_cond_cd) as pos_cond_cd,
           |trim(ta.pos_pin_capture_cd) as pos_pin_capture_cd,
           |trim(ta.retri_ref_no) as retri_ref_no,
           |trim(ta.auth_id_resp_cd) as auth_id_resp_cd,
           |trim(ta.notify_st) as notify_st,
           |ta.addn_private_data as addn_private_data,
           |trim(ta.addn_at) as addn_at,
           |ta.acct_id_1 as acct_id_1,
           |ta.acct_id_2 as acct_id_2,
           |ta.resv_fld as resv_fld,
           |ta.cdhd_auth_inf as cdhd_auth_inf,
           |case
           |	when
           |		substr(ta.sys_settle_dt,1,4) between '0001' and '9999' and substr(ta.sys_settle_dt,5,2) between '01' and '12' and
           |		substr(ta.sys_settle_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(ta.sys_settle_dt,1,4),substr(ta.sys_settle_dt,5,2),substr(ta.sys_settle_dt,7,2))),9,2)
           |	then concat_ws('-',substr(ta.sys_settle_dt,1,4),substr(ta.sys_settle_dt,5,2),substr(ta.sys_settle_dt,7,2))
           |	else null
           |end as sys_settle_dt,
           |trim(ta.recncl_in) as recncl_in,
           |trim(ta.match_in) as match_in,
           |trim(ta.sec_ctrl_inf) as sec_ctrl_inf,
           |trim(ta.card_seq) as card_seq,
           |ta.dtl_inq_data as dtl_inq_data,
           |tc.pri_key1 as pri_key1,
           |tc.fwd_chnl_head as fwd_chnl_head,
           |tc.chswt_plat_seq as chswt_plat_seq,
           |trim(tc.internal_trans_tp) as internal_trans_tp,
           |trim(tc.settle_trans_id) as settle_trans_id,
           |trim(tc.trans_tp) as trans_tp,
           |trim(tc.cups_settle_dt) as cups_settle_dt,
           |trim(tc.pri_acct_no) as pri_acct_no,
           |trim(tc.card_bin) as card_bin,
           |tc.req_trans_at as req_trans_at,
           |tc.resp_trans_at as resp_trans_at,
           |tc.trans_tot_at as trans_tot_at,
           |trim(tc.iss_ins_id_cd) as iss_ins_id_cd,
           |trim(tc.launch_trans_tm) as launch_trans_tm,
           |trim(tc.launch_trans_dt) as launch_trans_dt,
           |trim(tc.mchnt_cd) as mchnt_cd,
           |trim(tc.fwd_proc_in) as fwd_proc_in,
           |trim(tc.rcv_proc_in) as rcv_proc_in,
           |trim(tc.proj_tp) as proj_tp,
           |tc.usr_id as usr_id,
           |tc.conv_usr_id as conv_usr_id,
           |trim(tc.trans_st) as trans_st,
           |tc.inq_dtl_req as inq_dtl_req,
           |tc.inq_dtl_resp as inq_dtl_resp,
           |tc.iss_ins_resv as iss_ins_resv,
           |tc.ic_flds as ic_flds,
           |tc.cups_def_fld as cups_def_fld,
           |trim(tc.id_no) as id_no,
           |tc.cups_resv as cups_resv,
           |tc.acpt_ins_resv as acpt_ins_resv,
           |trim(tc.rout_ins_id_cd) as rout_ins_id_cd,
           |trim(tc.sub_rout_ins_id_cd) as sub_rout_ins_id_cd,
           |trim(tc.recv_access_resp_cd) as recv_access_resp_cd,
           |trim(tc.chswt_resp_cd) as chswt_resp_cd,
           |trim(tc.chswt_err_cd) as chswt_err_cd,
           |tc.resv_fld1 as resv_fld1,
           |tc.resv_fld2 as resv_fld2,
           |tc.to_ts as to_ts,
           |tc.external_amt as external_amt,
           |tc.card_pay_at as card_pay_at,
           |tc.right_purchase_at as right_purchase_at,
           |trim(tc.recv_second_resp_cd) as recv_second_resp_cd,
           |tc.req_acpt_ins_resv as req_acpt_ins_resv,
           |NULL as log_id,
           |NULL as conv_acct_no,
           |NULL as inner_pro_ind,
           |NULL as acct_proc_in,
           |NULL as order_id
           |
           |
           |from
           |(
           |select
           |tempd.seq_id                             ,
           |tempd.cdhd_usr_id                        ,
           |tempd.card_no                            ,
           |tempd.trans_tfr_tm                       ,
           |tempd.sys_tra_no                         ,
           |tempd.acpt_ins_id_cd                     ,
           |tempd.fwd_ins_id_cd                      ,
           |tempd.rcv_ins_id_cd                      ,
           |tempd.oper_module                        ,
           |tempd.trans_dt                           ,
           |tempd.trans_tm                           ,
           |tempd.buss_tp                            ,
           |tempd.um_trans_id                        ,
           |tempd.swt_right_tp                       ,
           |tempd.bill_id                            ,
           |tempd.bill_nm                            ,
           |tempd.chara_acct_tp                      ,
           |tempd.trans_at                           ,
           |tempd.point_at                           ,
           |tempd.mchnt_tp                           ,
           |tempd.resp_cd                            ,
           |tempd.card_accptr_term_id                ,
           |tempd.card_accptr_cd                     ,
           |tempd.trans_proc_start_ts as frist_trans_proc_start_ts,
           |NULL as second_trans_proc_start_ts       ,
           |NULL as third_trans_proc_start_ts        ,
           |tempd.trans_proc_end_ts                  ,
           |tempd.sys_det_cd                         ,
           |tempd.sys_err_cd                         ,
           |tempd.rec_upd_ts                         ,
           |tempd.chara_acct_nm                      ,
           |tempd.void_trans_tfr_tm                  ,
           |tempd.void_sys_tra_no                    ,
           |tempd.void_acpt_ins_id_cd                ,
           |tempd.void_fwd_ins_id_cd                 ,
           |tempd.orig_data_elemnt                   ,
           |tempd.rec_crt_ts                         ,
           |tempd.discount_at                        ,
           |tempd.bill_item_id                       ,
           |tempd.chnl_inf_index                     ,
           |tempd.bill_num                           ,
           |tempd.addn_discount_at                   ,
           |tempd.pos_entry_md_cd                    ,
           |tempd.udf_fld                            ,
           |tempd.card_accptr_nm_addr                ,
           |tempd.part_trans_dt                      ,
           |tempe.msg_tp                             ,
           |tempe.cdhd_fk                            ,
           |tempe.bill_tp                            ,
           |tempe.bill_bat_no                        ,
           |tempe.bill_inf                           ,
           |tempe.proc_cd                            ,
           |tempe.trans_curr_cd                      ,
           |tempe.settle_at                          ,
           |tempe.settle_curr_cd                     ,
           |tempe.card_accptr_local_tm               ,
           |tempe.card_accptr_local_dt               ,
           |tempe.expire_dt                          ,
           |tempe.msg_settle_dt                      ,
           |tempe.pos_cond_cd                        ,
           |tempe.pos_pin_capture_cd                 ,
           |tempe.retri_ref_no                       ,
           |tempe.auth_id_resp_cd                    ,
           |tempe.notify_st                          ,
           |tempe.addn_private_data                  ,
           |tempe.addn_at                            ,
           |tempe.acct_id_1                          ,
           |tempe.acct_id_2                          ,
           |tempe.resv_fld                           ,
           |tempe.cdhd_auth_inf                      ,
           |tempe.sys_settle_dt                      ,
           |tempe.recncl_in                          ,
           |tempe.match_in                           ,
           |tempe.sec_ctrl_inf                       ,
           |tempe.card_seq                           ,
           |tempe.dtl_inq_data
           |from
           |(select * from viw_chacc_acc_trans_dtl where um_trans_id<>'AC02202000') tempd
           |left join
           |(select * from viw_chacc_acc_trans_log where um_trans_id<>'AC02202000') tempe
           |on trim(tempd.trans_tfr_tm)=trim(tempe.trans_tfr_tm) and trim(tempd.sys_tra_no)=trim(tempe.sys_tra_no) and lpad(trim(tempd.acpt_ins_id_cd),11,'0')=lpad(trim(tempe.acpt_ins_id_cd),11,'0') and lpad(trim(tempd.fwd_ins_id_cd),11,'0')=lpad(trim(tempe.fwd_ins_id_cd),11,'0')
           |
           |union all
           |
           |select
           |tempc.seq_id                             ,
           |tempc.cdhd_usr_id                        ,
           |tempc.card_no                            ,
           |tempc.trans_tfr_tm                       ,
           |tempc.sys_tra_no                         ,
           |tempc.acpt_ins_id_cd                     ,
           |tempc.fwd_ins_id_cd                      ,
           |tempc.rcv_ins_id_cd                      ,
           |tempc.oper_module                        ,
           |tempc.trans_dt                           ,
           |tempc.trans_tm                           ,
           |tempc.buss_tp                            ,
           |tempc.um_trans_id                        ,
           |tempc.swt_right_tp                       ,
           |tempc.bill_id                            ,
           |tempc.bill_nm                            ,
           |tempc.chara_acct_tp                      ,
           |tempc.trans_at                           ,
           |tempc.point_at                           ,
           |tempc.mchnt_tp                           ,
           |tempc.resp_cd                            ,
           |tempc.card_accptr_term_id                ,
           |tempc.card_accptr_cd                     ,
           |max(case when tempc.rank=1 then tempc.trans_proc_start_ts else null end) as frist_trans_proc_start_ts,
           |max(case when tempc.rank=2 then tempc.trans_proc_start_ts else null end) as second_trans_proc_start_ts,
           |max(case when tempc.rank=3 then tempc.trans_proc_start_ts else null end) as third_trans_proc_start_ts,
           |tempc.trans_proc_end_ts                  ,
           |tempc.sys_det_cd                         ,
           |tempc.sys_err_cd                         ,
           |tempc.rec_upd_ts                         ,
           |tempc.chara_acct_nm                      ,
           |tempc.void_trans_tfr_tm                  ,
           |tempc.void_sys_tra_no                    ,
           |tempc.void_acpt_ins_id_cd                ,
           |tempc.void_fwd_ins_id_cd                 ,
           |tempc.orig_data_elemnt                   ,
           |tempc.rec_crt_ts                         ,
           |tempc.discount_at                        ,
           |tempc.bill_item_id                       ,
           |tempc.chnl_inf_index                     ,
           |tempc.bill_num                           ,
           |tempc.addn_discount_at                   ,
           |tempc.pos_entry_md_cd                    ,
           |tempc.udf_fld                            ,
           |tempc.card_accptr_nm_addr                ,
           |tempc.part_trans_dt                      ,
           |tempc.msg_tp                             ,
           |tempc.cdhd_fk                            ,
           |tempc.bill_tp                            ,
           |tempc.bill_bat_no                        ,
           |tempc.bill_inf                           ,
           |tempc.proc_cd                            ,
           |tempc.trans_curr_cd                      ,
           |tempc.settle_at                          ,
           |tempc.settle_curr_cd                     ,
           |tempc.card_accptr_local_tm               ,
           |tempc.card_accptr_local_dt               ,
           |tempc.expire_dt                          ,
           |tempc.msg_settle_dt                      ,
           |tempc.pos_cond_cd                        ,
           |tempc.pos_pin_capture_cd                 ,
           |tempc.retri_ref_no                       ,
           |tempc.auth_id_resp_cd                    ,
           |tempc.notify_st                          ,
           |tempc.addn_private_data                  ,
           |tempc.addn_at                            ,
           |tempc.acct_id_1                          ,
           |tempc.acct_id_2                          ,
           |tempc.resv_fld                           ,
           |tempc.cdhd_auth_inf                      ,
           |tempc.sys_settle_dt                      ,
           |tempc.recncl_in                          ,
           |tempc.match_in                           ,
           |tempc.sec_ctrl_inf                       ,
           |tempc.card_seq                           ,
           |tempc.dtl_inq_data
           |
           |from
           |(
           |select
           |tempw.seq_id                             ,
           |tempw.cdhd_usr_id                        ,
           |tempw.card_no                            ,
           |tempx.trans_tfr_tm                       ,
           |tempx.sys_tra_no                         ,
           |tempx.acpt_ins_id_cd                     ,
           |tempx.fwd_ins_id_cd                      ,
           |tempw.rcv_ins_id_cd                      ,
           |tempw.oper_module                        ,
           |tempw.trans_dt                           ,
           |tempw.trans_tm                           ,
           |tempw.buss_tp                            ,
           |tempw.um_trans_id                        ,
           |tempw.swt_right_tp                       ,
           |tempw.bill_id                            ,
           |tempw.bill_nm                            ,
           |tempw.chara_acct_tp                      ,
           |tempw.trans_at                           ,
           |tempw.point_at                           ,
           |tempw.mchnt_tp                           ,
           |tempw.resp_cd                            ,
           |tempw.card_accptr_term_id                ,
           |tempw.card_accptr_cd                     ,
           |tempw.trans_proc_start_ts                ,
           |tempw.trans_proc_end_ts                  ,
           |tempw.sys_det_cd                         ,
           |tempw.sys_err_cd                         ,
           |tempw.rec_upd_ts                         ,
           |tempw.chara_acct_nm                      ,
           |tempw.void_trans_tfr_tm                  ,
           |tempw.void_sys_tra_no                    ,
           |tempw.void_acpt_ins_id_cd                ,
           |tempw.void_fwd_ins_id_cd                 ,
           |tempw.orig_data_elemnt                   ,
           |tempw.rec_crt_ts                         ,
           |tempw.discount_at                        ,
           |tempw.bill_item_id                       ,
           |tempw.chnl_inf_index                     ,
           |tempw.bill_num                           ,
           |tempw.addn_discount_at                   ,
           |tempw.pos_entry_md_cd                    ,
           |tempw.udf_fld                            ,
           |tempw.card_accptr_nm_addr                ,
           |tempw.part_trans_dt                      ,
           |tempw.msg_tp                             ,
           |tempw.cdhd_fk                            ,
           |tempw.bill_tp                            ,
           |tempw.bill_bat_no                        ,
           |tempw.bill_inf                           ,
           |tempw.proc_cd                            ,
           |tempw.trans_curr_cd                      ,
           |tempw.settle_at                          ,
           |tempw.settle_curr_cd                     ,
           |tempw.card_accptr_local_tm               ,
           |tempw.card_accptr_local_dt               ,
           |tempw.expire_dt                          ,
           |tempw.msg_settle_dt                      ,
           |tempw.pos_cond_cd                        ,
           |tempw.pos_pin_capture_cd                 ,
           |tempw.retri_ref_no                       ,
           |tempw.auth_id_resp_cd                    ,
           |tempw.notify_st                          ,
           |tempw.addn_private_data                  ,
           |tempw.addn_at                            ,
           |tempw.acct_id_1                          ,
           |tempw.acct_id_2                          ,
           |tempw.resv_fld                           ,
           |tempw.cdhd_auth_inf                      ,
           |tempw.sys_settle_dt                      ,
           |tempw.recncl_in                          ,
           |tempw.match_in                           ,
           |tempw.sec_ctrl_inf                       ,
           |tempw.card_seq                           ,
           |tempw.dtl_inq_data                       ,
           |row_number() over (order by tempw.trans_proc_start_ts) rank
           |from
           |(
           |select
           |tempa.seq_id                             ,
           |tempa.cdhd_usr_id                        ,
           |tempa.card_no                            ,
           |tempa.trans_tfr_tm                       ,
           |tempa.sys_tra_no                         ,
           |tempa.acpt_ins_id_cd                     ,
           |tempa.fwd_ins_id_cd                      ,
           |tempa.rcv_ins_id_cd                      ,
           |tempa.oper_module                        ,
           |tempa.trans_dt                           ,
           |tempa.trans_tm                           ,
           |tempa.buss_tp                            ,
           |tempa.um_trans_id                        ,
           |tempa.swt_right_tp                       ,
           |tempa.bill_id                            ,
           |tempa.bill_nm                            ,
           |tempa.chara_acct_tp                      ,
           |tempa.trans_at                           ,
           |tempa.point_at                           ,
           |tempa.mchnt_tp                           ,
           |tempa.resp_cd                            ,
           |tempa.card_accptr_term_id                ,
           |tempa.card_accptr_cd                     ,
           |tempa.trans_proc_start_ts                ,
           |tempa.trans_proc_end_ts                  ,
           |tempa.sys_det_cd                         ,
           |tempa.sys_err_cd                         ,
           |tempa.rec_upd_ts                         ,
           |tempa.chara_acct_nm                      ,
           |tempa.void_trans_tfr_tm                  ,
           |tempa.void_sys_tra_no                    ,
           |tempa.void_acpt_ins_id_cd                ,
           |tempa.void_fwd_ins_id_cd                 ,
           |tempa.orig_data_elemnt                   ,
           |tempa.rec_crt_ts                         ,
           |tempa.discount_at                        ,
           |tempa.bill_item_id                       ,
           |tempa.chnl_inf_index                     ,
           |tempa.bill_num                           ,
           |tempa.addn_discount_at                   ,
           |tempa.pos_entry_md_cd                    ,
           |tempa.udf_fld                            ,
           |tempa.card_accptr_nm_addr                ,
           |tempa.part_trans_dt                      ,
           |tempb.msg_tp                             ,
           |tempb.cdhd_fk                            ,
           |tempb.bill_tp                            ,
           |tempb.bill_bat_no                        ,
           |tempb.bill_inf                           ,
           |tempb.proc_cd                            ,
           |tempb.trans_curr_cd                      ,
           |tempb.settle_at                          ,
           |tempb.settle_curr_cd                     ,
           |tempb.card_accptr_local_tm               ,
           |tempb.card_accptr_local_dt               ,
           |tempb.expire_dt                          ,
           |tempb.msg_settle_dt                      ,
           |tempb.pos_cond_cd                        ,
           |tempb.pos_pin_capture_cd                 ,
           |tempb.retri_ref_no                       ,
           |tempb.auth_id_resp_cd                    ,
           |tempb.notify_st                          ,
           |tempb.addn_private_data                  ,
           |tempb.addn_at                            ,
           |tempb.acct_id_1                          ,
           |tempb.acct_id_2                          ,
           |tempb.resv_fld                           ,
           |tempb.cdhd_auth_inf                      ,
           |tempb.sys_settle_dt                      ,
           |tempb.recncl_in                          ,
           |tempb.match_in                           ,
           |tempb.sec_ctrl_inf                       ,
           |tempb.card_seq                           ,
           |tempb.dtl_inq_data
           |from
           |(select * from viw_chacc_acc_trans_dtl where um_trans_id='AC02202000') tempa,
           |(select * from viw_chacc_acc_trans_log where um_trans_id='AC02202000') tempb
           |)
           |tempw
           |
           |right join
           |(
           |select
           |tempz.trans_tfr_tm,
           |tempz.sys_tra_no,
           |tempz.acpt_ins_id_cd,
           |tempz.fwd_ins_id_cd
           |from (select * from viw_chacc_acc_trans_dtl where um_trans_id='AC02202000') tempz,
           |(select * from viw_chacc_acc_trans_log where um_trans_id='AC02202000') tempy
           |group by
           |tempz.trans_tfr_tm,
           |tempz.sys_tra_no,
           |tempz.acpt_ins_id_cd,
           |tempz.fwd_ins_id_cd
           |having  count(*)>1
           |) tempx
           |on tempw.trans_tfr_tm=tempx.trans_tfr_tm and tempw.sys_tra_no=tempx.sys_tra_no and tempw.acpt_ins_id_cd=tempx.acpt_ins_id_cd and tempw.fwd_ins_id_cd=tempx.fwd_ins_id_cd
           |)
           |tempc
           |group by
           |tempc.seq_id                             ,
           |tempc.cdhd_usr_id                        ,
           |tempc.card_no                            ,
           |tempc.trans_tfr_tm                       ,
           |tempc.sys_tra_no                         ,
           |tempc.acpt_ins_id_cd                     ,
           |tempc.fwd_ins_id_cd                      ,
           |tempc.rcv_ins_id_cd                      ,
           |tempc.oper_module                        ,
           |tempc.trans_dt                           ,
           |tempc.trans_tm                           ,
           |tempc.buss_tp                            ,
           |tempc.um_trans_id                        ,
           |tempc.swt_right_tp                       ,
           |tempc.bill_id                            ,
           |tempc.bill_nm                            ,
           |tempc.chara_acct_tp                      ,
           |tempc.trans_at                           ,
           |tempc.point_at                           ,
           |tempc.mchnt_tp                           ,
           |tempc.resp_cd                            ,
           |tempc.card_accptr_term_id                ,
           |tempc.card_accptr_cd                     ,
           |tempc.trans_proc_end_ts                  ,
           |tempc.sys_det_cd                         ,
           |tempc.sys_err_cd                         ,
           |tempc.rec_upd_ts                         ,
           |tempc.chara_acct_nm                      ,
           |tempc.void_trans_tfr_tm                  ,
           |tempc.void_sys_tra_no                    ,
           |tempc.void_acpt_ins_id_cd                ,
           |tempc.void_fwd_ins_id_cd                 ,
           |tempc.orig_data_elemnt                   ,
           |tempc.rec_crt_ts                         ,
           |tempc.discount_at                        ,
           |tempc.bill_item_id                       ,
           |tempc.chnl_inf_index                     ,
           |tempc.bill_num                           ,
           |tempc.addn_discount_at                   ,
           |tempc.pos_entry_md_cd                    ,
           |tempc.udf_fld                            ,
           |tempc.card_accptr_nm_addr                ,
           |tempc.part_trans_dt                      ,
           |tempc.msg_tp                             ,
           |tempc.cdhd_fk                            ,
           |tempc.bill_tp                            ,
           |tempc.bill_bat_no                        ,
           |tempc.bill_inf                           ,
           |tempc.proc_cd                            ,
           |tempc.trans_curr_cd                      ,
           |tempc.settle_at                          ,
           |tempc.settle_curr_cd                     ,
           |tempc.card_accptr_local_tm               ,
           |tempc.card_accptr_local_dt               ,
           |tempc.expire_dt                          ,
           |tempc.msg_settle_dt                      ,
           |tempc.pos_cond_cd                        ,
           |tempc.pos_pin_capture_cd                 ,
           |tempc.retri_ref_no                       ,
           |tempc.auth_id_resp_cd                    ,
           |tempc.notify_st                          ,
           |tempc.addn_private_data                  ,
           |tempc.addn_at                            ,
           |tempc.acct_id_1                          ,
           |tempc.acct_id_2                          ,
           |tempc.resv_fld                           ,
           |tempc.cdhd_auth_inf                      ,
           |tempc.sys_settle_dt                      ,
           |tempc.recncl_in                          ,
           |tempc.match_in                           ,
           |tempc.sec_ctrl_inf                       ,
           |tempc.card_seq                           ,
           |tempc.dtl_inq_data
           |
           |)ta
           |
           |left join  ( select * from viw_chmgm_swt_log ) tc
           |on trim(ta.trans_tfr_tm)=trim(tc.tfr_dt_tm)  and  trim(ta.sys_tra_no)=trim(tc.sys_tra_no) and lpad(trim(ta.acpt_ins_id_cd),11,'0')=lpad(trim(tc.acpt_ins_id_cd),11,'0') and lpad(trim(ta.fwd_ins_id_cd),11,'0')=lpad(trim(tc.msg_fwd_ins_id_cd),11,'0')
           | """.stripMargin)
      println("#### JOB_HV_4 spark sql 逻辑完成的系统时间为:" + DateUtils.getCurrentSystemTime())

      results.registerTempTable("spark_acc_trans")
      println("#### JOB_HV_4 spark sql 临时表生成时间为:" + DateUtils.getCurrentSystemTime())

      if (!Option(results).isEmpty) {
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql(
          s"""
             |insert overwrite table hive_acc_trans partition (part_trans_dt)
             | select
             | seq_id,
             |cdhd_usr_id,
             |card_no,
             |trans_tfr_tm,
             |sys_tra_no,
             |acpt_ins_id_cd,
             |fwd_ins_id_cd,
             |rcv_ins_id_cd,
             |oper_module,
             |trans_dt,
             |trans_tm,
             |buss_tp,
             |um_trans_id,
             |swt_right_tp,
             |bill_id,
             |bill_nm,
             |chara_acct_tp,
             |trans_at,
             |point_at,
             |mchnt_tp,
             |resp_cd,
             |card_accptr_term_id,
             |card_accptr_cd,
             |frist_trans_proc_start_ts,
             |second_trans_proc_start_ts,
             |third_trans_proc_start_ts,
             |trans_proc_end_ts,
             |sys_det_cd,
             |sys_err_cd,
             |rec_upd_ts,
             |chara_acct_nm,
             | void_trans_tfr_tm,
             | void_sys_tra_no,
             | void_acpt_ins_id_cd,
             | void_fwd_ins_id_cd,
             | orig_data_elemnt,
             |rec_crt_ts,
             |discount_at,
             |bill_item_id,
             | chnl_inf_index,
             | bill_num,
             |addn_discount_at,
             | pos_entry_md_cd,
             | udf_fld,
             | card_accptr_nm_addr,
             | msg_tp,
             |cdhd_fk,
             |bill_tp,
             |bill_bat_no,
             |bill_inf,
             | proc_cd,
             |trans_curr_cd,
             |settle_at,
             | settle_curr_cd,
             |card_accptr_local_tm,
             | card_accptr_local_dt,
             |expire_dt,
             | msg_settle_dt,
             |pos_cond_cd,
             |pos_pin_capture_cd,
             | retri_ref_no,
             | auth_id_resp_cd,
             | notify_st,
             |addn_private_data,
             |addn_at,
             |acct_id_1,
             |acct_id_2,
             |resv_fld,
             |cdhd_auth_inf,
             |sys_settle_dt,
             |recncl_in,
             |match_in,
             | sec_ctrl_inf,
             |card_seq,
             | dtl_inq_data,
             | pri_key1,
             | fwd_chnl_head,
             |chswt_plat_seq,
             |internal_trans_tp,
             |settle_trans_id,
             |trans_tp,
             | cups_settle_dt,
             | pri_acct_no,
             |card_bin,
             | req_trans_at,
             | resp_trans_at,
             |trans_tot_at,
             |iss_ins_id_cd,
             | launch_trans_tm,
             | launch_trans_dt,
             |mchnt_cd,
             | fwd_proc_in,
             |rcv_proc_in,
             |proj_tp,
             | usr_id,
             |conv_usr_id,
             | trans_st,
             |inq_dtl_req,
             |inq_dtl_resp,
             | iss_ins_resv,
             | ic_flds,
             |cups_def_fld,
             |id_no,
             |cups_resv,
             |acpt_ins_resv,
             |rout_ins_id_cd,
             |sub_rout_ins_id_cd,
             |recv_access_resp_cd,
             |chswt_resp_cd,
             | chswt_err_cd,
             |resv_fld1,
             | resv_fld2,
             | to_ts,
             | external_amt,
             |card_pay_at,
             |right_purchase_at,
             | recv_second_resp_cd,
             |req_acpt_ins_resv,
             |NULL as log_id,
             |NULL as conv_acct_no,
             |NULL as inner_pro_ind,
             |NULL as acct_proc_in,
             |NULL as order_id,
             |p_trans_dt
             |from spark_acc_trans
        """.stripMargin)
        println("#### JOB_HV_4 插入分区完成的时间为：" + DateUtils.getCurrentSystemTime())
      } else {
        println("#### JOB_HV_4 加载的表spark_acc_trans中无数据！")
      }
    }

  }

/*
// 因为HIVE_CUPS_TRANS这张表数据过大，所以暂不做处理，测试通过
  /**
    * JOB_HV_4/03-06  数据清洗
    * hive_acc_trans->hive_trans_dtl,hive_trans_log,hive_swt_log,hive_cups_trans
    * Code by Xue
    *
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    */
  def JOB_HV_4(implicit sqlContext: HiveContext, start_dt: String, end_dt: String) = {
    println("hive_acc_trans->hive_trans_dtl,hive_trans_log,hive_swt_log,hive_cups_trans")
    DateUtils.timeCost("JOB_HV_4") {
      println("#### JOB_HV_4 Extract data start at: " + start_dt + " and end :" + end_dt)

      sqlContext.sql(s"use $hive_dbname")

      val df2_1 = sqlContext.sql(s"select * from hive_trans_dtl where part_trans_dt>='$start_dt' and part_trans_dt<='$end_dt'")
      df2_1.registerTempTable("viw_chacc_acc_trans_dtl")
      println("临时表 viw_chacc_acc_trans_dtl 创建成功")

      val df2_2 = sqlContext.sql(s"select * from hive_trans_log  where part_msg_settle_dt>='$start_dt'  and part_msg_settle_dt <='$end_dt'")
      df2_2.registerTempTable("viw_chacc_acc_trans_log")
      println("临时表 viw_chacc_acc_trans_log 创建成功")

      val df2_3 = sqlContext.sql(s"select * from hive_swt_log where part_trans_dt>='$start_dt' and part_trans_dt<='$end_dt'")
      df2_3.registerTempTable("viw_chmgm_swt_log")
      println("临时表 viw_chmgm_swt_log 创建成功")

      val df2_4 = sqlContext.sql(s"select * from hive_cups_trans where part_settle_dt>='$start_dt' and part_settle_dt<='$end_dt'")
      df2_4.registerTempTable("spark_hive_cups_trans")
      println("临时表 spark_hive_cups_trans 创建成功")


      val results = sqlContext.sql(
        s"""
           |select
           |ta.seq_id as seq_id,
           |trim(ta.cdhd_usr_id) as cdhd_usr_id,
           |trim(ta.card_no) as card_no,
           |trim(ta.trans_tfr_tm) as trans_tfr_tm,
           |trim(ta.sys_tra_no) as sys_tra_no,
           |trim(ta.acpt_ins_id_cd) as acpt_ins_id_cd,
           |trim(ta.fwd_ins_id_cd) as fwd_ins_id_cd,
           |trim(ta.rcv_ins_id_cd) as rcv_ins_id_cd,
           |trim(ta.oper_module) as oper_module,
           |case
           |	when
           |		substr(ta.trans_dt,1,4) between '0001' and '9999' and substr(ta.trans_dt,5,2) between '01' and '12' and
           |		substr(ta.trans_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(ta.trans_dt,1,4),substr(ta.trans_dt,5,2),substr(ta.trans_dt,7,2))),9,2)
           |	then concat_ws('-',substr(ta.trans_dt,1,4),substr(ta.trans_dt,5,2),substr(ta.trans_dt,7,2))
           |	else null
           |end as trans_dt,
           |case
           |	when
           |		substr(ta.trans_dt,1,4) between '0001' and '9999' and substr(ta.trans_dt,5,2) between '01' and '12' and
           |		substr(ta.trans_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(ta.trans_dt,1,4),substr(ta.trans_dt,5,2),substr(ta.trans_dt,7,2))),9,2) and
           |		substr(ta.trans_tm,1,2) between '00' and '24' and
           |		substr(ta.trans_tm,3,2) between '00' and '59' and
           |		substr(ta.trans_tm,5,2) between '00' and '59'
           |	then concat(concat_ws('-',substr(ta.trans_dt,1,4),substr(ta.trans_dt,5,2),substr(ta.trans_dt,7,2)), ' ', concat_ws(':',substr(ta.trans_tm,1,2),substr(ta.trans_tm,3,2),substr(ta.trans_tm,5,2)))
           |	else null
           |end as trans_tm,
           |trim(ta.buss_tp) as buss_tp,
           |trim(ta.um_trans_id) as um_trans_id,
           |trim(ta.swt_right_tp) as swt_right_tp,
           |trim(ta.bill_id) as bill_id,
           |ta.bill_nm as bill_nm,
           |trim(ta.chara_acct_tp) as chara_acct_tp,
           |case
           |	when length(trim(translate(trim(ta.trans_at),'-0123456789',' ')))=0 then trim(ta.trans_at)
           |	else null
           |end as trans_at,
           |ta.point_at as point_at,
           |trim(ta.mchnt_tp) as mchnt_tp,
           |trim(ta.resp_cd) as resp_cd,
           |trim(ta.card_accptr_term_id) as card_accptr_term_id,
           |trim(ta.card_accptr_cd) as card_accptr_cd,
           |ta.frist_trans_proc_start_ts  as frist_trans_proc_start_ts,
           |ta.second_trans_proc_start_ts as second_trans_proc_start_ts,
           |ta.third_trans_proc_start_ts as third_trans_proc_start_ts,
           |ta.trans_proc_end_ts as trans_proc_end_ts,
           |trim(ta.sys_det_cd) as sys_det_cd,
           |trim(ta.sys_err_cd) as sys_err_cd,
           |ta.rec_upd_ts as rec_upd_ts,
           |trim(ta.chara_acct_nm) as chara_acct_nm,
           |trim(ta.void_trans_tfr_tm) as void_trans_tfr_tm,
           |trim(ta.void_sys_tra_no) as void_sys_tra_no,
           |trim(ta.void_acpt_ins_id_cd) as void_acpt_ins_id_cd,
           |trim(ta.void_fwd_ins_id_cd) as void_fwd_ins_id_cd,
           |ta.orig_data_elemnt as orig_data_elemnt,
           |ta.rec_crt_ts as rec_crt_ts,
           |case
           |	when length(trim(translate(trim(ta.discount_at),'-0123456789',' ')))=0 then trim(ta.discount_at)
           |	else null
           |end as discount_at,
           |trim(ta.bill_item_id) as bill_item_id,
           |ta.chnl_inf_index as chnl_inf_index,
           |ta.bill_num as bill_num,
           |case
           |	when length(trim(translate(trim(ta.addn_discount_at),'-0123456789',' ')))=0 then trim(ta.addn_discount_at)
           |	else null
           |end as addn_discount_at,
           |trim(ta.pos_entry_md_cd) as pos_entry_md_cd,
           |ta.udf_fld as udf_fld,
           |trim(ta.card_accptr_nm_addr) as card_accptr_nm_addr,
           |ta.part_trans_dt as p_trans_dt,
           |trim(ta.msg_tp) as msg_tp,
           |trim(ta.cdhd_fk) as cdhd_fk,
           |trim(ta.bill_tp) as bill_tp,
           |trim(ta.bill_bat_no) as bill_bat_no,
           |ta.bill_inf as bill_inf,
           |trim(ta.proc_cd) as proc_cd,
           |trim(ta.trans_curr_cd) as trans_curr_cd,
           |case
           |	when length(trim(translate(trim(ta.settle_at),'-0123456789',' ')))=0 then trim(ta.settle_at)
           |	else null
           |end as settle_at,
           |trim(ta.settle_curr_cd) as settle_curr_cd,
           |trim(ta.card_accptr_local_tm) as card_accptr_local_tm,
           |trim(ta.card_accptr_local_dt) as card_accptr_local_dt,
           |trim(ta.expire_dt) as expire_dt,
           |case
           |	when
           |		substr(ta.msg_settle_dt,1,4) between '0001' and '9999' and substr(ta.msg_settle_dt,5,2) between '01' and '12' and
           |		substr(ta.msg_settle_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(ta.msg_settle_dt,1,4),substr(ta.msg_settle_dt,5,2),substr(ta.msg_settle_dt,7,2))),9,2)
           |	then concat_ws('-',substr(ta.msg_settle_dt,1,4),substr(ta.msg_settle_dt,5,2),substr(ta.msg_settle_dt,7,2))
           |	else null
           |end as msg_settle_dt,
           |trim(ta.pos_cond_cd) as pos_cond_cd,
           |trim(ta.pos_pin_capture_cd) as pos_pin_capture_cd,
           |trim(ta.retri_ref_no) as retri_ref_no,
           |trim(ta.auth_id_resp_cd) as auth_id_resp_cd,
           |trim(ta.notify_st) as notify_st,
           |ta.addn_private_data as addn_private_data,
           |trim(ta.addn_at) as addn_at,
           |ta.acct_id_1 as acct_id_1,
           |ta.acct_id_2 as acct_id_2,
           |ta.resv_fld as resv_fld,
           |ta.cdhd_auth_inf as cdhd_auth_inf,
           |case
           |	when
           |		substr(ta.sys_settle_dt,1,4) between '0001' and '9999' and substr(ta.sys_settle_dt,5,2) between '01' and '12' and
           |		substr(ta.sys_settle_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(ta.sys_settle_dt,1,4),substr(ta.sys_settle_dt,5,2),substr(ta.sys_settle_dt,7,2))),9,2)
           |	then concat_ws('-',substr(ta.sys_settle_dt,1,4),substr(ta.sys_settle_dt,5,2),substr(ta.sys_settle_dt,7,2))
           |	else null
           |end as sys_settle_dt,
           |trim(ta.recncl_in) as recncl_in,
           |trim(ta.match_in) as match_in,
           |trim(ta.sec_ctrl_inf) as sec_ctrl_inf,
           |trim(ta.card_seq) as card_seq,
           |ta.dtl_inq_data as dtl_inq_data,
           |tc.pri_key1 as pri_key1,
           |tc.fwd_chnl_head as fwd_chnl_head,
           |tc.chswt_plat_seq as chswt_plat_seq,
           |trim(tc.internal_trans_tp) as internal_trans_tp,
           |trim(tc.settle_trans_id) as settle_trans_id,
           |trim(tc.trans_tp) as trans_tp,
           |trim(tc.cups_settle_dt) as cups_settle_dt,
           |trim(tc.pri_acct_no) as pri_acct_no,
           |trim(tc.card_bin) as card_bin,
           |tc.req_trans_at as req_trans_at,
           |tc.resp_trans_at as resp_trans_at,
           |tc.trans_tot_at as trans_tot_at,
           |trim(tc.iss_ins_id_cd) as iss_ins_id_cd,
           |trim(tc.launch_trans_tm) as launch_trans_tm,
           |trim(tc.launch_trans_dt) as launch_trans_dt,
           |trim(tc.mchnt_cd) as mchnt_cd,
           |trim(tc.fwd_proc_in) as fwd_proc_in,
           |trim(tc.rcv_proc_in) as rcv_proc_in,
           |trim(tc.proj_tp) as proj_tp,
           |tc.usr_id as usr_id,
           |tc.conv_usr_id as conv_usr_id,
           |trim(tc.trans_st) as trans_st,
           |tc.inq_dtl_req as inq_dtl_req,
           |tc.inq_dtl_resp as inq_dtl_resp,
           |tc.iss_ins_resv as iss_ins_resv,
           |tc.ic_flds as ic_flds,
           |tc.cups_def_fld as cups_def_fld,
           |trim(tc.id_no) as id_no,
           |tc.cups_resv as cups_resv,
           |tc.acpt_ins_resv as acpt_ins_resv,
           |trim(tc.rout_ins_id_cd) as rout_ins_id_cd,
           |trim(tc.sub_rout_ins_id_cd) as sub_rout_ins_id_cd,
           |trim(tc.recv_access_resp_cd) as recv_access_resp_cd,
           |trim(tc.chswt_resp_cd) as chswt_resp_cd,
           |trim(tc.chswt_err_cd) as chswt_err_cd,
           |tc.resv_fld1 as resv_fld1,
           |tc.resv_fld2 as resv_fld2,
           |tc.to_ts as to_ts,
           |tc.external_amt as external_amt,
           |tc.card_pay_at as card_pay_at,
           |tc.right_purchase_at as right_purchase_at,
           |trim(tc.recv_second_resp_cd) as recv_second_resp_cd,
           |tc.req_acpt_ins_resv as req_acpt_ins_resv,
           |NULL as log_id,
           |NULL as conv_acct_no,
           |NULL as inner_pro_ind,
           |NULL as acct_proc_in,
           |NULL as order_id,
           |te.settle_dt                as      settle_dt                       ,
           |te.pri_key                  as      cups_pri_key                    ,
           |te.log_cd                   as      cups_log_cd                     ,
           |te.settle_tp                as      cups_settle_tp                  ,
           |te.settle_cycle             as      cups_settle_cycle               ,
           |te.block_id                 as      cups_block_id                   ,
           |te.orig_key                 as      cups_orig_key                   ,
           |te.related_key              as      cups_related_key                ,
           |te.trans_fwd_st             as      cups_trans_fwd_st               ,
           |te.trans_rcv_st             as      cups_trans_rcv_st               ,
           |te.sms_dms_conv_in          as      cups_sms_dms_conv_in            ,
           |te.fee_in                   as      cups_fee_in                     ,
           |te.cross_dist_in            as      cups_cross_dist_in              ,
           |te.orig_acpt_sdms_in        as      cups_orig_acpt_sdms_in          ,
           |te.tfr_in_in                as      cups_tfr_in_in                  ,
           |te.trans_md                 as      cups_trans_md                   ,
           |te.source_region_cd         as      cups_source_region_cd           ,
           |te.dest_region_cd           as      cups_dest_region_cd             ,
           |te.cups_card_in             as      cups_cups_card_in               ,
           |te.cups_sig_card_in         as      cups_cups_sig_card_in           ,
           |te.card_class               as      cups_card_class                 ,
           |te.card_attr                as      cups_card_attr                  ,
           |te.sti_in                   as      cups_sti_in                     ,
           |te.trans_proc_in            as      cups_trans_proc_in              ,
           |te.acq_ins_id_cd            as      cups_acq_ins_id_cd              ,
           |te.acq_ins_tp               as      cups_acq_ins_tp                 ,
           |te.fwd_ins_tp               as      cups_fwd_ins_tp                 ,
           |te.rcv_ins_id_cd            as      cups_rcv_ins_id_cd              ,
           |te.rcv_ins_tp               as      cups_rcv_ins_tp                 ,
           |te.iss_ins_id_cd            as      cups_iss_ins_id_cd              ,
           |te.iss_ins_tp               as      cups_iss_ins_tp                 ,
           |te.related_ins_id_cd        as      cups_related_ins_id_cd          ,
           |te.related_ins_tp           as      cups_related_ins_tp             ,
           |te.acpt_ins_tp              as      cups_acpt_ins_tp                ,
           |te.pri_acct_no              as      cups_pri_acct_no                ,
           |te.pri_acct_no_conv         as      cups_pri_acct_no_conv           ,
           |te.sys_tra_no_conv          as      cups_sys_tra_no_conv            ,
           |te.sw_sys_tra_no            as      cups_sw_sys_tra_no              ,
           |te.auth_dt                  as      cups_auth_dt                    ,
           |te.auth_id_resp_cd          as      cups_auth_id_resp_cd            ,
           |te.resp_cd1                 as      cups_resp_cd1                   ,
           |te.resp_cd2                 as      cups_resp_cd2                   ,
           |te.resp_cd3                 as      cups_resp_cd3                   ,
           |te.resp_cd4                 as      cups_resp_cd4                   ,
           |te.cu_trans_st              as      cups_cu_trans_st                ,
           |te.sti_takeout_in           as      cups_sti_takeout_in             ,
           |te.trans_id                 as      cups_trans_id                   ,
           |te.trans_tp                 as      cups_trans_tp                   ,
           |te.trans_chnl               as      cups_trans_chnl                 ,
           |te.card_media               as      cups_card_media                 ,
           |te.card_media_proc_md       as      cups_card_media_proc_md         ,
           |te.card_brand               as      cups_card_brand                 ,
           |te.expire_seg               as      cups_expire_seg                 ,
           |te.trans_id_conv            as      cups_trans_id_conv              ,
           |te.settle_mon               as      cups_settle_mon                 ,
           |te.settle_d                 as      cups_settle_d                   ,
           |te.orig_settle_dt           as      cups_orig_settle_dt             ,
           |te.settle_fwd_ins_id_cd     as      cups_settle_fwd_ins_id_cd       ,
           |te.settle_rcv_ins_id_cd     as      cups_settle_rcv_ins_id_cd       ,
           |te.trans_at                 as      cups_trans_at                   ,
           |te.orig_trans_at            as      cups_orig_trans_at              ,
           |te.trans_conv_rt            as      cups_trans_conv_rt              ,
           |te.trans_curr_cd            as      cups_trans_curr_cd              ,
           |te.cdhd_fee_at              as      cups_cdhd_fee_at                ,
           |te.cdhd_fee_conv_rt         as      cups_cdhd_fee_conv_rt           ,
           |te.cdhd_fee_acct_curr_cd    as      cups_cdhd_fee_acct_curr_cd      ,
           |te.repl_at                  as      cups_repl_at                    ,
           |te.exp_snd_chnl             as      cups_exp_snd_chnl               ,
           |te.confirm_exp_chnl         as      cups_confirm_exp_chnl           ,
           |te.extend_inf               as      cups_extend_inf                 ,
           |te.conn_md                  as      cups_conn_md                    ,
           |te.msg_tp                   as      cups_msg_tp                     ,
           |te.msg_tp_conv              as      cups_msg_tp_conv                ,
           |te.card_bin                 as      cups_card_bin                   ,
           |te.related_card_bin         as      cups_related_card_bin           ,
           |te.trans_proc_cd            as      cups_trans_proc_cd              ,
           |te.trans_proc_cd_conv       as      cups_trans_proc_cd_conv         ,
           |te.loc_trans_tm             as      cups_loc_trans_tm               ,
           |te.loc_trans_dt             as      cups_loc_trans_dt               ,
           |te.conv_dt                  as      cups_conv_dt                    ,
           |te.mchnt_tp                 as      cups_mchnt_tp                   ,
           |te.pos_entry_md_cd          as      cups_pos_entry_md_cd            ,
           |te.card_seq                 as      cups_card_seq                   ,
           |te.pos_cond_cd              as      cups_pos_cond_cd                ,
           |te.pos_cond_cd_conv         as      cups_pos_cond_cd_conv           ,
           |te.retri_ref_no             as      cups_retri_ref_no               ,
           |te.term_id                  as      cups_term_id                    ,
           |te.term_tp                  as      cups_term_tp                    ,
           |te.mchnt_cd                 as      cups_mchnt_cd                   ,
           |te.card_accptr_nm_addr      as      cups_card_accptr_nm_addr        ,
           |te.ic_data                  as      cups_ic_data                    ,
           |te.rsn_cd                   as      cups_rsn_cd                     ,
           |te.addn_pos_inf             as      cups_addn_pos_inf               ,
           |te.orig_msg_tp              as      cups_orig_msg_tp                ,
           |te.orig_msg_tp_conv         as      cups_orig_msg_tp_conv           ,
           |te.orig_sys_tra_no          as      cups_orig_sys_tra_no            ,
           |te.orig_sys_tra_no_conv     as      cups_orig_sys_tra_no_conv       ,
           |te.orig_tfr_dt_tm           as      cups_orig_tfr_dt_tm             ,
           |te.related_trans_id         as      cups_related_trans_id           ,
           |te.related_trans_chnl       as      cups_related_trans_chnl         ,
           |te.orig_trans_id            as      cups_orig_trans_id              ,
           |te.orig_trans_id_conv       as      cups_orig_trans_id_conv         ,
           |te.orig_trans_chnl          as      cups_orig_trans_chnl            ,
           |te.orig_card_media          as      cups_orig_card_media            ,
           |te.orig_card_media_proc_md  as      cups_orig_card_media_proc_md    ,
           |te.tfr_in_acct_no           as      cups_tfr_in_acct_no             ,
           |te.tfr_out_acct_no          as      cups_tfr_out_acct_no            ,
           |te.cups_resv                as      cups_cups_resv                  ,
           |te.ic_flds                  as      cups_ic_flds                    ,
           |te.cups_def_fld             as      cups_cups_def_fld               ,
           |te.spec_settle_in           as      cups_spec_settle_in             ,
           |te.settle_trans_id          as      cups_settle_trans_id            ,
           |te.spec_mcc_in              as      cups_spec_mcc_in                ,
           |te.iss_ds_settle_in         as      cups_iss_ds_settle_in           ,
           |te.acq_ds_settle_in         as      cups_acq_ds_settle_in           ,
           |te.settle_bmp               as      cups_settle_bmp                 ,
           |te.upd_in                   as      cups_upd_in                     ,
           |te.exp_rsn_cd               as      cups_exp_rsn_cd                 ,
           |te.to_ts                    as      cups_to_ts                      ,
           |te.resnd_num                as      cups_resnd_num                  ,
           |te.pri_cycle_no             as      cups_pri_cycle_no               ,
           |te.alt_cycle_no             as      cups_alt_cycle_no               ,
           |te.corr_pri_cycle_no        as      cups_corr_pri_cycle_no          ,
           |te.corr_alt_cycle_no        as      cups_corr_alt_cycle_no          ,
           |te.disc_in                  as      cups_disc_in                    ,
           |te.vfy_rslt                 as      cups_vfy_rslt                   ,
           |te.vfy_fee_cd               as      cups_vfy_fee_cd                 ,
           |te.orig_disc_in             as      cups_orig_disc_in               ,
           |te.orig_disc_curr_cd        as      cups_orig_disc_curr_cd          ,
           |te.fwd_settle_at            as      cups_fwd_settle_at              ,
           |te.rcv_settle_at            as      cups_rcv_settle_at              ,
           |te.fwd_settle_conv_rt       as      cups_fwd_settle_conv_rt         ,
           |te.rcv_settle_conv_rt       as      cups_rcv_settle_conv_rt         ,
           |te.fwd_settle_curr_cd       as      cups_fwd_settle_curr_cd         ,
           |te.rcv_settle_curr_cd       as      cups_rcv_settle_curr_cd         ,
           |te.disc_cd                  as      cups_disc_cd                    ,
           |te.allot_cd                 as      cups_allot_cd                   ,
           |te.total_disc_at            as      cups_total_disc_at              ,
           |te.fwd_orig_settle_at       as      cups_fwd_orig_settle_at         ,
           |te.rcv_orig_settle_at       as      cups_rcv_orig_settle_at         ,
           |te.vfy_fee_at               as      cups_vfy_fee_at                 ,
           |te.sp_mchnt_cd              as      cups_sp_mchnt_cd                ,
           |te.acct_ins_id_cd           as      cups_acct_ins_id_cd             ,
           |te.iss_ins_id_cd1           as      cups_iss_ins_id_cd1             ,
           |te.iss_ins_id_cd2           as      cups_iss_ins_id_cd2             ,
           |te.iss_ins_id_cd3           as      cups_iss_ins_id_cd3             ,
           |te.iss_ins_id_cd4           as      cups_iss_ins_id_cd4             ,
           |te.mchnt_ins_id_cd1         as      cups_mchnt_ins_id_cd1           ,
           |te.mchnt_ins_id_cd2         as      cups_mchnt_ins_id_cd2           ,
           |te.mchnt_ins_id_cd3         as      cups_mchnt_ins_id_cd3           ,
           |te.mchnt_ins_id_cd4         as      cups_mchnt_ins_id_cd4           ,
           |te.term_ins_id_cd1          as      cups_term_ins_id_cd1            ,
           |te.term_ins_id_cd2          as      cups_term_ins_id_cd2            ,
           |te.term_ins_id_cd3          as      cups_term_ins_id_cd3            ,
           |te.term_ins_id_cd4          as      cups_term_ins_id_cd4            ,
           |te.term_ins_id_cd5          as      cups_term_ins_id_cd5            ,
           |te.acpt_cret_disc_at        as      cups_acpt_cret_disc_at          ,
           |te.acpt_debt_disc_at        as      cups_acpt_debt_disc_at          ,
           |te.iss1_cret_disc_at        as      cups_iss1_cret_disc_at          ,
           |te.iss1_debt_disc_at        as      cups_iss1_debt_disc_at          ,
           |te.iss2_cret_disc_at        as      cups_iss2_cret_disc_at          ,
           |te.iss2_debt_disc_at        as      cups_iss2_debt_disc_at          ,
           |te.iss3_cret_disc_at        as      cups_iss3_cret_disc_at          ,
           |te.iss3_debt_disc_at        as      cups_iss3_debt_disc_at          ,
           |te.iss4_cret_disc_at        as      cups_iss4_cret_disc_at          ,
           |te.iss4_debt_disc_at        as      cups_iss4_debt_disc_at          ,
           |te.mchnt1_cret_disc_at      as      cups_mchnt1_cret_disc_at        ,
           |te.mchnt1_debt_disc_at      as      cups_mchnt1_debt_disc_at        ,
           |te.mchnt2_cret_disc_at      as      cups_mchnt2_cret_disc_at        ,
           |te.mchnt2_debt_disc_at      as      cups_mchnt2_debt_disc_at        ,
           |te.mchnt3_cret_disc_at      as      cups_mchnt3_cret_disc_at        ,
           |te.mchnt3_debt_disc_at      as      cups_mchnt3_debt_disc_at        ,
           |te.mchnt4_cret_disc_at      as      cups_mchnt4_cret_disc_at        ,
           |te.mchnt4_debt_disc_at      as      cups_mchnt4_debt_disc_at        ,
           |te.term1_cret_disc_at       as      cups_term1_cret_disc_at         ,
           |te.term1_debt_disc_at       as      cups_term1_debt_disc_at         ,
           |te.term2_cret_disc_at       as      cups_term2_cret_disc_at         ,
           |te.term2_debt_disc_at       as      cups_term2_debt_disc_at         ,
           |te.term3_cret_disc_at       as      cups_term3_cret_disc_at         ,
           |te.term3_debt_disc_at       as      cups_term3_debt_disc_at         ,
           |te.term4_cret_disc_at       as      cups_term4_cret_disc_at         ,
           |te.term4_debt_disc_at       as      cups_term4_debt_disc_at         ,
           |te.term5_cret_disc_at       as      cups_term5_cret_disc_at         ,
           |te.term5_debt_disc_at       as      cups_term5_debt_disc_at         ,
           |te.pay_in                   as      cups_pay_in                     ,
           |te.exp_id                   as      cups_exp_id                     ,
           |te.vou_in                   as      cups_vou_in                     ,
           |te.orig_log_cd              as      cups_orig_log_cd                ,
           |te.related_log_cd           as      cups_related_log_cd             ,
           |te.mdc_key                  as      cups_mdc_key                    ,
           |te.rec_upd_ts               as      cups_rec_upd_ts                 ,
           |te.rec_crt_ts               as      cups_rec_crt_ts                 ,
           |te.hp_settle_dt             as      cups_hp_settle_dt
           |
           |
           |from
           |(
           |select
           |tempd.seq_id                             ,
           |tempd.cdhd_usr_id                        ,
           |tempd.card_no                            ,
           |tempd.trans_tfr_tm                       ,
           |tempd.sys_tra_no                         ,
           |tempd.acpt_ins_id_cd                     ,
           |tempd.fwd_ins_id_cd                      ,
           |tempd.rcv_ins_id_cd                      ,
           |tempd.oper_module                        ,
           |tempd.trans_dt                           ,
           |tempd.trans_tm                           ,
           |tempd.buss_tp                            ,
           |tempd.um_trans_id                        ,
           |tempd.swt_right_tp                       ,
           |tempd.bill_id                            ,
           |tempd.bill_nm                            ,
           |tempd.chara_acct_tp                      ,
           |tempd.trans_at                           ,
           |tempd.point_at                           ,
           |tempd.mchnt_tp                           ,
           |tempd.resp_cd                            ,
           |tempd.card_accptr_term_id                ,
           |tempd.card_accptr_cd                     ,
           |tempd.trans_proc_start_ts as frist_trans_proc_start_ts,
           |NULL as second_trans_proc_start_ts       ,
           |NULL as third_trans_proc_start_ts        ,
           |tempd.trans_proc_end_ts                  ,
           |tempd.sys_det_cd                         ,
           |tempd.sys_err_cd                         ,
           |tempd.rec_upd_ts                         ,
           |tempd.chara_acct_nm                      ,
           |tempd.void_trans_tfr_tm                  ,
           |tempd.void_sys_tra_no                    ,
           |tempd.void_acpt_ins_id_cd                ,
           |tempd.void_fwd_ins_id_cd                 ,
           |tempd.orig_data_elemnt                   ,
           |tempd.rec_crt_ts                         ,
           |tempd.discount_at                        ,
           |tempd.bill_item_id                       ,
           |tempd.chnl_inf_index                     ,
           |tempd.bill_num                           ,
           |tempd.addn_discount_at                   ,
           |tempd.pos_entry_md_cd                    ,
           |tempd.udf_fld                            ,
           |tempd.card_accptr_nm_addr                ,
           |tempd.part_trans_dt                      ,
           |tempe.msg_tp                             ,
           |tempe.cdhd_fk                            ,
           |tempe.bill_tp                            ,
           |tempe.bill_bat_no                        ,
           |tempe.bill_inf                           ,
           |tempe.proc_cd                            ,
           |tempe.trans_curr_cd                      ,
           |tempe.settle_at                          ,
           |tempe.settle_curr_cd                     ,
           |tempe.card_accptr_local_tm               ,
           |tempe.card_accptr_local_dt               ,
           |tempe.expire_dt                          ,
           |tempe.msg_settle_dt                      ,
           |tempe.pos_cond_cd                        ,
           |tempe.pos_pin_capture_cd                 ,
           |tempe.retri_ref_no                       ,
           |tempe.auth_id_resp_cd                    ,
           |tempe.notify_st                          ,
           |tempe.addn_private_data                  ,
           |tempe.addn_at                            ,
           |tempe.acct_id_1                          ,
           |tempe.acct_id_2                          ,
           |tempe.resv_fld                           ,
           |tempe.cdhd_auth_inf                      ,
           |tempe.sys_settle_dt                      ,
           |tempe.recncl_in                          ,
           |tempe.match_in                           ,
           |tempe.sec_ctrl_inf                       ,
           |tempe.card_seq                           ,
           |tempe.dtl_inq_data
           |from
           |(select * from viw_chacc_acc_trans_dtl where um_trans_id<>'AC02202000') tempd
           |left join
           |(select * from viw_chacc_acc_trans_log where um_trans_id<>'AC02202000') tempe
           |on trim(tempd.trans_tfr_tm)=trim(tempe.trans_tfr_tm) and trim(tempd.sys_tra_no)=trim(tempe.sys_tra_no) and lpad(trim(tempd.acpt_ins_id_cd),11,'0')=lpad(trim(tempe.acpt_ins_id_cd),11,'0') and lpad(trim(tempd.fwd_ins_id_cd),11,'0')=lpad(trim(tempe.fwd_ins_id_cd),11,'0')
           |
           |union all
           |
           |select
           |tempc.seq_id                             ,
           |tempc.cdhd_usr_id                        ,
           |tempc.card_no                            ,
           |tempc.trans_tfr_tm                       ,
           |tempc.sys_tra_no                         ,
           |tempc.acpt_ins_id_cd                     ,
           |tempc.fwd_ins_id_cd                      ,
           |tempc.rcv_ins_id_cd                      ,
           |tempc.oper_module                        ,
           |tempc.trans_dt                           ,
           |tempc.trans_tm                           ,
           |tempc.buss_tp                            ,
           |tempc.um_trans_id                        ,
           |tempc.swt_right_tp                       ,
           |tempc.bill_id                            ,
           |tempc.bill_nm                            ,
           |tempc.chara_acct_tp                      ,
           |tempc.trans_at                           ,
           |tempc.point_at                           ,
           |tempc.mchnt_tp                           ,
           |tempc.resp_cd                            ,
           |tempc.card_accptr_term_id                ,
           |tempc.card_accptr_cd                     ,
           |max(case when tempc.rank=1 then tempc.trans_proc_start_ts else null end) as frist_trans_proc_start_ts,
           |max(case when tempc.rank=2 then tempc.trans_proc_start_ts else null end) as second_trans_proc_start_ts,
           |max(case when tempc.rank=3 then tempc.trans_proc_start_ts else null end) as third_trans_proc_start_ts,
           |tempc.trans_proc_end_ts                  ,
           |tempc.sys_det_cd                         ,
           |tempc.sys_err_cd                         ,
           |tempc.rec_upd_ts                         ,
           |tempc.chara_acct_nm                      ,
           |tempc.void_trans_tfr_tm                  ,
           |tempc.void_sys_tra_no                    ,
           |tempc.void_acpt_ins_id_cd                ,
           |tempc.void_fwd_ins_id_cd                 ,
           |tempc.orig_data_elemnt                   ,
           |tempc.rec_crt_ts                         ,
           |tempc.discount_at                        ,
           |tempc.bill_item_id                       ,
           |tempc.chnl_inf_index                     ,
           |tempc.bill_num                           ,
           |tempc.addn_discount_at                   ,
           |tempc.pos_entry_md_cd                    ,
           |tempc.udf_fld                            ,
           |tempc.card_accptr_nm_addr                ,
           |tempc.part_trans_dt                      ,
           |tempc.msg_tp                             ,
           |tempc.cdhd_fk                            ,
           |tempc.bill_tp                            ,
           |tempc.bill_bat_no                        ,
           |tempc.bill_inf                           ,
           |tempc.proc_cd                            ,
           |tempc.trans_curr_cd                      ,
           |tempc.settle_at                          ,
           |tempc.settle_curr_cd                     ,
           |tempc.card_accptr_local_tm               ,
           |tempc.card_accptr_local_dt               ,
           |tempc.expire_dt                          ,
           |tempc.msg_settle_dt                      ,
           |tempc.pos_cond_cd                        ,
           |tempc.pos_pin_capture_cd                 ,
           |tempc.retri_ref_no                       ,
           |tempc.auth_id_resp_cd                    ,
           |tempc.notify_st                          ,
           |tempc.addn_private_data                  ,
           |tempc.addn_at                            ,
           |tempc.acct_id_1                          ,
           |tempc.acct_id_2                          ,
           |tempc.resv_fld                           ,
           |tempc.cdhd_auth_inf                      ,
           |tempc.sys_settle_dt                      ,
           |tempc.recncl_in                          ,
           |tempc.match_in                           ,
           |tempc.sec_ctrl_inf                       ,
           |tempc.card_seq                           ,
           |tempc.dtl_inq_data
           |
           |from
           |(
           |select
           |tempw.seq_id                             ,
           |tempw.cdhd_usr_id                        ,
           |tempw.card_no                            ,
           |tempx.trans_tfr_tm                       ,
           |tempx.sys_tra_no                         ,
           |tempx.acpt_ins_id_cd                     ,
           |tempx.fwd_ins_id_cd                      ,
           |tempw.rcv_ins_id_cd                      ,
           |tempw.oper_module                        ,
           |tempw.trans_dt                           ,
           |tempw.trans_tm                           ,
           |tempw.buss_tp                            ,
           |tempw.um_trans_id                        ,
           |tempw.swt_right_tp                       ,
           |tempw.bill_id                            ,
           |tempw.bill_nm                            ,
           |tempw.chara_acct_tp                      ,
           |tempw.trans_at                           ,
           |tempw.point_at                           ,
           |tempw.mchnt_tp                           ,
           |tempw.resp_cd                            ,
           |tempw.card_accptr_term_id                ,
           |tempw.card_accptr_cd                     ,
           |tempw.trans_proc_start_ts                ,
           |tempw.trans_proc_end_ts                  ,
           |tempw.sys_det_cd                         ,
           |tempw.sys_err_cd                         ,
           |tempw.rec_upd_ts                         ,
           |tempw.chara_acct_nm                      ,
           |tempw.void_trans_tfr_tm                  ,
           |tempw.void_sys_tra_no                    ,
           |tempw.void_acpt_ins_id_cd                ,
           |tempw.void_fwd_ins_id_cd                 ,
           |tempw.orig_data_elemnt                   ,
           |tempw.rec_crt_ts                         ,
           |tempw.discount_at                        ,
           |tempw.bill_item_id                       ,
           |tempw.chnl_inf_index                     ,
           |tempw.bill_num                           ,
           |tempw.addn_discount_at                   ,
           |tempw.pos_entry_md_cd                    ,
           |tempw.udf_fld                            ,
           |tempw.card_accptr_nm_addr                ,
           |tempw.part_trans_dt                      ,
           |tempw.msg_tp                             ,
           |tempw.cdhd_fk                            ,
           |tempw.bill_tp                            ,
           |tempw.bill_bat_no                        ,
           |tempw.bill_inf                           ,
           |tempw.proc_cd                            ,
           |tempw.trans_curr_cd                      ,
           |tempw.settle_at                          ,
           |tempw.settle_curr_cd                     ,
           |tempw.card_accptr_local_tm               ,
           |tempw.card_accptr_local_dt               ,
           |tempw.expire_dt                          ,
           |tempw.msg_settle_dt                      ,
           |tempw.pos_cond_cd                        ,
           |tempw.pos_pin_capture_cd                 ,
           |tempw.retri_ref_no                       ,
           |tempw.auth_id_resp_cd                    ,
           |tempw.notify_st                          ,
           |tempw.addn_private_data                  ,
           |tempw.addn_at                            ,
           |tempw.acct_id_1                          ,
           |tempw.acct_id_2                          ,
           |tempw.resv_fld                           ,
           |tempw.cdhd_auth_inf                      ,
           |tempw.sys_settle_dt                      ,
           |tempw.recncl_in                          ,
           |tempw.match_in                           ,
           |tempw.sec_ctrl_inf                       ,
           |tempw.card_seq                           ,
           |tempw.dtl_inq_data                       ,
           |row_number() over (order by tempw.trans_proc_start_ts) rank
           |from
           |(
           |select
           |tempa.seq_id                             ,
           |tempa.cdhd_usr_id                        ,
           |tempa.card_no                            ,
           |tempa.trans_tfr_tm                       ,
           |tempa.sys_tra_no                         ,
           |tempa.acpt_ins_id_cd                     ,
           |tempa.fwd_ins_id_cd                      ,
           |tempa.rcv_ins_id_cd                      ,
           |tempa.oper_module                        ,
           |tempa.trans_dt                           ,
           |tempa.trans_tm                           ,
           |tempa.buss_tp                            ,
           |tempa.um_trans_id                        ,
           |tempa.swt_right_tp                       ,
           |tempa.bill_id                            ,
           |tempa.bill_nm                            ,
           |tempa.chara_acct_tp                      ,
           |tempa.trans_at                           ,
           |tempa.point_at                           ,
           |tempa.mchnt_tp                           ,
           |tempa.resp_cd                            ,
           |tempa.card_accptr_term_id                ,
           |tempa.card_accptr_cd                     ,
           |tempa.trans_proc_start_ts                ,
           |tempa.trans_proc_end_ts                  ,
           |tempa.sys_det_cd                         ,
           |tempa.sys_err_cd                         ,
           |tempa.rec_upd_ts                         ,
           |tempa.chara_acct_nm                      ,
           |tempa.void_trans_tfr_tm                  ,
           |tempa.void_sys_tra_no                    ,
           |tempa.void_acpt_ins_id_cd                ,
           |tempa.void_fwd_ins_id_cd                 ,
           |tempa.orig_data_elemnt                   ,
           |tempa.rec_crt_ts                         ,
           |tempa.discount_at                        ,
           |tempa.bill_item_id                       ,
           |tempa.chnl_inf_index                     ,
           |tempa.bill_num                           ,
           |tempa.addn_discount_at                   ,
           |tempa.pos_entry_md_cd                    ,
           |tempa.udf_fld                            ,
           |tempa.card_accptr_nm_addr                ,
           |tempa.part_trans_dt                      ,
           |tempb.msg_tp                             ,
           |tempb.cdhd_fk                            ,
           |tempb.bill_tp                            ,
           |tempb.bill_bat_no                        ,
           |tempb.bill_inf                           ,
           |tempb.proc_cd                            ,
           |tempb.trans_curr_cd                      ,
           |tempb.settle_at                          ,
           |tempb.settle_curr_cd                     ,
           |tempb.card_accptr_local_tm               ,
           |tempb.card_accptr_local_dt               ,
           |tempb.expire_dt                          ,
           |tempb.msg_settle_dt                      ,
           |tempb.pos_cond_cd                        ,
           |tempb.pos_pin_capture_cd                 ,
           |tempb.retri_ref_no                       ,
           |tempb.auth_id_resp_cd                    ,
           |tempb.notify_st                          ,
           |tempb.addn_private_data                  ,
           |tempb.addn_at                            ,
           |tempb.acct_id_1                          ,
           |tempb.acct_id_2                          ,
           |tempb.resv_fld                           ,
           |tempb.cdhd_auth_inf                      ,
           |tempb.sys_settle_dt                      ,
           |tempb.recncl_in                          ,
           |tempb.match_in                           ,
           |tempb.sec_ctrl_inf                       ,
           |tempb.card_seq                           ,
           |tempb.dtl_inq_data
           |from
           |(select * from viw_chacc_acc_trans_dtl where um_trans_id='AC02202000') tempa,
           |(select * from viw_chacc_acc_trans_log where um_trans_id='AC02202000') tempb
           |)
           |tempw
           |
           |right join
           |(
           |select
           |tempz.trans_tfr_tm,
           |tempz.sys_tra_no,
           |tempz.acpt_ins_id_cd,
           |tempz.fwd_ins_id_cd
           |from (select * from viw_chacc_acc_trans_dtl where um_trans_id='AC02202000') tempz,
           |(select * from viw_chacc_acc_trans_log where um_trans_id='AC02202000') tempy
           |group by
           |tempz.trans_tfr_tm,
           |tempz.sys_tra_no,
           |tempz.acpt_ins_id_cd,
           |tempz.fwd_ins_id_cd
           |having  count(*)>1
           |) tempx
           |on tempw.trans_tfr_tm=tempx.trans_tfr_tm and tempw.sys_tra_no=tempx.sys_tra_no and tempw.acpt_ins_id_cd=tempx.acpt_ins_id_cd and tempw.fwd_ins_id_cd=tempx.fwd_ins_id_cd
           |)
           |tempc
           |group by
           |tempc.seq_id                             ,
           |tempc.cdhd_usr_id                        ,
           |tempc.card_no                            ,
           |tempc.trans_tfr_tm                       ,
           |tempc.sys_tra_no                         ,
           |tempc.acpt_ins_id_cd                     ,
           |tempc.fwd_ins_id_cd                      ,
           |tempc.rcv_ins_id_cd                      ,
           |tempc.oper_module                        ,
           |tempc.trans_dt                           ,
           |tempc.trans_tm                           ,
           |tempc.buss_tp                            ,
           |tempc.um_trans_id                        ,
           |tempc.swt_right_tp                       ,
           |tempc.bill_id                            ,
           |tempc.bill_nm                            ,
           |tempc.chara_acct_tp                      ,
           |tempc.trans_at                           ,
           |tempc.point_at                           ,
           |tempc.mchnt_tp                           ,
           |tempc.resp_cd                            ,
           |tempc.card_accptr_term_id                ,
           |tempc.card_accptr_cd                     ,
           |tempc.trans_proc_end_ts                  ,
           |tempc.sys_det_cd                         ,
           |tempc.sys_err_cd                         ,
           |tempc.rec_upd_ts                         ,
           |tempc.chara_acct_nm                      ,
           |tempc.void_trans_tfr_tm                  ,
           |tempc.void_sys_tra_no                    ,
           |tempc.void_acpt_ins_id_cd                ,
           |tempc.void_fwd_ins_id_cd                 ,
           |tempc.orig_data_elemnt                   ,
           |tempc.rec_crt_ts                         ,
           |tempc.discount_at                        ,
           |tempc.bill_item_id                       ,
           |tempc.chnl_inf_index                     ,
           |tempc.bill_num                           ,
           |tempc.addn_discount_at                   ,
           |tempc.pos_entry_md_cd                    ,
           |tempc.udf_fld                            ,
           |tempc.card_accptr_nm_addr                ,
           |tempc.part_trans_dt                      ,
           |tempc.msg_tp                             ,
           |tempc.cdhd_fk                            ,
           |tempc.bill_tp                            ,
           |tempc.bill_bat_no                        ,
           |tempc.bill_inf                           ,
           |tempc.proc_cd                            ,
           |tempc.trans_curr_cd                      ,
           |tempc.settle_at                          ,
           |tempc.settle_curr_cd                     ,
           |tempc.card_accptr_local_tm               ,
           |tempc.card_accptr_local_dt               ,
           |tempc.expire_dt                          ,
           |tempc.msg_settle_dt                      ,
           |tempc.pos_cond_cd                        ,
           |tempc.pos_pin_capture_cd                 ,
           |tempc.retri_ref_no                       ,
           |tempc.auth_id_resp_cd                    ,
           |tempc.notify_st                          ,
           |tempc.addn_private_data                  ,
           |tempc.addn_at                            ,
           |tempc.acct_id_1                          ,
           |tempc.acct_id_2                          ,
           |tempc.resv_fld                           ,
           |tempc.cdhd_auth_inf                      ,
           |tempc.sys_settle_dt                      ,
           |tempc.recncl_in                          ,
           |tempc.match_in                           ,
           |tempc.sec_ctrl_inf                       ,
           |tempc.card_seq                           ,
           |tempc.dtl_inq_data
           |
           |)ta
           |
           |left join  ( select * from viw_chmgm_swt_log ) tc
           |on trim(ta.trans_tfr_tm)=trim(tc.tfr_dt_tm)  and  trim(ta.sys_tra_no)=trim(tc.sys_tra_no) and lpad(trim(ta.acpt_ins_id_cd),11,'0')=lpad(trim(tc.acpt_ins_id_cd),11,'0') and lpad(trim(ta.fwd_ins_id_cd),11,'0')=lpad(trim(tc.msg_fwd_ins_id_cd),11,'0')
           |
           |left join
           |(select * from spark_hive_cups_trans ) te
           |on trim(ta.trans_tfr_tm)=trim(te.tfr_dt_tm) and trim(ta.sys_tra_no)=trim(te.sys_tra_no) and lpad(trim(ta.acpt_ins_id_cd),11,'0')=lpad(trim(te.acpt_ins_id_cd),11,'0') and lpad(trim(ta.fwd_ins_id_cd),11,'0')=lpad(trim(te.fwd_ins_id_cd),11,'0')
           |
           | """.stripMargin)
      println("#### JOB_HV_4 spark sql 逻辑完成的系统时间为:" + DateUtils.getCurrentSystemTime())

      results.registerTempTable("spark_acc_trans")
      println("#### JOB_HV_4 spark sql 临时表生成时间为:" + DateUtils.getCurrentSystemTime())

      if (!Option(results).isEmpty) {
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql(
          s"""
             |insert overwrite table hive_acc_trans partition (part_trans_dt)
             | select
             | seq_id,
             |cdhd_usr_id,
             |card_no,
             |trans_tfr_tm,
             |sys_tra_no,
             |acpt_ins_id_cd,
             |fwd_ins_id_cd,
             |rcv_ins_id_cd,
             |oper_module,
             |trans_dt,
             |trans_tm,
             |buss_tp,
             |um_trans_id,
             |swt_right_tp,
             |bill_id,
             |bill_nm,
             |chara_acct_tp,
             |trans_at,
             |point_at,
             |mchnt_tp,
             |resp_cd,
             |card_accptr_term_id,
             |card_accptr_cd,
             |frist_trans_proc_start_ts,
             |second_trans_proc_start_ts,
             |third_trans_proc_start_ts,
             |trans_proc_end_ts,
             |sys_det_cd,
             |sys_err_cd,
             |rec_upd_ts,
             |chara_acct_nm,
             | void_trans_tfr_tm,
             | void_sys_tra_no,
             | void_acpt_ins_id_cd,
             | void_fwd_ins_id_cd,
             | orig_data_elemnt,
             |rec_crt_ts,
             |discount_at,
             |bill_item_id,
             | chnl_inf_index,
             | bill_num,
             |addn_discount_at,
             | pos_entry_md_cd,
             | udf_fld,
             | card_accptr_nm_addr,
             | msg_tp,
             |cdhd_fk,
             |bill_tp,
             |bill_bat_no,
             |bill_inf,
             | proc_cd,
             |trans_curr_cd,
             |settle_at,
             | settle_curr_cd,
             |card_accptr_local_tm,
             | card_accptr_local_dt,
             |expire_dt,
             | msg_settle_dt,
             |pos_cond_cd,
             |pos_pin_capture_cd,
             | retri_ref_no,
             | auth_id_resp_cd,
             | notify_st,
             |addn_private_data,
             |addn_at,
             |acct_id_1,
             |acct_id_2,
             |resv_fld,
             |cdhd_auth_inf,
             |sys_settle_dt,
             |recncl_in,
             |match_in,
             | sec_ctrl_inf,
             |card_seq,
             | dtl_inq_data,
             | pri_key1,
             | fwd_chnl_head,
             |chswt_plat_seq,
             |internal_trans_tp,
             |settle_trans_id,
             |trans_tp,
             | cups_settle_dt,
             | pri_acct_no,
             |card_bin,
             | req_trans_at,
             | resp_trans_at,
             |trans_tot_at,
             |iss_ins_id_cd,
             | launch_trans_tm,
             | launch_trans_dt,
             |mchnt_cd,
             | fwd_proc_in,
             |rcv_proc_in,
             |proj_tp,
             | usr_id,
             |conv_usr_id,
             | trans_st,
             |inq_dtl_req,
             |inq_dtl_resp,
             | iss_ins_resv,
             | ic_flds,
             |cups_def_fld,
             |id_no,
             |cups_resv,
             |acpt_ins_resv,
             |rout_ins_id_cd,
             |sub_rout_ins_id_cd,
             |recv_access_resp_cd,
             |chswt_resp_cd,
             | chswt_err_cd,
             |resv_fld1,
             | resv_fld2,
             | to_ts,
             | external_amt,
             |card_pay_at,
             |right_purchase_at,
             | recv_second_resp_cd,
             |req_acpt_ins_resv,
             |NULL as log_id,
             |NULL as conv_acct_no,
             |NULL as inner_pro_ind,
             |NULL as acct_proc_in,
             |NULL as order_id,
             |settle_dt                     ,
             |cups_pri_key                  ,
             |cups_log_cd                   ,
             |cups_settle_tp                ,
             |cups_settle_cycle             ,
             |cups_block_id                 ,
             |cups_orig_key                 ,
             |cups_related_key              ,
             |cups_trans_fwd_st             ,
             |cups_trans_rcv_st             ,
             |cups_sms_dms_conv_in          ,
             |cups_fee_in                   ,
             |cups_cross_dist_in            ,
             |cups_orig_acpt_sdms_in        ,
             |cups_tfr_in_in                ,
             |cups_trans_md                 ,
             |cups_source_region_cd         ,
             |cups_dest_region_cd           ,
             |cups_cups_card_in             ,
             |cups_cups_sig_card_in         ,
             |cups_card_class               ,
             |cups_card_attr                ,
             |cups_sti_in                   ,
             |cups_trans_proc_in            ,
             |cups_acq_ins_id_cd            ,
             |cups_acq_ins_tp               ,
             |cups_fwd_ins_tp               ,
             |cups_rcv_ins_id_cd            ,
             |cups_rcv_ins_tp               ,
             |cups_iss_ins_id_cd            ,
             |cups_iss_ins_tp               ,
             |cups_related_ins_id_cd        ,
             |cups_related_ins_tp           ,
             |cups_acpt_ins_tp              ,
             |cups_pri_acct_no              ,
             |cups_pri_acct_no_conv         ,
             |cups_sys_tra_no_conv          ,
             |cups_sw_sys_tra_no            ,
             |cups_auth_dt                  ,
             |cups_auth_id_resp_cd          ,
             |cups_resp_cd1                 ,
             |cups_resp_cd2                 ,
             |cups_resp_cd3                 ,
             |cups_resp_cd4                 ,
             |cups_cu_trans_st              ,
             |cups_sti_takeout_in           ,
             |cups_trans_id                 ,
             |cups_trans_tp                 ,
             |cups_trans_chnl               ,
             |cups_card_media               ,
             |cups_card_media_proc_md       ,
             |cups_card_brand               ,
             |cups_expire_seg               ,
             |cups_trans_id_conv            ,
             |cups_settle_mon               ,
             |cups_settle_d                 ,
             |cups_orig_settle_dt           ,
             |cups_settle_fwd_ins_id_cd     ,
             |cups_settle_rcv_ins_id_cd     ,
             |cups_trans_at                 ,
             |cups_orig_trans_at            ,
             |cups_trans_conv_rt            ,
             |cups_trans_curr_cd            ,
             |cups_cdhd_fee_at              ,
             |cups_cdhd_fee_conv_rt         ,
             |cups_cdhd_fee_acct_curr_cd    ,
             |cups_repl_at                  ,
             |cups_exp_snd_chnl             ,
             |cups_confirm_exp_chnl         ,
             |cups_extend_inf               ,
             |cups_conn_md                  ,
             |cups_msg_tp                   ,
             |cups_msg_tp_conv              ,
             |cups_card_bin                 ,
             |cups_related_card_bin         ,
             |cups_trans_proc_cd            ,
             |cups_trans_proc_cd_conv       ,
             |cups_loc_trans_tm             ,
             |cups_loc_trans_dt             ,
             |cups_conv_dt                  ,
             |cups_mchnt_tp                 ,
             |cups_pos_entry_md_cd          ,
             |cups_card_seq                 ,
             |cups_pos_cond_cd              ,
             |cups_pos_cond_cd_conv         ,
             |cups_retri_ref_no             ,
             |cups_term_id                  ,
             |cups_term_tp                  ,
             |cups_mchnt_cd                 ,
             |cups_card_accptr_nm_addr      ,
             |cups_ic_data                  ,
             |cups_rsn_cd                   ,
             |cups_addn_pos_inf             ,
             |cups_orig_msg_tp              ,
             |cups_orig_msg_tp_conv         ,
             |cups_orig_sys_tra_no          ,
             |cups_orig_sys_tra_no_conv     ,
             |cups_orig_tfr_dt_tm           ,
             |cups_related_trans_id         ,
             |cups_related_trans_chnl       ,
             |cups_orig_trans_id            ,
             |cups_orig_trans_id_conv       ,
             |cups_orig_trans_chnl          ,
             |cups_orig_card_media          ,
             |cups_orig_card_media_proc_md  ,
             |cups_tfr_in_acct_no           ,
             |cups_tfr_out_acct_no          ,
             |cups_cups_resv                ,
             |cups_ic_flds                  ,
             |cups_cups_def_fld             ,
             |cups_spec_settle_in           ,
             |cups_settle_trans_id          ,
             |cups_spec_mcc_in              ,
             |cups_iss_ds_settle_in         ,
             |cups_acq_ds_settle_in         ,
             |cups_settle_bmp               ,
             |cups_upd_in                   ,
             |cups_exp_rsn_cd               ,
             |cups_to_ts                    ,
             |cups_resnd_num                ,
             |cups_pri_cycle_no             ,
             |cups_alt_cycle_no             ,
             |cups_corr_pri_cycle_no        ,
             |cups_corr_alt_cycle_no        ,
             |cups_disc_in                  ,
             |cups_vfy_rslt                 ,
             |cups_vfy_fee_cd               ,
             |cups_orig_disc_in             ,
             |cups_orig_disc_curr_cd        ,
             |cups_fwd_settle_at            ,
             |cups_rcv_settle_at            ,
             |cups_fwd_settle_conv_rt       ,
             |cups_rcv_settle_conv_rt       ,
             |cups_fwd_settle_curr_cd       ,
             |cups_rcv_settle_curr_cd       ,
             |cups_disc_cd                  ,
             |cups_allot_cd                 ,
             |cups_total_disc_at            ,
             |cups_fwd_orig_settle_at       ,
             |cups_rcv_orig_settle_at       ,
             |cups_vfy_fee_at               ,
             |cups_sp_mchnt_cd              ,
             |cups_acct_ins_id_cd           ,
             |cups_iss_ins_id_cd1           ,
             |cups_iss_ins_id_cd2           ,
             |cups_iss_ins_id_cd3           ,
             |cups_iss_ins_id_cd4           ,
             |cups_mchnt_ins_id_cd1         ,
             |cups_mchnt_ins_id_cd2         ,
             |cups_mchnt_ins_id_cd3         ,
             |cups_mchnt_ins_id_cd4         ,
             |cups_term_ins_id_cd1          ,
             |cups_term_ins_id_cd2          ,
             |cups_term_ins_id_cd3          ,
             |cups_term_ins_id_cd4          ,
             |cups_term_ins_id_cd5          ,
             |cups_acpt_cret_disc_at        ,
             |cups_acpt_debt_disc_at        ,
             |cups_iss1_cret_disc_at        ,
             |cups_iss1_debt_disc_at        ,
             |cups_iss2_cret_disc_at        ,
             |cups_iss2_debt_disc_at        ,
             |cups_iss3_cret_disc_at        ,
             |cups_iss3_debt_disc_at        ,
             |cups_iss4_cret_disc_at        ,
             |cups_iss4_debt_disc_at        ,
             |cups_mchnt1_cret_disc_at      ,
             |cups_mchnt1_debt_disc_at      ,
             |cups_mchnt2_cret_disc_at      ,
             |cups_mchnt2_debt_disc_at      ,
             |cups_mchnt3_cret_disc_at      ,
             |cups_mchnt3_debt_disc_at      ,
             |cups_mchnt4_cret_disc_at      ,
             |cups_mchnt4_debt_disc_at      ,
             |cups_term1_cret_disc_at       ,
             |cups_term1_debt_disc_at       ,
             |cups_term2_cret_disc_at       ,
             |cups_term2_debt_disc_at       ,
             |cups_term3_cret_disc_at       ,
             |cups_term3_debt_disc_at       ,
             |cups_term4_cret_disc_at       ,
             |cups_term4_debt_disc_at       ,
             |cups_term5_cret_disc_at       ,
             |cups_term5_debt_disc_at       ,
             |cups_pay_in                   ,
             |cups_exp_id                   ,
             |cups_vou_in                   ,
             |cups_orig_log_cd              ,
             |cups_related_log_cd           ,
             |cups_mdc_key                  ,
             |cups_rec_upd_ts               ,
             |cups_rec_crt_ts               ,
             |cups_hp_settle_dt             ,
             |p_trans_dt
             |from spark_acc_trans
        """.stripMargin)
        println("#### JOB_HV_4 插入分区完成的时间为：" + DateUtils.getCurrentSystemTime())
      } else {
        println("#### JOB_HV_4 加载的表spark_acc_trans中无数据！")
      }
    }

  }
*/

  /**
    *JobName:  JOB_HV_40
    *Feature:  hive_achis_trans->hive_life_trans
    * @author tzq
    * @time 2016-8-22
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    * @return
    */
  def JOB_HV_40(implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {
      println("####JOB_HV_40(hive_life_trans -> hive_achis_trans)")
      println("#### JOB_HV_40 增量抽取的时间范围: "+start_dt+"--"+end_dt)
      DateUtils.timeCost("JOB_HV_40"){
      sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql(
          s"""
             |insert overwrite table hive_life_trans partition(part_settle_dt)
             |select
             |a.settle_dt,
             |a.trans_idx,
             |a.trans_tp,
             |a.trans_class,
             |a.trans_source ,
             |a.buss_chnl,
             |a.carrier_tp ,
             |a.pri_acct_no ,
             |a.mchnt_conn_tp,
             |a.access_tp ,
             |a.conn_md ,
             |a.acq_ins_id_cd,
             |a.acq_head,
             |a.fwd_ins_id_cd,
             |a.rcv_ins_id_cd,
             |a.iss_ins_id_cd,
             |a.iss_head ,
             |a.iss_head_nm,
             |a.mchnt_cd  ,
             |a.mchnt_nm ,
             |a.mchnt_country,
             |a.mchnt_url,
             |a.mchnt_front_url,
             |a.mchnt_back_url,
             |a.mchnt_tp,
             |a.mchnt_order_id,
             |a.mchnt_order_desc,
             |a.mchnt_add_info,
             |a.mchnt_reserve,
             |a.reserve,
             |a.sub_mchnt_cd,
             |a.sub_mchnt_company,
             |a.sub_mchnt_nm,
             |a.mchnt_class,
             |a.sys_tra_no,
             |a.trans_tm,
             |a.sys_tm,
             |a.trans_dt,
             |a.auth_id,
             |a.trans_at,
             |a.trans_curr_cd,
             |a.proc_st,
             |a.resp_cd,
             |a.proc_sys,
             |a.trans_no,
             |a.trans_st,
             |a.conv_dt,
             |a.settle_at,
             |a.settle_curr_cd,
             |a.settle_conv_rt,
             |a.cert_tp,
             |a.cert_id,
             |a.name,
             |a.phone_no,
             |a.usr_id,
             |a.mchnt_id,
             |a.pay_method,
             |a.trans_ip,
             |a.encoding,
             |a.mac_addr,
             |a.card_attr,
             |a.ebank_id,
             |a.ebank_mchnt_cd,
             |a.ebank_order_num,
             |a.ebank_idx,
             |a.ebank_rsp_tm,
             |a.kz_curr_cd,
             |a.kz_conv_rt,
             |a.kz_at,
             |a.delivery_country,
             |a.delivery_province,
             |a.delivery_city,
             |a.delivery_district,
             |a.delivery_street,
             |a.sms_tp,
             |a.sign_method,
             |a.verify_mode,
             |a.accpt_pos_id,
             |a.mer_cert_id,
             |a.cup_cert_id,
             |a.mchnt_version,
             |a.sub_trans_tp,
             |a.mac,
             |a.biz_tp,
             |a.source_idt,
             |a.delivery_risk,
             |a.trans_flag,
             |a.org_trans_idx,
             |a.org_sys_tra_no,
             |a.org_sys_tm,
             |a.org_mchnt_order_id,
             |a.org_trans_tm,
             |a.org_trans_at,
             |a.req_pri_data,
             |a.pri_data,
             |a.addn_at,
             |a.res_pri_data,
             |a.inq_dtl,
             |a.reserve_fld,
             |a.buss_code,
             |a.t_mchnt_cd,
             |a.is_oversea,
             |a.points_at,
             |a.pri_acct_tp,
             |a.area_cd,
             |a.mchnt_fee_at,
             |a.user_fee_at,
             |a.curr_exp,
             |a.rcv_acct,
             |a.track2,
             |a.track3,
             |a.customer_nm,
             |a.product_info,
             |a.customer_email,
             |a.cup_branch_ins_cd,
             |a.org_trans_dt,
             |a.special_calc_cost,
             |a.zero_cost,
             |a.advance_payment,
             |a.new_trans_tp,
             |a.flight_inf,
             |a.md_id,
             |a.ud_id,
             |a.syssp_id,
             |a.card_sn,
             |a.tfr_in_acct,
             |a.acct_id,
             |a.card_bin,
             |a.icc_data,
             |a.icc_data2,
             |a.card_seq_id,
             |a.pos_entry_cd,
             |a.pos_cond_cd,
             |a.term_id,
             |a.usr_num_tp,
             |a.addn_area_cd,
             |a.usr_num,
             |a.reserve1,
             |a.reserve2,
             |a.reserve3,
             |a.reserve4,
             |a.reserve5,
             |a.reserve6,
             |a.rec_st,
             |a.comments,
             |a.to_ts,
             |a.rec_crt_ts,
             |a.rec_upd_ts,
             |a.pay_acct,
             |a.trans_chnl,
             |a.tlr_st,
             |a.rvs_st,
             |a.out_trans_tp,
             |a.org_out_trans_tp,
             |a.bind_id,
             |a.ch_info,
             |a.card_risk_flag,
             |a.trans_step,
             |a.ctrl_msg,
             |a.mchnt_delv_tag,
             |a.mchnt_risk_tag,
             |a.bat_id,
             |a.payer_ip,
             |a.gt_sign_val,
             |a.mchnt_sign_val,
             |a.deduction_at,
             |a.src_sys_flag,
             |a.mac_ip,
             |a.mac_sq,
             |a.trans_ip_num,
             |a.cvn_flag,
             |a.expire_flag,
             |a.usr_inf,
             |a.imei,
             |a.iss_ins_tp,
             |a.dir_field,
             |a.buss_tp,
             |a.in_trans_tp,
             |b.buss_tp_nm as buss_tp_nm,
             |b.chnl_tp_nm as chnl_tp_nm,
             |to_date(a.settle_dt) as part_settle_dt
             |from
             |hive_achis_trans a
             |left join hive_life b
             |on a.mchnt_cd = b.mchnt_cd
             |where
             |a.mchnt_cd in ('048000049000001','048000049000002','048000049000003','048000065130001','048000092220001',
             |									 '048000048140002','048000048140003','048000048140004','048000094980001','443701049000008',
             |									 '443701094980005','802500049000098','048000049001001','048000049001002','048000049001003',
             |									 '048000065131001','048000092221001','048000048141002','048000048141003','048000048141004','048000094980001')
          """.stripMargin)
        println("#### JOB_HV_40 分区插入完成的时间为："+DateUtils.getCurrentSystemTime())
    }
  }


  /**
    * JobName: JOB_HV_42 2016年10月11日
    * Feature: hive_achis_trans->hive_active_code_pay_trans
    * @author tzq
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    * @return
    */
  def JOB_HV_42(implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {
    println("#### JOB_HV_42(hive_achis_trans->hive_active_code_pay_trans)")
    println("#### JOB_HV_42 增量抽取的时间范围: "+start_dt+"--"+end_dt)

    DateUtils.timeCost("JOB_HV_42"){
      sqlContext.sql(s"use $hive_dbname")
      sqlContext.sql(
        s"""
           |insert overwrite table hive_active_code_pay_trans partition(part_settle_dt)
           |select
           |settle_dt,
           |trans_idx,
           |trans_tp,
           |trans_class,
           |trans_source,
           |buss_chnl,
           |carrier_tp,
           |pri_acct_no,
           |mchnt_conn_tp,
           |access_tp,
           |conn_md,
           |acq_ins_id_cd,
           |acq_head,
           |fwd_ins_id_cd,
           |rcv_ins_id_cd,
           |iss_ins_id_cd,
           |iss_head,
           |iss_head_nm,
           |mchnt_cd,
           |mchnt_nm,
           |mchnt_country,
           |mchnt_url,
           |mchnt_front_url,
           |mchnt_back_url,
           |mchnt_tp,
           |mchnt_order_id,
           |mchnt_order_desc,
           |mchnt_add_info,
           |mchnt_reserve,
           |reserve,
           |sub_mchnt_cd,
           |sub_mchnt_company,
           |sub_mchnt_nm,
           |mchnt_class,
           |sys_tra_no,
           |trans_tm,
           |sys_tm,
           |trans_dt,
           |auth_id,
           |trans_at,
           |trans_curr_cd,
           |proc_st,
           |resp_cd,
           |proc_sys,
           |trans_no,
           |trans_st,
           |conv_dt,
           |settle_at,
           |settle_curr_cd,
           |settle_conv_rt,
           |cert_tp,
           |cert_id,
           |name,
           |phone_no,
           |usr_id,
           |mchnt_id,
           |pay_method,
           |trans_ip,
           |encoding,
           |mac_addr,
           |card_attr,
           |ebank_id,
           |ebank_mchnt_cd,
           |ebank_order_num,
           |ebank_idx,
           |ebank_rsp_tm,
           |kz_curr_cd,
           |kz_conv_rt,
           |kz_at,
           |delivery_country,
           |delivery_province,
           |delivery_city,
           |delivery_district,
           |delivery_street,
           |sms_tp,
           |sign_method,
           |verify_mode,
           |accpt_pos_id,
           |mer_cert_id,
           |cup_cert_id,
           |mchnt_version,
           |sub_trans_tp,
           |mac,
           |biz_tp,
           |source_idt,
           |delivery_risk,
           |trans_flag,
           |org_trans_idx,
           |org_sys_tra_no,
           |org_sys_tm,
           |org_mchnt_order_id,
           |org_trans_tm,
           |org_trans_at,
           |req_pri_data,
           |pri_data,
           |addn_at,
           |res_pri_data,
           |inq_dtl,
           |reserve_fld,
           |buss_code,
           |t_mchnt_cd,
           |is_oversea,
           |points_at,
           |pri_acct_tp,
           |area_cd,
           |mchnt_fee_at,
           |user_fee_at,
           |curr_exp,
           |rcv_acct,
           |track2,
           |track3,
           |customer_nm,
           |product_info,
           |customer_email,
           |cup_branch_ins_cd,
           |org_trans_dt,
           |special_calc_cost,
           |zero_cost,
           |advance_payment,
           |new_trans_tp,
           |flight_inf,
           |md_id,
           |ud_id,
           |syssp_id,
           |card_sn,
           |tfr_in_acct,
           |acct_id,
           |card_bin,
           |icc_data,
           |icc_data2,
           |card_seq_id,
           |pos_entry_cd,
           |pos_cond_cd,
           |term_id,
           |usr_num_tp,
           |addn_area_cd,
           |usr_num,
           |reserve1,
           |reserve2,
           |reserve3,
           |reserve4,
           |reserve5,
           |reserve6,
           |rec_st,
           |comments,
           |to_ts,
           |rec_crt_ts,
           |rec_upd_ts,
           |pay_acct,
           |trans_chnl,
           |tlr_st,
           |rvs_st,
           |out_trans_tp,
           |org_out_trans_tp,
           |bind_id,
           |ch_info,
           |card_risk_flag,
           |trans_step,
           |ctrl_msg,
           |mchnt_delv_tag,
           |mchnt_risk_tag,
           |bat_id,
           |payer_ip,
           |gt_sign_val,
           |mchnt_sign_val,
           |deduction_at,
           |src_sys_flag,
           |mac_ip,
           |mac_sq,
           |trans_ip_num,
           |cvn_flag,
           |expire_flag,
           |usr_inf,
           |imei,
           |iss_ins_tp,
           |dir_field,
           |buss_tp,
           |in_trans_tp,
           |to_date(settle_dt) as part_settle_dt
           |from
           |hive_achis_trans
           |where trans_source in ('0001','9001') and mchnt_conn_tp='81'
           |
	      """.stripMargin)
      println(s"#### JOB_HV_42 动态分区插入完成的时间为："+DateUtils.getCurrentSystemTime())

    }

  }


  /**
    * JobName: JOB_HV_43
    * Feature: hive.hive_swt_log -> hive.hive_switch_point_trans
    * @author YangXue
    * @time 2016-09-12
    * @param sqlContext,start_dt,end_dt
    */
  def JOB_HV_43(implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {

    println("#### JOB_HV_43(hive_swt_log -> hive_switch_point_trans)")

    val start_day = start_dt.replace("-","")
    val end_day = end_dt.replace("-","")
    println("#### JOB_HV_43 增量抽取的时间范围: "+start_day+"--"+end_day)

    DateUtils.timeCost("JOB_HV_43"){
      sqlContext.sql(s"use $hive_dbname")
      val results = sqlContext.sql(
        s"""
           |select
           |trim(tfr_dt_tm) as tfr_dt_tm,
           |trim(sys_tra_no) as sys_tra_no,
           |trim(acpt_ins_id_cd) as acpt_ins_id_cd,
           |trim(msg_fwd_ins_id_cd) as msg_fwd_ins_id_cd,
           |pri_key1,
           |fwd_chnl_head,
           |chswt_plat_seq,
           |trim(trans_tm) as trans_tm,
           |case
           |	when
           |		substr(trans_dt,1,4) between '0001' and '9999' and substr(trans_dt,5,2) between '01' and '12' and
           |		substr(trans_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(trans_dt,1,4),substr(trans_dt,5,2),substr(trans_dt,7,2))),9,2)
           |	then concat_ws('-',substr(trans_dt,1,4),substr(trans_dt,5,2),substr(trans_dt,7,2))
           |	else null
           |end as trans_dt,
           |case
           |	when
           |		substr(cswt_settle_dt,1,4) between '0001' and '9999' and substr(cswt_settle_dt,5,2) between '01' and '12' and
           |		substr(cswt_settle_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(cswt_settle_dt,1,4),substr(cswt_settle_dt,5,2),substr(cswt_settle_dt,7,2))),9,2)
           |	then concat_ws('-',substr(cswt_settle_dt,1,4),substr(cswt_settle_dt,5,2),substr(cswt_settle_dt,7,2))
           |	else null
           |end as cswt_settle_dt,
           |trim(internal_trans_tp) as internal_trans_tp,
           |trim(settle_trans_id) as settle_trans_id,
           |trim(trans_tp) as trans_tp,
           |trim(cups_settle_dt) as cups_settle_dt,
           |trim(msg_tp) as msg_tp,
           |trim(pri_acct_no) as pri_acct_no,
           |trim(card_bin) as card_bin,
           |trim(proc_cd) as proc_cd,
           |req_trans_at,
           |resp_trans_at,
           |trim(trans_curr_cd) as trans_curr_cd,
           |trans_tot_at,
           |trim(iss_ins_id_cd) as iss_ins_id_cd,
           |trim(launch_trans_tm) as launch_trans_tm,
           |trim(launch_trans_dt) as launch_trans_dt,
           |trim(mchnt_tp) as mchnt_tp,
           |trim(pos_entry_md_cd) as pos_entry_md_cd,
           |trim(card_seq_id) as card_seq_id,
           |trim(pos_cond_cd) as pos_cond_cd,
           |trim(pos_pin_capture_cd) as pos_pin_capture_cd,
           |trim(retri_ref_no) as retri_ref_no,
           |trim(term_id) as term_id,
           |trim(mchnt_cd) as mchnt_cd,
           |trim(card_accptr_nm_loc) as card_accptr_nm_loc,
           |trim(sec_related_ctrl_inf) as sec_related_ctrl_inf,
           |orig_data_elemts,
           |trim(rcv_ins_id_cd) as rcv_ins_id_cd,
           |trim(fwd_proc_in) as fwd_proc_in,
           |trim(rcv_proc_in) as rcv_proc_in,
           |trim(proj_tp) as proj_tp,
           |usr_id,
           |conv_usr_id,
           |trim(trans_st) as trans_st,
           |inq_dtl_req,
           |inq_dtl_resp,
           |iss_ins_resv,
           |ic_flds,
           |cups_def_fld,
           |trim(id_no) as id_no,
           |trim(cups_resv) as cups_resv,
           |acpt_ins_resv,
           |trim(rout_ins_id_cd) as rout_ins_id_cd,
           |trim(sub_rout_ins_id_cd) as sub_rout_ins_id_cd,
           |trim(recv_access_resp_cd) as recv_access_resp_cd,
           |trim(chswt_resp_cd) as chswt_resp_cd,
           |trim(chswt_err_cd) as chswt_err_cd,
           |resv_fld1,
           |resv_fld2,
           |to_ts,
           |rec_upd_ts,
           |rec_crt_ts,
           |settle_at,
           |external_amt,
           |discount_at,
           |card_pay_at,
           |right_purchase_at,
           |trim(recv_second_resp_cd) as recv_second_resp_cd,
           |req_acpt_ins_resv,
           |trim(log_id) as log_id,
           |trim(conv_acct_no) as conv_acct_no,
           |trim(inner_pro_ind) as inner_pro_ind,
           |trim(acct_proc_in) as acct_proc_in,
           |order_id
           |from
           |HIVE_SWT_LOG
           |where
           |part_trans_dt >= '$start_dt' and part_trans_dt <= '$end_dt' and
           |trans_tp in ('S370000000','S380000000')
      """.stripMargin
      )
      println("#### JOB_HV_43 spark sql 逻辑完成的系统时间为:"+DateUtils.getCurrentSystemTime())
      //println("#### JOB_HV_43------>results:"+results.count())

      results.registerTempTable("spark_swt_log")
      println("#### JOB_HV_43 registerTempTable--spark_swt_log 完成的系统时间为: "+DateUtils.getCurrentSystemTime())

      if(!Option(results).isEmpty){
        sqlContext.sql(
          """
            |insert overwrite table hive_switch_point_trans partition (part_trans_dt)
            |select
            |tfr_dt_tm,
            |sys_tra_no,
            |acpt_ins_id_cd,
            |msg_fwd_ins_id_cd,
            |pri_key1,
            |fwd_chnl_head,
            |chswt_plat_seq,
            |trans_tm,
            |trans_dt,
            |cswt_settle_dt,
            |internal_trans_tp,
            |settle_trans_id,
            |trans_tp,
            |cups_settle_dt,
            |msg_tp,
            |pri_acct_no,
            |card_bin,
            |proc_cd,
            |req_trans_at,
            |resp_trans_at,
            |trans_curr_cd,
            |trans_tot_at,
            |iss_ins_id_cd,
            |launch_trans_tm,
            |launch_trans_dt,
            |mchnt_tp,
            |pos_entry_md_cd,
            |card_seq_id,
            |pos_cond_cd,
            |pos_pin_capture_cd,
            |retri_ref_no,
            |term_id,
            |mchnt_cd,
            |card_accptr_nm_loc,
            |sec_related_ctrl_inf,
            |orig_data_elemts,
            |rcv_ins_id_cd,
            |fwd_proc_in,
            |rcv_proc_in,
            |proj_tp,
            |usr_id,
            |conv_usr_id,
            |trans_st,
            |inq_dtl_req,
            |inq_dtl_resp,
            |iss_ins_resv,
            |ic_flds,
            |cups_def_fld,
            |id_no,
            |cups_resv,
            |acpt_ins_resv,
            |rout_ins_id_cd,
            |sub_rout_ins_id_cd,
            |recv_access_resp_cd,
            |chswt_resp_cd,
            |chswt_err_cd,
            |resv_fld1,
            |resv_fld2,
            |to_ts,
            |rec_upd_ts,
            |rec_crt_ts,
            |settle_at,
            |external_amt,
            |discount_at,
            |card_pay_at,
            |right_purchase_at,
            |recv_second_resp_cd,
            |req_acpt_ins_resv,
            |log_id,
            |conv_acct_no,
            |inner_pro_ind,
            |acct_proc_in,
            |order_id,
            |trans_dt
            |from spark_swt_log
          """.stripMargin)
        println("#### JOB_HV_43 动态分区插入完成的时间为："+DateUtils.getCurrentSystemTime())
      }else{
        println("#### JOB_HV_43 spark sql 逻辑处理后无数据！")
      }
    }
  }

  /**
    * JobName: hive-job-77  数据来源于job71（2015-11-09和10两天有数据）
    * Feature:  hive_ach_order_inf(part_hp_trans_dt=2015-11-09)-->hive_life_order_inf
    * @author tzq
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    * @return
    */
  def JOB_HV_77(implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {
    println("#### JOB_HV_77(  hive_ach_order_inf-> hive_life_order_inf  )")
    println("#### JOB_HV_77 增量抽取的时间范围: "+start_dt+"--"+end_dt)

    DateUtils.timeCost("JOB_HV_77"){

      sqlContext.sql(s"use $hive_dbname")
      sqlContext.sql(
        s"""
           |insert overwrite table hive_life_order_inf partition(part_hp_trans_dt)
           |select
           |a.order_id,
           |a.sys_no,
           |a.mchnt_version,
           |a.encoding,
           |a.sign_method,
           |a.mchnt_trans_tp,
           |a.biz_tp,
           |a.pay_method,
           |a.trans_tp,
           |a.buss_chnl,
           |a.mchnt_front_url,
           |a.mchnt_back_url,
           |a.acq_ins_id_cd,
           |a.mchnt_cd,
           |a.mchnt_tp,
           |a.mchnt_nm,
           |a.sub_mchnt_cd,
           |a.sub_mchnt_nm,
           |a.mchnt_order_id,
           |a.trans_tm,
           |a.trans_dt,
           |a.sys_tm,
           |a.pay_timeout,
           |a.trans_at,
           |a.trans_curr_cd,
           |a.kz_at,
           |a.kz_curr_cd,
           |a.conv_dt,
           |a.deduct_at,
           |a.discount_info,
           |a.upoint_at,
           |a.top_info,
           |a.refund_at,
           |a.iss_ins_id_cd,
           |a.iss_head,
           |a.pri_acct_no,
           |a.card_attr,
           |a.usr_id,
           |a.phone_no,
           |a.trans_ip,
           |a.trans_st,
           |a.trans_no,
           |a.trans_idx,
           |a.sys_tra_no,
           |a.order_desc,
           |a.order_detail,
           |a.proc_sys,
           |a.proc_st,
           |a.trans_source,
           |a.resp_cd,
           |a.other_usr,
           |a.initial_pay,
           |a.to_ts,
           |a.rec_crt_ts,
           |a.rec_upd_ts,
           |b.buss_tp_nm as buss_tp_nm,
           |b.chnl_tp_nm as chnl_tp_nm,
           |a.part_hp_trans_dt
           |from
           |hive_ach_order_inf a
           |left join hive_life b
           |on a.mchnt_cd = b.mchnt_cd
           |where
           |a.mchnt_cd in ('048000049000001','048000049000002','048000049000003','048000065130001',
           |'048000092220001','048000048140002','048000048140003','048000048140004','048000094980001',
           |'443701049000008','443701094980005','802500049000098','048000049001001','048000049001002',
           |'048000049001003','048000065131001','048000092221001','048000048141002','048000048141003',
           |'048000048141004','048000094980001')
           |
	      """.stripMargin)

      println(s"#### JOB_HV_77 当前动态分区插入完成的时间为："+DateUtils.getCurrentSystemTime())

    }

  }

  /**
    * JobName: JOB_HV_78
    * Feature: hive_acc_trans->hive_cdhd_trans_year
    * @author tzq
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    */
  def JOB_HV_78(implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {
    println("#### JOB_HV_78( hive_cdhd_trans_year  -->  hive_acc_trans)")
    println("#### JOB_HV_78 增量抽取的时间范围: "+start_dt+"--"+end_dt)

    DateUtils.timeCost("JOB_HV_78"){
      sqlContext.sql(s"use $hive_dbname")
      sqlContext.sql(
        s"""
           |insert overwrite table hive_cdhd_trans_year partition (part_year)
           |select distinct cdhd_usr_id,year,year
           |from (
           |select distinct cdhd_usr_id,year(to_date(trans_dt)) as year
           |from hive_acc_trans
           |where
           |part_trans_dt>='$start_dt' and part_trans_dt<='$end_dt'
           |
           |union all
           |select cdhd_usr_id,year from hive_cdhd_trans_year
           |where
           |part_year >= year(to_date('$start_dt')) and part_year <= year(to_date('$end_dt'))
           |)tmp
           |
              """.stripMargin)
      println("#### JOB_HV_78 动态分区插入完成的时间为："+DateUtils.getCurrentSystemTime())
    }
  }
}
