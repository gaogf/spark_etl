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

    //从数据库中获取当前JOB的执行起始和结束日期
    val rowParams=UPSQL_TIMEPARAMS_JDBC.readTimeParams(sqlContext)
    val start_dt=DateUtils.getYesterdayByJob(rowParams.getString(0))//获取开始日期：start_dt-1
    val end_dt=rowParams.getString(1)//结束日期
    val interval=DateUtils.getIntervalDays(start_dt,end_dt).toInt

    println(s"#### SparkUPWH2H 数据抽取的起始日期为: $start_dt --  $end_dt")

    val JobName = if(args.length>0) args(0) else None
    println(s"#### 当前执行JobName为： $JobName ####")
    JobName match {
      /**
        * 每日模板job
        */
      case "JOB_HV_40"  => JOB_HV_40(sqlContext,start_dt,end_dt) //CODE BY TZQ
      case "JOB_HV_42"  => JOB_HV_42(sqlContext,start_dt,end_dt) //CODE BY TZQ
      case "JOB_HV_43"  => JOB_HV_43(sqlContext,start_dt,end_dt)  //CODE BY YX
      case "JOB_HV_77"  => JOB_HV_77(sqlContext,start_dt,end_dt) //CODE BY TZQ 动态分区插入失败，未解决！
      case "JOB_HV_78"  => JOB_HV_78(sqlContext,start_dt,end_dt) //CODE BY TZQ

      /**
        *  指标套表job
        */

      case _ => println("#### No Case Job,Please Input JobName")
    }

    sc.stop()

  }


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
