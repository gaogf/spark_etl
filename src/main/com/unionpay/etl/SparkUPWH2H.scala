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

    println(s"####当前JOB的执行日期为：start_dt=$start_dt,end_dt=$end_dt####")

    val jobName = if(args.length>0) args(0) else None
    println(s"#### 当前执行JobName为： $jobName ####")
    jobName match {
      /**
        * 每日模板job
        */
      case "JOB_HV_40"  => JOB_HV_40(sqlContext,start_dt,end_dt,interval) //CODE BY TZQ
      case "JOB_HV_42"  => JOB_HV_42(sqlContext,start_dt,end_dt,interval) //CODE BY TZQ
      case "JOB_HV_77"  => JOB_HV_77(sqlContext,start_dt,end_dt,interval) //CODE BY TZQ
      case "JOB_HV_78"  => JOB_HV_78(sqlContext,start_dt,end_dt) //CODE BY TZQ

      /**
        *  指标套表job
        */

    }

    sc.stop()

  }


  /**
    * hive-job-40/08-22
    * hive_life_trans->  hive_achis_trans（分区字段格式为：part_hp_settle_dt=2016-04-13）
    *
    * @author tzq
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    * @return
    */
  def JOB_HV_40(implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("###JOB_HV_40(hive_life_trans -> hive_achis_trans)")

    //1.循环从hive39的分区表中抽取数据到hive40的分区表中
    //1.1第一个分区从start_dt开始
    var part_dt = start_dt

    //spark sql 操作hive upw_hive 数据库中的表
    sqlContext.sql(s"use $hive_dbname")

    if(interval>0){
      for(i <- 0 to interval){

        //1.删除分区
        sqlContext.sql(s"alter table hive_life_trans drop partition (part_settle_dt='$part_dt')")

        //2.插入指定分区数据
        sqlContext.sql(
          s"""
             |insert into hive_life_trans partition(part_settle_dt='$part_dt')
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
             |b.chnl_tp_nm as chnl_tp_nm
             |from
             |hive_achis_trans a
             |left join hive_life b
             |on a.mchnt_cd = b.mchnt_cd
             |where
             |a.part_settle_dt = '$part_dt'
             |and a.mchnt_cd in ('048000049000001','048000049000002','048000049000003','048000065130001','048000092220001',
             |									 '048000048140002','048000048140003','048000048140004','048000094980001','443701049000008',
             |									 '443701094980005','802500049000098','048000049001001','048000049001002','048000049001003',
             |									 '048000065131001','048000092221001','048000048141002','048000048141003','048000048141004','048000094980001')
          """.stripMargin)
        //3.日期加1
        part_dt=DateUtils.addOneDay(part_dt)
      }
    }
  }


  /**
    * hive-job-42  2016年10月11日 星期二
    * hive_active_code_pay_trans-->hive_achis_trans
    * @author tzq
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    * @return
    */
  def JOB_HV_42(implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("###JOB_HV_42( hive_active_code_pay_trans-> hive_achis_trans)")
    //1.循环从hive39的分区表中抽取数据到hive42的分区表中
    //1.1第一个分区从start_dt开始
    var part_dt = start_dt

    //spark sql 操作hive upw_hive 数据库中的表
    sqlContext.sql(s"use $hive_dbname")

    if(interval>0){
      for(i <- 0 to interval){

        //1.删除分区
        sqlContext.sql(s"alter table hive_active_code_pay_trans drop partition (part_settle_dt='$part_dt')")

        //2.插入指定分区数据
        sqlContext.sql(
          s"""
             |insert into hive_active_code_pay_trans partition(part_settle_dt='$part_dt')
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
             |in_trans_tp
             |
             |from
             |hive_achis_trans
             |where
             |part_settle_dt = '$part_dt'
             |and trans_source in ('0001','9001') and mchnt_conn_tp='81'
             |
	      """.stripMargin)

        //3.日期加1

        part_dt=DateUtils.addOneDay(part_dt)//yyyy-MM-dd
      }
    }

  }

  /**
    * hive-job-77  数据来源于job71
    * hive_life_order_inf  ---->  hive_ach_order_inf(part_hp_trans_dt=2015-11-09)
    *
    * 测试：以分区part_hp_trans_dt=2015-11-09为例
    *    val start_dt="2015-11-09"
    *    val end_dt="2015-11-09"
    * 结果：正常
    * @author tzq
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    * @return
    */
  def JOB_HV_77(implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("###JOB_HV_77( hive_life_order_inf  -->  hive_ach_order_inf)")
    var part_dt = start_dt
    sqlContext.sql(s"use $hive_dbname")
    if(interval>=0){
      for(i <- 0 to interval){

        //1.删除分区
        sqlContext.sql(s"alter table hive_life_order_inf drop partition (part_hp_trans_dt='$part_dt')")

        //2.插入指定分区数据
        sqlContext.sql(
          s"""
             |insert into hive_life_order_inf partition(part_hp_trans_dt='$part_dt')
             |select
             |order_id,
             |sys_no,
             |mchnt_version,
             |encoding,
             |sign_method,
             |mchnt_trans_tp,
             |biz_tp,
             |pay_method,
             |trans_tp,
             |buss_chnl,
             |mchnt_front_url,
             |mchnt_back_url,
             |acq_ins_id_cd,
             |mchnt_cd,
             |mchnt_tp,
             |mchnt_nm,
             |sub_mchnt_cd,
             |sub_mchnt_nm,
             |mchnt_order_id,
             |trans_tm,
             |trans_dt,
             |sys_tm,
             |pay_timeout,
             |trans_at,
             |trans_curr_cd,
             |kz_at,
             |kz_curr_cd,
             |conv_dt,
             |deduct_at,
             |discount_info,
             |upoint_at,
             |top_info,
             |refund_at,
             |iss_ins_id_cd,
             |iss_head,
             |pri_acct_no,
             |card_attr,
             |usr_id,
             |phone_no,
             |trans_ip,
             |trans_st,
             |trans_no,
             |trans_idx,
             |sys_tra_no,
             |order_desc,
             |order_detail,
             |proc_sys,
             |proc_st,
             |trans_source,
             |resp_cd,
             |other_usr,
             |initial_pay,
             |to_ts,
             |rec_crt_ts,
             |rec_upd_ts
             |from
             |hive_ach_order_inf
             |where
             |part_hp_trans_dt= '$part_dt'
             |and mchnt_cd in ('048000049000001','048000049000002','048000049000003','048000065130001',
             |'048000092220001','048000048140002','048000048140003','048000048140004','048000094980001',
             |'443701049000008','443701094980005','802500049000098','048000049001001','048000049001002',
             |'048000049001003','048000065131001','048000092221001','048000048141002','048000048141003',
             |'048000048141004','048000094980001')
             |
	      """.stripMargin)

        println(s"insert into hive_life_order_inf at partition $part_dt successful !")

        //3.日期加1
        part_dt=DateUtils.addOneDay(part_dt)//yyyy-MM-dd
      }
    }

  }

  /**
    * JOB_HV_78
    *hive_cdhd_trans_year  -->  hive_acc_trans
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    */
  def JOB_HV_78(implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {
        println("###JOB_HV_78( hive_life_order_inf  -->  hive_ach_order_inf)")

       sqlContext.sql(s"use $hive_dbname")
        //2.插入指定分区数据
        val results=sqlContext.sql(
          s"""
             |insert into table hive_cdhd_trans_year partition (part_year)
             |select distinct cdhd_usr_id,year
             |from (
             |select distinct cdhd_usr_id,year(to_date(trans_dt)) as year
             |from hive_acc_trans
             |where
             |part_trans_dt>='$start_dt' and part_trans_dt<='$end_dt'
             |
             |union all
             |
             |select cdhd_usr_id,year from hive_cdhd_trans_year
             |where
             |part_year>=year(to_date('$start_dt')) and part_year<=year(to_date('$end_dt'))
             |)tmp
             |
	      """.stripMargin)

    println("results:"+results.count())

  }


}
