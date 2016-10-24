package com.unionpay.etl

import com.unionpay.conf.ConfigurationManager
import com.unionpay.constant.Constants
import com.unionpay.jdbc.UPSQL_TIMEPARAMS_JDBC
import com.unionpay.utils.DateUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * UPW->UPW hive库
  * Created by TZQ on 2016/10/24.
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

    /**
      * 每日模板job
      */
//    JOB_HV_40(sqlContext,start_dt,end_dt,interval)
//    JOB_HV_42(sqlContext,start_dt,end_dt,interval)

    /**
      *  指标套表job
      */


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
             |settle_dt,
             |trans_idx,
             |trans_tp ,
             |trans_class,
             |trans_source ,
             |buss_chnl,
             |carrier_tp ,
             |pri_acct_no ,
             |mchnt_conn_tp,
             |access_tp ,
             |conn_md ,
             |acq_ins_id_cd,
             |acq_head,
             |fwd_ins_id_cd,
             |rcv_ins_id_cd,
             |iss_ins_id_cd,
             |iss_head ,
             |iss_head_nm,
             |mchnt_cd  ,
             |mchnt_nm ,
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
             |
            |case
             |when mchnt_cd in ('048000049000001','048000049001001') then '水'
             |when mchnt_cd in ('048000049000002','048000049001002') then '电'
             |when mchnt_cd in ('048000049000003','048000049001003') then '煤'
             |when mchnt_cd in ('048000065130001','048000065131001') then '物业'
             |when mchnt_cd in ('048000092220001','048000092221001') then '交通罚款'
             |when mchnt_cd in ('048000048140002','048000048141002') then '移动'
             |when mchnt_cd in ('048000048140003','48000048141003')  then '联通'
             |when mchnt_cd in ('048000048140004','048000048141004') then '电信'
             |when mchnt_cd in ('048000094980001','048000094980001') then '信用卡还款'
             |when mchnt_cd='443701049000008' then '无卡转账'
             |when mchnt_cd='443701094980005' then '有卡转账'
             |when mchnt_cd = '802500049000098' then 'cp业务'
             |end
             |as buss_tp_nm,
             |
            |case
             |when mchnt_cd in ('048000049000001','048000049000002','048000049000003','048000065130001','048000092220001','048000048140002','048000048140003','048000048140004','048000094980001') then '客户端'
             |when mchnt_cd in ('048000049001001','048000049001002','048000049001003','048000065131001','048000092221001','048000048141002','048000048141003','048000048141004','048000094980001') then '门户'
             |else '客户端' end as chnl_tp_nm
             |
            |from
             |hive_achis_trans
             |where
             |part_settle_dt = '$part_dt'
             |and mchnt_cd in ('048000049000001','048000049000002','048000049000003','048000065130001','048000092220001','048000048140002','048000048140003','048000048140004','048000094980001','443701049000008','443701094980005','802500049000098','048000049001001','048000049001002','048000049001003','048000065131001','048000092221001','048000048141002','048000048141003','048000048141004','048000094980001')
             |
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


}
