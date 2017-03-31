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
//    start_dt=rowParams.getString(0)  //获取开始日期,大数据平台抽取T-1数据,start_dt取当前系统时间减一天
//    end_dt=rowParams.getString(1)//结束日期,大数据平台抽取T-1数据,end_dt取当前系统时间减一天

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

    //获取开始日期和结束日期的间隔天数
    val interval=DateUtils.getIntervalDays(start_dt,end_dt).toInt

    println(s"#### SparkUPWH2H 数据抽取的起始日期为: $start_dt --  $end_dt")

    val JobName = if(args.length>0) args(0) else None
    println(s"#### 当前执行JobName为： $JobName ####")
    JobName match {
      /**
        * 每日模板job
        */
      case "JOB_HV_5" => JOB_HV_5(sqlContext)  //CODE BY YX
      case "JOB_HV_4" => JOB_HV_4(sqlContext, start_dt, end_dt)  //CODE BY XTP
      case "JOB_HV_40"  => JOB_HV_40(sqlContext,start_dt,end_dt) //CODE BY TZQ.
      case "JOB_HV_42"  => JOB_HV_42(sqlContext,start_dt,end_dt) //CODE BY TZQ
      case "JOB_HV_43"  => JOB_HV_43(sqlContext,start_dt,end_dt)  //CODE BY YX


      /**
        *  指标套表job
        */
      case "JOB_HV_27" => JOB_HV_27(sqlContext, start_dt, end_dt) //CODE BY XTP
      case "JOB_HV_77"  => JOB_HV_77(sqlContext,start_dt,end_dt) //CODE BY TZQ
      case "JOB_HV_78"  => JOB_HV_78(sqlContext,start_dt,end_dt) //CODE BY TZQ

      case _ => println("#### No Case Job,Please Input JobName")
    }

    sc.stop()

  }

  /**
    * JobName: JOB_HV_5
    * Feature: hive.hive_cdhd_pri_acct_inf + hive.hive_ucbiz_cdhd_bas_inf -> hive.hive_pri_acct_inf
    *
    * @author YangXue
    * @time 2017-03-30
    * @param sqlContext
    */
  def JOB_HV_5(implicit sqlContext: HiveContext) = {
    println("#### JOB_HV_5(hive_cdhd_pri_acct_inf + hive_ucbiz_cdhd_bas_inf -> hive_pri_acct_inf)")
    println("#### JOB_HV_5 为全量抽取的表")

    DateUtils.timeCost("JOB_HV_5"){
      sqlContext.sql(s"use $hive_dbname")
      val results = sqlContext.sql(
        s"""
           |select
           |trim(t1.cdhd_usr_id) as cdhd_usr_id,
           |case
           |	when
           |		substr(t1.reg_dt,1,4) between '0001' and '9999' and substr(t1.reg_dt,5,2) between '01' and '12' and
           |		substr(t1.reg_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(t1.reg_dt,1,4),substr(t1.reg_dt,5,2),substr(t1.reg_dt,7,2))),9,2)
           |	then concat_ws('-',substr(t1.reg_dt,1,4),substr(t1.reg_dt,5,2),substr(t1.reg_dt,7,2))
           |	else null
           |end as reg_dt,
           |t1.usr_nm as usr_nm,
           |trim(t1.mobile) as mobile,
           |trim(t1.mobile_vfy_st) as mobile_vfy_st,
           |t1.email_addr as email_addr,
           |trim(t1.email_vfy_st) as email_vfy_st,
           |trim(t1.inf_source) as inf_source,
           |t1.real_nm as real_nm,
           |trim(t1.real_nm_st) as real_nm_st,
           |t1.nick_nm as nick_nm,
           |trim(t1.certif_tp) as certif_tp,
           |trim(t1.certif_id) as certif_id,
           |trim(t1.certif_vfy_st) as certif_vfy_st,
           |case
           |	when
           |		substr(t1.birth_dt,1,4) between '0001' and '9999' and substr(t1.birth_dt,5,2) between '01' and '12' and
           |		substr(t1.birth_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(t1.birth_dt,1,4),substr(t1.birth_dt,5,2),substr(t1.birth_dt,7,2))),9,2)
           |	then concat_ws('-',substr(t1.birth_dt,1,4),substr(t1.birth_dt,5,2),substr(t1.birth_dt,7,2))
           |	else null
           |end as birth_dt,
           |trim(t1.sex) as sex,
           |trim(t1.age) as age,
           |trim(t1.marital_st) as marital_st,
           |trim(t1.home_mem_num) as home_mem_num,
           |trim(t1.cntry_cd) as cntry_cd,
           |trim(t1.gb_region_cd) as gb_region_cd,
           |t1.comm_addr as comm_addr,
           |trim(t1.zip_cd) as zip_cd,
           |trim(t1.nationality) as nationality,
           |trim(t1.ed_lvl) as ed_lvl,
           |t1.msn_no as msn_no,
           |trim(t1.qq_no) as qq_no,
           |t1.person_homepage as person_homepage,
           |trim(t1.industry_id) as industry_id,
           |trim(t1.annual_income_lvl) as annual_income_lvl,
           |trim(t1.hobby) as hobby,
           |trim(t1.brand_prefer) as brand_prefer,
           |trim(t1.buss_dist_prefer) as buss_dist_prefer,
           |trim(t1.head_pic_file_path) as head_pic_file_path,
           |trim(t1.pwd_cue_ques) as pwd_cue_ques,
           |trim(t1.pwd_cue_answ) as pwd_cue_answ,
           |trim(t1.usr_eval_lvl) as usr_eval_lvl,
           |trim(t1.usr_class_lvl)as usr_class_lvl,
           |trim(t1.usr_st) as usr_st,
           |t1.open_func as open_func,
           |t1.rec_crt_ts as rec_crt_ts,
           |t1.rec_upd_ts as rec_upd_ts,
           |trim(t1.mobile_new) as mobile_new,
           |t1.email_addr_new as email_addr_new,
           |t1.activate_ts as activate_ts,
           |t1.activate_pwd as activate_pwd,
           |trim(t1.region_cd) as region_cd,
           |t1.ver_no as ver_no,
           |trim(t1.func_bmp) as func_bmp,
           |t1.point_pre_open_ts as point_pre_open_ts,
           |t1.refer_usr_id as refer_usr_id,
           |t1.vendor_fk as vendor_fk,
           |t1.phone as phone,
           |t1.vip_svc as vip_svc,
           |t1.user_lvl_id as user_lvl_id,
           |t1.auto_adjust_lvl_in as auto_adjust_lvl_in,
           |case
           |	when
           |		substr(t1.lvl_begin_dt,1,4) between '0001' and '9999' and substr(t1.lvl_begin_dt,5,2) between '01' and '12' and
           |		substr(t1.lvl_begin_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(t1.lvl_begin_dt,1,4),substr(t1.lvl_begin_dt,5,2),substr(t1.lvl_begin_dt,7,2))),9,2)
           |	then concat_ws('-',substr(t1.lvl_begin_dt,1,4),substr(t1.lvl_begin_dt,5,2),substr(t1.lvl_begin_dt,7,2))
           |	else null
           |end  as lvl_begin_dt,
           |t1.customer_title as customer_title,
           |t1.company as company,
           |t1.dept as dept,
           |t1.duty as duty,
           |t1.resv_phone as resv_phone,
           |t1.join_activity_list as join_activity_list,
           |t1.remark as remark,
           |t1.note as note,
           |case
           |	when
           |		substr(t1.usr_lvl_expire_dt,1,4) between '0001' and '9999' and substr(t1.usr_lvl_expire_dt,5,2) between '01' and '12' and
           |		substr(t1.usr_lvl_expire_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(t1.usr_lvl_expire_dt,1,4),substr(t1.usr_lvl_expire_dt,5,2),substr(t1.usr_lvl_expire_dt,7,2))),9,2)
           |	then concat_ws('-',substr(t1.usr_lvl_expire_dt,1,4),substr(t1.usr_lvl_expire_dt,5,2),substr(t1.usr_lvl_expire_dt,7,2))
           |	else null
           |end as usr_lvl_expire_dt,
           |trim(t1.reg_card_no) as reg_card_no,
           |trim(t1.reg_tm) as reg_tm,
           |t1.activity_source as activity_source,
           |trim(t1.chsp_svc_in) as chsp_svc_in,
           |trim(t1.accept_sms_in) as accept_sms_in,
           |trim(t1.prov_division_cd) as prov_division_cd,
           |trim(t1.city_division_cd) as city_division_cd,
           |trim(t1.vid_last_login) as vid_last_login,
           |trim(t1.pay_pwd) as pay_pwd,
           |trim(t1.pwd_set_st) as pwd_set_st,
           |trim(t1.realnm_in) as realnm_in,
           |case
           |	when length(t1.certif_id)=15 then concat('19',substr(t1.certif_id,7,6))
           |	when length(t1.certif_id)=18 and substr(t1.certif_id,7,2) in ('19','20') then substr(t1.certif_id,7,8)
           |	else null
           |end as birthday,
           |case
           |when length(trim(t1.certif_id)) in (15,18) then t2.name
           |else null
           |end as province_card,
           |case
           |when length(trim(t1.certif_id)) in (15,18) then t3.name
           |else null
           |end as city_card,
           |case
           |	when length(trim(t1.mobile)) >= 11
           |	then t4.name
           |	else null
           |end as mobile_provider,
           |case
           |	when length(t1.certif_id)=15
           |	then
           |		case
           |			when int(substr(t1.certif_id,15,1))%2 = 0 then '女'
           |			when int(substr(t1.certif_id,15,1))%2 = 1 then '男'
           |			else null
           |		end
           |	when length(t1.certif_id)=18
           |	then
           |		case
           |			when int(substr(t1.certif_id,17,1))%2 = 0 then '女'
           |			when int(substr(t1.certif_id,15,1))%2 = 1 then '男'
           |			else null
           |		end
           |	else null
           |end as sex_card,
           |case
           |	when trim(t1.city_division_cd)='210200' then '大连'
           |	when trim(t1.city_division_cd)='330200' then '宁波'
           |	when trim(t1.city_division_cd)='350200' then '厦门'
           |	when trim(t1.city_division_cd)='370200' then '青岛'
           |	when trim(t1.city_division_cd)='440300' then '深圳'
           |	when trim(t1.prov_division_cd) like '11%' then '北京'
           |	when trim(t1.prov_division_cd) like '12%' then '天津'
           |	when trim(t1.prov_division_cd) like '13%' then '河北'
           |	when trim(t1.prov_division_cd) like '14%' then '山西'
           |	when trim(t1.prov_division_cd) like '15%' then '内蒙古'
           |	when trim(t1.prov_division_cd) like '21%' then '辽宁'
           |	when trim(t1.prov_division_cd) like '22%' then '吉林'
           |	when trim(t1.prov_division_cd) like '23%' then '黑龙江'
           |	when trim(t1.prov_division_cd) like '31%' then '上海'
           |	when trim(t1.prov_division_cd) like '32%' then '江苏'
           |	when trim(t1.prov_division_cd) like '33%' then '浙江'
           |	when trim(t1.prov_division_cd) like '34%' then '安徽'
           |	when trim(t1.prov_division_cd) like '35%' then '福建'
           |	when trim(t1.prov_division_cd) like '36%' then '江西'
           |	when trim(t1.prov_division_cd) like '37%' then '山东'
           |	when trim(t1.prov_division_cd) like '41%' then '河南'
           |	when trim(t1.prov_division_cd) like '42%' then '湖北'
           |	when trim(t1.prov_division_cd) like '43%' then '湖南'
           |	when trim(t1.prov_division_cd) like '44%' then '广东'
           |	when trim(t1.prov_division_cd) like '45%' then '广西'
           |	when trim(t1.prov_division_cd) like '46%' then '海南'
           |	when trim(t1.prov_division_cd) like '50%' then '重庆'
           |	when trim(t1.prov_division_cd) like '51%' then '四川'
           |	when trim(t1.prov_division_cd) like '52%' then '贵州'
           |	when trim(t1.prov_division_cd) like '53%' then '云南'
           |	when trim(t1.prov_division_cd) like '54%' then '西藏'
           |	when trim(t1.prov_division_cd) like '61%' then '陕西'
           |	when trim(t1.prov_division_cd) like '62%' then '甘肃'
           |	when trim(t1.prov_division_cd) like '63%' then '青海'
           |	when trim(t1.prov_division_cd) like '64%' then '宁夏'
           |	when trim(t1.prov_division_cd) like '65%' then '新疆'
           |	else '总公司'
           |end as phone_location,
           |t5.relate_id as relate_id
           |from
           |hive_cdhd_pri_acct_inf t1
           |left join
           |hive_province_card t2 on trim(substr(t1.certif_id,1,2)) = trim(t2.id)
           |left join
           |hive_city_card t3 on trim(substr(t1.certif_id,1,4)) = trim(t3.id)
           |left join
           |hive_ct t4 on trim(substr(substr(t1.mobile,-11,11),1,4)) = trim(t4.id)
           |left join
           |hive_ucbiz_cdhd_bas_inf t5 on trim(t1.cdhd_usr_id) = trim(t5.usr_id)
         """.stripMargin)
      println("#### JOB_HV_5 spark sql 逻辑完成的系统时间为:"+DateUtils.getCurrentSystemTime())
      //println("#### JOB_HV_5------>results:"+results.count())

      results.registerTempTable("spark_pri_acct_inf")
      println("#### JOB_HV_5 registerTempTable--spark_pri_acct_inf 完成的系统时间为:"+DateUtils.getCurrentSystemTime())

      if(!Option(results).isEmpty){
        sqlContext.sql("insert overwrite table hive_pri_acct_inf select * from spark_pri_acct_inf")
        println("#### JOB_HV_5 全量数据插入完成的时间为："+DateUtils.getCurrentSystemTime())
      }else{
        println("#### JOB_HV_5 spark sql 逻辑处理后无数据！")
      }
    }
  }

  /**
    * JOB_HV_4/03-27  数据清洗
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
           |ta.first_trans_proc_start_ts  as first_trans_proc_start_ts,
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
           |tempd.trans_proc_start_ts as first_trans_proc_start_ts,
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
           |tempw.seq_id                             ,
           |tempw.cdhd_usr_id                        ,
           |tempw.card_no                            ,
           |tempw.trans_tfr_tm                       ,
           |tempw.sys_tra_no                         ,
           |tempw.acpt_ins_id_cd                     ,
           |tempw.fwd_ins_id_cd                      ,
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
           |tempv.first_trans_proc_start_ts as first_trans_proc_start_ts,
           |tempv.second_trans_proc_start_ts as second_trans_proc_start_ts,
           |tempv.third_trans_proc_start_ts as third_trans_proc_start_ts,
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
           |tempw.dtl_inq_data
           |
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
           |tempb.dtl_inq_data                       ,
           |row_number() over (partition by tempa.trans_tfr_tm,tempa.sys_tra_no,tempa.acpt_ins_id_cd,tempa.fwd_ins_id_cd order by tempa.trans_proc_start_ts) rank
           |from
           |(select * from viw_chacc_acc_trans_dtl where um_trans_id='AC02202000') tempa,
           |(select * from viw_chacc_acc_trans_log where um_trans_id='AC02202000') tempb
           |)
           |tempw
           |
           |right join
           |(
           |select
           |tempx.trans_tfr_tm,
           |tempx.sys_tra_no,
           |tempx.acpt_ins_id_cd,
           |tempx.fwd_ins_id_cd,
           |max(case when tempx.rank=1 then tempx.trans_proc_start_ts else null end) as first_trans_proc_start_ts,
           |max(case when tempx.rank=2 then tempx.trans_proc_start_ts else null end) as second_trans_proc_start_ts,
           |max(case when tempx.rank=3 then tempx.trans_proc_start_ts else null end) as third_trans_proc_start_ts
           |from
           |(
           |select
           |tempz.trans_tfr_tm,
           |tempz.sys_tra_no,
           |tempz.acpt_ins_id_cd,
           |tempz.fwd_ins_id_cd,
           |tempz.trans_proc_start_ts,
           |row_number() over (partition by tempz.trans_tfr_tm,tempz.sys_tra_no,tempz.acpt_ins_id_cd,tempz.fwd_ins_id_cd order by tempz.trans_proc_start_ts) rank
           |from
           |(select * from viw_chacc_acc_trans_dtl where um_trans_id='AC02202000') tempz,
           |(select * from viw_chacc_acc_trans_log where um_trans_id='AC02202000') tempy
           |)tempx
           |where tempx.rank<=3
           |group by
           |tempx.trans_tfr_tm,
           |tempx.sys_tra_no,
           |tempx.acpt_ins_id_cd,
           |tempx.fwd_ins_id_cd
           |) tempv
           |on tempw.trans_tfr_tm=tempv.trans_tfr_tm and tempw.sys_tra_no=tempv.sys_tra_no and tempw.acpt_ins_id_cd=tempv.acpt_ins_id_cd and tempw.fwd_ins_id_cd=tempv.fwd_ins_id_cd
           |where tempw.rank=1
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
             |first_trans_proc_start_ts,
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



  /**
    * JOB_HV_27/10-28
    * hive_search_trans->viw_chacc_acc_trans_log,viw_chmgm_swt_log
    * Code by Xue
    *
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    */
  def JOB_HV_27 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {
    DateUtils.timeCost("JOB_HV_27"){
      sqlContext.sql(s"use $hive_dbname")
      val results = sqlContext.sql(
        s"""
           |select
           |nvl(b.tfr_dt_tm,a.trans_tfr_tm) as tfr_dt_tm,
           |nvl(b.sys_tra_no,a.sys_tra_no) as sys_tra_no,
           |nvl(b.acpt_ins_id_cd,a.acpt_ins_id_cd) as acpt_ins_id_cd,
           |nvl(b.msg_fwd_ins_id_cd,a.fwd_ins_id_cd) as fwd_ins_id_cd,
           |b.pri_key1 as pri_key1,
           |b.fwd_chnl_head as fwd_chnl_head,
           |b.chswt_plat_seq as chswt_plat_seq,
           |trim(b.trans_tm) as trans_tm,
           |case when
           |substr(trim(b.trans_dt),1,4) between '0001' and '9999' and substr(trim(b.trans_dt),5,2) between '01' and '12' and
           |substr(trim(b.trans_dt),7,2) between '01' and substr(last_day(concat_ws('-',substr(trim(b.trans_dt),1,4),substr(trim(b.trans_dt),5,2),substr(trim(b.trans_dt),7,2))),9,2)
           |then concat_ws('-',substr(b.trans_dt,1,4),substr(b.trans_dt,5,2),substr(b.trans_dt,7,2))
           |else null end as trans_dt,
           |case when
           |substr(nvl(trim(b.cswt_settle_dt),trim(a.sys_settle_dt)) ,1,4) between '0001' and '9999' and substr(nvl(trim(b.cswt_settle_dt),trim(a.sys_settle_dt)),5,2) between '01' and '12' and
           |substr(nvl(trim(b.cswt_settle_dt),trim(a.sys_settle_dt)),7,2) between '01' and substr(last_day(concat_ws('-',substr(nvl(trim(b.cswt_settle_dt),trim(a.sys_settle_dt)),1,4),substr(nvl(trim(b.cswt_settle_dt),trim(a.sys_settle_dt)),5,2),substr(nvl(trim(b.cswt_settle_dt),trim(a.sys_settle_dt)),7,2))),9,2)
           |then concat_ws('-',substr(nvl(trim(b.cswt_settle_dt),trim(a.sys_settle_dt)),1,4),substr(nvl(trim(b.cswt_settle_dt),trim(a.sys_settle_dt)),5,2),substr(nvl(trim(b.cswt_settle_dt),trim(a.sys_settle_dt)),7,2))
           |else null end as cswt_settle_dt,
           |trim(b.internal_trans_tp) as internal_trans_tp,
           |trim(b.settle_trans_id) as settle_trans_id,
           |trim(b.trans_tp) as trans_tp,
           |trim(b.cups_settle_dt) as cups_settle_dt,
           |nvl(b.msg_tp,a.msg_tp) as msg_tp,
           |trim(b.pri_acct_no) as pri_acct_no,
           |trim(b.card_bin) as card_bin,
           |nvl(b.proc_cd,a.proc_cd) as proc_cd,
           |b.req_trans_at as req_trans_at,
           |b.resp_trans_at as resp_trans_at,
           |nvl(b.trans_curr_cd,a.trans_curr_cd) as trans_curr_cd,
           |b.trans_tot_at as trans_tot_at,
           |trim(b.iss_ins_id_cd) as iss_ins_id_cd,
           |trim(b.launch_trans_tm) as launch_trans_tm,
           |trim(b.launch_trans_dt) as launch_trans_dt,
           |nvl(b.mchnt_tp,a.mchnt_tp) as mchnt_tp,
           |trim(b.pos_entry_md_cd) as pos_entry_md_cd,
           |nvl(b.card_seq_id,a.card_seq) as card_seq_id,
           |trim(b.pos_cond_cd) as pos_cond_cd,
           |nvl(b.pos_pin_capture_cd,a.pos_pin_capture_cd) as pos_pin_capture_cd,
           |nvl(b.retri_ref_no,a.retri_ref_no) as retri_ref_no,
           |nvl(b.term_id,a.card_accptr_term_id) as term_id,
           |nvl(b.mchnt_cd,a.card_accptr_cd) as mchnt_cd,
           |nvl(b.card_accptr_nm_loc,a.card_accptr_nm_addr) as card_accptr_nm_loc,
           |nvl(b.sec_related_ctrl_inf,a.sec_ctrl_inf) as sec_related_ctrl_inf,
           |nvl(b.orig_data_elemts,a.orig_data_elemnt) as orig_data_elemts,
           |nvl(b.rcv_ins_id_cd,a.rcv_ins_id_cd) as rcv_ins_id_cd,
           |trim(b.fwd_proc_in) as fwd_proc_in,
           |trim(b.rcv_proc_in) as rcv_proc_in,
           |trim(b.proj_tp) as proj_tp,
           |b.usr_id as usr_id,
           |b.conv_usr_id as conv_usr_id,
           |trim(b.trans_st) as trans_st,
           |b.inq_dtl_req as inq_dtl_req,
           |b.inq_dtl_resp as inq_dtl_resp,
           |b.iss_ins_resv as iss_ins_resv,
           |b.ic_flds as ic_flds,
           |b.cups_def_fld as cups_def_fld,
           |trim(b.id_no) as id_no,
           |b.cups_resv as cups_resv,
           |b.acpt_ins_resv as acpt_ins_resv,
           |trim(b.rout_ins_id_cd) as rout_ins_id_cd,
           |trim(b.sub_rout_ins_id_cd) as sub_rout_ins_id_cd,
           |trim(b.recv_access_resp_cd) as recv_access_resp_cd,
           |trim(b.chswt_resp_cd) as chswt_resp_cd,
           |trim(b.chswt_err_cd) as chswt_err_cd,
           |b.resv_fld1 as resv_fld1,
           |b.resv_fld2 as resv_fld2,
           |b.to_ts as to_ts,
           |nvl(b.rec_upd_ts,a.rec_upd_ts) as rec_upd_ts,
           |b.rec_crt_ts as rec_crt_ts,
           |nvl(b.settle_at,a.settle_at) as settle_at,
           |b.external_amt as external_amt,
           |b.discount_at as discount_at,
           |b.card_pay_at as card_pay_at,
           |b.right_purchase_at as right_purchase_at,
           |trim(b.recv_second_resp_cd) as recv_second_resp_cd,
           |b.req_acpt_ins_resv as req_acpt_ins_resv,
           |trim(b.log_id) as log_id,
           |trim(b.conv_acct_no) as conv_acct_no,
           |trim(b.inner_pro_ind) as inner_pro_ind,
           |trim(b.acct_proc_in) as acct_proc_in,
           |b.order_id as order_id,
           |a.seq_id as seq_id,
           |trim(a.oper_module) as oper_module,
           |trim(a.um_trans_id) as um_trans_id,
           |trim(a.cdhd_fk) as cdhd_fk,
           |trim(a.bill_id) as bill_id,
           |trim(a.bill_tp) as bill_tp,
           |trim(a.bill_bat_no) as bill_bat_no,
           |a.bill_inf as bill_inf,
           |trim(a.card_no) as card_no,
           |case
           |	when length((translate(trim(a.trans_at),'-0123456789',' ')))=0 then trim(a.trans_at)
           |	else null
           |end as trans_at,
           |trim(a.settle_curr_cd) as settle_curr_cd,
           |trim(a.card_accptr_local_tm) as card_accptr_local_tm,
           |trim(a.card_accptr_local_dt) as card_accptr_local_dt,
           |trim(a.expire_dt) as expire_dt,
           |case when
           |substr(trim(a.msg_settle_dt),1,4) between '0001' and '9999' and substr(trim(a.msg_settle_dt),5,2) between '01' and '12' and
           |substr(trim(a.msg_settle_dt),7,2) between '01' and substr(last_day(concat_ws('-',substr(trim(a.msg_settle_dt),1,4),substr(trim(a.msg_settle_dt),5,2),substr(trim(a.msg_settle_dt),7,2))),9,2)
           |then concat_ws('-',substr(trim(a.msg_settle_dt),1,4),substr(trim(a.msg_settle_dt),5,2),substr(trim(a.msg_settle_dt),7,2))
           |else null end as msg_settle_dt,
           |trim(a.auth_id_resp_cd) as auth_id_resp_cd,
           |trim(a.resp_cd) as resp_cd,
           |trim(a.notify_st) as notify_st,
           |a.addn_private_data as addn_private_data,
           |a.udf_fld as udf_fld,
           |trim(a.addn_at) as addn_at,
           |a.acct_id_1 as acct_id_1,
           |a.acct_id_2 as acct_id_2,
           |a.resv_fld as resv_fld,
           |a.cdhd_auth_inf as cdhd_auth_inf,
           |trim(a.recncl_in) as recncl_in,
           |trim(a.match_in) as match_in,
           |a.trans_proc_start_ts as trans_proc_start_ts,
           |a.trans_proc_end_ts as trans_proc_end_ts,
           |trim(a.sys_det_cd) as sys_det_cd,
           |trim(a.sys_err_cd) as sys_err_cd,
           |a.dtl_inq_data as dtl_inq_data,
           |a.part_msg_settle_dt as p_msg_settle_dt
           |from
           |(select * from hive_trans_log where part_msg_settle_dt >= '$start_dt' and  part_msg_settle_dt <= '$end_dt' and um_trans_id='AC02003065' ) a
           |full join (select * from hive_swt_log where part_trans_dt >= '$start_dt' and part_trans_dt <= '$end_dt' and settle_trans_id='S38') b
           |on a.trans_tfr_tm=b.tfr_dt_tm and a.sys_tra_no=b.sys_tra_no and a.acpt_ins_id_cd=b.acpt_ins_id_cd and a.fwd_ins_id_cd=b.msg_fwd_ins_id_cd
           | """.stripMargin)

      println("#### JOB_HV_27 spark sql 逻辑完成的系统时间为:" + DateUtils.getCurrentSystemTime())

      results.registerTempTable("spark_hive_search_trans")
      println("#### JOB_HV_27 注册临时表的系统时间为:"+DateUtils.getCurrentSystemTime())
      //      println("JOB_HV_27------>results:"+results.count())

      if(!Option(results).isEmpty){
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql(
          """
            |insert overwrite table hive_search_trans partition (part_msg_settle_dt)
            |select
            |tfr_dt_tm                 ,
            |sys_tra_no                ,
            |acpt_ins_id_cd            ,
            |fwd_ins_id_cd             ,
            |pri_key1                  ,
            |fwd_chnl_head             ,
            |chswt_plat_seq            ,
            |trans_tm                  ,
            |trans_dt                  ,
            |cswt_settle_dt            ,
            |internal_trans_tp         ,
            |settle_trans_id           ,
            |trans_tp                  ,
            |cups_settle_dt            ,
            |msg_tp                    ,
            |pri_acct_no               ,
            |card_bin                  ,
            |proc_cd                   ,
            |req_trans_at              ,
            |resp_trans_at             ,
            |trans_curr_cd             ,
            |trans_tot_at              ,
            |iss_ins_id_cd             ,
            |launch_trans_tm           ,
            |launch_trans_dt           ,
            |mchnt_tp                  ,
            |pos_entry_md_cd           ,
            |card_seq_id               ,
            |pos_cond_cd               ,
            |pos_pin_capture_cd        ,
            |retri_ref_no              ,
            |term_id                   ,
            |mchnt_cd                  ,
            |card_accptr_nm_loc        ,
            |sec_related_ctrl_inf      ,
            |orig_data_elemts          ,
            |rcv_ins_id_cd             ,
            |fwd_proc_in               ,
            |rcv_proc_in               ,
            |proj_tp                   ,
            |usr_id                    ,
            |conv_usr_id               ,
            |trans_st                  ,
            |inq_dtl_req               ,
            |inq_dtl_resp              ,
            |iss_ins_resv              ,
            |ic_flds                   ,
            |cups_def_fld              ,
            |id_no                     ,
            |cups_resv                 ,
            |acpt_ins_resv             ,
            |rout_ins_id_cd            ,
            |sub_rout_ins_id_cd        ,
            |recv_access_resp_cd       ,
            |chswt_resp_cd             ,
            |chswt_err_cd              ,
            |resv_fld1                 ,
            |resv_fld2                 ,
            |to_ts                     ,
            |rec_upd_ts                ,
            |rec_crt_ts                ,
            |settle_at                 ,
            |external_amt              ,
            |discount_at               ,
            |card_pay_at               ,
            |right_purchase_at         ,
            |recv_second_resp_cd       ,
            |req_acpt_ins_resv         ,
            |log_id                    ,
            |conv_acct_no              ,
            |inner_pro_ind             ,
            |acct_proc_in              ,
            |order_id                  ,
            |seq_id                    ,
            |oper_module               ,
            |um_trans_id               ,
            |cdhd_fk                   ,
            |bill_id                   ,
            |bill_tp                   ,
            |bill_bat_no               ,
            |bill_inf                  ,
            |card_no                   ,
            |trans_at                  ,
            |settle_curr_cd            ,
            |card_accptr_local_tm      ,
            |card_accptr_local_dt      ,
            |expire_dt                 ,
            |msg_settle_dt             ,
            |auth_id_resp_cd           ,
            |resp_cd                   ,
            |notify_st                 ,
            |addn_private_data         ,
            |udf_fld                   ,
            |addn_at                   ,
            |acct_id_1                 ,
            |acct_id_2                 ,
            |resv_fld                  ,
            |cdhd_auth_inf             ,
            |recncl_in                 ,
            |match_in                  ,
            |trans_proc_start_ts       ,
            |trans_proc_end_ts         ,
            |sys_det_cd                ,
            |sys_err_cd                ,
            |dtl_inq_data              ,
            |p_msg_settle_dt
            |from spark_hive_search_trans
          """.stripMargin)
        println("#### JOB_HV_27 动态分区插入完成的时间为："+DateUtils.getCurrentSystemTime())

      }else{
        println("#### JOB_HV_27 spark sql 逻辑处理后无数据！")
      }


    }

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
             |part_settle_dt  >= '$start_dt' and part_settle_dt  <= '$end_dt'
             |and
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
           |where
           |part_settle_dt  >= '$start_dt' and part_settle_dt  <= '$end_dt'
           |and trans_source in ('0001','9001')
           |and mchnt_conn_tp='81'
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
    println("#### JOB_HV_43 增量抽取的时间范围: "+start_dt+" -- "+end_dt)

    DateUtils.timeCost("JOB_HV_43"){
      sqlContext.sql(s"use $hive_dbname")
      val results = sqlContext.sql(
        s"""
           |insert overwrite table hive_switch_point_trans partition(part_trans_dt)
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
           |order_id,
           		   |part_trans_dt
           |from
           |HIVE_SWT_LOG
           |where
           |part_trans_dt  >= '$start_dt' and part_trans_dt  <= '$end_dt' and
           |trans_tp in ('S370000000','S380000000')
      """.stripMargin
      )
      println("#### JOB_HV_43 spark sql 逻辑完成的系统时间为:"+DateUtils.getCurrentSystemTime())
      println("#### JOB_HV_43 动态分区插入完成的时间为："+DateUtils.getCurrentSystemTime())
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
           |part_hp_trans_dt  >= '$start_dt' and part_hp_trans_dt  <= '$end_dt'
           |and
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
