package com.unionpay.etl

import  com.unionpay.jdbc.DB2_JDBC
import  com.unionpay.jdbc.DB2_JDBC.ReadDB2
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 作业：抽取DB2中的数据到Hive数据仓库
  * Created by tzq on 2016/10/13.
  */
object SparkDB22Hive {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("SparkDB22Hive")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new HiveContext(sc)

//--------TAN ZHENG QIANG---------------------------------------------------------
//        JOB_HV_9
//        JOB_HV_10
//        JOB_HV_11
//        JOB_HV_12
//        JOB_HV_13

//        JOB_HV_14
//        JOB_HV_15      //未添加
//        JOB_HV_16
//        JOB_HV_23      //未添加
//        JOB_HV_26      //未添加
//        JOB_HV_37      //未添加
//        JOB_HV_38      //未添加
//        JOB_HV_40      //未添加
//        JOB_HV_44      //未添加
//        JOB_HV_48      //未添加
//        JOB_HV_54      //未添加
//        JOB_HV_67      //未添加
//        JOB_HV_68      //未添加
//        JOB_HV_42      //未添加

//--------XUE TAI PING----------------------------------------------------------
//        JOB_HV_4       //未添加
//        JOB_HV_8       //未添加
//        JOB_HV_25      //未添加
//        JOB_HV_28      //未添加
//        JOB_HV_29      //未添加
//        JOB_HV_31      //未添加
//        JOB_HV_32      //未添加
//        JOB_HV_46      //未添加
//        JOB_HV_47      //未添加
//        JOB_HV_69      //未添加
//        JOB_HV_69      //未添加

//--------YANG XUE--------------------------------------------------------------
//         JOB_HV_1      //未添加
//         JOB_HV_3      //未添加
//         JOB_HV_18     //未添加
//         JOB_HV_19     //未添加
//         JOB_HV_24     //未添加
//         JOB_HV_30     //未添加
//         JOB_HV_36     //未添加
//         JOB_HV_43     //未添加
//         JOB_HV_70     //未添加

  }

  /**
    * JOB_HV_9/08-23
    * hive_preferential_mchnt_inf->tbl_chmgm_preferential_mchnt_inf
    * @param sqlContext
    * @return
    */
  def JOB_HV_9(implicit sqlContext: HiveContext) = {
    println("###JOB_HV_9(hive_preferential_mchnt_inf --->tbl_chmgm_preferential_mchnt_inf)")
    val df = sqlContext.jdbc_mgmdb_DF("CH_MGMDB.TBL_CHMGM_PREFERENTIAL_MCHNT_INF")
    df.registerTempTable("db2_tbl_chmgm_preferential_mchnt_inf")
    val results = sqlContext.sql(
      """
        |select
        |trim(mchnt_cd) as mchnt_cd,
        |mchnt_nm,
        |mchnt_addr,
        |mchnt_phone,
        |mchnt_url,
        |trim(mchnt_city_cd) as mchnt_city_cd,
        |trim(mchnt_county_cd) as mchnt_county_cd,
        |trim(mchnt_prov) as mchnt_prov,
        |buss_dist_cd,
        |mchnt_type_id,
        |cooking_style_id,
        |rebate_rate,
        |discount_rate,
        |avg_consume,
        |trim(point_mchnt_in) as point_mchnt_in,
        |trim(discount_mchnt_in) as discount_mchnt_in,
        |trim(preferential_mchnt_in) as preferential_mchnt_in,
        |opt_sort_seq,
        |keywords,
        |mchnt_desc,
        |rec_crt_ts,
        |rec_upd_ts,
        |encr_loc_inf,
        |comment_num,
        |favor_num,
        |share_num,
        |park_inf,
        |buss_hour,
        |traffic_inf,
        |famous_service,
        |comment_value,
        |content_id,
        |trim(mchnt_st) as mchnt_st,
        |mchnt_first_para,
        |mchnt_second_para,
        |mchnt_longitude,
        |mchnt_latitude,
        |mchnt_longitude_web,
        |mchnt_latitude_web,
        |cup_branch_ins_id_cd,
        |branch_nm,
        |brand_id,
        |trim(buss_bmp) as buss_bmp,
        |trim(term_diff_store_tp_in) as term_diff_store_tp_in,
        |rec_id,
        |amap_longitude,
        |amap_latitude,
        |
        |case
        |when mchnt_city_cd='210200' then '大连'
        |when mchnt_city_cd='330200' then '宁波'
        |when mchnt_city_cd='350200' then '厦门'
        |when mchnt_city_cd='370200' then '青岛'
        |when mchnt_city_cd='440300' then '深圳'
        |when mchnt_prov like '11%' then '北京'
        |when mchnt_prov like '12%' then '天津'
        |when mchnt_prov like '13%' then '河北'
        |when mchnt_prov like '14%' then '山西'
        |when mchnt_prov like '15%' then '内蒙古'
        |when mchnt_prov like '21%' then '辽宁'
        |when mchnt_prov like '22%' then '吉林'
        |when mchnt_prov like '23%' then '黑龙江'
        |when mchnt_prov like '31%' then '上海'
        |when mchnt_prov like '32%' then '江苏'
        |when mchnt_prov like '33%' then '浙江'
        |when mchnt_prov like '34%' then '安徽'
        |when mchnt_prov like '35%' then '福建'
        |when mchnt_prov like '36%' then '江西'
        |when mchnt_prov like '37%' then '山东'
        |when mchnt_prov like '41%' then '河南'
        |when mchnt_prov like '42%' then '湖北'
        |when mchnt_prov like '43%' then '湖南'
        |when mchnt_prov like '44%' then '广东'
        |when mchnt_prov like '45%' then '广西'
        |when mchnt_prov like '46%' then '海南'
        |when mchnt_prov like '50%' then '重庆'
        |when mchnt_prov like '51%' then '四川'
        |when mchnt_prov like '52%' then '贵州'
        |when mchnt_prov like '53%' then '云南'
        |when mchnt_prov like '54%' then '西藏'
        |when mchnt_prov like '61%' then '陕西'
        |when mchnt_prov like '62%' then '甘肃'
        |when mchnt_prov like '63%' then '青海'
        |when mchnt_prov like '64%' then '宁夏'
        |when mchnt_prov like '65%' then '新疆'
        |else '其他' end
        |as prov_division_cd
        |from
        |db2_tbl_chmgm_preferential_mchnt_inf
      """.stripMargin)
    println("JOB_HV_9------>results:"+results.count())

    if(!Option(results).isEmpty){
      results.registerTempTable("spark_tbl_chmgm_preferential_mchnt_inf")
      sqlContext.sql("use upw_hive")
      sqlContext.sql("truncate table  hive_preferential_mchnt_inf")
      sqlContext.sql("insert into table hive_preferential_mchnt_inf select * from spark_tbl_chmgm_preferential_mchnt_inf")
    }else{
      println("加载的表tbl_chmgm_preferential_mchnt_inf中无数据！")
    }

  }


  /**
    * JOB_HV_10/08-23
    * hive_access_bas_inf->tbl_chmgm_access_bas_inf
    * @param sqlContext
    * @return
    */
  def JOB_HV_10(implicit sqlContext: HiveContext) = {
    println("###JOB_HV_10(hive_access_bas_inf->tbl_chmgm_access_bas_inf)")
    val df = sqlContext.jdbc_mgmdb_DF("CH_MGMDB.TBL_CHMGM_ACCESS_BAS_INF")
    df.registerTempTable("db2_tbl_chmgm_access_bas_inf")
    val results = sqlContext.sql(
    """
        |select
        |seq_id,
        |trim(ch_ins_tp) as ch_ins_tp,
        |ch_ins_id_cd,
        |ch_access_nm,
        |trim(region_cd) as region_cd,
        |cup_branch_ins_id_cd,
        |cup_branch_nm,
        |trim(access_ins_st) as access_ins_st,
        |allot_md_id,
        |trim(extend_ins_id_cd) as extend_ins_id_cd,
        |extend_ins_nm,
        |trim(ins_acct_id) as ins_acct_id,
        |trim(req_dt) as req_dt,
        |trim(enable_dt) as enable_dt,
        |trim(entry_usr_id) as entry_usr_id,
        |entry_ts,
        |trim(upd_usr_id) as upd_usr_id,
        |upd_ts,
        |trim(oper_action) as oper_action,
        |trim(aud_usr_id) as aud_usr_id,
        |aud_idea,
        |aud_ts,
        |trim(aud_st) as aud_st,
        |ver_no,
        |trim(receipt_tp) as receipt_tp,
        |event_id,
        |trim(sync_st) as sync_st,
        |trim(point_enable_in) as point_enable_in,
        |trim(access_prov_cd) as access_prov_cd,
        |trim(access_city_cd) as access_city_cd,
        |trim(access_district_cd) as access_district_cd,
        |trim(oper_in) as oper_in,
        |trim(exch_in) as exch_in,
        |trim(exch_out) as exch_out,
        |trim(acpt_notify_in) as acpt_notify_in,
        |trim(receipt_bank) as receipt_bank,
        |trim(receipt_bank_acct) as receipt_bank_acct,
        |trim(receipt_bank_acct_nm) as receipt_bank_acct_nm,
        |trim(temp_use_bmp) as temp_use_bmp,
        |trim(term_diff_store_tp_in) as term_diff_store_tp_in,
        |trim(receipt_flag) as receipt_flag,
        |trim(term_alter_in) as term_alter_in,
        |trim(urgent_sync_in) as urgent_sync_in,
        |acpt_notify_url,
        |trim(input_data_tp) as input_data_tp,
        |trim(entry_ins_id_cd) as entry_ins_id_cd,
        |entry_ins_cn_nm,
        |trim(entry_cup_branch_ins_id_cd) as entry_cup_branch_ins_id_cd,
        |entry_cup_branch_nm,
        |trim(term_mt_ins_id_cd) as term_mt_ins_id_cd,
        |term_mt_ins_cn_nm,
        |trim(fwd_ins_id_cd) as fwd_ins_id_cd,
        |trim(pri_mchnt_cd) as pri_mchnt_cd,
        |
        |case when trim(cup_branch_ins_id_cd)='00011000' then '北京'
        |when trim(cup_branch_ins_id_cd)='00011100' then '天津'
        |when trim(cup_branch_ins_id_cd)='00011200' then '河北'
        |when trim(cup_branch_ins_id_cd)='00011600' then '山西'
        |when trim(cup_branch_ins_id_cd)='00011900' then '内蒙古'
        |when trim(cup_branch_ins_id_cd)='00012210' then '辽宁'
        |when trim(cup_branch_ins_id_cd)='00012220' then '大连'
        |when trim(cup_branch_ins_id_cd)='00012400' then '吉林'
        |when trim(cup_branch_ins_id_cd)='00012600' then '黑龙江'
        |when trim(cup_branch_ins_id_cd)='00012900' then '上海'
        |when trim(cup_branch_ins_id_cd)='00013000' then '江苏'
        |when trim(cup_branch_ins_id_cd)='00013310' then '浙江'
        |when trim(cup_branch_ins_id_cd)='00013320' then '宁波'
        |when trim(cup_branch_ins_id_cd)='00013600' then '安徽'
        |when trim(cup_branch_ins_id_cd)='00013900' then '福建'
        |when trim(cup_branch_ins_id_cd)='00013930' then '厦门'
        |when trim(cup_branch_ins_id_cd)='00014200' then '江西'
        |when trim(cup_branch_ins_id_cd)='00014500' then '山东'
        |when trim(cup_branch_ins_id_cd)='00014520' then '青岛'
        |when trim(cup_branch_ins_id_cd)='00014900' then '河南'
        |when trim(cup_branch_ins_id_cd)='00015210' then '湖北'
        |when trim(cup_branch_ins_id_cd)='00015500' then '湖南'
        |when trim(cup_branch_ins_id_cd)='00015800' then '广东'
        |when trim(cup_branch_ins_id_cd)='00015840' then '深圳'
        |when trim(cup_branch_ins_id_cd)='00016100' then '广西'
        |when trim(cup_branch_ins_id_cd)='00016400' then '海南'
        |when trim(cup_branch_ins_id_cd)='00016500' then '四川'
        |when trim(cup_branch_ins_id_cd)='00016530' then '重庆'
        |when trim(cup_branch_ins_id_cd)='00017000' then '贵州'
        |when trim(cup_branch_ins_id_cd)='00017310' then '云南'
        |when trim(cup_branch_ins_id_cd)='00017700' then '西藏'
        |when trim(cup_branch_ins_id_cd)='00017900' then '陕西'
        |when trim(cup_branch_ins_id_cd)='00018200' then '甘肃'
        |when trim(cup_branch_ins_id_cd)='00018500' then '青海'
        |when trim(cup_branch_ins_id_cd)='00018700' then '宁夏'
        |when trim(cup_branch_ins_id_cd)='00018800' then '新疆'
        |else '总公司' end
        |as cup_branch_ins_id_nm
        |from
        |db2_tbl_chmgm_access_bas_inf
      """.stripMargin)
    println("job10--------->原表：results :"+results.count())

    if(!Option(results).isEmpty){
      results.registerTempTable("spark_tbl_chmgm_access_bas_inf")
      sqlContext.sql("use upw_hive")
      sqlContext.sql("truncate table  hive_access_bas_inf")
      sqlContext.sql("insert into table hive_access_bas_inf select * from spark_tbl_chmgm_access_bas_inf")

    }else{
      println("加载的表tbl_chmgm_access_bas_inf中无数据！")
    }
  }


  /**
    * JOB_HV_11/08-23
    * hive_ticket_bill_bas_inf->tbl_chacc_ticket_bill_bas_inf
    * @param sqlContext
    * @return
    */
  def JOB_HV_11(implicit sqlContext: HiveContext) = {
    println("###JOB_HV_11(hive_ticket_bill_bas_inf->tbl_chacc_ticket_bill_bas_inf)")
    val df = sqlContext.jdbc_accdb_DF("CH_ACCDB.TBL_CHACC_TICKET_BILL_BAS_INF")

    df.registerTempTable("db2_tbl_chacc_ticket_bill_bas_inf")
    val results = sqlContext.sql(
      """
        |select
        |trim(bill_id) as bill_id,
        |bill_nm,
        |bill_desc,
        |trim(bill_tp) as bill_tp,
        |trim(mchnt_cd) as mchnt_cd,
        |mchnt_nm,
        |trim(affl_id) as affl_id,
        |affl_nm,
        |bill_pic_path,
        |trim(acpt_in) as acpt_in,
        |trim(enable_in) as enable_in,
        |dwn_total_num,
        |dwn_num,
        |case
        |	when
        |		substr(valid_begin_dt,1,4) between '0001' and '9999' and substr(valid_begin_dt,5,2) between '01' and '12' and
        |		substr(valid_begin_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(valid_begin_dt,1,4),substr(valid_begin_dt,5,2),substr(valid_begin_dt,7,2))),9,2)
        |	then concat_ws('-',substr(valid_begin_dt,1,4),substr(valid_begin_dt,5,2),substr(valid_begin_dt,7,2))
        |	else null
        |end as valid_begin_dt,
        |
        |case
        |	when
        |		substr(valid_end_dt,1,4) between '0001' and '9999' and substr(valid_end_dt,5,2) between '01' and '12' and
        |		substr(valid_end_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(valid_end_dt,1,4),substr(valid_end_dt,5,2),substr(valid_end_dt,7,2))),9,2)
        |	then concat_ws('-',substr(valid_end_dt,1,4),substr(valid_end_dt,5,2),substr(valid_end_dt,7,2))
        |	else null
        |end as valid_end_dt,
        |
        |
        |dwn_num_limit,
        |disc_rate,
        |money_at,
        |low_trans_at_limit,
        |high_trans_at_limit,
        |trim(sale_in) as sale_in,
        |bill_price,
        |trim(bill_st) as bill_st,
        |trim(oper_action) as oper_action,
        |trim(aud_st) as aud_st,
        |trim(aud_usr_id) as aud_usr_id,
        |aud_ts,
        |aud_idea,
        |trim(rec_upd_usr_id) as rec_upd_usr_id,
        |rec_upd_ts,
        |trim(rec_crt_usr_id) as rec_crt_usr_id,
        |rec_crt_ts,
        |ver_no,
        |trim(bill_related_card_bin) as bill_related_card_bin,
        |event_id,
        |trim(oper_in) as oper_in,
        |trim(sync_st)as sync_st,
        |trim(blkbill_in) as blkbill_in,
        |trim(chara_grp_cd) as chara_grp_cd,
        |chara_grp_nm,
        |trim(obtain_chnl) as obtain_chnl,
        |trim(cfg_tp) as cfg_tp,
        |delay_use_days,
        |trim(bill_rule_bmp) as bill_rule_bmp,
        |cup_branch_ins_id_cd,
        |trim(cycle_deduct_in) as cycle_deduct_in,
        |discount_at_max,
        |trim(input_data_tp) as input_data_tp,
        |trim(temp_use_in) as temp_use_in,
        |aud_id,
        |trim(entry_ins_id_cd) as entry_ins_id_cd,
        |entry_ins_cn_nm,
        |trim(entry_cup_branch_ins_id_cd) as entry_cup_branch_ins_id_cd,
        |entry_cup_branch_nm,
        |trim(exclusive_in) as exclusive_in,
        |trim(data_src) as data_src,
        |bill_original_price,
        |trim(price_trend_st) as price_trend_st,
        |trim(bill_sub_tp) as bill_sub_tp,
        |pay_timeout,
        |trim(auto_refund_st) as auto_refund_st,
        |trim(anytime_refund_st) as anytime_refund_st,
        |trim(imprest_tp_st) as imprest_tp_st,
        |trim(rand_tp) as rand_tp,
        |expt_val,
        |std_deviation,
        |rand_at_min,
        |rand_at_max,
        |trim(disc_max_in) as disc_max_in,
        |disc_max,
        |trim(rand_period_tp) as rand_period_tp,
        |trim(rand_period) as rand_period,
        |
        |case
        |when trim(cup_branch_ins_id_cd)='00011000' then '北京'
        |when trim(cup_branch_ins_id_cd)='00011100' then '天津'
        |when trim(cup_branch_ins_id_cd)='00011200' then '河北'
        |when trim(cup_branch_ins_id_cd)='00011600' then '山西'
        |when trim(cup_branch_ins_id_cd)='00011900' then '内蒙古'
        |when trim(cup_branch_ins_id_cd)='00012210' then '辽宁'
        |when trim(cup_branch_ins_id_cd)='00012220' then '大连'
        |when trim(cup_branch_ins_id_cd)='00012400' then '吉林'
        |when trim(cup_branch_ins_id_cd)='00012600' then '黑龙江'
        |when trim(cup_branch_ins_id_cd)='00012900' then '上海'
        |when trim(cup_branch_ins_id_cd)='00013000' then '江苏'
        |when trim(cup_branch_ins_id_cd)='00013310' then '浙江'
        |when trim(cup_branch_ins_id_cd)='00013320' then '宁波'
        |when trim(cup_branch_ins_id_cd)='00013600' then '安徽'
        |when trim(cup_branch_ins_id_cd)='00013900' then '福建'
        |when trim(cup_branch_ins_id_cd)='00013930' then '厦门'
        |when trim(cup_branch_ins_id_cd)='00014200' then '江西'
        |when trim(cup_branch_ins_id_cd)='00014500' then '山东'
        |when trim(cup_branch_ins_id_cd)='00014520' then '青岛'
        |when trim(cup_branch_ins_id_cd)='00014900' then '河南'
        |when trim(cup_branch_ins_id_cd)='00015210' then '湖北'
        |when trim(cup_branch_ins_id_cd)='00015500' then '湖南'
        |when trim(cup_branch_ins_id_cd)='00015800' then '广东'
        |when trim(cup_branch_ins_id_cd)='00015840' then '深圳'
        |when trim(cup_branch_ins_id_cd)='00016100' then '广西'
        |when trim(cup_branch_ins_id_cd)='00016400' then '海南'
        |when trim(cup_branch_ins_id_cd)='00016500' then '四川'
        |when trim(cup_branch_ins_id_cd)='00016530' then '重庆'
        |when trim(cup_branch_ins_id_cd)='00017000' then '贵州'
        |when trim(cup_branch_ins_id_cd)='00017310' then '云南'
        |when trim(cup_branch_ins_id_cd)='00017700' then '西藏'
        |when trim(cup_branch_ins_id_cd)='00017900' then '陕西'
        |when trim(cup_branch_ins_id_cd)='00018200' then '甘肃'
        |when trim(cup_branch_ins_id_cd)='00018500' then '青海'
        |when trim(cup_branch_ins_id_cd)='00018700' then '宁夏'
        |when trim(cup_branch_ins_id_cd)='00018800' then '新疆'
        |else '总公司' end
        |as  CUP_BRANCH_INS_ID_NM
        |
        |from
        |db2_tbl_chacc_ticket_bill_bas_inf
      """.stripMargin)
    println("job11-------results:"+results.count())
    if(!Option(results).isEmpty){
      results.registerTempTable("spark_db2_tbl_chacc_ticket_bill_bas_inf")
      sqlContext.sql("use upw_hive")
      sqlContext.sql("truncate table  hive_ticket_bill_bas_inf")
      sqlContext.sql("insert into table hive_ticket_bill_bas_inf select * from spark_db2_tbl_chacc_ticket_bill_bas_inf")

    }else{
      println("加载的表TBL_CHMGM_TICKET_BILL_BAS_INF中无数据！")
    }
  }

  /**
    * JOB_HV_12/08-23
    * hive_chara_grp_def_bat->tbl_chmgm_chara_grp_def_bat
    * @param sqlContext
    * @return
    */
  def JOB_HV_12(implicit sqlContext: HiveContext) = {
    println("###JOB_HV_12(hive_chara_grp_def_bat->tbl_chmgm_chara_grp_def_bat)")
    val df = sqlContext.jdbc_mgmdb_DF("CH_MGMDB.TBL_CHMGM_CHARA_GRP_DEF_BAT")
    df.registerTempTable("db2_tbl_chmgm_chara_grp_def_bat")
    val results = sqlContext.sql(
      """
        |select
        |rec_id,
        |trim(chara_grp_cd) as chara_grp_cd,
        |chara_grp_nm,
        |trim(chara_data)as chara_data,
        |trim(chara_data_tp) as chara_data_tp,
        |trim(aud_usr_id) as aud_usr_id,
        |aud_idea,
        |aud_ts,
        |trim(aud_in) as aud_in,
        |trim(oper_action) as oper_action,
        |rec_crt_ts,
        |rec_upd_ts,
        |trim(rec_crt_usr_id) as rec_crt_usr_id,
        |trim(rec_upd_usr_id)as rec_upd_usr_id,
        |ver_no,
        |event_id,
        |trim(oper_in) as oper_in,
        |trim(sync_st) as sync_st,
        |cup_branch_ins_id_cd,
        |trim(input_data_tp) as input_data_tp,
        |brand_id,
        |trim(entry_ins_id_cd) as entry_ins_id_cd,
        |entry_ins_cn_nm,
        |trim(entry_cup_branch_ins_id_cd) as entry_cup_branch_ins_id_cd,
        |order_in_grp
        |
        |from
        |db2_tbl_chmgm_chara_grp_def_bat
      """.stripMargin)
    println("JOB_HV_12-------results:"+results.count())

    if(!Option(results).isEmpty){
      results.registerTempTable("spark_tbl_chmgm_chara_grp_def_bat")
      sqlContext.sql("use upw_hive")
      sqlContext.sql("truncate table  hive_chara_grp_def_bat")
      sqlContext.sql("insert into table hive_chara_grp_def_bat select * from spark_tbl_chmgm_chara_grp_def_bat")

    }else{
      println("加载的表tbl_chmgm_chara_grp_def_bat中无数据！")
    }

  }


  /**
    * hive-job-13/08-22
    * hive_card_bin->tbl_chmgm_card_bin
    * @param sqlContext
    * @return
    */
  def JOB_HV_13(implicit sqlContext: HiveContext) = {
    println("###JOB_HV_13(hive_card_bin->tbl_chmgm_card_bin)")
    val df = sqlContext.jdbc_mgmdb_DF("CH_MGMDB.TBL_CHMGM_CARD_BIN")

    df.registerTempTable("db2_tbl_chmgm_card_bin")
    val results = sqlContext.sql(
      """
        |select
        |trim(card_bin) as card_bin,
        |trim(card_cn_nm) as card_cn_nm,
        |trim(card_en_nm) as card_en_nm,
        |trim(iss_ins_id_cd) as iss_ins_id_cd,
        |iss_ins_cn_nm,
        |iss_ins_en_nm,
        |card_bin_len,
        |trim(card_attr) as card_attr,
        |trim(card_cata) as card_cata,
        |trim(card_class) as card_class,
        |trim(card_brand) as card_brand,
        |trim(card_prod) as card_prod,
        |trim(card_lvl) as card_lvl,
        |trim(card_media) as card_media,
        |trim(ic_app_tp) as ic_app_tp,
        |trim(pan_len) as pan_len,
        |trim(pan_sample) as pan_sample,
        |trim(pay_curr_cd1) as pay_curr_cd1,
        |trim(pay_curr_cd2) as pay_curr_cd2,
        |trim(pay_curr_cd3) as pay_curr_cd3,
        |trim(card_bin_priv_bmp) as card_bin_priv_bmp,
        |
        |case
        |		when
        |			substr(publish_dt,1,4) between '0001' and '9999' and substr(publish_dt,5,2) between '01' and '12' and
        |			substr(publish_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(publish_dt,1,4),substr(publish_dt,5,2),substr(publish_dt,7,2))),9,2)
        |		then concat_ws('-',substr(publish_dt,1,4),substr(publish_dt,5,2),substr(publish_dt,7,2))
        |		else null
        |	end as publish_dt,
        |
        |trim(card_vfy_algo) as card_vfy_algo,
        |trim(frn_trans_in) as frn_trans_in,
        |trim(oper_in) as oper_in,
        |event_id,
        |rec_id,
        |trim(rec_upd_usr_id) as rec_upd_usr_id,
        |rec_upd_ts,
        |rec_crt_ts
        |from
        |db2_tbl_chmgm_card_bin
      """.stripMargin)
    println("JOB_HV_13---------results: "+results.count())

    if(!Option(results).isEmpty){
      results.registerTempTable("spark_db2_tbl_chmgm_card_bin")
      sqlContext.sql("use upw_hive")
      sqlContext.sql("truncate table  hive_card_bin")
      sqlContext.sql("insert into table hive_card_bin select * from spark_db2_tbl_chmgm_card_bin")

    }else{
      println("加载的表TBL_CHMGM_CARD_BIN中无数据！")
    }
  }


  /**
    * hive-job-14/08-22
    * hive_inf_source_dtl->tbl_inf_source_dtl
    * @param sqlContext
    * @return
    */
  def JOB_HV_14(implicit sqlContext: HiveContext) = {
    println("###JOB_HV_14(hive_inf_source_dtl->tbl_inf_source_dtl)")
    val df = sqlContext.jdbc_accdb_DF("CH_ACCDB.TBL_INF_SOURCE_DTL")
    df.registerTempTable("db2_tbl_inf_source_dtl")
    val results = sqlContext.sql(
      """
        |select
        |trim(access_id) as access_id ,
        |access_nm
        |from
        |db2_tbl_inf_source_dtl
      """.stripMargin)
    println("JOB_HV_14-------------results :"+results.count())

    if(!Option(results).isEmpty){
      results.registerTempTable("spark_db2_tbl_inf_source_dtl")
      sqlContext.sql("use upw_hive")
      sqlContext.sql("truncate table  hive_inf_source_dtl")
      sqlContext.sql("insert into table hive_inf_source_dtl select * from spark_db2_tbl_inf_source_dtl")
    }else{
      println("加载的表TBL_INF_SOURCE_DTL中无数据！")
    }

  }


  /**
    * JOB_HV_16/08-23
    * hive_mchnt_inf_wallet->tbl_chmgm_mchnt_inf/TBL_CHMGM_STORE_TERM_RELATION/TBL_CHMGM_ACCESS_BAS_INF
    * @param sqlContext
    * @return
    */
  def JOB_HV_16(implicit sqlContext: HiveContext) = {
    println("###JOB_HV_16(hive_mchnt_inf_wallet->tbl_chmgm_mchnt_inf/TBL_CHMGM_STORE_TERM_RELATION/TBL_CHMGM_ACCESS_BAS_INF)")
    val df = sqlContext.jdbc_mgmdb_DF("CH_MGMDB.TBL_CHMGM_MCHNT_INF")
    df.registerTempTable("db2_tbl_chmgm_mchnt_inf")

    val df1 = sqlContext.jdbc_mgmdb_DF("CH_MGMDB.TBL_CHMGM_STORE_TERM_RELATION")
    df1.registerTempTable("db2_tbl_chmgm_store_term_relation")

    val df2 = sqlContext.jdbc_mgmdb_DF("CH_MGMDB.TBL_CHMGM_ACCESS_BAS_INF")
    df2.registerTempTable("db2_tbl_chmgm_access_bas_inf")

    val results = sqlContext.sql(
      """
        |select
        |trim(a.mchnt_cd) as mchnt_cd,
        |mchnt_cn_abbr,
        |mchnt_cn_nm,
        |mchnt_en_abbr,
        |mchnt_en_nm,
        |trim(cntry_cd) as cntry_cd,
        |trim(mchnt_st) as mchnt_st,
        |trim(mchnt_tp) as mchnt_tp,
        |trim(region_cd) as region_cd,
        |cup_branch_ins_id_cd,
        |
        |case
        |when trim(cup_branch_ins_id_cd)='00011000' then '北京'
        |when trim(cup_branch_ins_id_cd)='00011100' then '天津'
        |when trim(cup_branch_ins_id_cd)='00011200' then '河北'
        |when trim(cup_branch_ins_id_cd)='00011600' then '山西'
        |when trim(cup_branch_ins_id_cd)='00011900' then '内蒙古'
        |when trim(cup_branch_ins_id_cd)='00012210' then '辽宁'
        |when trim(cup_branch_ins_id_cd)='00012220' then '大连'
        |when trim(cup_branch_ins_id_cd)='00012400' then '吉林'
        |when trim(cup_branch_ins_id_cd)='00012600' then '黑龙江'
        |when trim(cup_branch_ins_id_cd)='00012900' then '上海'
        |when trim(cup_branch_ins_id_cd)='00013000' then '江苏'
        |when trim(cup_branch_ins_id_cd)='00013310' then '浙江'
        |when trim(cup_branch_ins_id_cd)='00013320' then '宁波'
        |when trim(cup_branch_ins_id_cd)='00013600' then '安徽'
        |when trim(cup_branch_ins_id_cd)='00013900' then '福建'
        |when trim(cup_branch_ins_id_cd)='00013930' then '厦门'
        |when trim(cup_branch_ins_id_cd)='00014200' then '江西'
        |when trim(cup_branch_ins_id_cd)='00014500' then '山东'
        |when trim(cup_branch_ins_id_cd)='00014520' then '青岛'
        |when trim(cup_branch_ins_id_cd)='00014900' then '河南'
        |when trim(cup_branch_ins_id_cd)='00015210' then '湖北'
        |when trim(cup_branch_ins_id_cd)='00015500' then '湖南'
        |when trim(cup_branch_ins_id_cd)='00015800' then '广东'
        |when trim(cup_branch_ins_id_cd)='00015840' then '深圳'
        |when trim(cup_branch_ins_id_cd)='00016100' then '广西'
        |when trim(cup_branch_ins_id_cd)='00016400' then '海南'
        |when trim(cup_branch_ins_id_cd)='00016500' then '四川'
        |when trim(cup_branch_ins_id_cd)='00016530' then '重庆'
        |when trim(cup_branch_ins_id_cd)='00017000' then '贵州'
        |when trim(cup_branch_ins_id_cd)='00017310' then '云南'
        |when trim(cup_branch_ins_id_cd)='00017700' then '西藏'
        |when trim(cup_branch_ins_id_cd)='00017900' then '陕西'
        |when trim(cup_branch_ins_id_cd)='00018200' then '甘肃'
        |when trim(cup_branch_ins_id_cd)='00018500' then '青海'
        |when trim(cup_branch_ins_id_cd)='00018700' then '宁夏'
        |when trim(cup_branch_ins_id_cd)='00018800' then '新疆'
        |else '总公司' end
        |as cup_branch_ins_id_nm,
        |
        |trim(frn_acq_ins_id_cd) as frn_acq_ins_id_cd,
        |trim(acq_access_ins_id_cd) as acq_access_ins_id_cd,
        |trim(acpt_ins_id_cd) as acpt_ins_id_cd,
        |trim(mchnt_grp) as mchnt_grp,
        |trim(gb_region_cd) as gb_region_cd,
        |
        |case
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
        |else '其他' end  as gb_region_nm,
        |
        |trim(acq_ins_id_cd) as acq_ins_id_cd,
        |trim(oper_in) as oper_in,
        |buss_addr,
        |trim(point_conv_in) as point_conv_in,
        |event_id,
        |rec_id,
        |trim(rec_upd_usr_id) as rec_upd_usr_id,
        |rec_upd_ts,
        |rec_crt_ts,
        |trim(artif_certif_tp) as artif_certif_tp,
        |trim(artif_certif_id) as artif_certif_id,
        |phone,
        |trim(conn_md) as conn_md,
        |trim(settle_ins_tp) as settle_ins_tp,
        |
        |concat(open_buss_bmp,'000000000000000000000000000000000000000000000000')
        |
        |from (select * from db2_tbl_chmgm_mchnt_inf)  a
        |left join
        |(select
        |nvl(mchnt_cd,ch_ins_id_cd) as mchnt_cd,
        |   ( case
        |     when  mchnt_cd is not null and ch_ins_id_cd is not null then '11'
        |     when  mchnt_cd is null and ch_ins_id_cd is not null then '01'
        |      when mchnt_cd is  not null and ch_ins_id_cd is null then '10' else '00' end)
        |     as open_buss_bmp
        |from
        |(select mchnt_cd from db2_tbl_chmgm_store_term_relation)  b
        |left join
        |(select  ch_ins_id_cd from db2_tbl_chmgm_access_bas_inf where ch_ins_tp='m') c
        |on b.mchnt_cd=c.ch_ins_id_cd ) d
        |on a.mchnt_cd=d.mchnt_cd
        |
      """.stripMargin)

    println("JOB_HV_16-----results:"+results.count())

    if(!Option(results).isEmpty){
      results.registerTempTable("spark_tbl_chmgm_mchnt_inf")
      sqlContext.sql("use upw_hive")
      sqlContext.sql("truncate table  hive_mchnt_inf_wallet")
      sqlContext.sql("insert into table hive_mchnt_inf_wallet select * from spark_tbl_chmgm_mchnt_inf")

    }else{
      println("指定表hive_mchnt_inf_wallet无数据插入！")
    }

  }


  //=========Created by xuetaiping====================================================================



  //=========Created by yangxue=======================================================================


}


