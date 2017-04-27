package com.unionpay.etl


import com.unionpay.conf.ConfigurationManager
import com.unionpay.constant.Constants
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 初始化建表类
  */
object Create_Hive_Tables {
  //指定HIVE数据库名
  private lazy val hive_dbname = ConfigurationManager.getProperty(Constants.HIVE_DBNAME)

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Create_Hive_Tables")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new HiveContext(sc)

    println("=======Create all tables on the hive=======")

    //    仅初始化使用，默认关闭

    hive_ct //参数表
    hive_life //参数表
    hive_city_card //参数表
    hive_province_card //参数表
    hive_acc_trans
    hive_achis_trans
    hive_active_card_acq_branch_mon
    hive_card_bind_inf
    hive_discount_bas_inf
    hive_download_trans
    hive_inf_source_class
    hive_ins_inf
    hive_mchnt_para
    hive_passive_code_pay_trans
    hive_cdhd_pri_acct_inf
    hive_pri_acct_inf
    hive_switch_point_trans
    hive_ucbiz_cdhd_bas_inf
    hive_access_bas_inf
    hive_active_code_pay_trans
    hive_branch_acpt_ins_inf
    hive_brand_inf
    hive_card_bin
    hive_cdhd_cashier_maktg_reward_dtl
    hive_cashier_point_acct_oper_dtl
    hive_chara_grp_def_bat
    hive_cups_trans
    hive_filter_app_det
    hive_filter_rule_det
    hive_inf_source_dtl
    hive_life_trans
    hive_mchnt_inf_wallet
    hive_mchnt_tp_grp
    hive_org_tdapp_activitynew
    hive_org_tdapp_device
    hive_org_tdapp_devicenew
    hive_org_tdapp_eventnew
    hive_org_tdapp_exceptionnew
    hive_org_tdapp_keyvalue
    hive_org_tdapp_tlaunchnew
    hive_org_tdapp_exception
    hive_org_tdapp_newuser
    hive_org_tdapp_tappevent
    hive_preferential_mchnt_inf
    hive_prize_bas
    hive_signer_log
    hive_ticket_bill_bas_inf
    hive_undefine_store_inf
    hive_undefine_store_inf_temp
    hive_user_td_d
    hive_bill_order_trans
    hive_bill_sub_order_trans
    hive_mchnt_tp
    hive_offline_point_trans
    hive_online_point_trans
    hive_prize_activity_bas_inf
    hive_prize_discount_result
    hive_prize_lvl_add_rule
    hive_prize_lvl
    hive_store_term_relation
    hive_term_inf
    hive_ach_order_inf
    hive_bill_order_aux_inf
    hive_bill_sub_order_detail_inf
    hive_ticket_bill_acct_adj_task
    hive_search_trans
    hive_buss_dist
    hive_cdhd_bill_acct_inf
    hive_cashier_bas_inf
    hive_access_static_inf
    hive_region_cd
    hive_aconl_ins_bas
    hive_org_tdapp_tactivity
    hive_org_tdapp_tlaunch
    hive_org_tdapp_terminate
    hive_trans_dtl
    hive_trans_log
    hive_swt_log
    hive_cdhd_trans_year
    hive_life_order_inf
    hive_rtdtrs_dtl_ach_bill
    hive_point_trans
    hive_mksvc_order
    hive_wlonl_transfer_order
    hive_wlonl_uplan_coupon
    hive_wlonl_acc_notes
    hive_ubp_order
    hive_mnsvc_business_instal_info
    hive_mtdtrs_dtl_ach_bat_file
    hive_mtdtrs_dtl_ach_bat_file_dtl
    hive_rtdtrs_dtl_achis_bill

    hive_rtdtrs_dtl_achis_note
    hive_rtdtrs_dtl_achis_order
    hive_rtdtrs_dtl_achis_order_error
    hive_rtdtrs_dtl_sor_cmsp

    hive_rtapam_dim_ins
    hive_stmtrs_scl_usr_geo_loc_quota
    hive_rtapam_dim_intnl_domin


    println("=======Create all tables on the hive successfully=======")

    sc.stop()

  }

  def hive_acc_trans(implicit sqlContext: HiveContext) = {
    println("=======Create hive_acc_trans=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_acc_trans")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_acc_trans(
         |seq_id                            string    ,
         |cdhd_usr_id                       string    ,
         |card_no                           string    ,
         |trans_tfr_tm                      string    ,
         |sys_tra_no                        string    ,
         |acpt_ins_id_cd                    string    ,
         |fwd_ins_id_cd                     string    ,
         |rcv_ins_id_cd                     string    ,
         |oper_module                       string    ,
         |trans_dt                          timestamp ,
         |trans_tm                          timestamp ,
         |buss_tp                           string    ,
         |um_trans_id                       string    ,
         |swt_right_tp                      string    ,
         |bill_id                           string    ,
         |bill_nm                           string    ,
         |chara_acct_tp                     string    ,
         |trans_at                          int       ,
         |point_at                          bigint    ,
         |mchnt_tp                          string    ,
         |resp_cd                           string    ,
         |card_accptr_term_id               string    ,
         |card_accptr_cd                    string    ,
         |first_trans_proc_start_ts         timestamp ,
         |second_trans_proc_start_ts        timestamp ,
         |third_trans_proc_start_ts         timestamp ,
         |trans_proc_end_ts                 timestamp ,
         |sys_det_cd                        string    ,
         |sys_err_cd                        string    ,
         |rec_upd_ts                        timestamp ,
         |chara_acct_nm                     string    ,
         |void_trans_tfr_tm                 string    ,
         |void_sys_tra_no                   string    ,
         |void_acpt_ins_id_cd               string    ,
         |void_fwd_ins_id_cd                string    ,
         |orig_data_elemnt                  string    ,
         |rec_crt_ts                        timestamp ,
         |discount_at                       int       ,
         |bill_item_id                      string    ,
         |chnl_inf_index                    int       ,
         |bill_num                          bigint    ,
         |addn_discount_at                  int       ,
         |pos_entry_md_cd                   string    ,
         |udf_fld                           string    ,
         |card_accptr_nm_addr               string    ,
         |msg_tp                            string    ,
         |cdhd_fk                           string    ,
         |bill_tp                           string    ,
         |bill_bat_no                       string    ,
         |bill_inf                          string    ,
         |proc_cd                           string    ,
         |trans_curr_cd                     string    ,
         |settle_at                         int       ,
         |settle_curr_cd                    string    ,
         |card_accptr_local_tm              string    ,
         |card_accptr_local_dt              string    ,
         |expire_dt                         string    ,
         |msg_settle_dt                     timestamp      ,
         |pos_cond_cd                       string    ,
         |pos_pin_capture_cd                string    ,
         |retri_ref_no                      string    ,
         |auth_id_resp_cd                   string    ,
         |notify_st                         string    ,
         |addn_private_data                 string    ,
         |addn_at                           string    ,
         |acct_id_1                         string    ,
         |acct_id_2                         string    ,
         |resv_fld                          string    ,
         |cdhd_auth_inf                     string    ,
         |sys_settle_dt                     timestamp      ,
         |recncl_in                         string    ,
         |match_in                          string    ,
         |sec_ctrl_inf                      string    ,
         |card_seq                          string    ,
         |dtl_inq_data                      string    ,
         |pri_key1                          string    ,
         |fwd_chnl_head                     string    ,
         |chswt_plat_seq                    bigint    ,
         |internal_trans_tp                 string    ,
         |settle_trans_id                   string    ,
         |trans_tp                          string    ,
         |cups_settle_dt                    string    ,
         |pri_acct_no                       string    ,
         |card_bin                          string    ,
         |req_trans_at                      int       ,
         |resp_trans_at                     int       ,
         |trans_tot_at                      int       ,
         |iss_ins_id_cd                     string    ,
         |launch_trans_tm                   string    ,
         |launch_trans_dt                   string    ,
         |mchnt_cd                          string    ,
         |fwd_proc_in                       string    ,
         |rcv_proc_in                       string    ,
         |proj_tp                           string    ,
         |usr_id                            string    ,
         |conv_usr_id                       string    ,
         |trans_st                          string    ,
         |inq_dtl_req                       string    ,
         |inq_dtl_resp                      string    ,
         |iss_ins_resv                      string    ,
         |ic_flds                           string    ,
         |cups_def_fld                      string    ,
         |id_no                             string    ,
         |cups_resv                         string    ,
         |acpt_ins_resv                     string    ,
         |rout_ins_id_cd                    string    ,
         |sub_rout_ins_id_cd                string    ,
         |recv_access_resp_cd               string    ,
         |chswt_resp_cd                     string    ,
         |chswt_err_cd                      string    ,
         |resv_fld1                         string    ,
         |resv_fld2                         string    ,
         |to_ts                             timestamp ,
         |external_amt                      int       ,
         |card_pay_at                       int       ,
         |right_purchase_at                 int       ,
         |recv_second_resp_cd               string    ,
         |req_acpt_ins_resv                 string    ,
         |log_id                            string    ,
         |conv_acct_no                      string    ,
         |inner_pro_ind                     string    ,
         |acct_proc_in                      string    ,
         |order_id                          string
         |)
         |partitioned by(part_trans_dt string)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/incident/trans/hive_acc_trans'
         | """.stripMargin)

    println("=======Create hive_acc_trans successfully ! =======")

  }

  /*
  def hive_acc_trans(implicit sqlContext: HiveContext) = {
    println("=======Create hive_acc_trans=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_acc_trans(
         |seq_id                            string    ,
         |cdhd_usr_id                       string    ,
         |card_no                           string    ,
         |trans_tfr_tm                      string    ,
         |sys_tra_no                        string    ,
         |acpt_ins_id_cd                    string    ,
         |fwd_ins_id_cd                     string    ,
         |rcv_ins_id_cd                     string    ,
         |oper_module                       string    ,
         |trans_dt                          timestamp ,
         |trans_tm                          timestamp ,
         |buss_tp                           string    ,
         |um_trans_id                       string    ,
         |swt_right_tp                      string    ,
         |bill_id                           string    ,
         |bill_nm                           string    ,
         |chara_acct_tp                     string    ,
         |trans_at                          int       ,
         |point_at                          bigint    ,
         |mchnt_tp                          string    ,
         |resp_cd                           string    ,
         |card_accptr_term_id               string    ,
         |card_accptr_cd                    string    ,
         |frist_trans_proc_start_ts         timestamp ,
         |second_trans_proc_start_ts        timestamp ,
         |third_trans_proc_start_ts         timestamp ,
         |trans_proc_end_ts                 timestamp ,
         |sys_det_cd                        string    ,
         |sys_err_cd                        string    ,
         |rec_upd_ts                        timestamp ,
         |chara_acct_nm                     string    ,
         |void_trans_tfr_tm                 string    ,
         |void_sys_tra_no                   string    ,
         |void_acpt_ins_id_cd               string    ,
         |void_fwd_ins_id_cd                string    ,
         |orig_data_elemnt                  string    ,
         |rec_crt_ts                        timestamp ,
         |discount_at                       int       ,
         |bill_item_id                      string    ,
         |chnl_inf_index                    int       ,
         |bill_num                          bigint    ,
         |addn_discount_at                  int       ,
         |pos_entry_md_cd                   string    ,
         |udf_fld                           string    ,
         |card_accptr_nm_addr               string    ,
         |msg_tp                            string    ,
         |cdhd_fk                           string    ,
         |bill_tp                           string    ,
         |bill_bat_no                       string    ,
         |bill_inf                          string    ,
         |proc_cd                           string    ,
         |trans_curr_cd                     string    ,
         |settle_at                         int       ,
         |settle_curr_cd                    string    ,
         |card_accptr_local_tm              string    ,
         |card_accptr_local_dt              string    ,
         |expire_dt                         string    ,
         |msg_settle_dt                     timestamp      ,
         |pos_cond_cd                       string    ,
         |pos_pin_capture_cd                string    ,
         |retri_ref_no                      string    ,
         |auth_id_resp_cd                   string    ,
         |notify_st                         string    ,
         |addn_private_data                 string    ,
         |addn_at                           string    ,
         |acct_id_1                         string    ,
         |acct_id_2                         string    ,
         |resv_fld                          string    ,
         |cdhd_auth_inf                     string    ,
         |sys_settle_dt                     timestamp      ,
         |recncl_in                         string    ,
         |match_in                          string    ,
         |sec_ctrl_inf                      string    ,
         |card_seq                          string    ,
         |dtl_inq_data                      string    ,
         |pri_key1                          string    ,
         |fwd_chnl_head                     string    ,
         |chswt_plat_seq                    bigint    ,
         |internal_trans_tp                 string    ,
         |settle_trans_id                   string    ,
         |trans_tp                          string    ,
         |cups_settle_dt                    string    ,
         |pri_acct_no                       string    ,
         |card_bin                          string    ,
         |req_trans_at                      int       ,
         |resp_trans_at                     int       ,
         |trans_tot_at                      int       ,
         |iss_ins_id_cd                     string    ,
         |launch_trans_tm                   string    ,
         |launch_trans_dt                   string    ,
         |mchnt_cd                          string    ,
         |fwd_proc_in                       string    ,
         |rcv_proc_in                       string    ,
         |proj_tp                           string    ,
         |usr_id                            string    ,
         |conv_usr_id                       string    ,
         |trans_st                          string    ,
         |inq_dtl_req                       string    ,
         |inq_dtl_resp                      string    ,
         |iss_ins_resv                      string    ,
         |ic_flds                           string    ,
         |cups_def_fld                      string    ,
         |id_no                             string    ,
         |cups_resv                         string    ,
         |acpt_ins_resv                     string    ,
         |rout_ins_id_cd                    string    ,
         |sub_rout_ins_id_cd                string    ,
         |recv_access_resp_cd               string    ,
         |chswt_resp_cd                     string    ,
         |chswt_err_cd                      string    ,
         |resv_fld1                         string    ,
         |resv_fld2                         string    ,
         |to_ts                             timestamp ,
         |external_amt                      int       ,
         |card_pay_at                       int       ,
         |right_purchase_at                 int       ,
         |recv_second_resp_cd               string    ,
         |req_acpt_ins_resv                 string    ,
         |log_id                            string    ,
         |conv_acct_no                      string    ,
         |inner_pro_ind                     string    ,
         |acct_proc_in                      string    ,
         |order_id                          string    ,
         |settle_dt                        timestamp     ,
         |cups_pri_key                     string        ,
         |cups_log_cd                      string        ,
         |cups_settle_tp                   string        ,
         |cups_settle_cycle                string        ,
         |cups_block_id                    string        ,
         |cups_orig_key                    string        ,
         |cups_related_key                 string        ,
         |cups_trans_fwd_st                string        ,
         |cups_trans_rcv_st                string        ,
         |cups_sms_dms_conv_in             string        ,
         |cups_fee_in                      string        ,
         |cups_cross_dist_in               string        ,
         |cups_orig_acpt_sdms_in           string        ,
         |cups_tfr_in_in                   string        ,
         |cups_trans_md                    string        ,
         |cups_source_region_cd            string        ,
         |cups_dest_region_cd              string        ,
         |cups_cups_card_in                string        ,
         |cups_cups_sig_card_in            string        ,
         |cups_card_class                  string        ,
         |cups_card_attr                   string        ,
         |cups_sti_in                      string        ,
         |cups_trans_proc_in               string        ,
         |cups_acq_ins_id_cd               string        ,
         |cups_acq_ins_tp                  string        ,
         |cups_fwd_ins_tp                  string        ,
         |cups_rcv_ins_id_cd               string        ,
         |cups_rcv_ins_tp                  string        ,
         |cups_iss_ins_id_cd               string        ,
         |cups_iss_ins_tp                  string        ,
         |cups_related_ins_id_cd           string        ,
         |cups_related_ins_tp              string        ,
         |cups_acpt_ins_tp                 string        ,
         |cups_pri_acct_no                 string        ,
         |cups_pri_acct_no_conv            string        ,
         |cups_sys_tra_no_conv             string        ,
         |cups_sw_sys_tra_no               string        ,
         |cups_auth_dt                     string        ,
         |cups_auth_id_resp_cd             string        ,
         |cups_resp_cd1                    string        ,
         |cups_resp_cd2                    string        ,
         |cups_resp_cd3                    string        ,
         |cups_resp_cd4                    string        ,
         |cups_cu_trans_st                 string        ,
         |cups_sti_takeout_in              string        ,
         |cups_trans_id                    string        ,
         |cups_trans_tp                    string        ,
         |cups_trans_chnl                  string        ,
         |cups_card_media                  string        ,
         |cups_card_media_proc_md          string        ,
         |cups_card_brand                  string        ,
         |cups_expire_seg                  string        ,
         |cups_trans_id_conv               string        ,
         |cups_settle_mon                  string        ,
         |cups_settle_d                    string        ,
         |cups_orig_settle_dt              timestamp     ,
         |cups_settle_fwd_ins_id_cd        string        ,
         |cups_settle_rcv_ins_id_cd        string        ,
         |cups_trans_at                    int           ,
         |cups_orig_trans_at               int           ,
         |cups_trans_conv_rt               int           ,
         |cups_trans_curr_cd               string        ,
         |cups_cdhd_fee_at                 int           ,
         |cups_cdhd_fee_conv_rt            int           ,
         |cups_cdhd_fee_acct_curr_cd       string        ,
         |cups_repl_at                     string        ,
         |cups_exp_snd_chnl                string        ,
         |cups_confirm_exp_chnl            string        ,
         |cups_extend_inf                  string        ,
         |cups_conn_md                     string        ,
         |cups_msg_tp                      string        ,
         |cups_msg_tp_conv                 string        ,
         |cups_card_bin                    string        ,
         |cups_related_card_bin            string        ,
         |cups_trans_proc_cd               string        ,
         |cups_trans_proc_cd_conv          string        ,
         |cups_loc_trans_tm                string        ,
         |cups_loc_trans_dt                string        ,
         |cups_conv_dt                     string        ,
         |cups_mchnt_tp                    string        ,
         |cups_pos_entry_md_cd             string        ,
         |cups_card_seq                    string        ,
         |cups_pos_cond_cd                 string        ,
         |cups_pos_cond_cd_conv            string        ,
         |cups_retri_ref_no                string        ,
         |cups_term_id                     string        ,
         |cups_term_tp                     string        ,
         |cups_mchnt_cd                    string        ,
         |cups_card_accptr_nm_addr         string        ,
         |cups_ic_data                     string        ,
         |cups_rsn_cd                      string        ,
         |cups_addn_pos_inf                string        ,
         |cups_orig_msg_tp                 string        ,
         |cups_orig_msg_tp_conv            string        ,
         |cups_orig_sys_tra_no             string        ,
         |cups_orig_sys_tra_no_conv        string        ,
         |cups_orig_tfr_dt_tm              string        ,
         |cups_related_trans_id            string        ,
         |cups_related_trans_chnl          string        ,
         |cups_orig_trans_id               string        ,
         |cups_orig_trans_id_conv          string        ,
         |cups_orig_trans_chnl             string        ,
         |cups_orig_card_media             string        ,
         |cups_orig_card_media_proc_md     string        ,
         |cups_tfr_in_acct_no              string        ,
         |cups_tfr_out_acct_no             string        ,
         |cups_cups_resv                   string        ,
         |cups_ic_flds                     string        ,
         |cups_cups_def_fld                string        ,
         |cups_spec_settle_in              string        ,
         |cups_settle_trans_id             string        ,
         |cups_spec_mcc_in                 string        ,
         |cups_iss_ds_settle_in            string        ,
         |cups_acq_ds_settle_in            string        ,
         |cups_settle_bmp                  string        ,
         |cups_upd_in                      string        ,
         |cups_exp_rsn_cd                  string        ,
         |cups_to_ts                       string        ,
         |cups_resnd_num                   int           ,
         |cups_pri_cycle_no                string        ,
         |cups_alt_cycle_no                string        ,
         |cups_corr_pri_cycle_no           string        ,
         |cups_corr_alt_cycle_no           string        ,
         |cups_disc_in                     string        ,
         |cups_vfy_rslt                    string        ,
         |cups_vfy_fee_cd                  string        ,
         |cups_orig_disc_in                string        ,
         |cups_orig_disc_curr_cd           string        ,
         |cups_fwd_settle_at               int           ,
         |cups_rcv_settle_at               int           ,
         |cups_fwd_settle_conv_rt          int           ,
         |cups_rcv_settle_conv_rt          int           ,
         |cups_fwd_settle_curr_cd          string        ,
         |cups_rcv_settle_curr_cd          string        ,
         |cups_disc_cd                     string        ,
         |cups_allot_cd                    string        ,
         |cups_total_disc_at               int           ,
         |cups_fwd_orig_settle_at          int           ,
         |cups_rcv_orig_settle_at          int           ,
         |cups_vfy_fee_at                  int           ,
         |cups_sp_mchnt_cd                 string        ,
         |cups_acct_ins_id_cd              string        ,
         |cups_iss_ins_id_cd1              string        ,
         |cups_iss_ins_id_cd2              string        ,
         |cups_iss_ins_id_cd3              string        ,
         |cups_iss_ins_id_cd4              string        ,
         |cups_mchnt_ins_id_cd1            string        ,
         |cups_mchnt_ins_id_cd2            string        ,
         |cups_mchnt_ins_id_cd3            string        ,
         |cups_mchnt_ins_id_cd4            string        ,
         |cups_term_ins_id_cd1             string        ,
         |cups_term_ins_id_cd2             string        ,
         |cups_term_ins_id_cd3             string        ,
         |cups_term_ins_id_cd4             string        ,
         |cups_term_ins_id_cd5             string        ,
         |cups_acpt_cret_disc_at           int           ,
         |cups_acpt_debt_disc_at           int           ,
         |cups_iss1_cret_disc_at           int           ,
         |cups_iss1_debt_disc_at           int           ,
         |cups_iss2_cret_disc_at           int           ,
         |cups_iss2_debt_disc_at           int           ,
         |cups_iss3_cret_disc_at           int           ,
         |cups_iss3_debt_disc_at           int           ,
         |cups_iss4_cret_disc_at           int           ,
         |cups_iss4_debt_disc_at           int           ,
         |cups_mchnt1_cret_disc_at         int           ,
         |cups_mchnt1_debt_disc_at         int           ,
         |cups_mchnt2_cret_disc_at         int           ,
         |cups_mchnt2_debt_disc_at         int           ,
         |cups_mchnt3_cret_disc_at         int           ,
         |cups_mchnt3_debt_disc_at         int           ,
         |cups_mchnt4_cret_disc_at         int           ,
         |cups_mchnt4_debt_disc_at         int           ,
         |cups_term1_cret_disc_at          int           ,
         |cups_term1_debt_disc_at          int           ,
         |cups_term2_cret_disc_at          int           ,
         |cups_term2_debt_disc_at          int           ,
         |cups_term3_cret_disc_at          int           ,
         |cups_term3_debt_disc_at          int           ,
         |cups_term4_cret_disc_at          int           ,
         |cups_term4_debt_disc_at          int           ,
         |cups_term5_cret_disc_at          int           ,
         |cups_term5_debt_disc_at          int           ,
         |cups_pay_in                      string        ,
         |cups_exp_id                      string        ,
         |cups_vou_in                      string        ,
         |cups_orig_log_cd                 string        ,
         |cups_related_log_cd              string        ,
         |cups_mdc_key                     string        ,
         |cups_rec_upd_ts                  string        ,
         |cups_rec_crt_ts                  string        ,
         |cups_hp_settle_dt                string
         |)
         |partitioned by(part_trans_dt string)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/incident/trans/hive_acc_trans'
         | """.stripMargin)

    println("=======Create hive_acc_trans successfully ! =======")

  }
  */

  def hive_achis_trans(implicit sqlContext: HiveContext) = {
    println("=======Create hive_achis_trans=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_achis_trans")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_achis_trans(
         |settle_dt          timestamp    ,
         |trans_idx          string       ,
         |trans_tp           string       ,
         |trans_class        string       ,
         |trans_source       string       ,
         |buss_chnl          string       ,
         |carrier_tp         string       ,
         |pri_acct_no        string       ,
         |mchnt_conn_tp      string       ,
         |access_tp          string       ,
         |conn_md            string       ,
         |acq_ins_id_cd      string       ,
         |acq_head           string       ,
         |fwd_ins_id_cd      string       ,
         |rcv_ins_id_cd      string       ,
         |iss_ins_id_cd      string       ,
         |iss_head           string       ,
         |iss_head_nm        string       ,
         |mchnt_cd           string       ,
         |mchnt_nm           string       ,
         |mchnt_country      string       ,
         |mchnt_url          string       ,
         |mchnt_front_url    string       ,
         |mchnt_back_url     string       ,
         |mchnt_tp           string       ,
         |mchnt_order_id     string       ,
         |mchnt_order_desc   string       ,
         |mchnt_add_info     string       ,
         |mchnt_reserve      string       ,
         |reserve            string       ,
         |sub_mchnt_cd       string       ,
         |sub_mchnt_company  string       ,
         |sub_mchnt_nm       string       ,
         |mchnt_class        string       ,
         |sys_tra_no         string       ,
         |trans_tm           string       ,
         |sys_tm             string       ,
         |trans_dt           timestamp    ,
         |auth_id            string       ,
         |trans_at           int          ,
         |trans_curr_cd      string       ,
         |proc_st            string       ,
         |resp_cd            string       ,
         |proc_sys           string       ,
         |trans_no           string       ,
         |trans_st           string       ,
         |conv_dt            string       ,
         |settle_at          int          ,
         |settle_curr_cd     string       ,
         |settle_conv_rt     int          ,
         |cert_tp            string       ,
         |cert_id            string       ,
         |name               string       ,
         |phone_no           string       ,
         |usr_id             int          ,
         |mchnt_id           int          ,
         |pay_method         string       ,
         |trans_ip           string       ,
         |encoding           string       ,
         |mac_addr           string       ,
         |card_attr          string       ,
         |ebank_id           string       ,
         |ebank_mchnt_cd     string       ,
         |ebank_order_num    string       ,
         |ebank_idx          string       ,
         |ebank_rsp_tm       string       ,
         |kz_curr_cd         string       ,
         |kz_conv_rt         int          ,
         |kz_at              int          ,
         |delivery_country   int          ,
         |delivery_province  int          ,
         |delivery_city      int          ,
         |delivery_district  int          ,
         |delivery_street    string       ,
         |sms_tp             string       ,
         |sign_method        string       ,
         |verify_mode        string       ,
         |accpt_pos_id       string       ,
         |mer_cert_id        string       ,
         |cup_cert_id        int          ,
         |mchnt_version      string       ,
         |sub_trans_tp       string       ,
         |mac                string       ,
         |biz_tp             string       ,
         |source_idt         string       ,
         |delivery_risk      string       ,
         |trans_flag         string       ,
         |org_trans_idx      string       ,
         |org_sys_tra_no     string       ,
         |org_sys_tm         string       ,
         |org_mchnt_order_id string       ,
         |org_trans_tm       string       ,
         |org_trans_at       int          ,
         |req_pri_data       string       ,
         |pri_data           string       ,
         |addn_at            string       ,
         |res_pri_data       string       ,
         |inq_dtl            string       ,
         |reserve_fld        string       ,
         |buss_code          string       ,
         |t_mchnt_cd         string       ,
         |is_oversea         string       ,
         |points_at          int          ,
         |pri_acct_tp        string       ,
         |area_cd            string       ,
         |mchnt_fee_at       int          ,
         |user_fee_at        int          ,
         |curr_exp           string       ,
         |rcv_acct           string       ,
         |track2             string       ,
         |track3             string       ,
         |customer_nm        string       ,
         |product_info       string       ,
         |customer_email     string       ,
         |cup_branch_ins_cd  string       ,
         |org_trans_dt       timestamp    ,
         |special_calc_cost  string       ,
         |zero_cost          string       ,
         |advance_payment    string       ,
         |new_trans_tp       string       ,
         |flight_inf         string       ,
         |md_id              string       ,
         |ud_id              string       ,
         |syssp_id           string       ,
         |card_sn            string       ,
         |tfr_in_acct        string       ,
         |acct_id            string       ,
         |card_bin           string       ,
         |icc_data           string       ,
         |icc_data2          string       ,
         |card_seq_id        string       ,
         |pos_entry_cd       string       ,
         |pos_cond_cd        string       ,
         |term_id            string       ,
         |usr_num_tp         string       ,
         |addn_area_cd       string       ,
         |usr_num            string       ,
         |reserve1           string       ,
         |reserve2           string       ,
         |reserve3           string       ,
         |reserve4           string       ,
         |reserve5           string       ,
         |reserve6           string       ,
         |rec_st             string       ,
         |comments           string       ,
         |to_ts              string       ,
         |rec_crt_ts         string       ,
         |rec_upd_ts         string       ,
         |pay_acct           string       ,
         |trans_chnl         string       ,
         |tlr_st             string       ,
         |rvs_st             string       ,
         |out_trans_tp       string       ,
         |org_out_trans_tp   string       ,
         |bind_id            string       ,
         |ch_info            string       ,
         |card_risk_flag     string       ,
         |trans_step         string       ,
         |ctrl_msg           string       ,
         |mchnt_delv_tag     string       ,
         |mchnt_risk_tag     string       ,
         |bat_id             string       ,
         |payer_ip           string       ,
         |gt_sign_val        string       ,
         |mchnt_sign_val     string       ,
         |deduction_at       string       ,
         |src_sys_flag       string       ,
         |mac_ip             string       ,
         |mac_sq             string       ,
         |trans_ip_num       int          ,
         |cvn_flag           string       ,
         |expire_flag        string       ,
         |usr_inf            string       ,
         |imei               string       ,
         |iss_ins_tp         string       ,
         |dir_field          string       ,
         |buss_tp            string       ,
         |in_trans_tp        string
         |)
         |partitioned by (part_settle_dt string)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/incident/ods/hive_achis_trans'
         | """.stripMargin)

    println("=======Create hive_achis_trans successfully ! =======")

  }

  def hive_active_card_acq_branch_mon(implicit sqlContext: HiveContext) = {
    println("=======Create hive_active_card_acq_branch_mon=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_active_card_acq_branch_mon")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_active_card_acq_branch_mon(
         |trans_month             string ,
         |trans_class            string ,
         |trans_cd               string ,
         |trans_chnl_id          tinyint,
         |card_brand_id          tinyint,
         |card_attr_id           tinyint,
         |acq_intnl_org_id_cd    string ,
         |iss_root_ins_id_cd     string ,
         |active_card_num        bigint ,
         |hp_settle_month        string
         |)
         |partitioned by (part_settle_month string)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/product/card/hive_active_card_acq_branch_mon'
         | """.stripMargin)

    println("=======Create hive_active_card_acq_branch_mon successfully ! =======")

  }

  def hive_card_bind_inf(implicit sqlContext: HiveContext) = {
    println("=======Create hive_card_bind_inf=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_card_bind_inf")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_card_bind_inf(
         |cdhd_card_bind_seq     bigint    ,
         |cdhd_usr_id            string    ,
         |bind_tp                string    ,
         |bind_card_no           string    ,
         |bind_ts                timestamp ,
         |unbind_ts              timestamp ,
         |card_auth_st           string    ,
         |card_bind_st           string    ,
         |ins_id_cd              string    ,
         |auth_ts                timestamp ,
         |func_bmp               string    ,
         |rec_crt_ts             timestamp ,
         |rec_upd_ts             timestamp ,
         |ver_no                 int       ,
         |sort_seq               bigint    ,
         |cash_in                string    ,
         |acct_point_ins_id_cd   string    ,
         |acct_owner             string    ,
         |bind_source            string    ,
         |card_media             string    ,
         |backup_fld1            string    ,
         |backup_fld2            string    ,
         |iss_ins_id_cd          string    ,
         |iss_ins_cn_nm          string    ,
         |frist_bind_ts          timestamp
         |)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/relation/hive_card_bind_inf'
         | """.stripMargin)

    println("=======Create hive_card_bind_inf successfully ! =======")

  }

  def hive_city_card(implicit sqlContext: HiveContext) = {
    println("=======Create hive_city_card=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_city_card")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_city_card(
         |id                      string    ,
         |name                    string
         |)
         |row format delimited fields terminated by ','
         |stored as textfile
         |location '/user/ch_hypas/upw_hive/parameter/hive_city_card'
         | """.stripMargin)

    println("=======Create hive_city_card successfully ! =======")

  }

  def hive_ct(implicit sqlContext: HiveContext) = {
    println("=======Create hive_ct=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_ct")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_ct(
         |id                      string ,
         |name                    string
         |)
         |row format delimited fields terminated by ','
         |stored as textfile
         |location '/user/ch_hypas/upw_hive/parameter/hive_ct'
         | """.stripMargin)

    println("=======Create hive_ct successfully ! =======")

  }

  def hive_life(implicit sqlContext: HiveContext) = {
    println("=======Create hive_life=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_life")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_life(
         |MCHNT_CD                     string    ,
         |CHNL_TP_NM                   string    ,
         |BUSS_TP_NM                   string
         |)
         |row format delimited fields terminated by ','
         |stored as textfile
         |location '/user/ch_hypas/upw_hive/parameter/hive_life'
         | """.stripMargin)

    println("=======Create hive_life successfully ! =======")

  }

  def hive_discount_bas_inf(implicit sqlContext: HiveContext) = {
    println("=======Create hive_discount_bas_inf=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_discount_bas_inf")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_discount_bas_inf(
         |loc_activity_id         int        ,
         |loc_activity_nm         string     ,
         |loc_activity_desc       string     ,
         |activity_begin_dt       timestamp  ,
         |activity_end_dt         timestamp  ,
         |agio_mchnt_num          int        ,
         |eff_mchnt_num           int        ,
         |sync_bat_no             int        ,
         |sync_st                 string     ,
         |rule_upd_in             string     ,
         |rule_grp_id             int        ,
         |rec_crt_ts              timestamp  ,
         |rec_crt_usr_id          string     ,
         |rec_crt_ins_id_cd       string     ,
         |cup_branch_ins_id_nm    string     ,
         |rec_upd_ts              timestamp  ,
         |rec_upd_usr_id          string     ,
         |rec_upd_ins_id_cd       string     ,
         |del_in                  string     ,
         |ver_no                  int        ,
         |run_st                  string     ,
         |posp_from_in            string     ,
         |group_id                string
         |)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/incident/prize/hive_discount_bas_inf'
         | """.stripMargin)

    println("=======Create hive_discount_bas_inf successfully ! =======")

  }

  def hive_download_trans(implicit sqlContext: HiveContext) = {
    println("=======Create hive_download_trans=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_download_trans")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_download_trans(
         |seq_id               bigint   ,
         |cdhd_usr_id          string   ,
         |pri_acct_no          string   ,
         |acpt_ins_id_cd       string   ,
         |fwd_ins_id_cd        string   ,
         |sys_tra_no           string   ,
         |tfr_dt_tm            string   ,
         |rcv_ins_id_cd        string   ,
         |onl_trans_tra_no     string   ,
         |mchnt_cd             string   ,
         |mchnt_nm             string   ,
         |trans_dt             timestamp,
         |trans_tm             string   ,
         |trans_at             int      ,
         |buss_tp              string   ,
         |um_trans_id          string   ,
         |swt_right_tp         string   ,
         |swt_right_nm         string   ,
         |bill_id              string   ,
         |bill_nm              string   ,
         |chara_acct_tp        string   ,
         |point_at             bigint   ,
         |bill_num             bigint   ,
         |chara_acct_nm        string   ,
         |plan_id              int      ,
         |sys_det_cd           string   ,
         |acct_addup_tp        string   ,
         |remark               string   ,
         |bill_item_id         string   ,
         |trans_st             string   ,
         |discount_at          int      ,
         |booking_st           string   ,
         |plan_nm              string   ,
         |bill_acq_md          string   ,
         |oper_in              string   ,
         |rec_crt_ts           timestamp,
         |rec_upd_ts           timestamp,
         |term_id              string   ,
         |pos_entry_md_cd      string   ,
         |orig_data_elemnt     string   ,
         |udf_fld              string   ,
         |card_accptr_nm_addr  string   ,
         |token_card_no        string
         |)
         |partitioned by(part_trans_dt string)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/incident/trans/hive_download_trans'
         | """.stripMargin)

    println("=======Create hive_download_trans successfully ! =======")

  }

  def hive_inf_source_class(implicit sqlContext: HiveContext) = {
    println("=======Create hive_inf_source_class=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_ins_inf")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_inf_source_class(
         |access_nm string ,
         |class     string
         |)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/parameter/hive_inf_source_class'
         | """.stripMargin)

    println("=======Create hive_inf_source_class successfully ! =======")

  }

  def hive_ins_inf(implicit sqlContext: HiveContext) = {
    println("=======Create hive_ins_inf=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_ins_inf")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_ins_inf(
         |ins_id_cd                     string   ,
         |ins_cata                      string   ,
         |ins_tp                        string   ,
         |hdqrs_ins_id_cd               string   ,
         |cup_branch_ins_id_cd          string   ,
         |cup_branch_ins_id_nm          string   ,
         |frn_cup_branch_ins_id_cd      string   ,
         |area_cd                       string   ,
         |admin_division_cd             string   ,
         |region_cd                     string   ,
         |ins_cn_nm                     string   ,
         |ins_cn_abbr                   string   ,
         |ins_en_nm                     string   ,
         |ins_en_abbr                   string   ,
         |ins_st                        string   ,
         |lic_no                        string   ,
         |artif_certif_id               string   ,
         |artif_nm                      string   ,
         |artif_certif_expire_dt        string   ,
         |principal_nm                  string   ,
         |contact_person_nm             string   ,
         |phone                         string   ,
         |fax_no                        string   ,
         |email_addr                    string   ,
         |ins_addr                      string   ,
         |zip_cd                        string   ,
         |oper_in                       string   ,
         |event_id                      int      ,
         |rec_id                        int      ,
         |rec_upd_usr_id                string   ,
         |rec_upd_ts                    timestamp,
         |rec_crt_ts                    timestamp
         |)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/participant/ins/hive_ins_inf'
         | """.stripMargin)

    println("=======Create hive_ins_inf successfully ! =======")

  }

  def hive_mchnt_para(implicit sqlContext: HiveContext) = {
    println("=======Create hive_mchnt_para=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_mchnt_para")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_mchnt_para(
         |mchnt_para_id             bigint     ,
         |mchnt_para_cn_nm          string     ,
         |mchnt_para_en_nm          string     ,
         |mchnt_para_tp             string     ,
         |mchnt_para_level          int        ,
         |mchnt_para_parent_id      bigint     ,
         |mchnt_para_order          int        ,
         |rec_crt_ts                timestamp  ,
         |rec_upd_ts                timestamp  ,
         |ver_no                    int
         |)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/parameter/hive_mchnt_para'
         | """.stripMargin)

    println("=======Create hive_mchnt_para successfully ! =======")

  }

  def hive_passive_code_pay_trans(implicit sqlContext: HiveContext) = {
    println("=======Create hive_passive_code_pay_trans=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_passive_code_pay_trans")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_passive_code_pay_trans(
         |trans_seq              string     ,
         |cdhd_card_bind_seq     bigint     ,
         |cdhd_usr_id            string     ,
         |card_no                string     ,
         |tran_mode              string     ,
         |trans_tp               string     ,
         |tran_certi             string     ,
         |trans_rdm_num          string     ,
         |trans_expire_ts        timestamp  ,
         |order_id               string     ,
         |device_cd              string     ,
         |mchnt_cd               string     ,
         |mchnt_nm               string     ,
         |sub_mchnt_cd           string     ,
         |sub_mchnt_nm           string     ,
         |settle_dt              string     ,
         |trans_at               bigint     ,
         |discount_at            bigint     ,
         |discount_info          string     ,
         |refund_at              int        ,
         |orig_trans_seq         string     ,
         |trans_st               string     ,
         |rec_crt_ts             timestamp  ,
         |rec_upd_ts             timestamp  ,
         |trans_dt               timestamp       ,
         |resp_code              string     ,
         |resp_msg               string     ,
         |out_trade_no           string     ,
         |body                   string     ,
         |terminal_id            string     ,
         |extend_params          string
         |)
         |partitioned by (part_trans_dt string)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/incident/trans/hive_passive_code_pay_trans'
         | """.stripMargin)

    println("=======Create hive_passive_code_pay_trans successfully ! =======")

  }

  def hive_cdhd_pri_acct_inf(implicit sqlContext: HiveContext) = {
    println("=======Create hive_cdhd_pri_acct_inf=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_cdhd_pri_acct_inf")
    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_cdhd_pri_acct_inf(
         |cdhd_usr_id               string     ,
         |reg_dt                    timestamp  ,
         |usr_nm                    string     ,
         |mobile                    string     ,
         |mobile_vfy_st             string     ,
         |email_addr                string     ,
         |email_vfy_st              string     ,
         |inf_source                string     ,
         |real_nm                   string     ,
         |real_nm_st                string     ,
         |nick_nm                   string     ,
         |certif_tp                 string     ,
         |certif_id                 string     ,
         |certif_vfy_st             string     ,
         |birth_dt                  timestamp  ,
         |sex                       string     ,
         |age                       string     ,
         |marital_st                string     ,
         |home_mem_num              string     ,
         |cntry_cd                  string     ,
         |gb_region_cd              string     ,
         |comm_addr                 string     ,
         |zip_cd                    string     ,
         |nationality               string     ,
         |ed_lvl                    string     ,
         |msn_no                    string     ,
         |qq_no                     string     ,
         |person_homepage           string     ,
         |industry_id               string     ,
         |annual_income_lvl         string     ,
         |hobby                     string     ,
         |brand_prefer              string     ,
         |buss_dist_prefer          string     ,
         |head_pic_file_path        string     ,
         |pwd_cue_ques              string     ,
         |pwd_cue_answ              string     ,
         |usr_eval_lvl              string     ,
         |usr_class_lvl             string     ,
         |usr_st                    string     ,
         |open_func                 string     ,
         |rec_crt_ts                timestamp  ,
         |rec_upd_ts                timestamp  ,
         |mobile_new                string     ,
         |email_addr_new            string     ,
         |activate_ts               timestamp  ,
         |activate_pwd              string     ,
         |region_cd                 string     ,
         |ver_no                    int        ,
         |func_bmp                  string     ,
         |point_pre_open_ts         timestamp  ,
         |refer_usr_id              string     ,
         |vendor_fk                 bigint     ,
         |phone                     string     ,
         |vip_svc                   string     ,
         |user_lvl_id               int        ,
         |auto_adjust_lvl_in        int        ,
         |lvl_begin_dt              timestamp  ,
         |customer_title            string     ,
         |company                   string     ,
         |dept                      string     ,
         |duty                      string     ,
         |resv_phone                string     ,
         |join_activity_list        string     ,
         |remark                    string     ,
         |note                      string     ,
         |usr_lvl_expire_dt         timestamp  ,
         |reg_card_no               string     ,
         |reg_tm                    string     ,
         |activity_source           string     ,
         |chsp_svc_in               string     ,
         |accept_sms_in             string     ,
         |prov_division_cd          string     ,
         |city_division_cd          string     ,
         |vid_last_login            string     ,
         |pay_pwd                   string     ,
         |pwd_set_st                string     ,
         |realnm_in                 string
         |)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/participant/user/hive_cdhd_pri_acct_inf'
         | """.stripMargin)

    println("=======Create hive_cdhd_pri_acct_inf successfully ! =======")

  }


  def hive_pri_acct_inf(implicit sqlContext: HiveContext) = {
    println("=======Create hive_pri_acct_inf=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_pri_acct_inf")
    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_pri_acct_inf(
         |cdhd_usr_id            string    ,
         |reg_dt                 timestamp      ,
         |usr_nm                 string    ,
         |mobile                 string    ,
         |mobile_vfy_st          string    ,
         |email_addr             string    ,
         |email_vfy_st           string    ,
         |inf_source             string    ,
         |real_nm                string    ,
         |real_nm_st             string    ,
         |nick_nm                string    ,
         |certif_tp              string    ,
         |certif_id              string    ,
         |certif_vfy_st          string    ,
         |birth_dt               timestamp      ,
         |sex                    string    ,
         |age                    string    ,
         |marital_st             string    ,
         |home_mem_num           string    ,
         |cntry_cd               string    ,
         |gb_region_cd           string    ,
         |comm_addr              string    ,
         |zip_cd                 string    ,
         |nationality            string    ,
         |ed_lvl                 string    ,
         |msn_no                 string    ,
         |qq_no                  string    ,
         |person_homepage        string    ,
         |industry_id            string    ,
         |annual_income_lvl      string    ,
         |hobby                  string    ,
         |brand_prefer           string    ,
         |buss_dist_prefer       string    ,
         |head_pic_file_path     string    ,
         |pwd_cue_ques           string    ,
         |pwd_cue_answ           string    ,
         |usr_eval_lvl           string    ,
         |usr_class_lvl          string    ,
         |usr_st                 string    ,
         |open_func              string    ,
         |rec_crt_ts             timestamp ,
         |rec_upd_ts             timestamp ,
         |mobile_new             string    ,
         |email_addr_new         string    ,
         |activate_ts            timestamp ,
         |activate_pwd           string    ,
         |region_cd              string    ,
         |ver_no                 int       ,
         |func_bmp               string    ,
         |point_pre_open_ts      timestamp ,
         |refer_usr_id           string    ,
         |vendor_fk              bigint    ,
         |phone                  string    ,
         |vip_svc                string    ,
         |user_lvl_id            int       ,
         |auto_adjust_lvl_in     int       ,
         |lvl_begin_dt           timestamp      ,
         |customer_title         string    ,
         |company                string    ,
         |dept                   string    ,
         |duty                   string    ,
         |resv_phone             string    ,
         |join_activity_list     string    ,
         |remark                 string    ,
         |note                   string    ,
         |usr_lvl_expire_dt      timestamp      ,
         |reg_card_no            string    ,
         |reg_tm                 string    ,
         |activity_source        string    ,
         |chsp_svc_in            string    ,
         |accept_sms_in          string    ,
         |prov_division_cd       string    ,
         |city_division_cd       string    ,
         |vid_last_login         string    ,
         |pay_pwd                string    ,
         |pwd_set_st             string    ,
         |realnm_in	       string    ,
         |birthday               string    ,
         |province_card          string    ,
         |city_card              string    ,
         |mobile_provider        string    ,
         |sex_card               string    ,
         |phone_location         string    ,
         |relate_id              string
         |)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/participant/user/hive_pri_acct_inf'
         | """.stripMargin)

    println("=======Create hive_pri_acct_inf successfully ! =======")

  }

  def hive_province_card(implicit sqlContext: HiveContext) = {
    println("=======Create hive_province_card=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_province_card")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_province_card(
         |id                      string ,
         |name                    string
         |)
         |row format delimited fields terminated by ','
         |stored as textfile
         |location '/user/ch_hypas/upw_hive/parameter/hive_province_card'
         | """.stripMargin)

    println("=======Create hive_province_card successfully ! =======")

  }

  def hive_switch_point_trans(implicit sqlContext: HiveContext) = {
    println("=======Create hive_switch_point_trans=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_switch_point_trans")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_switch_point_trans(
         |tfr_dt_tm                   string    ,
         |sys_tra_no                  string    ,
         |acpt_ins_id_cd              string    ,
         |msg_fwd_ins_id_cd           string    ,
         |pri_key1                    string    ,
         |fwd_chnl_head               string    ,
         |chswt_plat_seq              bigint    ,
         |trans_tm                    string    ,
         |trans_dt                    timestamp ,
         |cswt_settle_dt              timestamp ,
         |internal_trans_tp           string    ,
         |settle_trans_id             string    ,
         |trans_tp                    string    ,
         |cups_settle_dt              string    ,
         |msg_tp                      string    ,
         |pri_acct_no                 string    ,
         |card_bin                    string    ,
         |proc_cd                     string    ,
         |req_trans_at                bigint    ,
         |resp_trans_at               bigint    ,
         |trans_curr_cd               string    ,
         |trans_tot_at                bigint    ,
         |iss_ins_id_cd               string    ,
         |launch_trans_tm             string    ,
         |launch_trans_dt             string    ,
         |mchnt_tp                    string    ,
         |pos_entry_md_cd             string    ,
         |card_seq_id                 string    ,
         |pos_cond_cd                 string    ,
         |pos_pin_capture_cd          string    ,
         |retri_ref_no                string    ,
         |term_id                     string    ,
         |mchnt_cd                    string    ,
         |card_accptr_nm_loc          string    ,
         |sec_related_ctrl_inf        string    ,
         |orig_data_elemts            string    ,
         |rcv_ins_id_cd               string    ,
         |fwd_proc_in                 string    ,
         |rcv_proc_in                 string    ,
         |proj_tp                     string    ,
         |usr_id                      string    ,
         |conv_usr_id                 string    ,
         |trans_st                    string    ,
         |inq_dtl_req                 string    ,
         |inq_dtl_resp                string    ,
         |iss_ins_resv                string    ,
         |ic_flds                     string    ,
         |cups_def_fld                string    ,
         |id_no                       string    ,
         |cups_resv                   string    ,
         |acpt_ins_resv               string    ,
         |rout_ins_id_cd              string    ,
         |sub_rout_ins_id_cd          string    ,
         |recv_access_resp_cd         string    ,
         |chswt_resp_cd               string    ,
         |chswt_err_cd                string    ,
         |resv_fld1                   string    ,
         |resv_fld2                   string    ,
         |to_ts                       timestamp ,
         |rec_upd_ts                  timestamp ,
         |rec_crt_ts                  timestamp ,
         |settle_at                   bigint    ,
         |external_amt                bigint    ,
         |discount_at                 bigint    ,
         |card_pay_at                 bigint    ,
         |right_purchase_at           bigint    ,
         |recv_second_resp_cd         string    ,
         |req_acpt_ins_resv           string    ,
         |log_id                      string    ,
         |conv_acct_no                string    ,
         |inner_pro_ind               string    ,
         |acct_proc_in                string    ,
         |order_id                    string
         |)
         |partitioned by(part_trans_dt string)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/incident/trans/hive_switch_point_trans'
         | """.stripMargin)

    println("=======Create hive_switch_point_trans successfully ! =======")

  }

  def hive_ucbiz_cdhd_bas_inf(implicit sqlContext: HiveContext) = {
    println("=======Create hive_ucbiz_cdhd_bas_inf=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_ucbiz_cdhd_bas_inf")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_ucbiz_cdhd_bas_inf(
         |usr_id                string ,
         |cntry_phone_cd        string ,
         |mobile                string ,
         |email_addr            string ,
         |usr_nm                string ,
         |nick_nm               string ,
         |new_mobile            string ,
         |email_vfy_st          string ,
         |inf_source            string ,
         |data_source           string ,
         |usr_st                string ,
         |reg_ts                string ,
         |reg_md                string ,
         |relate_id             string ,
         |access_id             string ,
         |rec_crt_ts            string ,
         |rec_upd_ts            string ,
         |wel_state             string ,
         |ip_addr               string ,
         |login_pwd             string ,
         |pay_pwd               string ,
         |union_flag            string ,
         |func_bmp              int    ,
         |ver_no                int    ,
         |func_bmp_char         string
         |)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/participant/user/hive_ucbiz_cdhd_bas_inf'
         | """.stripMargin)

    println("=======Create hive_ucbiz_cdhd_bas_inf successfully ! =======")

  }

  def hive_access_bas_inf(implicit sqlContext: HiveContext) = {
    println("=======Create hive_access_bas_inf=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_access_bas_inf")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_access_bas_inf(
         |seq_id											bigint,
         |ch_ins_tp                   string,
         |ch_ins_id_cd                string,
         |ch_access_nm                string,
         |region_cd                   string,
         |cup_branch_ins_id_cd        string,
         |cup_branch_nm               string,
         |access_ins_st               string,
         |allot_md_id                 int   ,
         |extend_ins_id_cd            string,
         |extend_ins_nm               string,
         |ins_acct_id                 string,
         |req_dt                      string,
         |enable_dt                   string,
         |entry_usr_id                string,
         |entry_ts                    timestamp,
         |upd_usr_id                  string,
         |upd_ts                      timestamp,
         |oper_action                 string,
         |aud_usr_id                  string,
         |aud_idea                    string,
         |aud_ts                      timestamp,
         |aud_st                      string,
         |ver_no                      int,
         |receipt_tp                  string ,
         |event_id                    int,
         |sync_st                     string,
         |point_enable_in             string,
         |access_prov_cd              string,
         |access_city_cd              string,
         |access_district_cd          string,
         |oper_in                     string,
         |exch_in                     string,
         |exch_out                    string,
         |acpt_notify_in              string,
         |receipt_bank                string,
         |receipt_bank_acct           string,
         |receipt_bank_acct_nm        string,
         |temp_use_bmp                string,
         |term_diff_store_tp_in       string,
         |receipt_flag                string,
         |term_alter_in               string,
         |urgent_sync_in              string,
         |acpt_notify_url             string,
         |input_data_tp               string,
         |entry_ins_id_cd             string,
         |entry_ins_cn_nm             string,
         |entry_cup_branch_ins_id_cd  string,
         |entry_cup_branch_nm         string,
         |term_mt_ins_id_cd           string,
         |term_mt_ins_cn_nm           string,
         |fwd_ins_id_cd               string,
         |pri_mchnt_cd                string,
         |cup_branch_ins_id_nm        string
         |
         |)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/participant/ins/hive_access_bas_inf'
         | """.stripMargin)

    println("=======Create hive_access_bas_inf successfully ! =======")

  }

  def hive_active_code_pay_trans(implicit sqlContext: HiveContext) = {
    println("=======Create hive_active_code_pay_trans=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_active_code_pay_trans")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_active_code_pay_trans(
         |settle_dt           	timestamp ,
         |trans_idx               string,
         |trans_tp                string,
         |trans_class             string,
         |trans_source            string,
         |buss_chnl               string,
         |carrier_tp              string,
         |pri_acct_no             string,
         |mchnt_conn_tp           string,
         |access_tp               string,
         |conn_md                 string,
         |acq_ins_id_cd           string,
         |acq_head                string,
         |fwd_ins_id_cd           string,
         |rcv_ins_id_cd           string,
         |iss_ins_id_cd           string,
         |iss_head                string,
         |iss_head_nm             string,
         |mchnt_cd                string,
         |mchnt_nm                string,
         |mchnt_country           string,
         |mchnt_url               string,
         |mchnt_front_url         string,
         |mchnt_back_url          string,
         |mchnt_tp                string,
         |mchnt_order_id          string,
         |mchnt_order_desc        string,
         |mchnt_add_info          string,
         |mchnt_reserve           string,
         |reserve                 string,
         |sub_mchnt_cd            string,
         |sub_mchnt_company       string,
         |sub_mchnt_nm            string,
         |mchnt_class             string,
         |sys_tra_no              string,
         |trans_tm                string,
         |sys_tm                  string,
         |trans_dt                timestamp  ,
         |auth_id                 string,
         |trans_at                int   ,
         |trans_curr_cd           string,
         |proc_st                 string,
         |resp_cd                 string,
         |proc_sys                string,
         |trans_no                string,
         |trans_st                string,
         |conv_dt                 string,
         |settle_at               int   ,
         |settle_curr_cd          string,
         |settle_conv_rt          int   ,
         |cert_tp                 string,
         |cert_id                 string,
         |name                    string,
         |phone_no                string,
         |usr_id                  int   ,
         |mchnt_id                int   ,
         |pay_method              string,
         |trans_ip                string,
         |encoding                string,
         |mac_addr                string,
         |card_attr               string,
         |ebank_id                string,
         |ebank_mchnt_cd          string,
         |ebank_order_num         string,
         |ebank_idx               string,
         |ebank_rsp_tm            string,
         |kz_curr_cd              string,
         |kz_conv_rt              int   ,
         |kz_at                   int   ,
         |delivery_country        int   ,
         |delivery_province       int   ,
         |delivery_city           int   ,
         |delivery_district       int   ,
         |delivery_street         string,
         |sms_tp                  string,
         |sign_method             string,
         |verify_mode             string,
         |accpt_pos_id            string,
         |mer_cert_id             string,
         |cup_cert_id             int   ,
         |mchnt_version           string,
         |sub_trans_tp            string,
         |mac                     string,
         |biz_tp                  string,
         |source_idt              string,
         |delivery_risk           string,
         |trans_flag              string,
         |org_trans_idx           string,
         |org_sys_tra_no          string,
         |org_sys_tm              string,
         |org_mchnt_order_id      string,
         |org_trans_tm            string,
         |org_trans_at            int   ,
         |req_pri_data            string,
         |pri_data                string,
         |addn_at                 string,
         |res_pri_data            string,
         |inq_dtl                 string,
         |reserve_fld             string,
         |buss_code               string,
         |t_mchnt_cd              string,
         |is_oversea              string,
         |points_at               int   ,
         |pri_acct_tp             string,
         |area_cd                 string,
         |mchnt_fee_at            int   ,
         |user_fee_at             int   ,
         |curr_exp                string,
         |rcv_acct                string,
         |track2                  string,
         |track3                  string,
         |customer_nm             string,
         |product_info            string,
         |customer_email          string,
         |cup_branch_ins_cd       string,
         |org_trans_dt            timestamp  ,
         |special_calc_cost       string,
         |zero_cost               string,
         |advance_payment         string,
         |new_trans_tp            string,
         |flight_inf              string,
         |md_id                   string,
         |ud_id                   string,
         |syssp_id                string,
         |card_sn                 string,
         |tfr_in_acct             string,
         |acct_id                 string,
         |card_bin                string,
         |icc_data                string,
         |icc_data2               string,
         |card_seq_id             string,
         |pos_entry_cd            string,
         |pos_cond_cd             string,
         |term_id                 string,
         |usr_num_tp              string,
         |addn_area_cd            string,
         |usr_num                 string,
         |reserve1                string,
         |reserve2                string,
         |reserve3                string,
         |reserve4                string,
         |reserve5                string,
         |reserve6                string,
         |rec_st                  string,
         |comments                string,
         |to_ts                   string,
         |rec_crt_ts              string,
         |rec_upd_ts              string,
         |pay_acct                string,
         |trans_chnl              string,
         |tlr_st                  string,
         |rvs_st                  string,
         |out_trans_tp            string,
         |org_out_trans_tp        string,
         |bind_id                 string,
         |ch_info                 string,
         |card_risk_flag          string,
         |trans_step              string,
         |ctrl_msg                string,
         |mchnt_delv_tag          string,
         |mchnt_risk_tag          string,
         |bat_id                  string,
         |payer_ip                string,
         |gt_sign_val             string,
         |mchnt_sign_val          string,
         |deduction_at            string,
         |src_sys_flag            string,
         |mac_ip                  string,
         |mac_sq                  string,
         |trans_ip_num            int   ,
         |cvn_flag                string,
         |expire_flag             string,
         |usr_inf                 string,
         |imei                    string,
         |iss_ins_tp              string,
         |dir_field               string,
         |buss_tp                 string,
         |in_trans_tp             string
         |
         |)
         |partitioned by (part_settle_dt string)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/incident/trans/hive_active_code_pay_trans'
         | """.stripMargin)

    println("=======Create hive_active_code_pay_trans successfully ! =======")

  }

  def hive_branch_acpt_ins_inf(implicit sqlContext: HiveContext) = {
    println("=======Create hive_branch_acpt_ins_inf=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_branch_acpt_ins_inf")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_branch_acpt_ins_inf(
         |cup_branch_ins_id_cd   string,
         |ins_id_cd              string,
         |cup_branch_ins_id_nm   string
         |)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/participant/ins/hive_branch_acpt_ins_inf'
         | """.stripMargin)

    println("=======Create hive_branch_acpt_ins_inf successfully ! =======")

  }

  def hive_brand_inf(implicit sqlContext: HiveContext) = {
    println("=======Create hive_brand_inf=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_brand_inf")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_brand_inf(
         |brand_id			         bigint,
         |brand_nm               string,
         |buss_bmp               string,
         |cup_branch_ins_id_cd   string,
         |avg_consume            int   ,
         |brand_desc             string,
         |avg_comment            bigint,
         |brand_st               string,
         |content_id             int   ,
         |rec_crt_ts             timestamp,
         |rec_upd_ts             timestamp,
         |brand_env_grade        bigint,
         |brand_srv_grade        bigint,
         |brand_popular_grade    bigint,
         |brand_taste_grade      bigint,
         |brand_tp               string,
         |entry_ins_id_cd        string,
         |entry_ins_cn_nm        string,
         |rec_crt_usr_id         string,
         |rec_upd_usr_id         string
         |)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/participant/other/hive_brand_inf'
         | """.stripMargin)

    println("=======Create hive_brand_inf successfully ! =======")

  }

  def hive_card_bin(implicit sqlContext: HiveContext) = {
    println("=======Create hive_card_bin=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_card_bin")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_card_bin(
         |card_bin           string,
         |card_cn_nm         string,
         |card_en_nm         string,
         |iss_ins_id_cd      string,
         |iss_ins_cn_nm      string,
         |iss_ins_en_nm      string,
         |card_bin_len       int,
         |card_attr          string,
         |card_cata          string,
         |card_class         string,
         |card_brand         string,
         |card_prod          string,
         |card_lvl           string,
         |card_media         string,
         |ic_app_tp          string,
         |pan_len            string,
         |pan_sample         string,
         |pay_curr_cd1       string,
         |pay_curr_cd2       string,
         |pay_curr_cd3       string,
         |card_bin_priv_bmp  string,
         |publish_dt         timestamp,
         |card_vfy_algo      string,
         |frn_trans_in       string,
         |oper_in            string,
         |event_id           int,
         |rec_id             int,
         |rec_upd_usr_id     string,
         |rec_upd_ts         timestamp,
         |rec_crt_ts         timestamp
         |)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/product/card/hive_card_bin'
         | """.stripMargin)

    println("=======Create hive_card_bin successfully ! =======")

  }

  def hive_cdhd_cashier_maktg_reward_dtl(implicit sqlContext: HiveContext) = {
    println("=======Create hive_cdhd_cashier_maktg_reward_dtl=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_cdhd_cashier_maktg_reward_dtl")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_cdhd_cashier_maktg_reward_dtl(
         |seq_id                bigint,
         |trans_tfr_tm          string,
         |sys_tra_no            string,
         |acpt_ins_id_cd        string,
         |fwd_ins_id_cd         string,
         |trans_id              string,
         |pri_acct_no           string,
         |trans_at              bigint,
         |remain_trans_at       bigint,
         |settle_dt             timestamp,
         |mchnt_tp              string,
         |iss_ins_id_cd         string,
         |acq_ins_id_cd         string,
         |cup_branch_ins_id_cd  string,
         |mchnt_cd              string,
         |term_id               string,
         |trans_curr_cd         string,
         |trans_chnl            string,
         |orig_tfr_dt_tm        string,
         |orig_sys_tra_no       string,
         |orig_acpt_ins_id_cd   string,
         |orig_fwd_ins_id_cd    string,
         |loc_activity_id       int,
         |prize_lvl             int,
         |activity_tp           string,
         |reward_point_rate     bigint,
         |reward_point_max      bigint,
         |prize_result_seq      int,
         |trans_direct          string,
         |reward_usr_tp         string,
         |cdhd_usr_id           string,
         |mobile                string,
         |reward_card_no        string,
         |reward_point_at       int,
         |bill_id               string,
         |reward_bill_num       int,
         |prize_dt              string,
         |rec_crt_dt            string,
         |acct_dt               string,
         |rec_st                string,
         |rec_crt_ts            timestamp,
         |rec_upd_ts            timestamp,
         |over_plan_in          string,
         |is_match_in           string,
         |rebate_activity_id    int,
         |rebate_activity_nm    string,
         |rebate_prize_lvl      int,
         |buss_tp               string,
         |ins_acct_id           string,
         |chara_acct_tp         string,
         |trans_pri_key         string,
         |orig_trans_pri_key    string,
         |cup_branch_ins_id_nm  string
         |)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/parameter/hive_cdhd_cashier_maktg_reward_dtl'
         | """.stripMargin)

    println("=======Create hive_cdhd_cashier_maktg_reward_dtl successfully ! =======")

  }

  def hive_cashier_point_acct_oper_dtl(implicit sqlContext: HiveContext) = {
    println("=======Create hive_cashier_point_acct_oper_dtl=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_cashier_point_acct_oper_dtl")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_cashier_point_acct_oper_dtl(
         | acct_oper_id					bigint ,
         | cashier_usr_id       string ,
         | acct_oper_ts         timestamp ,
         | acct_oper_point_at   bigint,
         | acct_oper_related_id string,
         | acct_oper_tp         string,
         | ver_no               int
         |)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/incident/other/hive_cashier_point_acct_oper_dtl'
         | """.stripMargin)

    println("=======Create hive_cashier_point_acct_oper_dtl successfully ! =======")

  }

  def hive_chara_grp_def_bat(implicit sqlContext: HiveContext) = {
    println("=======Create hive_chara_grp_def_bat=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_chara_grp_def_bat")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_chara_grp_def_bat(
         |rec_id                       bigint,
         |chara_grp_cd                 string,
         |chara_grp_nm                 string,
         |chara_data                   string,
         |chara_data_tp                string,
         |aud_usr_id                   string,
         |aud_idea                     string,
         |aud_ts                       timestamp ,
         |aud_in                       string,
         |oper_action                  string,
         |rec_crt_ts                   timestamp,
         |rec_upd_ts                   timestamp,
         |rec_crt_usr_id               string,
         |rec_upd_usr_id               string,
         |ver_no                       int,
         |event_id                     int,
         |oper_in                      string,
         |sync_st                      string,
         |cup_branch_ins_id_cd         string,
         |input_data_tp                string,
         |brand_id                     bigint,
         |entry_ins_id_cd              string,
         |entry_ins_cn_nm              string,
         |entry_cup_branch_ins_id_cd   string,
         |order_in_grp                 int
         |)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/agreement/hive_chara_grp_def_bat'
         | """.stripMargin)

    println("=======Create hive_chara_grp_def_bat successfully ! =======")

  }

  def hive_cups_trans(implicit sqlContext: HiveContext) = {
    println("=======Create hive_cups_trans=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_cups_trans")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_cups_trans(
         |settle_dt                 timestamp      ,
         |pri_key                   string         ,
         |log_cd                    string         ,
         |settle_tp                 string         ,
         |settle_cycle              string         ,
         |block_id                  string         ,
         |orig_key                  string         ,
         |related_key               string         ,
         |trans_fwd_st              string         ,
         |trans_rcv_st              string         ,
         |sms_dms_conv_in           string         ,
         |fee_in                    string         ,
         |cross_dist_in             string         ,
         |orig_acpt_sdms_in         string         ,
         |tfr_in_in                 string         ,
         |trans_md                  string         ,
         |source_region_cd          string         ,
         |dest_region_cd            string         ,
         |cups_card_in              string         ,
         |cups_sig_card_in          string         ,
         |card_class                string         ,
         |card_attr                 string         ,
         |sti_in                    string         ,
         |trans_proc_in             string         ,
         |acq_ins_id_cd             string         ,
         |acq_ins_tp                string         ,
         |fwd_ins_id_cd             string         ,
         |fwd_ins_tp                string         ,
         |rcv_ins_id_cd             string         ,
         |rcv_ins_tp                string         ,
         |iss_ins_id_cd             string         ,
         |iss_ins_tp                string         ,
         |related_ins_id_cd         string         ,
         |related_ins_tp            string         ,
         |acpt_ins_id_cd            string         ,
         |acpt_ins_tp               string         ,
         |pri_acct_no               string         ,
         |pri_acct_no_conv          string         ,
         |sys_tra_no                string         ,
         |sys_tra_no_conv           string         ,
         |sw_sys_tra_no             string         ,
         |auth_dt                   string         ,
         |auth_id_resp_cd           string         ,
         |resp_cd1                  string         ,
         |resp_cd2                  string         ,
         |resp_cd3                  string         ,
         |resp_cd4                  string         ,
         |cu_trans_st               string         ,
         |sti_takeout_in            string         ,
         |trans_id                  string         ,
         |trans_tp                  string         ,
         |trans_chnl                string         ,
         |card_media                string         ,
         |card_media_proc_md        string         ,
         |card_brand                string         ,
         |expire_seg                string         ,
         |trans_id_conv             string         ,
         |settle_mon                string         ,
         |settle_d                  string         ,
         |orig_settle_dt            timestamp      ,
         |settle_fwd_ins_id_cd      string         ,
         |settle_rcv_ins_id_cd      string         ,
         |trans_at                  int            ,
         |orig_trans_at             int            ,
         |trans_conv_rt             int            ,
         |trans_curr_cd             string         ,
         |cdhd_fee_at               int            ,
         |cdhd_fee_conv_rt          int            ,
         |cdhd_fee_acct_curr_cd     string         ,
         |repl_at                   string         ,
         |exp_snd_chnl              string         ,
         |confirm_exp_chnl          string         ,
         |extend_inf                string         ,
         |conn_md                   string         ,
         |msg_tp                    string         ,
         |msg_tp_conv               string         ,
         |card_bin                  string         ,
         |related_card_bin          string         ,
         |trans_proc_cd             string         ,
         |trans_proc_cd_conv        string         ,
         |tfr_dt_tm                 string         ,
         |loc_trans_tm              string         ,
         |loc_trans_dt              string         ,
         |conv_dt                   string         ,
         |mchnt_tp                  string         ,
         |pos_entry_md_cd           string         ,
         |card_seq                  string         ,
         |pos_cond_cd               string         ,
         |pos_cond_cd_conv          string         ,
         |retri_ref_no              string         ,
         |term_id                   string         ,
         |term_tp                   string         ,
         |mchnt_cd                  string         ,
         |card_accptr_nm_addr       string         ,
         |ic_data                   string         ,
         |rsn_cd                    string         ,
         |addn_pos_inf              string         ,
         |orig_msg_tp               string         ,
         |orig_msg_tp_conv          string         ,
         |orig_sys_tra_no           string         ,
         |orig_sys_tra_no_conv      string         ,
         |orig_tfr_dt_tm            string         ,
         |related_trans_id          string         ,
         |related_trans_chnl        string         ,
         |orig_trans_id             string         ,
         |orig_trans_id_conv        string         ,
         |orig_trans_chnl           string         ,
         |orig_card_media           string         ,
         |orig_card_media_proc_md   string         ,
         |tfr_in_acct_no            string         ,
         |tfr_out_acct_no           string         ,
         |cups_resv                 string         ,
         |ic_flds                   string         ,
         |cups_def_fld              string         ,
         |spec_settle_in            string         ,
         |settle_trans_id           string         ,
         |spec_mcc_in               string         ,
         |iss_ds_settle_in          string         ,
         |acq_ds_settle_in          string         ,
         |settle_bmp                string         ,
         |upd_in                    string         ,
         |exp_rsn_cd                string         ,
         |to_ts                     string         ,
         |resnd_num                 int            ,
         |pri_cycle_no              string         ,
         |alt_cycle_no              string         ,
         |corr_pri_cycle_no         string         ,
         |corr_alt_cycle_no         string         ,
         |disc_in                   string         ,
         |vfy_rslt                  string         ,
         |vfy_fee_cd                string         ,
         |orig_disc_in              string         ,
         |orig_disc_curr_cd         string         ,
         |fwd_settle_at             int            ,
         |rcv_settle_at             int            ,
         |fwd_settle_conv_rt        int            ,
         |rcv_settle_conv_rt        int            ,
         |fwd_settle_curr_cd        string         ,
         |rcv_settle_curr_cd        string         ,
         |disc_cd                   string         ,
         |allot_cd                  string         ,
         |total_disc_at             int            ,
         |fwd_orig_settle_at        int            ,
         |rcv_orig_settle_at        int            ,
         |vfy_fee_at                int            ,
         |sp_mchnt_cd               string         ,
         |acct_ins_id_cd            string         ,
         |iss_ins_id_cd1            string         ,
         |iss_ins_id_cd2            string         ,
         |iss_ins_id_cd3            string         ,
         |iss_ins_id_cd4            string         ,
         |mchnt_ins_id_cd1          string         ,
         |mchnt_ins_id_cd2          string         ,
         |mchnt_ins_id_cd3          string         ,
         |mchnt_ins_id_cd4          string         ,
         |term_ins_id_cd1           string         ,
         |term_ins_id_cd2           string         ,
         |term_ins_id_cd3           string         ,
         |term_ins_id_cd4           string         ,
         |term_ins_id_cd5           string         ,
         |acpt_cret_disc_at         int            ,
         |acpt_debt_disc_at         int            ,
         |iss1_cret_disc_at         int            ,
         |iss1_debt_disc_at         int            ,
         |iss2_cret_disc_at         int            ,
         |iss2_debt_disc_at         int            ,
         |iss3_cret_disc_at         int            ,
         |iss3_debt_disc_at         int            ,
         |iss4_cret_disc_at         int            ,
         |iss4_debt_disc_at         int            ,
         |mchnt1_cret_disc_at       int            ,
         |mchnt1_debt_disc_at       int            ,
         |mchnt2_cret_disc_at       int            ,
         |mchnt2_debt_disc_at       int            ,
         |mchnt3_cret_disc_at       int            ,
         |mchnt3_debt_disc_at       int            ,
         |mchnt4_cret_disc_at       int            ,
         |mchnt4_debt_disc_at       int            ,
         |term1_cret_disc_at        int            ,
         |term1_debt_disc_at        int            ,
         |term2_cret_disc_at        int            ,
         |term2_debt_disc_at        int            ,
         |term3_cret_disc_at        int            ,
         |term3_debt_disc_at        int            ,
         |term4_cret_disc_at        int            ,
         |term4_debt_disc_at        int            ,
         |term5_cret_disc_at        int            ,
         |term5_debt_disc_at        int            ,
         |pay_in                    string         ,
         |exp_id                    string         ,
         |vou_in                    string         ,
         |orig_log_cd               string         ,
         |related_log_cd            string         ,
         |mdc_key                   string         ,
         |rec_upd_ts                string         ,
         |rec_crt_ts                string         ,
         |hp_settle_dt              string
         |)
         |partitioned by (part_settle_dt string)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/incident/ods/hive_cups_trans'
         | """.stripMargin)

    println("=======Create hive_cups_trans successfully ! =======")

  }

  def hive_filter_app_det(implicit sqlContext: HiveContext) = {
    println("=======Create hive_filter_app_det=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_filter_app_det")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_filter_app_det(
         |app_id						int   ,
         |app_usage_id      string,
         |rule_grp_cata     string,
         |activity_plat     string,
         |loc_activity_id   int   ,
         |sync_st           string,
         |sync_bat_no       int   ,
         |rule_grp_id       int   ,
         |oper_in           string,
         |event_id          int   ,
         |rec_id            int   ,
         |rec_crt_usr_id    string,
         |rec_crt_ts        timestamp,
         |rec_upd_usr_id    string ,
         |rec_upd_ts        timestamp ,
         |del_in            string  ,
         |ver_no            int
         |)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/incident/prize/hive_filter_app_det'
         | """.stripMargin)

    println("=======Create hive_filter_app_det successfully ! =======")

  }

  def hive_filter_rule_det(implicit sqlContext: HiveContext) = {
    println("=======Create hive_filter_rule_det =======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_filter_rule_det")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_filter_rule_det(
         |rule_grp_id			int ,
         |rule_grp_cata   string,
         |rule_min_val    string,
         |rule_max_val    string,
         |activity_plat   string,
         |sync_st         string,
         |sync_bat_no     int ,
         |oper_in         string,
         |event_id        int ,
         |rec_id          int ,
         |rec_crt_usr_id  string,
         |rec_crt_ts      timestamp,
         |rec_upd_usr_id  string ,
         |rec_upd_ts      timestamp,
         |del_in          string ,
         |ver_no          int
         |)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/agreement/hive_filter_rule_det'
         | """.stripMargin)

    println("=======Create hive_filter_rule_det successfully ! =======")

  }

  def hive_inf_source_dtl(implicit sqlContext: HiveContext) = {
    println("=======Create hive_inf_source_dtl=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_inf_source_dtl")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_inf_source_dtl(
         |access_id string,
         |access_nm string
         |)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/parameter/hive_inf_source_dtl'
         | """.stripMargin)

    println("=======Create hive_inf_source_dtl successfully ! =======")

  }

  def hive_life_trans(implicit sqlContext: HiveContext) = {
    println("=======Create hive_life_trans=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_life_trans")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_life_trans(
         |settle_dt           timestamp,
         |trans_idx           string,
         |trans_tp            string,
         |trans_class         string,
         |trans_source        string,
         |buss_chnl           string,
         |carrier_tp          string,
         |pri_acct_no         string,
         |mchnt_conn_tp       string,
         |access_tp           string,
         |conn_md             string,
         |acq_ins_id_cd       string,
         |acq_head            string,
         |fwd_ins_id_cd       string,
         |rcv_ins_id_cd       string,
         |iss_ins_id_cd       string,
         |iss_head            string,
         |iss_head_nm         string,
         |mchnt_cd            string,
         |mchnt_nm            string,
         |mchnt_country       string,
         |mchnt_url           string,
         |mchnt_front_url     string,
         |mchnt_back_url      string,
         |mchnt_tp            string,
         |mchnt_order_id      string,
         |mchnt_order_desc    string,
         |mchnt_add_info      string,
         |mchnt_reserve       string,
         |reserve             string,
         |sub_mchnt_cd        string,
         |sub_mchnt_company   string,
         |sub_mchnt_nm        string,
         |mchnt_class         string,
         |sys_tra_no          string,
         |trans_tm            string,
         |sys_tm              string,
         |trans_dt            timestamp,
         |auth_id             string,
         |trans_at            int,
         |trans_curr_cd       string,
         |proc_st             string,
         |resp_cd             string,
         |proc_sys            string,
         |trans_no            string,
         |trans_st            string,
         |conv_dt             string,
         |settle_at           int,
         |settle_curr_cd      string,
         |settle_conv_rt      int,
         |cert_tp             string,
         |cert_id             string,
         |name                string,
         |phone_no            string,
         |usr_id              int,
         |mchnt_id            int,
         |pay_method          string,
         |trans_ip            string,
         |encoding            string,
         |mac_addr            string,
         |card_attr           string,
         |ebank_id            string,
         |ebank_mchnt_cd      string,
         |ebank_order_num     string,
         |ebank_idx           string,
         |ebank_rsp_tm        string,
         |kz_curr_cd          string,
         |kz_conv_rt          int,
         |kz_at               int,
         |delivery_country    int,
         |delivery_province   int,
         |delivery_city       int,
         |delivery_district   int,
         |delivery_street     string,
         |sms_tp              string,
         |sign_method         string,
         |verify_mode         string,
         |accpt_pos_id        string,
         |mer_cert_id         string,
         |cup_cert_id         int,
         |mchnt_version       string,
         |sub_trans_tp        string,
         |mac                 string,
         |biz_tp              string,
         |source_idt          string,
         |delivery_risk       string,
         |trans_flag          string,
         |org_trans_idx       string,
         |org_sys_tra_no      string,
         |org_sys_tm          string,
         |org_mchnt_order_id  string,
         |org_trans_tm        string,
         |org_trans_at        int,
         |req_pri_data        string,
         |pri_data            string,
         |addn_at             string,
         |res_pri_data        string,
         |inq_dtl             string,
         |reserve_fld         string,
         |buss_code           string,
         |t_mchnt_cd          string,
         |is_oversea          string,
         |points_at           int,
         |pri_acct_tp         string,
         |area_cd             string,
         |mchnt_fee_at        int,
         |user_fee_at         int,
         |curr_exp            string,
         |rcv_acct            string,
         |track2              string,
         |track3              string,
         |customer_nm         string,
         |product_info        string,
         |customer_email      string,
         |cup_branch_ins_cd   string,
         |org_trans_dt        timestamp  ,
         |special_calc_cost   string,
         |zero_cost           string,
         |advance_payment     string,
         |new_trans_tp        string,
         |flight_inf          string,
         |md_id               string,
         |ud_id               string,
         |syssp_id            string,
         |card_sn             string,
         |tfr_in_acct         string,
         |acct_id             string,
         |card_bin            string,
         |icc_data            string,
         |icc_data2           string,
         |card_seq_id         string,
         |pos_entry_cd        string,
         |pos_cond_cd         string,
         |term_id             string,
         |usr_num_tp          string,
         |addn_area_cd        string,
         |usr_num             string,
         |reserve1            string,
         |reserve2            string,
         |reserve3            string,
         |reserve4            string,
         |reserve5            string,
         |reserve6            string,
         |rec_st              string,
         |comments            string,
         |to_ts               string,
         |rec_crt_ts          string,
         |rec_upd_ts          string,
         |pay_acct            string,
         |trans_chnl          string,
         |tlr_st              string,
         |rvs_st              string,
         |out_trans_tp        string,
         |org_out_trans_tp    string,
         |bind_id             string,
         |ch_info             string,
         |card_risk_flag      string,
         |trans_step          string,
         |ctrl_msg            string,
         |mchnt_delv_tag      string,
         |mchnt_risk_tag      string,
         |bat_id              string,
         |payer_ip            string,
         |gt_sign_val         string,
         |mchnt_sign_val      string,
         |deduction_at        string,
         |src_sys_flag        string,
         |mac_ip              string,
         |mac_sq              string,
         |trans_ip_num        int   ,
         |cvn_flag            string,
         |expire_flag         string,
         |usr_inf             string,
         |imei                string,
         |iss_ins_tp          string,
         |dir_field           string,
         |buss_tp             string,
         |in_trans_tp         string,
         |buss_tp_nm          string,
         |chnl_tp_nm          string
         |)
         |partitioned by (part_settle_dt string)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/incident/trans/hive_life_trans'
         | """.stripMargin)

    println("=======Create hive_life_trans successfully ! =======")

  }

  def hive_mchnt_inf_wallet(implicit sqlContext: HiveContext) = {
    println("=======Create hive_mchnt_inf_wallet=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_mchnt_inf_wallet")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_mchnt_inf_wallet(
         |mchnt_cd               string,
         |mchnt_cn_abbr          string,
         |mchnt_cn_nm            string,
         |mchnt_en_abbr          string,
         |mchnt_en_nm            string,
         |cntry_cd               string,
         |mchnt_st               string,
         |mchnt_tp               string,
         |region_cd              string,
         |cup_branch_ins_id_cd   string,
         |cup_branch_ins_id_nm   string,
         |frn_acq_ins_id_cd      string,
         |acq_access_ins_id_cd   string,
         |acpt_ins_id_cd         string,
         |mchnt_grp              string,
         |gb_region_cd           string,
         |gb_region_nm           string,
         |acq_ins_id_cd          string,
         |oper_in                string,
         |buss_addr              string,
         |point_conv_in          string,
         |event_id               string,
         |rec_id                 string,
         |rec_upd_usr_id         string,
         |rec_upd_ts             timestamp,
         |rec_crt_ts             timestamp,
         |artif_certif_tp        string,
         |artif_certif_id        string,
         |phone                  string,
         |conn_md                string,
         |settle_ins_tp          string,
         |open_buss_bmp          string
         |)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/participant/mchnt/hive_mchnt_inf_wallet'
         | """.stripMargin)

    println("=======Create hive_mchnt_inf_wallet successfully ! =======")

  }

  def hive_mchnt_tp_grp(implicit sqlContext: HiveContext) = {
    println("=======Create hive_mchnt_tp_grp=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_mchnt_tp_grp")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_mchnt_tp_grp(
         |mchnt_tp_grp						string,
         |mchnt_tp_grp_desc_cn    string,
         |mchnt_tp_grp_desc_en    string,
         |rec_id                  int   ,
         |rec_st                  string,
         |last_oper_in            string,
         |rec_upd_usr_id          string,
         |rec_upd_ts              timestamp,
         |rec_crt_ts              timestamp,
         |sync_st                 string ,
         |sync_bat_no             int    ,
         |sync_ts                 timestamp
         |)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/parameter/hive_mchnt_tp_grp'
         | """.stripMargin)

    println("=======Create hive_mchnt_tp_grp successfully ! =======")

  }

  def hive_org_tdapp_activitynew(implicit sqlContext: HiveContext) = {
    println("=======Create hive_org_tdapp_activitynew=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_org_tdapp_activitynew")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_org_tdapp_activitynew
         |(
         |loguuid            string   ,
         |developerid        int      ,
         |productid          int      ,
         |platformid         int      ,
         |partnerid          int      ,
         |appversion         string   ,
         |tduserid           int      ,
         |mobileid           int      ,
         |channel            int      ,
         |os                 int      ,
         |pixel              string   ,
         |countryid          int      ,
         |provinceid         int      ,
         |isp                int      ,
         |language           string   ,
         |jailbroken         int      ,
         |cracked            int      ,
         |starttime_hour     int      ,
         |starttime_day      int      ,
         |starttime_week     int      ,
         |starttime_month    int      ,
         |starttime_year     int
         |)
         |partitioned by (part_updays string,part_daytime string)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/incident/td/hive_org_tdapp_activitynew'
         | """.stripMargin)

    println("=======Create hive_org_tdapp_activitynew successfully ! =======")

  }

  def hive_org_tdapp_device(implicit sqlContext: HiveContext) = {
    println("=======Create hive_org_tdapp_device=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_org_tdapp_device")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_org_tdapp_device
         |(
         |loguuid              string      ,
         |developerid          int         ,
         |productid            int         ,
         |platformid           int         ,
         |partnerid            int         ,
         |appversion           string      ,
         |tduserid             int         ,
         |eventid              string      ,
         |starttime            bigint      ,
         |starttime_hour       int         ,
         |starttime_day        int         ,
         |starttime_week       int         ,
         |starttime_month      int         ,
         |starttime_year       int
         |)
         |partitioned by (part_updays string,part_daytime string)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/incident/td/hive_org_tdapp_device'
         | """.stripMargin)

    println("=======Create hive_org_tdapp_device successfully ! =======")

  }

  def hive_org_tdapp_devicenew(implicit sqlContext: HiveContext) = {
    println("=======Create hive_org_tdapp_devicenew=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_org_tdapp_devicenew")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_org_tdapp_devicenew
         |(
         |loguuid             string    ,
         |developerid         int       ,
         |productid           int       ,
         |platformid          int       ,
         |partnerid           int       ,
         |appversion          string    ,
         |tduserid            int       ,
         |mobileid            int       ,
         |channel             int       ,
         |os                  int       ,
         |pixel               string    ,
         |countryid           int       ,
         |provinceid          int       ,
         |isp                 int       ,
         |language            string    ,
         |jailbroken          int       ,
         |cracked             int       ,
         |starttime_hour      int       ,
         |starttime_day       int       ,
         |starttime_week      int       ,
         |starttime_month     int       ,
         |starttime_year      int       ,
         |return_status       int
         |)
         |partitioned by (part_updays string,part_daytime string)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/incident/td/hive_org_tdapp_devicenew'
         | """.stripMargin)

    println("=======Create hive_org_tdapp_devicenew successfully ! =======")

  }

  def hive_org_tdapp_eventnew(implicit sqlContext: HiveContext) = {
    println("=======Create hive_org_tdapp_eventnew=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_org_tdapp_eventnew")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_org_tdapp_eventnew
         |(
         |loguuid             string   ,
         |developerid         int      ,
         |productid           int      ,
         |platformid          int      ,
         |partnerid           int      ,
         |appversion          string   ,
         |tduserid            int      ,
         |mobileid            int      ,
         |channel             int      ,
         |os                  int      ,
         |pixel               string   ,
         |countryid           int      ,
         |provinceid          int      ,
         |isp                 int      ,
         |language            string   ,
         |jailbroken          int      ,
         |cracked             int      ,
         |starttime_hour      int      ,
         |starttime_day       int      ,
         |starttime_week      int      ,
         |starttime_month     int      ,
         |starttime_year      int
         |)
         |partitioned by (part_updays string,part_daytime string)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/incident/td/hive_org_tdapp_eventnew'
         | """.stripMargin)

    println("=======Create hive_org_tdapp_eventnew successfully ! =======")

  }


  def hive_org_tdapp_exceptionnew(implicit sqlContext: HiveContext) = {
    println("=======Create hive_org_tdapp_exceptionnew=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_org_tdapp_exceptionnew")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_org_tdapp_exceptionnew
         |(
         |loguuid             string    ,
         |developerid         int       ,
         |productid           int       ,
         |platformid          int       ,
         |partnerid           int       ,
         |appversion          string    ,
         |tduserid            int       ,
         |mobileid            int       ,
         |channel             int       ,
         |os                  int       ,
         |pixel               string    ,
         |countryid           int       ,
         |provinceid          int       ,
         |isp                 int       ,
         |language            string    ,
         |jailbroken          int       ,
         |cracked             int       ,
         |starttime_hour      int       ,
         |starttime_day       int       ,
         |starttime_week      int       ,
         |starttime_month     int       ,
         |starttime_year      int       ,
         |return_status       int
         |)
         |partitioned by (part_updays string,part_daytime string)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/incident/td/hive_org_tdapp_exceptionnew'
         | """.stripMargin)

    println("=======Create hive_org_tdapp_exceptionnew successfully ! =======")

  }

  def hive_org_tdapp_keyvalue(implicit sqlContext: HiveContext) = {
    println("=======Create hive_org_tdapp_keyvalue=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_org_tdapp_keyvalue")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_org_tdapp_keyvalue
         |(
         |loguuid                string     ,
         |developerid            int        ,
         |productid              int        ,
         |platformid             int        ,
         |partnerid              int        ,
         |appversion             string     ,
         |eventid                string     ,
         |label                  string     ,
         |eventcount             int        ,
         |keystring              string     ,
         |value                  string     ,
         |valuenumber            int        ,
         |type                   string     ,
         |starttime              bigint     ,
         |starttime_hour         int        ,
         |starttime_day          int        ,
         |starttime_week         int        ,
         |starttime_month        int        ,
         |starttime_year         int
         |)
         |partitioned by (part_updays string,part_daytime string)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/incident/td/hive_org_tdapp_keyvalue'
         | """.stripMargin)

    println("=======Create hive_org_tdapp_keyvalue successfully ! =======")

  }


  def hive_org_tdapp_tlaunchnew(implicit sqlContext: HiveContext) = {
    println("=======Create hive_org_tdapp_tlaunchnew=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_org_tdapp_tlaunchnew")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_org_tdapp_tlaunchnew
         |(
         |loguuid             string     ,
         |developerid         int        ,
         |productid           int        ,
         |platformid          int        ,
         |partnerid           int        ,
         |appversion          string     ,
         |tduserid            int        ,
         |mobileid            int        ,
         |channel             int        ,
         |os                  int        ,
         |pixel               string     ,
         |countryid           int        ,
         |provinceid          int        ,
         |isp                 int        ,
         |language            string     ,
         |jailbroken          int        ,
         |cracked             int        ,
         |starttime_hour      int        ,
         |starttime_day       int        ,
         |starttime_week      int        ,
         |starttime_month     int        ,
         |starttime_year      int
         |)
         |partitioned by (part_updays string,part_daytime string)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/incident/td/hive_org_tdapp_tlaunchnew'
         | """.stripMargin)

    println("=======Create hive_org_tdapp_tlaunchnew successfully ! =======")

  }

  def hive_preferential_mchnt_inf(implicit sqlContext: HiveContext) = {
    println("=======Create hive_preferential_mchnt_inf=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_preferential_mchnt_inf")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_preferential_mchnt_inf(
         |mchnt_cd                  string,
         |mchnt_nm                  string,
         |mchnt_addr                string,
         |mchnt_phone               string,
         |mchnt_url                 string,
         |mchnt_city_cd             string,
         |mchnt_county_cd           string,
         |mchnt_prov                string,
         |buss_dist_cd              bigint,
         |mchnt_type_id             bigint,
         |cooking_style_id          bigint,
         |rebate_rate               bigint,
         |discount_rate             bigint,
         |avg_consume               int   ,
         |point_mchnt_in            string,
         |discount_mchnt_in         string,
         |preferential_mchnt_in     string,
         |opt_sort_seq              int   ,
         |keywords                  string,
         |mchnt_desc                string,
         |rec_crt_ts                timestamp,
         |rec_upd_ts                timestamp,
         |encr_loc_inf              string ,
         |comment_num               int,
         |favor_num                 int,
         |share_num                 int,
         |park_inf                  string,
         |buss_hour                 string,
         |traffic_inf               string,
         |famous_service            string,
         |comment_value             int,
         |content_id                int,
         |mchnt_st                  string,
         |mchnt_first_para          bigint,
         |mchnt_second_para         bigint,
         |mchnt_longitude           decimal(15,12),
         |mchnt_latitude            decimal(15,12),
         |mchnt_longitude_web       decimal(15,12),
         |mchnt_latitude_web        decimal(15,12),
         |cup_branch_ins_id_cd      string,
         |branch_nm                 string,
         |brand_id                  bigint,
         |buss_bmp                  string,
         |term_diff_store_tp_in     string,
         |rec_id                    bigint,
         |amap_longitude            decimal(15,12),
         |amap_latitude             decimal(15,12),
         |prov_division_cd          string
         |)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/participant/mchnt/hive_preferential_mchnt_inf'
         | """.stripMargin)

    println("=======Create hive_preferential_mchnt_inf successfully ! =======")

  }

  def hive_prize_bas(implicit sqlContext: HiveContext) = {
    println("=======Create hive_prize_bas=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_prize_bas")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_prize_bas(
         |loc_activity_id    int ,
         |prize_tp           string,
         |activity_plat      string,
         |sync_st            string,
         |sync_bat_no        int ,
         |prize_id           int ,
         |prize_st           string,
         |prize_lvl_num      int   ,
         |prize_nm           string,
         |prize_begin_dt     string,
         |prize_end_dt       string,
         |week_tm_bmp        string,
         |oper_in            string,
         |event_id           int ,
         |rec_id             int ,
         |rec_upd_usr_id     string,
         |rec_upd_ts         timestamp,
         |rec_crt_ts         timestamp,
         |rec_crt_usr_id     string,
         |del_in             string,
         |ver_no             int
         |)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/incident/prize/hive_prize_bas'
         | """.stripMargin)

    println("=======Create hive_prize_bas successfully ! =======")

  }

  def hive_signer_log(implicit sqlContext: HiveContext) = {
    println("=======Create hive_signer_log=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_signer_log")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_signer_log(
         |mchnt_cd						 string,
         |term_id              string,
         |cashier_trans_tm     string,
         |pri_acct_no          string,
         |sync_bat_no          string
         |)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/incident/other/hive_signer_log'
         | """.stripMargin)

    println("=======Create hive_signer_log successfully ! =======")

  }

  def hive_ticket_bill_bas_inf(implicit sqlContext: HiveContext) = {
    println("=======Create hive_ticket_bill_bas_inf=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_ticket_bill_bas_inf")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_ticket_bill_bas_inf(
         |bill_id                     string,
         |bill_nm                     string,
         |bill_desc                   string,
         |bill_tp                     string,
         |mchnt_cd                    string,
         |mchnt_nm                    string,
         |affl_id                     string,
         |affl_nm                     string,
         |bill_pic_path               string,
         |acpt_in                     string,
         |enable_in                   string,
         |dwn_total_num               bigint,
         |dwn_num                     bigint,
         |valid_begin_dt              timestamp,
         |valid_end_dt                timestamp,
         |dwn_num_limit               bigint,
         |disc_rate                   int,
         |money_at                    int,
         |low_trans_at_limit          bigint,
         |high_trans_at_limit         bigint,
         |sale_in                     string,
         |bill_price                  bigint,
         |bill_st                     string,
         |oper_action                 string,
         |aud_st                      string,
         |aud_usr_id                  string,
         |aud_ts                      timestamp,
         |aud_idea                    string,
         |rec_upd_usr_id              string,
         |rec_upd_ts                  timestamp,
         |rec_crt_usr_id              string ,
         |rec_crt_ts                  timestamp,
         |ver_no                      int ,
         |bill_related_card_bin       string,
         |event_id                    string,
         |oper_in                     string,
         |sync_st                     string,
         |blkbill_in                  string,
         |chara_grp_cd                string,
         |chara_grp_nm                string,
         |obtain_chnl                 string,
         |cfg_tp                      string,
         |delay_use_days              int   ,
         |bill_rule_bmp               string,
         |cup_branch_ins_id_cd        string,
         |cycle_deduct_in             string,
         |discount_at_max             bigint,
         |input_data_tp               string,
         |temp_use_in                 string,
         |aud_id                      bigint,
         |entry_ins_id_cd             string,
         |entry_ins_cn_nm             string,
         |entry_cup_branch_ins_id_cd  string,
         |entry_cup_branch_nm         string,
         |exclusive_in                string,
         |data_src                    string,
         |bill_original_price         bigint,
         |price_trend_st              string,
         |bill_sub_tp                 string,
         |pay_timeout                 int ,
         |auto_refund_st              string,
         |anytime_refund_st           string,
         |imprest_tp_st               string,
         |rand_tp                     string,
         |expt_val                    int,
         |std_deviation               int,
         |rand_at_min                 int,
         |rand_at_max                 int,
         |disc_max_in                 string,
         |disc_max                    int,
         |rand_period_tp              string,
         |rand_period                 string,
         |cup_branch_ins_id_nm        string
         |)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/product/bill/hive_ticket_bill_bas_inf'
         | """.stripMargin)

    println("=======Create hive_ticket_bill_bas_inf successfully ! =======")

  }

  def hive_undefine_store_inf(implicit sqlContext: HiveContext) = {
    println("=======Create hive_undefine_store_inf=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_undefine_store_inf")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_undefine_store_inf(
         |mchnt_cd          	string,
         |term_id             string,
         |store_cd            string,
         |store_grp_cd        string,
         |brand_id            bigint
         |)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/participant/mchnt/hive_undefine_store_inf'
         | """.stripMargin)

    println("=======Create hive_undefine_store_inf successfully ! =======")

  }

  def hive_undefine_store_inf_temp(implicit sqlContext: HiveContext) = {
    println("=======Create hive_undefine_store_inf_temp=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_undefine_store_inf_temp")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_undefine_store_inf_temp(
         |mchnt_cd          	string,
         |term_id             string,
         |store_cd            string,
         |store_grp_cd        string,
         |brand_id            bigint
         |)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/participant/mchnt/hive_undefine_store_inf_temp'
         | """.stripMargin)

    println("=======Create hive_undefine_store_inf_temp successfully ! =======")

  }
  def hive_user_td_d(implicit sqlContext: HiveContext) = {
    println("=======Create hive_use_td_d=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_user_td_d")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_use_td_d(
         |tduser_id		string,
         |relate_id   string,
         |start_int   bigint,
         |end_int     bigint,
         |start_dt    timestamp,
         |end_dt      timestamp
         |)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/relation/hive_user_td_d'
         | """.stripMargin)

    println("=======Create hive_user_td_d successfully ! =======")

  }

  def hive_bill_order_trans(implicit sqlContext: HiveContext) = {
    println("=======Create hive_bill_order_trans=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_bill_order_trans")

    sqlContext.sql(
      s"""
         create table if not exists $hive_dbname.HIVE_BILL_ORDER_TRANS
         |(
         |bill_order_id          string,
         |mchnt_cd               string,
         |mchnt_nm               string,
         |sub_mchnt_cd           string,
         |cdhd_usr_id            string,
         |sub_mchnt_nm           string,
         |related_usr_id         bigint,
         |cups_trace_number      string,
         |trans_tm               string,
         |trans_dt               timestamp,
         |orig_trans_seq         string,
         |trans_seq              string,
         |mobile_order_id        string,
         |acp_order_id           string,
         |delivery_prov_cd       string,
         |delivery_city_cd       string,
         |delivery_district_nm   string,
         |delivery_district_cd   string,
         |delivery_zip_cd        string,
         |delivery_address       string,
         |receiver_nm            string,
         |receiver_mobile        string,
         |delivery_time_desc     string,
         |invoice_desc           string,
         |trans_at               bigint,
         |refund_at              bigint,
         |order_st               string,
         |order_crt_ts           timestamp,
         |order_timeout_ts       timestamp,
         |card_no                string,
         |order_chnl             string,
         |order_ip               string,
         |device_inf             string,
         |remark                 string,
         |rec_crt_ts             timestamp,
         |crt_cdhd_usr_id        string,
         |rec_upd_ts             timestamp,
         |upd_cdhd_usr_id        string
         |)
         |partitioned by(part_trans_dt string)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/incident/trans/hive_bill_order_trans'
         | """.stripMargin)

    println("=======Create hive_bill_order_trans successfully ! =======")

  }

  def hive_bill_sub_order_trans(implicit sqlContext: HiveContext) = {
    println("=======Create HIVE_BILL_SUB_ORDER_TRANS=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_bill_sub_order_trans")

    sqlContext.sql(
      s"""
         create table if not exists $hive_dbname.HIVE_BILL_SUB_ORDER_TRANS(
         |bill_sub_order_id    bigint          comment '票券子订单编号  ',
         |bill_order_id        string          comment '票券订单编号   ',
         |mchnt_cd             string          comment '商户代码     ',
         |mchnt_nm             string          comment '商户名称     ',
         |sub_mchnt_cd         string          comment '二级商户号    ',
         |sub_mchnt_nm         string          comment '二级商户名称   ',
         |bill_id              string          comment '票券id     ',
         |bill_price           bigint          comment '票券销售价格   ',
         |trans_seq            string          comment '交易流水号(um)',
         |refund_reason        string          comment '退货原因     ',
         |order_st             string          comment '订单状态     ',
         |rec_crt_ts           timestamp       comment '记录创建时间   ',
         |crt_cdhd_usr_id      string          comment '创建用户标识码  ',
         |rec_upd_ts           timestamp       comment '记录更新时间   ',
         |upd_cdhd_usr_id      string          comment '修改用户标识码  ',
         |order_timeout_ts     timestamp       comment '订单超时时    ',
         |trans_dt             timestamp       comment '交易日期     ',
         |related_usr_id       bigint          comment '关联用户id   ',
         |trans_process        string          comment '订单流水     ',
         |response_code        string          comment '交易应答码-1  ',
         |response_msg         string          comment '交易报文     '
         |)
         |partitioned by(part_trans_dt string)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/incident/order/hive_bill_sub_order_trans'
         | """.stripMargin)

    println("=======Create hive_bill_sub_order_trans successfully ! =======")

  }

  def hive_mchnt_tp(implicit sqlContext: HiveContext) = {
    println("=======Create HIVE_MCHNT_TP=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_mchnt_tp")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.HIVE_MCHNT_TP(
         |mchnt_tp          string,
         |mchnt_tp_grp      string,
         |mchnt_tp_desc_cn  string,
         |mchnt_tp_desc_en  string,
         |rec_id            int,
         |rec_st            string,
         |mcc_type          string,
         |last_oper_in      string,
         |rec_upd_usr_id    string,
         |rec_upd_ts        timestamp,
         |rec_crt_ts        timestamp,
         |sync_st           string,
         |sync_bat_no       int,
         |sync_ts           timestamp
         |)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/parameter/hive_mchnt_tp'
         | """.stripMargin)

    println("=======Create hive_mchnt_tp successfully ! =======")

  }

  def hive_offline_point_trans(implicit sqlContext: HiveContext) = {
    println("=======Create HIVE_OFFLINE_POINT_TRANS=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_offline_point_trans")
    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.HIVE_OFFLINE_POINT_TRANS(
         |dtl_seq                      bigint,
         |cdhd_usr_id                  string,
         |pri_acct_no                  string,
         |acpt_ins_id_cd               string,
         |fwd_ins_id_cd                string,
         |sys_tra_no                   string,
         |tfr_dt_tm                    string,
         |card_class                   string,
         |card_attr                    string,
         |card_std                     string,
         |card_media                   string,
         |cups_card_in                 string,
         |cups_sig_card_in             string,
         |trans_id                     string,
         |region_cd                    string,
         |card_bin                     string,
         |mchnt_tp                     string,
         |mchnt_cd                     string,
         |term_id                      string,
         |trans_dt                     timestamp,
         |trans_tm                     string,
         |settle_dt                    timestamp,
         |settle_at                    int,
         |trans_chnl                   string,
         |acpt_term_tp                 string,
         |point_plan_id                int,
         |plan_id                      int,
         |ins_acct_id                  string,
         |point_at                     bigint,
         |oper_st                      string,
         |rule_id                      int,
         |pri_key                      string,
         |ver_no                       int,
         |acct_addup_bat_dt            timestamp,
         |iss_ins_id_cd                string,
         |extra_sp_ins_acct_id         string,
         |extra_point_at               bigint,
         |extend_ins_id_cd             string,
         |cup_branch_ins_id_cd         string,
         |cup_branch_ins_id_nm         string,
         |um_trans_id                  string,
         |buss_tp                      string,
         |bill_id                      string,
         |bill_num                     bigint,
         |oper_dt                      timestamp,
         |tmp_flag                     string,
         |bill_nm                      string,
         |chara_acct_tp                string,
         |chara_acct_nm                string,
         |acct_addup_tp                string,
         |rec_crt_ts                   timestamp,
         |rec_upd_ts                   timestamp,
         |orig_trans_seq               string,
         |notice_on_account            string,
         |orig_tfr_dt_tm               string,
         |orig_sys_tra_no              string,
         |orig_acpt_ins_id_cd          string,
         |orig_fwd_ins_id_cd           string,
         |indirect_trans_in            string,
         |booking_rec_id               bigint,
         |booking_in                   string,
         |plan_nm                      string,
         |plan_give_total_num          bigint,
         |plan_give_limit_tp           string,
         |plan_give_limit              int,
         |day_give_limit               int,
         |give_limit_in                string,
         |retri_ref_no                 string
         |)
         |partitioned by(part_trans_dt string)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/incident/trans/hive_offline_point_trans'
         | """.stripMargin)

    println("=======Create hive_offline_point_trans successfully ! =======")

  }

  def hive_online_point_trans(implicit sqlContext: HiveContext) = {
    println("=======Create hive_online_point_trans=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_online_point_trans")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_online_point_trans(
         |TRANS_ID                     bigint,
         |CDHD_USR_ID                  string,
         |TRANS_TP                     string,
         |BUSS_TP                      string,
         |TRANS_POINT_AT               bigint,
         |CHARA_ACCT_TP                string,
         |BILL_ID                      string,
         |BILL_NUM                     bigint,
         |TRANS_DT                     timestamp,
         |TRANS_TM                     string,
         |VENDOR_ID                    string,
         |REMARK                       string,
         |CARD_NO                      string,
         |STATUS                       string,
         |TERM_TRANS_SEQ               string,
         |ORIG_TERM_TRANS_SEQ          string,
         |MCHNT_CD                     string,
         |TERM_ID                      string,
         |REFUND_TS                    timestamp,
         |ORDER_TP                     string,
         |TRANS_AT                     bigint,
         |SVC_ORDER_ID                 string,
         |TRANS_DTL                    string,
         |EXCH_RATE                    bigint,
         |DISC_AT_POINT                bigint,
         |CDHD_FK                      string,
         |BILL_NM                      string,
         |CHARA_ACCT_NM                string,
         |REC_CRT_TS                   timestamp,
         |TRANS_SEQ                    string,
         |SYS_ERR_CD                   string,
         |BILL_ACQ_MD                  string,
         |CUP_BRANCH_INS_ID_CD         string,
         |CUP_BRANCH_INS_ID_NM         string
         |)
         |partitioned by(part_trans_dt string)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/incident/trans/hive_online_point_trans'
         | """.stripMargin)

    println("=======Create hive_online_point_trans successfully ! =======")

  }

  def hive_prize_activity_bas_inf(implicit sqlContext: HiveContext) = {
    println("=======Create HIVE_PRIZE_ACTIVITY_BAS_INF=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_prize_activity_bas_inf")
    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.HIVE_PRIZE_ACTIVITY_BAS_INF
         |(
         |loc_activity_id        int,
         |activity_plat          string,
         |loc_activity_nm        string,
         |loc_activity_desc      string,
         |activity_begin_dt      timestamp,
         |activity_end_dt        timestamp,
         |week_tm_bmp            string,
         |check_st               string,
         |sync_st                string,
         |sync_bat_no            int,
         |run_st                 string,
         |oper_in                string,
         |event_id               int,
         |rec_id                 int,
         |rec_upd_usr_id         string,
         |rec_upd_ts             timestamp,
         |rec_crt_ts             timestamp,
         |rec_crt_usr_id         string,
         |del_in                 string,
         |aud_usr_id             string,
         |aud_ts                 timestamp,
         |aud_idea               string,
         |activity_st            string,
         |loc_activity_crt_ins   string,
         |cup_branch_ins_id_nm   string,
         |ver_no                 int,
         |activity_tp            string,
         |reprize_limit          string,
         |sms_flag               string,
         |cashier_reward_in      string,
         |mchnt_cd               string
         |)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/incident/prize/hive_prize_activity_bas_inf'
         | """.stripMargin)

    println("=======Create hive_prize_activity_bas_inf successfully ! =======")

  }

  def hive_prize_discount_result(implicit sqlContext: HiveContext) = {
    println("=======Create hive_prize_discount_result=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_prize_discount_result")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.HIVE_PRIZE_DISCOUNT_RESULT
         |(
         |prize_result_seq       int,
         |trans_id               string,
         |sys_tra_no             string,
         |sys_tra_no_conv        string,
         |pri_acct_no            string,
         |trans_at               int,
         |trans_at_conv          int,
         |trans_pos_at           int,
         |trans_dt_tm            string,
         |loc_trans_dt           string,
         |loc_trans_tm           string,
         |settle_dt              timestamp,
         |mchnt_tp               string,
         |acpt_ins_id_cd         string,
         |iss_ins_id_cd          string,
         |acq_ins_id_cd          string,
         |cup_branch_ins_id_cd   string,
         |cup_branch_ins_id_nm   string,
         |mchnt_cd               string,
         |term_id                string,
         |trans_curr_cd          string,
         |trans_chnl             string,
         |prod_in                string,
         |agio_app_id            string,
         |agio_inf               string,
         |prize_app_id           string,
         |prize_id               string,
         |prize_lvl              string,
         |rec_crt_dt             timestamp,
         |is_match_in            string,
         |fwd_ins_id_cd          string,
         |orig_trans_tfr_tm      string,
         |orig_sys_tra_no        string,
         |orig_acpt_ins_id_cd    string,
         |orig_fwd_ins_id_cd     string,
         |sub_card_no            string,
         |is_proced              string,
         |entity_card_no         string,
         |cloud_pay_in           string,
         |card_media             string
         |)
         |partitioned by(part_settle_dt string)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/incident/prize/hive_prize_discount_result'
         | """.stripMargin)

    println("=======Create hive_prize_discount_result successfully ! =======")

  }

  def hive_prize_lvl_add_rule(implicit sqlContext: HiveContext) = {
    println("=======Create hive_prize_lvl_add_rule=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_prize_lvl_add_rule")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_prize_lvl_add_rule
         |(
         |LOC_ACTIVITY_ID    int,
         |ACTIVITY_PLAT      int,
         |LOC_ACTIVITY_NM    bigint,
         |LOC_ACTIVITY_DESC  bigint,
         |ACTIVITY_BEGIN_DT  string,
         |ACTIVITY_END_DT    int,
         |WEEK_TM_BMP        string,
         |CHECK_ST           string,
         |SYNC_ST            bigint,
         |SYNC_BAT_NO        bigint,
         |RUN_ST             bigint,
         |OPER_IN            string
         |)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/agreement/hive_prize_lvl_add_rule'
         | """.stripMargin)

    println("=======Create hive_prize_lvl_add_rule successfully ! =======")

  }

  def hive_prize_lvl(implicit sqlContext: HiveContext) = {
    println("=======Create HIVE_PRIZE_LVL=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_prize_lvl")
    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_prize_lvl
         |(
         |loc_activity_id      int,
         |prize_tp             string,
         |prize_lvl            int,
         |prize_id_lvl         int,
         |activity_plat        string,
         |prize_id             int,
         |run_st               string,
         |sync_st              string,
         |sync_bat_no          int,
         |lvl_prize_num        int,
         |prize_lvl_desc       string,
         |reprize_limit        string,
         |prize_pay_tp         string,
         |cycle_prize_num      int,
         |cycle_span           int,
         |cycle_unit           string,
         |progrs_in            string,
         |seg_prize_num        int,
         |min_prize_trans_at   bigint,
         |max_prize_trans_at   bigint,
         |prize_at             bigint,
         |oper_in              string,
         |event_id             int,
         |rec_id               int,
         |rec_upd_usr_id       string,
         |rec_upd_ts           timestamp,
         |rec_crt_ts           timestamp,
         |ver_no               int
         |)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/incident/prize/hive_prize_lvl'
         | """.stripMargin)

    println("=======Create hive_prize_lvl successfully ! =======")

  }

  def hive_store_term_relation(implicit sqlContext: HiveContext) = {
    println("=======Create hive_store_term_relation=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_store_term_relation")
    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_store_term_relation(
         |rec_id                bigint,
         |mchnt_cd              string,
         |term_id               string,
         |third_party_ins_fk    int,
         |rec_upd_usr_id        string,
         |rec_upd_ts            timestamp,
         |rec_crt_usr_id        string,
         |rec_crt_ts            timestamp,
         |third_party_ins_id    string,
         |is_trans_tp           string
         |)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/relation/hive_store_term_relation'
         | """.stripMargin)

    println("=======Create hive_store_term_relation successfully ! =======")

  }

  def hive_term_inf(implicit sqlContext: HiveContext) = {
    println("=======Create HIVE_TERM_INF=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_term_inf")
    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_term_inf(
         |mchnt_cd        string     ,
         |term_id         string     ,
         |term_tp         string     ,
         |term_st         string     ,
         |open_dt         timestamp  ,
         |close_dt        timestamp  ,
         |bank_cd         string     ,
         |rec_st          string     ,
         |last_oper_in    string     ,
         |event_id        int        ,
         |rec_id          int        ,
         |rec_upd_usr_id  string     ,
         |rec_upd_ts      timestamp  ,
         |rec_crt_ts      timestamp  ,
         |is_trans_at_tp  string
         |)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/participant/other/hive_term_inf'
      """.stripMargin)

    println("=======Create hive_term_inf successfully ! =======")

  }

  def hive_ach_order_inf(implicit sqlContext: HiveContext) = {
    println("=======Create Table hive_ach_order_inf  =======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_ach_order_inf")
    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname .hive_ach_order_inf(
         |order_id             string ,
         |sys_no               string ,
         |mchnt_version        string ,
         |encoding             string ,
         |sign_method          string ,
         |mchnt_trans_tp       string ,
         |biz_tp               string ,
         |pay_method           string ,
         |trans_tp             string ,
         |buss_chnl            string ,
         |mchnt_front_url      string ,
         |mchnt_back_url       string ,
         |acq_ins_id_cd        string ,
         |mchnt_cd             string ,
         |mchnt_tp             string ,
         |mchnt_nm             string ,
         |sub_mchnt_cd         string ,
         |sub_mchnt_nm         string ,
         |mchnt_order_id       string ,
         |trans_tm             string ,
         |trans_dt             timestamp,
         |sys_tm               string ,
         |pay_timeout          string ,
         |trans_at             string ,
         |trans_curr_cd        string ,
         |kz_at                string ,
         |kz_curr_cd           string ,
         |conv_dt              string ,
         |deduct_at            string ,
         |discount_info        string ,
         |upoint_at            string ,
         |top_info             string ,
         |refund_at            string ,
         |iss_ins_id_cd        string ,
         |iss_head             string ,
         |pri_acct_no          string ,
         |card_attr            string ,
         |usr_id               string ,
         |phone_no             string ,
         |trans_ip             string ,
         |trans_st             string ,
         |trans_no             string ,
         |trans_idx            string ,
         |sys_tra_no           string ,
         |order_desc           string ,
         |order_detail         string ,
         |proc_sys             string ,
         |proc_st              string ,
         |trans_source         string ,
         |resp_cd              string ,
         |other_usr            string ,
         |initial_pay          string ,
         |to_ts                string ,
         |rec_crt_ts           string ,
         |rec_upd_ts           string
         |)
         |partitioned by (part_hp_trans_dt string,part_trans_dt string)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/incident/order/hive_ach_order_inf'
      """.stripMargin)

    println("=======Create hive_ach_order_inf successfully ! =======")

  }

  def hive_bill_order_aux_inf(implicit sqlContext: HiveContext) = {
    println("=======Create Table hive_bill_order_aux_inf  =======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_bill_order_aux_inf")
    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_bill_order_aux_inf(
         |bill_order_id            string ,
         |mchnt_cd                 string ,
         |mchnt_nm                 string ,
         |sub_mchnt_cd             string ,
         |cdhd_usr_id              string ,
         |sub_mchnt_nm             string ,
         |related_usr_id           bigint ,
         |cups_trace_number        string ,
         |trans_tm                 string ,
         |trans_dt                 string ,
         |orig_trans_seq           string ,
         |trans_seq                string ,
         |mobile_order_id          string ,
         |acp_order_id             string ,
         |delivery_prov_cd         string ,
         |delivery_city_cd         string ,
         |delivery_district_cd     string ,
         |delivery_zip_cd          string ,
         |delivery_address         string ,
         |receiver_nm              string ,
         |receiver_mobile          string ,
         |delivery_time_desc       string ,
         |invoice_desc             string ,
         |trans_at                 bigint ,
         |refund_at                bigint ,
         |order_st                 string ,
         |order_crt_ts             timestamp,
         |order_timeout_ts         timestamp,
         |card_no                  string,
         |order_chnl               string,
         |order_ip                 string,
         |device_inf               string,
         |remark                   string,
         |rec_crt_ts               timestamp,
         |crt_cdhd_usr_id          string ,
         |rec_upd_ts               timestamp,
         |upd_cdhd_usr_id          string
         |
        |)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/incident/order/hive_bill_order_aux_inf'
      """.stripMargin)

    println("=======Create hive_bill_order_aux_inf successfully ! =======")

  }

  def hive_bill_sub_order_detail_inf(implicit sqlContext: HiveContext) = {
    println("=======Create Table hive_bill_sub_order_detail_inf for JOB_HV_73 by TZQ =======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_bill_sub_order_detail_inf")
    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_bill_sub_order_detail_inf(
         |bill_sub_order_id   bigint,
         |bill_order_id       string,
         |mchnt_cd            string,
         |mchnt_nm            string,
         |sub_mchnt_cd        string,
         |sub_mchnt_nm        string,
         |bill_id             string,
         |bill_price          bigint,
         |trans_seq           string,
         |refund_reason       string,
         |order_st            string,
         |rec_crt_ts          timestamp ,
         |crt_cdhd_usr_id     string ,
         |rec_upd_ts          timestamp ,
         |upd_cdhd_usr_id     string,
         |order_timeout_ts    timestamp,
         |trans_dt            string,
         |related_usr_id      bigint,
         |trans_process       string,
         |response_code       string,
         |response_msg        string
         |)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/incident/order/hive_bill_sub_order_detail_inf'
      """.stripMargin)

    println("=======Create hive_bill_sub_order_detail_inf successfully ! =======")

  }

  def hive_ticket_bill_acct_adj_task(implicit sqlContext: HiveContext) = {
    println("=======Create Table hive_ticket_bill_acct_adj_task =======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_ticket_bill_acct_adj_task")
    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_ticket_bill_acct_adj_task(
         |task_id                 int ,
         |task_tp                 string,
         |usr_tp                  string,
         |adj_ticket_bill         int ,
         |usr_id                  string,
         |bill_id                 string,
         |proc_usr_id             string,
         |crt_ts                  timestamp,
         |aud_usr_id              string ,
         |aud_ts                  timestamp,
         |aud_idea                string,
         |current_st              string,
         |file_path               string,
         |file_nm                 string,
         |result_file_path        string,
         |result_file_nm          string,
         |remark                  string,
         |ver_no                  int   ,
         |chk_usr_id              string,
         |chk_ts                  timestamp,
         |chk_idea                string ,
         |cup_branch_ins_id_cd    string,
         |adj_rsn_cd              string,
         |rec_upd_ts              timestamp,
         |rec_crt_ts              timestamp,
         |card_no                 string ,
         |rec_crt_usr_id          string ,
         |acc_resp_cd             string ,
         |acc_err_msg             string ,
         |entry_ins_id_cd         string ,
         |entry_ins_cn_nm         string
         |
         |)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/incident/other/hive_ticket_bill_acct_adj_task'
      """.stripMargin)

    println("=======Create hive_ticket_bill_acct_adj_task successfully ! =======")

  }

  def hive_search_trans (implicit sqlContext: HiveContext) = {
    println("=======Create Table hive_search_trans for JOB_HV_27 by XTP =======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_search_trans")
    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_search_trans
         |(
         |tfr_dt_tm	string,
         |sys_tra_no	string,
         |acpt_ins_id_cd	string,
         |fwd_ins_id_cd	string,
         |pri_key1	string,
         |fwd_chnl_head	string,
         |chswt_plat_seq	bigint,
         |trans_tm	string,
         |trans_dt	timestamp,
         |cswt_settle_dt	timestamp,
         |internal_trans_tp	string,
         |settle_trans_id	string,
         |trans_tp	string,
         |cups_settle_dt	string,
         |msg_tp	string,
         |pri_acct_no	string,
         |card_bin	string,
         |proc_cd	string,
         |req_trans_at	int,
         |resp_trans_at	int,
         |trans_curr_cd	string,
         |trans_tot_at	int,
         |iss_ins_id_cd	string,
         |launch_trans_tm	string,
         |launch_trans_dt	string,
         |mchnt_tp	string,
         |pos_entry_md_cd	string,
         |card_seq_id	string,
         |pos_cond_cd	string,
         |pos_pin_capture_cd	string,
         |retri_ref_no	string,
         |term_id	string,
         |mchnt_cd	string,
         |card_accptr_nm_loc	string,
         |sec_related_ctrl_inf	string,
         |orig_data_elemts	string,
         |rcv_ins_id_cd	string,
         |fwd_proc_in	string,
         |rcv_proc_in	string,
         |proj_tp	string,
         |usr_id	string,
         |conv_usr_id	string,
         |trans_st	string,
         |inq_dtl_req	string,
         |inq_dtl_resp	string,
         |iss_ins_resv	string,
         |ic_flds	string,
         |cups_def_fld	string,
         |id_no	string,
         |cups_resv	string,
         |acpt_ins_resv	string,
         |rout_ins_id_cd	string,
         |sub_rout_ins_id_cd	string,
         |recv_access_resp_cd	string,
         |chswt_resp_cd	string,
         |chswt_err_cd	string,
         |resv_fld1	string,
         |resv_fld2	string,
         |to_ts	timestamp,
         |rec_upd_ts	timestamp,
         |rec_crt_ts	timestamp,
         |settle_at	int,
         |external_amt	int,
         |discount_at	int,
         |card_pay_at	int,
         |right_purchase_at	int,
         |recv_second_resp_cd	string,
         |req_acpt_ins_resv	string,
         |log_id	string,
         |conv_acct_no	string,
         |inner_pro_ind	string,
         |acct_proc_in	string,
         |order_id	string,
         |seq_id	bigint,
         |oper_module	string,
         |um_trans_id	string,
         |cdhd_fk	string,
         |bill_id	string,
         |bill_tp	string,
         |bill_bat_no	string,
         |bill_inf	string,
         |card_no	string,
         |trans_at	int,
         |settle_curr_cd	string,
         |card_accptr_local_tm	string,
         |card_accptr_local_dt	string,
         |expire_dt	string,
         |msg_settle_dt	timestamp,
         |auth_id_resp_cd	string,
         |resp_cd	string,
         |notify_st	string,
         |addn_private_data	string,
         |udf_fld	string,
         |addn_at	string,
         |acct_id_1	string,
         |acct_id_2	string,
         |resv_fld	string,
         |cdhd_auth_inf	string,
         |recncl_in	string,
         |match_in	string,
         |trans_proc_start_ts	timestamp,
         |trans_proc_end_ts	timestamp,
         |sys_det_cd	string,
         |sys_err_cd	string,
         |dtl_inq_data	string
         |)
         |partitioned by(part_msg_settle_dt string)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/incident/trans/hive_search_trans'
      """.stripMargin)

    println("=======Create hive_search_trans successfully ! =======")

  }


  def hive_buss_dist(implicit sqlContext: HiveContext) = {
    println("=======Create Table hive_buss_dist for JOB_HV_34 by XTP =======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_buss_dist")
    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_buss_dist
         |(
         |rec_id                    string          comment '记录编码     ',
         |chara_acct_tp             string          comment '商圈代码     ',
         |chara_acct_attr           string          comment '特征账户属性   ',
         |chara_acct_desc           string          comment '特征账户描述   ',
         |entry_grp_cd              string          comment '输入组代码    ',
         |entry_blkbill_in          string          comment '输入黑白名单标识 ',
         |output_grp_cd             string          comment '输出组代码    ',
         |output_blkbill_in         string          comment '输出黑白名单标识 ',
         |chara_acct_st             string          comment '特征账户状态   ',
         |aud_usr_id                string          comment '审核人编号    ',
         |aud_idea                  string          comment '审核意见     ',
         |aud_ts                    timestamp       comment '审核时间     ',
         |aud_in                    string          comment '审核标识     ',
         |oper_action               string          comment '操作动作     ',
         |rec_crt_ts                timestamp       comment '记录创建时间   ',
         |rec_upd_ts                timestamp       comment '记录修改时间   ',
         |rec_crt_usr_id            string          comment '记录创建用户标识 ',
         |rec_upd_usr_id            string          comment '记录修改用户标识 ',
         |ver_no                    int             comment '版本编号     ',
         |chara_acct_nm             string          comment '商圈名称     ',
         |ch_ins_id_cd              string          comment '机构id     ',
         |um_ins_tp                 string          comment '机构类型     ',
         |ins_nm                    string          comment '机构名称     ',
         |oper_in                   string          comment '操作标志     ',
         |event_id                  int             comment '事件号      ',
         |sync_st                   string          comment '同步状态     ',
         |cup_branch_ins_id_cd      string          comment '所属银联分公司代码',
         |scene_usage_in            int             comment '场景使用标识   '
         |)
         |comment '商圈信息表'
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/region/hive_buss_dist'
      """.stripMargin)

    println("=======Create hive_buss_dist successfully ! =======")

  }

  def hive_cdhd_bill_acct_inf(implicit sqlContext: HiveContext) = {
    println("=======Create Table hive_cdhd_bill_acct_inf for JOB_HV_35 by XTP =======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_cdhd_bill_acct_inf")
    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_cdhd_bill_acct_inf
         |(
         |seq_id                     bigint               comment '序列ID    ',
         |cdhd_usr_id                string               comment '持卡人用户标识码',
         |cdhd_fk                    string               comment '持卡人外键   ',
         |bill_id                    string               comment '票券ID    ',
         |bill_bat_no                string               comment '票券批次号   ',
         |bill_tp                    string               comment '票券类型    ',
         |mem_nm                     string               comment '会员姓名    ',
         |bill_num                   bigint               comment '票券数量    ',
         |usage_num                  bigint               comment '使用次数    ',
         |acct_st                    string               comment '账户状态    ',
         |rec_crt_ts                 timestamp            comment '记录创建时间  ',
         |rec_upd_ts                 timestamp            comment '记录更新时间  ',
         |ver_no                     int                  comment '版本编号    ',
         |bill_related_card_no       string               comment '票券关联卡号  ',
         |scene_id                   string               comment '场景标识    ',
         |freeze_bill_num            bigint               comment '冻结票券数量  '
         |)
         |comment '持卡人票券账户表'
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/product/bill/hive_cdhd_bill_acct_inf'
      """.stripMargin)

    println("=======Create hive_cdhd_bill_acct_inf successfully ! =======")

  }


  def hive_cashier_bas_inf(implicit sqlContext: HiveContext) = {
    println("=======Create Table hive_cashier_bas_inf for JOB_HV_54 by XTP =======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_cashier_bas_inf")
    sqlContext.sql(
      s"""
         create table if not exists $hive_dbname.hive_cashier_bas_inf(
         |cashier_usr_id	string            ,
         |reg_dt	timestamp                 ,
         |usr_nm	string                    ,
         |real_nm	string                    ,
         |real_nm_st	string                ,
         |certif_id	string                ,
         |certif_vfy_st	string            ,
         |bind_card_no	string            ,
         |card_auth_st	string            ,
         |bind_card_ts	timestamp         ,
         |mobile	string                    ,
         |mobile_vfy_st	string            ,
         |email_addr	string                ,
         |email_vfy_st	string            ,
         |mchnt_cd	string                ,
         |mchnt_nm	string                ,
         |gb_region_cd	string            ,
         |comm_addr	string                ,
         |zip_cd	string                    ,
         |industry_id	string                ,
         |hobby	string                    ,
         |cashier_lvl	string                ,
         |login_greeting	string            ,
         |pwd_cue_ques	string            ,
         |pwd_cue_answ	string            ,
         |usr_st	string                    ,
         |inf_source	string                ,
         |cup_branch_ins_id_nm	string    ,
         |rec_crt_ts	timestamp             ,
         |rec_upd_ts	timestamp             ,
         |mobile_new	string                ,
         |email_addr_new	string            ,
         |activate_ts	timestamp             ,
         |activate_pwd	string            ,
         |region_cd	string                ,
         |last_sign_in_ts	timestamp         ,
         |ver_no	int                       ,
         |birth_dt	string                ,
         |sex	string                        ,
         |master_in	string
         |)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/participant/ins/hive_cashier_bas_inf'
      """.stripMargin)

    println("=======Create hive_cashier_bas_inf successfully ! =======")

  }

  def hive_access_static_inf(implicit sqlContext: HiveContext) = {
    println("=======Create Table hive_access_static_inf for JOB_HV_75 by XTP =======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_access_static_inf")
    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_access_static_inf
         |(
         |access_ins_id_cd	string   ,
         |access_ins_abbr_cd	string   ,
         |access_ins_nm	string       ,
         |access_sys_cd	string       ,
         |access_ins_tp	string       ,
         |access_bitmap	string       ,
         |trans_rcv_priv_bmp	string   ,
         |trans_snd_priv_bmp	string   ,
         |enc_key_index	string       ,
         |mac_algo	string           ,
         |mak_len	string               ,
         |pin_algo	string           ,
         |pik_len	string               ,
         |enc_rsa_key_seq	string       ,
         |rsa_enc_key_len	string       ,
         |resv_fld	string           ,
         |mchnt_lvl	int              ,
         |valid_in	string           ,
         |event_id	int              ,
         |oper_in	string               ,
         |rec_id	int                  ,
         |rec_upd_usr_id	string       ,
         |rec_crt_ts	timestamp        ,
         |rec_upd_ts	timestamp
         |)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/participant/ins/hive_access_static_inf'
      """.stripMargin)

    println("=======Create hive_access_static_inf successfully ! =======")

  }

  def hive_region_cd(implicit sqlContext: HiveContext) = {
    println("=======Create Table hive_region_cd for JOB_HV_76 by XTP =======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_region_cd")
    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_region_cd
         |(
         |region_cd	string                ,
         |region_cn_nm	string            ,
         |region_en_nm	string            ,
         |prov_region_cd	string            ,
         |city_region_cd	string            ,
         |cntry_region_cd	string            ,
         |region_lvl	string                ,
         |oper_in	string                    ,
         |event_id	int                   ,
         |rec_id	int                       ,
         |rec_upd_usr_id	string            ,
         |rec_upd_ts	timestamp             ,
         |rec_crt_ts	timestamp             ,
         |td_cup_branch_ins_id_nm	string    ,
         |cup_branch_ins_id_nm	string
         |)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/region/hive_region_cd'
      """.stripMargin)

    println("=======Create hive_region_cd successfully ! =======")

  }

  def hive_aconl_ins_bas(implicit sqlContext: HiveContext) = {
    println("=======Create Table hive_region_cd for JOB_HV_79 by XTP =======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_aconl_ins_bas")
    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_aconl_ins_bas
         |    (
         |      ins_id_cd              string  ,
         |      ins_cata               string  ,
         |      ins_tp                 string  ,
         |      hdqrs_ins_id_cd        string  ,
         |      cup_branch_ins_id_cd   string  ,
         |      region_cd              string  ,
         |      area_cd                string  ,
         |      ins_cn_nm              string  ,
         |      ins_cn_abbr            string  ,
         |      ins_en_nm              string  ,
         |      ins_en_abbr            string  ,
         |      rec_st                 string  ,
         |      rec_upd_ts             timestamp  ,
         |      rec_crt_ts             timestamp  ,
         |      comments               string  ,
         |      oper_in                string  ,
         |      event_id               int  ,
         |      rec_id                 int  ,
         |      rec_upd_usr_id         string ,
         |	  cup_branch_ins_id_nm   string
         |    )
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/participant/ins/hive_aconl_ins_bas'
      """.stripMargin)

    println("=======Create hive_aconl_ins_bas successfully ! =======")

  }

  def hive_org_tdapp_tactivity(implicit sqlContext: HiveContext) = {
    println("=======Create hive_org_tdapp_tactivity=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_org_tdapp_tactivity")
    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_org_tdapp_tactivity
         |(
         |loguuid              string     ,
         |developerid          int        ,
         |productid            int        ,
         |platformid           int        ,
         |partnerid            int        ,
         |appversion           string     ,
         |tduserid             int        ,
         |refpagenameid        int        ,
         |pagenameid           int        ,
         |duration             int        ,
         |sessionid            string     ,
         |starttime            bigint     ,
         |starttime_hour       int        ,
         |starttime_day        int        ,
         |starttime_week       int        ,
         |starttime_month      int        ,
         |starttime_year       int
         |)
         |partitioned by (part_updays string,part_daytime string)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/incident/td/hive_org_tdapp_tactivity'
         |
         | """.stripMargin)

    println("=======Create hive_org_tdapp_tactivity successfully ! =======")

  }

  def hive_org_tdapp_tlaunch(implicit sqlContext: HiveContext) = {
    println("=======Create hive_org_tdapp_tlaunch=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_org_tdapp_tlaunch")
    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_org_tdapp_tlaunch(
         |loguuid						string,
         |developerid       int   ,
         |productid         int   ,
         |platformid        int   ,
         |partnerid         int   ,
         |appversion        string,
         |tduserid          int   ,
         |mobileid          int   ,
         |channel           int   ,
         |os                int   ,
         |pixel             string,
         |countryid         int   ,
         |provinceid        int   ,
         |isp               int   ,
         |language          string,
         |jailbroken        int   ,
         |cracked           int   ,
         |sessionid         string,
         |session_duration  int   ,
         |interval_level    int   ,
         |starttime         bigint,
         |starttime_hour    int   ,
         |starttime_day     int   ,
         |starttime_week    int   ,
         |starttime_month   int   ,
         |starttime_year    int
         |
         |)
         |partitioned by (part_updays string,part_daytime string)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/incident/td/hive_org_tdapp_tlaunch'
         |
         | """.stripMargin)

    println("=======Create hive_org_tdapp_tlaunch successfully ! =======")

  }

  def hive_org_tdapp_terminate(implicit sqlContext: HiveContext) = {
    println("=======Create hive_org_tdapp_terminate=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_org_tdapp_terminate")
    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_org_tdapp_terminate(
         |loguuid          string,
         |developerid      int   ,
         |productid        int   ,
         |platformid       int   ,
         |partnerid        int   ,
         |appversion       string,
         |devid            string,
         |sessionid        string,
         |session_duration int   ,
         |usetime_level    int   ,
         |starttime        bigint,
         |starttime_hour   int   ,
         |starttime_day    int   ,
         |starttime_week   int   ,
         |starttime_month  int   ,
         |starttime_year   int
         |)
         |partitioned by (part_updays string,part_daytime string)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/incident/td/hive_org_tdapp_terminate'
         |
         | """.stripMargin)

    println("=======Create hive_org_tdapp_terminate successfully ! =======")

  }

   def hive_org_tdapp_exception(implicit sqlContext: HiveContext) = {
    println("=======Create hive_org_tdapp_exception=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_org_tdapp_exception")
    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_org_tdapp_exception
         |(
         |loguuid              string    ,
         |developerid          int       ,
         |productid            int       ,
         |platformid           int       ,
         |appversion           string    ,
         |tduserid             int       ,
         |os                   int       ,
         |osversion            string    ,
         |mobileid             int       ,
         |errorname            string    ,
         |errormessage         string    ,
         |errcount             int       ,
         |hashcode             string    ,
         |starttime            bigint    ,
         |starttime_hour       int       ,
         |starttime_day        int       ,
         |starttime_week       int       ,
         |starttime_month      int       ,
         |starttime_year       int
         |)
         |partitioned by (part_updays string,part_daytime string)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/incident/td/hive_org_tdapp_exception'
         | """.stripMargin)

    println("=======Create hive_org_tdapp_exception successfully ! =======")

  }

  def hive_org_tdapp_newuser(implicit sqlContext: HiveContext) = {
    println("=======Create hive_org_tdapp_newuser=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_org_tdapp_newuser")
    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_org_tdapp_newuser
         |(
         |loguuid           string    ,
         |developerid       int       ,
         |productid         int       ,
         |platformid        int       ,
         |partnerid         int       ,
         |appversion        string    ,
         |tduserid          int       ,
         |mobileid          int       ,
         |channel           int       ,
         |os                int       ,
         |pixel             string    ,
         |countryid         int       ,
         |provinceid        int       ,
         |isp               int       ,
         |language          string    ,
         |jailbroken        int       ,
         |cracked           int       ,
         |starttime_hour    int       ,
         |starttime_day     int       ,
         |starttime_week    int       ,
         |starttime_month   int       ,
         |starttime_year    int
         |)
         |partitioned by (part_updays string,part_daytime string,from_table string)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/incident/td/hive_org_tdapp_newuser'
         | """.stripMargin)
    println("=======Create hive_org_tdapp_newuser successfully ! =======")

  }


    def hive_trans_dtl(implicit sqlContext: HiveContext) = {
      println("=======Create Table:  hive_trans_dtl=======")
      sqlContext.sql(s"use $hive_dbname")
      sqlContext.sql("drop table if exists hive_trans_dtl")

      sqlContext.sql(
        s"""
           |create table if not exists $hive_dbname.hive_trans_dtl(
           |seq_id                string    ,
           |cdhd_usr_id           string    ,
           |card_no               string    ,
           |trans_tfr_tm          string    ,
           |sys_tra_no            string    ,
           |acpt_ins_id_cd        string    ,
           |fwd_ins_id_cd         string    ,
           |rcv_ins_id_cd         string    ,
           |oper_module           string    ,
           |trans_dt              string    ,
           |trans_tm              string    ,
           |buss_tp               string    ,
           |um_trans_id           string    ,
           |swt_right_tp          string    ,
           |bill_id               string    ,
           |bill_nm               string    ,
           |chara_acct_tp         string    ,
           |trans_at              string    ,
           |point_at              bigint    ,
           |mchnt_tp              string    ,
           |resp_cd               string    ,
           |card_accptr_term_id   string    ,
           |card_accptr_cd        string    ,
           |trans_proc_start_ts   timestamp ,
           |trans_proc_end_ts     timestamp ,
           |sys_det_cd            string    ,
           |sys_err_cd            string    ,
           |rec_upd_ts            timestamp ,
           |chara_acct_nm         string    ,
           |void_trans_tfr_tm     string    ,
           |void_sys_tra_no       string    ,
           |void_acpt_ins_id_cd   string    ,
           |void_fwd_ins_id_cd    string    ,
           |orig_data_elemnt      string    ,
           |rec_crt_ts            timestamp ,
           |discount_at           string    ,
           |bill_item_id          string    ,
           |chnl_inf_index        int       ,
           |bill_num              bigint    ,
           |addn_discount_at      string    ,
           |pos_entry_md_cd       string    ,
           |udf_fld               string    ,
           |card_accptr_nm_addr   string
           |)
           |partitioned by (part_trans_dt string)
           |row format delimited fields terminated by '!|'
           |stored as parquet
           |location '/user/ch_hypas/upw_hive/incident/trans/hive_trans_dtl'
           | """.stripMargin
      )
      println("=======Create hive_trans_dtl successfully ! =======")

    }

    def hive_trans_log(implicit sqlContext: HiveContext) = {
      println("=======Create Table: hive_trans_log=======")
      sqlContext.sql(s"use $hive_dbname")
      sqlContext.sql("drop table if exists hive_trans_log")

      sqlContext.sql(
        s"""
           |create table if not exists $hive_dbname.hive_trans_log(
           |seq_id                  bigint      ,
           |trans_tfr_tm            string      ,
           |sys_tra_no              string      ,
           |acpt_ins_id_cd          string      ,
           |fwd_ins_id_cd           string      ,
           |rcv_ins_id_cd           string      ,
           |oper_module             string      ,
           |um_trans_id             string      ,
           |msg_tp                  string      ,
           |cdhd_fk                 string      ,
           |bill_id                 string      ,
           |bill_tp                 string      ,
           |bill_bat_no             string      ,
           |bill_inf                string      ,
           |card_no                 string      ,
           |proc_cd                 string      ,
           |trans_at                string      ,
           |trans_curr_cd           string      ,
           |settle_at               string      ,
           |settle_curr_cd          string      ,
           |card_accptr_local_tm    string      ,
           |card_accptr_local_dt    string      ,
           |expire_dt               string      ,
           |msg_settle_dt           string      ,
           |mchnt_tp                string      ,
           |pos_entry_md_cd         string      ,
           |pos_cond_cd             string      ,
           |pos_pin_capture_cd      string      ,
           |retri_ref_no            string      ,
           |auth_id_resp_cd         string      ,
           |resp_cd                 string      ,
           |notify_st               string      ,
           |card_accptr_term_id     string      ,
           |card_accptr_cd          string      ,
           |card_accptr_nm_addr     string      ,
           |addn_private_data       string      ,
           |udf_fld                 string      ,
           |addn_at                 string      ,
           |orig_data_elemnt        string      ,
           |acct_id_1               string      ,
           |acct_id_2               string      ,
           |resv_fld                string      ,
           |cdhd_auth_inf           string      ,
           |sys_settle_dt           string      ,
           |recncl_in               string      ,
           |match_in                string      ,
           |trans_proc_start_ts     timestamp   ,
           |trans_proc_end_ts       timestamp   ,
           |sys_det_cd              string      ,
           |sys_err_cd              string      ,
           |sec_ctrl_inf            string      ,
           |card_seq                string      ,
           |rec_upd_ts              timestamp   ,
           |dtl_inq_data            string
           |)
           |partitioned by (part_msg_settle_dt string)
           |row format delimited fields terminated by '!|'
           |stored as parquet
           |location '/user/ch_hypas/upw_hive/incident/trans/hive_trans_log'
           | """.stripMargin
      )

      println("=======Create hive_trans_log successfully ! =======")

    }


    def hive_swt_log(implicit sqlContext: HiveContext) = {
      println("=======Create Table: hive_swt_log=======")
      sqlContext.sql(s"use $hive_dbname")
      sqlContext.sql("drop table if exists hive_swt_log")

      sqlContext.sql(
        s"""
           |create table if not exists $hive_dbname.hive_swt_log(
           |tfr_dt_tm                string       ,
           |sys_tra_no               string       ,
           |acpt_ins_id_cd           string       ,
           |msg_fwd_ins_id_cd        string       ,
           |pri_key1                 string       ,
           |fwd_chnl_head            string       ,
           |chswt_plat_seq           bigint       ,
           |trans_tm                 string       ,
           |trans_dt                 string       ,
           |cswt_settle_dt           string       ,
           |internal_trans_tp        string       ,
           |settle_trans_id          string       ,
           |trans_tp                 string       ,
           |cups_settle_dt           string       ,
           |msg_tp                   string       ,
           |pri_acct_no              string       ,
           |card_bin                 string       ,
           |proc_cd                  string       ,
           |req_trans_at             bigint       ,
           |resp_trans_at            bigint       ,
           |trans_curr_cd            string       ,
           |trans_tot_at             bigint       ,
           |iss_ins_id_cd            string       ,
           |launch_trans_tm          string       ,
           |launch_trans_dt          string       ,
           |mchnt_tp                 string       ,
           |pos_entry_md_cd          string       ,
           |card_seq_id              string       ,
           |pos_cond_cd              string       ,
           |pos_pin_capture_cd       string       ,
           |retri_ref_no             string       ,
           |term_id                  string       ,
           |mchnt_cd                 string       ,
           |card_accptr_nm_loc       string       ,
           |sec_related_ctrl_inf     string       ,
           |orig_data_elemts         string       ,
           |rcv_ins_id_cd            string       ,
           |fwd_proc_in              string       ,
           |rcv_proc_in              string       ,
           |proj_tp                  string       ,
           |usr_id                   string       ,
           |conv_usr_id              string       ,
           |trans_st                 string       ,
           |inq_dtl_req              string       ,
           |inq_dtl_resp             string       ,
           |iss_ins_resv             string       ,
           |ic_flds                  string       ,
           |cups_def_fld             string       ,
           |id_no                    string       ,
           |cups_resv                string       ,
           |acpt_ins_resv            string       ,
           |rout_ins_id_cd           string       ,
           |sub_rout_ins_id_cd       string       ,
           |recv_access_resp_cd      string       ,
           |chswt_resp_cd            string       ,
           |chswt_err_cd             string       ,
           |resv_fld1                string       ,
           |resv_fld2                string       ,
           |to_ts                    timestamp    ,
           |rec_upd_ts               timestamp    ,
           |rec_crt_ts               timestamp    ,
           |settle_at                bigint       ,
           |external_amt             bigint       ,
           |discount_at              bigint       ,
           |card_pay_at              bigint       ,
           |right_purchase_at        bigint       ,
           |recv_second_resp_cd      string       ,
           |req_acpt_ins_resv        string       ,
           |log_id                   string       ,
           |conv_acct_no             string       ,
           |inner_pro_ind            string       ,
           |acct_proc_in             string       ,
           |order_id                 string
           |)
           |partitioned by (part_trans_dt string)
           |row format delimited fields terminated by '!|'
           |stored as parquet
           |location '/user/ch_hypas/upw_hive/incident/trans/hive_swt_log'
           | """.stripMargin
      )

      println("=======Create hive_swt_log successfully ! =======")

    }

    def hive_cdhd_trans_year(implicit sqlContext: HiveContext) = {
      println("=======Create Table : hive_cdhd_trans_year=======")
      sqlContext.sql(s"use $hive_dbname")
      sqlContext.sql("drop table if exists hive_cdhd_trans_year")

      sqlContext.sql(
        s"""
           |create table if not exists $hive_dbname.hive_cdhd_trans_year(
           |cdhd_usr_id  string,
           |year         string
           |)
           |partitioned by (part_year    string)
           |row format delimited fields terminated by '!|'
           |stored as parquet
           |location '/user/ch_hypas/upw_hive/parameter/hive_cdhd_trans_year'
           | """.stripMargin
      )

      println("=======Create hive_cdhd_trans_year successfully ! =======")

    }
    def hive_life_order_inf(implicit sqlContext: HiveContext) = {
      println("=======Create Table : hive_life_order_inf=======")
      sqlContext.sql(s"use $hive_dbname")
      sqlContext.sql("drop table if exists hive_life_order_inf")

      sqlContext.sql(
        s"""
           |create table if not exists $hive_dbname.hive_life_order_inf(
           |order_id              string ,
           |sys_no                string ,
           |mchnt_version         string ,
           |encoding              string ,
           |sign_method           string ,
           |mchnt_trans_tp        string ,
           |biz_tp                string ,
           |pay_method            string ,
           |trans_tp              string ,
           |buss_chnl             string ,
           |mchnt_front_url       string ,
           |mchnt_back_url        string ,
           |acq_ins_id_cd         string ,
           |mchnt_cd              string ,
           |mchnt_tp              string ,
           |mchnt_nm              string ,
           |sub_mchnt_cd          string ,
           |sub_mchnt_nm          string ,
           |mchnt_order_id        string ,
           |trans_tm              string ,
           |trans_dt              timestamp ,
           |sys_tm                string ,
           |pay_timeout           string ,
           |trans_at              string ,
           |trans_curr_cd         string ,
           |kz_at                 string ,
           |kz_curr_cd            string ,
           |conv_dt               string ,
           |deduct_at             string ,
           |discount_info         string ,
           |upoint_at             string ,
           |top_info              string ,
           |refund_at             string ,
           |iss_ins_id_cd         string ,
           |iss_head              string ,
           |pri_acct_no           string ,
           |card_attr             string ,
           |usr_id                string ,
           |phone_no              string ,
           |trans_ip              string ,
           |trans_st              string ,
           |trans_no              string ,
           |trans_idx             string ,
           |sys_tra_no            string ,
           |order_desc            string ,
           |order_detail          string ,
           |proc_sys              string ,
           |proc_st               string ,
           |trans_source          string ,
           |resp_cd               string ,
           |other_usr             string ,
           |initial_pay           string ,
           |to_ts                 string ,
           |rec_crt_ts            string ,
           |rec_upd_ts            string ,
           |buss_tp_nm						string ,
           |chnl_tp_nm						string
           |
           |)
           |partitioned by (part_hp_trans_dt string)
           |row format delimited fields terminated by '!|'
           |stored as parquet
           |location '/user/ch_hypas/upw_hive/incident/order/hive_life_order_inf'
           |
           | """.stripMargin
      )

      println("=======Create hive_life_order_inf successfully ! =======")

    }

  def hive_org_tdapp_tappevent(implicit sqlContext: HiveContext) = {
    println("=======Create hive_org_tdapp_tappevent=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_org_tdapp_tappevent")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_org_tdapp_tappevent
         |(
         |loguuid             string     ,
         |developerid         int        ,
         |productid           int        ,
         |platformid          int        ,
         |partnerid           int        ,
         |appversion          string     ,
         |tduserid            int        ,
         |eventid             string     ,
         |label               string     ,
         |eventcount          int        ,
         |starttime           bigint     ,
         |starttime_hour      int        ,
         |starttime_day       int        ,
         |starttime_week      int        ,
         |starttime_month     int        ,
         |starttime_year      int
         |)
         |partitioned by (part_updays string,part_daytime string)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/incident/td/hive_org_tdapp_tappevent'
         | """.stripMargin)

    println("=======Create hive_org_tdapp_tappevent successfully ! =======")

  }

  def hive_rtdtrs_dtl_ach_bill(implicit sqlContext: HiveContext) = {
    println("=======Create hive_rtdtrs_dtl_ach_bill=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_rtdtrs_dtl_ach_bill")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_rtdtrs_dtl_ach_bill
         |(
         |settle_dt        string,
         |trans_no         string,
         |trans_id         string,
         |trans_idx        string,
         |mchnt_version    string,
         |encoding         string,
         |cert_id          string,
         |sign_method      string,
         |trans_tp         string,
         |sub_trans_tp     string,
         |in_trans_tp      string,
         |biz_tp           string,
         |buss_chnl        string,
         |access_tp        string,
         |acq_ins_id_cd    string,
         |mchnt_cd         string,
         |mchnt_nm         string,
         |mchnt_abbr       string,
         |sub_mchnt_cd     string,
         |sub_mchnt_nm     string,
         |sub_mchnt_abbr   string,
         |mchnt_order_id   string,
         |mchnt_tp         string,
         |t_mchnt_cd       string,
         |buss_order_id    string,
         |rel_trans_idx    string,
         |trans_tm         string,
         |trans_dt         string,
         |trans_md         string,
         |carrier_tp       string,
         |pri_acct_no      string,
         |trans_at         bigint,
         |points_at        bigint,
         |trans_curr_cd    string,
         |trans_st         string,
         |pay_timeout      string,
         |term_id          string,
         |token_id         string,
         |iss_ins_id_cd    string,
         |card_attr        string,
         |proc_st          string,
         |resp_cd          string,
         |proc_sys         string,
         |iss_head         string,
         |iss_head_nm      string,
         |pay_method       string,
         |advance_payment  string,
         |auth_id          string,
         |usr_id           string,
         |trans_class      string,
         |out_trans_tp     string,
         |fwd_ins_id_cd    string,
         |rcv_ins_id_cd    string,
         |pos_cond_cd      string,
         |trans_source     string,
         |verify_mode      string,
         |iss_ins_tp       string,
         |conv_dt          string,
         |settle_at        bigint,
         |settle_curr_cd   string,
         |settle_conv_rt   int   ,
         |achis_settle_dt  string,
         |kz_at            bigint,
         |kz_conv_rt       int   ,
         |kz_curr_cd       string,
         |refund_at        bigint,
         |mchnt_country    string,
         |name             string,
         |phone_no         string,
         |trans_ip         string,
         |sys_tra_no       string,
         |sys_tm           string,
         |client_id        string,
         |buss_tp          string,
         |card_risk_flag   string,
         |ebank_order_num  string,
         |order_desc       string,
         |usr_num_tp       string,
         |usr_num          string,
         |area_cd          string,
         |addn_area_cd     string,
         |reserve_fld      string,
         |db_tag           string,
         |db_adtnl         string,
         |rec_crt_ts       string,
         |rec_upd_ts       string,
         |field1           string,
         |field2           string,
         |field3           string,
         |customer_email   string,
         |bind_id          string,
         |enc_cert_id      string,
         |mchnt_front_url  string,
         |mchnt_back_url   string,
         |req_reserved     string,
         |reserved         string,
         |log_id           string,
         |mail_no          string,
         |icc_data2        string,
         |token_data       string,
         |bill_data        string,
         |acct_data        string,
         |risk_data        string,
         |req_pri_data     string,
         |order_detail     string
         |)
         |partitioned by (hp_settle_dt string)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/incident/order/hive_rtdtrs_dtl_ach_bill'
         | """.stripMargin)

    println("=======Create hive_rtdtrs_dtl_ach_bill successfully ! =======")

  }

  /**
    *
    * JOB_HV_83
    * table  hive_point_trans
    */
  def hive_point_trans(implicit sqlContext: HiveContext) = {
    println("=======Create hive_point_trans=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_point_trans")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_point_trans
         |(
         |trans_id               string    ,
         |trans_dt               timestamp ,
         |trans_ts               timestamp ,
         |trans_tp               string    ,
         |src_id                 string   ,
         |orig_trans_id          string    ,
         |refund_at              bigint    ,
         |mchnt_order_at          bigint   ,
         |mchnt_order_curr         string  ,
         |mchnt_order_dt          string   ,
         |mchnt_order_id           string  ,
         |mchnt_cd                 string  ,
         |mchnt_tp                 string  ,
         |mchnt_nm                 string  ,
         |usr_id                   int     ,
         |mobile                   string  ,
         |card_no                  string  ,
         |result_cd                string  ,
         |result_msg               string  ,
         |rec_crt_ts               timestamp ,
         |rec_upd_ts               timestamp  ,
         |tran_src                 string    ,
         |chnl_mchnt_cd            string ,
         |extra_info               string
         |)
         |partitioned by (part_trans_dt string)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/incident/trans/hive_point_trans'
         | """.stripMargin)

    println("=======Create hive_point_trans successfully ! =======")
  }

  /**
    * JOB_HV_84
    *  table  hive_mksvc_order
    *
    */
  def hive_mksvc_order(implicit  sqlContext :HiveContext)={
    println("=======Create hive_mksvc_order=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_mksvc_order")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_mksvc_order
         |(
         |order_id                      string     ,
         |source_order_tp               string     ,
         |source_order_id               string     ,
         |source_order_ts               string     ,
         |trans_at                       int       ,
         |sys_id                        string     ,
         |mchnt_cd                      string     ,
         |order_st                       string    ,
         |activity_id                    int       ,
         |award_lvl                      int       ,
         |award_ts                       string    ,
         |source_order_st                string    ,
         |mchnt_order_id                 string    ,
         |usr_id                         int       ,
         |upop_usr_id                    int       ,
         |source_order_checked           string    ,
         |order_dt                       timestamp ,
         |order_digest                   string    ,
         |usr_ip                         string    ,
         |rec_crt_ts                     timestamp ,
         |rec_upd_ts                     timestamp ,
         |rec_st                         int       ,
         |rec_crt_oper_id                string    ,
         |order_memo                     string    ,
         |order_extend_inf               string    ,
         |award_id                       int       ,
         |mobile                         string    ,
         |card_no                        string    ,
         |unified_usr_id                 string
         |)
         |partitioned by (part_order_dt string)
         | row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/incident/order/hive_mksvc_order'
         | """.stripMargin)

    println("=======Create hive_mksvc_order successfully ! =======")
  }

  /**
    * JOB_HV_85
    *  table hive_wlonl_transfer_order
    */
  def hive_wlonl_transfer_order(implicit  sqlContext :HiveContext)={
    println("=======Create hive_wlonl_transfer_order=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_wlonl_transfer_order")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_wlonl_transfer_order
         |(
         |id                          int            ,
         |user_id                     string         ,
         |tn                          string         ,
         |pan                         string         ,
         |trans_amount                 int           ,
         |order_desc                   string        ,
         |order_detail                 string        ,
         |status                       string        ,
         |create_time                  timestamp     ,
         |trans_type                    string       ,
         |order_dt                      timestamp
         |)
         |partitioned by (part_order_dt string)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/incident/order/hive_wlonl_transfer_order'
         | """.stripMargin)

    println("=======Create hive_wlonl_transfer_order successfully ! =======")
  }

  /**
    * JOB_HV_86
    * table hive_wlonl_uplan_coupon
    */
  def hive_wlonl_uplan_coupon(implicit  sqlContext :HiveContext)={
    println("=======Create hive_wlonl_uplan_coupon=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_wlonl_uplan_coupon")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_wlonl_uplan_coupon
         |(
         |id                                int     ,
         |user_id                           string     ,
         |pmt_code                          string     ,
         |proc_dt                           string     ,
         |coupon_id                         string       ,
         |refnum                            string     ,
         |valid_start_date                  string     ,
         |valid_end_date                    string
         |)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/agreement/hive_wlonl_uplan_coupon'
         | """.stripMargin)

    println("=======Create hive_wlonl_uplan_coupon successfully ! =======")
  }

  /**
    * JOB_HV_87
    * table hive_wlonl_acc_notes
    */
  def hive_wlonl_acc_notes (implicit  sqlContext :HiveContext)={
    println("=======Create hive_wlonl_acc_notes=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_wlonl_acc_notes")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_wlonl_acc_notes
         |(
         |notes_id                          bigint     ,
         |notes_tp                          string     ,
         |notes_at                          bigint     ,
         |trans_tm                          string     ,
         |trans_in_acc                      string       ,
         |trans_in_acc_tp                   string     ,
         |trans_out_acc                     string     ,
         |trans_out_acc_tp                  string    ,
         |mchnt_nm                          string ,
         |notes_class                       string ,
         |notes_class_nm                    string ,
         |notes_class_child                 string ,
         |notes_class_child_nm              string ,
         |reimburse                         string ,
         |members                           string ,
         |currency_tp                       string ,
         |pro_nm                            string ,
         |user_id                           string ,
         |rec_st                            string ,
         |photo_url                         string ,
         |remark                            string ,
         |ext1                              string ,
         |ext2                              string ,
         |ext3                              string ,
         |rec_crt_ts                        timestamp ,
         |rec_upd_ts                        timestamp
         |)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/agreement/hive_wlonl_acc_notes'
         | """.stripMargin)

    println("=======Create hive_wlonl_acc_notes successfully ! =======")
  }

  /**
    * JOB_HV_88
    * table hive_ubp_order
    */
  def hive_ubp_order (implicit  sqlContext :HiveContext)={
    println("=======Create hive_ubp_order=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_ubp_order")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_ubp_order
         |(
         |order_id                     string     ,
         |order_at                      bigint     ,
         |refund_at                     bigint     ,
         |order_st                      string     ,
         |order_dt                      timestamp       ,
         |order_tm                      string     ,
         |order_timeout                 int     ,
         |usr_id                        int     ,
         |usr_ip                        string ,
         |mer_id                        string ,
         |sub_mer_id                    string ,
         |card_no                       string ,
         |bill_no                       string ,
         |chnl_tp                       string ,
         |order_desc                    string ,
         |access_order_id               string ,
         |access_reserved               string ,
         |gw_tp                         string ,
         |notice_front_url              string ,
         |notice_back_url               string ,
         |biz_tp                        string ,
         |biz_map                       string ,
         |ext                           string ,
         |rec_crt_ts                    timestamp ,
         |rec_upd_ts                    timestamp ,
         |sub_biz_tp                    string    ,
         |settle_id                     string    ,
         |ins_id_cd                     string    ,
         |access_md                     string    ,
         |mcc                           string    ,
         |submcc                        string    ,
         |mer_name                      string    ,
         |mer_abbr                      string    ,
         |sub_mer_name                  string    ,
         |sub_mer_abbr                  string    ,
         |upoint_at                     bigint    ,
         |qr_code                       string    ,
         |payment_valid_tm              string    ,
         |receive_ins_id_cd             string    ,
         |term_id                       string
         |)
         |partitioned by (part_order_dt string)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/incident/order/hive_ubp_order'
         | """.stripMargin)

    println("=======Create hive_ubp_order successfully ! =======")
  }

  /**
    * JOB_HV_89
    * table  hive_mnsvc_business_instal_info
    */
  def hive_mnsvc_business_instal_info (implicit  sqlContext :HiveContext)={
    println("=======Create hive_mnsvc_business_instal_info=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_mnsvc_business_instal_info")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_mnsvc_business_instal_info
         |(
         |instal_info_id                     bigint  ,
         |token_id                           string  ,
         |user_id                            string  ,
         |card_no                            string  ,
         |bank_cd                            string  ,
         |instal_amt                         bigint  ,
         |curr_num                           string  ,
         |period                             int    ,
         |fee_option                         string ,
         |cred_no                            string ,
         |prod_id                            string ,
         |samt_pnt                           string ,
         |apply_time                         string ,
         |instal_apply_st                    string ,
         |rec_st                             string ,
         |remark                             string ,
         |rec_crt_ts                         timestamp ,
         |rec_upd_ts                         timestamp ,
         |ext1                               string ,
         |ext2                               string ,
         |ext3                               string
         |)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/agreement/mnsvc_business_instal_info'
         | """.stripMargin)

    println("=======Create hive_mnsvc_business_instal_info successfully ! =======")
  }


  /**
    * JOB_HV_91
    */
  def hive_mtdtrs_dtl_ach_bat_file (implicit  sqlContext :HiveContext)={
    println("=======Create hive_mtdtrs_dtl_ach_bat_file=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_mtdtrs_dtl_ach_bat_file")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_mtdtrs_dtl_ach_bat_file
         |(
         |settle_dt              string,
         |bat_id                 string,
         |out_bat_id             string,
         |machine_id             string,
         |file_nm                string,
         |file_path              string,
         |rs_file_path           string,
         |download_num           int,
         |file_prio              string,
         |file_tp                string,
         |file_st                string,
         |file_source            string,
         |proc_begin_ts          string,
         |proc_end_ts            string,
         |proc_succ_num          int,
         |proc_succ_at           decimal(12,0),
         |mchnt_cd               string,
         |mchnt_acct_no          string,
         |acbat_settle_dt        string,
         |trans_sum_num          int,
         |verified_num           int,
         |trans_sum_at           decimal(12,0),
         |file_send_dt           string,
         |oper_id                string,
         |auth_oper_id           string,
         |file_head_info         string,
         |upload_oper            string,
         |acq_ins_id_cd          string,
         |file_req_sn            string,
         |fail_reason            string,
         |acq_audit_oper_id      string,
         |acq_audit_ts           string,
         |acq_adjust_oper_id     string,
         |acq_adjust_ts          string,
         |version                string,
         |reserve1               string,
         |reserve2               string,
         |rec_upd_oper_id        string,
         |rec_upd_trans_id       string,
         |chnl_tp                string,
         |conn_md                string,
         |rec_crt_ts             string,
         |rec_upd_ts             string
         |)
         |partitioned by (part_settle_dt string)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/incident/trans/hive_mtdtrs_dtl_ach_bat_file'
         | """.stripMargin)

    println("=======Create hive_mtdtrs_dtl_ach_bat_file successfully ! =======")
  }

  /**
    *  JOB_HV_92
    */
  def hive_mtdtrs_dtl_ach_bat_file_dtl (implicit  sqlContext :HiveContext)={
    println("=======create hive_mtdtrs_dtl_ach_bat_file_dtl=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql(
     s"""
        |create table if not exists $hive_dbname.hive_mtdtrs_dtl_ach_bat_file_dtl
        |(
        |settle_dt               string,
        |trans_idx               string,
        |bat_id                  string,
        |mchnt_cd                string,
        |mchnt_tp                string,
        |mchnt_addr              string,
        |trans_tp                string,
        |buss_tp                 string,
        |proc_st                 string,
        |proc_sys                string,
        |mchnt_order_id          string,
        |usr_id_tp               string,
        |usr_id                  string,
        |cvn2                    string,
        |expire_dt               string,
        |bill_tp                 string,
        |bill_no                 string,
        |trans_at                decimal(12,0),
        |fee_at                  decimal(12,0),
        |trans_tm                string,
        |trans_curr_cd           string,
        |pay_acct                string,
        |pay_acct_tp             string,
        |pri_acct_no             string,
        |iss_ins_id_cd           string,
        |iss_ins_nm              string,
        |customer_nm             string,
        |customer_mobile         string,
        |customer_email          string,
        |org_order_id            string,
        |org_trans_tm            string,
        |org_trans_at            decimal(12,0),
        |refund_rsn              string,
        |cert_tp                 string,
        |cert_id                 string,
        |conn_md                 string,
        |product_info            string,
        |acct_balance            decimal(12,0),
        |out_trans_idx           string,
        |org_trans_idx           string,
        |sys_tra_no              string,
        |sys_tm                  string,
        |retri_ref_no            string,
        |fwd_ins_id_cd           string,
        |rcv_ins_id_cd           string,
        |trans_method            string,
        |trans_terminal_tp       string,
        |svr_cond_cd             string,
        |sd_tag                  string,
        |bill_interval           string,
        |resp_cd                 string,
        |org_rec_info            string,
        |comments                string,
        |machine_id              string,
        |file_prio               string,
        |file_tp                 string,
        |trans_chnl              string,
        |chnl_tp                 string,
        |resp_desc               string,
        |broker_seq              bigint,
        |iss_province            string,
        |iss_city                string,
        |acq_ins_id_cd           string,
        |dtl_req_sn              string,
        |reserve1                string,
        |reserve2                string,
        |reserve3                string,
        |reserve4                string,
        |rec_upd_oper_id         string,
        |rec_upd_trans_id        string,
        |rec_crt_ts              string,
        |rec_upd_ts              string
        |)
        |partitioned by (part_settle_dt string)
        |row format delimited fields terminated by '!|'
        |stored as parquet
        |location '/user/ch_hypas/upw_hive/incident/trans/hive_mtdtrs_dtl_ach_bat_file_dtl'
      """.stripMargin)

    println("=======create hive_mtdtrs_dtl_ach_bat_file_dtl successfully ! =======")
  }


  /**
    *  JOB_HV_93
    */
  def hive_rtdtrs_dtl_achis_bill (implicit  sqlContext :HiveContext)={
    println("=======create hive_rtdtrs_dtl_achis_bill=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_rtdtrs_dtl_achis_bill")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_rtdtrs_dtl_achis_bill
         |(
         |settle_dt           string ,
         |trans_idx           string ,
         |acq_trans_idx       string ,
         |fwd_sys_cd          string ,
         |trans_tp            string ,
         |trans_class         string ,
         |pri_acct_no         string ,
         |acq_ins_id_cd       string ,
         |fwd_ins_id_cd       string ,
         |iss_ins_id_cd       string ,
         |iss_head            string ,
         |iss_head_nm         string ,
         |mchnt_cd            string ,
         |mchnt_nm            string ,
         |mchnt_country       string ,
         |mchnt_url           string ,
         |mchnt_front_url     string ,
         |mchnt_back_url      string ,
         |mchnt_delv_tag      string ,
         |mchnt_tp            string ,
         |mchnt_risk_tag      string ,
         |mchnt_order_id      string ,
         |sys_tra_no          string ,
         |sys_tm              string ,
         |trans_tm            string ,
         |trans_dt            string ,
         |trans_at            bigint ,
         |trans_curr_cd       string ,
         |trans_st            string ,
         |refund_at           bigint ,
         |auth_id             string ,
         |settle_at           bigint ,
         |settle_curr_cd      string ,
         |settle_conv_rt      bigint ,
         |conv_dt             string ,
         |cert_tp             string ,
         |cert_id             string ,
         |name                string ,
         |phone_no            string ,
         |org_trans_idx       string ,
         |org_sys_tra_no      string ,
         |org_sys_tm          string ,
         |proc_st             string ,
         |resp_cd             string ,
         |proc_sys            string ,
         |usr_id              int    ,
         |mchnt_id            int    ,
         |pay_method          string ,
         |trans_ip            string ,
         |trans_no            string ,
         |encoding            string ,
         |mac_addr            string ,
         |card_attr           string ,
         |kz_curr_cd          string ,
         |kz_conv_rt          bigint ,
         |kz_at               bigint ,
         |sub_mchnt_cd        string ,
         |sub_mchnt_nm        string ,
         |verify_mode         string ,
         |mchnt_reserve       string ,
         |reserve             string ,
         |mchnt_version       string ,
         |biz_tp              string ,
         |is_oversea          string ,
         |reserve1            string ,
         |reserve2            string ,
         |reserve3            string ,
         |reserve4            string ,
         |reserve5            string ,
         |reserve6            string ,
         |rec_st              string ,
         |comments            string ,
         |rec_crt_ts          string ,
         |rec_upd_ts          string ,
         |tlr_st              string ,
         |req_pri_data        string ,
         |out_trans_tp        string ,
         |org_out_trans_tp    string ,
         |ebank_id            string ,
         |ebank_mchnt_cd      string ,
         |ebank_order_num     string ,
         |ebank_idx           string ,
         |ebank_rsp_tm        string ,
         |mchnt_conn_tp       string ,
         |access_tp           string ,
         |trans_source        string ,
         |bind_id             string ,
         |card_risk_flag      string ,
         |buss_chnl           string
         |)
         |partitioned by (part_settle_dt string)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/incident/trans/hive_rtdtrs_dtl_achis_bill'
        """.stripMargin)

    println("=======create hive_rtdtrs_dtl_achis_bill successfully ! =======")
  }


  /**
    *  JOB_HV_94
    */
  def hive_rtdtrs_dtl_achis_note (implicit  sqlContext :HiveContext)={
    println("=======create hive_rtdtrs_dtl_achis_note=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_rtdtrs_dtl_achis_note")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_rtdtrs_dtl_achis_note(
         |settle_dt       string   ,
         |trans_no        string   ,
         |cli_tp          string   ,
         |cli_ver         string   ,
         |os_type         string   ,
         |os_ver          string   ,
         |phone_res       string   ,
         |locale          string   ,
         |imei            string   ,
         |uid             string   ,
         |up_vid          string   ,
         |up_uid          string   ,
         |sid             string   ,
         |ip              string   ,
         |gw_ip           string   ,
         |mac             string   ,
         |rec_crt_ts      string   ,
         |rec_upd_ts      string
         |)
         |partitioned by (part_settle_dt string)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/incident/trans/hive_rtdtrs_dtl_achis_note'
        """.stripMargin)

    println("=======create hive_rtdtrs_dtl_achis_note successfully ! =======")
  }


  /**
    *  JOB_HV_95
    */
  def hive_rtdtrs_dtl_achis_order (implicit  sqlContext :HiveContext)={
    println("=======create hive_rtdtrs_dtl_achis_order=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_rtdtrs_dtl_achis_order")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_rtdtrs_dtl_achis_order(
         |settle_dt                  string     ,
         |trans_no                   string     ,
         |trans_tp                   string     ,
         |trans_class                string     ,
         |trans_source               string     ,
         |buss_chnl                  string     ,
         |pri_acct_no                string     ,
         |fwd_ins_id_cd              string     ,
         |acq_ins_id_cd              string     ,
         |iss_ins_id_cd              string     ,
         |iss_head                   string     ,
         |iss_head_nm                string     ,
         |mchnt_cd                   string     ,
         |mchnt_nm                   string     ,
         |mchnt_country              string     ,
         |mchnt_url                  string     ,
         |mchnt_front_url            string     ,
         |mchnt_back_url             string     ,
         |mchnt_delv_tag             string     ,
         |mchnt_tp                   string     ,
         |mchnt_risk_tag             string     ,
         |mchnt_order_id             string     ,
         |sys_tra_no                 string     ,
         |sys_tm                     string     ,
         |trans_tm                   string     ,
         |trans_dt                   string     ,
         |trans_at                   bigint     ,
         |trans_curr_cd              string     ,
         |trans_st                   string     ,
         |auth_id                    string     ,
         |settle_at                  bigint     ,
         |settle_curr_cd             string     ,
         |settle_conv_rt             bigint     ,
         |conv_dt                    string     ,
         |cert_tp                    string     ,
         |cert_id                    string     ,
         |name                       string     ,
         |phone_no                   string     ,
         |org_trans_idx              string     ,
         |org_sys_tra_no             string     ,
         |org_sys_tm                 string     ,
         |proc_st                    string     ,
         |resp_cd                    string     ,
         |proc_sys                   string     ,
         |usr_id                     int        ,
         |mchnt_id                   int        ,
         |pay_method                 string     ,
         |trans_ip                   string     ,
         |refund_at                  bigint     ,
         |card_attr                  string     ,
         |kz_curr_cd                 string     ,
         |kz_conv_rt                 bigint     ,
         |kz_at                      bigint     ,
         |sub_mchnt_cd               string     ,
         |sub_mchnt_nm               string     ,
         |points_at                  bigint     ,
         |verify_mode                string     ,
         |is_oversea                 string     ,
         |pri_data                   string     ,
         |access_tp                  string     ,
         |rcv_ins_id_cd              string     ,
         |zero_cost                  string     ,
         |advance_payment            string     ,
         |pos_cond_cd                string     ,
         |reserve_fld                string     ,
         |reserve1                   string     ,
         |reserve2                   string     ,
         |reserve3                   string     ,
         |reserve4                   string     ,
         |reserve5                   string     ,
         |reserve6                   string     ,
         |comments                   string     ,
         |rec_st                     string     ,
         |rec_crt_ts                 string     ,
         |rec_upd_ts                 string     ,
         |trans_chnl                 string     ,
         |source_idt                 string     ,
         |out_trans_tp               string     ,
         |md_id                      string     ,
         |ud_id                      string     ,
         |syssp_id                   string     ,
         |src_sys_flag               string     ,
         |auth_at                    bigint     ,
         |iss_ins_tp                 string
         |)
         |partitioned by (part_settle_dt string)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/incident/trans/hive_rtdtrs_dtl_achis_order'
        """.stripMargin)

    println("=======create hive_rtdtrs_dtl_achis_order successfully ! =======")
  }

  /**
    *  JOB_HV_96
    */
  def hive_rtdtrs_dtl_achis_order_error (implicit  sqlContext :HiveContext)={
    println("=======create hive_rtdtrs_dtl_achis_order_error=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_rtdtrs_dtl_achis_order_error")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_rtdtrs_dtl_achis_order_error(
         |settle_dt           string    ,
         |trans_idx           string    ,
         |pri_acct_no         string    ,
         |trans_at            bigint    ,
         |trans_tm            string    ,
         |trans_tp            string    ,
         |out_trans_tp        string    ,
         |acq_ins_id_cd       string    ,
         |trans_curr_cd       string    ,
         |mchnt_cd            string    ,
         |mchnt_order_id      string    ,
         |rec_crt_ts          string    ,
         |trans_source        string    ,
         |buss_chnl           string    ,
         |iss_ins_id_cd       string    ,
         |error_tp            string    ,
         |error_msg           string    ,
         |error_cd            string    ,
         |advice_msg          string    ,
         |resp_cd             string    ,
         |sub_trans_tp        string
         |)
         |partitioned by (part_settle_dt string)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/incident/trans/hive_rtdtrs_dtl_achis_order_error'
        """.stripMargin)

    println("=======create hive_rtdtrs_dtl_achis_order_error successfully ! =======")
  }

  /**
    *  JOB_HV_97
    */
  def hive_rtdtrs_dtl_sor_cmsp (implicit  sqlContext :HiveContext)={
    println("=======create hive_rtdtrs_dtl_sor_cmsp=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_rtdtrs_dtl_sor_cmsp")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_rtdtrs_dtl_sor_cmsp(
         |trans_seq                          string    ,
         |trans_submit_tm                    string    ,
         |new_trans_submit_tm                string    ,
         |trans_tp_cd                        string    ,
         |trans_rcv_tm                       string    ,
         |trans_fin_tm                       string    ,
         |to_tm                              string    ,
         |to_moni_in                         string    ,
         |moni_dist                          string    ,
         |ab_in                              string    ,
         |trans_st_cd                        string    ,
         |rvsl_st_cd                         string    ,
         |source_line_svc                    string    ,
         |source_msg_cd                      string    ,
         |term_csn                           string    ,
         |trans_tp                           string    ,
         |mchnt_cd                           string    ,
         |mchnt_nm                           string    ,
         |order_seq                          string    ,
         |trans_at                           bigint    ,
         |trans_curr_cd                      string    ,
         |acctnum1                           string    ,
         |acctnum2                           string    ,
         |rspcode                            string    ,
         |spcode                             string    ,
         |terminal_id                        string    ,
         |settle_dt                          string    ,
         |syssp_id                           string    ,
         |pos_cd                             string    ,
         |card_no                            string    ,
         |app_cd                             int       ,
         |trans_type_id                      int       ,
         |cmsp_card_type_id                  int       ,
         |pboc_card_type_id                  int       ,
         |md_ins_id_cd                       string    ,
         |issuer_ins_id_cd                   string    ,
         |trans_cd                           string    ,
         |resp_cd1                           string    ,
         |resp_cd4                           string    ,
         |card_attr_id                       int       ,
         |card_brand_id                      int       ,
         |acpt_ins_id_cd                     string    ,
         |fwd_ins_id_cd                      string    ,
         |rcv_ins_id_cd                      string    ,
         |iss_ins_id_cd                      string    ,
         |settle_flag                        string    ,
         |ic_flds_cvert                      string    ,
         |src_sys                            string    ,
         |trans_seq_conv                     string    ,
         |new_trans_submit_tm_conv           string    ,
         |acpt_ins_id_cd_conv                string    ,
         |fwd_ins_id_cd_conv                 string    ,
         |proc_sys_flag                      string
         |)
         |partitioned by (part_settle_dt string)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/incident/trans/hive_rtdtrs_dtl_sor_cmsp'
        """.stripMargin)

    println("=======create hive_rtdtrs_dtl_sor_cmsp successfully ! =======")
  }

  /**
    *  JOB_HV_98
    */
  def hive_rtapam_dim_ins (implicit  sqlContext :HiveContext)={
    println("=======create hive_rtapam_dim_ins=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_rtapam_dim_ins")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_rtapam_dim_ins(
         |ins_id                             int   ,
         |ins_cn_nm                          string,
         |ins_id_cd                          string,
         |root_ins_nm                        string,
         |root_ins_cd                        string,
         |settle_root_ins_nm                 string,
         |settle_root_ins_cd                 string,
         |intnl_org_nm                       string,
         |intnl_org_cd                       string,
         |settle_intnl_org_nm                string,
         |settle_intnl_org_cd                string,
         |ins_cata_id1                       int   ,
         |ins_cata_nm1                       string,
         |ins_cata_id2                       int   ,
         |ins_cata_nm2                       string,
         |ins_cata_id3                       int   ,
         |ins_cata_nm3                       string,
         |ins_cata_id4                       int   ,
         |ins_cata_nm4                       string,
         |settle_ins_cata_id1                int   ,
         |settle_ins_cata_nm1                string,
         |settle_ins_cata_id2                int   ,
         |settle_ins_cata_nm2                string,
         |settle_ins_cata_id3                int   ,
         |settle_ins_cata_nm3                string,
         |settle_ins_cata_id4                int   ,
         |settle_ins_cata_nm4                string,
         |domin_id_1                         int   ,
         |domin_nm_1                         string,
         |domin_id_2                         int   ,
         |domin_nm_2                         string,
         |domin_tp_2                         string,
         |domin_id_3                         int   ,
         |domin_nm_3                         string,
         |src_sys                            string,
         |edw_rec_start_ts                   string,
         |edw_rec_end_ts                     string
         |)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/participant/ins/hive_rtapam_dim_ins'
        """.stripMargin)
    println("=======create hive_rtapam_dim_ins successfully ! =======")
  }

  /**
    *  JOB_HV_99  hive_stmtrs_scl_usr_geo_loc_quota
    */
  def hive_stmtrs_scl_usr_geo_loc_quota (implicit  sqlContext :HiveContext)={
    println("=======create hive_stmtrs_scl_usr_geo_loc_quota=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_stmtrs_scl_usr_geo_loc_quota")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_stmtrs_scl_usr_geo_loc_quota(
         |settle_month        string ,
         |cdhd_usr_id         string ,
         |intnl_org_cd_list   string ,
         |bind_card_no_list   string ,
         |hp_settle_month     string
         |)
         |partitioned by (part_settle_month string)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/product/card/hive_stmtrs_scl_usr_geo_loc_quota'
        """.stripMargin)
    println("=======create hive_stmtrs_scl_usr_geo_loc_quota successfully ! =======")
  }

  /**
    *  JOB_HV_100
    */
  def hive_rtapam_dim_intnl_domin(implicit  sqlContext :HiveContext)={
    println("=======create hive_rtapam_dim_intnl_domin=======")
    sqlContext.sql(s"use $hive_dbname")
    sqlContext.sql("drop table if exists hive_rtapam_dim_intnl_domin")

    sqlContext.sql(
      s"""
         |create table if not exists $hive_dbname.hive_rtapam_dim_intnl_domin(
         |domin_id           int      ,
         |par_domin_id       int      ,
         |domin_nm           string   ,
         |domin_tp           string   ,
         |intnl_org_id_cd    string   ,
         |intnl_org_nm       string   ,
         |prov_id            int      ,
         |edw_rec_crt_usr    string   ,
         |edw_rec_crt_ts     string   ,
         |edw_rec_upd_usr    string   ,
         |edw_rec_upd_ts     string   ,
         |src_sys            string   ,
         |src_busi_key       string
         |
         |)
         |row format delimited fields terminated by '!|'
         |stored as parquet
         |location '/user/ch_hypas/upw_hive/region/hive_rtapam_dim_intnl_domin'
        """.stripMargin)
    println("=======create hive_rtapam_dim_intnl_domin successfully ! =======")
  }
}