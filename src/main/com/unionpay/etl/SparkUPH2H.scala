package com.unionpay.etl
import java.text.SimpleDateFormat

import com.unionpay.conf.ConfigurationManager
import com.unionpay.constant.Constants
import com.unionpay.jdbc.UPSQL_TIMEPARAMS_JDBC
import com.unionpay.utils.DateUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Days, LocalDate, Months}

/**
  * 作业：抽取银联Hive的数据到钱包Hive数据仓库
  */
object SparkUPH2H {
  //UP NAMENODE URL
  private val up_namenode=ConfigurationManager.getProperty(Constants.UP_NAMENODE)
  //UP HIVE DATA ROOT URL
  private val up_hivedataroot=ConfigurationManager.getProperty(Constants.UP_HIVEDATAROOT)
  //指定HIVE数据库名
  private lazy val hive_dbname =ConfigurationManager.getProperty(Constants.HIVE_DBNAME)
  private  lazy  val dateFormatter=DateTimeFormat.forPattern("yyyy-MM-dd")
  private  lazy  val dateFormat_2=DateTimeFormat.forPattern("yyyyMMdd")

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkUPH2H")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new HiveContext(sc)

    var start_dt: String = s"0000-00-00"
    var end_dt: String = s"0000-00-00"
    var start_month: String = s"0000-00-00"
    var end_month: String = s"0000-00-00"


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
      println("#### 请指定 SparkUPH2H 数据抽取的起始日期和结束日期 ！")
      System.exit(0)
    }

    //获取开始日期和结束日期的间隔天数或间隔月数
    val interval=DateUtils.getIntervalDays(start_dt,end_dt).toInt

    //适用于按月抽取的JOB
    start_month = DateUtils.getLastMonthByJob(start_dt)
    end_month = DateUtils.getLastMonthByJob(end_dt)
    val months=DateUtils.getIntervalMonths(start_month,end_month)

    println(s"#### SparkUPH2H 数据抽取的起始日期为: $start_dt --  $end_dt")

    val JobName = if(args.length>0) args(0) else None
    println(s"#### The Current Job Name is ： [$JobName]")
    JobName match {
      /**
        * 每日模板job
        */
      case "JOB_HV_39"  => JOB_HV_39(sqlContext,start_dt,end_dt,interval) //CODE BY YX
      case "JOB_HV_49"  => JOB_HV_49 //CODE BY YX
      case "JOB_HV_52"  => JOB_HV_52(sqlContext,start_month,end_month,months) //CODE BY YX



      /**
        * 指标套表job
        */
      case "JOB_HV_41"  => JOB_HV_41(sqlContext, start_dt,end_dt,interval) //CODE BY XTP   already formatted
      case "JOB_HV_50"  =>  JOB_HV_50(sqlContext,start_dt,end_dt,interval) //CODE BY XTP
      case "JOB_HV_51"  =>  JOB_HV_51(sqlContext,start_dt,end_dt,interval) //CODE BY XTP

      case "JOB_HV_55"  =>  JOB_HV_55(sqlContext,start_dt,end_dt,interval) //CODE BY TZQ
      case "JOB_HV_56"  =>  JOB_HV_56(sqlContext,start_dt,end_dt,interval) //CODE BY TZQ
      case "JOB_HV_57"  =>  JOB_HV_57(sqlContext,start_dt,end_dt,interval) //CODE BY TZQ

      case "JOB_HV_58"  =>  JOB_HV_58(sqlContext,start_dt,end_dt,interval) //CODE BY XTP
      case "JOB_HV_59"  =>  JOB_HV_59(sqlContext,start_dt,end_dt,interval) //CODE BY XTP
      case "JOB_HV_60"  =>  JOB_HV_60(sqlContext,start_dt,end_dt,interval) //CODE BY XTP
      case "JOB_HV_61"  =>  JOB_HV_61(sqlContext,start_dt,end_dt,interval) //CODE BY XTP
      case "JOB_HV_62"  =>  JOB_HV_62(sqlContext,start_dt,end_dt,interval) //CODE BY XTP
      case "JOB_HV_63"  =>  JOB_HV_63(sqlContext,start_dt,end_dt,interval) //CODE BY XTP
      case "JOB_HV_64"  =>  JOB_HV_64(sqlContext,start_dt,end_dt,interval) //CODE BY XTP
      case "JOB_HV_65"  =>  JOB_HV_65(sqlContext,start_dt,end_dt,interval) //CODE BY XTP

      case "JOB_HV_71"  =>  JOB_HV_71(sqlContext,start_dt,end_dt,interval) //CODE BY TZQ
      case "JOB_HV_90"  =>  JOB_HV_90(sqlContext,start_dt,end_dt,interval) //CODE BY TZQ
      case "JOB_HV_91"  =>  JOB_HV_91(sqlContext,start_dt,end_dt,interval) //CODE BY TZQ
      case "JOB_HV_92"  =>  JOB_HV_92(sqlContext,start_dt,end_dt,interval) //CODE BY TZQ
      case "JOB_HV_93"  =>  JOB_HV_93(sqlContext,start_dt,end_dt,interval) //CODE BY TZQ
      case "JOB_HV_94"  =>  JOB_HV_94(sqlContext,start_dt,end_dt,interval) //CODE BY XTP
      case "JOB_HV_95"  =>  JOB_HV_95(sqlContext,start_dt,end_dt,interval) //CODE BY XTP
      case "JOB_HV_96"  =>  JOB_HV_96(sqlContext,start_dt,end_dt,interval) //CODE BY XTP
      case "JOB_HV_97"  =>  JOB_HV_97(sqlContext,start_dt,end_dt,interval) //CODE BY XTP

      case "JOB_HV_98"  =>  JOB_HV_98   //CODE BY TZQ
      case "JOB_HV_99"  =>  JOB_HV_99(sqlContext,start_month,end_month,months) //CODE BY TZQ
      case "JOB_HV_100"  => JOB_HV_100  //CODE BY TZQ

      case _ => println("#### No Case Job,Please Input JobName")

    }

    sc.stop()
  }




  /**
    * JobName: JOB_HV_39
    * Feature: uphive.rtdtrs_dtl_achis -> hive.hive_achis_trans
    *
    * @author YangXue
    * @time 2016-08-30
    * @param sqlContext,end_dt
    */

  def JOB_HV_39(implicit sqlContext: HiveContext,start_dt:String,end_dt:String, interval: Int) = {

    DateUtils.timeCost("JOB_HV_39") {
      println("#### JOB_HV_39(rtdtrs_dtl_achis -> hive_achis_trans)")
      println("#### JOB_HV_39 增量抽取的时间范围为:" + start_dt + "  --  " + end_dt)

      var today_dt = start_dt
      if (interval >= 0) {
        for (i <- 0 to interval) {
          println(s"#### JOB_HV_39  spark sql 开始抽取[$today_dt]数据")

          val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/incident/ods/hive_achis_trans/part_settle_dt=$today_dt")
          println(s"#### JOB_HV_39 read $up_namenode/ 数据完成时间为:" + DateUtils.getCurrentSystemTime())

          df.registerTempTable("spark_hive_achis_trans")
          println("#### JOB_HV_39 registerTempTable--spark_hive_achis_trans 完成的系统时间为:" + DateUtils.getCurrentSystemTime())

          if (!Option(df).isEmpty) {
            sqlContext.sql(s"use $hive_dbname")
            sqlContext.sql(
              s"""
                 |insert overwrite table hive_achis_trans partition (part_settle_dt)
                 |select
                 |settle_dt          ,
                 |trans_idx          ,
                 |trans_tp           ,
                 |trans_class        ,
                 |trans_source       ,
                 |buss_chnl          ,
                 |carrier_tp         ,
                 |pri_acct_no        ,
                 |mchnt_conn_tp      ,
                 |access_tp          ,
                 |conn_md            ,
                 |acq_ins_id_cd      ,
                 |acq_head           ,
                 |fwd_ins_id_cd      ,
                 |rcv_ins_id_cd      ,
                 |iss_ins_id_cd      ,
                 |iss_head           ,
                 |iss_head_nm        ,
                 |mchnt_cd           ,
                 |mchnt_nm           ,
                 |mchnt_country      ,
                 |mchnt_url          ,
                 |mchnt_front_url    ,
                 |mchnt_back_url     ,
                 |mchnt_tp           ,
                 |mchnt_order_id     ,
                 |mchnt_order_desc   ,
                 |mchnt_add_info     ,
                 |mchnt_reserve      ,
                 |reserve            ,
                 |sub_mchnt_cd       ,
                 |sub_mchnt_company  ,
                 |sub_mchnt_nm       ,
                 |mchnt_class        ,
                 |sys_tra_no         ,
                 |trans_tm           ,
                 |sys_tm             ,
                 |trans_dt           ,
                 |auth_id            ,
                 |trans_at           ,
                 |trans_curr_cd      ,
                 |proc_st            ,
                 |resp_cd            ,
                 |proc_sys           ,
                 |trans_no           ,
                 |trans_st           ,
                 |conv_dt            ,
                 |settle_at          ,
                 |settle_curr_cd     ,
                 |settle_conv_rt     ,
                 |cert_tp            ,
                 |cert_id            ,
                 |name               ,
                 |phone_no           ,
                 |usr_id             ,
                 |mchnt_id           ,
                 |pay_method         ,
                 |trans_ip           ,
                 |encoding           ,
                 |mac_addr           ,
                 |card_attr          ,
                 |ebank_id           ,
                 |ebank_mchnt_cd     ,
                 |ebank_order_num    ,
                 |ebank_idx          ,
                 |ebank_rsp_tm       ,
                 |kz_curr_cd         ,
                 |kz_conv_rt         ,
                 |kz_at              ,
                 |delivery_country   ,
                 |delivery_province  ,
                 |delivery_city      ,
                 |delivery_district  ,
                 |delivery_street    ,
                 |sms_tp             ,
                 |sign_method        ,
                 |verify_mode        ,
                 |accpt_pos_id       ,
                 |mer_cert_id        ,
                 |cup_cert_id        ,
                 |mchnt_version      ,
                 |sub_trans_tp       ,
                 |mac                ,
                 |biz_tp             ,
                 |source_idt         ,
                 |delivery_risk      ,
                 |trans_flag         ,
                 |org_trans_idx      ,
                 |org_sys_tra_no     ,
                 |org_sys_tm         ,
                 |org_mchnt_order_id ,
                 |org_trans_tm       ,
                 |org_trans_at       ,
                 |req_pri_data       ,
                 |pri_data           ,
                 |addn_at            ,
                 |res_pri_data       ,
                 |inq_dtl            ,
                 |reserve_fld        ,
                 |buss_code          ,
                 |t_mchnt_cd         ,
                 |is_oversea         ,
                 |points_at          ,
                 |pri_acct_tp        ,
                 |area_cd            ,
                 |mchnt_fee_at       ,
                 |user_fee_at        ,
                 |curr_exp           ,
                 |rcv_acct           ,
                 |track2             ,
                 |track3             ,
                 |customer_nm        ,
                 |product_info       ,
                 |customer_email     ,
                 |cup_branch_ins_cd  ,
                 |org_trans_dt       ,
                 |special_calc_cost  ,
                 |zero_cost          ,
                 |advance_payment    ,
                 |new_trans_tp       ,
                 |flight_inf         ,
                 |md_id              ,
                 |ud_id              ,
                 |syssp_id           ,
                 |card_sn            ,
                 |tfr_in_acct        ,
                 |acct_id            ,
                 |card_bin           ,
                 |icc_data           ,
                 |icc_data2          ,
                 |card_seq_id        ,
                 |pos_entry_cd       ,
                 |pos_cond_cd        ,
                 |term_id            ,
                 |usr_num_tp         ,
                 |addn_area_cd       ,
                 |usr_num            ,
                 |reserve1           ,
                 |reserve2           ,
                 |reserve3           ,
                 |reserve4           ,
                 |reserve5           ,
                 |reserve6           ,
                 |rec_st             ,
                 |comments           ,
                 |to_ts              ,
                 |rec_crt_ts        ,
                 |rec_upd_ts        ,
                 |pay_acct          ,
                 |trans_chnl        ,
                 |tlr_st            ,
                 |rvs_st            ,
                 |out_trans_tp      ,
                 |org_out_trans_tp  ,
                 |bind_id           ,
                 |ch_info           ,
                 |card_risk_flag    ,
                 |trans_step        ,
                 |ctrl_msg          ,
                 |mchnt_delv_tag    ,
                 |mchnt_risk_tag    ,
                 |bat_id            ,
                 |payer_ip          ,
                 |gt_sign_val       ,
                 |mchnt_sign_val    ,
                 |deduction_at      ,
                 |src_sys_flag      ,
                 |mac_ip            ,
                 |mac_sq            ,
                 |trans_ip_num      ,
                 |cvn_flag          ,
                 |expire_flag       ,
                 |usr_inf           ,
                 |imei              ,
                 |iss_ins_tp        ,
                 |dir_field         ,
                 |buss_tp           ,
                 |in_trans_tp       ,
                 |'$today_dt' as p_settle_dt
                 |from
                 |spark_hive_achis_trans
           """.stripMargin)
            println("#### JOB_HV_39 分区数据插入完成的时间为：" + DateUtils.getCurrentSystemTime())
          } else {
            println(s"#### JOB_HV_39 read $up_namenode/ 无数据！")
          }
          today_dt = DateUtils.addOneDay(today_dt)
        }
      }
    }
  }


  /**
    * hive-job-41 2016-11-03
    * rtdtrs_dtl_cups to hive_cups_trans
    *
    * @author Xue
    * @param sqlContext
    */
  def JOB_HV_41(implicit sqlContext: HiveContext,start_dt:String,end_dt: String, interval: Int) {
    println("#### JOB_HV_41(rtdtrs_dtl_cups -> hive_cups_trans)")

    var today_dt = start_dt
    if (interval >= 0) {
      println("#### JOB_HV_41 增量抽取的时间范围为: "+start_dt+ "-"+ end_dt)
      DateUtils.timeCost("JOB_HV_41") {
        val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/incident/ods/hive_cups_trans/part_settle_dt=$today_dt")
        println(s"#### JOB_HV_41 read $up_namenode/ 数据完成时间为:" + DateUtils.getCurrentSystemTime())

        df.registerTempTable("spark_hive_cups_trans")
        println("#### JOB_HV_41 registerTempTable--spark_hive_cups_trans 完成的系统时间为:" + DateUtils.getCurrentSystemTime())

        if (!Option(df).isEmpty) {
          sqlContext.sql(s"use $hive_dbname")
          sqlContext.sql(
            s"""
               |insert overwrite table hive_cups_trans partition (part_settle_dt)
               |select
               |settle_dt                        ,
               |pri_key                          ,
               |log_cd                           ,
               |settle_tp                        ,
               |settle_cycle                     ,
               |block_id                         ,
               |orig_key                         ,
               |related_key                      ,
               |trans_fwd_st                     ,
               |trans_rcv_st                     ,
               |sms_dms_conv_in                  ,
               |fee_in                           ,
               |cross_dist_in                    ,
               |orig_acpt_sdms_in                ,
               |tfr_in_in                        ,
               |trans_md                         ,
               |source_region_cd                 ,
               |dest_region_cd                   ,
               |cups_card_in                     ,
               |cups_sig_card_in                 ,
               |card_class                       ,
               |card_attr                        ,
               |sti_in                           ,
               |trans_proc_in                    ,
               |acq_ins_id_cd                    ,
               |acq_ins_tp                       ,
               |fwd_ins_id_cd                    ,
               |fwd_ins_tp                       ,
               |rcv_ins_id_cd                    ,
               |rcv_ins_tp                       ,
               |iss_ins_id_cd                    ,
               |iss_ins_tp                       ,
               |related_ins_id_cd                ,
               |related_ins_tp                   ,
               |acpt_ins_id_cd                   ,
               |acpt_ins_tp                      ,
               |pri_acct_no                      ,
               |pri_acct_no_conv                 ,
               |sys_tra_no                       ,
               |sys_tra_no_conv                  ,
               |sw_sys_tra_no                    ,
               |auth_dt                          ,
               |auth_id_resp_cd                  ,
               |resp_cd1                         ,
               |resp_cd2                         ,
               |resp_cd3                         ,
               |resp_cd4                         ,
               |cu_trans_st                      ,
               |sti_takeout_in                   ,
               |trans_id                         ,
               |trans_tp                         ,
               |trans_chnl                       ,
               |card_media                       ,
               |card_media_proc_md               ,
               |card_brand                       ,
               |expire_seg                       ,
               |trans_id_conv                    ,
               |settle_mon                       ,
               |settle_d                         ,
               |orig_settle_dt                   ,
               |settle_fwd_ins_id_cd             ,
               |settle_rcv_ins_id_cd             ,
               |trans_at                         ,
               |orig_trans_at                    ,
               |trans_conv_rt                    ,
               |trans_curr_cd                    ,
               |cdhd_fee_at                      ,
               |cdhd_fee_conv_rt                 ,
               |cdhd_fee_acct_curr_cd            ,
               |repl_at                          ,
               |exp_snd_chnl                     ,
               |confirm_exp_chnl                 ,
               |extend_inf                       ,
               |conn_md                          ,
               |msg_tp                           ,
               |msg_tp_conv                      ,
               |card_bin                         ,
               |related_card_bin                 ,
               |trans_proc_cd                    ,
               |trans_proc_cd_conv               ,
               |tfr_dt_tm                        ,
               |loc_trans_tm                     ,
               |loc_trans_dt                     ,
               |conv_dt                          ,
               |mchnt_tp                         ,
               |pos_entry_md_cd                  ,
               |card_seq                         ,
               |pos_cond_cd                      ,
               |pos_cond_cd_conv                 ,
               |retri_ref_no                     ,
               |term_id                          ,
               |term_tp                          ,
               |mchnt_cd                         ,
               |card_accptr_nm_addr              ,
               |ic_data                          ,
               |rsn_cd                           ,
               |addn_pos_inf                     ,
               |orig_msg_tp                      ,
               |orig_msg_tp_conv                 ,
               |orig_sys_tra_no                  ,
               |orig_sys_tra_no_conv             ,
               |orig_tfr_dt_tm                   ,
               |related_trans_id                 ,
               |related_trans_chnl               ,
               |orig_trans_id                    ,
               |orig_trans_id_conv               ,
               |orig_trans_chnl                  ,
               |orig_card_media                  ,
               |orig_card_media_proc_md          ,
               |tfr_in_acct_no                   ,
               |tfr_out_acct_no                  ,
               |cups_resv                        ,
               |ic_flds                          ,
               |cups_def_fld                     ,
               |spec_settle_in                   ,
               |settle_trans_id                  ,
               |spec_mcc_in                      ,
               |iss_ds_settle_in                 ,
               |acq_ds_settle_in                 ,
               |settle_bmp                       ,
               |upd_in                           ,
               |exp_rsn_cd                       ,
               |to_ts                            ,
               |resnd_num                        ,
               |pri_cycle_no                     ,
               |alt_cycle_no                     ,
               |corr_pri_cycle_no                ,
               |corr_alt_cycle_no                ,
               |disc_in                          ,
               |vfy_rslt                         ,
               |vfy_fee_cd                       ,
               |orig_disc_in                     ,
               |orig_disc_curr_cd                ,
               |fwd_settle_at                    ,
               |rcv_settle_at                    ,
               |fwd_settle_conv_rt               ,
               |rcv_settle_conv_rt               ,
               |fwd_settle_curr_cd               ,
               |rcv_settle_curr_cd               ,
               |disc_cd                          ,
               |allot_cd                         ,
               |total_disc_at                    ,
               |fwd_orig_settle_at               ,
               |rcv_orig_settle_at               ,
               |vfy_fee_at                       ,
               |sp_mchnt_cd                      ,
               |acct_ins_id_cd                   ,
               |iss_ins_id_cd1                   ,
               |iss_ins_id_cd2                   ,
               |iss_ins_id_cd3                   ,
               |iss_ins_id_cd4                   ,
               |mchnt_ins_id_cd1                 ,
               |mchnt_ins_id_cd2                 ,
               |mchnt_ins_id_cd3                 ,
               |mchnt_ins_id_cd4                 ,
               |term_ins_id_cd1                  ,
               |term_ins_id_cd2                  ,
               |term_ins_id_cd3                  ,
               |term_ins_id_cd4                  ,
               |term_ins_id_cd5                  ,
               |acpt_cret_disc_at                ,
               |acpt_debt_disc_at                ,
               |iss1_cret_disc_at                ,
               |iss1_debt_disc_at                ,
               |iss2_cret_disc_at                ,
               |iss2_debt_disc_at                ,
               |iss3_cret_disc_at                ,
               |iss3_debt_disc_at                ,
               |iss4_cret_disc_at                ,
               |iss4_debt_disc_at                ,
               |mchnt1_cret_disc_at              ,
               |mchnt1_debt_disc_at              ,
               |mchnt2_cret_disc_at              ,
               |mchnt2_debt_disc_at              ,
               |mchnt3_cret_disc_at              ,
               |mchnt3_debt_disc_at              ,
               |mchnt4_cret_disc_at              ,
               |mchnt4_debt_disc_at              ,
               |term1_cret_disc_at               ,
               |term1_debt_disc_at               ,
               |term2_cret_disc_at               ,
               |term2_debt_disc_at               ,
               |term3_cret_disc_at               ,
               |term3_debt_disc_at               ,
               |term4_cret_disc_at               ,
               |term4_debt_disc_at               ,
               |term5_cret_disc_at               ,
               |term5_debt_disc_at               ,
               |pay_in                           ,
               |exp_id                           ,
               |vou_in                           ,
               |orig_log_cd                      ,
               |related_log_cd                   ,
               |mdc_key                          ,
               |rec_upd_ts                       ,
               |rec_crt_ts                       ,
               |'$today_dt' as p_settle_dt
               |from
               |spark_hive_cups_trans
           """.stripMargin)
          println("#### JOB_HV_41 分区数据插入完成的时间为：" + DateUtils.getCurrentSystemTime())
        } else {
          println(s"#### JOB_HV_41 read $up_namenode/ 无数据！")
        }
        today_dt = DateUtils.addOneDay(today_dt)
      }
    }
  }


  /**
    * JobName: JOB_HV_49
    * Feature: uphive.rtapam_prv_ucbiz_cdhd_bas_inf -> hive_ucbiz_cdhd_bas_inf
    *
    * @author YangXue
    * @time 2016-09-14
    * @param sqlContext
    */
  def JOB_HV_49(implicit sqlContext: HiveContext) = {
    println("#### JOB_HV_49(rtapam_prv_ucbiz_cdhd_bas_inf -> hive_ucbiz_cdhd_bas_inf)")
    println("#### JOB_HV_49 为全量抽取的表")

    DateUtils.timeCost("JOB_HV_49"){
      val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/participant/user/hive_ucbiz_cdhd_bas_inf")
      println(s"#### JOB_HV_49 read $up_namenode/ 数据完成时间为:"+DateUtils.getCurrentSystemTime())

      df.registerTempTable("spark_ucbiz_cdhd_bas_inf")
      println("#### JOB_HV_49 registerTempTable--spark_ucbiz_cdhd_bas_inf 完成的系统时间为:"+DateUtils.getCurrentSystemTime())

      if(!Option(df).isEmpty){
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql("insert overwrite table hive_ucbiz_cdhd_bas_inf select * from spark_ucbiz_cdhd_bas_inf")
        println("#### JOB_HV_49 全量数据插入完成的时间为："+DateUtils.getCurrentSystemTime())
      }else{
        println(s"#### JOB_HV_49 read $up_namenode/ 无数据！")
      }
    }
  }


  /**
    * hive-job-50 2016-11-03
    * org_tdapp_keyvalue to hive_org_tdapp_keyvalue
    *
    * @author XTP
    * @param sqlContext
    */
  def JOB_HV_50(implicit sqlContext: HiveContext, start_dt: String, end_dt: String, interval: Int) = {
    println("#### JOB_HV_50(org_tdapp_keyvalue to hive_org_tdapp_keyvalue)")

    DateUtils.timeCost("JOB_HV_50") {
      var today_dt = start_dt
      if (interval >= 0) {
        println(s"#### JOB_HV_50  spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())

        for (i <- 0 to interval) {
          val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/incident/td/hive_org_tdapp_keyvalue/part_updays=$today_dt")
          println(s"#### JOB_HV_50 read $up_namenode/ 读取大数据平台数据完成时间为:" + DateUtils.getCurrentSystemTime())

          df.registerTempTable("spark_hive_org_tdapp_keyvalue")
          println("#### JOB_HV_50 registerTempTable--spark_hive_org_tdapp_keyvalue 完成的系统时间为:" + DateUtils.getCurrentSystemTime())

          if (!Option(df).isEmpty) {
            sqlContext.sql(s"use $hive_dbname")

            println(s"#### JOB_HV_50 删除大数据平台分区(part_updays='$today_dt')数据, 时间为:" + DateUtils.getCurrentSystemTime())
            sqlContext.sql(s"alter table hive_org_tdapp_keyvalue drop partition(part_updays='$today_dt')")

            sqlContext.sql(
              s"""
                 |insert overwrite table hive_org_tdapp_keyvalue partition (part_updays,part_daytime)
                 |select
                 |loguuid           ,
                 |developerid       ,
                 |productid         ,
                 |platformid        ,
                 |partnerid         ,
                 |appversion        ,
                 |eventid           ,
                 |label             ,
                 |eventcount        ,
                 |keystring         ,
                 |value             ,
                 |valuenumber       ,
                 |type              ,
                 |starttime         ,
                 |starttime_hour    ,
                 |starttime_day     ,
                 |starttime_week    ,
                 |starttime_month   ,
                 |starttime_year    ,
                 |'$today_dt'       ,
                 |case
                 |when
                 |substr(part_daytime,1,4) between '0001' and '9999' and substr(part_daytime,6,2) between '01' and '12' and
                 |substr(part_daytime,9,2) between '01' and '31'
                 |then part_daytime
                 |else '0000-00-00'
                 |end as p_daytime
                 |from  spark_hive_org_tdapp_keyvalue
           """.stripMargin)
            println("#### JOB_HV_50 分区数据插入完成的时间为：" + DateUtils.getCurrentSystemTime())
          } else {
            println(s"#### JOB_HV_50 read $up_namenode/ 无数据！")
          }
          today_dt = DateUtils.addOneDay(today_dt)
        }
      }
    }
  }



  /**
    * hive-job-51 2016-12-27
    * org_tdapp_tappevent to hive_org_tdapp_tappevent
    *
    * @author XTP
    * @param sqlContext
    */
  def JOB_HV_51(implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("#### JOB_HV_51(org_tdapp_tappevent to hive_org_tdapp_tappevent)")

    DateUtils.timeCost("JOB_HV_51") {
      var today_dt = start_dt
      if (interval >= 0) {
        println(s"#### JOB_HV_51  spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())

        for (i <- 0 to interval) {
          val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/incident/td/hive_org_tdapp_tappevent/part_updays=$today_dt")
          println(s"#### JOB_HV_51 read $up_namenode/ 读取大数据平台数据完成时间为:" + DateUtils.getCurrentSystemTime())

          df.registerTempTable("spark_hive_org_tdapp_tappevent")
          println("#### JOB_HV_51 registerTempTable--spark_hive_org_tdapp_tappevent 完成的系统时间为:" + DateUtils.getCurrentSystemTime())

          if (!Option(df).isEmpty) {
            sqlContext.sql(s"use $hive_dbname")

            println(s"#### JOB_HV_51 删除大数据平台分区(part_updays='$today_dt')数据, 时间为:" + DateUtils.getCurrentSystemTime())
            sqlContext.sql(s"alter table hive_org_tdapp_tappevent drop partition(part_updays='$today_dt')")

            sqlContext.sql(
              s"""
                 |insert overwrite table hive_org_tdapp_tappevent partition (part_updays,part_daytime)
                 |select
                 |loguuid          ,
                 |developerid      ,
                 |productid        ,
                 |platformid       ,
                 |partnerid        ,
                 |appversion       ,
                 |tduserid         ,
                 |eventid          ,
                 |label            ,
                 |eventcount       ,
                 |starttime        ,
                 |starttime_hour   ,
                 |starttime_day    ,
                 |starttime_week   ,
                 |starttime_month  ,
                 |starttime_year   ,
                 |'$today_dt'      ,
                 |case
                 |when
                 |substr(part_daytime,1,4) between '0001' and '9999' and substr(part_daytime,6,2) between '01' and '12' and
                 |substr(part_daytime,9,2) between '01' and '31'
                 |then part_daytime
                 |else '0000-00-00'
                 |end as p_daytime
                 |from  spark_hive_org_tdapp_tappevent
           """.stripMargin)
            println("#### JOB_HV_51 分区数据插入完成的时间为：" + DateUtils.getCurrentSystemTime())
          } else {
            println(s"#### JOB_HV_51 read $up_namenode/ 无数据！")
          }
          today_dt = DateUtils.addOneDay(today_dt)
        }
      }
    }
  }


  /**
    * JobName: JOB_HV_52
    * Feature: uphive.stmtrs_bsl_active_card_acq_branch_mon1 -> hive.hive_active_card_acq_branch_mon
    * 每月7、8、9三天抽取
    *
    * @author YangXue
    * @time 2016-08-29
    * @param sqlContext,end_dt
    */
  def JOB_HV_52(implicit sqlContext: HiveContext, start_month: String, end_month: String, months: Int) = {

    DateUtils.timeCost("JOB_HV_52"){
      println("#### JOB_HV_52(stmtrs_bsl_active_card_acq_branch_mon1 -> hive_active_card_acq_branch_mon)")
      println(s"#### JOB_HV_52  spark sql 抽取[$start_month -- $end_month]数据开始时间为:" + DateUtils.getCurrentSystemTime())

      var cur_month = start_month
      if(months >= 0){
        for(i <- 0 to months){
          val part_month = cur_month.substring(0,7)
          println(s"#### JOB_HV_52  spark sql 开始抽取[$part_month]数据")

          val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/product/card/hive_active_card_acq_branch_mon/part_settle_month=$part_month")
          println(s"#### JOB_HV_52 read $up_namenode/ 数据完成时间为:" + DateUtils.getCurrentSystemTime())

          df.registerTempTable("spark_active_card_acq_branch_mon")
          println("#### JOB_HV_52 registerTempTable--spark_active_card_acq_branch_mon 完成的系统时间为:" + DateUtils.getCurrentSystemTime())

          if(!Option(df).isEmpty){
            sqlContext.sql(s"use $hive_dbname")
            sqlContext.sql(
              s"""
                 |insert overwrite table hive_active_card_acq_branch_mon partition (part_settle_month)
                 |select
                 |trans_month,
                 |trans_class,
                 |trans_cd,
                 |trans_chnl_id,
                 |card_brand_id,
                 |card_attr_id,
                 |acq_intnl_org_id_cd,
                 |iss_root_ins_id_cd,
                 |active_card_num,
                 |hp_settle_month,
                 |'$part_month' as p_settle_month
                 |from
                 |spark_active_card_acq_branch_mon
           """.stripMargin)
            println("#### JOB_HV_52 分区数据插入完成的时间为："+DateUtils.getCurrentSystemTime())
          }else{
            println(s"#### JOB_HV_52 read $up_namenode/ 无数据！")
          }
          cur_month = DateUtils.addOneMonth(cur_month)
        }
      }
    }
  }


  /**
    * hive-job-55 2016-11-15
    * org_tdapp_tactivity to  hive_org_tdapp_tactivity
    *
    * @author tzq,XTP
    * @param sqlContext
    */
  def JOB_HV_55(implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("#### JOB_HV_55(org_tdapp_tactivity to  hive_org_tdapp_tactivity)")

    DateUtils.timeCost("JOB_HV_55") {
      var today_dt = start_dt
      if (interval >= 0) {
        println(s"#### JOB_HV_55  spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())

        for (i <- 0 to interval) {
          val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/incident/td/hive_org_tdapp_tactivity/part_updays=$today_dt")
          println(s"#### JOB_HV_55 read $up_namenode/ 读取大数据平台数据完成时间为:" + DateUtils.getCurrentSystemTime())

          df.registerTempTable("spark_hive_org_tdapp_tactivity")
          println("#### JOB_HV_55 registerTempTable--spark_hive_org_tdapp_tactivity 完成的系统时间为:" + DateUtils.getCurrentSystemTime())

          if (!Option(df).isEmpty) {
            sqlContext.sql(s"use $hive_dbname")

            println(s"#### JOB_HV_55 删除大数据平台分区(part_updays='$today_dt')数据, 时间为:" + DateUtils.getCurrentSystemTime())
            sqlContext.sql(s"alter table hive_org_tdapp_tactivity drop partition(part_updays='$today_dt')")

            sqlContext.sql(
              s"""
                 |insert overwrite table hive_org_tdapp_tactivity partition (part_updays,part_daytime)
                 |select
                 |loguuid			    ,
                 |developerid     ,
                 |productid       ,
                 |platformid      ,
                 |partnerid       ,
                 |appversion      ,
                 |tduserid        ,
                 |refpagenameid   ,
                 |pagenameid      ,
                 |duration        ,
                 |sessionid       ,
                 |starttime       ,
                 |starttime_hour  ,
                 |starttime_day   ,
                 |starttime_week  ,
                 |starttime_month ,
                 |starttime_year  ,
                 |'$today_dt'     ,
                 |case
                 |when
                 |substr(part_daytime,1,4) between '0001' and '9999' and substr(part_daytime,6,2) between '01' and '12' and
                 |substr(part_daytime,9,2) between '01' and '31'
                 |then part_daytime
                 |else '0000-00-00'
                 |end as p_daytime
                 |from  spark_hive_org_tdapp_tactivity
           """.stripMargin)
            println("#### JOB_HV_55 分区数据插入完成的时间为：" + DateUtils.getCurrentSystemTime())
          } else {
            println(s"#### JOB_HV_55 read $up_namenode/ 无数据！")
          }
          today_dt = DateUtils.addOneDay(today_dt)
        }
      }
    }
  }

  /**
    * hive-job-56 2016-11-24
    * org_tdapp_tlaunch to  hive_org_tdapp_tlaunch
    *
    * @author tzq,XTP
    * @param sqlContext
    */
  def JOB_HV_56(implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("#### JOB_HV_56(org_tdapp_tlaunch to  hive_org_tdapp_tlaunch)")

    DateUtils.timeCost("JOB_HV_56") {
      var today_dt = start_dt
      if (interval >= 0) {
        println(s"#### JOB_HV_56  spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())

        for (i <- 0 to interval) {
          val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/incident/td/hive_org_tdapp_tlaunch/part_updays=$today_dt")
          println(s"#### JOB_HV_56 read $up_namenode/ 读取大数据平台数据完成时间为:" + DateUtils.getCurrentSystemTime())

          df.registerTempTable("spark_hive_org_tdapp_tlaunch")
          println("#### JOB_HV_56 registerTempTable--spark_hive_org_tdapp_tlaunch 完成的系统时间为:" + DateUtils.getCurrentSystemTime())

          if (!Option(df).isEmpty) {
            sqlContext.sql(s"use $hive_dbname")
            println(s"#### JOB_HV_56 删除大数据平台分区(part_updays='$today_dt')数据, 时间为:" + DateUtils.getCurrentSystemTime())
            sqlContext.sql(s"alter table hive_org_tdapp_tlaunch drop partition(part_updays='$today_dt')")

            sqlContext.sql(
              s"""
                 |insert overwrite table hive_org_tdapp_tlaunch partition (part_updays,part_daytime)
                 |select
                 |loguuid            ,
                 |developerid        ,
                 |productid          ,
                 |platformid         ,
                 |partnerid          ,
                 |appversion         ,
                 |tduserid           ,
                 |mobileid           ,
                 |channel            ,
                 |os                 ,
                 |pixel              ,
                 |countryid          ,
                 |provinceid         ,
                 |isp                ,
                 |language           ,
                 |jailbroken         ,
                 |cracked            ,
                 |sessionid          ,
                 |session_duration   ,
                 |interval_level     ,
                 |starttime          ,
                 |starttime_hour     ,
                 |starttime_day      ,
                 |starttime_week     ,
                 |starttime_month    ,
                 |starttime_year     ,
                 |'$today_dt'        ,
                 |case
                 |when
                 |substr(part_daytime,1,4) between '0001' and '9999' and substr(part_daytime,6,2) between '01' and '12' and
                 |substr(part_daytime,9,2) between '01' and '31'
                 |then part_daytime
                 |else '0000-00-00'
                 |end as p_daytime
                 |from  spark_hive_org_tdapp_tlaunch
           """.stripMargin)
            println("#### JOB_HV_56 分区数据插入完成的时间为：" + DateUtils.getCurrentSystemTime())
          } else {
            println(s"#### JOB_HV_56 read $up_namenode 无数据！")
          }
          today_dt = DateUtils.addOneDay(today_dt)
        }
      }
    }
  }


  /**
    * hive-job-57   2016-11-24
    * org_tdapp_terminate to  hive_org_tdapp_terminate
    *
    * @author tzq,XTP
    * @param sqlContext
    */
  def JOB_HV_57(implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("#### JOB_HV_57(org_tdapp_terminate to  hive_org_tdapp_terminate)")

    DateUtils.timeCost("JOB_HV_57") {
      var today_dt = start_dt
      if (interval >= 0) {
        println(s"#### JOB_HV_57  spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())

        for (i <- 0 to interval) {
          val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/incident/td/hive_org_tdapp_terminate/part_updays=$today_dt")
          println(s"#### JOB_HV_57 read $up_namenode/ 读取大数据平台数据完成时间为:" + DateUtils.getCurrentSystemTime())

          df.registerTempTable("spark_hive_org_tdapp_terminate")
          println("#### JOB_HV_57 registerTempTable--spark_hive_org_tdapp_terminate 完成的系统时间为:" + DateUtils.getCurrentSystemTime())

          if (!Option(df).isEmpty) {
            sqlContext.sql(s"use $hive_dbname")
            println(s"#### JOB_HV_57 删除大数据平台分区(part_updays='$today_dt')数据, 时间为:" + DateUtils.getCurrentSystemTime())
            sqlContext.sql(s"alter table hive_org_tdapp_terminate drop partition(part_updays='$today_dt')")

            sqlContext.sql(
              s"""
                 |insert overwrite table hive_org_tdapp_terminate partition (part_updays,part_daytime)
                 |select
                 |loguuid              ,
                 |developerid          ,
                 |productid            ,
                 |platformid           ,
                 |partnerid            ,
                 |appversion           ,
                 |devid                ,
                 |sessionid            ,
                 |session_duration     ,
                 |usetime_level        ,
                 |starttime            ,
                 |starttime_hour       ,
                 |starttime_day        ,
                 |starttime_week       ,
                 |starttime_month      ,
                 |starttime_year       ,
                 |'$today_dt'          ,
                 |case
                 |when
                 |substr(part_daytime,1,4) between '0001' and '9999' and substr(part_daytime,6,2) between '01' and '12' and
                 |substr(part_daytime,9,2) between '01' and '31'
                 |then part_daytime
                 |else '0000-00-00'
                 |end as p_daytime
                 |from  spark_hive_org_tdapp_terminate
           """.stripMargin)
            println("#### JOB_HV_57 分区数据插入完成的时间为：" + DateUtils.getCurrentSystemTime())
          } else {
            println(s"#### JOB_HV_57 read $up_namenode/ 无数据！")
          }
          today_dt = DateUtils.addOneDay(today_dt)
        }
      }
    }
  }


  /**
    * hive-job-58 2016-11-15
    * org_tdapp_activitynew to hive_org_tdapp_activitynew
    *
    * @author Xue
    * @param sqlContext
    */
  def JOB_HV_58(implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("#### JOB_HV_58(org_tdapp_activitynew to hive_org_tdapp_activitynew)")

    DateUtils.timeCost("JOB_HV_58") {
      var today_dt = start_dt
      if (interval >= 0) {
        println(s"#### JOB_HV_58  spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())

        for (i <- 0 to interval) {
          val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/incident/td/hive_org_tdapp_activitynew/part_updays=$today_dt")
          println(s"#### JOB_HV_58 read $up_namenode/ 读取大数据平台数据完成时间为:" + DateUtils.getCurrentSystemTime())

          df.registerTempTable("spark_hive_org_tdapp_activitynew")
          println("#### JOB_HV_58 registerTempTable--spark_hive_org_tdapp_activitynew 完成的系统时间为:" + DateUtils.getCurrentSystemTime())

          if (!Option(df).isEmpty) {
            sqlContext.sql(s"use $hive_dbname")
            println(s"#### JOB_HV_58 删除大数据平台分区(part_updays='$today_dt')数据, 时间为:" + DateUtils.getCurrentSystemTime())
            sqlContext.sql(s"alter table hive_org_tdapp_activitynew drop partition(part_updays='$today_dt')")

            sqlContext.sql(
              s"""
                 |insert overwrite table hive_org_tdapp_activitynew partition (part_updays,part_daytime)
                 |select
                 |loguuid          ,
                 |developerid      ,
                 |productid        ,
                 |platformid       ,
                 |partnerid        ,
                 |appversion       ,
                 |tduserid         ,
                 |mobileid         ,
                 |channel          ,
                 |os               ,
                 |pixel            ,
                 |countryid        ,
                 |provinceid       ,
                 |isp              ,
                 |language         ,
                 |jailbroken       ,
                 |cracked          ,
                 |starttime_hour   ,
                 |starttime_day    ,
                 |starttime_week   ,
                 |starttime_month  ,
                 |starttime_year   ,
                 |'$today_dt'      ,
                 |case
                 |when
                 |substr(part_daytime,1,4) between '0001' and '9999' and substr(part_daytime,6,2) between '01' and '12' and
                 |substr(part_daytime,9,2) between '01' and '31'
                 |then part_daytime
                 |else '0000-00-00'
                 |end as p_daytime
                 |from  spark_hive_org_tdapp_activitynew
           """.stripMargin)
            println("#### JOB_HV_58 分区数据插入完成的时间为：" + DateUtils.getCurrentSystemTime())
          } else {
            println(s"#### JOB_HV_58 read $up_namenode/ 无数据！")
          }
          today_dt = DateUtils.addOneDay(today_dt)
        }
      }
    }
  }


  /**
    * hive-job-59 2016-11-16
    * org_tdapp_devicenew to hive_org_tdapp_devicenew
    *
    * @author Xue
    * @param sqlContext
    */

  def JOB_HV_59(implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("#### JOB_HV_59(org_tdapp_devicenew to hive_org_tdapp_devicenew)")

    DateUtils.timeCost("JOB_HV_59") {
      var today_dt = start_dt
      if (interval >= 0) {
        println(s"#### JOB_HV_59  spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())

        for (i <- 0 to interval) {
          val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/incident/td/hive_org_tdapp_devicenew/part_updays=$today_dt")
          println(s"#### JOB_HV_59 read $up_namenode/ 读取大数据平台数据完成时间为:" + DateUtils.getCurrentSystemTime())

          df.registerTempTable("spark_hive_org_tdapp_devicenew")
          println("#### JOB_HV_59 registerTempTable--spark_hive_org_tdapp_devicenew 完成的系统时间为:" + DateUtils.getCurrentSystemTime())

          if (!Option(df).isEmpty) {

            sqlContext.sql(s"use $hive_dbname")

            println(s"#### JOB_HV_59 删除大数据平台分区(part_updays='$today_dt')数据, 时间为:" + DateUtils.getCurrentSystemTime())
            sqlContext.sql(s"alter table hive_org_tdapp_devicenew drop partition(part_updays='$today_dt')")

            sqlContext.sql(
              s"""
                 |insert overwrite table hive_org_tdapp_devicenew partition (part_updays,part_daytime)
                 |select
                 |loguuid         ,
                 |developerid     ,
                 |productid       ,
                 |platformid      ,
                 |partnerid       ,
                 |appversion      ,
                 |tduserid        ,
                 |mobileid        ,
                 |channel         ,
                 |os              ,
                 |pixel           ,
                 |countryid       ,
                 |provinceid      ,
                 |isp             ,
                 |language        ,
                 |jailbroken      ,
                 |cracked         ,
                 |starttime_hour  ,
                 |starttime_day   ,
                 |starttime_week  ,
                 |starttime_month ,
                 |starttime_year  ,
                 |return_status   ,
                 |'$today_dt'     ,
                 |case
                 |when
                 |substr(part_daytime,1,4) between '0001' and '9999' and substr(part_daytime,6,2) between '01' and '12' and
                 |substr(part_daytime,9,2) between '01' and '31'
                 |then part_daytime
                 |else '0000-00-00'
                 |end as p_daytime
                 |from  spark_hive_org_tdapp_devicenew
           """.stripMargin)
            println("#### JOB_HV_59 分区数据插入完成的时间为：" + DateUtils.getCurrentSystemTime())
          } else {
            println(s"#### JOB_HV_59 read $up_namenode/ 无数据！")
          }
          today_dt = DateUtils.addOneDay(today_dt)
        }
      }
    }
  }


  /**
    * hive-job-60 2016-11-17
    * org_tdapp_eventnew to hive_org_tdapp_eventnew
    *
    * @author Xue
    * @param sqlContext
    */

  def JOB_HV_60(implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("#### JOB_HV_60(org_tdapp_eventnew to hive_org_tdapp_eventnew)")

    DateUtils.timeCost("JOB_HV_60") {
      var today_dt = start_dt
      if (interval >= 0) {
        println(s"#### JOB_HV_60  spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())

        for (i <- 0 to interval) {
          val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/incident/td/hive_org_tdapp_eventnew/part_updays=$today_dt")
          println(s"#### JOB_HV_60 read $up_namenode/ 读取大数据平台数据完成时间为:" + DateUtils.getCurrentSystemTime())

          df.registerTempTable("spark_hive_org_tdapp_eventnew")
          println("#### JOB_HV_60 registerTempTable--spark_hive_org_tdapp_eventnew 完成的系统时间为:" + DateUtils.getCurrentSystemTime())

          if (!Option(df).isEmpty) {
            sqlContext.sql(s"use $hive_dbname")
            println(s"#### JOB_HV_60 删除大数据平台分区(part_updays='$today_dt')数据, 时间为:" + DateUtils.getCurrentSystemTime())
            sqlContext.sql(s"alter table hive_org_tdapp_eventnew drop partition(part_updays='$today_dt')")
            sqlContext.sql(
              s"""
                 |insert overwrite table hive_org_tdapp_eventnew partition (part_updays,part_daytime)
                 |select
                 |loguuid          ,
                 |developerid      ,
                 |productid        ,
                 |platformid       ,
                 |partnerid        ,
                 |appversion       ,
                 |tduserid         ,
                 |mobileid         ,
                 |channel          ,
                 |os               ,
                 |pixel            ,
                 |countryid        ,
                 |provinceid       ,
                 |isp              ,
                 |language         ,
                 |jailbroken       ,
                 |cracked          ,
                 |starttime_hour   ,
                 |starttime_day    ,
                 |starttime_week   ,
                 |starttime_month  ,
                 |starttime_year   ,
                 |'$today_dt'      ,
                 |case
                 |when
                 |substr(part_daytime,1,4) between '0001' and '9999' and substr(part_daytime,6,2) between '01' and '12' and
                 |substr(part_daytime,9,2) between '01' and '31'
                 |then part_daytime
                 |else '0000-00-00'
                 |end as p_daytime
                 |from  spark_hive_org_tdapp_eventnew
           """.stripMargin)
            println("#### JOB_HV_60 分区数据插入完成的时间为：" + DateUtils.getCurrentSystemTime())
          } else {
            println(s"#### JOB_HV_60 read $up_namenode 无数据！")
          }
          today_dt = DateUtils.addOneDay(today_dt)
        }
      }
    }
  }


  /**
    * hive-job-61 2016-11-22
    * org_tdapp_exceptionnew to hive_org_tdapp_exceptionnew
    *
    * @author Xue
    * @param sqlContext
    */
  def JOB_HV_61(implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("#### JOB_HV_61(org_tdapp_exceptionnew to hive_org_tdapp_exceptionnew)")

    DateUtils.timeCost("JOB_HV_61") {
      var today_dt = start_dt
      if (interval >= 0){
        println(s"#### JOB_HV_61  spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())

        for (i <- 0 to interval) {
          val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/incident/td/hive_org_tdapp_exceptionnew/part_updays=$today_dt")
          println(s"#### JOB_HV_61 read $up_namenode/ 读取大数据平台数据完成时间为:" + DateUtils.getCurrentSystemTime())

          df.registerTempTable("spark_hive_org_tdapp_exceptionnew")
          println("#### JOB_HV_61 registerTempTable--spark_hive_org_tdapp_exceptionnew 完成的系统时间为:" + DateUtils.getCurrentSystemTime())

          if (!Option(df).isEmpty) {
            sqlContext.sql(s"use $hive_dbname")
            println(s"#### JOB_HV_61 删除大数据平台分区(part_updays='$today_dt')数据, 时间为:" + DateUtils.getCurrentSystemTime())
            sqlContext.sql(s"alter table hive_org_tdapp_exceptionnew drop partition(part_updays='$today_dt')")

            sqlContext.sql(
              s"""
                 |insert overwrite table hive_org_tdapp_exceptionnew partition (part_updays,part_daytime)
                 |select
                 |loguuid             ,
                 |developerid         ,
                 |productid           ,
                 |platformid          ,
                 |partnerid           ,
                 |appversion          ,
                 |tduserid            ,
                 |mobileid            ,
                 |channel             ,
                 |os                  ,
                 |pixel               ,
                 |countryid           ,
                 |provinceid          ,
                 |isp                 ,
                 |language            ,
                 |jailbroken          ,
                 |cracked             ,
                 |starttime_hour      ,
                 |starttime_day       ,
                 |starttime_week      ,
                 |starttime_month     ,
                 |starttime_year      ,
                 |return_status       ,
                 |'$today_dt'         ,
                 |case
                 |when
                 |substr(part_daytime,1,4) between '0001' and '9999' and substr(part_daytime,6,2) between '01' and '12' and
                 |substr(part_daytime,9,2) between '01' and '31'
                 |then part_daytime
                 |else '0000-00-00'
                 |end as p_daytime
                 |from  spark_hive_org_tdapp_exceptionnew
           """.stripMargin)
            println("#### JOB_HV_61 分区数据插入完成的时间为：" + DateUtils.getCurrentSystemTime())
          } else {
            println(s"#### JOB_HV_61 read $up_namenode/ 无数据！")
          }
          today_dt = DateUtils.addOneDay(today_dt)
        }
      }
    }
  }


  /**
    * hive-job-62 2016-11-22
    * org_tdapp_tlaunchnew to hive_org_tdapp_tlaunchnew
    *
    * @author Xue
    * @param sqlContext
    */

  def JOB_HV_62(implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("#### JOB_HV_62(org_tdapp_tlaunchnew to hive_org_tdapp_tlaunchnew)")

    DateUtils.timeCost("JOB_HV_62") {
      var today_dt = start_dt
      if (interval >= 0) {
        println(s"#### JOB_HV_62  spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())

        for (i <- 0 to interval) {
          val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/incident/td/hive_org_tdapp_tlaunchnew/part_updays=$today_dt")
          println(s"#### JOB_HV_62 read $up_namenode/ 读取大数据平台数据完成时间为:" + DateUtils.getCurrentSystemTime())

          df.registerTempTable("spark_hive_org_tdapp_tlaunchnew")
          println("#### JOB_HV_62 registerTempTable--spark_hive_org_tdapp_tlaunchnew 完成的系统时间为:" + DateUtils.getCurrentSystemTime())

          if (!Option(df).isEmpty) {
            sqlContext.sql(s"use $hive_dbname")
            println(s"#### JOB_HV_62 删除大数据平台分区(part_updays='$today_dt')数据, 时间为:" + DateUtils.getCurrentSystemTime())
            sqlContext.sql(s"alter table hive_org_tdapp_tlaunchnew drop partition(part_updays='$today_dt')")

            sqlContext.sql(
              s"""
                 |insert overwrite table hive_org_tdapp_tlaunchnew partition (part_updays,part_daytime)
                 |select
                 |loguuid             ,
                 |developerid         ,
                 |productid           ,
                 |platformid          ,
                 |partnerid           ,
                 |appversion          ,
                 |tduserid            ,
                 |mobileid            ,
                 |channel             ,
                 |os                  ,
                 |pixel               ,
                 |countryid           ,
                 |provinceid          ,
                 |isp                 ,
                 |language            ,
                 |jailbroken          ,
                 |cracked             ,
                 |starttime_hour      ,
                 |starttime_day       ,
                 |starttime_week      ,
                 |starttime_month     ,
                 |starttime_year      ,
                 |'$today_dt'         ,
                 |case
                 |when
                 |substr(part_daytime,1,4) between '0001' and '9999' and substr(part_daytime,6,2) between '01' and '12' and
                 |substr(part_daytime,9,2) between '01' and '31'
                 |then part_daytime
                 |else '0000-00-00'
                 |end as p_daytime
                 |from  spark_hive_org_tdapp_tlaunchnew
           """.stripMargin)
            println("#### JOB_HV_62 分区数据插入完成的时间为：" + DateUtils.getCurrentSystemTime())
          } else {
            println(s"#### JOB_HV_62 read $up_namenode/ 无数据！")
          }
          today_dt = DateUtils.addOneDay(today_dt)
        }
      }
    }
  }


  /**
    * hive-job-63 2016-11-22
    * org_tdapp_device to hive_org_tdapp_device
    *
    * @author Xue
    * @param sqlContext
    */

  def JOB_HV_63(implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("#### JOB_HV_63(org_tdapp_device to hive_org_tdapp_device)")

    DateUtils.timeCost("JOB_HV_63") {
      var today_dt = start_dt
      if (interval >= 0) {
        println(s"#### JOB_HV_63  spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())

        for (i <- 0 to interval) {
          val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/incident/td/hive_org_tdapp_device/part_updays=$today_dt")
          println(s"#### JOB_HV_63 read $up_namenode/ 读取大数据平台数据完成时间为:" + DateUtils.getCurrentSystemTime())

          df.registerTempTable("spark_hive_org_tdapp_device")
          println("#### JOB_HV_63 registerTempTable--spark_hive_org_tdapp_device 完成的系统时间为:" + DateUtils.getCurrentSystemTime())

          if (!Option(df).isEmpty) {
            sqlContext.sql(s"use $hive_dbname")
            println(s"#### JOB_HV_63 删除大数据平台分区(part_updays='$today_dt')数据, 时间为:" + DateUtils.getCurrentSystemTime())
            sqlContext.sql(s"alter table hive_org_tdapp_device drop partition(part_updays='$today_dt')")
            sqlContext.sql(
              s"""
                 |insert overwrite table hive_org_tdapp_device partition (part_updays,part_daytime)
                 |select
                 |loguuid              ,
                 |developerid          ,
                 |productid            ,
                 |platformid           ,
                 |partnerid            ,
                 |appversion           ,
                 |tduserid             ,
                 |eventid              ,
                 |starttime            ,
                 |starttime_hour       ,
                 |starttime_day        ,
                 |starttime_week       ,
                 |starttime_month      ,
                 |starttime_year       ,
                 |'$today_dt'          ,
                 |case
                 |when
                 |substr(part_daytime,1,4) between '0001' and '9999' and substr(part_daytime,6,2) between '01' and '12' and
                 |substr(part_daytime,9,2) between '01' and '31'
                 |then part_daytime
                 |else '0000-00-00'
                 |end as p_daytime
                 |from  spark_hive_org_tdapp_device
           """.stripMargin)
            println("#### JOB_HV_63 分区数据插入完成的时间为：" + DateUtils.getCurrentSystemTime())
          } else {
            println(s"#### JOB_HV_63 read $up_namenode/ 无数据！")
          }
          today_dt = DateUtils.addOneDay(today_dt)
        }
      }
    }
  }


  /**
    * hive-job-64 2016-11-25
    * org_tdapp_exception to hive_org_tdapp_exception
    *
    * @author Xue
    * @param sqlContext
    */

  def JOB_HV_64(implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("#### JOB_HV_64(org_tdapp_exception to hive_org_tdapp_exception)")

    DateUtils.timeCost("JOB_HV_64") {
      var today_dt = start_dt
      if (interval >= 0) {
        println(s"#### JOB_HV_64  spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())

        for (i <- 0 to interval) {
          val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/incident/td/hive_org_tdapp_exception/part_updays=$today_dt")
          println(s"#### JOB_HV_64 read $up_namenode/ 读取大数据平台数据完成时间为:" + DateUtils.getCurrentSystemTime())

          df.registerTempTable("spark_hive_org_tdapp_exception")
          println("#### JOB_HV_64 registerTempTable--spark_hive_org_tdapp_exception 完成的系统时间为:" + DateUtils.getCurrentSystemTime())

          if (!Option(df).isEmpty) {
            sqlContext.sql(s"use $hive_dbname")
            println(s"#### JOB_HV_64 删除大数据平台分区(part_updays='$today_dt')数据, 时间为:" + DateUtils.getCurrentSystemTime())
            sqlContext.sql(s"alter table hive_org_tdapp_exception drop partition(part_updays='$today_dt')")
            sqlContext.sql(
              s"""
                 |insert overwrite table hive_org_tdapp_exception partition (part_updays,part_daytime)
                 |select
                 |loguuid          ,
                 |developerid      ,
                 |productid        ,
                 |platformid       ,
                 |appversion       ,
                 |tduserid         ,
                 |os               ,
                 |osversion        ,
                 |mobileid         ,
                 |errorname        ,
                 |errormessage     ,
                 |errcount         ,
                 |hashcode         ,
                 |starttime        ,
                 |starttime_hour   ,
                 |starttime_day    ,
                 |starttime_week   ,
                 |starttime_month  ,
                 |starttime_year   ,
                 |'$today_dt'      ,
                 |case
                 |when
                 |substr(part_daytime,1,4) between '0001' and '9999' and substr(part_daytime,6,2) between '01' and '12' and
                 |substr(part_daytime,9,2) between '01' and '31'
                 |then part_daytime
                 |else '0000-00-00'
                 |end as p_daytime
                 |from  spark_hive_org_tdapp_exception
           """.stripMargin)
            println("#### JOB_HV_64 分区数据插入完成的时间为：" + DateUtils.getCurrentSystemTime())
          } else {
            println(s"#### JOB_HV_64 read $up_namenode/ 无数据！")
          }
          today_dt = DateUtils.addOneDay(today_dt)
        }
      }
    }
  }


  /**
    * hive-job-65 2016-11-25
    * org_tdapp_newuser to hive_org_tdapp_newuser
    *
    * @author Xue
    * @param sqlContext
    */
  def JOB_HV_65(implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("#### JOB_HV_65(org_tdapp_newuser to hive_org_tdapp_newuser)")

    DateUtils.timeCost("JOB_HV_65") {
      var today_dt = start_dt
      if (interval >= 0) {
        println(s"#### JOB_HV_65  spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())

        for (i <- 0 to interval) {
          val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/incident/td/hive_org_tdapp_newuser/part_updays=$today_dt")
          println(s"#### JOB_HV_65 read $up_namenode 读取大数据平台数据完成时间为:" + DateUtils.getCurrentSystemTime())

          df.registerTempTable("spark_hive_org_tdapp_newuser")
          println("#### JOB_HV_65 registerTempTable--spark_hive_org_tdapp_newuser 完成的系统时间为:" + DateUtils.getCurrentSystemTime())

          if (!Option(df).isEmpty) {
            sqlContext.sql(s"use $hive_dbname")
            println(s"#### JOB_HV_65 删除大数据平台分区(part_updays='$today_dt')数据, 时间为:" + DateUtils.getCurrentSystemTime())
            sqlContext.sql(s"alter table hive_org_tdapp_newuser drop partition(part_updays='$today_dt')")
            sqlContext.sql(
              s"""
                 |insert overwrite table hive_org_tdapp_newuser partition (part_updays,part_daytime,from_table)
                 |select
                 |loguuid           ,
                 |developerid       ,
                 |productid         ,
                 |platformid        ,
                 |partnerid         ,
                 |appversion        ,
                 |tduserid          ,
                 |mobileid          ,
                 |channel           ,
                 |os                ,
                 |pixel             ,
                 |countryid         ,
                 |provinceid        ,
                 |isp               ,
                 |language          ,
                 |jailbroken        ,
                 |cracked           ,
                 |starttime_hour    ,
                 |starttime_day     ,
                 |starttime_week    ,
                 |starttime_month   ,
                 |starttime_year    ,
                 |'$today_dt'       ,
                 |case
                 |when
                 |substr(part_daytime,1,4) between '0001' and '9999' and substr(part_daytime,6,2) between '01' and '12' and
                 |substr(part_daytime,9,2) between '01' and '31'
                 |then part_daytime
                 |else '0000-00-00'
                 |end as p_daytime,
                 |from_table
                 |from  spark_hive_org_tdapp_newuser
           """.stripMargin)
            println("#### JOB_HV_65 分区数据插入完成的时间为：" + DateUtils.getCurrentSystemTime())
          } else {
            println(s"#### JOB_HV_65 read $up_namenode 无数据！")
          }
          today_dt = DateUtils.addOneDay(today_dt)
        }
      }
    }
  }



  /**
    * hive-job-71 2016-11-28
    * hive_ach_order_inf -> hbkdb.rtdtrs_dtl_ach_order_inf
    *
    * 测试时段:2015-11-09~2015-11-10
    *
    * @author tzq
    * @param sqlContext
    * @param end_dt
    * @param start_dt
    * @param interval
    */
  def JOB_HV_71(implicit sqlContext: HiveContext, start_dt: String, end_dt: String, interval: Int) = {
    println("#### JOB_HV_71( hive_ach_order_inf <- hbkdb.rtdtrs_dtl_ach_order_inf) #####")
    DateUtils.timeCost("JOB_HV_71") {
      var part_dt = start_dt
      sqlContext.sql(s"use $hive_dbname")
      if (interval >= 0) {
        for (i <- 0 to interval) {
          println(s"#### JOB_HV_71 从落地表抽取数据开始时间为:" + DateUtils.getCurrentSystemTime())
          val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/incident/order/hive_ach_order_inf/part_hp_trans_dt=$part_dt")
          println(s"#### read $up_namenode at partition= $part_dt successful ######")
          df.registerTempTable("spark_hive_ach_order_inf")

          sqlContext.sql(s"use $hive_dbname")
          println(s"#### JOB_HV_71 删除一级分区，时间为:" + DateUtils.getCurrentSystemTime())
          sqlContext.sql(s"alter table hive_ach_order_inf drop partition(part_trans_dt='$part_dt')")

          println(s"#### JOB_HV_71 自动分区插入大数据平台，开始时间为:" + DateUtils.getCurrentSystemTime())
          sqlContext.sql(
            s"""
               |insert overwrite table hive_ach_order_inf partition(part_hp_trans_dt,part_trans_dt)
               |select
               |order_id             ,
               |sys_no               ,
               |mchnt_version        ,
               |encoding             ,
               |sign_method          ,
               |mchnt_trans_tp       ,
               |biz_tp               ,
               |pay_method           ,
               |trans_tp             ,
               |buss_chnl            ,
               |mchnt_front_url      ,
               |mchnt_back_url       ,
               |acq_ins_id_cd        ,
               |mchnt_cd             ,
               |mchnt_tp             ,
               |mchnt_nm             ,
               |sub_mchnt_cd         ,
               |sub_mchnt_nm         ,
               |mchnt_order_id       ,
               |trans_tm             ,
               |trans_dt             ,
               |sys_tm               ,
               |pay_timeout          ,
               |trans_at             ,
               |trans_curr_cd        ,
               |kz_at                ,
               |kz_curr_cd           ,
               |conv_dt              ,
               |deduct_at            ,
               |discount_info        ,
               |upoint_at            ,
               |top_info             ,
               |refund_at            ,
               |iss_ins_id_cd        ,
               |iss_head             ,
               |pri_acct_no          ,
               |card_attr            ,
               |usr_id               ,
               |phone_no             ,
               |trans_ip             ,
               |trans_st             ,
               |trans_no             ,
               |trans_idx            ,
               |sys_tra_no           ,
               |order_desc           ,
               |order_detail         ,
               |proc_sys             ,
               |proc_st              ,
               |trans_source         ,
               |resp_cd              ,
               |other_usr            ,
               |initial_pay          ,
               |to_ts                ,
               |rec_crt_ts           ,
               |rec_upd_ts           ,
               |'$part_dt'             ,
               |part_trans_dt
               |from
               |spark_hive_ach_order_inf
       """.stripMargin)

          println(s"#### JOB_HV_71 自动分区插入大数据平台，完成时间为:" + DateUtils.getCurrentSystemTime())

          part_dt = DateUtils.addOneDay(part_dt)
        }
      }
    }

  }

  /**
    * JOB_HIVE_90  2017-3-9
    * hbkdb.rtdtrs_dtl_ach_bill --> upw_hive.hive_rtdtrs_dtl_ach_bill
    * notice: 测试分区 2016-07-19
    *
    * @author tzq
    * @param sqlContext
    * @param end_dt
    * @param start_dt
    * @param interval
    */
  def JOB_HV_90(implicit sqlContext: HiveContext,start_dt: String,end_dt:String,interval:Int) = {
    println("#### JOB_HV_90( hive_rtdtrs_dtl_ach_bill <- hbkdb.rtdtrs_dtl_ach_order_inf) #####")
    DateUtils.timeCost("JOB_HV_90") {
      var part_dt = start_dt
      sqlContext.sql(s"use $hive_dbname")

      if(interval >= 0){
        for(i <- 0 to interval){
          println(s"#### JOB_HV_90 从落地表抽取数据开始时间为:" + DateUtils.getCurrentSystemTime())
          val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/incident/order/hive_rtdtrs_dtl_ach_bill/part_settle_dt=$part_dt")
          println(s"#### read $up_namenode at partition= $part_dt successful ######")
          df.registerTempTable("spark_hive_rtdtrs_dtl_ach_bill")

          sqlContext.sql(s"use $hive_dbname")

          println(s"#### JOB_HV_90 删除分区，时间为:" + DateUtils.getCurrentSystemTime())
          sqlContext.sql(s"alter table hive_rtdtrs_dtl_ach_bill drop partition(part_settle_dt='$part_dt')")

          println(s"#### JOB_HV_90 自动分区插入大数据平台，开始时间为:" + DateUtils.getCurrentSystemTime())
          sqlContext.sql(
            s"""
               |insert overwrite table hive_rtdtrs_dtl_ach_bill partition(part_settle_dt)
               |select
               |settle_dt,
               |trans_no,
               |trans_id,
               |trans_idx,
               |mchnt_version,
               |encoding,
               |cert_id,
               |sign_method,
               |trans_tp,
               |sub_trans_tp,
               |in_trans_tp,
               |biz_tp,
               |buss_chnl,
               |access_tp,
               |acq_ins_id_cd,
               |mchnt_cd,
               |mchnt_nm,
               |mchnt_abbr,
               |sub_mchnt_cd,
               |sub_mchnt_nm,
               |sub_mchnt_abbr,
               |mchnt_order_id,
               |mchnt_tp,
               |t_mchnt_cd,
               |buss_order_id,
               |rel_trans_idx,
               |trans_tm,
               |trans_dt,
               |trans_md,
               |carrier_tp,
               |pri_acct_no,
               |trans_at,
               |points_at,
               |trans_curr_cd,
               |trans_st,
               |pay_timeout,
               |term_id,
               |token_id,
               |iss_ins_id_cd,
               |card_attr,
               |proc_st,
               |resp_cd,
               |proc_sys,
               |iss_head,
               |iss_head_nm,
               |pay_method,
               |advance_payment,
               |auth_id,
               |usr_id,
               |trans_class,
               |out_trans_tp,
               |fwd_ins_id_cd,
               |rcv_ins_id_cd,
               |pos_cond_cd,
               |trans_source,
               |verify_mode,
               |iss_ins_tp,
               |conv_dt,
               |settle_at,
               |settle_curr_cd,
               |settle_conv_rt,
               |achis_settle_dt,
               |kz_at,
               |kz_conv_rt,
               |kz_curr_cd,
               |refund_at,
               |mchnt_country,
               |name,
               |phone_no,
               |trans_ip,
               |sys_tra_no,
               |sys_tm,
               |client_id,
               |buss_tp,
               |card_risk_flag,
               |ebank_order_num,
               |order_desc,
               |usr_num_tp,
               |usr_num,
               |area_cd,
               |addn_area_cd,
               |reserve_fld,
               |db_tag,
               |db_adtnl,
               |rec_crt_ts,
               |rec_upd_ts,
               |field1,
               |field2,
               |field3,
               |customer_email,
               |bind_id,
               |enc_cert_id,
               |mchnt_front_url,
               |mchnt_back_url,
               |req_reserved,
               |reserved,
               |log_id,
               |mail_no,
               |icc_data2,
               |token_data,
               |bill_data,
               |acct_data,
               |risk_data,
               |req_pri_data,
               |order_detail,
               |'$part_dt'
               |from
               |spark_hive_rtdtrs_dtl_ach_bill
       """.stripMargin)

          println(s"#### JOB_HV_90 自动分区插入大数据平台，完成时间为:" + DateUtils.getCurrentSystemTime())

          part_dt=DateUtils.addOneDay(part_dt)
        }
      }
    }
    }

  /**
      * JOB_HV_91  2017-3-22
      * hbkdb.mtdtrs_dtl_ach_bat_file --> upw_hive.hive_mtdtrs_dtl_ach_bat_file
      * notice: 测试分区(源头分区hp_settle_dt=20151127  24 条数据)
    *
    * @author tzq
      * @param sqlContext
      * @param end_dt
      * @param start_dt
      * @param interval
      */
    def JOB_HV_91(implicit sqlContext: HiveContext, start_dt: String, end_dt: String, interval: Int) = {
      println("#### JOB_HV_91( hbkdb.mtdtrs_dtl_ach_bat_file --> upw_hive.hive_mtdtrs_dtl_ach_bat_file) #####")
      DateUtils.timeCost("JOB_HV_91") {
        var part_dt = start_dt
        sqlContext.sql(s"use $hive_dbname")

        if (interval >= 0) {
          for (i <- 0 to interval) {
            println(s"#### JOB_HV_91 从落地表抽取数据开始时间为:" + DateUtils.getCurrentSystemTime())
            val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/incident/trans/hive_mtdtrs_dtl_ach_bat_file/part_settle_dt=$part_dt")
            println(s"#### read $up_namenode at partition= $part_dt successful ######")
            df.registerTempTable("spark_hive_mtdtrs_dtl_ach_bat_file")

            sqlContext.sql(s"use $hive_dbname")

            println(s"#### JOB_HV_91 删除分区，时间为:" + DateUtils.getCurrentSystemTime())
            sqlContext.sql(s"alter table hive_mtdtrs_dtl_ach_bat_file drop partition(part_settle_dt='$part_dt')")

            println(s"#### JOB_HV_91 自动分区插入大数据平台，开始时间为:" + DateUtils.getCurrentSystemTime())
            sqlContext.sql(
              s"""
                 |insert overwrite table hive_mtdtrs_dtl_ach_bat_file partition(part_settle_dt)
                 |select
                 |bat_id            ,
                 |out_bat_id        ,
                 |machine_id        ,
                 |file_nm           ,
                 |file_path         ,
                 |rs_file_path      ,
                 |download_num      ,
                 |file_prio         ,
                 |file_tp           ,
                 |file_st           ,
                 |file_source       ,
                 |proc_begin_ts     ,
                 |proc_end_ts       ,
                 |proc_succ_num     ,
                 |proc_succ_at      ,
                 |mchnt_cd          ,
                 |mchnt_acct_no     ,
                 |acbat_settle_dt   ,
                 |trans_sum_num     ,
                 |verified_num      ,
                 |trans_sum_at      ,
                 |file_send_dt      ,
                 |oper_id           ,
                 |auth_oper_id      ,
                 |file_head_info    ,
                 |upload_oper       ,
                 |acq_ins_id_cd     ,
                 |file_req_sn       ,
                 |fail_reason       ,
                 |acq_audit_oper_id ,
                 |acq_audit_ts      ,
                 |acq_adjust_oper_id,
                 |acq_adjust_ts     ,
                 |version           ,
                 |reserve1          ,
                 |reserve2          ,
                 |rec_upd_oper_id   ,
                 |rec_upd_trans_id  ,
                 |chnl_tp           ,
                 |conn_md           ,
                 |rec_crt_ts        ,
                 |rec_upd_ts        ,
                 |'$part_dt'
                 |from
                 |spark_hive_mtdtrs_dtl_ach_bat_file
         """.stripMargin)

            println(s"#### JOB_HV_91 自动分区插入大数据平台，完成时间为:" + DateUtils.getCurrentSystemTime())
            part_dt = DateUtils.addOneDay(part_dt)
          }
        }
      }
    }



  /**
    * JOB_HV_92  2017-3-23
    * hbkdb.mtdtrs_dtl_ach_bat_file_dtl --> upw_hive.hive_mtdtrs_dtl_ach_bat_file_dtl
    * notice: 测试分区(源头分区hp_settle_dt=20151127  27 条数据)
    *
    * @author tzq
    * @param sqlContext
    * @param end_dt
    * @param start_dt
    * @param interval
    */
  def JOB_HV_92(implicit sqlContext: HiveContext, start_dt: String, end_dt: String, interval: Int) = {
    println("#### JOB_HV_92( hbkdb.mtdtrs_dtl_ach_bat_file --> upw_hive.hive_mtdtrs_dtl_ach_bat_file_dtl) #####")
    DateUtils.timeCost("JOB_HV_92") {
      var part_dt = start_dt
      sqlContext.sql(s"use $hive_dbname")

      if (interval >= 0) {
        for (i <- 0 to interval) {
          println(s"#### JOB_HV_92 从落地表抽取数据开始时间为:" + DateUtils.getCurrentSystemTime())
          val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/incident/trans/hive_mtdtrs_dtl_ach_bat_file_dtl/part_settle_dt=$part_dt")
          println(s"#### read $up_namenode at partition= $part_dt successful ######")
          df.registerTempTable("spark_hive_mtdtrs_dtl_ach_bat_file_dtl")

          sqlContext.sql(s"use $hive_dbname")

          println(s"#### JOB_HV_92 删除分区，时间为:" + DateUtils.getCurrentSystemTime())
          sqlContext.sql(s"alter table hive_mtdtrs_dtl_ach_bat_file_dtl drop partition(part_settle_dt='$part_dt')")

          println(s"#### JOB_HV_92 自动分区插入大数据平台，开始时间为:" + DateUtils.getCurrentSystemTime())
          sqlContext.sql(
            s"""
               |insert overwrite table hive_mtdtrs_dtl_ach_bat_file_dtl partition(part_settle_dt)
               |select
               |settle_dt         ,
               |trans_idx         ,
               |bat_id            ,
               |mchnt_cd          ,
               |mchnt_tp          ,
               |mchnt_addr        ,
               |trans_tp          ,
               |buss_tp           ,
               |proc_st           ,
               |proc_sys          ,
               |mchnt_order_id    ,
               |usr_id_tp         ,
               |usr_id            ,
               |cvn2              ,
               |expire_dt         ,
               |bill_tp           ,
               |bill_no           ,
               |trans_at          ,
               |fee_at            ,
               |trans_tm          ,
               |trans_curr_cd     ,
               |pay_acct          ,
               |pay_acct_tp       ,
               |pri_acct_no       ,
               |iss_ins_id_cd     ,
               |iss_ins_nm        ,
               |customer_nm       ,
               |customer_mobile   ,
               |customer_email    ,
               |org_order_id      ,
               |org_trans_tm      ,
               |org_trans_at      ,
               |refund_rsn        ,
               |cert_tp           ,
               |cert_id           ,
               |conn_md           ,
               |product_info      ,
               |acct_balance      ,
               |out_trans_idx     ,
               |org_trans_idx     ,
               |sys_tra_no        ,
               |sys_tm            ,
               |retri_ref_no      ,
               |fwd_ins_id_cd     ,
               |rcv_ins_id_cd     ,
               |trans_method      ,
               |trans_terminal_tp ,
               |svr_cond_cd       ,
               |sd_tag            ,
               |bill_interval     ,
               |resp_cd           ,
               |org_rec_info      ,
               |comments          ,
               |machine_id        ,
               |file_prio         ,
               |file_tp           ,
               |trans_chnl        ,
               |chnl_tp           ,
               |resp_desc         ,
               |broker_seq        ,
               |iss_province      ,
               |iss_city          ,
               |acq_ins_id_cd     ,
               |dtl_req_sn        ,
               |reserve1          ,
               |reserve2          ,
               |reserve3          ,
               |reserve4          ,
               |rec_upd_oper_id   ,
               |rec_upd_trans_id  ,
               |rec_crt_ts        ,
               |rec_upd_ts        ,
               |'$part_dt'
               |from
               |spark_hive_mtdtrs_dtl_ach_bat_file_dtl
         """.stripMargin)

          println(s"#### JOB_HV_92 自动分区插入大数据平台，完成时间为:" + DateUtils.getCurrentSystemTime())
          part_dt = DateUtils.addOneDay(part_dt)
        }
      }
    }
  }

  /**
    * JOB_HV_93  2017-3-27
    * hbkdb.mtdtrs_dtl_ach_bat_file_dtl --> upw_hive.hive_rtdtrs_dtl_achis_bill
    * notice: 测试分区(源头分区hp_settle_dt=20150601 无数据) 待测试
    *
    * @author tzq
    * @param sqlContext
    * @param end_dt
    * @param start_dt
    * @param interval
    */
  def JOB_HV_93(implicit sqlContext: HiveContext, start_dt: String, end_dt: String, interval: Int) = {
    println("#### JOB_HV_93( hbkdb.mtdtrs_dtl_ach_bat_file --> upw_hive.hive_rtdtrs_dtl_achis_bill) #####")
    DateUtils.timeCost("JOB_HV_93") {
      var part_dt = start_dt
      sqlContext.sql(s"use $hive_dbname")

      if (interval >= 0) {
        for (i <- 0 to interval) {
          println(s"#### JOB_HV_93 从落地表抽取数据开始时间为:" + DateUtils.getCurrentSystemTime())
          val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/incident/trans/hive_rtdtrs_dtl_achis_bill/part_settle_dt=$part_dt")
          println(s"#### read $up_namenode at partition= $part_dt successful ######")
          df.registerTempTable("spark_hive_rtdtrs_dtl_achis_bill")

          sqlContext.sql(s"use $hive_dbname")

          println(s"#### JOB_HV_93 删除分区，时间为:" + DateUtils.getCurrentSystemTime())
          sqlContext.sql(s"alter table hive_rtdtrs_dtl_achis_bill drop partition(part_settle_dt='$part_dt')")

          println(s"#### JOB_HV_93 自动分区插入大数据平台，开始时间为:" + DateUtils.getCurrentSystemTime())
          sqlContext.sql(
            s"""
               |insert overwrite table hive_rtdtrs_dtl_achis_bill partition(part_settle_dt)
               |settle_dt        ,
               |trans_idx        ,
               |acq_trans_idx    ,
               |fwd_sys_cd       ,
               |trans_tp         ,
               |trans_class      ,
               |pri_acct_no      ,
               |acq_ins_id_cd    ,
               |fwd_ins_id_cd    ,
               |iss_ins_id_cd    ,
               |iss_head         ,
               |iss_head_nm      ,
               |mchnt_cd         ,
               |mchnt_nm         ,
               |mchnt_country    ,
               |mchnt_url        ,
               |mchnt_front_url  ,
               |mchnt_back_url   ,
               |mchnt_delv_tag   ,
               |mchnt_tp         ,
               |mchnt_risk_tag   ,
               |mchnt_order_id   ,
               |sys_tra_no       ,
               |sys_tm           ,
               |trans_tm         ,
               |trans_dt         ,
               |trans_at         ,
               |trans_curr_cd    ,
               |trans_st         ,
               |refund_at        ,
               |auth_id          ,
               |settle_at        ,
               |settle_curr_cd   ,
               |settle_conv_rt   ,
               |conv_dt          ,
               |cert_tp          ,
               |cert_id          ,
               |name             ,
               |phone_no         ,
               |org_trans_idx    ,
               |org_sys_tra_no   ,
               |org_sys_tm       ,
               |proc_st          ,
               |resp_cd          ,
               |proc_sys         ,
               |usr_id           ,
               |mchnt_id         ,
               |pay_method       ,
               |trans_ip         ,
               |trans_no         ,
               |encoding         ,
               |mac_addr         ,
               |card_attr        ,
               |kz_curr_cd       ,
               |kz_conv_rt       ,
               |kz_at            ,
               |sub_mchnt_cd     ,
               |sub_mchnt_nm     ,
               |verify_mode      ,
               |mchnt_reserve    ,
               |reserve          ,
               |mchnt_version    ,
               |biz_tp           ,
               |is_oversea       ,
               |reserve1         ,
               |reserve2         ,
               |reserve3         ,
               |reserve4         ,
               |reserve5         ,
               |reserve6         ,
               |rec_st           ,
               |comments         ,
               |rec_crt_ts       ,
               |rec_upd_ts       ,
               |tlr_st           ,
               |req_pri_data     ,
               |out_trans_tp     ,
               |org_out_trans_tp ,
               |ebank_id         ,
               |ebank_mchnt_cd   ,
               |ebank_order_num  ,
               |ebank_idx        ,
               |ebank_rsp_tm     ,
               |mchnt_conn_tp    ,
               |access_tp        ,
               |trans_source     ,
               |bind_id          ,
               |card_risk_flag   ,
               |buss_chnl        ,
               |'$part_dt'
               |from
               |spark_hive_rtdtrs_dtl_achis_bill
         """.stripMargin)

          println(s"#### JOB_HV_93 自动分区插入大数据平台，完成时间为:" + DateUtils.getCurrentSystemTime())
          part_dt = DateUtils.addOneDay(part_dt)
        }
      }
    }
  }


  /**
    * hive-job-94 2017-03-30
    * rtdtrs_dtl_achis_note -> hive_rtdtrs_dtl_achis_note
    *
    * @author Xue
    * @param sqlContext
    */
  def JOB_HV_94(implicit sqlContext: HiveContext,start_dt:String,end_dt: String, interval: Int) {
    println("#### JOB_HV_94(rtdtrs_dtl_achis_note -> hive_rtdtrs_dtl_achis_note)")

    var today_dt = start_dt
    if (interval >= 0) {
      println("#### JOB_HV_94 增量抽取的时间范围为: "+start_dt+ "-"+ end_dt)
      DateUtils.timeCost("JOB_HV_94") {
        val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/incident/trans/hive_rtdtrs_dtl_achis_note/part_settle_dt=$today_dt")
        println(s"#### JOB_HV_94 read $up_namenode/ 数据完成时间为:" + DateUtils.getCurrentSystemTime())

        df.registerTempTable("spark_hive_rtdtrs_dtl_achis_note")
        println("#### JOB_HV_94 registerTempTable--spark_hive_rtdtrs_dtl_achis_note 完成的系统时间为:" + DateUtils.getCurrentSystemTime())

        if (!Option(df).isEmpty) {
          sqlContext.sql(s"use $hive_dbname")
          sqlContext.sql(
            s"""
               |insert overwrite table hive_rtdtrs_dtl_achis_note partition (part_settle_dt)
               |select
               |settle_dt      ,
               |trans_no       ,
               |cli_tp         ,
               |cli_ver        ,
               |os_type        ,
               |os_ver         ,
               |phone_res      ,
               |locale         ,
               |imei           ,
               |uid            ,
               |up_vid         ,
               |up_uid         ,
               |sid            ,
               |ip             ,
               |gw_ip          ,
               |mac            ,
               |rec_crt_ts     ,
               |rec_upd_ts     ,
               |$today_dt
               |from
               |spark_hive_rtdtrs_dtl_achis_note
           """.stripMargin)
          println("#### JOB_HV_94 分区数据插入完成的时间为：" + DateUtils.getCurrentSystemTime())
        } else {
          println(s"#### JOB_HV_94 read $up_namenode/ 无数据！")
        }
        today_dt = DateUtils.addOneDay(today_dt)
      }
    }
  }

  /**
    * hive-job-95 2017-03-30
    * rtdtrs_dtl_achis_order -> hive_rtdtrs_dtl_achis_order
    *
    * @author Xue
    * @param sqlContext
    */
  def JOB_HV_95(implicit sqlContext: HiveContext,start_dt:String,end_dt: String, interval: Int) {
    println("#### JOB_HV_95(rtdtrs_dtl_achis_order -> hive_rtdtrs_dtl_achis_order)")

    var today_dt = start_dt
    if (interval >= 0) {
      println("#### JOB_HV_95 增量抽取的时间范围为: "+start_dt+ "-"+ end_dt)
      DateUtils.timeCost("JOB_HV_95") {
        val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/incident/trans/hive_rtdtrs_dtl_achis_order/part_settle_dt=$today_dt")
        println(s"#### JOB_HV_95 read $up_namenode/ 数据完成时间为:" + DateUtils.getCurrentSystemTime())

        df.registerTempTable("spark_hive_rtdtrs_dtl_achis_order")
        println("#### JOB_HV_95 registerTempTable--spark_hive_rtdtrs_dtl_achis_order 完成的系统时间为:" + DateUtils.getCurrentSystemTime())

        if (!Option(df).isEmpty) {
          sqlContext.sql(s"use $hive_dbname")
          sqlContext.sql(
            s"""
               |insert overwrite table hive_rtdtrs_dtl_achis_order partition (part_settle_dt)
               |select
               |settle_dt               ,
               |trans_no                ,
               |trans_tp                ,
               |trans_class             ,
               |trans_source            ,
               |buss_chnl               ,
               |pri_acct_no             ,
               |fwd_ins_id_cd           ,
               |acq_ins_id_cd           ,
               |iss_ins_id_cd           ,
               |iss_head                ,
               |iss_head_nm             ,
               |mchnt_cd                ,
               |mchnt_nm                ,
               |mchnt_country           ,
               |mchnt_url               ,
               |mchnt_front_url         ,
               |mchnt_back_url          ,
               |mchnt_delv_tag          ,
               |mchnt_tp                ,
               |mchnt_risk_tag          ,
               |mchnt_order_id          ,
               |sys_tra_no              ,
               |sys_tm                  ,
               |trans_tm                ,
               |trans_dt                ,
               |trans_at                ,
               |trans_curr_cd           ,
               |trans_st                ,
               |auth_id                 ,
               |settle_at               ,
               |settle_curr_cd          ,
               |settle_conv_rt          ,
               |conv_dt                 ,
               |cert_tp                 ,
               |cert_id                 ,
               |name                    ,
               |phone_no                ,
               |org_trans_idx           ,
               |org_sys_tra_no          ,
               |org_sys_tm              ,
               |proc_st                 ,
               |resp_cd                 ,
               |proc_sys                ,
               |usr_id                  ,
               |mchnt_id                ,
               |pay_method              ,
               |trans_ip                ,
               |refund_at               ,
               |card_attr               ,
               |kz_curr_cd              ,
               |kz_conv_rt              ,
               |kz_at                   ,
               |sub_mchnt_cd            ,
               |sub_mchnt_nm            ,
               |points_at               ,
               |verify_mode             ,
               |is_oversea              ,
               |pri_data                ,
               |access_tp               ,
               |rcv_ins_id_cd           ,
               |zero_cost               ,
               |advance_payment         ,
               |pos_cond_cd             ,
               |reserve_fld             ,
               |reserve1                ,
               |reserve2                ,
               |reserve3                ,
               |reserve4                ,
               |reserve5                ,
               |reserve6                ,
               |comments                ,
               |rec_st                  ,
               |rec_crt_ts              ,
               |rec_upd_ts              ,
               |trans_chnl              ,
               |source_idt              ,
               |out_trans_tp            ,
               |md_id                   ,
               |ud_id                   ,
               |syssp_id                ,
               |src_sys_flag            ,
               |auth_at                 ,
               |iss_ins_tp              ,
               |$today_dt
               |from
               |spark_hive_rtdtrs_dtl_achis_order
           """.stripMargin)
          println("#### JOB_HV_95 分区数据插入完成的时间为：" + DateUtils.getCurrentSystemTime())
        } else {
          println(s"#### JOB_HV_95 read $up_namenode/ 无数据！")
        }
        today_dt = DateUtils.addOneDay(today_dt)
      }
    }
  }

  /**
    * hive-job-96 2017-03-30
    * rtdtrs_dtl_achis_order_error -> hive_rtdtrs_dtl_achis_order_error
    *
    * @author Xue
    * @param sqlContext
    */
  def JOB_HV_96(implicit sqlContext: HiveContext,start_dt:String,end_dt: String, interval: Int) {
    println("#### JOB_HV_96(rtdtrs_dtl_achis_order_error -> hive_rtdtrs_dtl_achis_order_error)")

    var today_dt = start_dt
    if (interval >= 0) {
      println("#### JOB_HV_96 增量抽取的时间范围为: "+start_dt+ "-"+ end_dt)
      DateUtils.timeCost("JOB_HV_96") {
        val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/incident/trans/hive_rtdtrs_dtl_achis_order_error/part_settle_dt=$today_dt")
        println(s"#### JOB_HV_96 read $up_namenode/ 数据完成时间为:" + DateUtils.getCurrentSystemTime())

        df.registerTempTable("spark_hive_rtdtrs_dtl_achis_order_error")
        println("#### JOB_HV_96 registerTempTable--spark_hive_rtdtrs_dtl_achis_order_error 完成的系统时间为:" + DateUtils.getCurrentSystemTime())

        if (!Option(df).isEmpty) {
          sqlContext.sql(s"use $hive_dbname")
          sqlContext.sql(
            s"""
               |insert overwrite table hive_rtdtrs_dtl_achis_order_error partition (part_settle_dt)
               |select
               |settle_dt            ,
               |trans_idx            ,
               |pri_acct_no          ,
               |trans_at             ,
               |trans_tm             ,
               |trans_tp             ,
               |out_trans_tp         ,
               |acq_ins_id_cd        ,
               |trans_curr_cd        ,
               |mchnt_cd             ,
               |mchnt_order_id       ,
               |rec_crt_ts           ,
               |trans_source         ,
               |buss_chnl            ,
               |iss_ins_id_cd        ,
               |error_tp             ,
               |error_msg            ,
               |error_cd             ,
               |advice_msg           ,
               |resp_cd              ,
               |sub_trans_tp         ,
               |$today_dt
               |from
               |spark_hive_rtdtrs_dtl_achis_order_error
           """.stripMargin)
          println("#### JOB_HV_96 分区数据插入完成的时间为：" + DateUtils.getCurrentSystemTime())
        } else {
          println(s"#### JOB_HV_96 read $up_namenode/ 无数据！")
        }
        today_dt = DateUtils.addOneDay(today_dt)
      }
    }
  }

  /**
    * hive-job-97 2017-03-30
    * rtdtrs_dtl_sor_cmsp -> hive_rtdtrs_dtl_sor_cmsp
    *
    * @author Xue
    * @param sqlContext
    */
  def JOB_HV_97(implicit sqlContext: HiveContext,start_dt:String,end_dt: String, interval: Int) {
    println("#### JOB_HV_97(rtdtrs_dtl_sor_cmsp -> hive_rtdtrs_dtl_sor_cmsp)")

    var today_dt = start_dt
    if (interval >= 0) {
      println("#### JOB_HV_97 增量抽取的时间范围为: "+start_dt+ "-"+ end_dt)
      DateUtils.timeCost("JOB_HV_97") {
        val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/incident/trans/hive_rtdtrs_dtl_sor_cmsp/part_settle_dt=$today_dt")
        println(s"#### JOB_HV_97 read $up_namenode/ 数据完成时间为:" + DateUtils.getCurrentSystemTime())

        df.registerTempTable("spark_hive_rtdtrs_dtl_sor_cmsp")
        println("#### JOB_HV_97 registerTempTable--spark_hive_rtdtrs_dtl_sor_cmsp 完成的系统时间为:" + DateUtils.getCurrentSystemTime())

        if (!Option(df).isEmpty) {
          sqlContext.sql(s"use $hive_dbname")
          sqlContext.sql(
            s"""
               |insert overwrite table hive_rtdtrs_dtl_sor_cmsp partition (part_settle_dt)
               |select
               |trans_seq                         ,
               |trans_submit_tm                   ,
               |new_trans_submit_tm               ,
               |trans_tp_cd                       ,
               |trans_rcv_tm                      ,
               |trans_fin_tm                      ,
               |to_tm                             ,
               |to_moni_in                        ,
               |moni_dist                         ,
               |ab_in                             ,
               |trans_st_cd                       ,
               |rvsl_st_cd                        ,
               |source_line_svc                   ,
               |source_msg_cd                     ,
               |term_csn                          ,
               |trans_tp                          ,
               |mchnt_cd                          ,
               |mchnt_nm                          ,
               |order_seq                         ,
               |trans_at                          ,
               |trans_curr_cd                     ,
               |acctnum1                          ,
               |acctnum2                          ,
               |rspcode                           ,
               |spcode                            ,
               |terminal_id                       ,
               |settle_dt                         ,
               |syssp_id                          ,
               |pos_cd                            ,
               |card_no                           ,
               |app_cd                            ,
               |trans_type_id                     ,
               |cmsp_card_type_id                 ,
               |pboc_card_type_id                 ,
               |md_ins_id_cd                      ,
               |issuer_ins_id_cd                  ,
               |trans_cd                          ,
               |resp_cd1                          ,
               |resp_cd4                          ,
               |card_attr_id                      ,
               |card_brand_id                     ,
               |acpt_ins_id_cd                    ,
               |fwd_ins_id_cd                     ,
               |rcv_ins_id_cd                     ,
               |iss_ins_id_cd                     ,
               |settle_flag                       ,
               |ic_flds_cvert                     ,
               |src_sys                           ,
               |trans_seq_conv                    ,
               |new_trans_submit_tm_conv          ,
               |acpt_ins_id_cd_conv               ,
               |fwd_ins_id_cd_conv                ,
               |proc_sys_flag                     ,
               |$today_dt
               |from
               |spark_hive_rtdtrs_dtl_sor_cmsp
           """.stripMargin)
          println("#### JOB_HV_97 分区数据插入完成的时间为：" + DateUtils.getCurrentSystemTime())
        } else {
          println(s"#### JOB_HV_97 read $up_namenode/ 无数据！")
        }
        today_dt = DateUtils.addOneDay(today_dt)
      }
    }
  }

  /**
    * JOB_HV_98 2017-4-26
    * hbkdb.rtapam_dim_ins -> upw_hive.hive_rtapam_dim_ins
    *
    * @author TZQ
    * @param sqlContext
    */
  def JOB_HV_98(implicit sqlContext: HiveContext) {
    println("#### JOB_HV_98(hbkdb.rtapam_dim_ins -> upw_hive.hive_rtapam_dim_ins)")

      println("#### JOB_HV_98 全量抽取数据，开始时间："+ DateUtils.getCurrentSystemTime())
      DateUtils.timeCost("JOB_HV_98") {
        val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/participant/ins/hive_rtapam_dim_ins")
        println(s"#### JOB_HV_98 read $up_namenode 数据完成时间为:" + DateUtils.getCurrentSystemTime())

        df.registerTempTable("spark_hive_rtapam_dim_ins")
        println("#### JOB_HV_98 registerTempTable--spark_hive_rtapam_dim_ins 完成的系统时间为:" + DateUtils.getCurrentSystemTime())

        if (!Option(df).isEmpty) {
          sqlContext.sql(s"use $hive_dbname")
          sqlContext.sql("truncate table hive_rtdtrs_dtl_sor_cmsp")
          sqlContext.sql(
            s"""
               |insert overwrite table hive_rtapam_dim_ins
               |select
               |ins_id                ,
               |ins_cn_nm             ,
               |ins_id_cd             ,
               |root_ins_nm           ,
               |root_ins_cd           ,
               |settle_root_ins_nm    ,
               |settle_root_ins_cd    ,
               |intnl_org_nm          ,
               |intnl_org_cd          ,
               |settle_intnl_org_nm   ,
               |settle_intnl_org_nm   ,
               |ins_cata_id1          ,
               |ins_cata_nm1          ,
               |ins_cata_id2          ,
               |ins_cata_nm2          ,
               |ins_cata_id3          ,
               |ins_cata_nm3          ,
               |ins_cata_id4          ,
               |ins_cata_nm4          ,
               |settle_ins_cata_id1   ,
               |settle_ins_cata_nm1   ,
               |settle_ins_cata_id2   ,
               |settle_ins_cata_nm2   ,
               |settle_ins_cata_id3   ,
               |settle_ins_cata_nm3   ,
               |settle_ins_cata_id4   ,
               |settle_ins_cata_nm4   ,
               |domin_id_1            ,
               |domin_nm_1            ,
               |domin_id_2            ,
               |domin_nm_2            ,
               |domin_tp_2            ,
               |domin_id_3            ,
               |domin_nm_3            ,
               |src_sys               ,
               |edw_rec_start_ts      ,
               |edw_rec_end_ts        ,
               |case
               |when trim(INTNL_ORG_NM)='厦门分公司' then '厦门'
               |when trim(INTNL_ORG_NM)='吉林分公司' then '吉林'
               |when trim(INTNL_ORG_NM)='天津分公司' then '天津'
               |when trim(INTNL_ORG_NM)='宁波分公司' then '宁波'
               |when trim(INTNL_ORG_NM)='山西分公司' then '山西'
               |when trim(INTNL_ORG_NM)='广东分公司' then '广东'
               |when trim(INTNL_ORG_NM)='河北分公司' then '河北'
               |when trim(INTNL_ORG_NM)='湖北分公司' then '湖北'
               |when trim(INTNL_ORG_NM)='辽宁分公司' then '辽宁'
               |when trim(INTNL_ORG_NM)='陕西分公司' then '陕西'
               |when trim(INTNL_ORG_NM)='云南分公司' then '云南'
               |when trim(INTNL_ORG_NM)='宁夏分公司' then '宁夏'
               |when trim(INTNL_ORG_NM)='安徽分公司' then '安徽'
               |when trim(INTNL_ORG_NM)='江苏分公司' then '江苏'
               |when trim(INTNL_ORG_NM)='河南分公司' then '河南'
               |when trim(INTNL_ORG_NM)='深圳分公司' then '深圳'
               |when trim(INTNL_ORG_NM)='湖南分公司' then '湖南'
               |when trim(INTNL_ORG_NM)='福建分公司' then '福建'
               |when trim(INTNL_ORG_NM)='青岛分公司' then '青岛'
               |when trim(INTNL_ORG_NM)='内蒙分公司' then '内蒙'
               |when trim(INTNL_ORG_NM)='大连分公司' then '大连'
               |when trim(INTNL_ORG_NM)='山东分公司' then '山东'
               |when trim(INTNL_ORG_NM)='广西分公司' then '广西'
               |when trim(INTNL_ORG_NM)='新疆分公司' then '新疆'
               |when trim(INTNL_ORG_NM)='海南分公司' then '海南'
               |when trim(INTNL_ORG_NM)='贵州分公司' then '贵州'
               |when trim(INTNL_ORG_NM)='上海分公司' then '上海'
               |when trim(INTNL_ORG_NM)='北京分公司' then '北京'
               |when trim(INTNL_ORG_NM)='四川分公司' then '四川'
               |when trim(INTNL_ORG_NM)='江西分公司' then '江西'
               |when trim(INTNL_ORG_NM)='浙江分公司' then '浙江'
               |when trim(INTNL_ORG_NM)='甘肃分公司' then '甘肃'
               |when trim(INTNL_ORG_NM)='西藏分公司' then '西藏'
               |when trim(INTNL_ORG_NM)='重庆分公司' then '重庆'
               |when trim(INTNL_ORG_NM)='青海分公司' then '青海'
               |when trim(INTNL_ORG_NM)='黑龙江分公司' then '黑龙江'
               |else '总公司' end
               |from
               |spark_hive_rtapam_dim_ins
           """.stripMargin)
          println("#### JOB_HV_98 全量数据插入完成的时间为：" + DateUtils.getCurrentSystemTime())
        } else {
          println(s"#### JOB_HV_98 read $up_namenode  无数据！")
        }
      }
  }


  /**
    * JOB_HV_99 2017-04-26
    *  hbkdb.stmtrs_scl_usr_geo_loc_quota -> upw_hive.hive_stmtrs_scl_usr_geo_loc_quota
    * @author tzq
    * @param sqlContext
    * @param start_month
    * @param end_month
    * @param months
    */
  def JOB_HV_99(implicit sqlContext: HiveContext, start_month: String, end_month: String, months: Int) {
    println("#### JOB_HV_99(hbkdb.stmtrs_scl_usr_geo_loc_quota -> upw_hive.hive_stmtrs_scl_usr_geo_loc_quota)")

    var cur_month = start_month
    if (months >= 0) {
      for(i <- 0 to months){
        val part_month = cur_month.substring(0,7).replace("-","")
        println("#### JOB_HV_99 增量抽取的时间范围为: "+start_month+ "-"+ end_month)
        DateUtils.timeCost("JOB_HV_99") {
          val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/product/card/hive_stmtrs_scl_usr_geo_loc_quota/part_settle_month=$part_month")
          println(s"#### JOB_HV_99 read $up_namenode/ 数据完成时间为:" + DateUtils.getCurrentSystemTime())

          df.registerTempTable("spark_hive_stmtrs_scl_usr_geo_loc_quota")
          println("#### JOB_HV_99 registerTempTable--spark_hive_stmtrs_scl_usr_geo_loc_quota 完成的系统时间为:" + DateUtils.getCurrentSystemTime())

          if (!Option(df).isEmpty) {
            sqlContext.sql(s"use $hive_dbname")
            sqlContext.sql(
              s"""
                 |insert overwrite table hive_stmtrs_scl_usr_geo_loc_quota partition (part_settle_month)
                 |select
                 |settle_month       ,
                 |cdhd_usr_id        ,
                 |intnl_org_cd_list  ,
                 |bind_card_no_list  ,
                 |hp_settle_month    ,
                 |'$part_month' as p_settle_month
                 |from
                 |spark_hive_stmtrs_scl_usr_geo_loc_quota
           """.stripMargin)
            println("#### JOB_HV_99 分区数据插入完成的时间为：" + DateUtils.getCurrentSystemTime())
          } else {
            println(s"#### JOB_HV_99 read $up_namenode/ 无数据！")
          }
          cur_month = DateUtils.addOneMonth(cur_month)
        }
      }
      }
  }

  /**
    * JOB_HV_100 2017-4-26
    * hbkdb.rtapam_dim_intnl_domin-> upw_hive.hive_rtapam_dim_intnl_domin
    *
    * @author TZQ
    * @param sqlContext
    */
  def JOB_HV_100(implicit sqlContext: HiveContext) {
    println("#### JOB_HV_100(hbkdb.rtapam_dim_intnl_domin-> upw_hive.hive_rtapam_dim_intnl_domin )")

    println("#### JOB_HV_100 全量抽取数据，开始时间："+ DateUtils.getCurrentSystemTime())
    DateUtils.timeCost("JOB_HV_100") {
      val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/region/hive_rtapam_dim_intnl_domin")
      println(s"#### JOB_HV_100 read $up_namenode 数据完成时间为:" + DateUtils.getCurrentSystemTime())

      df.registerTempTable("spark_hive_rtapam_dim_intnl_domin")
      println("#### JOB_HV_100 registerTempTable--spark_hive_rtapam_dim_intnl_domin 完成的系统时间为:" + DateUtils.getCurrentSystemTime())

      if (!Option(df).isEmpty) {
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql("truncate table hive_rtapam_dim_intnl_domin")
        sqlContext.sql(
          s"""
             |insert overwrite table hive_rtapam_dim_intnl_domin
             |select
             |domin_id            ,
             |par_domin_id        ,
             |domin_nm            ,
             |domin_tp            ,
             |intnl_org_id_cd     ,
             |intnl_org_nm        ,
             |prov_id             ,
             |edw_rec_crt_usr     ,
             |edw_rec_crt_ts      ,
             |edw_rec_upd_usr     ,
             |edw_rec_upd_ts      ,
             |src_sys             ,
             |src_busi_key        ,
             |case
             |when trim(INTNL_ORG_NM)='厦门分公司' then '厦门'
             |when trim(INTNL_ORG_NM)='吉林分公司' then '吉林'
             |when trim(INTNL_ORG_NM)='天津分公司' then '天津'
             |when trim(INTNL_ORG_NM)='宁波分公司' then '宁波'
             |when trim(INTNL_ORG_NM)='山西分公司' then '山西'
             |when trim(INTNL_ORG_NM)='广东分公司' then '广东'
             |when trim(INTNL_ORG_NM)='河北分公司' then '河北'
             |when trim(INTNL_ORG_NM)='湖北分公司' then '湖北'
             |when trim(INTNL_ORG_NM)='辽宁分公司' then '辽宁'
             |when trim(INTNL_ORG_NM)='陕西分公司' then '陕西'
             |when trim(INTNL_ORG_NM)='云南分公司' then '云南'
             |when trim(INTNL_ORG_NM)='宁夏分公司' then '宁夏'
             |when trim(INTNL_ORG_NM)='安徽分公司' then '安徽'
             |when trim(INTNL_ORG_NM)='江苏分公司' then '江苏'
             |when trim(INTNL_ORG_NM)='河南分公司' then '河南'
             |when trim(INTNL_ORG_NM)='深圳分公司' then '深圳'
             |when trim(INTNL_ORG_NM)='湖南分公司' then '湖南'
             |when trim(INTNL_ORG_NM)='福建分公司' then '福建'
             |when trim(INTNL_ORG_NM)='青岛分公司' then '青岛'
             |when trim(INTNL_ORG_NM)='内蒙分公司' then '内蒙'
             |when trim(INTNL_ORG_NM)='大连分公司' then '大连'
             |when trim(INTNL_ORG_NM)='山东分公司' then '山东'
             |when trim(INTNL_ORG_NM)='广西分公司' then '广西'
             |when trim(INTNL_ORG_NM)='新疆分公司' then '新疆'
             |when trim(INTNL_ORG_NM)='海南分公司' then '海南'
             |when trim(INTNL_ORG_NM)='贵州分公司' then '贵州'
             |when trim(INTNL_ORG_NM)='上海分公司' then '上海'
             |when trim(INTNL_ORG_NM)='北京分公司' then '北京'
             |when trim(INTNL_ORG_NM)='四川分公司' then '四川'
             |when trim(INTNL_ORG_NM)='江西分公司' then '江西'
             |when trim(INTNL_ORG_NM)='浙江分公司' then '浙江'
             |when trim(INTNL_ORG_NM)='甘肃分公司' then '甘肃'
             |when trim(INTNL_ORG_NM)='西藏分公司' then '西藏'
             |when trim(INTNL_ORG_NM)='重庆分公司' then '重庆'
             |when trim(INTNL_ORG_NM)='青海分公司' then '青海'
             |when trim(INTNL_ORG_NM)='黑龙江分公司' then '黑龙江'
             |else '总公司' end
             |from
             |spark_hive_rtapam_dim_intnl_domin
           """.stripMargin)
        println("#### JOB_HV_100 全量数据插入完成的时间为：" + DateUtils.getCurrentSystemTime())
      } else {
        println(s"#### JOB_HV_100 read $up_namenode/ 无数据！")
      }
    }
  }

}
