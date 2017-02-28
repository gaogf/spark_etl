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
import org.joda.time.{DateTime, Days, LocalDate}

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



    /**
      * 从数据库中获取当前JOB的执行起始和结束日期。
      * 日常调度使用。
      */
    val rowParams=UPSQL_TIMEPARAMS_JDBC.readTimeParams(sqlContext)
    start_dt=DateUtils.getYesterdayByJob(rowParams.getString(0))  //获取开始日期：start_dt-1
    end_dt=rowParams.getString(1)//结束日期


    /**
      * 从命令行获取当前JOB的执行起始和结束日期。
      * 无规则日期的增量数据抽取，主要用于数据初始化和调试。
      */
//    if (args.length > 1) {
//      start_dt = args(1)
//      end_dt = args(2)
//    } else {
//      println("#### 缺少参数输入")
//      println("#### 请指定 SparkDB22Hive 数据抽取的起始日期")
//    }


    val interval=DateUtils.getIntervalDays(start_dt,end_dt).toInt

    println(s"#### SparkUPH2H 数据抽取的起始日期为: $start_dt --  $end_dt")

    val JobName = if(args.length>0) args(0) else None
    println(s"#### The Current Job Name is ： [$JobName]")
    JobName match {
      /**
        * 每日模板job
        */
      case "JOB_HV_39"  => JOB_HV_39(sqlContext,end_dt) //CODE BY YX
      case "JOB_HV_49"  => JOB_HV_49 //CODE BY YX
      case "JOB_HV_52"  => JOB_HV_52(sqlContext,end_dt) //CODE BY YX



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

  def JOB_HV_39(implicit sqlContext: HiveContext,end_dt:String) = {
    println("#### JOB_HV_39(rtdtrs_dtl_achis -> hive_achis_trans)")

    val today_dt = end_dt
    println("#### JOB_HV_39 增量抽取的时间范围为: "+end_dt)

    DateUtils.timeCost("JOB_HV_39") {
      val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/incident/ods/hive_achis_trans/part_settle_dt=$today_dt")
      println(s"#### JOB_HV_39 read $up_namenode/ 数据完成时间为:"+DateUtils.getCurrentSystemTime())

      df.registerTempTable("spark_hive_achis_trans")
      println("#### JOB_HV_39 registerTempTable--spark_hive_achis_trans 完成的系统时间为:"+DateUtils.getCurrentSystemTime())

      if(!Option(df).isEmpty){
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
        println("#### JOB_HV_39 分区数据插入完成的时间为："+DateUtils.getCurrentSystemTime())
      } else {
        println(s"#### JOB_HV_39 read $up_namenode/ 无数据！")
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
    if (interval > 0) {
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
    * @author Xue
    * @param sqlContext
    */
  def JOB_HV_50(implicit sqlContext: HiveContext,start_dt:String,end_dt:String,interval:Int) = {
    println("#### JOB_HV_50(org_tdapp_keyvalue to hive_org_tdapp_keyvalue)")

    DateUtils.timeCost("JOB_HV_50") {
      var today_dt = start_dt
      if (interval > 0) {
        println(s"#### JOB_HV_50  spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())

        for (i <- 0 to interval) {
          val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/incident/td/hive_org_tdapp_keyvalue/part_updays=$today_dt")
          println(s"#### JOB_HV_50 read $up_namenode/ 读取大数据平台数据完成时间为:" + DateUtils.getCurrentSystemTime())

          df.registerTempTable("spark_hive_org_tdapp_keyvalue")
          println("#### JOB_HV_50 registerTempTable--spark_hive_org_tdapp_keyvalue 完成的系统时间为:" + DateUtils.getCurrentSystemTime())

          if (!Option(df).isEmpty) {
            sqlContext.sql(s"use $hive_dbname")
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
      if (interval > 0) {
        println(s"#### JOB_HV_51  spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())

        for (i <- 0 to interval) {
          val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/incident/td/hive_org_tdapp_tappevent/part_updays=$today_dt")
          println(s"#### JOB_HV_51 read $up_namenode/ 读取大数据平台数据完成时间为:" + DateUtils.getCurrentSystemTime())

          df.registerTempTable("spark_hive_org_tdapp_tappevent")
          println("#### JOB_HV_51 registerTempTable--spark_hive_org_tdapp_tappevent 完成的系统时间为:" + DateUtils.getCurrentSystemTime())

          if (!Option(df).isEmpty) {
            sqlContext.sql(s"use $hive_dbname")
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
  def JOB_HV_52(implicit sqlContext: HiveContext, end_dt: String) = {
    println("#### JOB_HV_52(stmtrs_bsl_active_card_acq_branch_mon1 -> hive_active_card_acq_branch_mon)")

    val part_dt = end_dt.substring(0, 7)
    println("#### JOB_HV_52 增量抽取的时间范围为: " +part_dt)

    DateUtils.timeCost("JOB_HV_52") {
      val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/product/card/hive_active_card_acq_branch_mon/part_settle_month=$part_dt")
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
             |hp_settle_month
             |from
             |spark_active_card_acq_branch_mon
           """.stripMargin)
        println("#### JOB_HV_52 分区数据插入完成的时间为："+DateUtils.getCurrentSystemTime())
      }else{
        println(s"#### JOB_HV_52 read $up_namenode/ 无数据！")
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
      if (interval > 0) {
        println(s"#### JOB_HV_58  spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())

        for (i <- 0 to interval) {
          val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/incident/td/hive_org_tdapp_activitynew/part_updays=$today_dt")
          println(s"#### JOB_HV_58 read $up_namenode/ 读取大数据平台数据完成时间为:" + DateUtils.getCurrentSystemTime())

          df.registerTempTable("spark_hive_org_tdapp_activitynew")
          println("#### JOB_HV_58 registerTempTable--spark_hive_org_tdapp_activitynew 完成的系统时间为:" + DateUtils.getCurrentSystemTime())

          if (!Option(df).isEmpty) {
            sqlContext.sql(s"use $hive_dbname")
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
      if (interval > 0) {
        println(s"#### JOB_HV_59  spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())

        for (i <- 0 to interval) {
          val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/incident/td/hive_org_tdapp_devicenew/part_updays=$today_dt")
          println(s"#### JOB_HV_59 read $up_namenode/ 读取大数据平台数据完成时间为:" + DateUtils.getCurrentSystemTime())

          df.registerTempTable("spark_hive_org_tdapp_devicenew")
          println("#### JOB_HV_59 registerTempTable--spark_hive_org_tdapp_devicenew 完成的系统时间为:" + DateUtils.getCurrentSystemTime())

          if (!Option(df).isEmpty) {
            sqlContext.sql(s"use $hive_dbname")
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
      if (interval > 0) {
        println(s"#### JOB_HV_60  spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())

        for (i <- 0 to interval) {
          val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/incident/td/hive_org_tdapp_eventnew/part_updays=$today_dt")
          println(s"#### JOB_HV_60 read $up_namenode/ 读取大数据平台数据完成时间为:" + DateUtils.getCurrentSystemTime())

          df.registerTempTable("spark_hive_org_tdapp_eventnew")
          println("#### JOB_HV_60 registerTempTable--spark_hive_org_tdapp_eventnew 完成的系统时间为:" + DateUtils.getCurrentSystemTime())

          if (!Option(df).isEmpty) {
            sqlContext.sql(s"use $hive_dbname")
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
            println(s"#### JOB_HV_60 read $up_namenode/ 无数据！")
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
      if (interval > 0) {
        println(s"#### JOB_HV_61  spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())

        for (i <- 0 to interval) {
          val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/incident/td/hive_org_tdapp_exceptionnew/part_updays=$today_dt")
          println(s"#### JOB_HV_61 read $up_namenode/ 读取大数据平台数据完成时间为:" + DateUtils.getCurrentSystemTime())

          df.registerTempTable("spark_hive_org_tdapp_exceptionnew")
          println("#### JOB_HV_61 registerTempTable--spark_hive_org_tdapp_exceptionnew 完成的系统时间为:" + DateUtils.getCurrentSystemTime())

          if (!Option(df).isEmpty) {
            sqlContext.sql(s"use $hive_dbname")
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
      if (interval > 0) {
        println(s"#### JOB_HV_62  spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())

        for (i <- 0 to interval) {
          val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/incident/td/hive_org_tdapp_tlaunchnew/part_updays=$today_dt")
          println(s"#### JOB_HV_62 read $up_namenode/ 读取大数据平台数据完成时间为:" + DateUtils.getCurrentSystemTime())

          df.registerTempTable("spark_hive_org_tdapp_tlaunchnew")
          println("#### JOB_HV_62 registerTempTable--spark_hive_org_tdapp_tlaunchnew 完成的系统时间为:" + DateUtils.getCurrentSystemTime())

          if (!Option(df).isEmpty) {
            sqlContext.sql(s"use $hive_dbname")
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
      if (interval > 0) {
        println(s"#### JOB_HV_63  spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())

        for (i <- 0 to interval) {
          val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/incident/td/hive_org_tdapp_device/part_updays=$today_dt")
          println(s"#### JOB_HV_63 read $up_namenode/ 读取大数据平台数据完成时间为:" + DateUtils.getCurrentSystemTime())

          df.registerTempTable("spark_hive_org_tdapp_device")
          println("#### JOB_HV_63 registerTempTable--spark_hive_org_tdapp_device 完成的系统时间为:" + DateUtils.getCurrentSystemTime())

          if (!Option(df).isEmpty) {
            sqlContext.sql(s"use $hive_dbname")
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
      if (interval > 0) {
        println(s"#### JOB_HV_64  spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())

        for (i <- 0 to interval) {
          val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/incident/td/hive_org_tdapp_exception/part_updays=$today_dt")
          println(s"#### JOB_HV_64 read $up_namenode/ 读取大数据平台数据完成时间为:" + DateUtils.getCurrentSystemTime())

          df.registerTempTable("spark_hive_org_tdapp_exception")
          println("#### JOB_HV_64 registerTempTable--spark_hive_org_tdapp_exception 完成的系统时间为:" + DateUtils.getCurrentSystemTime())

          if (!Option(df).isEmpty) {
            sqlContext.sql(s"use $hive_dbname")
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
      if (interval > 0) {
        println(s"#### JOB_HV_65  spark sql 清洗[$today_dt]数据开始时间为:" + DateUtils.getCurrentSystemTime())

        for (i <- 0 to interval) {
          val df = sqlContext.read.parquet(s"$up_namenode/$up_hivedataroot/incident/td/hive_org_tdapp_newuser/part_updays=$today_dt")
          println(s"#### JOB_HV_65 read $up_namenode 读取大数据平台数据完成时间为:" + DateUtils.getCurrentSystemTime())

          df.registerTempTable("spark_hive_org_tdapp_newuser")
          println("#### JOB_HV_65 registerTempTable--spark_hive_org_tdapp_newuser 完成的系统时间为:" + DateUtils.getCurrentSystemTime())

          if (!Option(df).isEmpty) {
            sqlContext.sql(s"use $hive_dbname")
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
    * @author tzq
    * @param sqlContext
    * @param end_dt
    * @param start_dt
    * @param interval
    */
  def JOB_HV_71(implicit sqlContext: HiveContext,start_dt: String,end_dt:String,interval:Int) = {
    println("#### JOB_HV_71( hive_ach_order_inf <- hbkdb.rtdtrs_dtl_ach_order_inf) #####")

    var part_dt = start_dt

    sqlContext.sql(s"use $hive_dbname")

    if(interval >= 0){
      for(i <- 0 to interval){
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

        part_dt=DateUtils.addOneDay(part_dt)
      }
    }
  }

}
