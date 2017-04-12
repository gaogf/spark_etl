package com.unionpay.etl

import java.text.SimpleDateFormat
import com.unionpay.conf.ConfigurationManager
import com.unionpay.constant.Constants
import com.unionpay.jdbc.DB2_JDBC.ReadDB2
import com.unionpay.jdbc.DB2_JDBC.ReadDB2_WithUR
import com.unionpay.jdbc.UPSQL_TIMEPARAMS_JDBC
import com.unionpay.utils.DateUtils
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.format.DateTimeFormat
import org.joda.time.{Days, LocalDate}

/**
  * 作业：抽取DB2中的数据到钱包Hive数据仓库
  */
object SparkDB22Hive {
  private lazy val dateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd")
  //指定HIVE数据库名
  private lazy val hive_dbname = ConfigurationManager.getProperty(Constants.HIVE_DBNAME)
  private lazy val schemas_accdb = ConfigurationManager.getProperty(Constants.SCHEMAS_ACCDB)
  private lazy val schemas_mgmdb = ConfigurationManager.getProperty(Constants.SCHEMAS_MGMDB)
  private lazy  val schemas_upoupdb=ConfigurationManager.getProperty(Constants.SCHEMAS_UPOUPDB)
  private lazy val schemas_mnsvcdb=ConfigurationManager.getProperty(Constants.SCHEMAS_MNSVCDB)
  private lazy val schemas_wlonldb=ConfigurationManager.getProperty(Constants.SCHEMAS_WLONLDB)

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("SparkDB22Hive")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.Kryoserializer.buffer.max", "1024m")
      .set("spark.yarn.driver.memoryOverhead", "1024")
      .set("spark.yarn.executor.memoryOverhead", "2000")
      .set("spark.newwork.buffer.timeout", "300s")
      .set("spark.executor.heartbeatInterval", "30s")
      .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
      .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new HiveContext(sc)

    var start_dt: String = s"0000-00-00"
    var end_dt: String = s"0000-00-00"

    /**
      * 从数据库中获取当前JOB的执行起始和结束日期。
      * 日常调度使用。
      */
//    val rowParams = UPSQL_TIMEPARAMS_JDBC.readTimeParams(sqlContext)
//    println("读取mysql取时间段："+rowParams)
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
      println("#### 请指定 SparkDB22Hive 数据抽取的起始日期和结束日期 ！")
      System.exit(0)
    }

    val interval = DateUtils.getIntervalDays(start_dt, end_dt).toInt //not use

    println(s"#### SparkDB22Hive 数据抽取的起始日期为: $start_dt --  $end_dt")

    val JobName = if (args.length > 0) args(0) else None
    println(s"#### The Current Job Name is ： [$JobName]")
    JobName match {
      /**
        * 每日模板job
        */
      case "JOB_HV_1" => JOB_HV_1 //CODE BY YX
      case "JOB_HV_3" => JOB_HV_3 //CODE BY YX
      case "JOB_HV_8" => JOB_HV_8(sqlContext, start_dt, end_dt) //CODE BY XTP
      case "JOB_HV_9" => JOB_HV_9 //CODE BY TZQ
      case "JOB_HV_10" => JOB_HV_10 //CODE BY TZQ
      case "JOB_HV_11" => JOB_HV_11 //CODE BY TZQ
      case "JOB_HV_12" => JOB_HV_12 //CODE BY TZQ
      case "JOB_HV_13" => JOB_HV_13 //CODE BY TZQ
      case "JOB_HV_14" => JOB_HV_14 //CODE BY TZQ
      case "JOB_HV_16" => JOB_HV_16 //CODE BY TZQ
      case "JOB_HV_18" => JOB_HV_18(sqlContext, start_dt, end_dt) //CODE BY YX
      case "JOB_HV_19" => JOB_HV_19 //CODE BY YX
      case "JOB_HV_28" => JOB_HV_28(sqlContext, start_dt, end_dt) //CODE BY XTP
      case "JOB_HV_29" => JOB_HV_29(sqlContext, start_dt, end_dt) //CODE BY XTP
      case "JOB_HV_30" => JOB_HV_30(sqlContext, start_dt, end_dt) //CODE BY YX
      case "JOB_HV_32" => JOB_HV_32(sqlContext, start_dt, end_dt) //CODE BY XTP
      case "JOB_HV_33" => JOB_HV_33(sqlContext, start_dt, end_dt) //CODE BY XTP
      case "JOB_HV_36" => JOB_HV_36 //CODE BY YX
      case "JOB_HV_44" => JOB_HV_44 //CODE BY TZQ
      case "JOB_HV_46" => JOB_HV_46 //CODE BY XTP
      case "JOB_HV_47" => JOB_HV_47 //CODE BY XTP
      case "JOB_HV_48" => JOB_HV_48 //CODE BY TZQ
      case "JOB_HV_54" => JOB_HV_54 //CODE BY TZQ
      case "JOB_HV_67" => JOB_HV_67 //CODE BY TZQ
      case "JOB_HV_68" => JOB_HV_68 //CODE BY TZQ
      case "JOB_HV_69" => JOB_HV_69 //CODE BY XTP
      case "JOB_HV_70" => JOB_HV_70 //CODE BY YX
      case "JOB_HV_79" => JOB_HV_79 //CODE BY XTP
      case "JOB_HV_80" => JOB_HV_80(sqlContext, start_dt, end_dt) //CODE BY XTP
      case "JOB_HV_81" => JOB_HV_81(sqlContext, start_dt, end_dt) //CODE BY XTP
      case "JOB_HV_82" => JOB_HV_82(sqlContext, start_dt, end_dt) //CODE BY XTP

      /**
        * 指标套表job
        */
      case "JOB_HV_15" => JOB_HV_15(sqlContext, start_dt, end_dt)//CODE BY TZQ
      case "JOB_HV_20_INI_1" => JOB_HV_20_INI_1 //CODE BY XTP
      case "JOB_HV_20_INI_2" => JOB_HV_20_INI_2 //CODE BY XTP
      case "JOB_HV_20" => JOB_HV_20(sqlContext, start_dt, end_dt) //CODE BY XTP
      case "JOB_HV_23" => JOB_HV_23 //CODE BY TZQ
      case "JOB_HV_24" => JOB_HV_24 //CODE BY YX
      case "JOB_HV_25" => JOB_HV_25 //CODE BY XTP
      case "JOB_HV_26" => JOB_HV_26 //CODE BY TZQ
      case "JOB_HV_31" => JOB_HV_31(sqlContext, start_dt, end_dt) //CODE BY XTP
      case "JOB_HV_34" => JOB_HV_34 //CODE BY XTP
      case "JOB_HV_35" => JOB_HV_35 //CODE BY XTP
      case "JOB_HV_37" => JOB_HV_37 //CODE BY TZQ
      case "JOB_HV_38" => JOB_HV_38 //CODE BY TZQ
      case "JOB_HV_45" => JOB_HV_45 //CODE BY YX
      case "JOB_HV_74" => JOB_HV_74//CODE BY TZQ
      case "JOB_HV_75" => JOB_HV_75 //CODE BY XTP
      case "JOB_HV_76" => JOB_HV_76 //CODE BY XTP

      /**
        * 新增其他JOB
        */
      case "JOB_HV_83" => JOB_HV_83(sqlContext, start_dt, end_dt) //CODE BY LT
      case "JOB_HV_84" => JOB_HV_84(sqlContext, start_dt, end_dt) //CODE BY LT
      case "JOB_HV_85" => JOB_HV_85(sqlContext, start_dt, end_dt) //CODE BY LT
      case "JOB_HV_86" => JOB_HV_86 //CODE BY LT
      case "JOB_HV_87" => JOB_HV_87 //CODE BY LT
      case "JOB_HV_88" => JOB_HV_88(sqlContext, start_dt, end_dt) //CODE BY LT
      case "JOB_HV_89" => JOB_HV_89  //CODE BY LT
      case _ => println("#### No Case Job,Please Input JobName")
    }

    sc.stop()

  }


  /**
    * JobName: JOB_HV_1
    * Feature: db2.tbl_chacc_cdhd_card_bind_inf -> hive.hive_card_bind_inf
    *
    * @author YangXue
    * @time 2016-08-19
    * @param sqlContext
    */
  def JOB_HV_1(implicit sqlContext: HiveContext) = {

    println("#### JOB_HV_1(tbl_chacc_cdhd_card_bind_inf -> hive_card_bind_inf)")
    println("#### JOB_HV_1 为全量抽取的表")

    DateUtils.timeCost("JOB_HV_1"){
      val df = sqlContext.readDB2_ACC(s"$schemas_accdb.TBL_CHACC_CDHD_CARD_BIND_INF")
      println("#### JOB_HV_1 readDB2_ACC 的系统时间为:"+DateUtils.getCurrentSystemTime())

      df.registerTempTable("spark_db2_card_bind_inf")
      println("#### JOB_HV_1 注册临时表的系统时间为:"+DateUtils.getCurrentSystemTime())

      val results = sqlContext.sql(
        """
          |select
          |cdhd_card_bind_seq as cdhd_card_bind_seq,
          |trim(cdhd_usr_id) as cdhd_usr_id,
          |trim(bind_tp) as bind_tp,
          |trim(bind_card_no) as bind_card_no,
          |bind_ts as bind_ts,
          |unbind_ts as unbind_ts,
          |trim(card_auth_st) as card_auth_st,
          |trim(card_bind_st) as card_bind_st,
          |trim(ins_id_cd) as ins_id_cd,
          |auth_ts as auth_ts,
          |trim(func_bmp) as func_bmp,
          |rec_crt_ts as rec_crt_ts,
          |rec_upd_ts as rec_upd_ts,
          |ver_no as ver_no,
          |sort_seq as sort_seq,
          |trim(cash_in) as cash_in,
          |trim(acct_point_ins_id_cd) as acct_point_ins_id_cd,
          |acct_owner as acct_owner,
          |bind_source as bind_source,
          |trim(card_media) as card_media,
          |backup_fld1 as backup_fld1,
          |backup_fld2 as backup_fld2,
          |trim(iss_ins_id_cd) as iss_ins_id_cd,
          |iss_ins_cn_nm as iss_ins_cn_nm,
          |case
          |when card_auth_st in ('1','2','3')
          |then min(rec_crt_ts)over(partition by cdhd_usr_id)
          |else null
          |end as frist_bind_ts
          |from spark_db2_card_bind_inf
        """.stripMargin)
      println("#### JOB_HV_1 spark sql 逻辑完成的系统时间为:"+DateUtils.getCurrentSystemTime())
      //println("#### JOB_HV_1------>results:"+results.count())

      results.registerTempTable("spark_card_bind_inf")
      println("#### JOB_HV_1 registerTempTable--spark_card_bind_inf完成的系统时间为:"+DateUtils.getCurrentSystemTime())

      if(!Option(results).isEmpty){
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql("insert overwrite table hive_card_bind_inf select * from spark_card_bind_inf")
        println("#### JOB_HV_1 全量数据插入完成的时间为："+DateUtils.getCurrentSystemTime())
      }else{
        println("#### JOB_HV_1 spark sql 逻辑处理后无数据！")
      }
    }
  }


  /**
    * JobName: JOB_HV_3
    * Feature: db2.tbl_chacc_cdhd_pri_acct_inf -> hive.hive_cdhd_pri_acct_inf
    *
    * @author YangXue
    * @time 2017-03-30
    * @param sqlContext
    */
  def JOB_HV_3(implicit sqlContext: HiveContext) = {
    println("#### JOB_HV_3(tbl_chacc_cdhd_pri_acct_inf -> hive_cdhd_pri_acct_inf)")
    println("#### JOB_HV_3 为全量抽取的表")

    DateUtils.timeCost("JOB_HV_3"){
      val df1 = sqlContext.readDB2_ACC(s"$schemas_accdb.TBL_CHACC_CDHD_PRI_ACCT_INF")
      println("#### JOB_HV_3 readDB2_ACC 的系统时间为:"+DateUtils.getCurrentSystemTime())

      val results=df1.registerTempTable("spark_db2_cdhd_pri_acct_inf")
      println("#### JOB_HV_3 注册临时表的系统时间为:"+DateUtils.getCurrentSystemTime())

      if(!Option(results).isEmpty){
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql("insert overwrite table hive_cdhd_pri_acct_inf select * from spark_db2_cdhd_pri_acct_inf")
        println("#### JOB_HV_3 全量数据插入完成的时间为："+DateUtils.getCurrentSystemTime())
      }else{
        println("#### JOB_HV_3 spark sql 逻辑处理后无数据！")
      }
    }
  }



  /**
    * JOB_HV_8/10-14
    * HIVE_STORE_TERM_RELATION->TBL_CHMGM_STORE_TERM_RELATION
    * Code by Xue
    *
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    * @return
    */
  def JOB_HV_8 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String) =  {

    println("#### JOB_HV_8(tbl_chmgm_store_term_relation,hive_trans_dtl ->hive_store_term_relation )")
    println("#### JOB_HV_8 为全量抽取的表")

    DateUtils.timeCost("JOB_HV_8") {
      val df = sqlContext.readDB2_MGM(s"$schemas_mgmdb.tbl_chmgm_store_term_relation")
      println("#### JOB_HV_8 readDB2_MGM 的系统时间为:"+DateUtils.getCurrentSystemTime())

      df.registerTempTable("tbl_chmgm_store_term_relation")
      println("#### JOB_HV_8 注册临时表的系统时间为:"+DateUtils.getCurrentSystemTime())

      sqlContext.sql(s"use $hive_dbname")
      val hive_trans_dtl = sqlContext.sql(s"select * from hive_trans_dtl where part_trans_dt>='$start_dt' and part_trans_dt<='$end_dt'")
      hive_trans_dtl.registerTempTable("hive_trans_dtl")


      val results = sqlContext.sql(
        """
         |select
         |tempa.rec_id as rec_id,
         |tempa.mchnt_cd as mchnt_cd,
         |tempa.term_id as term_id,
         |tempa.third_party_ins_fk as third_party_ins_fk,
         |tempa.rec_upd_usr_id as rec_upd_usr_id,
         |tempa.rec_upd_ts as rec_upd_ts,
         |tempa.rec_crt_usr_id as rec_crt_usr_id,
         |tempa.rec_crt_ts as rec_crt_ts,
         |tempa.third_party_ins_id as third_party_ins_id,
         |'0' as is_trans_tp
         |from
         |(
         |select
         |a.rec_id as rec_id,
         |a.mchnt_cd as mchnt_cd,
         |a.term_id as term_id,
         |a.third_party_ins_fk as third_party_ins_fk,
         |a.rec_upd_usr_id as rec_upd_usr_id,
         |a.rec_upd_ts as rec_upd_ts,
         |a.rec_crt_usr_id as rec_crt_usr_id,
         |a.rec_crt_ts as rec_crt_ts,
         |a.third_party_ins_id as third_party_ins_id,
         |b.card_accptr_cd as card_accptr_cd,
         |b.card_accptr_term_id as card_accptr_term_id
         |from
         |tbl_chmgm_store_term_relation a
         |left join
         |(
         |select distinct
         |card_accptr_term_id,
         |card_accptr_cd
         |from
         |hive_trans_dtl
         |) b
         |on a.mchnt_cd = b.card_accptr_cd and a.term_id = b.card_accptr_term_id
         |) tempa
         |where tempa.card_accptr_cd is null and tempa.card_accptr_term_id is null
         |
         |union all
         |
         |
         |select
         |tempb.rec_id as rec_id,
         |tempb.mchnt_cd as mchnt_cd,
         |tempb.term_id as term_id,
         |tempb.third_party_ins_fk as third_party_ins_fk,
         |tempb.rec_upd_usr_id as rec_upd_usr_id,
         |tempb.rec_upd_ts as rec_upd_ts,
         |tempb.rec_crt_usr_id as rec_crt_usr_id,
         |tempb.rec_crt_ts as rec_crt_ts,
         |tempb.third_party_ins_id as third_party_ins_id,
         |'1' as is_trans_tp
         |from
         |(
         |select
         |a.rec_id as rec_id,
         |a.mchnt_cd as mchnt_cd,
         |a.term_id as term_id,
         |a.third_party_ins_fk as third_party_ins_fk,
         |a.rec_upd_usr_id as rec_upd_usr_id,
         |a.rec_upd_ts as rec_upd_ts,
         |a.rec_crt_usr_id as rec_crt_usr_id,
         |a.rec_crt_ts as rec_crt_ts,
         |a.third_party_ins_id as third_party_ins_id,
         |b.card_accptr_cd as card_accptr_cd,
         |b.card_accptr_term_id as card_accptr_term_id
         |from
         |tbl_chmgm_store_term_relation a
         |left join
         |(
         |select distinct
         |card_accptr_term_id,
         |card_accptr_cd
         |from
         |hive_trans_dtl
         |) b
         |on a.mchnt_cd = b.card_accptr_cd and a.term_id = b.card_accptr_term_id
         |) tempb
         |where tempb.card_accptr_cd is not null  and tempb.card_accptr_term_id is not null
         | """.stripMargin)

      results.registerTempTable("spark_tbl_chmgm_store_term_relation")
      println("#### JOB_HV_8 spark sql 逻辑完成的系统时间为:"+DateUtils.getCurrentSystemTime())

     if(!Option(results).isEmpty){
       sqlContext.sql(s"use $hive_dbname")
       sqlContext.sql("insert overwrite table hive_store_term_relation select * from spark_tbl_chmgm_store_term_relation")
       println("#### JOB_HV_8 全量数据插入完成的时间为："+DateUtils.getCurrentSystemTime())
     }else{
       println("#### JOB_HV_8 spark sql 逻辑处理后无数据！")
     }
  }

 }


  /**
    * JobName:JOB_HV_9
    * Feature:tbl_chmgm_preferential_mchnt_inf->hive_preferential_mchnt_inf
    *
    * @author tzq
    * @time 2016-8-23
    * @param sqlContext
    * @return
    */
  def JOB_HV_9(implicit sqlContext: HiveContext) = {
    println("###JOB_HV_9[全量抽取](hive_preferential_mchnt_inf --->tbl_chmgm_preferential_mchnt_inf)")
    DateUtils.timeCost("JOB_HV_9"){
      val df = sqlContext.readDB2_MGM(s"$schemas_mgmdb.TBL_CHMGM_PREFERENTIAL_MCHNT_INF")
      println("#### JOB_HV_9 readDB2_MGM 的系统时间为:"+DateUtils.getCurrentSystemTime())

      df.registerTempTable("db2_tbl_chmgm_preferential_mchnt_inf")
      println("#### JOB_HV_9 注册临时表 的系统时间为:"+DateUtils.getCurrentSystemTime())
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

      println("#### JOB_HV_9 spark sql 逻辑完成的系统时间为:"+DateUtils.getCurrentSystemTime())
      //println("JOB_HV_9------>results:"+results.count())

      if(!Option(results).isEmpty){
        results.registerTempTable("spark_tbl_chmgm_preferential_mchnt_inf")
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql("truncate table  hive_preferential_mchnt_inf")
        sqlContext.sql("insert into table hive_preferential_mchnt_inf select * from spark_tbl_chmgm_preferential_mchnt_inf")
        println("#### JOB_HV_9 全量数据插入完成的系统时间为:"+DateUtils.getCurrentSystemTime())
      }else{
        println("#### JOB_HV_9 spark sql 逻辑处理后无数据！")
      }
    }

  }


  /**
    * JobName:JOB_HV_10
    * Feature:db2.tbl_chmgm_access_bas_inf->hive_access_bas_inf
    *
    * @author tzq
    * @time 2016-8-23
    * @param sqlContext
    */
  def JOB_HV_10(implicit sqlContext: HiveContext) = {
    println("###JOB_HV_10[全量抽取](hive_access_bas_inf->tbl_chmgm_access_bas_inf)")
    DateUtils.timeCost("JOB_HV_10"){
      val df = sqlContext.readDB2_MGM(s"$schemas_mgmdb.TBL_CHMGM_ACCESS_BAS_INF")
      println("#### JOB_HV_10 readDB2_ACC 的系统时间为:"+DateUtils.getCurrentSystemTime())
      df.registerTempTable("db2_tbl_chmgm_access_bas_inf")
      println("#### JOB_HV_10 注册临时表 的系统时间为:"+DateUtils.getCurrentSystemTime())
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
      println("#### JOB_HV_10 spark sql 逻辑完成的系统时间为:"+DateUtils.getCurrentSystemTime())
//      println("job10--------->原表：results :"+results.count())

      if(!Option(results).isEmpty){
        results.registerTempTable("spark_tbl_chmgm_access_bas_inf")
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql("truncate table  hive_access_bas_inf")
        sqlContext.sql("insert into table hive_access_bas_inf select * from spark_tbl_chmgm_access_bas_inf")
        println("#### JOB_HV_10 全量数据插入完成的系统时间为:"+DateUtils.getCurrentSystemTime())
      }else{
        println("#### JOB_HV_10 spark sql 逻辑处理后无数据！")
      }
    }

  }


  /**
    * JobName:JOB_HV_11
    * Feature:db2.tbl_chacc_ticket_bill_bas_inf->hive_ticket_bill_bas_inf
    *
    * @author tzq
    * @time 2016-8-23
    * @param sqlContext
    */
  def JOB_HV_11(implicit sqlContext: HiveContext) = {
    println("###JOB_HV_11[全量抽取](hive_ticket_bill_bas_inf->tbl_chacc_ticket_bill_bas_inf)")
    DateUtils.timeCost("JOB_HV_11") {
      val df = sqlContext.readDB2_ACC(s"$schemas_accdb.TBL_CHACC_TICKET_BILL_BAS_INF")
      println("#### JOB_HV_11 readDB2_ACC 的系统时间为:" + DateUtils.getCurrentSystemTime())
      df.registerTempTable("db2_tbl_chacc_ticket_bill_bas_inf")
      println("#### JOB_HV_11 注册临时表 的系统时间为:" + DateUtils.getCurrentSystemTime())
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
      println("#### JOB_HV_11 spark sql 逻辑完成的系统时间为:" + DateUtils.getCurrentSystemTime())
      //println("job11-------results:"+results.count())
      if (!Option(results).isEmpty) {
        results.registerTempTable("spark_db2_tbl_chacc_ticket_bill_bas_inf")
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql("truncate table  hive_ticket_bill_bas_inf")
        sqlContext.sql("insert into table hive_ticket_bill_bas_inf select * from spark_db2_tbl_chacc_ticket_bill_bas_inf")
        println("#### JOB_HV_11 全量数据插入完成的系统时间为:" + DateUtils.getCurrentSystemTime())
      } else {
        println("#### JOB_HV_11 spark sql 逻辑处理后无数据！")
      }
    }


  }

  /**
    * JobName:JOB_HV_12
    * Feature:db2.tbl_chmgm_chara_grp_def_bat->hive_chara_grp_def_bat
    *
    * @author tzq
    * @time 2016-08-23
    * @param sqlContext
    */
  def JOB_HV_12(implicit sqlContext: HiveContext) = {
    println("###JOB_HV_12[全量抽取](hive_chara_grp_def_bat->tbl_chmgm_chara_grp_def_bat)")
    DateUtils.timeCost("JOB_HV_12"){
        val df = sqlContext.readDB2_MGM(s"$schemas_mgmdb.TBL_CHMGM_CHARA_GRP_DEF_BAT")
        println("#### JOB_HV_12 readDB2_MGM 的系统时间为:" + DateUtils.getCurrentSystemTime())
        df.registerTempTable("db2_tbl_chmgm_chara_grp_def_bat")
        println("#### JOB_HV_12 注册表 的系统时间为:" + DateUtils.getCurrentSystemTime())
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
         println("#### JOB_HV_12 spark sql 逻辑完成的系统时间为:" + DateUtils.getCurrentSystemTime())
         //println("JOB_HV_12-------results:"+results.count())

        if(!Option(results).isEmpty){
          results.registerTempTable("spark_tbl_chmgm_chara_grp_def_bat")
          sqlContext.sql(s"use $hive_dbname")
          sqlContext.sql("truncate table  hive_chara_grp_def_bat")
          sqlContext.sql("insert into table hive_chara_grp_def_bat select * from spark_tbl_chmgm_chara_grp_def_bat")
          println("#### JOB_HV_12 全量数据插入完成的系统时间为:" + DateUtils.getCurrentSystemTime())
        }else{
          println("#### JOB_HV_12 spark sql 逻辑处理后无数据！")
        }
      }

  }


  /**
    * JobName:hive-job-13/08-22
    * Fethure:hive_card_bin->tbl_chmgm_card_bin
    *
    * @author tzq
    * @param sqlContext
    * @return
    */
  def JOB_HV_13(implicit sqlContext: HiveContext) = {
    println("###JOB_HV_13[全量抽取](hive_card_bin->tbl_chmgm_card_bin)")
    DateUtils.timeCost("JOb_HV_13"){
      val df = sqlContext.readDB2_MGM(s"$schemas_mgmdb.TBL_CHMGM_CARD_BIN")
      println("#### JOB_HV_13 readDB2_MGM 的系统时间为:" + DateUtils.getCurrentSystemTime())

      df.registerTempTable("db2_tbl_chmgm_card_bin")
      println("#### JOB_HV_13 注册表 的系统时间为:" + DateUtils.getCurrentSystemTime())
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
      println("#### JOB_HV_13 spark sql 逻辑完成的系统时间为:" + DateUtils.getCurrentSystemTime())
      // println("JOB_HV_13---------results: "+results.count())

      if(!Option(results).isEmpty){
        results.registerTempTable("spark_db2_tbl_chmgm_card_bin")
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql("truncate table  hive_card_bin")
        sqlContext.sql("insert into table hive_card_bin select * from spark_db2_tbl_chmgm_card_bin")
        println("#### JOB_HV_13 全量数据插入完成的系统时间为:" + DateUtils.getCurrentSystemTime())
      }else{
        println("#### JOB_HV_13 spark sql 逻辑处理后无数据！")
      }
    }

  }


  /**
    * JobName:JOB_HV_14
    * Feature:hive_inf_source_dtl->tbl_inf_source_dtl
    *
    * @time 2016-8-23
    * @author tzq
    * @param sqlContext
    * @return
    */
  def JOB_HV_14(implicit sqlContext: HiveContext) = {
    println("###JOB_HV_14[全量抽取](hive_inf_source_dtl->tbl_inf_source_dtl)")
    DateUtils.timeCost("JOB_HV_14"){
      val df = sqlContext.readDB2_ACC(s"$schemas_accdb.TBL_INF_SOURCE_DTL")
      println("#### JOB_HV_14 readDB2_ACC 的系统时间为:" + DateUtils.getCurrentSystemTime())
      df.registerTempTable("db2_tbl_inf_source_dtl")
      println("#### JOB_HV_14 注册表 的系统时间为:" + DateUtils.getCurrentSystemTime())
      val results = sqlContext.sql(
          """
            |select
            |trim(access_id) as access_id ,
            |access_nm
            |from
            |db2_tbl_inf_source_dtl
          """.stripMargin)
        println("#### JOB_HV_14 spark sql 逻辑完成的系统时间为:" + DateUtils.getCurrentSystemTime())
        //println("JOB_HV_14-------------results :"+results.count())

        if(!Option(results).isEmpty){
          results.registerTempTable("spark_db2_tbl_inf_source_dtl")
          sqlContext.sql(s"use $hive_dbname")
          sqlContext.sql("truncate table  hive_inf_source_dtl")
          sqlContext.sql("insert into table hive_inf_source_dtl select * from spark_db2_tbl_inf_source_dtl")
          println("#### JOB_HV_14 全量数据插入完成的系统时间为:" + DateUtils.getCurrentSystemTime())
        }else{
          println("#### JOB_HV_14 spark sql 逻辑处理后无数据！")
        }
      }

  }

  /**
    * JobName: JOB_HV_15/2016-8-23
    * Feature: hive_undefine_store_inf-->  hive_acc_trans + hive_store_term_relation
    * @author tzq
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    */
  def JOB_HV_15(implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {
    println("###JOB_HV_15(hive_undefine_store_inf-->  hive_acc_trans + hive_store_term_relation)")
    DateUtils.timeCost("JOB_HV_15"){
      sqlContext.sql(s"use $hive_dbname")
      val results = sqlContext.sql(
        s"""
          |select
          |c.card_accptr_cd as mchnt_cd,
          |c.card_accptr_term_id as term_id,
          |null as store_cd,
          |null as store_grp_cd,
          |null as brand_id
          |from
          |(select
          |a.card_accptr_cd,a.card_accptr_term_id
          |from
          |(select
          |card_accptr_term_id,
          |card_accptr_cd
          |from hive_acc_trans
          |where part_trans_dt >='$start_dt'and part_trans_dt<='$end_dt'
          |group by card_accptr_term_id,card_accptr_cd) a
          |
          |left join
          |(
          |select
          |mchnt_cd,term_id
          |from
          |hive_store_term_relation  )b
          |
          |on
          |a.card_accptr_cd = b.mchnt_cd and a.card_accptr_term_id = b.term_id
          |
          |where b.mchnt_cd is null
          |
          |) c
          |
          |left join
          |
          |hive_undefine_store_inf d
          |on
          |c.card_accptr_cd = d.mchnt_cd and
          |c.card_accptr_term_id = d.term_id
          |where d.mchnt_cd is null
          |
          |
        """.stripMargin)
      println("#### JOB_HV_15 spark sql 逻辑完成的系统时间为:" + DateUtils.getCurrentSystemTime())
      if(!Option(results).isEmpty){
        results.registerTempTable("hive_undefine_store_inf_tmp")
        println("#### JOB_HV_15 注册临时表 的系统时间为:" + DateUtils.getCurrentSystemTime())
        sqlContext.sql("truncate table  hive_undefine_store_inf_temp ")
        sqlContext.sql("insert into  table hive_undefine_store_inf_temp select * from hive_undefine_store_inf_tmp")
        sqlContext.sql("insert into  table hive_undefine_store_inf select * from hive_undefine_store_inf_temp")
        println("#### JOB_HV_15 全量数据插入完成的系统时间为:" + DateUtils.getCurrentSystemTime())
      }else{
        println("#### JOB_HV_15 spark sql 逻辑处理后无数据！")
      }
    }

  }

  /**
    * JobName:JOB_HV_16
    * Feature:hive_mchnt_inf_wallet->tbl_chmgm_mchnt_inf/TBL_CHMGM_STORE_TERM_RELATION/TBL_CHMGM_ACCESS_BAS_INF
    *
    * @time 2016-8-23
    * @author tzq
    * @param sqlContext
    * @return
    */
  def JOB_HV_16(implicit sqlContext: HiveContext) = {
    println("###JOB_HV_16[全量抽取](hive_mchnt_inf_wallet->tbl_chmgm_mchnt_inf/TBL_CHMGM_STORE_TERM_RELATION/TBL_CHMGM_ACCESS_BAS_INF)")
    DateUtils.timeCost("JOB_HV_16"){
      val df = sqlContext.readDB2_MGM(s"$schemas_mgmdb.TBL_CHMGM_MCHNT_INF")
      println("#### JOB_HV_16 readDB2_MGM (TBL_CHMGM_MCHNT_INF) 的系统时间为:" + DateUtils.getCurrentSystemTime())
      df.registerTempTable("db2_tbl_chmgm_mchnt_inf")
      println("#### JOB_HV_16 注册临时表db2_tbl_chmgm_mchnt_inf 的系统时间为:" + DateUtils.getCurrentSystemTime())

      val df1 = sqlContext.readDB2_MGM(s"$schemas_mgmdb.TBL_CHMGM_STORE_TERM_RELATION")
      println("#### JOB_HV_16 readDB2_MGM (TBL_CHMGM_STORE_TERM_RELATION) 的系统时间为:" + DateUtils.getCurrentSystemTime())
      df1.registerTempTable("db2_tbl_chmgm_store_term_relation")
      println("#### JOB_HV_16 注册临时表db2_tbl_chmgm_store_term_relation 的系统时间为:" + DateUtils.getCurrentSystemTime())

      val df2 = sqlContext.readDB2_MGM(s"$schemas_mgmdb.TBL_CHMGM_ACCESS_BAS_INF")
      println("#### JOB_HV_16 readDB2_MGM (TBL_CHMGM_ACCESS_BAS_INF) 的系统时间为:" + DateUtils.getCurrentSystemTime())
      df2.registerTempTable("db2_tbl_chmgm_access_bas_inf")
      println("#### JOB_HV_16 注册临时表db2_tbl_chmgm_access_bas_inf 的系统时间为:" + DateUtils.getCurrentSystemTime())

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
          |else '其它' end  as gb_region_nm,
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
      println("#### JOB_HV_16 spark sql 逻辑完成的系统时间为:" + DateUtils.getCurrentSystemTime())
      //println("###JOB_HV_16-----results:"+results.count())

      if(!Option(results).isEmpty){
        results.registerTempTable("spark_tbl_chmgm_mchnt_inf")
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql("truncate table  hive_mchnt_inf_wallet")
        sqlContext.sql("insert into table hive_mchnt_inf_wallet select * from spark_tbl_chmgm_mchnt_inf")
        println("#### JOB_HV_16 全量数据插入完成的系统时间为:" + DateUtils.getCurrentSystemTime())
      }else{
        println("#### JOB_HV_16 spark sql 逻辑处理后无数据！")
      }
    }
  }


  /**
    * JobName: JOB_HV_18
    * Feature: db2.viw_chmgm_trans_his -> hive.hive_download_trans
    *
    * @author YangXue
    * @time 2016-08-26
    * @param sqlContext,start_dt,end_dt
    */
  def JOB_HV_18(implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {

    println("#### JOB_HV_18(viw_chmgm_trans_his -> hive_download_trans)")

    val start_day = start_dt.replace("-","")
    val end_day = end_dt.replace("-","")
    println("#### JOB_HV_18 增量抽取的时间范围: "+start_day+"--"+end_day)

    DateUtils.timeCost("JOB_HV_18"){
      val df = sqlContext.readDB2_MGM_4para(s"$schemas_mgmdb.VIW_CHMGM_TRANS_HIS","trans_dt",s"$start_day",s"$end_day")
      println("#### JOB_HV_18 readDB2_MGM_4para 的系统时间为:"+DateUtils.getCurrentSystemTime())

      df.registerTempTable("db2_trans_his")
      println("#### JOB_HV_18 注册临时表的系统时间为:"+DateUtils.getCurrentSystemTime())

      val results = sqlContext.sql(
        s"""
           |select
           |seq_id as seq_id,
           |trim(cdhd_usr_id)    as cdhd_usr_id,
           |trim(pri_acct_no)    as pri_acct_no,
           |trim(acpt_ins_id_cd) as acpt_ins_id_cd,
           |trim(fwd_ins_id_cd)  as fwd_ins_id_cd,
           |trim(sys_tra_no)     as sys_tra_no,
           |trim(tfr_dt_tm)      as tfr_dt_tm,
           |trim(rcv_ins_id_cd)  as rcv_ins_id_cd,
           |onl_trans_tra_no as onl_trans_tra_no,
           |trim(mchnt_cd)       as mchnt_cd,
           |mchnt_nm as mchnt_nm,
           |case
           |	when
           |		substr(trans_dt,1,4) between '0001' and '9999' and substr(trans_dt,5,2) between '01' and '12' and
           |		substr(trans_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(trans_dt,1,4),substr(trans_dt,5,2),substr(trans_dt,7,2))),9,2)
           |	then concat_ws('-',substr(trans_dt,1,4),substr(trans_dt,5,2),substr(trans_dt,7,2))
           |	else null
           |end as trans_dt,
           |trim(trans_tm) as trans_tm,
           |case
           |	when length(trim(translate(trim(trans_at),'0123456789',' ')))=0 then trans_at
           |	else null
           |end as trans_at,
           |trim(buss_tp) 			as buss_tp,
           |trim(um_trans_id) 	as um_trans_id,
           |trim(swt_right_tp) 	as swt_right_tp,
           |trim(swt_right_nm) 	as swt_right_nm,
           |trim(bill_id) as bill_id,
           |bill_nm as bill_nm,
           |trim(chara_acct_tp) as chara_acct_tp,
           |point_at as point_at,
           |bill_num as bill_num,
           |trim(chara_acct_nm) as chara_acct_nm,
           |plan_id as plan_id,
           |trim(sys_det_cd) 		as sys_det_cd,
           |trim(acct_addup_tp) as acct_addup_tp,
           |remark as remark,
           |trim(bill_item_id) 	as bill_item_id,
           |trim(trans_st) 			as trans_st,
           |case
           |	when length(trim(translate(trim(discount_at),'0123456789',' ')))=0 then discount_at
           |	else null
           |end as discount_at,
           |trim(booking_st) 		as booking_st,
           |plan_nm as plan_nm,
           |trim(bill_acq_md) 	as bill_acq_md,
           |trim(oper_in) 			as oper_in,
           |rec_crt_ts as rec_crt_ts,
           |rec_upd_ts as rec_upd_ts,
           |trim(term_id) 			as term_id,
           |trim(pos_entry_md_cd) as pos_entry_md_cd,
           |orig_data_elemnt as orig_data_elemnt,
           |udf_fld as udf_fld,
           |trim(card_accptr_nm_addr) as card_accptr_nm_addr,
           |trim(token_card_no) as token_card_no,
           |case
           |	when
           |		substr(trans_dt,1,4) between '0001' and '9999' and substr(trans_dt,5,2) between '01' and '12' and
           |		substr(trans_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(trans_dt,1,4),substr(trans_dt,5,2),substr(trans_dt,7,2))),9,2)
           |	then concat_ws('-',substr(trans_dt,1,4),substr(trans_dt,5,2),substr(trans_dt,7,2))
           |	else substr(rec_crt_ts,1,10)
           | end as p_trans_dt
           |from
           |db2_trans_his
           |where
           |trim(um_trans_id) in ('12','17')
         """.stripMargin)
      println("#### JOB_HV_18 spark sql 逻辑完成的系统时间为:"+DateUtils.getCurrentSystemTime())
      //println("###JOB_HV_18------>results:"+results.count())

      results.registerTempTable("spark_trans_his")
      println("#### JOB_HV_18 registerTempTable--spark_trans_his 完成的系统时间为:"+DateUtils.getCurrentSystemTime())

      if(!Option(results).isEmpty){
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql(
          s"""
             |insert overwrite table hive_download_trans partition (part_trans_dt)
             |select
             |seq_id,
             |cdhd_usr_id,
             |pri_acct_no,
             |acpt_ins_id_cd,
             |fwd_ins_id_cd,
             |sys_tra_no,
             |tfr_dt_tm,
             |rcv_ins_id_cd,
             |onl_trans_tra_no,
             |mchnt_cd,
             |mchnt_nm,
             |trans_dt,
             |trans_tm,
             |trans_at,
             |buss_tp,
             |um_trans_id,
             |swt_right_tp,
             |swt_right_nm,
             |bill_id,
             |bill_nm,
             |chara_acct_tp,
             |point_at,
             |bill_num,
             |chara_acct_nm,
             |plan_id,
             |sys_det_cd,
             |acct_addup_tp,
             |remark,
             |bill_item_id,
             |trans_st,
             |discount_at,
             |booking_st,
             |plan_nm,
             |bill_acq_md,
             |oper_in,
             |rec_crt_ts,
             |rec_upd_ts,
             |term_id,
             |pos_entry_md_cd,
             |orig_data_elemnt,
             |udf_fld,
             |card_accptr_nm_addr,
             |token_card_no,
             |p_trans_dt
             |from
             |spark_trans_his
         """.stripMargin)
        println("#### JOB_HV_18 动态分区插入完成的时间为："+DateUtils.getCurrentSystemTime())
      }else{
        println("#### JOB_HV_18 spark sql 逻辑处理后无数据！")
      }
    }
  }


  /**
    * JobName: JOB_HV_19
    * Feature: db2.tbl_chmgm_ins_inf -> hive.hive_ins_inf
    *
    * @author YangXue
    * @time 2016-08-19
    * @param sqlContext
    */
  def JOB_HV_19(implicit sqlContext: HiveContext) = {

    println("#### JOB_HV_19(tbl_chmgm_ins_inf -> hive_ins_inf)")
    println("#### JOB_HV_19 为全量抽取的表")

    DateUtils.timeCost("JOB_HV_19"){
      val df = sqlContext.readDB2_MGM(s"$schemas_mgmdb.TBL_CHMGM_INS_INF")
      println("#### JOB_HV_19 readDB2_MGM 的系统时间为:"+DateUtils.getCurrentSystemTime())

      df.registerTempTable("spark_db2_ins_inf")
      println("#### JOB_HV_19 注册临时表的系统时间为:"+DateUtils.getCurrentSystemTime())

      val results = sqlContext.sql(
        """
          |select
          |	trim(ins_id_cd) as ins_id_cd,
          |	trim(ins_cata) as ins_cata,
          |	trim(ins_tp) as ins_tp,
          |	trim(hdqrs_ins_id_cd) as hdqrs_ins_id_cd,
          |	cup_branch_ins_id_cd,
          | 	case
          |		when trim(cup_branch_ins_id_cd)='00011000' then '北京'
          |		when trim(cup_branch_ins_id_cd)='00011100' then '天津'
          |		when trim(cup_branch_ins_id_cd)='00011200' then '河北'
          |		when trim(cup_branch_ins_id_cd)='00011600' then '山西'
          |		when trim(cup_branch_ins_id_cd)='00011900' then '内蒙古'
          |		when trim(cup_branch_ins_id_cd)='00012210' then '辽宁'
          |		when trim(cup_branch_ins_id_cd)='00012220' then '大连'
          |		when trim(cup_branch_ins_id_cd)='00012400' then '吉林'
          |		when trim(cup_branch_ins_id_cd)='00012600' then '黑龙江'
          |		when trim(cup_branch_ins_id_cd)='00012900' then '上海'
          |		when trim(cup_branch_ins_id_cd)='00013000' then '江苏'
          |		when trim(cup_branch_ins_id_cd)='00013310' then '浙江'
          |		when trim(cup_branch_ins_id_cd)='00013320' then '宁波'
          |		when trim(cup_branch_ins_id_cd)='00013600' then '安徽'
          |		when trim(cup_branch_ins_id_cd)='00013900' then '福建'
          |		when trim(cup_branch_ins_id_cd)='00013930' then '厦门'
          |		when trim(cup_branch_ins_id_cd)='00014200' then '江西'
          |		when trim(cup_branch_ins_id_cd)='00014500' then '山东'
          |		when trim(cup_branch_ins_id_cd)='00014520' then '青岛'
          |		when trim(cup_branch_ins_id_cd)='00014900' then '河南'
          |		when trim(cup_branch_ins_id_cd)='00015210' then '湖北'
          |		when trim(cup_branch_ins_id_cd)='00015500' then '湖南'
          |		when trim(cup_branch_ins_id_cd)='00015800' then '广东'
          |		when trim(cup_branch_ins_id_cd)='00015840' then '深圳'
          |		when trim(cup_branch_ins_id_cd)='00016100' then '广西'
          |		when trim(cup_branch_ins_id_cd)='00016400' then '海南'
          |		when trim(cup_branch_ins_id_cd)='00016500' then '四川'
          |		when trim(cup_branch_ins_id_cd)='00016530' then '重庆'
          |		when trim(cup_branch_ins_id_cd)='00017000' then '贵州'
          |		when trim(cup_branch_ins_id_cd)='00017310' then '云南'
          |		when trim(cup_branch_ins_id_cd)='00017700' then '西藏'
          |		when trim(cup_branch_ins_id_cd)='00017900' then '陕西'
          |		when trim(cup_branch_ins_id_cd)='00018200' then '甘肃'
          |		when trim(cup_branch_ins_id_cd)='00018500' then '青海'
          |		when trim(cup_branch_ins_id_cd)='00018700' then '宁夏'
          |		when trim(cup_branch_ins_id_cd)='00018800' then '新疆'
          |		else '总公司'
          |	end as cup_branch_ins_id_nm,
          |	trim(frn_cup_branch_ins_id_cd) as frn_cup_branch_ins_id_cd,
          |	trim(area_cd) as area_cd,
          |	trim(admin_division_cd) as admin_division_cd,
          |	trim(region_cd) as region_cd,
          |	ins_cn_nm,
          |	ins_cn_abbr,
          |	ins_en_nm,
          |	ins_en_abbr,
          |	trim(ins_st) as ins_st,
          |	lic_no,
          |	trim(artif_certif_id) as artif_certif_id,
          |	artif_nm,
          |	trim(artif_certif_expire_dt) as artif_certif_expire_dt,
          |	principal_nm,
          |	contact_person_nm,
          |	phone,
          |	fax_no,
          |	email_addr,
          |	ins_addr,
          |	trim(zip_cd) as zip_cd,
          |	trim(oper_in) as oper_in,
          |	event_id,
          |	rec_id,
          |	trim(rec_upd_usr_id) as rec_upd_usr_id,
          |	rec_upd_ts,
          |	rec_crt_ts
          |from spark_db2_ins_inf
        """.stripMargin
      )
      println("#### JOB_HV_19 spark sql 逻辑完成的系统时间为:"+DateUtils.getCurrentSystemTime())
      //println("#### JOB_HV_19------>results:"+results.count())

      results.registerTempTable("spark_ins_inf")
      println("#### JOB_HV_19 registerTempTable--spark_ins_inf 完成的系统时间为:"+DateUtils.getCurrentSystemTime())

      if(!Option(results).isEmpty){
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql("insert overwrite table hive_ins_inf select * from spark_ins_inf")
        println("#### JOB_HV_19 全量数据插入完成的时间为："+DateUtils.getCurrentSystemTime())
      }else{
        println("#### JOB_HV_19 spark sql 逻辑处理后无数据！")
      }
    }
  }


  /**
    * JobName: JOB_HV_20_INI_1
    * Feature: db2.tbl_chmgm_term_inf -> hive.hive_term_inf
    *
    * @author YangXue
    * @time 2016-10-31
    * @param sqlContext
    */
  def JOB_HV_20_INI_1 (implicit sqlContext: HiveContext) = {

    println("#### JOB_HV_20_INI_1(tbl_chmgm_term_inf -> hive_term_inf)")
    println("#### JOB_HV_20_INI_1 为全量抽取的表")
    DateUtils.timeCost("JOB_HV_20_INI_1") {
      val df = sqlContext.readDB2_MGM(s"$schemas_mgmdb.tbl_chmgm_term_inf")
      println("#### JOB_HV_20_INI_1 readDB2_MGM 的系统时间为:" + DateUtils.getCurrentSystemTime())

      df.registerTempTable("spark_db2_tbl_chmgm_term_inf")
      println("#### JOB_HV_20_INI_I 注册临时表的系统时间为:" + DateUtils.getCurrentSystemTime())

      val results = sqlContext.sql(
        s"""
           |select
           |mchnt_cd,
           |term_id,
           |term_tp,
           |term_st,
           |case
           |when substr(open_dt,1,4) between '0001' and '9999' and
           |     substr(open_dt,5,2) between '01' and '12' and
           |     substr(open_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(open_dt,1,4),substr(open_dt,5,2),substr(7,2))),9,2)
           |then concat_ws('-',substr(open_dt,1,4),substr(open_dt,5,2),substr(open_dt,7,2))
           |else null
           |end as open_dt,
           |case
           |when substr(close_dt,1,4) between '0001' and '9999' and
           |     substr(close_dt,5,2) between '01' and '12' and
           |     substr(close_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(close_dt,1,4),substr(close_dt,5,2),substr(close_dt,7,2))),9,2)
           |then concat_ws('-',substr(close_dt,1,4),substr(close_dt,5,2),substr(close_dt,7,2))
           |else null
           |end as close_dt,
           |bank_cd,
           |rec_st,
           |last_oper_in,
           |event_id,
           |rec_id,
           |rec_upd_usr_id,
           |rec_upd_ts,
           |rec_crt_ts,
           |'0' as is_trans_at_tp
           |from
           |spark_db2_tbl_chmgm_term_inf
           """.stripMargin)
      println("#### JOB_HV_20_INI_1 spark sql 逻辑完成的系统时间为:" + DateUtils.getCurrentSystemTime())
      //println("#### JOB_HV_20_INI_1------>results:"+results.count())

      results.registerTempTable("spark_hive_term_inf_1")
      println("#### JOB_HV_20_INI_1 registerTempTable-- spark_hive_term_inf_1 完成的系统时间为:" + DateUtils.getCurrentSystemTime())


      if (!Option(results).isEmpty) {
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql(s"insert overwrite table hive_term_inf_1 select * from spark_hive_term_inf_1")
        println("#### JOB_HV_20_INI_1 全量数据插入完成的时间为：" + DateUtils.getCurrentSystemTime())
      } else {
        println("#### JOB_HV_20_INI_1 spark sql 逻辑处理后无数据！")
      }
    }
  }

  /**
    * JOB_HV_20_INI_2/2017-01-12
    * hive_term_inf->hive_term_inf,hive_acc_trans
    * Code by Xue
    *
    * @param sqlContext
    * @return
    */
  def JOB_HV_20_INI_2(implicit sqlContext: HiveContext) = {
    println("#### JOB_HV_20_INI_2(hive_term_inf->hive_term_inf,hive_acc_trans)")

    DateUtils.timeCost("JOB_HV_20_INI_2") {

      sqlContext.sql(s"use $hive_dbname")
      val df_1 = sqlContext.sql(s"select * from hive_term_inf_1")
      df_1.registerTempTable("hive_term_inf_1")
      println("#### 临时表 hive_term_inf 的时间为:" + DateUtils.getCurrentSystemTime())

      val df_2 = sqlContext.sql(s"select distinct card_accptr_term_id,card_accptr_cd,trans_at from hive_acc_trans where trans_at is not NULL")
      df_2.registerTempTable("hive_acc_trans_2")
      println("#### 临时表 hive_acc_trans_2 的时间为:" + DateUtils.getCurrentSystemTime())

      val results = sqlContext.sql(
        s"""
           |select
           |T.mchnt_cd          as       mchnt_cd         ,
           |T.term_id           as       term_id          ,
           |T.term_tp           as       term_tp          ,
           |T.term_st           as       term_st          ,
           |case
           |	when
           |		substr(T.open_dt,1,4) between '0001' and '9999' and substr(T.open_dt,5,2) between '01' and '12' and
           |		substr(T.open_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(T.open_dt,1,4),substr(T.open_dt,5,2),substr(T.open_dt,7,2))),9,2)
           |	then concat_ws('-',substr(T.open_dt,1,4),substr(T.open_dt,5,2),substr(T.open_dt,7,2))
           |	else null
           |end as open_dt,
           |case
           |	when
           |		substr(T.close_dt,1,4) between '0001' and '9999' and substr(T.close_dt,5,2) between '01' and '12' and
           |		substr(T.close_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(T.close_dt,1,4),substr(T.close_dt,5,2),substr(T.close_dt,7,2))),9,2)
           |	then concat_ws('-',substr(T.close_dt,1,4),substr(T.close_dt,5,2),substr(T.close_dt,7,2))
           |	else null
           |end as close_dt,
           |T.bank_cd           as       bank_cd          ,
           |T.rec_st            as       rec_st           ,
           |T.last_oper_in      as       last_oper_in     ,
           |T.event_id          as       event_id         ,
           |T.rec_id            as       rec_id           ,
           |T.rec_upd_usr_id    as       rec_upd_usr_id   ,
           |T.rec_upd_ts        as       rec_upd_ts       ,
           |T.rec_crt_ts        as       rec_crt_ts       ,
           |T.is_trans_at_tp    as       is_trans_at_tp
           |from
           |(
           |
           |select
           |A.mchnt_cd    as   mchnt_cd   ,
           |A.term_id  as term_id        ,
           |A.term_tp    as term_tp      ,
           |A.term_st  as term_st        ,
           |A.open_dt     as open_dt     ,
           |A.close_dt     as close_dt    ,
           |A.bank_cd    as bank_cd      ,
           |A.rec_st     as rec_st      ,
           |A.last_oper_in  as last_oper_in   ,
           |A.event_id   as event_id      ,
           |A.rec_id   as rec_id        ,
           |A.rec_upd_usr_id as rec_upd_usr_id  ,
           |A.rec_upd_ts   as rec_upd_ts    ,
           |A.rec_crt_ts   as rec_crt_ts    ,
           |'1' as is_trans_at_tp
           |from
           |hive_term_inf_1  A
           |left join
           |hive_acc_trans_2 B
           |on
           |A.mchnt_cd = B.card_accptr_cd and A.term_id = B.card_accptr_term_id
           |where B.card_accptr_cd is not null and B.card_accptr_term_id is not null
           |
           |union all
           |
           |select
           |C.mchnt_cd    as   mchnt_cd   ,
           |C.term_id  as term_id        ,
           |C.term_tp    as term_tp      ,
           |C.term_st  as term_st        ,
           |C.open_dt     as open_dt     ,
           |C.close_dt     as close_dt    ,
           |C.bank_cd    as bank_cd      ,
           |C.rec_st     as rec_st      ,
           |C.last_oper_in  as last_oper_in   ,
           |C.event_id   as event_id      ,
           |C.rec_id   as rec_id        ,
           |C.rec_upd_usr_id as rec_upd_usr_id  ,
           |C.rec_upd_ts   as rec_upd_ts    ,
           |C.rec_crt_ts   as rec_crt_ts    ,
           |C.is_trans_at_tp as is_trans_at_tp
           |from
           |hive_term_inf  C
           |left join
           |hive_acc_trans_2 D
           |on
           |C.mchnt_cd = D.card_accptr_cd and C.term_id = D.card_accptr_term_id
           |where D.card_accptr_cd is null and D.card_accptr_term_id is null
           |
           |) T
           |
           | """.stripMargin)

      results.registerTempTable("spark_hive_term_inf_ini_2")

      println("#### JOB_HV_20_INI_2 registerTempTable-- spark_hive_term_inf_ini_2 完成的时间为:" + DateUtils.getCurrentSystemTime())


      if (!Option(results).isEmpty) {
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql(
          s"""
             |insert overwrite table hive_term_inf_copy
             |select *
             |from spark_hive_term_inf_ini_2
          """.stripMargin)
        println("#### JOB_HV_20_INI_2 全量数据插入完成的时间为：" + DateUtils.getCurrentSystemTime())
      } else {
        println("#### JOB_HV_20_INI_2 spark sql 逻辑处理后无数据！")
      }
    }

  }


  /**
    * JOB_HV_20/2017-01-11
    * hive_term_inf->hive_term_inf,tbl_chmgm_term_inf,hive_acc_trans
    * Code by Xue
    *
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    * @return
    */
  def JOB_HV_20(implicit sqlContext: HiveContext, start_dt: String, end_dt: String) = {
    println("#### JOB_HV_20(hive_term_inf->hive_term_inf,tbl_chmgm_term_inf,hive_acc_trans)")

    DateUtils.timeCost("JOB_HV_20") {
      val start_day = start_dt.concat(" 00:00:00")
      val end_day = end_dt.concat(" 00:00:00")
      println("#### JOB_HV_20 全量抽取的时间范围: " + start_day + "--" + end_day)

      val df = sqlContext.readDB2_MGM_4para(s"$schemas_mgmdb.tbl_chmgm_term_inf", "rec_upd_ts", s"$start_day", s"$end_day")
      df.registerTempTable("tbl_chmgm_term_inf")
      println("#### JOB_HV_20 readDB2_ACC_4para--tbl_chmgm_term_inf 的时间为:" + DateUtils.getCurrentSystemTime())

      sqlContext.sql(s"use $hive_dbname")
      val df_1 = sqlContext.sql(s"select * from hive_term_inf_copy")
      df_1.registerTempTable("hive_term_inf_copy")
      println("#### 临时表 hive_term_inf_copy 的时间为:" + DateUtils.getCurrentSystemTime())

      sqlContext.sql(s"use $hive_dbname")
      val df_2 = sqlContext.sql(s"select * from hive_acc_trans where part_trans_dt>='$start_dt' and part_trans_dt<='$end_dt'")
      df_2.registerTempTable("hive_acc_trans")
      println("#### 临时表 hive_acc_trans 的时间为:" + DateUtils.getCurrentSystemTime())

      val JOB_HV_20_H = sqlContext.sql(
        """
          |select
          |e.mchnt_cd         ,
          |e.term_id          ,
          |e.term_tp          ,
          |e.term_st          ,
          |e.open_dt          ,
          |e.close_dt         ,
          |e.bank_cd          ,
          |e.rec_st           ,
          |e.last_oper_in     ,
          |e.event_id         ,
          |e.rec_id           ,
          |e.rec_upd_usr_id   ,
          |e.rec_upd_ts       ,
          |e.rec_crt_ts       ,
          |case when e.is_trans_at_tp = '0' and f.trans_at is not null then '1'
          |	 when e.is_trans_at_tp = '1' and f.trans_at is null then '2'
          |	 when e.is_trans_at_tp = '2' and f.trans_at is not null then '1'
          |	 else e.is_trans_at_tp
          |	 end as is_trans_at_tp
          |from
          |
          |(
          |
          |select
          |a.mchnt_cd   as   mchnt_cd    ,
          |a.term_id  as    term_id      ,
          |a.term_tp    as term_tp       ,
          |a.term_st     as term_st      ,
          |a.open_dt   as open_dt       ,
          |a.close_dt  as close_dt       ,
          |a.bank_cd    as bank_cd      ,
          |a.rec_st    as rec_st       ,
          |a.last_oper_in  as last_oper_in   ,
          |a.event_id     as event_id    ,
          |a.rec_id    as rec_id       ,
          |a.rec_upd_usr_id   as rec_upd_usr_id ,
          |a.rec_upd_ts   as rec_upd_ts    ,
          |a.rec_crt_ts  as rec_crt_ts     ,
          |b.is_trans_at_tp as is_trans_at_tp
          |from
          |hive_term_inf_copy b
          |left  join tbl_chmgm_term_inf a
          |on b.mchnt_cd = a.mchnt_cd and b.term_id = a.term_id
          |where a.mchnt_cd is not null and a.term_id is not null
          |
          |union all
          |
          |select
          |a.mchnt_cd   as mchnt_cd      ,
          |a.term_id     as term_id     ,
          |a.term_tp     as term_tp     ,
          |a.term_st    as term_st      ,
          |a.open_dt    as open_dt      ,
          |a.close_dt   as close_dt      ,
          |a.bank_cd    as bank_cd      ,
          |a.rec_st    as rec_st       ,
          |a.last_oper_in   as last_oper_in   ,
          |a.event_id    as event_id     ,
          |a.rec_id    as rec_id       ,
          |a.rec_upd_usr_id  as rec_upd_usr_id ,
          |a.rec_upd_ts   as rec_upd_ts    ,
          |a.rec_crt_ts    as rec_crt_ts   ,
          |'0' as is_trans_at_tp
          |from
          |hive_term_inf_copy b
          |left join tbl_chmgm_term_inf a
          |on b.mchnt_cd = a.mchnt_cd and b.term_id = a.term_id
          |where a.mchnt_cd is  null and a.term_id is  null
          |
          |) e
          |
          |left join
          |
          |(
          |select
          |tempb.card_accptr_term_id,
          |tempb.card_accptr_cd,
          |tempb.trans_at
          |from
          |(
          |select
          |card_accptr_term_id,
          |card_accptr_cd,
          |trans_at,
          |rank() over (partition by card_accptr_term_id,card_accptr_cd order by rec_upd_ts desc )  as rank
          |from hive_acc_trans
          |) tempb
          |where tempb.rank=1
          |) f
          |on e.mchnt_cd = f.card_accptr_cd and e.term_id = f.card_accptr_term_id
          |where f.card_accptr_cd is not null and f.card_accptr_term_id is not null
        """.stripMargin)
      JOB_HV_20_H.registerTempTable("JOB_HV_20_H")
      println("#### 临时表 JOB_HV_20_H 的时间为:" + DateUtils.getCurrentSystemTime())

      val JOB_HV_20_G = sqlContext.sql(
        """
          |select
          |e.mchnt_cd         ,
          |e.term_id          ,
          |e.term_tp          ,
          |e.term_st          ,
          |e.open_dt          ,
          |e.close_dt         ,
          |e.bank_cd          ,
          |e.rec_st           ,
          |e.last_oper_in     ,
          |e.event_id         ,
          |e.rec_id           ,
          |e.rec_upd_usr_id   ,
          |e.rec_upd_ts       ,
          |e.rec_crt_ts       ,
          |e.is_trans_at_tp
          |
          |from
          |
          |(
          |
          |select
          |a.mchnt_cd         ,
          |a.term_id          ,
          |a.term_tp          ,
          |a.term_st          ,
          |a.open_dt          ,
          |a.close_dt         ,
          |a.bank_cd          ,
          |a.rec_st           ,
          |a.last_oper_in     ,
          |a.event_id         ,
          |a.rec_id           ,
          |a.rec_upd_usr_id   ,
          |a.rec_upd_ts       ,
          |a.rec_crt_ts       ,
          |b.is_trans_at_tp
          |from
          |hive_term_inf_copy b
          |left join tbl_chmgm_term_inf a
          |on b.mchnt_cd = a.mchnt_cd and b.term_id = a.term_id
          |where a.mchnt_cd is not null and a.term_id is not null
          |
          |union all
          |
          |select
          |a.mchnt_cd         ,
          |a.term_id          ,
          |a.term_tp          ,
          |a.term_st          ,
          |a.open_dt          ,
          |a.close_dt         ,
          |a.bank_cd          ,
          |a.rec_st           ,
          |a.last_oper_in     ,
          |a.event_id         ,
          |a.rec_id           ,
          |a.rec_upd_usr_id   ,
          |a.rec_upd_ts       ,
          |a.rec_crt_ts       ,
          |'0' as is_trans_at_tp
          |from
          |hive_term_inf_copy b
          |left join tbl_chmgm_term_inf  a
          |on b.mchnt_cd = a.mchnt_cd and b.term_id = a.term_id
          |where a.mchnt_cd is  null and a.term_id is  null
          |) e
          |
          |left join
          |
          |(
          |select
          |tempb.card_accptr_term_id,
          |tempb.card_accptr_cd,
          |tempb.trans_at
          |from
          |(
          |select
          |card_accptr_term_id,
          |card_accptr_cd,
          |trans_at,
          |rank() over (partition by card_accptr_term_id,card_accptr_cd order by rec_upd_ts desc )  as rank
          |from hive_acc_trans
          |) tempb
          |where tempb.rank=1
          |) f
          |on e.mchnt_cd = f.card_accptr_cd and e.term_id = f.card_accptr_term_id
          |where f.card_accptr_cd is null and f.card_accptr_term_id is  null
        """.stripMargin)
      JOB_HV_20_G.registerTempTable("JOB_HV_20_G")
      println("#### 临时表 JOB_HV_20_G 的时间为:" + DateUtils.getCurrentSystemTime())

      val results = sqlContext.sql(
        s"""
           |select
           |t.mchnt_cd          as       mchnt_cd         ,
           |t.term_id           as       term_id          ,
           |t.term_tp           as       term_tp          ,
           |t.term_st           as       term_st          ,
           |case
           |	when
           |		substr(t.open_dt,1,4) between '0001' and '9999' and substr(t.open_dt,5,2) between '01' and '12' and
           |		substr(t.open_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(t.open_dt,1,4),substr(t.open_dt,5,2),substr(t.open_dt,7,2))),9,2)
           |	then concat_ws('-',substr(t.open_dt,1,4),substr(t.open_dt,5,2),substr(t.open_dt,7,2))
           |	else null
           |end as open_dt,
           |case
           |	when
           |		substr(t.close_dt,1,4) between '0001' and '9999' and substr(t.close_dt,5,2) between '01' and '12' and
           |		substr(t.close_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(t.close_dt,1,4),substr(t.close_dt,5,2),substr(t.close_dt,7,2))),9,2)
           |	then concat_ws('-',substr(t.close_dt,1,4),substr(t.close_dt,5,2),substr(t.close_dt,7,2))
           |	else null
           |end as close_dt,
           |t.bank_cd           as       bank_cd          ,
           |t.rec_st            as       rec_st           ,
           |t.last_oper_in      as       last_oper_in     ,
           |t.event_id          as       event_id         ,
           |t.rec_id            as       rec_id           ,
           |t.rec_upd_usr_id    as       rec_upd_usr_id   ,
           |t.rec_upd_ts        as       rec_upd_ts       ,
           |t.rec_crt_ts        as       rec_crt_ts       ,
           |t.is_trans_at_tp    as       is_trans_at_tp
           |from
           |(
           |select * from
           |job_hv_20_h
           |union all
           |select * from
           |job_hv_20_g
           |) t
           | """.stripMargin)

      results.registerTempTable("spark_hive_term_inf")

      println("#### JOB_HV_20 registerTempTable--spark_hive_term_inf完成的时间为:" + DateUtils.getCurrentSystemTime())


      if (!Option(results).isEmpty) {
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql(
          s"""
             |insert overwrite table hive_term_inf select * from spark_hive_term_inf
          """.stripMargin)
        println("#### JOB_HV_20 全量数据插入完成的时间为：" + DateUtils.getCurrentSystemTime())
      } else {
        println("#### JOB_HV_20 spark sql 逻辑处理后无数据！")
      }
    }

  }


  /**
    * JobName:JOB_HV_23
    * Feature:db2.tbl_chmgm_brand_inf->hive.hive_brand_inf
    *
    * @author tzq
    * @time 2016-10-9
    * @param sqlContext
    * @return
    */
  def JOB_HV_23(implicit sqlContext: HiveContext) = {
    println("###JOB_HV_23[全量抽取](hive_brand_inf->tbl_chmgm_brand_inf)")
    DateUtils.timeCost("JOB_HV_23"){
      val df = sqlContext.readDB2_MGM(s"$schemas_mgmdb.TBL_CHMGM_BRAND_INF")
      println("#### JOB_HV_23 readDB2_MGM 的系统时间为:" + DateUtils.getCurrentSystemTime())
      df.registerTempTable("db2_tbl_chmgm_brand_inf")
      println("#### JOB_HV_23 注册临时表 的系统时间为:" + DateUtils.getCurrentSystemTime())
      val results = sqlContext.sql(
        """
          |select
          |brand_id,
          |brand_nm,
          |trim(buss_bmp),
          |cup_branch_ins_id_cd,
          |avg_consume,
          |brand_desc,
          |avg_comment,
          |trim(brand_st),
          |content_id,
          |rec_crt_ts,
          |rec_upd_ts,
          |brand_env_grade,
          |brand_srv_grade,
          |brand_popular_grade,
          |brand_taste_grade,
          |trim(brand_tp),
          |trim(entry_ins_id_cd),
          |entry_ins_cn_nm,
          |trim(rec_crt_usr_id),
          |trim(rec_upd_usr_id)
          |
          |from
          |db2_tbl_chmgm_brand_inf
        """.stripMargin)
      println("#### JOB_HV_23 spark sql 逻辑完成的系统时间为:" + DateUtils.getCurrentSystemTime())
      //println("###JOB_HV_23---------->results:"+results.count())

      if(!Option(results).isEmpty){
        results.registerTempTable("spark_db2_tbl_chmgm_brand_inf")
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql("truncate table  hive_brand_inf")
        sqlContext.sql("insert into table hive_brand_inf select * from spark_db2_tbl_chmgm_brand_inf")
        println("#### JOB_HV_23 全量数据插入完成的系统时间为:" + DateUtils.getCurrentSystemTime())
      }else{
        println("#### JOB_HV_23 spark sql 逻辑处理后无数据！")
      }
    }


  }


  /**
    * JobName: JOB_HV_24
    * Feature: db2.tbl_chmgm_mchnt_para -> hive.hive_mchnt_para
    *
    * @author YangXue
    * @time 2016-09-18
    * @param sqlContext
    */
  def JOB_HV_24(implicit sqlContext: HiveContext) = {

    println("#### JOB_HV_24(tbl_chmgm_mchnt_para -> hive_mchnt_para)")
    println("#### JOB_HV_24 为全量抽取的表")

    DateUtils.timeCost("JOB_HV_24"){
      val df = sqlContext.readDB2_MGM(s"$schemas_mgmdb.TBL_CHMGM_MCHNT_PARA")
      println("#### JOB_HV_24 readDB2_MGM 的系统时间为:"+DateUtils.getCurrentSystemTime())

      df.registerTempTable("spark_db2_mchnt_para")
      println("#### JOB_HV_24 注册临时表的系统时间为:"+DateUtils.getCurrentSystemTime())

      val results = sqlContext.sql(
        """
          |select
          |mchnt_para_id,
          |mchnt_para_cn_nm,
          |mchnt_para_en_nm,
          |trim(mchnt_para_tp),
          |mchnt_para_level,
          |mchnt_para_parent_id,
          |mchnt_para_order,
          |rec_crt_ts,
          |rec_upd_ts,
          |ver_no
          |from
          |spark_db2_mchnt_para
        """.stripMargin
      )
      println("#### JOB_HV_24 spark sql 逻辑完成的系统时间为:"+DateUtils.getCurrentSystemTime())
      //println("#### JOB_HV_24------>results:"+results.count())

      results.registerTempTable("spark_mchnt_para")
      println("#### JOB_HV_24 registerTempTable--spark_mchnt_para 完成的系统时间为:"+DateUtils.getCurrentSystemTime())

      if(!Option(results).isEmpty){
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql("insert overwrite table hive_mchnt_para select * from spark_mchnt_para")
        println("#### JOB_HV_24 全量数据插入完成的时间为："+DateUtils.getCurrentSystemTime())
      }else{
        println("#### JOB_HV_24 spark sql 逻辑处理后无数据！")
      }
    }
  }

  /**
    * JOB_HV_25/10-14
    * HIVE_MCHNT_TP->tbl_mcmgm_mchnt_tp
    * Code by Xue
    *
    * @param sqlContext
    * @return
    */
  def JOB_HV_25 (implicit sqlContext: HiveContext) = {
    DateUtils.timeCost("JOB_HV_25"){
      println("###JOB_HV_25(hive_mchnt_tp->tbl_mcmgm_mchnt_tp)")
      val df2_1 = sqlContext.readDB2_MGM(s"$schemas_mgmdb.tbl_mcmgm_mchnt_tp")
      df2_1.registerTempTable("tbl_mcmgm_mchnt_tp")
      val results = sqlContext.sql(
        """
          |select
          |trim(ta.mchnt_tp) as mchnt_tp,
          |trim(ta.mchnt_tp_grp) as mchnt_tp_grp,
          |ta.mchnt_tp_desc_cn as mchnt_tp_desc_cn,
          |ta.mchnt_tp_desc_en as mchnt_tp_desc_en,
          |ta.rec_id as rec_id,
          |ta.rec_st as rec_st,
          |ta.mcc_type as mcc_type,
          |ta.last_oper_in as last_oper_in,
          |trim(ta.rec_upd_usr_id) as rec_upd_usr_id,
          |ta.rec_upd_ts as rec_upd_ts,
          |ta.rec_crt_ts as rec_crt_ts,
          |ta.sync_st as sync_st,
          |ta.sync_bat_no as sync_bat_no,
          |ta.sync_ts as sync_ts
          |
          |from tbl_mcmgm_mchnt_tp ta
          |
          | """.stripMargin)

      results.registerTempTable("spark_hive_mchnt_tp")
//      println("JOB_HV_25------>results:"+results.count())

      if(!Option(results).isEmpty){
        results.registerTempTable("spark_hive_mchnt_tp")
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql("truncate table  hive_mchnt_tp")
        sqlContext.sql("insert into table hive_mchnt_tp select * from spark_hive_mchnt_tp")
        println("insert into table hive_mchnt_tp successfully!")
      }else{
        println("加载的表spark_hive_mchnt_tp中无数据！")
      }
    }

  }

  /**
    * JobName: hive-job-26
    * Feature: db2.tbl_mcmgm_mchnt_tp_grp->hive.hive_mchnt_tp_grp
  *
  * @author tzq
    * @time 2016-09-18
  * @param sqlContext
    */
  def JOB_HV_26(implicit sqlContext: HiveContext) = {
    println("###JOB_HV_26[全量抽取](hive_mchnt_tp_grp-->tbl_mcmgm_mchnt_tp_grp)")
    DateUtils.timeCost("JOB_HV_26"){
      val df = sqlContext.readDB2_MGM(s"$schemas_mgmdb.TBL_MCMGM_MCHNT_TP_GRP")
      println("#### JOB_HV_26 readDB2_MGM 的系统时间为:" + DateUtils.getCurrentSystemTime())
      df.registerTempTable("db2_tbl_mcmgm_mchnt_tp_grp")
      println("#### JOB_HV_26 注册临时表 的系统时间为:" + DateUtils.getCurrentSystemTime())
      val results = sqlContext.sql(
        """
          |
          |select
          |trim(mchnt_tp_grp),
          |mchnt_tp_grp_desc_cn,
          |mchnt_tp_grp_desc_en,
          |rec_id,
          |rec_st,
          |last_oper_in,
          |trim(rec_upd_usr_id),
          |rec_upd_ts,
          |rec_crt_ts,
          |sync_st,
          |sync_bat_no,
          |sync_ts
          |
          |from
          |db2_tbl_mcmgm_mchnt_tp_grp
        """.stripMargin)
      println("#### JOB_HV_26 spark sql 逻辑完成的系统时间为:" + DateUtils.getCurrentSystemTime())
      //println("###JOB_HV_26---------->results:"+results.count())

      if(!Option(results).isEmpty){
        results.registerTempTable("spark_db2_tbl_mcmgm_mchnt_tp_grp")
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql("truncate table  hive_mchnt_tp_grp")
        sqlContext.sql("insert into table hive_mchnt_tp_grp select * from spark_db2_tbl_mcmgm_mchnt_tp_grp")
        println("#### JOB_HV_26 全量数据插入完成的系统时间为:" + DateUtils.getCurrentSystemTime())
      }else{
        println("#### JOB_HV_26 spark sql 逻辑处理后无数据！")
      }
    }

  }




  /**
    * JOB_HV_28/10-14
    * hive_online_point_trans->viw_chacc_online_point_trans_inf
    * Code by Xue
    *
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    */
  def JOB_HV_28 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {
    println("#### JOB_HV_28(hive_online_point_trans->viw_chacc_online_point_trans_inf)")

    DateUtils.timeCost("JOB_HV_28"){
      val start_day = start_dt.replace("-","")
      val end_day = end_dt.replace("-","")
      println("#### JOB_HV_28 增量抽取的时间范围: "+start_day+"--"+end_day)

      val df = sqlContext.readDB2_ACC_4para(s"$schemas_accdb.viw_chacc_online_point_trans_inf","trans_dt",s"$start_day",s"$end_day")
      df.registerTempTable("viw_chacc_online_point_trans_inf")
      println("#### JOB_HV_28 readDB2_ACC_4para--viw_chacc_online_point_trans_inf 的时间为:"+DateUtils.getCurrentSystemTime())

      val results = sqlContext.sql(
        s"""
           |select
           |ta.trans_id as trans_id,
           |trim(ta.cdhd_usr_id) as cdhd_usr_id,
           |trim(ta.trans_tp) as trans_tp,
           |trim(ta.buss_tp) as buss_tp,
           |ta.trans_point_at as trans_point_at,
           |trim(ta.chara_acct_tp) as chara_acct_tp,
           |trim(ta.bill_id) as bill_id,
           |ta.bill_num as bill_num,
           |case
           |	when
           |		substr(ta.trans_dt,1,4) between '0001' and '9999' and substr(ta.trans_dt,5,2) between '01' and '12' and
           |		substr(ta.trans_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(ta.trans_dt,1,4),substr(ta.trans_dt,5,2),substr(ta.trans_dt,7,2))),9,2)
           |	then concat_ws('-',substr(ta.trans_dt,1,4),substr(ta.trans_dt,5,2),substr(ta.trans_dt,7,2))
           |	else null
           |end as trans_dt,
           |trim(ta.trans_tm) as trans_tm,
           |ta.vendor_id as vendor_id,
           |ta.remark as remark,
           |trim(ta.card_no) as card_no,
           |trim(ta.status) as status,
           |ta.term_trans_seq as term_trans_seq,
           |ta.orig_term_trans_seq as orig_term_trans_seq,
           |trim(ta.mchnt_cd) as mchnt_cd,
           |trim(ta.term_id) as term_id,
           |ta.refund_ts as refund_ts,
           |trim(ta.order_tp) order_tp,
           |ta.trans_at as trans_at,
           |trim(ta.svc_order_id) as svc_order_id,
           |ta.trans_dtl as trans_dtl,
           |ta.exch_rate as exch_rate,
           |ta.disc_at_point as disc_at_point,
           |trim(ta.cdhd_fk) as cdhd_fk,
           |ta.bill_nm as bill_nm,
           |trim(ta.chara_acct_nm) as chara_acct_nm,
           |ta.rec_crt_ts as rec_crt_ts,
           |trim(ta.trans_seq) as trans_seq,
           |trim(ta.sys_err_cd) as sys_err_cd,
           |trim(ta.bill_acq_md) as bill_acq_md,
           |ta.cup_branch_ins_id_cd as cup_branch_ins_id_cd,
           |case
           |	when trim(ta.cup_branch_ins_id_cd)='00011000' then '北京'
           |	when trim(ta.cup_branch_ins_id_cd)='00011100' then '天津'
           |	when trim(ta.cup_branch_ins_id_cd)='00011200' then '河北'
           |	when trim(ta.cup_branch_ins_id_cd)='00011600' then '山西'
           |    when trim(ta.cup_branch_ins_id_cd)='00011900' then '内蒙古'
           |    when trim(ta.cup_branch_ins_id_cd)='00012210' then '辽宁'
           |    when trim(ta.cup_branch_ins_id_cd)='00012220' then '大连'
           |    when trim(ta.cup_branch_ins_id_cd)='00012400' then '吉林'
           |    when trim(ta.cup_branch_ins_id_cd)='00012600' then '黑龙江'
           |    when trim(ta.cup_branch_ins_id_cd)='00012900' then '上海'
           |    when trim(ta.cup_branch_ins_id_cd)='00013000' then '江苏'
           |    when trim(ta.cup_branch_ins_id_cd)='00013310' then '浙江'
           |    when trim(ta.cup_branch_ins_id_cd)='00013320' then '宁波'
           |    when trim(ta.cup_branch_ins_id_cd)='00013600' then '安徽'
           |    when trim(ta.cup_branch_ins_id_cd)='00013900' then '福建'
           |    when trim(ta.cup_branch_ins_id_cd)='00013930' then '厦门'
           |    when trim(ta.cup_branch_ins_id_cd)='00014200' then '江西'
           |    when trim(ta.cup_branch_ins_id_cd)='00014500' then '山东'
           |    when trim(ta.cup_branch_ins_id_cd)='00014520' then '青岛'
           |    when trim(ta.cup_branch_ins_id_cd)='00014900' then '河南'
           |    when trim(ta.cup_branch_ins_id_cd)='00015210' then '湖北'
           |    when trim(ta.cup_branch_ins_id_cd)='00015500' then '湖南'
           |    when trim(ta.cup_branch_ins_id_cd)='00015800' then '广东'
           |    when trim(ta.cup_branch_ins_id_cd)='00015840' then '深圳'
           |    when trim(ta.cup_branch_ins_id_cd)='00016100' then '广西'
           |    when trim(ta.cup_branch_ins_id_cd)='00016400' then '海南'
           |    when trim(ta.cup_branch_ins_id_cd)='00016500' then '四川'
           |    when trim(ta.cup_branch_ins_id_cd)='00016530' then '重庆'
           |    when trim(ta.cup_branch_ins_id_cd)='00017000' then '贵州'
           |    when trim(ta.cup_branch_ins_id_cd)='00017310' then '云南'
           |    when trim(ta.cup_branch_ins_id_cd)='00017700' then '西藏'
           |    when trim(ta.cup_branch_ins_id_cd)='00017900' then '陕西'
           |    when trim(ta.cup_branch_ins_id_cd)='00018200' then '甘肃'
           |    when trim(ta.cup_branch_ins_id_cd)='00018500' then '青海'
           |    when trim(ta.cup_branch_ins_id_cd)='00018700' then '宁夏'
           |    when trim(ta.cup_branch_ins_id_cd)='00018800' then '新疆'
           |else '总公司' end as cup_branch_ins_id_nm,
           |case
           |	when
           |		substr(ta.trans_dt,1,4) between '0001' and '9999' and substr(ta.trans_dt,5,2) between '01' and '12' and
           |		substr(ta.trans_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(ta.trans_dt,1,4),substr(ta.trans_dt,5,2),substr(ta.trans_dt,7,2))),9,2)
           |	then concat_ws('-',substr(ta.trans_dt,1,4),substr(ta.trans_dt,5,2),substr(ta.trans_dt,7,2))
           |	else substr(ta.rec_crt_ts,1,10)
           |end as p_trans_dt
           |from viw_chacc_online_point_trans_inf ta
           |where  ta.trans_tp in ('03','09','11','18','28')
           | """.stripMargin)

      results.registerTempTable("spark_hive_online_point_trans")
      println("#### JOB_HV_28 registerTempTable--spark_hive_online_point_trans完成的时间为:"+DateUtils.getCurrentSystemTime())

      if(!Option(results).isEmpty){
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql(
          """
            |insert overwrite table hive_online_point_trans partition (part_trans_dt)
            |select
            |trans_id,
            |cdhd_usr_id,
            |trans_tp,
            |buss_tp,
            |trans_point_at,
            |chara_acct_tp,
            |bill_id,
            |bill_num,
            |trans_dt,
            |trans_tm,
            |vendor_id,
            |remark,
            |card_no,
            |status,
            |term_trans_seq,
            |orig_term_trans_seq,
            |mchnt_cd,
            |term_id,
            |refund_ts,
            |order_tp,
            |trans_at,
            |svc_order_id,
            |trans_dtl,
            |exch_rate,
            |disc_at_point,
            |cdhd_fk,
            |bill_nm,
            |chara_acct_nm,
            |rec_crt_ts,
            |trans_seq,
            |sys_err_cd,
            |bill_acq_md,
            |cup_branch_ins_id_cd,
            |cup_branch_ins_id_nm,
            |p_trans_dt
            |from spark_hive_online_point_trans
          """.stripMargin)
        println("#### JOB_HV_28 动态分区插入完成的时间为："+DateUtils.getCurrentSystemTime())

      }else{
        println("#### JOB_HV_28 spark sql 逻辑处理后无数据！")
      }
    }

  }

  /**
    * JOB_HV_29/10-14
    * hive_offline_point_trans->tbl_chacc_cdhd_point_addup_dtl
    * Code by Xue
    *
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    */
  def JOB_HV_29 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {
    println("#### JOB_HV_29(hive_offline_point_trans->tbl_chacc_cdhd_point_addup_dtl)")

    DateUtils.timeCost("JOB_HV_29") {
      val start_day = start_dt.replace("-", "")
      val end_day = end_dt.replace("-", "")
      println("#### JOB_HV_29增量抽取的时间范围: " + start_day + "--" + end_day)

      val df = sqlContext.readDB2_ACC_4para(s"$schemas_accdb.tbl_chacc_cdhd_point_addup_dtl", "trans_dt", s"$start_day", s"$end_day")
      println("#### JOB_HV_29 readDB2_ACC_4para 的系统时间为:" + DateUtils.getCurrentSystemTime())
      df.registerTempTable("tbl_chacc_cdhd_point_addup_dtl")
      println("#### JOB_HV_29 注册临时表的系统时间为:" + DateUtils.getCurrentSystemTime())

      val results = sqlContext.sql(
        s"""
           |select
           |ta.dtl_seq as dtl_seq,
           |trim(ta.cdhd_usr_id) as cdhd_usr_id,
           |trim(ta.pri_acct_no) as pri_acct_no,
           |trim(ta.acpt_ins_id_cd) as acpt_ins_id_cd,
           |trim(ta.fwd_ins_id_cd) as fwd_ins_id_cd,
           |trim(ta.sys_tra_no) as sys_tra_no,
           |trim(ta.tfr_dt_tm) as tfr_dt_tm,
           |trim(ta.card_class) as card_class,
           |trim(ta.card_attr) as card_attr,
           |trim(ta.card_std) as card_std,
           |trim(ta.card_media) as card_media,
           |trim(ta.cups_card_in) as cups_card_in,
           |trim(ta.cups_sig_card_in) as cups_sig_card_in,
           |trim(ta.trans_id) as trans_id,
           |trim(ta.region_cd) as region_cd,
           |trim(ta.card_bin) as card_bin,
           |trim(ta.mchnt_tp) as mchnt_tp,
           |trim(ta.mchnt_cd) as mchnt_cd,
           |trim(ta.term_id) as term_id,
           |case
           |	when
           |		substr(ta.trans_dt,1,4) between '0001' and '9999' and substr(ta.trans_dt,5,2) between '01' and '12' and
           |		substr(ta.trans_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(ta.trans_dt,1,4),substr(ta.trans_dt,5,2),substr(ta.trans_dt,7,2))),9,2)
           |	then concat_ws('-',substr(ta.trans_dt,1,4),substr(ta.trans_dt,5,2),substr(ta.trans_dt,7,2))
           |	else null
           |end as trans_dt,
           |trim(ta.trans_tm) as trans_tm,
           |case
           |	when
           |		substr(ta.settle_dt,1,4) between '0001' and '9999' and substr(ta.settle_dt,5,2) between '01' and '12' and
           |		substr(ta.settle_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(ta.settle_dt,1,4),substr(ta.settle_dt,5,2),substr(ta.settle_dt,7,2))),9,2)
           |	then concat_ws('-',substr(ta.settle_dt,1,4),substr(ta.settle_dt,5,2),substr(ta.settle_dt,7,2))
           |	else null
           |end as settle_dt,
           |ta.settle_at as settle_at,
           |trim(ta.trans_chnl) as trans_chnl,
           |trim(ta.acpt_term_tp) as acpt_term_tp,
           |ta.point_plan_id as point_plan_id,
           |ta.plan_id as plan_id,
           |trim(ta.ins_acct_id) as ins_acct_id,
           |ta.point_at as point_at,
           |trim(ta.oper_st) as oper_st,
           |ta.rule_id as rule_id,
           |trim(ta.pri_key) as pri_key,
           |ta.ver_no as ver_no,
           |case
           |	when
           |		substr(ta.acct_addup_bat_dt,1,4) between '0001' and '9999' and substr(ta.acct_addup_bat_dt,5,2) between '01' and '12' and
           |		substr(ta.acct_addup_bat_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(ta.acct_addup_bat_dt,1,4),substr(ta.acct_addup_bat_dt,5,2),substr(ta.acct_addup_bat_dt,7,2))),9,2)
           |	then concat_ws('-',substr(ta.acct_addup_bat_dt,1,4),substr(ta.acct_addup_bat_dt,5,2),substr(ta.acct_addup_bat_dt,7,2))
           |	else null
           |end as acct_addup_bat_dt,
           |trim(ta.iss_ins_id_cd) as iss_ins_id_cd,
           |trim(ta.extra_sp_ins_acct_id) as extra_sp_ins_acct_id,
           |ta.extra_point_at as extra_point_at,
           |trim(ta.extend_ins_id_cd) as extend_ins_id_cd,
           |ta.cup_branch_ins_id_cd as cup_branch_ins_id_cd,
           |case
           |	when trim(ta.cup_branch_ins_id_cd)='00011000' then '北京'
           |	when trim(ta.cup_branch_ins_id_cd)='00011100' then '天津'
           |	when trim(ta.cup_branch_ins_id_cd)='00011200' then '河北'
           |	when trim(ta.cup_branch_ins_id_cd)='00011600' then '山西'
           |	when trim(ta.cup_branch_ins_id_cd)='00011900' then '内蒙古'
           |	when trim(ta.cup_branch_ins_id_cd)='00012210' then '辽宁'
           |	when trim(ta.cup_branch_ins_id_cd)='00012220' then '大连'
           |	when trim(ta.cup_branch_ins_id_cd)='00012400' then '吉林'
           |	when trim(ta.cup_branch_ins_id_cd)='00012600' then '黑龙江'
           |	when trim(ta.cup_branch_ins_id_cd)='00012900' then '上海'
           |	when trim(ta.cup_branch_ins_id_cd)='00013000' then '江苏'
           |	when trim(ta.cup_branch_ins_id_cd)='00013310' then '浙江'
           |	when trim(ta.cup_branch_ins_id_cd)='00013320' then '宁波'
           |	when trim(ta.cup_branch_ins_id_cd)='00013600' then '安徽'
           |	when trim(ta.cup_branch_ins_id_cd)='00013900' then '福建'
           |	when trim(ta.cup_branch_ins_id_cd)='00013930' then '厦门'
           |	when trim(ta.cup_branch_ins_id_cd)='00014200' then '江西'
           |	when trim(ta.cup_branch_ins_id_cd)='00014500' then '山东'
           |	when trim(ta.cup_branch_ins_id_cd)='00014520' then '青岛'
           |	when trim(ta.cup_branch_ins_id_cd)='00014900' then '河南'
           |	when trim(ta.cup_branch_ins_id_cd)='00015210' then '湖北'
           |	when trim(ta.cup_branch_ins_id_cd)='00015500' then '湖南'
           |	when trim(ta.cup_branch_ins_id_cd)='00015800' then '广东'
           |	when trim(ta.cup_branch_ins_id_cd)='00015840' then '深圳'
           |	when trim(ta.cup_branch_ins_id_cd)='00016100' then '广西'
           |	when trim(ta.cup_branch_ins_id_cd)='00016400' then '海南'
           |	when trim(ta.cup_branch_ins_id_cd)='00016500' then '四川'
           |	when trim(ta.cup_branch_ins_id_cd)='00016530' then '重庆'
           |	when trim(ta.cup_branch_ins_id_cd)='00017000' then '贵州'
           |	when trim(ta.cup_branch_ins_id_cd)='00017310' then '云南'
           |	when trim(ta.cup_branch_ins_id_cd)='00017700' then '西藏'
           |	when trim(ta.cup_branch_ins_id_cd)='00017900' then '陕西'
           |	when trim(ta.cup_branch_ins_id_cd)='00018200' then '甘肃'
           |	when trim(ta.cup_branch_ins_id_cd)='00018500' then '青海'
           |	when trim(ta.cup_branch_ins_id_cd)='00018700' then '宁夏'
           |	when trim(ta.cup_branch_ins_id_cd)='00018800' then '新疆'
           |else '总公司' end as cup_branch_ins_id_nm,
           |trim(ta.um_trans_id) as um_trans_id,
           |trim(ta.buss_tp) as buss_tp,
           |trim(ta.bill_id) as bill_id,
           |ta.bill_num as bill_num,
           |case
           |	when
           |		substr(ta.oper_dt,1,4) between '0001' and '9999' and substr(ta.oper_dt,5,2) between '01' and '12' and
           |		substr(ta.oper_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(ta.oper_dt,1,4),substr(ta.oper_dt,5,2),substr(ta.oper_dt,7,2))),9,2)
           |	then concat_ws('-',substr(ta.oper_dt,1,4),substr(ta.oper_dt,5,2),substr(ta.oper_dt,7,2))
           |	else null
           |end as oper_dt,
           |trim(ta.tmp_flag) as tmp_flag,
           |ta.bill_nm as bill_nm,
           |trim(ta.chara_acct_tp) as chara_acct_tp,
           |trim(ta.chara_acct_nm) as chara_acct_nm,
           |trim(ta.acct_addup_tp) as acct_addup_tp,
           |ta.rec_crt_ts as rec_crt_ts,
           |ta.rec_upd_ts as rec_upd_ts,
           |trim(ta.orig_trans_seq) as orig_trans_seq,
           |trim(ta.notice_on_account) as notice_on_account,
           |trim(ta.orig_tfr_dt_tm) as orig_tfr_dt_tm,
           |trim(ta.orig_sys_tra_no) as orig_sys_tra_no,
           |trim(ta.orig_acpt_ins_id_cd) as orig_acpt_ins_id_cd,
           |trim(ta.orig_fwd_ins_id_cd) as orig_fwd_ins_id_cd,
           |trim(ta.indirect_trans_in) as indirect_trans_in,
           |ta.booking_rec_id as booking_rec_id,
           |trim(ta.booking_in) as booking_in,
           |ta.plan_nm as plan_nm,
           |ta.plan_give_total_num as plan_give_total_num,
           |trim(ta.plan_give_limit_tp) as plan_give_limit_tp,
           |ta.plan_give_limit as plan_give_limit,
           |ta.day_give_limit as day_give_limit,
           |trim(ta.give_limit_in) as give_limit_in,
           |trim(ta.retri_ref_no) as retri_ref_no,
           |case
           |	when
           |		substr(ta.trans_dt,1,4) between '0001' and '9999' and substr(ta.trans_dt,5,2) between '01' and '12' and
           |		substr(ta.trans_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(ta.trans_dt,1,4),substr(ta.trans_dt,5,2),substr(ta.trans_dt,7,2))),9,2)
           |	then concat_ws('-',substr(ta.trans_dt,1,4),substr(ta.trans_dt,5,2),substr(ta.trans_dt,7,2))
           |	else substr(ta.rec_crt_ts,1,10)
           |end as p_trans_dt
           |
           |from tbl_chacc_cdhd_point_addup_dtl ta
           |where ta.um_trans_id in('AD00000002','AD00000003','AD00000004','AD00000005','AD00000006','AD00000007')
           | """.stripMargin)
      println("#### JOB_HV_29 spark sql 逻辑完成的系统时间为:" + DateUtils.getCurrentSystemTime())

      results.registerTempTable("spark_hive_offline_point_trans")
      println("#### JOB_HV_29 registerTempTable--spark_hive_offline_point_trans 完成的系统时间为:" + DateUtils.getCurrentSystemTime())

      if (!Option(results).isEmpty) {
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql(
          s"""
             |insert overwrite table hive_offline_point_trans partition (part_trans_dt)
             |select
             |dtl_seq                     ,
             |cdhd_usr_id                 ,
             |pri_acct_no                 ,
             |acpt_ins_id_cd              ,
             |fwd_ins_id_cd               ,
             |sys_tra_no                  ,
             |tfr_dt_tm                   ,
             |card_class                  ,
             |card_attr                   ,
             |card_std                    ,
             |card_media                  ,
             |cups_card_in                ,
             |cups_sig_card_in            ,
             |trans_id                    ,
             |region_cd                   ,
             |card_bin                    ,
             |mchnt_tp                    ,
             |mchnt_cd                    ,
             |term_id                     ,
             |trans_dt                    ,
             |trans_tm                    ,
             |settle_dt                   ,
             |settle_at                   ,
             |trans_chnl                  ,
             |acpt_term_tp                ,
             |point_plan_id               ,
             |plan_id                     ,
             |ins_acct_id                 ,
             |point_at                    ,
             |oper_st                     ,
             |rule_id                     ,
             |pri_key                     ,
             |ver_no                      ,
             |acct_addup_bat_dt           ,
             |iss_ins_id_cd               ,
             |extra_sp_ins_acct_id        ,
             |extra_point_at              ,
             |extend_ins_id_cd            ,
             |cup_branch_ins_id_cd        ,
             |cup_branch_ins_id_nm        ,
             |um_trans_id                 ,
             |buss_tp                     ,
             |bill_id                     ,
             |bill_num                    ,
             |oper_dt                     ,
             |tmp_flag                    ,
             |bill_nm                     ,
             |chara_acct_tp               ,
             |chara_acct_nm               ,
             |acct_addup_tp               ,
             |rec_crt_ts                  ,
             |rec_upd_ts                  ,
             |orig_trans_seq              ,
             |notice_on_account           ,
             |orig_tfr_dt_tm              ,
             |orig_sys_tra_no             ,
             |orig_acpt_ins_id_cd         ,
             |orig_fwd_ins_id_cd          ,
             |indirect_trans_in           ,
             |booking_rec_id              ,
             |booking_in                  ,
             |plan_nm                     ,
             |plan_give_total_num         ,
             |plan_give_limit_tp          ,
             |plan_give_limit             ,
             |day_give_limit              ,
             |give_limit_in               ,
             |retri_ref_no                ,
             |p_trans_dt
             |from
             |spark_hive_offline_point_trans
         """.stripMargin)
        println("#### JOB_HV_29 动态分区插入完成的时间为："+DateUtils.getCurrentSystemTime())
      } else {
        println("#### JOB_HV_29 spark sql 逻辑处理后无数据！")
      }
    }
  }


  /**
    * JobName: JOB_HV_30
    * Feature: db2.viw_chacc_code_pay_tran_dtl -> hive.hive_passive_code_pay_trans
    *
    * @author YangXue
    * @time 2016-08-22
    * @param sqlContext,start_dt,end_dt
    */
  def JOB_HV_30(implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {

    println("#### JOB_HV_30(viw_chacc_code_pay_tran_dtl -> hive_passive_code_pay_trans)")

    val start_day = start_dt.replace("-","")
    val end_day = end_dt.replace("-","")
    println("#### JOB_HV_30 增量抽取的时间范围: "+start_day+"--"+end_day)

    DateUtils.timeCost("JOB_HV_30"){
      val df = sqlContext.readDB2_ACC_4para(s"$schemas_accdb.VIW_CHACC_CODE_PAY_TRAN_DTL","trans_dt",s"$start_day",s"$end_day")
      println("#### JOB_HV_30 readDB2_ACC_4para 的系统时间为:"+DateUtils.getCurrentSystemTime())

      df.registerTempTable("spark_db2_code_pay_tran_dtl")
      println("#### JOB_HV_30 注册临时表的系统时间为:"+DateUtils.getCurrentSystemTime())

      val results = sqlContext.sql(
        s"""
           |select
           |trim(trans_seq) as trans_seq,
           |cdhd_card_bind_seq,
           |trim(cdhd_usr_id) as cdhd_usr_id,
           |trim(card_no) as card_no,
           |trim(tran_mode) as tran_mode,
           |trim(trans_tp) as trans_tp,
           |tran_certi,
           |trim(trans_rdm_num) as trans_rdm_num,
           |trans_expire_ts,
           |order_id,
           |device_cd,
           |trim(mchnt_cd) as mchnt_cd,
           |mchnt_nm,
           |trim(sub_mchnt_cd) as sub_mchnt_cd,
           |sub_mchnt_nm,
           |trim(settle_dt) as settle_dt,
           |trans_at,
           |discount_at,
           |discount_info,
           |refund_at,
           |trim(orig_trans_seq) as orig_trans_seq,
           |trim(trans_st) trans_st,
           |rec_crt_ts,
           |rec_upd_ts,
           |case
           |when
           |	substr(trans_dt,1,4) between '0001' and '9999' and substr(trans_dt,5,2) between '01' and '12' and
           |	substr(trans_dt,7,2) between '01' and last_day(concat_ws('-',substr(trans_dt,1,4),substr(trans_dt,5,2),substr(trans_dt,7,2)))
           |then concat_ws('-',substr(trans_dt,1,4),substr(trans_dt,5,2),substr(trans_dt,7,2))
           |else null
           |end as trans_dt,
           |trim(resp_code) as resp_code,
           |resp_msg,
           |trim(out_trade_no) as out_trade_no,
           |trim(body) as body,
           |trim(terminal_id) as terminal_id,
           |extend_params,
           |case
           |when
           |	substr(trans_dt,1,4) between '0001' and '9999' and substr(trans_dt,5,2) between '01' and '12' and
           |	substr(trans_dt,7,2) between '01' and last_day(concat_ws('-',substr(trans_dt,1,4),substr(trans_dt,5,2),substr(trans_dt,7,2)))
           |then concat_ws('-',substr(trans_dt,1,4),substr(trans_dt,5,2),substr(trans_dt,7,2))
           |else substr(rec_crt_ts,1,10)
           |end as p_trans_dt
           |from
           |spark_db2_code_pay_tran_dtl
         """.stripMargin
      )
      println("#### JOB_HV_30 spark sql 逻辑完成的系统时间为:"+DateUtils.getCurrentSystemTime())
      //println("#### JOB_HV_30------>results:"+results.count())

      results.registerTempTable("spark_code_pay_tran_dtl")
      println("#### JOB_HV_30 registerTempTable--spark_code_pay_tran_dtl 完成的系统时间为:"+DateUtils.getCurrentSystemTime())

      if(!Option(results).isEmpty){
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql(
          s"""
             |insert overwrite table hive_passive_code_pay_trans partition (part_trans_dt)
             |select
             |trans_seq,
             |cdhd_card_bind_seq,
             |cdhd_usr_id,
             |card_no,
             |tran_mode,
             |trans_tp,
             |tran_certi,
             |trans_rdm_num,
             |trans_expire_ts,
             |order_id,
             |device_cd,
             |mchnt_cd,
             |mchnt_nm,
             |sub_mchnt_cd,
             |sub_mchnt_nm,
             |settle_dt,
             |trans_at,
             |discount_at,
             |discount_info,
             |refund_at,
             |orig_trans_seq,
             |trim(trans_st) trans_st,
             |rec_crt_ts,
             |rec_upd_ts,
             |trans_dt,
             |resp_code,
             |resp_msg,
             |out_trade_no,
             |body,
             |terminal_id,
             |extend_params,
             |p_trans_dt
             |from
             |spark_code_pay_tran_dtl
           """.stripMargin)
        println("#### JOB_HV_30 动态分区插入完成的时间为："+DateUtils.getCurrentSystemTime())
      }else{
        println("#### JOB_HV_30 spark sql 逻辑处理后无数据！")
      }
    }
  }


  /**
    * JOB_HV_31/03-06
    * hive_bill_order_trans->viw_chmgm_bill_order_aux_inf
    * Code by Xue
    *
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    */
  def JOB_HV_31 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {
    println("#### JOB_HV_31(hive_bill_order_trans->viw_chmgm_bill_order_aux_inf)")

    DateUtils.timeCost("JOB_HV_31"){
      val start_day = start_dt.replace("-","")
      val end_day = end_dt.replace("-","")
      println("#### JOB_HV_31 增量抽取的时间范围: " + start_day + "--" + end_day)

      val df2_1 = sqlContext.readDB2_MGM_4para(s"$schemas_mgmdb.viw_chmgm_bill_order_aux_inf","trans_dt",s"$start_day",s"$end_day")
      println("#### JOB_HV_31 readDB2_MGM_4para 的系统时间为:" + DateUtils.getCurrentSystemTime())

      df2_1.registerTempTable("viw_chmgm_bill_order_aux_inf")
      println("#### JOB_HV_31 注册临时表的系统时间为:" + DateUtils.getCurrentSystemTime())

      val results = sqlContext.sql(
        s"""
           |select
           |trim(ta.bill_order_id) as bill_order_id,
           |trim(ta.mchnt_cd) as mchnt_cd,
           |ta.mchnt_nm as mchnt_nm,
           |trim(ta.sub_mchnt_cd) as sub_mchnt_cd,
           |trim(ta.cdhd_usr_id) as cdhd_usr_id,
           |ta.sub_mchnt_nm as sub_mchnt_nm,
           |ta.related_usr_id as related_usr_id,
           |trim(ta.cups_trace_number) as cups_trace_number,
           |trim(ta.trans_tm) as trans_tm,
           |case
           |	when
           |		substr(ta.trans_dt,1,4) between '0001' and '9999' and substr(ta.trans_dt,5,2) between '01' and '12' and
           |		substr(ta.trans_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(ta.trans_dt,1,4),substr(ta.trans_dt,5,2),substr(ta.trans_dt,7,2))),9,2)
           |	then concat_ws('-',substr(ta.trans_dt,1,4),substr(ta.trans_dt,5,2),substr(ta.trans_dt,7,2))
           |	else null
           |end as trans_dt,
           |trim(ta.orig_trans_seq) as orig_trans_seq,
           |trim(ta.trans_seq) as trans_seq,
           |trim(ta.mobile_order_id) as mobile_order_id,
           |trim(ta.acp_order_id) as acp_order_id,
           |trim(ta.delivery_prov_cd) as delivery_prov_cd,
           |trim(ta.delivery_city_cd) as delivery_city_cd,
           |(case
           |when ta.delivery_city_cd='210200' then '大连'
           |when ta.delivery_city_cd='330200' then '宁波'
           |when ta.delivery_city_cd='350200' then '厦门'
           |when ta.delivery_city_cd='370200' then '青岛'
           |when ta.delivery_city_cd='440300' then '深圳'
           |when ta.delivery_prov_cd like '11%' then '北京'
           |when ta.delivery_prov_cd like '12%' then '天津'
           |when ta.delivery_prov_cd like '13%' then '河北'
           |when ta.delivery_prov_cd like '14%' then '山西'
           |when ta.delivery_prov_cd like '15%' then '内蒙古'
           |when ta.delivery_prov_cd like '21%' then '辽宁'
           |when ta.delivery_prov_cd like '22%' then '吉林'
           |when ta.delivery_prov_cd like '23%' then '黑龙江'
           |when ta.delivery_prov_cd like '31%' then '上海'
           |when ta.delivery_prov_cd like '32%' then '江苏'
           |when ta.delivery_prov_cd like '33%' then '浙江'
           |when ta.delivery_prov_cd like '34%' then '安徽'
           |when ta.delivery_prov_cd like '35%' then '福建'
           |when ta.delivery_prov_cd like '36%' then '江西'
           |when ta.delivery_prov_cd like '37%' then '山东'
           |when ta.delivery_prov_cd like '41%' then '河南'
           |when ta.delivery_prov_cd like '42%' then '湖北'
           |when ta.delivery_prov_cd like '43%' then '湖南'
           |when ta.delivery_prov_cd like '44%' then '广东'
           |when ta.delivery_prov_cd like '45%' then '广西'
           |when ta.delivery_prov_cd like '46%' then '海南'
           |when ta.delivery_prov_cd like '50%' then '重庆'
           |when ta.delivery_prov_cd like '51%' then '四川'
           |when ta.delivery_prov_cd like '52%' then '贵州'
           |when ta.delivery_prov_cd like '53%' then '云南'
           |when ta.delivery_prov_cd like '54%' then '西藏'
           |when ta.delivery_prov_cd like '61%' then '陕西'
           |when ta.delivery_prov_cd like '62%' then '甘肃'
           |when ta.delivery_prov_cd like '63%' then '青海'
           |when ta.delivery_prov_cd like '64%' then '宁夏'
           |when ta.delivery_prov_cd like '65%' then '新疆'
           |else '总公司' end) as delivery_district_nm,
           |trim(ta.delivery_district_cd) as delivery_district_cd,
           |trim(ta.delivery_zip_cd) as delivery_zip_cd,
           |ta.delivery_address as delivery_address,
           |trim(ta.receiver_nm) as receiver_nm,
           |trim(ta.receiver_mobile) as receiver_mobile,
           |ta.delivery_time_desc as delivery_time_desc,
           |ta.invoice_desc as invoice_desc,
           |ta.trans_at as trans_at,
           |ta.refund_at as refund_at,
           |trim(ta.order_st) as order_st,
           |ta.order_crt_ts as order_crt_ts,
           |ta.order_timeout_ts as order_timeout_ts,
           |trim(ta.card_no) as card_no,
           |trim(ta.order_chnl) as order_chnl,
           |trim(ta.order_ip) as order_ip,
           |ta.device_inf as device_inf,
           |ta.remark as remark,
           |ta.rec_crt_ts as rec_crt_ts,
           |trim(ta.crt_cdhd_usr_id) as crt_cdhd_usr_id,
           |ta.rec_upd_ts as rec_upd_ts,
           |trim(ta.upd_cdhd_usr_id) as upd_cdhd_usr_id,
           |case
           |	when
           |		substr(ta.trans_dt,1,4) between '0001' and '9999' and substr(ta.trans_dt,5,2) between '01' and '12' and
           |		substr(ta.trans_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(ta.trans_dt,1,4),substr(ta.trans_dt,5,2),substr(ta.trans_dt,7,2))),9,2)
           |	then concat_ws('-',substr(ta.trans_dt,1,4),substr(ta.trans_dt,5,2),substr(ta.trans_dt,7,2))
           |	else substr(ta.rec_crt_ts,1,10)
           |end as p_trans_dt
           |from viw_chmgm_bill_order_aux_inf ta
           | """.stripMargin)

      println("#### JOB_HV_31 spark sql 逻辑完成的系统时间为:" + DateUtils.getCurrentSystemTime())
      results.registerTempTable("spark_hive_bill_order_trans")
      println("#### JOB_HV_31 registerTempTable-- spark_hive_bill_order_trans 完成的系统时间为:" + DateUtils.getCurrentSystemTime())

      if (!Option(results).isEmpty) {
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql(
          s"""
             |insert overwrite table hive_bill_order_trans partition (part_trans_dt)
             |select
             |bill_order_id            ,
             |mchnt_cd                 ,
             |mchnt_nm                 ,
             |sub_mchnt_cd             ,
             |cdhd_usr_id              ,
             |sub_mchnt_nm             ,
             |related_usr_id           ,
             |cups_trace_number        ,
             |trans_tm                 ,
             |trans_dt                 ,
             |orig_trans_seq           ,
             |trans_seq                ,
             |mobile_order_id          ,
             |acp_order_id             ,
             |delivery_prov_cd         ,
             |delivery_city_cd         ,
             |delivery_district_nm     ,
             |delivery_district_cd     ,
             |delivery_zip_cd          ,
             |delivery_address         ,
             |receiver_nm              ,
             |receiver_mobile          ,
             |delivery_time_desc       ,
             |invoice_desc             ,
             |trans_at                 ,
             |refund_at                ,
             |order_st                 ,
             |order_crt_ts             ,
             |order_timeout_ts         ,
             |card_no                  ,
             |order_chnl               ,
             |order_ip                 ,
             |device_inf               ,
             |remark                   ,
             |rec_crt_ts               ,
             |crt_cdhd_usr_id          ,
             |rec_upd_ts               ,
             |upd_cdhd_usr_id          ,
             |p_trans_dt
             |from
             |spark_hive_bill_order_trans
         """.stripMargin)
        println("#### JOB_HV_31 动态分区插入完成的时间为：" + DateUtils.getCurrentSystemTime())
      } else {
        println("#### JOB_HV_31 spark sql 逻辑处理后无数据！")
      }

    }


  }

  /**
    * JOB_HV_32/10-14
    * hive_prize_discount_result->tbl_umsvc_prize_discount_result
    * Code by Xue
    *
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    * @return
    */
  def JOB_HV_32 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {
    println("#### JOB_HV_32(hive_prize_discount_result->tbl_umsvc_prize_discount_result)")

    DateUtils.timeCost("JOB_HV_32") {
      val start_day = start_dt.replace("-", "")
      val end_day = end_dt.replace("-", "")
      println("#### JOB_HV_32 增量抽取的时间范围: " + start_day + "--" + end_day)

      val df = sqlContext.readDB2_MGM_4para(s"$schemas_mgmdb.tbl_umsvc_prize_discount_result", "settle_dt", s"$start_day", s"$end_day")
      println("#### JOB_HV_32 readDB2_MGM_4para 的系统时间为:" + DateUtils.getCurrentSystemTime())

      df.registerTempTable("tbl_umsvc_prize_discount_result")
      println("#### JOB_HV_32 注册临时表的系统时间为:" + DateUtils.getCurrentSystemTime())

      val results = sqlContext.sql(
        s"""
           |select
           |ta.prize_result_seq as prize_result_seq,
           |trim(ta.trans_id) as trans_id,
           |trim(ta.sys_tra_no) as sys_tra_no,
           |trim(ta.sys_tra_no_conv) as sys_tra_no_conv,
           |trim(ta.pri_acct_no) as pri_acct_no,
           |ta.trans_at as trans_at,
           |ta.trans_at_conv as trans_at_conv,
           |ta.trans_pos_at as trans_pos_at,
           |trim(ta.trans_dt_tm) as trans_dt_tm,
           |trim(ta.loc_trans_dt) as loc_trans_dt,
           |trim(ta.loc_trans_tm) as loc_trans_tm,
           |case
           |	when
           |		substr(ta.settle_dt,1,4) between '0001' and '9999' and substr(ta.settle_dt,5,2) between '01' and '12' and
           |		substr(ta.settle_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(ta.settle_dt,1,4),substr(ta.settle_dt,5,2),substr(ta.settle_dt,7,2))),9,2)
           |	then concat_ws('-',substr(ta.settle_dt,1,4),substr(ta.settle_dt,5,2),substr(ta.settle_dt,7,2))
           |	else null
           |end as settle_dt,
           |trim(mchnt_tp) as mchnt_tp,
           |trim(acpt_ins_id_cd) as acpt_ins_id_cd,
           |trim(iss_ins_id_cd) as iss_ins_id_cd,
           |trim(acq_ins_id_cd) as acq_ins_id_cd,
           |cup_branch_ins_id_cd as cup_branch_ins_id_cd,
           |case
           |	when trim(ta.cup_branch_ins_id_cd)='00011000' then '北京'
           |	when trim(ta.cup_branch_ins_id_cd)='00011100' then '天津'
           |	when trim(ta.cup_branch_ins_id_cd)='00011200' then '河北'
           |	when trim(ta.cup_branch_ins_id_cd)='00011600' then '山西'
           |    when trim(ta.cup_branch_ins_id_cd)='00011900' then '内蒙古'
           |    when trim(ta.cup_branch_ins_id_cd)='00012210' then '辽宁'
           |    when trim(ta.cup_branch_ins_id_cd)='00012220' then '大连'
           |    when trim(ta.cup_branch_ins_id_cd)='00012400' then '吉林'
           |    when trim(ta.cup_branch_ins_id_cd)='00012600' then '黑龙江'
           |    when trim(ta.cup_branch_ins_id_cd)='00012900' then '上海'
           |    when trim(ta.cup_branch_ins_id_cd)='00013000' then '江苏'
           |    when trim(ta.cup_branch_ins_id_cd)='00013310' then '浙江'
           |    when trim(ta.cup_branch_ins_id_cd)='00013320' then '宁波'
           |    when trim(ta.cup_branch_ins_id_cd)='00013600' then '安徽'
           |    when trim(ta.cup_branch_ins_id_cd)='00013900' then '福建'
           |    when trim(ta.cup_branch_ins_id_cd)='00013930' then '厦门'
           |    when trim(ta.cup_branch_ins_id_cd)='00014200' then '江西'
           |    when trim(ta.cup_branch_ins_id_cd)='00014500' then '山东'
           |    when trim(ta.cup_branch_ins_id_cd)='00014520' then '青岛'
           |    when trim(ta.cup_branch_ins_id_cd)='00014900' then '河南'
           |    when trim(ta.cup_branch_ins_id_cd)='00015210' then '湖北'
           |    when trim(ta.cup_branch_ins_id_cd)='00015500' then '湖南'
           |    when trim(ta.cup_branch_ins_id_cd)='00015800' then '广东'
           |    when trim(ta.cup_branch_ins_id_cd)='00015840' then '深圳'
           |    when trim(ta.cup_branch_ins_id_cd)='00016100' then '广西'
           |    when trim(ta.cup_branch_ins_id_cd)='00016400' then '海南'
           |    when trim(ta.cup_branch_ins_id_cd)='00016500' then '四川'
           |    when trim(ta.cup_branch_ins_id_cd)='00016530' then '重庆'
           |    when trim(ta.cup_branch_ins_id_cd)='00017000' then '贵州'
           |    when trim(ta.cup_branch_ins_id_cd)='00017310' then '云南'
           |    when trim(ta.cup_branch_ins_id_cd)='00017700' then '西藏'
           |    when trim(ta.cup_branch_ins_id_cd)='00017900' then '陕西'
           |    when trim(ta.cup_branch_ins_id_cd)='00018200' then '甘肃'
           |    when trim(ta.cup_branch_ins_id_cd)='00018500' then '青海'
           |    when trim(ta.cup_branch_ins_id_cd)='00018700' then '宁夏'
           |    when trim(ta.cup_branch_ins_id_cd)='00018800' then '新疆'
           |else '总公司' end as cup_branch_ins_id_nm,
           |trim(ta.mchnt_cd) as mchnt_cd,
           |trim(ta.term_id) as term_id,
           |trim(ta.trans_curr_cd) as trans_curr_cd,
           |trim(ta.trans_chnl) as trans_chnl,
           |trim(ta.prod_in) as prod_in,
           |ta.agio_app_id as agio_app_id,
           |trim(ta.agio_inf) as agio_inf,
           |ta.prize_app_id as prize_app_id,
           |ta.prize_id as prize_id,
           |ta.prize_lvl as prize_lvl,
           |case
           |	when
           |		substr(ta.rec_crt_dt,1,4) between '0001' and '9999' and substr(ta.rec_crt_dt,5,2) between '01' and '12' and
           |		substr(ta.rec_crt_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(ta.rec_crt_dt,1,4),substr(ta.rec_crt_dt,5,2),substr(ta.rec_crt_dt,7,2))),9,2)
           |	then concat_ws('-',substr(ta.rec_crt_dt,1,4),substr(ta.rec_crt_dt,5,2),substr(ta.rec_crt_dt,7,2))
           |	else null
           |end as rec_crt_dt,
           |trim(ta.is_match_in) as is_match_in,
           |trim(ta.fwd_ins_id_cd) as fwd_ins_id_cd,
           |trim(ta.orig_trans_tfr_tm) as orig_trans_tfr_tm,
           |trim(ta.orig_sys_tra_no) as orig_sys_tra_no,
           |trim(ta.orig_acpt_ins_id_cd) as orig_acpt_ins_id_cd,
           |trim(ta.orig_fwd_ins_id_cd) as orig_fwd_ins_id_cd,
           |trim(ta.sub_card_no) as sub_card_no,
           |trim(ta.is_proced) as is_proced,
           |ta.entity_card_no as entity_card_no,
           |ta.cloud_pay_in as cloud_pay_in,
           |trim(ta.card_media) as card_media,
           |case
           |	when
           |		substr(ta.settle_dt,1,4) between '0001' and '9999' and substr(ta.settle_dt,5,2) between '01' and '12' and
           |		substr(ta.settle_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(ta.settle_dt,1,4),substr(ta.settle_dt,5,2),substr(ta.settle_dt,7,2))),9,2)
           |	then concat_ws('-',substr(ta.settle_dt,1,4),substr(ta.settle_dt,5,2),substr(ta.settle_dt,7,2))
           |	else
           |	(case when
           |		substr(ta.rec_crt_dt,1,4) between '0001' and '9999' and substr(ta.rec_crt_dt,5,2) between '01' and '12' and
           |		substr(ta.rec_crt_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(ta.rec_crt_dt,1,4),substr(ta.rec_crt_dt,5,2),substr(ta.rec_crt_dt,7,2))),9,2)
           |	then concat_ws('-',substr(ta.rec_crt_dt,1,4),substr(ta.rec_crt_dt,5,2),substr(ta.rec_crt_dt,7,2))
           |	else null end )
           |end as p_settle_dt
           |from tbl_umsvc_prize_discount_result ta
           | """.stripMargin)

      println("#### JOB_HV_32 spark sql 逻辑完成的系统时间为:" + DateUtils.getCurrentSystemTime())

      results.registerTempTable("spark_hive_prize_discount_result")
      println("#### JOB_HV_32 registerTempTable--spark_hive_prize_discount_result 完成的系统时间为:" + DateUtils.getCurrentSystemTime())

      if (!Option(results).isEmpty) {
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql(
          s"""
             |insert overwrite table hive_prize_discount_result partition (part_settle_dt)
             |select
             |prize_result_seq         ,
             |trans_id                 ,
             |sys_tra_no               ,
             |sys_tra_no_conv          ,
             |pri_acct_no              ,
             |trans_at                 ,
             |trans_at_conv            ,
             |trans_pos_at             ,
             |trans_dt_tm              ,
             |loc_trans_dt             ,
             |loc_trans_tm             ,
             |settle_dt                ,
             |mchnt_tp                 ,
             |acpt_ins_id_cd           ,
             |iss_ins_id_cd            ,
             |acq_ins_id_cd            ,
             |cup_branch_ins_id_cd     ,
             |cup_branch_ins_id_nm     ,
             |mchnt_cd                 ,
             |term_id                  ,
             |trans_curr_cd            ,
             |trans_chnl               ,
             |prod_in                  ,
             |agio_app_id              ,
             |agio_inf                 ,
             |prize_app_id             ,
             |prize_id                 ,
             |prize_lvl                ,
             |rec_crt_dt               ,
             |is_match_in              ,
             |fwd_ins_id_cd            ,
             |orig_trans_tfr_tm        ,
             |orig_sys_tra_no          ,
             |orig_acpt_ins_id_cd      ,
             |orig_fwd_ins_id_cd       ,
             |sub_card_no              ,
             |is_proced                ,
             |entity_card_no           ,
             |cloud_pay_in             ,
             |card_media               ,
             |p_settle_dt
             |from
             |spark_hive_prize_discount_result
         """.stripMargin)
        println("#### JOB_HV_32 动态分区插入完成的时间为：" + DateUtils.getCurrentSystemTime())
      } else {
        println("#### JOB_HV_32 spark sql 逻辑处理后无数据！")
      }
    }
  }


  /**
    * JOB_HV_33/10-19
    * hive_bill_sub_order_trans->viw_chmgm_bill_sub_order_detail_inf
    * Code by Xue
    *
    * @param start_dt
    * @param end_dt
    * @param sqlContext
    * @return
    */
  def JOB_HV_33(implicit sqlContext: HiveContext, start_dt: String, end_dt: String) = {
    println("#### JOB_HV_33(hive_bill_sub_order_trans->viw_chmgm_bill_sub_order_detail_inf)")

    DateUtils.timeCost("JOB_HV_33") {
      val start_day = start_dt.replace("-", "")
      val end_day = end_dt.replace("-", "")
      println("#### JOB_HV_33 增量抽取的时间范围: " + start_day + "--" + end_day)

      val df = sqlContext.readDB2_MGM_4para(s"$schemas_mgmdb.viw_chmgm_bill_sub_order_detail_inf", "trans_dt", s"$start_day", s"$end_day")
      println("#### JOB_HV_33 readDB2_MGM_4para 的系统时间为:" + DateUtils.getCurrentSystemTime())

      df.registerTempTable("viw_chmgm_bill_sub_order_detail_inf")
      println("#### JOB_HV_33 注册临时表的系统时间为:" + DateUtils.getCurrentSystemTime())

      val results = sqlContext.sql(
        s"""
           |select
           |ta.bill_sub_order_id as bill_sub_order_id,
           |trim(ta.bill_order_id) as bill_order_id,
           |trim(ta.mchnt_cd) as mchnt_cd,
           |ta.mchnt_nm as mchnt_nm,
           |trim(ta.sub_mchnt_cd) as sub_mchnt_cd,
           |ta.sub_mchnt_nm as sub_mchnt_nm,
           |trim(ta.bill_id) as bill_id,
           |ta.bill_price as bill_price,
           |trim(ta.trans_seq) as trans_seq,
           |ta.refund_reason as refund_reason,
           |trim(ta.order_st) as order_st,
           |ta.rec_crt_ts as rec_crt_ts,
           |trim(ta.crt_cdhd_usr_id) as crt_cdhd_usr_id,
           |ta.rec_upd_ts as rec_upd_ts,
           |trim(ta.upd_cdhd_usr_id) as upd_cdhd_usr_id,
           |ta.order_timeout_ts as order_timeout_ts,
           |case when
           |substr(ta.trans_dt,1,4) between '0001' and '9999' and substr(ta.trans_dt,5,2) between '01' and '12' and
           |substr(ta.trans_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(ta.trans_dt,1,4),substr(ta.trans_dt,5,2),substr(ta.trans_dt,7,2))),9,2)
           |then concat_ws('-',substr(ta.trans_dt,1,4),substr(ta.trans_dt,5,2),substr(ta.trans_dt,7,2))
           |else null end as trans_dt,
           |ta.related_usr_id as related_usr_id,
           |ta.trans_process as trans_process,
           |ta.response_code as response_code,
           |ta.response_msg as response_msg,
           |case when
           |substr(ta.trans_dt,1,4) between '0001' and '9999' and substr(ta.trans_dt,5,2) between '01' and '12' and
           |substr(ta.trans_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(ta.trans_dt,1,4),substr(ta.trans_dt,5,2),substr(ta.trans_dt,7,2))),9,2)
           |then concat_ws('-',substr(ta.trans_dt,1,4),substr(ta.trans_dt,5,2),substr(ta.trans_dt,7,2))
           |else substr(ta.rec_crt_ts,1,10)
           |end as p_trans_dt
           |from viw_chmgm_bill_sub_order_detail_inf ta
           | """.stripMargin)
      println("#### JOB_HV_33 spark sql 逻辑完成的系统时间为:" + DateUtils.getCurrentSystemTime())

      results.registerTempTable("spark_hive_bill_sub_order_trans")
      println("#### JOB_HV_33 registerTempTable--spark_hive_bill_sub_order_trans 完成的系统时间为:" + DateUtils.getCurrentSystemTime())

      if (!Option(results).isEmpty) {
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql(
          s"""
             |insert overwrite table hive_bill_sub_order_trans partition (part_trans_dt)
             |select
             |bill_sub_order_id    ,
             |bill_order_id        ,
             |mchnt_cd             ,
             |mchnt_nm             ,
             |sub_mchnt_cd         ,
             |sub_mchnt_nm         ,
             |bill_id              ,
             |bill_price           ,
             |trans_seq            ,
             |refund_reason        ,
             |order_st             ,
             |rec_crt_ts           ,
             |crt_cdhd_usr_id      ,
             |rec_upd_ts           ,
             |upd_cdhd_usr_id      ,
             |order_timeout_ts     ,
             |trans_dt             ,
             |related_usr_id       ,
             |trans_process        ,
             |response_code        ,
             |response_msg         ,
             |p_trans_dt
             |from
             |spark_hive_bill_sub_order_trans
         """.stripMargin)
        println("#### JOB_HV_33 动态分区插入完成的时间为：" + DateUtils.getCurrentSystemTime())
      } else {
        println("#### JOB_HV_33 spark sql 逻辑处理后无数据！")
      }
    }
  }

  /**
    * hive-job-36 2016-08-26
    * TBL_CHACC_CDHD_BILL_ACCT_INF -> HIVE_BUSS_DIST
    *
    * @author Xue
    * @param sqlContext
    */
  def JOB_HV_34(implicit sqlContext: HiveContext) = {
    println("###JOB_HV_34(tbl_chmgm_chara_acct_def_tmp -> hive_buss_dist)")
    println("#### JOB_HV_34 为全量抽取的表")

    DateUtils.timeCost("JOB_HV_34"){
      sqlContext.sql(s"use $hive_dbname")
      val df = sqlContext.readDB2_MGM(s"$schemas_mgmdb.tbl_chmgm_chara_acct_def_tmp")
      println("#### JOB_HV_34 readDB2_MGM 的系统时间为:"+DateUtils.getCurrentSystemTime())
      df.registerTempTable("tbl_chmgm_chara_acct_def_tmp")
      println("#### JOB_HV_34 注册临时表的系统时间为:"+DateUtils.getCurrentSystemTime())
      val results = sqlContext.sql(
        """
          |select
          |ta.rec_id as rec_id,
          |trim(ta.chara_acct_tp) as chara_acct_tp,
          |trim(ta.chara_acct_attr) as chara_acct_attr,
          |ta.chara_acct_desc as chara_acct_desc,
          |trim(ta.entry_grp_cd) as entry_grp_cd,
          |trim(ta.entry_blkbill_in) as entry_blkbill_in,
          |trim(ta.output_grp_cd)  as output_grp_cd,
          |trim(ta.output_blkbill_in) as output_blkbill_in,
          |trim(ta.chara_acct_st) as chara_acct_st,
          |trim(ta.aud_usr_id) as aud_usr_id,
          |ta.aud_idea as aud_idea,
          |ta.aud_ts as aud_ts,
          |trim(ta.aud_in) as aud_in,
          |trim(ta.oper_action) as oper_action,
          |ta.rec_crt_ts as rec_crt_ts,
          |ta.rec_upd_ts as rec_upd_ts,
          |trim(ta.rec_crt_usr_id) as rec_crt_usr_id,
          |trim(ta.rec_upd_usr_id) as rec_upd_usr_id,
          |ta.ver_no as ver_no,
          |trim(ta.chara_acct_nm) as chara_acct_nm,
          |ta.ch_ins_id_cd as ch_ins_id_cd,
          |trim(ta.um_ins_tp) as um_ins_tp,
          |ta.ins_nm  as ins_nm,
          |trim(ta.oper_in) as oper_in,
          |ta.event_id as event_id,
          |trim(ta.sync_st) as sync_st,
          |ta.cup_branch_ins_id_cd as cup_branch_ins_id_cd,
          |ta.scene_usage_in as scene_usage_in
          |from tbl_chmgm_chara_acct_def_tmp ta
        """.stripMargin
      )
      println("#### JOB_HV_34 spark sql 逻辑完成的系统时间为:"+DateUtils.getCurrentSystemTime())

      results.registerTempTable("spark_hive_buss_dist")
      println("#### JOB_HV_34 registerTempTable-- spark_hive_buss_dist完成的系统时间为:"+DateUtils.getCurrentSystemTime())

      if(!Option(results).isEmpty){
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql("truncate table hive_buss_dist")
        sqlContext.sql("insert into table hive_buss_dist select * from spark_hive_buss_dist")
        println("#### JOB_HV_34 全量数据插入完成的时间为："+DateUtils.getCurrentSystemTime())
      }else{
        println("#### JOB_HV_34 spark sql 逻辑处理后无数据！")
      }
    }

  }


  /**
    * hive-job-36 2016-08-26
    * TBL_CHACC_CDHD_BILL_ACCT_INF -> HIVE_CDHD_BILL_ACCT_INF
    *
    * @author Xue
    * @param sqlContext
    */
  def JOB_HV_35(implicit sqlContext: HiveContext) = {
    DateUtils.timeCost("JOB_HV_35"){
      println("###JOB_HV_35(tbl_chacc_cdhd_bill_acct_inf -> hive_cdhd_bill_acct_inf)")
      sqlContext.sql(s"use $hive_dbname")
      val df = sqlContext.readDB2_ACC(s"$schemas_accdb.tbl_chacc_cdhd_bill_acct_inf")
      df.registerTempTable("tbl_chacc_cdhd_bill_acct_inf")
      val results = sqlContext.sql(
        """
          |select
          |ta.seq_id as seq_id,
          |trim(ta.cdhd_usr_id) as cdhd_usr_id,
          |trim(ta.cdhd_fk) as cdhd_fk,
          |trim(ta.bill_id) as bill_id,
          |trim(ta.bill_bat_no) as bill_bat_no,
          |trim(ta.bill_tp) as bill_tp,
          |ta.mem_nm as mem_nm,
          |ta.bill_num as bill_num,
          |ta.usage_num as usage_num,
          |trim(ta.acct_st) as acct_st,
          |ta.rec_crt_ts as rec_crt_ts,
          |ta.rec_upd_ts as rec_upd_ts,
          |ta.ver_no as ver_no,
          |trim(ta.bill_related_card_no) as bill_related_card_no,
          |trim(ta.scene_id) as scene_id,
          |ta.freeze_bill_num  as freeze_bill_num
          |from tbl_chacc_cdhd_bill_acct_inf ta
        """.stripMargin
      )

//      println("JOB_HV_35------>results:"+results.count())
      if(!Option(results).isEmpty){
        results.registerTempTable("spark_hive_cdhd_bill_acct_inf")
        sqlContext.sql("truncate table hive_cdhd_bill_acct_inf")
        sqlContext.sql("insert into table hive_cdhd_bill_acct_inf select * from spark_hive_cdhd_bill_acct_inf")
        println("###JOB_HV_35(insert into table hive_cdhd_bill_acct_inf successful) ")
      }else{
        println("加载的表TBL_CHACC_CDHD_BILL_ACCT_INF中无数据！")
      }
    }

  }


  /**
    * JobName: JOB_HV_36
    * Feature: db2.tbl_umsvc_discount_bas_inf -> hive.hive_discount_bas_inf
    *
    * @author YangXue
    * @time 2016-08-26
    * @param sqlContext
    */
  def JOB_HV_36(implicit sqlContext: HiveContext) = {
    println("#### JOB_HV_36(tbl_umsvc_discount_bas_inf -> hive_discount_bas_inf)")
    println("#### JOB_HV_36 为全量抽取的表")

    DateUtils.timeCost("JOB_HV_36"){
      val df = sqlContext.readDB2_MGM(s"$schemas_mgmdb.TBL_UMSVC_DISCOUNT_BAS_INF")
      println("#### JOB_HV_36 readDB2_MGM 的系统时间为:"+DateUtils.getCurrentSystemTime())

      df.registerTempTable("spark_db2_dis_bas_inf")
      println("#### JOB_HV_36 注册临时表的系统时间为:"+DateUtils.getCurrentSystemTime())

      val results = sqlContext.sql(
        """
          |select
          |loc_activity_id,
          |loc_activity_nm,
          |loc_activity_desc,
          |case
          |	when
          |		substr(activity_begin_dt,1,4) between '0001' and '9999' and substr(activity_begin_dt,5,2) between '01' and '12' and
          |		substr(activity_begin_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(activity_begin_dt,1,4),substr(activity_begin_dt,5,2),substr(activity_begin_dt,7,2))),9,2)
          |	then concat_ws('-',substr(activity_begin_dt,1,4),substr(activity_begin_dt,5,2),substr(activity_begin_dt,7,2))
          |	else null
          |end as activity_begin_dt,
          |case
          |	when
          |		substr(activity_end_dt,1,4) between '0001' and '9999' and substr(activity_end_dt,5,2) between '01' and '12' and
          |		substr(activity_end_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(activity_end_dt,1,4),substr(activity_end_dt,5,2),substr(activity_end_dt,7,2))),9,2)
          |	then concat_ws('-',substr(activity_end_dt,1,4),substr(activity_end_dt,5,2),substr(activity_end_dt,7,2))
          |	else null
          |end as activity_end_dt,
          |agio_mchnt_num,
          |eff_mchnt_num,
          |sync_bat_no,
          |trim(sync_st) as sync_st,
          |trim(rule_upd_in) as rule_upd_in,
          |rule_grp_id,
          |rec_crt_ts,
          |trim(rec_crt_usr_id) as rec_crt_usr_id,
          |trim(rec_crt_ins_id_cd) as rec_crt_ins_id_cd,
          |case
          |when trim(rec_crt_ins_id_cd)='00011000' then '北京'
          |	when trim(rec_crt_ins_id_cd)='00011100' then '天津'
          |	when trim(rec_crt_ins_id_cd)='00011200' then '河北'
          |	when trim(rec_crt_ins_id_cd)='00011600' then '山西'
          |	when trim(rec_crt_ins_id_cd)='00011900' then '内蒙古'
          |	when trim(rec_crt_ins_id_cd)='00012210' then '辽宁'
          |	when trim(rec_crt_ins_id_cd)='00012220' then '大连'
          |	when trim(rec_crt_ins_id_cd)='00012400' then '吉林'
          |	when trim(rec_crt_ins_id_cd)='00012600' then '黑龙江'
          |	when trim(rec_crt_ins_id_cd)='00012900' then '上海'
          |	when trim(rec_crt_ins_id_cd)='00013000' then '江苏'
          |	when trim(rec_crt_ins_id_cd)='00013310' then '浙江'
          |	when trim(rec_crt_ins_id_cd)='00013320' then '宁波'
          |	when trim(rec_crt_ins_id_cd)='00013600' then '安徽'
          |	when trim(rec_crt_ins_id_cd)='00013900' then '福建'
          |	when trim(rec_crt_ins_id_cd)='00013930' then '厦门'
          |	when trim(rec_crt_ins_id_cd)='00014200' then '江西'
          |	when trim(rec_crt_ins_id_cd)='00014500' then '山东'
          |	when trim(rec_crt_ins_id_cd)='00014520' then '青岛'
          |	when trim(rec_crt_ins_id_cd)='00014900' then '河南'
          |	when trim(rec_crt_ins_id_cd)='00015210' then '湖北'
          |	when trim(rec_crt_ins_id_cd)='00015500' then '湖南'
          |	when trim(rec_crt_ins_id_cd)='00015800' then '广东'
          |	when trim(rec_crt_ins_id_cd)='00015840' then '深圳'
          |	when trim(rec_crt_ins_id_cd)='00016100' then '广西'
          |	when trim(rec_crt_ins_id_cd)='00016400' then '海南'
          |	when trim(rec_crt_ins_id_cd)='00016500' then '四川'
          |	when trim(rec_crt_ins_id_cd)='00016530' then '重庆'
          |	when trim(rec_crt_ins_id_cd)='00017000' then '贵州'
          |	when trim(rec_crt_ins_id_cd)='00017310' then '云南'
          |	when trim(rec_crt_ins_id_cd)='00017700' then '西藏'
          |	when trim(rec_crt_ins_id_cd)='00017900' then '陕西'
          |	when trim(rec_crt_ins_id_cd)='00018200' then '甘肃'
          |	when trim(rec_crt_ins_id_cd)='00018500' then '青海'
          |	when trim(rec_crt_ins_id_cd)='00018700' then '宁夏'
          |	when trim(rec_crt_ins_id_cd)='00018800' then '新疆'
          |	else '总公司'
          |end as cup_branch_ins_id_nm,
          |rec_upd_ts,
          |trim(rec_upd_usr_id) as rec_upd_usr_id,
          |trim(rec_upd_ins_id_cd) as rec_upd_ins_id_cd,
          |trim(del_in) as del_in,
          |ver_no,
          |trim(run_st) as run_st,
          |trim(posp_from_in) as posp_from_in,
          |trim(group_id) as group_id
          |from
          |spark_db2_dis_bas_inf
        """.stripMargin
      )
      println("#### JOB_HV_36 spark sql 逻辑完成的系统时间为:"+DateUtils.getCurrentSystemTime())
      //println("#### JOB_HV_36------>results:"+results.count())

      results.registerTempTable("spark_dis_bas_inf")
      println("#### JOB_HV_36 registerTempTable--spark_dis_bas_inf 完成的系统时间为:"+DateUtils.getCurrentSystemTime())

      if(!Option(results).isEmpty){
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql("insert overwrite table hive_discount_bas_inf select * from spark_dis_bas_inf")
        println("#### JOB_HV_36 全量数据插入完成的时间为："+DateUtils.getCurrentSystemTime())
      }else{
        println("#### JOB_HV_36 spark sql 逻辑处理后无数据！")
      }
    }
  }

  /**
    * JobName:hive-job-37
    * Feature:DB2.tbl_umsvc_filter_app_det-->hive_filter_app_det
    *
    * @author tzq
    * @time 2016-9-21
    * @param sqlContext
    * @return
    */
  def JOB_HV_37(implicit sqlContext: HiveContext) = {
    println("###JOB_HV_37[全量抽取](hive_filter_app_det-->tbl_umsvc_filter_app_det)")
    DateUtils.timeCost("JOB_HV_37"){
      val df = sqlContext.readDB2_MGM(s"$schemas_mgmdb.TBL_UMSVC_FILTER_APP_DET")
      println("#### JOB_HV_37 readDB2_MGM 的系统时间为:" + DateUtils.getCurrentSystemTime())
      df.registerTempTable("db2_tbl_umsvc_filter_app_det")
      println("#### JOB_HV_37 注册临时表 的系统时间为:" + DateUtils.getCurrentSystemTime())

      val results = sqlContext.sql(
        """
          |select
          |app_id,
          |trim(app_usage_id),
          |trim(rule_grp_cata),
          |trim(activity_plat),
          |loc_activity_id,
          |trim(sync_st),
          |sync_bat_no,
          |rule_grp_id,
          |trim(oper_in),
          |event_id,
          |rec_id,
          |trim(rec_crt_usr_id),
          |rec_crt_ts,
          |trim(rec_upd_usr_id),
          |rec_upd_ts,
          |trim(del_in),
          |ver_no
          |from
          |db2_tbl_umsvc_filter_app_det
        """.stripMargin)
      println("#### JOB_HV_37 spark sql 逻辑完成的系统时间为:" + DateUtils.getCurrentSystemTime())
      //println("###JOB_HV_37---------->results:"+results.count())
      if(!Option(results).isEmpty){
        results.registerTempTable("spark_db2_tbl_umsvc_filter_app_det")
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql("truncate table  hive_filter_app_det")
        sqlContext.sql("insert into table hive_filter_app_det select * from spark_db2_tbl_umsvc_filter_app_det")
        println("#### JOB_HV_37 全量数据插入完成的系统时间为:" + DateUtils.getCurrentSystemTime())
      }else{
        println("#### JOB_HV_37 spark sql 逻辑处理后无数据！")
      }
    }


  }


  /**
    * JobName: JOB_HV_38
    * Feature: db2.tbl_umsvc_filter_rule_det->hive.hive_filter_rule_det-->
    *
    * @author tzq
    * @time 2016-9-21
    * @param sqlContext
    * @return
    */
  def JOB_HV_38(implicit sqlContext: HiveContext) = {
    println("###JOB_HV_38[全量抽取](hive_filter_rule_det-->tbl_umsvc_filter_rule_det)")
    DateUtils.timeCost("JOB_HV_38"){
      val df = sqlContext.readDB2_MGM(s"$schemas_mgmdb.TBL_UMSVC_FILTER_RULE_DET")
      println("#### JOB_HV_38 readDB2_MGM 的系统时间为:" + DateUtils.getCurrentSystemTime())
      df.registerTempTable("db2_tbl_umsvc_filter_rule_det")
      println("#### JOB_HV_38 注册临时表 的系统时间为:" + DateUtils.getCurrentSystemTime())

      val results = sqlContext.sql(
        """
          |
          |select
          |rule_grp_id,
          |trim(rule_grp_cata),
          |trim(rule_min_val),
          |trim(rule_max_val),
          |trim(activity_plat),
          |trim(sync_st),
          |sync_bat_no,
          |trim(oper_in),
          |event_id,
          |rec_id,
          |trim(rec_crt_usr_id),
          |rec_crt_ts,
          |trim(rec_upd_usr_id),
          |rec_upd_ts,
          |trim(del_in),
          |ver_no
          |from
          |db2_tbl_umsvc_filter_rule_det
        """.stripMargin)
      println("#### JOB_HV_38 spark sql 逻辑完成的系统时间为:" + DateUtils.getCurrentSystemTime())
      //println("###JOB_HV_38---------->results:"+results.count())

      if(!Option(results).isEmpty){
        results.registerTempTable("spark_db2_tbl_umsvc_filter_rule_det")
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql("truncate table  hive_filter_rule_det")
        sqlContext.sql("insert into table hive_filter_rule_det select * from db2_tbl_umsvc_filter_rule_det")
        println("#### JOB_HV_38 全量数据插入完成的系统时间为:" + DateUtils.getCurrentSystemTime())
      }else{
        println("#### JOB_HV_38 spark sql 逻辑处理后无数据！")
      }
    }

  }


  /**
    * JobName: JOB_HV_44
    * Feature: db2.viw_chacc_cdhd_cashier_maktg_reward_dtl->hive.hive_cdhd_cashier_maktg_reward_dtl
    *
    * @author tzq
    * @time 2016-8-22
    * @param sqlContext
    * @return
    */
  def JOB_HV_44(implicit sqlContext: HiveContext) = {
    println("###JOB_HV_44[全量抽取](viw_chacc_cdhd_cashier_maktg_reward_dtl)")
    DateUtils.timeCost("JOB_HV_44"){
      val df= sqlContext.readDB2_ACC(s"$schemas_accdb.VIW_CHACC_CDHD_CASHIER_MAKTG_REWARD_DTL")
      println("#### JOB_HV_44 readDB2_ACC 的系统时间为:" + DateUtils.getCurrentSystemTime())
      df.registerTempTable("db2_cdhd_cashier_maktg_reward_dtl")
      println("#### JOB_HV_44 注册临时表 的系统时间为:" + DateUtils.getCurrentSystemTime())

      val results = sqlContext.sql(
        """
          |select
          |seq_id ,
          |trim(trans_tfr_tm) as trans_tfr_tm,
          |trim(sys_tra_no) as sys_tra_no,
          |trim(acpt_ins_id_cd) as acpt_ins_id_cd,
          |trim(fwd_ins_id_cd) as fwd_ins_id_cd,
          |trim(trans_id) as trans_id,
          |trim(pri_acct_no) as pri_acct_no,
          |trans_at,
          |remain_trans_at,
          |
          |case
          |	when
          |		substr(settle_dt,1,4) between '0001' and '9999' and substr(settle_dt,5,2) between '01' and '12' and
          |		substr(settle_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(settle_dt,1,4),substr(settle_dt,5,2),substr(settle_dt,7,2))),9,2)
          |	then concat_ws('-',substr(settle_dt,1,4),substr(settle_dt,5,2),substr(settle_dt,7,2))
          |	else null
          |end as settle_dt,
          |trim(mchnt_tp) as mchnt_tp,
          |trim(iss_ins_id_cd) as iss_ins_id_cd,
          |trim(acq_ins_id_cd) as acq_ins_id_cd,
          |cup_branch_ins_id_cd,
          |trim(mchnt_cd) as mchnt_cd,
          |trim(term_id) as term_id,
          |trim(trans_curr_cd) as trans_curr_cd,
          |trim(trans_chnl) as trans_chnl,
          |trim(orig_tfr_dt_tm) as orig_tfr_dt_tm,
          |trim(orig_sys_tra_no) as orig_sys_tra_no,
          |trim(orig_acpt_ins_id_cd) as orig_acpt_ins_id_cd,
          |trim(orig_fwd_ins_id_cd) as orig_fwd_ins_id_cd,
          |loc_activity_id,
          |prize_lvl,
          |trim(activity_tp) as activity_tp,
          |reward_point_rate,
          |reward_point_max,
          |prize_result_seq,
          |trim(trans_direct) as trans_direct,
          |trim(reward_usr_tp) as reward_usr_tp,
          |trim(cdhd_usr_id) as cdhd_usr_id,
          |trim(mobile) as mobile,
          |trim(reward_card_no) as reward_card_no,
          |reward_point_at,
          |trim(bill_id) as bill_id,
          |reward_bill_num,
          |trim(prize_dt) as prize_dt,
          |trim(rec_crt_dt) as rec_crt_dt,
          |trim(acct_dt) as acct_dt,
          |trim(rec_st) as rec_st,
          |rec_crt_ts,
          |rec_upd_ts,
          |trim(over_plan_in) as over_plan_in,
          |trim(is_match_in) as is_match_in,
          |rebate_activity_id,
          |rebate_activity_nm,
          |rebate_prize_lvl,
          |trim(buss_tp) as buss_tp,
          |trim(ins_acct_id) as ins_acct_id,
          |trim(chara_acct_tp) as chara_acct_tp,
          |trans_pri_key,
          |orig_trans_pri_key,
          |
          |case
          |when trim(cup_branch_ins_id_cd)='00011000'    then '北京'
          |when trim(cup_branch_ins_id_cd)='00011100'    then '天津'
          |when trim(cup_branch_ins_id_cd)='00011200'    then '河北'
          |when trim(cup_branch_ins_id_cd)='00011600'    then '山西'
          |when trim(cup_branch_ins_id_cd)='00011900'    then '内蒙古'
          |when trim(cup_branch_ins_id_cd)='00012210'    then '辽宁'
          |when trim(cup_branch_ins_id_cd)='00012220'    then '大连'
          |when trim(cup_branch_ins_id_cd)='00012400'    then '吉林'
          |when trim(cup_branch_ins_id_cd)='00012600'    then '黑龙江'
          |when trim(cup_branch_ins_id_cd)='00012900'    then '上海'
          |when trim(cup_branch_ins_id_cd)='00013000'    then '江苏'
          |when trim(cup_branch_ins_id_cd)='00013310'    then '浙江'
          |when trim(cup_branch_ins_id_cd)='00013320'    then '宁波'
          |when trim(cup_branch_ins_id_cd)='00013600'    then '安徽'
          |when trim(cup_branch_ins_id_cd)='00013900'    then '福建'
          |when trim(cup_branch_ins_id_cd)='00013930'    then '厦门'
          |when trim(cup_branch_ins_id_cd)='00014200'    then '江西'
          |when trim(cup_branch_ins_id_cd)='00014500'    then '山东'
          |when trim(cup_branch_ins_id_cd)='00014520'    then '青岛'
          |when trim(cup_branch_ins_id_cd)='00014900'    then '河南'
          |when trim(cup_branch_ins_id_cd)='00015210'    then '湖北'
          |when trim(cup_branch_ins_id_cd)='00015500'    then '湖南'
          |when trim(cup_branch_ins_id_cd)='00015800'    then '广东'
          |when trim(cup_branch_ins_id_cd)='00015840'    then '深圳'
          |when trim(cup_branch_ins_id_cd)='00016100'    then '广西'
          |when trim(cup_branch_ins_id_cd)='00016400'    then '海南'
          |when trim(cup_branch_ins_id_cd)='00016500'    then '四川'
          |when trim(cup_branch_ins_id_cd)='00016530'    then '重庆'
          |when trim(cup_branch_ins_id_cd)='00017000'    then '贵州'
          |when trim(cup_branch_ins_id_cd)='00017310'    then '云南'
          |when trim(cup_branch_ins_id_cd)='00017700'    then '西藏'
          |when trim(cup_branch_ins_id_cd)='00017900'    then '陕西'
          |when trim(cup_branch_ins_id_cd)='00018200'    then '甘肃'
          |when trim(cup_branch_ins_id_cd)='00018500'    then '青海'
          |when trim(cup_branch_ins_id_cd)='00018700'    then '宁夏'
          |when trim(cup_branch_ins_id_cd)='00018800'    then '新疆'
          |else '总公司' end as cup_branch_ins_id_nm
          |from
          |db2_cdhd_cashier_maktg_reward_dtl
        """.stripMargin)
      println("#### JOB_HV_44 spark sql 逻辑完成的系统时间为:" + DateUtils.getCurrentSystemTime())
      //println("###JOB_HV_44---------results 条目数: "+results.count())
      if(!Option(results).isEmpty){
        results.registerTempTable("spark_cdhd_cashier_maktg_reward_dtl")
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql("truncate table  hive_cdhd_cashier_maktg_reward_dtl")
        sqlContext.sql("insert into table hive_cdhd_cashier_maktg_reward_dtl select * from spark_cdhd_cashier_maktg_reward_dtl")
        println("#### JOB_HV_44 全量数据插入完成的系统时间为:" + DateUtils.getCurrentSystemTime())
      }else{
        println("#### JOB_HV_44 spark sql 逻辑处理后无数据！")
      }
    }


  }


  /**
    * JobName: JOB_HV_45
    * Feature: db2.tbl_cup_branch_acpt_ins_inf -> hive.hive_branch_acpt_ins_inf
    *
    * @author YangXue
    * @time 2016-10-28
    * @param sqlContext
    */
  def JOB_HV_45 (implicit sqlContext: HiveContext) = {

    println("#### JOB_HV_45(tbl_cup_branch_acpt_ins_inf -> hive_branch_acpt_ins_inf)")
    println("#### JOB_HV_45 为全量抽取的表")

    DateUtils.timeCost("JOB_HV_45"){
      val df = sqlContext.readDB2_ACC(s"$schemas_accdb.TBL_CUP_BRANCH_ACPT_INS_INF")
      println("#### JOB_HV_45 readDB2_ACC 的系统时间为:"+DateUtils.getCurrentSystemTime())

      df.registerTempTable("spark_db2_tbl_cup_branch_acpt_ins_inf")
      println("#### JOB_HV_45 注册临时表的系统时间为:"+DateUtils.getCurrentSystemTime())

      val results = sqlContext.sql(
        """
          |select
          |trim(cup_branch_ins_id_cd) as cup_branch_ins_id_cd,
          |trim(ins_id_cd) as ins_id_cd,
          |case
          |when cup_branch_ins_id_cd='00011000'  then '北京'
          |when cup_branch_ins_id_cd='00011100'  then '天津'
          |when cup_branch_ins_id_cd='00011200'  then '河北'
          |when cup_branch_ins_id_cd='00011600'  then '山西'
          |when cup_branch_ins_id_cd='00011900'  then '内蒙古'
          |when cup_branch_ins_id_cd='00012210'  then '辽宁'
          |when cup_branch_ins_id_cd='00012220'  then '大连'
          |when cup_branch_ins_id_cd='00012400'  then '吉林'
          |when cup_branch_ins_id_cd='00012600'  then '黑龙江'
          |when cup_branch_ins_id_cd='00012900'  then '上海'
          |when cup_branch_ins_id_cd='00013000'  then '江苏'
          |when cup_branch_ins_id_cd='00013310'  then '浙江'
          |when cup_branch_ins_id_cd='00013320'  then '宁波'
          |when cup_branch_ins_id_cd='00013600'  then '安徽'
          |when cup_branch_ins_id_cd='00013900'  then '福建'
          |when cup_branch_ins_id_cd='00013930'  then '厦门'
          |when cup_branch_ins_id_cd='00014200'  then '江西'
          |when cup_branch_ins_id_cd='00014500'  then '山东'
          |when cup_branch_ins_id_cd='00014520'  then '青岛'
          |when cup_branch_ins_id_cd='00014900'  then '河南'
          |when cup_branch_ins_id_cd='00015210'  then '湖北'
          |when cup_branch_ins_id_cd='00015500'  then '湖南'
          |when cup_branch_ins_id_cd='00015800'  then '广东'
          |when cup_branch_ins_id_cd='00015840'  then '深圳'
          |when cup_branch_ins_id_cd='00016100'  then '广西'
          |when cup_branch_ins_id_cd='00016400'  then '海南'
          |when cup_branch_ins_id_cd='00016500'  then '四川'
          |when cup_branch_ins_id_cd='00016530'  then '重庆'
          |when cup_branch_ins_id_cd='00017000'  then '贵州'
          |when cup_branch_ins_id_cd='00017310'  then '云南'
          |when cup_branch_ins_id_cd='00017700'  then '西藏'
          |when cup_branch_ins_id_cd='00017900'  then '陕西'
          |when cup_branch_ins_id_cd='00018200'  then '甘肃'
          |when cup_branch_ins_id_cd='00018500'  then '青海'
          |when cup_branch_ins_id_cd='00018700'  then '宁夏'
          |when cup_branch_ins_id_cd='00018800'  then '新疆'
          |else '总公司'
          |end as cup_branch_ins_id_cd
          |from
          |spark_db2_tbl_cup_branch_acpt_ins_inf
        """.stripMargin)
      println("#### JOB_HV_45 spark sql 逻辑完成的系统时间为:"+DateUtils.getCurrentSystemTime())
      //println("#### JOB_HV_45------>results:"+results.count())

      results.registerTempTable("spark_hive_branch_acpt_ins_inf")
      println("#### JOB_HV_45 registerTempTable--spark_hive_branch_acpt_ins_inf 完成的系统时间为:"+DateUtils.getCurrentSystemTime())

      if(!Option(results).isEmpty){
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql("insert overwrite table hive_branch_acpt_ins_inf select * from spark_hive_branch_acpt_ins_inf")
        println("#### JOB_HV_45 全量数据插入完成的时间为："+DateUtils.getCurrentSystemTime())
      }else{
        println("#### JOB_HV_45 spark sql 逻辑处理后无数据！")
      }
    }
  }

  /**
    * JOB_HV_46/10-14
    * hive_prize_activity_bas_inf->tbl_umsvc_prize_activity_bas_inf
    * Code by Xue
    *
    * @param sqlContext
    * @return
    */

  def JOB_HV_46 (implicit sqlContext: HiveContext) = {
    println("#### JOB_HV_46(hive_prize_activity_bas_inf<-tbl_umsvc_prize_activity_bas_inf)")
    println("#### JOB_HV_46 为全量抽取的表")

    DateUtils.timeCost("JOB_HV_46"){
      val df = sqlContext.readDB2_MGM(s"$schemas_mgmdb.tbl_umsvc_prize_activity_bas_inf")
      println("#### JOB_HV_46 readDB2_MGM 的系统时间为:"+DateUtils.getCurrentSystemTime())

      df.registerTempTable("tbl_umsvc_prize_activity_bas_inf")
      println("#### JOB_HV_46 注册临时表的系统时间为:"+DateUtils.getCurrentSystemTime())

      val results = sqlContext.sql(
        """
          |select
          |ta.loc_activity_id as loc_activity_id,
          |trim(ta.activity_plat) as activity_plat,
          |ta.loc_activity_nm as loc_activity_nm,
          |ta.loc_activity_desc as loc_activity_desc,
          |concat_ws('-',substr(trim(ta.activity_begin_dt),1,4),substr(trim(ta.activity_begin_dt),5,2),substr(trim(ta.activity_begin_dt),7,2)) as activity_begin_dt,
          |concat_ws('-',substr(trim(ta.activity_end_dt),1,4),substr(trim(ta.activity_end_dt),5,2),substr(trim(ta.activity_end_dt),7,2)) as activity_end_dt,
          |trim(ta.week_tm_bmp) as week_tm_bmp,
          |trim(ta.check_st) as check_st,
          |trim(ta.sync_st) as sync_st,
          |ta.sync_bat_no as sync_bat_no,
          |trim(ta.run_st) as run_st,
          |trim(ta.oper_in) as oper_in,
          |ta.event_id as event_id,
          |ta.rec_id as rec_id,
          |trim(ta.rec_upd_usr_id) as rec_upd_usr_id,
          |ta.rec_upd_ts as rec_upd_ts,
          |ta.rec_crt_ts as rec_crt_ts,
          |trim(ta.rec_crt_usr_id) as rec_crt_usr_id,
          |trim(ta.del_in) as del_in,
          |trim(ta.aud_usr_id) as aud_usr_id,
          |ta.aud_ts as aud_ts,
          |ta.aud_idea as aud_idea,
          |trim(ta.activity_st) as activity_st,
          |trim(ta.loc_activity_crt_ins) as loc_activity_crt_ins,
          |case
          |	when trim(ta.loc_activity_crt_ins)='00011000' then '北京'
          |	when trim(ta.loc_activity_crt_ins)='00011100' then '天津'
          |	when trim(ta.loc_activity_crt_ins)='00011200' then '河北'
          |	when trim(ta.loc_activity_crt_ins)='00011600' then '山西'
          |	when trim(ta.loc_activity_crt_ins)='00011900' then '内蒙古'
          |	when trim(ta.loc_activity_crt_ins)='00012210' then '辽宁'
          |	when trim(ta.loc_activity_crt_ins)='00012220' then '大连'
          |	when trim(ta.loc_activity_crt_ins)='00012400' then '吉林'
          |	when trim(ta.loc_activity_crt_ins)='00012600' then '黑龙江'
          |	when trim(ta.loc_activity_crt_ins)='00012900' then '上海'
          |	when trim(ta.loc_activity_crt_ins)='00013000' then '江苏'
          |	when trim(ta.loc_activity_crt_ins)='00013310' then '浙江'
          |	when trim(ta.loc_activity_crt_ins)='00013320' then '宁波'
          |	when trim(ta.loc_activity_crt_ins)='00013600' then '安徽'
          |	when trim(ta.loc_activity_crt_ins)='00013900' then '福建'
          |	when trim(ta.loc_activity_crt_ins)='00013930' then '厦门'
          |	when trim(ta.loc_activity_crt_ins)='00014200' then '江西'
          |	when trim(ta.loc_activity_crt_ins)='00014500' then '山东'
          |	when trim(ta.loc_activity_crt_ins)='00014520' then '青岛'
          |	when trim(ta.loc_activity_crt_ins)='00014900' then '河南'
          |	when trim(ta.loc_activity_crt_ins)='00015210' then '湖北'
          |	when trim(ta.loc_activity_crt_ins)='00015500' then '湖南'
          |	when trim(ta.loc_activity_crt_ins)='00015800' then '广东'
          |	when trim(ta.loc_activity_crt_ins)='00015840' then '深圳'
          |	when trim(ta.loc_activity_crt_ins)='00016100' then '广西'
          |	when trim(ta.loc_activity_crt_ins)='00016400' then '海南'
          |	when trim(ta.loc_activity_crt_ins)='00016500' then '四川'
          |	when trim(ta.loc_activity_crt_ins)='00016530' then '重庆'
          |	when trim(ta.loc_activity_crt_ins)='00017000' then '贵州'
          |	when trim(ta.loc_activity_crt_ins)='00017310' then '云南'
          |	when trim(ta.loc_activity_crt_ins)='00017700' then '西藏'
          |	when trim(ta.loc_activity_crt_ins)='00017900' then '陕西'
          |	when trim(ta.loc_activity_crt_ins)='00018200' then '甘肃'
          |	when trim(ta.loc_activity_crt_ins)='00018500' then '青海'
          |	when trim(ta.loc_activity_crt_ins)='00018700' then '宁夏'
          |	when trim(ta.loc_activity_crt_ins)='00018800' then '新疆'
          |else '总公司' end as cup_branch_ins_id_nm,
          |ta.ver_no as ver_no,
          |trim(ta.activity_tp) as activity_tp,
          |trim(ta.reprize_limit) as reprize_limit,
          |ta.sms_flag as sms_flag,
          |trim(ta.cashier_reward_in) as cashier_reward_in,
          |trim(ta.mchnt_cd) as mchnt_cd
          |
          |from tbl_umsvc_prize_activity_bas_inf ta
          | """.stripMargin)
      println("#### JOB_HV_46 spark sql 逻辑完成的系统时间为:"+DateUtils.getCurrentSystemTime())

      results.registerTempTable("spark_hive_prize_activity_bas_inf")
      println("#### JOB_HV_46 registerTempTable--spark_hive_prize_activity_bas_inf 完成的系统时间为:"+DateUtils.getCurrentSystemTime())

      if(!Option(results).isEmpty){
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql("insert overwrite table hive_prize_activity_bas_inf select * from spark_hive_prize_activity_bas_inf")
        println("#### JOB_HV_46 全量数据插入完成的时间为："+DateUtils.getCurrentSystemTime())
      }else{
        println("#### JOB_HV_46 spark sql 逻辑处理后无数据！")
      }
    }


  }

  /**
    * JOB_HV_47/10-14
    * hive_prize_lvl_add_rule->tbl_umsvc_prize_lvl_add_rule
    * Code by Xue
    *
    * @param sqlContext
    * @return
    */

  def JOB_HV_47 (implicit sqlContext: HiveContext) = {
    println("#### JOB_HV_47(hive_prize_lvl_add_rule->tbl_umsvc_prize_lvl_add_rule)")
    println("#### JOB_HV_47 为全量抽取的表")

    DateUtils.timeCost("JOB_HV_47"){
      val df2_1 = sqlContext.readDB2_MGM(s"$schemas_mgmdb.tbl_umsvc_prize_lvl_add_rule")
      println("#### JOB_HV_47 readDB2_MGM 的系统时间为:"+DateUtils.getCurrentSystemTime())

      df2_1.registerTempTable("tbl_umsvc_prize_lvl_add_rule")
      println("#### JOB_HV_47 注册临时表的系统时间为:"+DateUtils.getCurrentSystemTime())

      val results = sqlContext.sql(
        """
          |select
          |ta.prize_id_lvl as bill_id,
          |ta.prize_lvl as prize_lvl,
          |ta.reward_point_rate as reward_point_rate,
          |ta.reward_point_max as reward_point_max,
          |trim(ta.bill_id) as bill_id,
          |ta.reward_bill_num as reward_bill_num,
          |trim(ta.chd_reward_md) as chd_reward_md,
          |trim(ta.csh_reward_md) as csh_reward_md,
          |ta.csh_reward_point_rate as csh_reward_point_rate,
          |ta.csh_reward_point_max as csh_reward_point_max,
          |ta.csh_reward_day_max as csh_reward_day_max,
          |trim(ta.buss_tp) as buss_tp
          |
          |from tbl_umsvc_prize_lvl_add_rule ta
          |
          | """.stripMargin)
      println("#### JOB_HV_47 spark sql 逻辑完成的系统时间为:"+DateUtils.getCurrentSystemTime())

      results.registerTempTable("spark_hive_prize_lvl_add_rule")
      println("#### JOB_HV_47 registerTempTable--spark_hive_prize_lvl_add_rule 完成的系统时间为:"+DateUtils.getCurrentSystemTime())

      if(!Option(results).isEmpty){
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql("insert overwrite table hive_prize_lvl_add_rule select * from spark_hive_prize_lvl_add_rule")
        println("#### JOB_HV_47 全量数据插入完成的时间为："+DateUtils.getCurrentSystemTime())
      }else{
        println("#### JOB_HV_47 spark sql 逻辑处理后无数据！")
      }
    }

  }


  /**
    *JobName: JOB_HV_48
    *Feature: db2.tbl_umsvc_prize_bas->hive.hive_prize_bas
    *
    * @author tzq
    * @time 2016-8-29
    * @param sqlContext
    * @return
    */
  def JOB_HV_48(implicit sqlContext: HiveContext) = {
    println("###JOB_HV_48[全量抽取](hive_prize_bas->tbl_umsvc_prize_bas)")
    DateUtils.timeCost("JOB_HV_48"){
      val df = sqlContext.readDB2_MGM(s"$schemas_mgmdb.TBL_UMSVC_PRIZE_BAS")
      println("#### JOB_HV_48 readDB2_MGM 的系统时间为:" + DateUtils.getCurrentSystemTime())
      df.registerTempTable("db2_tbl_umsvc_prize_bas")
      println("#### JOB_HV_48 注册临时表 的系统时间为:" + DateUtils.getCurrentSystemTime())
      val results = sqlContext.sql(
        """
          |select
          |loc_activity_id,
          |trim(prize_tp) as prize_tp,
          |trim(activity_plat) as activity_plat,
          |trim(sync_st) as sync_st,
          |sync_bat_no,
          |prize_id,
          |trim(prize_st) as prize_st,
          |prize_lvl_num,
          |prize_nm,
          |trim(prize_begin_dt) as prize_begin_dt,
          |trim(prize_end_dt) as prize_end_dt,
          |trim(week_tm_bmp) as week_tm_bmp,
          |trim(oper_in) as oper_in,
          |event_id,
          |rec_id,
          |trim(rec_upd_usr_id) as rec_upd_usr_id,
          |rec_upd_ts,
          |rec_crt_ts,
          |trim(rec_crt_usr_id) as rec_crt_usr_id,
          |trim(del_in ) as del_in,
          |ver_no
          |
          |from
          |db2_tbl_umsvc_prize_bas
        """.stripMargin)
        println("#### JOB_HV_48 spark sql 逻辑完成的系统时间为:" + DateUtils.getCurrentSystemTime())
//      println("###JOB_HV_48--------->results : "+results.count())

      if(!Option(results).isEmpty){
        results.registerTempTable("spark_tbl_umsvc_prize_bas")
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql("truncate table  hive_prize_bas")
        sqlContext.sql("insert into table hive_prize_bas select * from spark_tbl_umsvc_prize_bas")
        println("#### JOB_HV_48 全量数据插入完成的系统时间为:" + DateUtils.getCurrentSystemTime())
      }else{
        println("#### JOB_HV_48 spark sql 逻辑处理后无数据！")      }
    }
  }

  /**
    * JobName: JOB_HV_54
    * Feature: db2.tbl_chacc_cashier_bas_inf->hive.hive_cashier_bas_inf
    *
    * @author tzq
    * @time 2016-8-29
    * @param sqlContext
    * @return
    */
  def JOB_HV_54(implicit sqlContext: HiveContext) = {
    println("###JOB_HV_54[全量抽取](tbl_chacc_cashier_bas_inf)")
    DateUtils.timeCost("JOB_HV_54"){
     val df = sqlContext.readDB2_ACC(s"$schemas_accdb.TBL_CHACC_CASHIER_BAS_INF")
     println("#### JOB_HV_54 readDB2_ACC 的系统时间为:" + DateUtils.getCurrentSystemTime())
     df.registerTempTable("db2_tbl_chacc_cashier_bas_inf")

     println("#### JOB_HV_54 注册临时表 的系统时间为:" + DateUtils.getCurrentSystemTime())

     val results = sqlContext.sql(
       """
         |select
         |trim(cashier_usr_id) as cashier_usr_id,
         |case
         |	when
         |		substr(trim(reg_dt),1,4) between '0001' and '9999' and substr(trim(reg_dt),5,2) between '01' and '12' and
         |		substr(trim(reg_dt),7,2) between '01' and substr(last_day(concat_ws('-',substr(trim(reg_dt),1,4),substr(trim(reg_dt),5,2),substr(trim(reg_dt),7,2))),9,2)
         |	then concat_ws('-',substr(trim(reg_dt),1,4),substr(trim(reg_dt),5,2),substr(trim(reg_dt),7,2))
         |	else null
         |end as reg_dt,
         |
         |usr_nm,
         |real_nm,
         |trim(real_nm_st) as real_nm_st,
         |trim(certif_id) as certif_id,
         |trim(certif_vfy_st) as certif_vfy_st,
         |trim(bind_card_no) as bind_card_no,
         |trim(card_auth_st) as card_auth_st,
         |bind_card_ts,
         |trim(mobile) as mobile,
         |trim(mobile_vfy_st) as mobile_vfy_st,
         |email_addr,
         |trim(email_vfy_st) as email_vfy_st,
         |trim(mchnt_cd) as mchnt_cd,
         |mchnt_nm,
         |trim(gb_region_cd) as gb_region_cd,
         |comm_addr,
         |trim(zip_cd) as zip_cd,
         |trim(industry_id) as industry_id,
         |hobby,
         |trim(cashier_lvl) as cashier_lvl,
         |login_greeting,
         |pwd_cue_ques,
         |pwd_cue_answ,
         |trim(usr_st) as usr_st,
         |trim(inf_source) as inf_source,
         |
         |case
         |when trim(inf_source)='00013600' then '安徽'
         |when trim(inf_source)='00011000' then '北京'
         |when trim(inf_source)='00012220' then '大连'
         |when trim(inf_source)='00013900' then '福建'
         |when trim(inf_source)='00018200' then '甘肃'
         |when trim(inf_source)='00015800' then '广东'
         |when trim(inf_source)='00016100' then '广西'
         |when trim(inf_source)='00017000' then '贵州'
         |when trim(inf_source)='00016400' then '海南'
         |when trim(inf_source)='00011200' then '河北'
         |when trim(inf_source)='00014900' then '河南'
         |when trim(inf_source)='00015210' then '湖北'
         |when trim(inf_source)='00015500' then '湖南'
         |when trim(inf_source)='00012400' then '吉林'
         |when trim(inf_source)='00013000' then '江苏'
         |when trim(inf_source)='00014200' then '江西'
         |when trim(inf_source)='00012210' then '辽宁'
         |when trim(inf_source)='00013320' then '宁波'
         |when trim(inf_source)='00018700' then '宁夏'
         |when trim(inf_source)='00014520' then '青岛'
         |when trim(inf_source)='00018500' then '青海'
         |when trim(inf_source)='00013930' then '厦门'
         |when trim(inf_source)='00014500' then '山东'
         |when trim(inf_source)='00011600' then '山西'
         |when trim(inf_source)='00017900' then '陕西'
         |when trim(inf_source)='00012900' then '上海'
         |when trim(inf_source)='00015840' then '深圳'
         |when trim(inf_source)='00016500' then '四川'
         |when trim(inf_source)='00011100' then '天津'
         |when trim(inf_source)='00017700' then '西藏'
         |when trim(inf_source)='00018800' then '新疆'
         |when trim(inf_source)='00017310' then '云南'
         |when trim(inf_source)='00013310' then '浙江'
         |when trim(inf_source)='00016530' then '重庆'
         |when trim(inf_source)='00012600' then '黑龙江'
         |when trim(inf_source)='00011900' then '内蒙古'
         |when trim(inf_source)='00010000' then '总公司'
         |else '其他' end
         |as cup_branch_ins_id_nm,
         |
         |rec_crt_ts,
         |rec_upd_ts,
         |trim(mobile_new) as mobile_new,
         |email_addr_new,
         |activate_ts,
         |activate_pwd,
         |trim(region_cd) as region_cd,
         |last_sign_in_ts,
         |ver_no,
         |trim(birth_dt) as birth_dt,
         |trim(sex) as sex,
         |trim(master_in) as master_in
         |from
         |db2_tbl_chacc_cashier_bas_inf
       """.stripMargin)
     println("#### JOB_HV_54 spark sql 逻辑完成的系统时间为:" + DateUtils.getCurrentSystemTime())
     //println("###JOB_HV_54--------->results："+results.count())
     if(!Option(results).isEmpty){
       results.registerTempTable("spark_bl_chacc_cashier_bas_inf")
       sqlContext.sql(s"use $hive_dbname")
       sqlContext.sql("truncate table  hive_cashier_bas_inf")
       sqlContext.sql("insert into table hive_cashier_bas_inf select * from spark_bl_chacc_cashier_bas_inf")
       println("#### JOB_HV_54 全量数据插入完成的系统时间为:" + DateUtils.getCurrentSystemTime())
     }else{
       println("#### JOB_HV_54 spark sql 逻辑处理后无数据！")
     }
   }

  }



  /**
    * JobName: JOB_HV_67
    * Feature: db2.tbl_umtxn_signer_log->hive.Hive_signer_log
    *
    * @author tzq
    * @time 2016-8-29
    * @param sqlContext
    */
  def JOB_HV_67(implicit sqlContext: HiveContext) = {
    println("###JOB_HV_67[全量抽取](Hive_signer_log -->tbl_umtxn_signer_log)")
    DateUtils.timeCost("JOB_HV_67"){
     val df = sqlContext.readDB2_ACC(s"$schemas_accdb.TBL_UMTXN_SIGNER_LOG")
      println("#### JOB_HV_67 readDB2_ACC 的系统时间为:" + DateUtils.getCurrentSystemTime())
      df.registerTempTable("db2_tbl_umtxn_signer_log")
      println("#### JOB_HV_67 注册临时表 的系统时间为:" + DateUtils.getCurrentSystemTime())
      val results = sqlContext.sql(
       """
         |select
         |trim(mchnt_cd),
         |trim(term_id),
         |trim(cashier_trans_tm),
         |trim(pri_acct_no),
         |trim(sync_bat_no)
         |from
         |db2_tbl_umtxn_signer_log
       """.stripMargin)
      println("#### JOB_HV_67 spark sql 逻辑完成的系统时间为:" + DateUtils.getCurrentSystemTime())
     //println("###JOB_HV_67---------->results："+results.count())

     if(!Option(results).isEmpty){
       results.registerTempTable("spark_db2_tbl_umtxn_signer_log")
       sqlContext.sql(s"use $hive_dbname")
       sqlContext.sql("truncate table  hive_signer_log")
       sqlContext.sql("insert into table hive_signer_log select * from spark_db2_tbl_umtxn_signer_log")
       println("#### JOB_HV_67 全量数据插入完成的系统时间为:" + DateUtils.getCurrentSystemTime())
     }else{
       println("#### JOB_HV_67 spark sql 逻辑处理后无数据！")     }
   }


  }


  /**
    * JobName: JOB_HV_68
    * Feature: db2.tbl_umtxn_cashier_point_acct_oper_dtl->hive_cashier_point_acct_oper_dtl
    *
    * @author tzq
    * @time 2016-8-29
    * @param sqlContext
    * @return
    */
  def JOB_HV_68(implicit sqlContext: HiveContext) = {
    println("###JOB_HV_68[全量抽取](tbl_umtxn_cashier_point_acct_oper_dtl)")
    DateUtils.timeCost("JOB_HV_68"){
      val df = sqlContext.readDB2_ACC(s"$schemas_accdb.TBL_UMTXN_CASHIER_POINT_ACCT_OPER_DTL")
      println("#### JOB_HV_68 readDB2_ACC 的系统时间为:" + DateUtils.getCurrentSystemTime())
      df.registerTempTable("db2_tbl_umtxn_cashier_point_acct_oper_dtl")
      println("#### JOB_HV_68 注册临时表 的系统时间为:" + DateUtils.getCurrentSystemTime())
      val results = sqlContext.sql(
        """
          |
          |select
          |acct_oper_id,
          |trim(cashier_usr_id),
          |acct_oper_ts,
          |acct_oper_point_at,
          |trim(acct_oper_related_id),
          |trim(acct_oper_tp),
          |ver_no
          |from
          |db2_tbl_umtxn_cashier_point_acct_oper_dtl
        """.stripMargin)
      println("#### JOB_HV_68 spark sql 逻辑完成的系统时间为:" + DateUtils.getCurrentSystemTime())
      //println("###JOB_HV_68---------->results："+results.count())

      if(!Option(results).isEmpty){
        results.registerTempTable("spark_db2_tbl_umtxn_cashier_point_acct_oper_dtl")
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql("truncate table  hive_cashier_point_acct_oper_dtl")
        sqlContext.sql("insert into table hive_cashier_point_acct_oper_dtl select * from spark_db2_tbl_umtxn_cashier_point_acct_oper_dtl")
        println("#### JOB_HV_68 全量数据插入完成的系统时间为:" + DateUtils.getCurrentSystemTime())
      }else{
        println("#### JOB_HV_68 spark sql 逻辑处理后无数据！")
      }
    }

  }

  /**
    * JOB_HV_69/10-14
    * hive_prize_lvl->tbl_umsvc_prize_lvl
    * Code by Xue
    *
    * @param sqlContext
    * @return
    */
  def JOB_HV_69 (implicit sqlContext: HiveContext) = {

    println("#### JOB_HV_69(hive_prize_lvl->tbl_umsvc_prize_lvl)")
    println("#### JOB_HV_69 为全量抽取的表")

    DateUtils.timeCost("JOB_HV_69"){
      val df = sqlContext.readDB2_MGM(s"$schemas_mgmdb.tbl_umsvc_prize_lvl")
      println("#### JOB_HV_69 readDB2_MGM 的系统时间为:"+DateUtils.getCurrentSystemTime())

      df.registerTempTable("tbl_umsvc_prize_lvl")
      println("#### JOB_HV_69 注册临时表的系统时间为:"+DateUtils.getCurrentSystemTime())

      val results = sqlContext.sql(
        """
          |select
          |ta.loc_activity_id as loc_activity_id,
          |trim(ta.prize_tp) as prize_tp,
          |ta.prize_lvl as  prize_lvl,
          |ta.prize_id_lvl as prize_id_lvl,
          |trim(ta.activity_plat) as activity_plat,
          |ta.prize_id as prize_id,
          |trim(ta.run_st) as run_st,
          |trim(ta.sync_st) as sync_st,
          |ta.sync_bat_no as sync_bat_no,
          |ta.lvl_prize_num as lvl_prize_num,
          |trim(ta.prize_lvl_desc) as prize_lvl_desc,
          |trim(ta.reprize_limit) as reprize_limit,
          |ta.prize_pay_tp as prize_pay_tp,
          |ta.cycle_prize_num as cycle_prize_num,
          |ta.cycle_span as cycle_span,
          |trim(ta.cycle_unit) as cycle_unit,
          |trim(ta.progrs_in) as progrs_in,
          |ta.seg_prize_num as seg_prize_num,
          |ta.min_prize_trans_at  as min_prize_trans_at,
          |ta.max_prize_trans_at as max_prize_trans_at,
          |ta.prize_at as prize_at,
          |trim(ta.oper_in) as oper_in,
          |ta.event_id as event_id,
          |ta.rec_id as rec_id,
          |trim(ta.rec_upd_usr_id) as rec_upd_usr_id,
          |ta.rec_upd_ts as rec_upd_ts,
          |ta.rec_crt_ts as rec_crt_ts,
          |ta.ver_no as ver_no
          |from tbl_umsvc_prize_lvl ta
          | """.stripMargin)
      println("#### JOB_HV_69 spark sql 逻辑完成的系统时间为:"+DateUtils.getCurrentSystemTime())

      results.registerTempTable("spark_hive_prize_lvl")
      println("#### JOB_HV_69 registerTempTable--spark_hive_prize_lvl 完成的系统时间为:"+DateUtils.getCurrentSystemTime())

      if(!Option(results).isEmpty){
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql("insert overwrite table hive_prize_lvl select * from spark_hive_prize_lvl")
        println("#### JOB_HV_69 全量数据插入完成的时间为："+DateUtils.getCurrentSystemTime())
      }else{
        println("#### JOB_HV_69 spark sql 逻辑处理后无数据！")
      }
    }
  }



  /**
    * JobName: JOB_HV_70
    * Feature: db2.tbl_inf_source_class -> hive.hive_inf_source_class
    *
    * @author YangXue
    * @time 2016-09-12
    * @param sqlContext
    */
  def JOB_HV_70(implicit sqlContext: HiveContext) = {

    println("#### JOB_HV_70(tbl_inf_source_class -> hive_inf_source_class)")
    println("#### JOB_HV_70 为全量抽取的表")

    DateUtils.timeCost("JOB_HV_70"){
      val df = sqlContext.readDB2_ACC(s"$schemas_accdb.TBL_INF_SOURCE_CLASS")
      println("#### JOB_HV_70 readDB2_ACC 的系统时间为:"+DateUtils.getCurrentSystemTime())

      df.registerTempTable("spark_db2_inf_source_class")
      println("#### JOB_HV_70 注册临时表的系统时间为:"+DateUtils.getCurrentSystemTime())

      val results = sqlContext.sql(
        """
          |select
          |access_nm,
          |class
          |from
          |spark_db2_inf_source_class
        """.stripMargin
      )
      println("#### JOB_HV_70 spark sql 逻辑完成的系统时间为:"+DateUtils.getCurrentSystemTime())
      //println("#### JOB_HV_70------>results:"+results.count())

      results.registerTempTable("spark_inf_source_class")
      println("#### JOB_HV_70 registerTempTable--spark_inf_source_class 完成的系统时间为:"+DateUtils.getCurrentSystemTime())

      if(!Option(results).isEmpty){
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql("insert overwrite table hive_inf_source_class select * from spark_inf_source_class")
        println("#### JOB_HV_70 全量数据插入完成的时间为："+DateUtils.getCurrentSystemTime())
      }else{
        println("#### JOB_HV_70 spark sql 逻辑处理后无数据！")
      }
    }
  }

  /**
    * hive-job-74 2016-11-3
    * hive_ticket_bill_acct_adj_task  ->  tbl_chmgm_ticket_bill_acct_adj_task
    *
    * @author tzq
    * @param sqlContext
    */
  def JOB_HV_74(implicit sqlContext: HiveContext) = {
    println("###JOB_HV_74(hive_ticket_bill_acct_adj_task  ->  tbl_chmgm_ticket_bill_acct_adj_task)")
    DateUtils.timeCost("JOB_HV_74"){
    sqlContext.sql(s"use $hive_dbname")
    println("#### JOB_HV_74 readmgmdb_DF 的系统时间为:"+DateUtils.getCurrentSystemTime())
    val df = sqlContext.jdbc_mgmdb_DF(s"$schemas_mgmdb.tbl_chmgm_ticket_bill_acct_adj_task")
    df.registerTempTable("db2_tbl_chmgm_ticket_bill_acct_adj_task")
    val results = sqlContext.sql(
      """
        |select
        |task_id,
        |trim(task_tp),
        |trim(usr_tp),
        |adj_ticket_bill,
        |usr_id,
        |trim(bill_id),
        |trim(proc_usr_id),
        |crt_ts,
        |trim(aud_usr_id),
        |aud_ts,
        |aud_idea,
        |trim(current_st),
        |file_path,
        |file_nm,
        |result_file_path,
        |result_file_nm,
        |remark,
        |ver_no,
        |trim(chk_usr_id),
        |chk_ts,
        |chk_idea,
        |trim(cup_branch_ins_id_cd),
        |trim(adj_rsn_cd),
        |rec_upd_ts,
        |rec_crt_ts,
        |trim(card_no),
        |trim(rec_crt_usr_id),
        |trim(acc_resp_cd),
        |acc_err_msg,
        |trim(entry_ins_id_cd),
        |entry_ins_cn_nm
        |
        |from
        |db2_tbl_chmgm_ticket_bill_acct_adj_task
        |
      """.stripMargin
    )
//    println("JOB_HV_74------>results:"+results.count())
    if(!Option(results).isEmpty){
      results.registerTempTable("spark_db2_tbl_chmgm_ticket_bill_acct_adj_task")
      sqlContext.sql("truncate table hive_ticket_bill_acct_adj_task")
      sqlContext.sql("insert into table hive_ticket_bill_acct_adj_task select * from spark_db2_tbl_chmgm_ticket_bill_acct_adj_task")
      println("#### JOB_HV_74 全量数据插入完成的时间为："+DateUtils.getCurrentSystemTime())
    }else{
      println("#### JOB_HV_74 spark sql 逻辑处理后无数据！")
    }
    }
  }

  /**
    * JOB_HV_75/11-9
    * hive_access_static_inf->tbl_chmgm_access_static_inf
    * Code by Xue
    *
    * @param sqlContext
    */
  def JOB_HV_75 (implicit sqlContext: HiveContext) = {
    println("#### JOB_HV_75(hive_access_static_inf->tbl_chmgm_access_static_inf)")
    println("#### JOB_HV_75 为全量抽取的表")

    DateUtils.timeCost("JOB_HV_75"){
      val df2_1 = sqlContext.readDB2_MGM(s"$schemas_mgmdb.tbl_chmgm_access_static_inf ")
      println("#### JOB_HV_75 readDB2_MGM 的系统时间为:"+DateUtils.getCurrentSystemTime())
      df2_1.registerTempTable("tbl_chmgm_access_static_inf")
      println("#### JOB_HV_75 注册临时表的系统时间为:"+DateUtils.getCurrentSystemTime())

      val results = sqlContext.sql(
        s"""
           |select
           |trim(ta.access_ins_id_cd) as access_ins_id_cd      ,
           |trim(ta.access_ins_abbr_cd) as access_ins_abbr_cd  ,
           |ta.access_ins_nm as access_ins_nm                  ,
           |trim(ta.access_sys_cd) as access_sys_cd            ,
           |trim(ta.access_ins_tp) as access_ins_tp            ,
           |trim(ta.access_bitmap) as access_bitmap            ,
           |trim(ta.trans_rcv_priv_bmp) as trans_rcv_priv_bmp  ,
           |trim(ta.trans_snd_priv_bmp) as trans_snd_priv_bmp  ,
           |trim(ta.enc_key_index) as enc_key_index            ,
           |trim(ta.mac_algo) as mac_algo                      ,
           |trim(ta.mak_len) as mak_len                        ,
           |trim(ta.pin_algo) as pin_algo                      ,
           |trim(ta.pik_len) as pik_len                        ,
           |trim(ta.enc_rsa_key_seq) as enc_rsa_key_seq        ,
           |trim(ta.rsa_enc_key_len) as rsa_enc_key_len        ,
           |ta.resv_fld as resv_fld                            ,
           |ta.mchnt_lvl as mchnt_lvl                          ,
           |trim(ta.valid_in) as valid_in                      ,
           |ta.event_id as event_id                            ,
           |trim(ta.oper_in) as oper_in                        ,
           |ta.rec_id as rec_id                                ,
           |trim(ta.rec_upd_usr_id) as rec_upd_usr_id          ,
           |ta.rec_crt_ts as rec_crt_ts                        ,
           |ta.rec_upd_ts as rec_upd_ts
           |
           |from tbl_chmgm_access_static_inf ta
           |
           | """.stripMargin)
      println("#### JOB_HV_75 spark sql 逻辑完成的系统时间为:"+DateUtils.getCurrentSystemTime())

      results.registerTempTable("spark_hive_access_static_inf")
      println("#### JOB_HV_75 registerTempTable--spark_hive_access_static_inf 完成的系统时间为:"+DateUtils.getCurrentSystemTime())

      if(!Option(results).isEmpty){
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql("truncate table  hive_access_static_inf")
        sqlContext.sql("insert into table hive_access_static_inf select * from spark_hive_access_static_inf")
        println("#### JOB_HV_75 全量数据插入完成的时间为："+DateUtils.getCurrentSystemTime())
      }else{
        println("#### JOB_HV_75 spark sql 逻辑处理后无数据！")

      }
    }

  }


  /**
    * JOB_HV_76/11-9
    * hive_region_cd->tbl_chmgm_region_cd
    * Code by Xue
    *
    * @param sqlContext
    */
  def JOB_HV_76 (implicit sqlContext: HiveContext) = {
    println("#### JOB_HV_76(hive_region_cd->tbl_chmgm_region_cd)")
    println("#### JOB_HV_76 为全量抽取的表")

    DateUtils.timeCost("JOB_HV_76"){
      val df2_1 = sqlContext.readDB2_MGM(s"$schemas_mgmdb.tbl_chmgm_region_cd ")
      println("#### JOB_HV_76 readDB2_MGM 的系统时间为:"+DateUtils.getCurrentSystemTime())

      df2_1.registerTempTable("tbl_chmgm_region_cd")
      println("#### JOB_HV_76 注册临时表的系统时间为:"+DateUtils.getCurrentSystemTime())

      val results = sqlContext.sql(
        s"""
           |select
           |trim(ta.region_cd) as region_cd                          ,
           |ta.region_cn_nm as region_cn_nm                          ,
           |trim(ta.region_en_nm) as region_en_nm                    ,
           |trim(ta.prov_region_cd) as prov_region_cd                ,
           |trim(ta.city_region_cd) as city_region_cd                ,
           |trim(ta.cntry_region_cd) as cntry_region_cd              ,
           |trim(ta.region_lvl) as region_lvl                        ,
           |trim(ta.oper_in) as oper_in                              ,
           |ta.event_id as event_id                                  ,
           |ta.rec_id as rec_id                                      ,
           |trim(ta.rec_upd_usr_id) as rec_upd_usr_id                ,
           |ta.rec_upd_ts as rec_upd_ts                              ,
           |ta.rec_crt_ts as rec_crt_ts                              ,
           |case when ta.prov_region_cd like '10%' then '北京'
           |	 when ta.prov_region_cd like '11%' then '天津'
           |	 when ta.prov_region_cd like '12%' then '河北'
           |	 when ta.prov_region_cd like '16%' then '山西'
           |	 when ta.prov_region_cd like '19%' then '内蒙古'
           |	 when ta.prov_region_cd like '22%' then '辽宁'
           |	 when ta.prov_region_cd like '24%' then '吉林'
           |	 when ta.prov_region_cd like '26%' then '黑龙江'
           |	 when ta.prov_region_cd like '29%' then '上海'
           |	 when ta.prov_region_cd like '30%' then '江苏'
           |	 when ta.prov_region_cd like '33%' then '浙江'
           |	 when ta.prov_region_cd like '36%' then '安徽'
           |	 when ta.prov_region_cd like '39%' then '福建'
           |	 when ta.prov_region_cd like '42%' then '江西'
           |	 when ta.prov_region_cd like '45%' then '山东'
           |	 when ta.prov_region_cd like '49%' then '河南'
           |	 when ta.prov_region_cd like '52%' then '湖北'
           |	 when ta.prov_region_cd like '55%' then '湖南'
           |	 when ta.prov_region_cd like '58%' then '广东'
           |	 when ta.prov_region_cd like '61%' then '广西'
           |	 when ta.prov_region_cd like '64%' then '海南'
           |	 when ta.prov_region_cd like '65%' then '四川'
           |	 when ta.prov_region_cd like '69%' then '重庆'
           |	 when ta.prov_region_cd like '70%' then '贵州'
           |	 when ta.prov_region_cd like '73%' then '云南'
           |	 when ta.prov_region_cd like '77%' then '西藏'
           |	 when ta.prov_region_cd like '79%' then '陕西'
           |	 when ta.prov_region_cd like '82%' then '甘肃'
           |	 when ta.prov_region_cd like '85%' then '青海'
           |	 when ta.prov_region_cd like '87%' then '宁夏'
           |	 when ta.prov_region_cd like '88%' then '新疆'
           |end as td_cup_branch_ins_id_nm,
           |case when ta.city_region_cd = '3930' then '厦门'
           |     when ta.city_region_cd = '2220' then '大连'
           |     when ta.city_region_cd = '5840' then '深圳'
           |     when ta.city_region_cd = '4520' then '青岛'
           |     when ta.city_region_cd = '3320' then '宁波'
           |     when ta.prov_region_cd like '10%' then '北京'
           |     when ta.prov_region_cd like '11%' then '天津'
           |     when ta.prov_region_cd like '12%' then '河北'
           |     when ta.prov_region_cd like '16%' then '山西'
           |     when ta.prov_region_cd like '19%' then '内蒙古'
           |     when ta.prov_region_cd like '22%' then '辽宁'
           |     when ta.prov_region_cd like '24%' then '吉林'
           |     when ta.prov_region_cd like '26%' then '黑龙江'
           |     when ta.prov_region_cd like '29%' then '上海'
           |     when ta.prov_region_cd like '30%' then '江苏'
           |     when ta.prov_region_cd like '33%' then '浙江'
           |     when ta.prov_region_cd like '36%' then '安徽'
           |     when ta.prov_region_cd like '39%' then '福建'
           |     when ta.prov_region_cd like '42%' then '江西'
           |     when ta.prov_region_cd like '45%' then '山东'
           |     when ta.prov_region_cd like '49%' then '河南'
           |     when ta.prov_region_cd like '52%' then '湖北'
           |     when ta.prov_region_cd like '55%' then '湖南'
           |     when ta.prov_region_cd like '58%' then '广东'
           |     when ta.prov_region_cd like '61%' then '广西'
           |     when ta.prov_region_cd like '64%' then '海南'
           |     when ta.prov_region_cd like '65%' then '四川'
           |     when ta.prov_region_cd like '69%' then '重庆'
           |     when ta.prov_region_cd like '70%' then '贵州'
           |     when ta.prov_region_cd like '73%' then '云南'
           |     when ta.prov_region_cd like '77%' then '西藏'
           |     when ta.prov_region_cd like '79%' then '陕西'
           |     when ta.prov_region_cd like '82%' then '甘肃'
           |     when ta.prov_region_cd like '85%' then '青海'
           |     when ta.prov_region_cd like '87%' then '宁夏'
           |     when ta.prov_region_cd like '88%' then '新疆'
           |end as cup_branch_ins_id_nm
           |
           |from tbl_chmgm_region_cd ta
           |
           | """.stripMargin)
      println("#### JOB_HV_76 spark sql 逻辑完成的系统时间为:"+DateUtils.getCurrentSystemTime())

      results.registerTempTable("spark_hive_region_cd")
      println("#### JOB_HV_76 registerTempTable--spark_hive_region_cd 完成的系统时间为:"+DateUtils.getCurrentSystemTime())

      if(!Option(results).isEmpty){
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql("truncate table  hive_region_cd")
        sqlContext.sql("insert into table hive_region_cd select * from spark_hive_region_cd")
        println("#### JOB_HV_76 全量数据插入完成的时间为："+DateUtils.getCurrentSystemTime())
      }else{
        println("#### JOB_HV_76 spark sql 逻辑处理后无数据！")
      }
    }

  }


  /**
    * hive-job-79/11-11
    * hive_aconl_ins_bas->tbl_aconl_ins_bas
    *
    * @author XTP
    * @param sqlContext
    * @return
    */
  def JOB_HV_79(implicit sqlContext: HiveContext) = {

    println("#### JOB_HV_79(hive_aconl_ins_bas->tbl_aconl_ins_bas)")
    println("#### JOB_HV_79 为全量抽取的表")

    DateUtils.timeCost("JOB_HV_79"){
      val df = sqlContext.readDB2_ACC(s"$schemas_accdb.tbl_aconl_ins_bas")
      println("#### JOB_HV_79 readDB2_ACC 的系统时间为:"+DateUtils.getCurrentSystemTime())

      df.registerTempTable("tbl_aconl_ins_bas")
      println("#### JOB_HV_79 注册临时表的系统时间为:"+DateUtils.getCurrentSystemTime())

      val results = sqlContext.sql(
        """
          |select
          |trim(ta.ins_id_cd) as ins_id_cd ,
          |trim(ta.ins_cata) as ins_cata ,
          |trim(ta.ins_tp) as ins_tp ,
          |trim(ta.hdqrs_ins_id_cd) as hdqrs_ins_id_cd ,
          |trim(ta.cup_branch_ins_id_cd) as cup_branch_ins_id_cd ,
          |trim(ta.region_cd) as region_cd ,
          |trim(ta.area_cd) as area_cd ,
          |ta.ins_cn_nm as ins_cn_nm ,
          |ta.ins_cn_abbr as ins_cn_abbr ,
          |ta.ins_en_nm as ins_en_nm ,
          |ta.ins_en_abbr as ins_en_abbr ,
          |trim(ta.rec_st) as rec_st ,
          |ta.rec_upd_ts as rec_upd_ts ,
          |ta.rec_crt_ts as rec_crt_ts ,
          |ta.comments as comments ,
          |trim(ta.oper_in) as oper_in ,
          |ta.event_id as event_id ,
          |ta.rec_id as rec_id ,
          |trim(ta.rec_upd_usr_id) as rec_upd_usr_id ,
          |case when trim(ta.cup_branch_ins_id_cd)='00011000' then '北京'
          |when trim(ta.cup_branch_ins_id_cd)='00011100' then '天津'
          |when trim(ta.cup_branch_ins_id_cd)='00011200' then '河北'
          |when trim(ta.cup_branch_ins_id_cd)='00011600' then '山西'
          |when trim(ta.cup_branch_ins_id_cd)='00011900' then '内蒙古'
          |when trim(ta.cup_branch_ins_id_cd)='00012210' then '辽宁'
          |when trim(ta.cup_branch_ins_id_cd)='00012220' then '大连'
          |when trim(ta.cup_branch_ins_id_cd)='00012400' then '吉林'
          |when trim(ta.cup_branch_ins_id_cd)='00012600' then '黑龙江'
          |when trim(ta.cup_branch_ins_id_cd)='00012900' then '上海'
          |when trim(ta.cup_branch_ins_id_cd)='00013000' then '江苏'
          |when trim(ta.cup_branch_ins_id_cd)='00013310' then '浙江'
          |when trim(ta.cup_branch_ins_id_cd)='00013320' then '宁波'
          |when trim(ta.cup_branch_ins_id_cd)='00013600' then '安徽'
          |when trim(ta.cup_branch_ins_id_cd)='00013900' then '福建'
          |when trim(ta.cup_branch_ins_id_cd)='00013930' then '厦门'
          |when trim(ta.cup_branch_ins_id_cd)='00014200' then '江西'
          |when trim(ta.cup_branch_ins_id_cd)='00014500' then '山东'
          |when trim(ta.cup_branch_ins_id_cd)='00014520' then '青岛'
          |when trim(ta.cup_branch_ins_id_cd)='00014900' then '河南'
          |when trim(ta.cup_branch_ins_id_cd)='00015210' then '湖北'
          |when trim(ta.cup_branch_ins_id_cd)='00015500' then '湖南'
          |when trim(ta.cup_branch_ins_id_cd)='00015800' then '广东'
          |when trim(ta.cup_branch_ins_id_cd)='00015840' then '深圳'
          |when trim(ta.cup_branch_ins_id_cd)='00016100' then '广西'
          |when trim(ta.cup_branch_ins_id_cd)='00016400' then '海南'
          |when trim(ta.cup_branch_ins_id_cd)='00016500' then '四川'
          |when trim(ta.cup_branch_ins_id_cd)='00016530' then '重庆'
          |when trim(ta.cup_branch_ins_id_cd)='00017000' then '贵州'
          |when trim(ta.cup_branch_ins_id_cd)='00017310' then '云南'
          |when trim(ta.cup_branch_ins_id_cd)='00017700' then '西藏'
          |when trim(ta.cup_branch_ins_id_cd)='00017900' then '陕西'
          |when trim(ta.cup_branch_ins_id_cd)='00018200' then '甘肃'
          |when trim(ta.cup_branch_ins_id_cd)='00018500' then '青海'
          |when trim(ta.cup_branch_ins_id_cd)='00018700' then '宁夏'
          |when trim(ta.cup_branch_ins_id_cd)='00018800' then '新疆'
          |else '总公司' end as cup_branch_ins_id_nm
          |from tbl_aconl_ins_bas ta
          |
        """.stripMargin)
      println("#### JOB_HV_79 spark sql 逻辑完成的系统时间为:"+DateUtils.getCurrentSystemTime())

      results.registerTempTable("spark_tbl_aconl_ins_bas")
      println("#### JOB_HV_79 registerTempTable--spark_tbl_aconl_ins_bas 完成的系统时间为:"+DateUtils.getCurrentSystemTime())

      if(!Option(results).isEmpty){
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql("insert overwrite table hive_aconl_ins_bas select * from spark_tbl_aconl_ins_bas")
        println("#### JOB_HV_79 全量数据插入完成的时间为："+DateUtils.getCurrentSystemTime())
      }else{
        println("#### JOB_HV_79 spark sql 逻辑处理后无数据！")
      }
    }

  }


  /**
    * JOB_HV_80/11-30
    * hive_trans_dtl->viw_chacc_acc_trans_dtl
    * Code by Xue
    *
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    */
  def JOB_HV_80 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {
    println("#### JOB_HV_80(hive_trans_dtl->viw_chacc_acc_trans_dtl)")

    DateUtils.timeCost("JOB_HV_80"){
      val start_day = start_dt.replace("-","")
      val end_day = end_dt.replace("-","")
      println("#### JOB_HV_80落地增量抽取的时间范围: "+start_day+"--"+end_day)

      val df = sqlContext.readDB2_ACC_4para(s"$schemas_accdb.viw_chacc_acc_trans_dtl","trans_dt",s"$start_day",s"$end_day")
      println("#### JOB_HV_80 readDB2_ACC_4para 的系统时间为:"+DateUtils.getCurrentSystemTime())

      df.registerTempTable("db2_viw_chacc_acc_trans_dtl")
      println("#### JOB_HV_80 注册临时表的系统时间为:"+DateUtils.getCurrentSystemTime())

      if(!Option(df).isEmpty){
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql(
          s"""
             |insert overwrite table hive_trans_dtl partition (part_trans_dt)
             |select
             |dtl.seq_id                 ,
             |trim(dtl.cdhd_usr_id)            ,
             |trim(dtl.card_no)                ,
             |trim(dtl.trans_tfr_tm)           ,
             |trim(dtl.sys_tra_no)             ,
             |trim(dtl.acpt_ins_id_cd)         ,
             |trim(dtl.fwd_ins_id_cd)          ,
             |trim(dtl.rcv_ins_id_cd)          ,
             |trim(dtl.oper_module)            ,
             |dtl.trans_dt               ,
             |trim(dtl.trans_tm)               ,
             |trim(dtl.buss_tp)                ,
             |trim(dtl.um_trans_id)            ,
             |trim(dtl.swt_right_tp)           ,
             |trim(dtl.bill_id)                ,
             |dtl.bill_nm                ,
             |trim(dtl.chara_acct_tp)          ,
             |trim(dtl.trans_at)               ,
             |dtl.point_at               ,
             |trim(dtl.mchnt_tp)               ,
             |trim(dtl.resp_cd)                ,
             |trim(dtl.card_accptr_term_id)    ,
             |trim(dtl.card_accptr_cd)         ,
             |dtl.trans_proc_start_ts    ,
             |dtl.trans_proc_end_ts      ,
             |trim(dtl.sys_det_cd)             ,
             |trim(dtl.sys_err_cd)             ,
             |dtl.rec_upd_ts             ,
             |trim(dtl.chara_acct_nm)          ,
             |trim(dtl.void_trans_tfr_tm)      ,
             |trim(dtl.void_sys_tra_no)        ,
             |trim(dtl.void_acpt_ins_id_cd)    ,
             |trim(dtl.void_fwd_ins_id_cd)     ,
             |dtl.orig_data_elemnt       ,
             |dtl.rec_crt_ts             ,
             |trim(dtl.discount_at)            ,
             |trim(dtl.bill_item_id)           ,
             |dtl.chnl_inf_index         ,
             |dtl.bill_num               ,
             |trim(dtl.addn_discount_at)       ,
             |trim(dtl.pos_entry_md_cd)        ,
             |dtl.udf_fld                ,
             |trim(dtl.card_accptr_nm_addr)    ,
             |case
             |	when
             |		substr(dtl.trans_dt,1,4) between '0001' and '9999' and substr(dtl.trans_dt,5,2) between '01' and '12' and
             |		substr(dtl.trans_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(dtl.trans_dt,1,4),substr(dtl.trans_dt,5,2),substr(dtl.trans_dt,7,2))),9,2)
             |	then concat_ws('-',substr(dtl.trans_dt,1,4),substr(dtl.trans_dt,5,2),substr(dtl.trans_dt,7,2))
             |	else substr(dtl.rec_crt_ts,1,10)
             |end as p_trans_dt
             |from db2_viw_chacc_acc_trans_dtl dtl
           """.stripMargin)
        println("#### JOB_HV_80 动态分区插入hive_trans_dtl 成功！")
      }else{
        println("#### db2_viw_chacc_acc_trans_dtl 表中无数据！")
      }
    }


  }

  /**
    * JOB_HV_81/11-30
    * hive_trans_log->viw_chacc_acc_trans_log
    * Code by Xue
    *
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    */
  def JOB_HV_81 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {
    println("#### JOB_HV_81(hive_trans_log->viw_chacc_acc_trans_log)")

    DateUtils.timeCost("JOB_HV_81"){
      val start_day = start_dt.replace("-","")
      val end_day = end_dt.replace("-","")
      println("#### JOB_HV_81 落地增量抽取的时间范围: "+start_day+"--"+end_day)

      val df = sqlContext.readDB2_ACC_4para(s"$schemas_accdb.viw_chacc_acc_trans_log","msg_settle_dt",s"$start_day",s"$end_day")
      println("#### JOB_HV_81 readDB2_ACC_4para 的系统时间为:"+DateUtils.getCurrentSystemTime())

      df.registerTempTable("db2_viw_chacc_acc_trans_log")
      println("#### JOB_HV_81 注册临时表的系统时间为:"+DateUtils.getCurrentSystemTime())

      if(!Option(df).isEmpty){
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql(
          s"""
             |insert overwrite table hive_trans_log partition (part_msg_settle_dt)
             |select
             |log.seq_id                   ,
             |trim(log.trans_tfr_tm)           ,
             |trim(log.sys_tra_no)             ,
             |trim(log.acpt_ins_id_cd)         ,
             |trim(log.fwd_ins_id_cd)          ,
             |trim(log.rcv_ins_id_cd)          ,
             |trim(log.oper_module)            ,
             |trim(log.um_trans_id)            ,
             |trim(log.msg_tp)                 ,
             |trim(log.cdhd_fk)                ,
             |trim(log.bill_id)                ,
             |trim(log.bill_tp)                ,
             |trim(log.bill_bat_no)            ,
             |null as bill_inf             ,
             |trim(log.card_no)                ,
             |trim(log.proc_cd)                ,
             |trim(log.trans_at)               ,
             |trim(log.trans_curr_cd)          ,
             |trim(log.settle_at)              ,
             |trim(log.settle_curr_cd)         ,
             |trim(log.card_accptr_local_tm)   ,
             |trim(log.card_accptr_local_dt)   ,
             |trim(log.expire_dt)              ,
             |trim(log.msg_settle_dt)          ,
             |trim(log.mchnt_tp)               ,
             |trim(log.pos_entry_md_cd)        ,
             |trim(log.pos_cond_cd)            ,
             |trim(log.pos_pin_capture_cd)     ,
             |trim(log.retri_ref_no)           ,
             |trim(log.auth_id_resp_cd)        ,
             |trim(log.resp_cd)                ,
             |trim(log.notify_st)              ,
             |trim(log.card_accptr_term_id)    ,
             |trim(log.card_accptr_cd)         ,
             |trim(log.card_accptr_nm_addr)    ,
             |null as addn_private_data    ,
             |log.udf_fld                  ,
             |trim(log.addn_at)                ,
             |log.orig_data_elemnt         ,
             |log.acct_id_1                ,
             |log.acct_id_2                ,
             |null as resv_fld             ,
             |log.cdhd_auth_inf            ,
             |trim(log.sys_settle_dt)          ,
             |trim(log.recncl_in)              ,
             |trim(log.match_in)               ,
             |log.trans_proc_start_ts      ,
             |log.trans_proc_end_ts        ,
             |trim(log.sys_det_cd)             ,
             |trim(log.sys_err_cd)             ,
             |trim(log.sec_ctrl_inf)           ,
             |trim(log.card_seq)               ,
             |log.rec_upd_ts               ,
             |null as dtl_inq_data         ,
             |case
             |	when
             |		substr(log.msg_settle_dt,1,4) between '0001' and '9999' and substr(log.msg_settle_dt,5,2) between '01' and '12' and
             |		substr(log.msg_settle_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(log.msg_settle_dt,1,4),substr(log.msg_settle_dt,5,2),substr(log.msg_settle_dt,7,2))),9,2)
             |	then concat_ws('-',substr(log.msg_settle_dt,1,4),substr(log.msg_settle_dt,5,2),substr(log.msg_settle_dt,7,2))
             |	else substr(log.rec_upd_ts,1,10)
             |end as p_settle_dt
             |from db2_viw_chacc_acc_trans_log log
        """.stripMargin)
        println("#### JOB_HV_81 动态分区插入hive_trans_log 成功！")
      }else{
        println("#### db2_viw_chacc_acc_trans_log 表中无数据！")
      }
    }


  }
  /**
    * JOB_HV_82/11-30
    * hive_swt_log->viw_chmgm_swt_log
    * Code by Xue
    *
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    */
  def JOB_HV_82 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {
    println("#### JOB_HV_82(hive_swt_log->viw_chmgm_swt_log)")

    DateUtils.timeCost("JOB_HV_82"){
      val start_day = start_dt.replace("-","")
      val end_day = end_dt.replace("-","")
      println("#### JOB_HV_82 落地增量抽取的时间范围: "+start_day+"--"+end_day)

      val df =  sqlContext.readDB2_MGM_4para(s"$schemas_mgmdb.viw_chmgm_swt_log","trans_dt",s"$start_day",s"$end_day")

      println("#### JOB_HV_82 readDB2_MGM_4para 的系统时间为:"+DateUtils.getCurrentSystemTime())

      df.registerTempTable("db2_viw_chmgm_swt_log")
      println("#### JOB_HV_82 注册临时表的系统时间为:"+DateUtils.getCurrentSystemTime())

      if(!Option(df).isEmpty){
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql(
          s"""
             |insert overwrite table hive_swt_log partition (part_trans_dt)
             |select
             |trim(log.tfr_dt_tm)                ,
             |trim(log.sys_tra_no)               ,
             |trim(log.acpt_ins_id_cd)           ,
             |trim(log.msg_fwd_ins_id_cd)        ,
             |log.pri_key1                       ,
             |log.fwd_chnl_head                  ,
             |log.chswt_plat_seq                 ,
             |trim(log.trans_tm)                 ,
             |trim(log.trans_dt)                 ,
             |trim(log.cswt_settle_dt)           ,
             |trim(log.internal_trans_tp)        ,
             |trim(log.settle_trans_id)          ,
             |trim(log.trans_tp)                 ,
             |trim(log.cups_settle_dt)           ,
             |trim(log.msg_tp)                   ,
             |trim(log.pri_acct_no)              ,
             |trim(log.card_bin)                 ,
             |trim(log.proc_cd)                  ,
             |log.req_trans_at                   ,
             |log.resp_trans_at                  ,
             |trim(log.trans_curr_cd)            ,
             |log.trans_tot_at                   ,
             |trim(log.iss_ins_id_cd)            ,
             |trim(log.launch_trans_tm)          ,
             |trim(log.launch_trans_dt)          ,
             |trim(log.mchnt_tp)                 ,
             |trim(log.pos_entry_md_cd)          ,
             |trim(log.card_seq_id)              ,
             |trim(log.pos_cond_cd)              ,
             |trim(log.pos_pin_capture_cd)       ,
             |trim(log.retri_ref_no)             ,
             |trim(log.term_id)                  ,
             |trim(log.mchnt_cd)                 ,
             |trim(log.card_accptr_nm_loc)       ,
             |trim(log.sec_related_ctrl_inf)     ,
             |log.orig_data_elemts               ,
             |trim(log.rcv_ins_id_cd)            ,
             |trim(log.fwd_proc_in)              ,
             |trim(log.rcv_proc_in)              ,
             |trim(log.proj_tp)                  ,
             |log.usr_id                         ,
             |log.conv_usr_id                    ,
             |trim(log.trans_st)                 ,
             |null as inq_dtl_req                ,
             |null as inq_dtl_resp               ,
             |log.iss_ins_resv                   ,
             |null as ic_flds                    ,
             |log.cups_def_fld                   ,
             |null as id_no                      ,
             |log.cups_resv                      ,
             |null as acpt_ins_resv              ,
             |trim(log.rout_ins_id_cd)           ,
             |trim(log.sub_rout_ins_id_cd)       ,
             |trim(log.recv_access_resp_cd)      ,
             |trim(log.chswt_resp_cd)            ,
             |trim(log.chswt_err_cd)             ,
             |log.resv_fld1                      ,
             |log.resv_fld2                      ,
             |log.to_ts                          ,
             |log.rec_upd_ts                     ,
             |log.rec_crt_ts                     ,
             |log.settle_at                      ,
             |log.external_amt                   ,
             |log.discount_at                    ,
             |log.card_pay_at                    ,
             |log.right_purchase_at              ,
             |trim(log.recv_second_resp_cd)      ,
             |null as req_acpt_ins_resv          ,
             |null as log_id                     ,
             |null as conv_acct_no               ,
             |null as inner_pro_ind              ,
             |null as acct_proc_in               ,
             |null as order_id                   ,
             |case
             |	when
             |		substr(log.trans_dt,1,4) between '0001' and '9999' and substr(log.trans_dt,5,2) between '01' and '12' and
             |		substr(log.trans_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(log.trans_dt,1,4),substr(log.trans_dt,5,2),substr(log.trans_dt,7,2))),9,2)
             |	then concat_ws('-',substr(log.trans_dt,1,4),substr(log.trans_dt,5,2),substr(log.trans_dt,7,2))
             |	else substr(log.rec_crt_ts,1,10)
             |end as p_trans_dt
             |from db2_viw_chmgm_swt_log log
        """.stripMargin)
        println("#### JOB_HV_82 动态分区插入hive_swt_log 成功！")
      }else{
        println("#### db2_viw_chmgm_swt_log 表中无数据！")
      }
    }

  }


  /**
    * JOB_HV_83 2017年3月15日
    * hive_point_trans->tbl_point_trans
    * @author  liutao
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    */
  def JOB_HV_83 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {
    println("#### job_hv_83(hive_point_trans->tbl_point_trans)")

    DateUtils.timeCost("JOB_HV_83"){
      val start_day = start_dt.replace("-","")
      val end_day = end_dt.replace("-","")
      println("#### JOB_HV_83 落地增量抽取的时间范围: "+start_day+"--"+end_day)
      val df =sqlContext.readDB2_MarketingWith4param(s"$schemas_upoupdb.tbl_point_trans","trans_dt",start_day,end_day)
      println("#### JOB_HV_83readDB2_Marketing 的系统时间为:"+DateUtils.getCurrentSystemTime())
      df.registerTempTable("spark_db2_tbl_point_trans")
      println("#### JOB_HV_83 注册临时表的系统时间为:"+DateUtils.getCurrentSystemTime())

      if(!Option(df).isEmpty){
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql(
          s"""
             |insert overwrite table hive_point_trans partition (part_trans_dt)
             |select
             |trim(trans.trans_id)                    ,
             |case
             |when
             |substr(trans.trans_dt,1,4) between '0001' and '9999' and substr(trans.trans_dt,5,2) between '01' and '12' and
             |substr(trans.trans_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(trans.trans_dt,1,4),substr(trans.trans_dt,5,2),substr(trans.trans_dt,7,2))),9,2)
             |then concat_ws('-',substr(trans.trans_dt,1,4),substr(trans.trans_dt,5,2),substr(trans.trans_dt,7,2))
             |else substr(trans.rec_crt_ts,1,10)
             |end as  trans_dt                        ,
             |trans.trans_ts                          ,
             |trim(trans.trans_tp)                    ,
             |trans.src_id                            ,
             |trim(trans.orig_trans_id)               ,
             |trans.refund_at                         ,
             |trans.mchnt_order_at                    ,
             |trim(trans.mchnt_order_curr)           ,
             |trim(trans.mchnt_order_dt)             ,
             |trim(trans.mchnt_order_id)             ,
             |trim(trans.mchnt_cd)                   ,
             |trim(trans.mchnt_tp)                   ,
             |trans.mchnt_nm                         ,
             |trans.usr_id                           ,
             |trim(trans.mobile)                     ,
             |trim(trans.card_no)                    ,
             |trim(trans.result_cd)                  ,
             |trans.result_msg                      ,
             |trans.rec_crt_ts                      ,
             |trans.rec_upd_ts                      ,
             |trim(trans.tran_src)                  ,
             |trim(trans.chnl_mchnt_cd)             ,
             |trans.extra_info                      ,
             |case
             |when
             |substr(trans.trans_dt,1,4) between '0001' and '9999' and substr(trans.trans_dt,5,2) between '01' and '12' and
             |substr(trans.trans_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(trans.trans_dt,1,4),substr(trans.trans_dt,5,2),substr(trans.trans_dt,7,2))),9,2)
             |then concat_ws('-',substr(trans.trans_dt,1,4),substr(trans.trans_dt,5,2),substr(trans.trans_dt,7,2))
             |else substr(trans.rec_crt_ts,1,10) end as  part_trans_dt
             |from spark_db2_tbl_point_trans trans
        """.stripMargin)
        println("#### JOB_HV_83动态分区插入hive_point_trans成功！")
      }else{
        println("#### db2_tbl_point_trans 表中无数据！")
      }
    }


  }

  /**
    * JOB_HV_84 2017年3月16日
    * hive_mksvc_order->tbl_mksvc_order
    * @author liutao
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    */
  def JOB_HV_84(implicit sqlContext: HiveContext,start_dt:String,end_dt:String)={
    println("#### job_hv_84(hive_mksvc_order->tbl_mksvc_order)")

    DateUtils.timeCost("JOB_HV_84"){
      val start_day = start_dt.replace("-","")
      val end_day = end_dt.replace("-","")
      println("#### JOB_HV_84 落地增量抽取的时间范围: "+start_day+"--"+end_day)
      val df =sqlContext.readDB2_MarketingWith4param(s"$schemas_upoupdb.tbl_mksvc_order","order_dt",start_day,end_day)
      println("#### JOB_HV_84读取营销库的系统时间为:"+DateUtils.getCurrentSystemTime())

      df.registerTempTable("spark_db2_tbl_mksvc_order")
      println("#### JOB_HV_84 注册临时表的系统时间为:"+DateUtils.getCurrentSystemTime())

      if(!Option(df).isEmpty){
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql(
          s"""
             |insert overwrite table hive_mksvc_order partition (part_order_dt)
             |select
             |trim(orders.order_id)                           ,
             |trim(orders.source_order_tp)                    ,
             |orders.source_order_id                          ,
             |trim(orders.source_order_ts)                    ,
             |orders.trans_at                                 ,
             |trim(orders.sys_id)                             ,
             |trim(orders.mchnt_cd)                           ,
             |trim(orders.order_st)                           ,
             |orders.activity_id                              ,
             |orders.award_lvl                                ,
             |trim(orders.award_ts)                           ,
             |orders.source_order_st                          ,
             |orders.mchnt_order_id                           ,
             |orders.usr_id                                   ,
             |orders.upop_usr_id                              ,
             |trim(orders.source_order_checked)               ,
             |case
             |when
             |substr(orders.order_dt,1,4) between '0001' and '9999' and substr(orders.order_dt,5,2) between '01' and '12' and
             |substr(orders.order_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(orders.order_dt,1,4),substr(orders.order_dt,5,2),substr(orders.order_dt,7,2))),9,2)
             |then concat_ws('-',substr(orders.order_dt,1,4),substr(orders.order_dt,5,2),substr(orders.order_dt,7,2))
             |else substr(orders.rec_crt_ts,1,10)
             |end as order_dt                                 ,
             |trim(orders.order_digest)                        ,
             |orders.usr_ip                                    ,
             |orders.rec_crt_ts                                ,
             |orders.rec_upd_ts                                ,
             |orders.rec_st                                    ,
             |trim(orders.rec_crt_oper_id)                     ,
             |orders.order_memo                                ,
             |orders.order_extend_inf                          ,
             |orders.award_id                                  ,
             |orders.mobile                                    ,
             |orders.card_no                                   ,
             |orders.unified_usr_id                            ,
             |case
             |when
             |substr(orders.order_dt,1,4) between '0001' and '9999' and substr(orders.order_dt,5,2) between '01' and '12' and
             |substr(orders.order_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(orders.order_dt,1,4),substr(orders.order_dt,5,2),substr(orders.order_dt,7,2))),9,2)
             |then concat_ws('-',substr(orders.order_dt,1,4),substr(orders.order_dt,5,2),substr(orders.order_dt,7,2))
             |else substr(orders.rec_crt_ts,1,10)  end as part_order_dt
             |from spark_db2_tbl_mksvc_order orders
        """.stripMargin)
        println("#### JOB_HV_84动态分区插入hive_mksvc_order成功！")
      }else{
        println("#### db2_tbl_mksvc_order 表中无数据！")
      }
    }

  }

  /**
    * JOB_HV_85
    * hive_wlonl_transfer_order->tbl_wlonl_transfer_order
    * @author liutao
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    */
  def JOB_HV_85(implicit sqlContext: HiveContext,start_dt:String,end_dt:String)={
    println("#### job_hv_85(hive_wlonl_transfer_order->tbl_wlonl_transfer_order")

    DateUtils.timeCost("JOB_HV_85"){
      val start_day = start_dt.replace("-","")
      val end_day = end_dt.replace("-","")
      println("#### JOB_HV_85 落地增量抽取的时间范围: "+start_day+"--"+end_day)
      val df=sqlContext.readDB2_MbgWith3param(s"$schemas_wlonldb.tbl_wlonl_transfer_order",start_day,end_day)
      println("#### JOB_HV_85读取营销库的系统时间为:"+DateUtils.getCurrentSystemTime())

      df.registerTempTable("spark_db2_tbl_wlonl_transfer_order")
      println("#### JOB_HV_85 注册临时表的系统时间为:"+DateUtils.getCurrentSystemTime())

      if(!Option(df).isEmpty){
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql(
          s"""
             |insert overwrite table hive_wlonl_transfer_order partition (part_order_dt)
             |select
             |trorder.id                               ,
             |trorder.user_id                          ,
             |trorder.tn                               ,
             |trorder.pan                              ,
             |trorder.trans_amount                     ,
             |trorder.order_desc                       ,
             |trorder.order_detail                     ,
             |trorder.status                           ,
             |trorder.create_time                      ,
             |trorder.trans_type                       ,
             |case
             |when
             |substr(trorder.create_time,1,4) between '0001' and '9999' and substr(trorder.create_time,5,2) between '01' and '12' and
             |substr(trorder.create_time,7,2) between '01' and substr(last_day(concat_ws('-',substr(trorder.create_time,1,4),substr(trorder.create_time,5,2),substr(trorder.create_time,7,2))),9,2)
             |then concat_ws('-',substr(trorder.create_time,1,4),substr(trorder.create_time,5,2),substr(trorder.create_time,7,2))
             |else '' end as  order_dt                  ,
             |case
             |when
             |substr(trorder.create_time,1,4) between '0001' and '9999' and substr(trorder.create_time,5,2) between '01' and '12' and
             |substr(trorder.create_time,7,2) between '01' and substr(last_day(concat_ws('-',substr(trorder.create_time,1,4),substr(trorder.create_time,5,2),substr(trorder.create_time,7,2))),9,2)
             |then concat_ws('-',substr(trorder.create_time,1,4),substr(trorder.create_time,5,2),substr(trorder.create_time,7,2))
             |else '' end as part_order_dt
             |from spark_db2_tbl_wlonl_transfer_order trorder
        """.stripMargin)
        println("#### JOB_HV_85动态分区插入hive_wlonl_transfer_order成功！")
      }else{
        println("#### db2_tbl_wlonl_transfer_order 表中无数据！")
      }
    }

  }

  /**
    * JOB_HV_86 2017年3月18日
    * hive_wlonl_uplan_coupon->tbl_wlonl_uplan_coupon
    * @author  liutao
    * @param sqlContext
    */
  def JOB_HV_86(implicit sqlContext: HiveContext)={
    println("#### job_hv_86(hive_wlonl_uplan_coupon->tbl_wlonl_uplan_coupon)")

    DateUtils.timeCost("JOB_HV_86"){
      val df =sqlContext.readDB2_Mbg(s"$schemas_wlonldb.tbl_wlonl_uplan_coupon")
      println("#### JOB_HV_86读取联机库的系统时间为:"+DateUtils.getCurrentSystemTime())
      df.registerTempTable("spark_db2_tbl_wlonl_uplan_coupon")
      println("#### JOB_HV_86 注册临时表的系统时间为:"+DateUtils.getCurrentSystemTime())
      if(!Option(df).isEmpty){
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql(
          s"""
             |insert overwrite table hive_wlonl_uplan_coupon
             |select
             | id                 ,
             |user_id             ,
             |pmt_code            ,
             |proc_dt             ,
             |coupon_id           ,
             |refnum              ,
             |valid_start_date    ,
             |valid_end_date
             |from spark_db2_tbl_wlonl_uplan_coupon coupon
        """.stripMargin)
        println("#### JOB_HV_86动态分区插入hive_wlonl_uplan_coupon成功！")
      }else{
        println("#### db2_tbl_wlonl_uplan_coupon 表中无数据！")
      }
    }

  }

  /**
    * JOB_HV_87 2017年3月31日
    * hive_wlonl_acc_notes->tbl_wlonl_acc_notes
    * @author  liutao
    * @param sqlContext
    */
  def JOB_HV_87(implicit sqlContext: HiveContext)={
    println("#### job_hv_87(hive_wlonl_acc_notes->tbl_wlonl_acc_notes)")

    DateUtils.timeCost("JOB_HV_87"){
      val df =sqlContext.readDB2_Mbg(s"$schemas_wlonldb.tbl_wlonl_acc_notes")
      println("#### JOB_HV_87读取联机库的系统时间为:"+DateUtils.getCurrentSystemTime())

      df.registerTempTable("spark_db2_tbl_wlonl_acc_notes")
      println("#### JOB_HV_87 注册临时表的系统时间为:"+DateUtils.getCurrentSystemTime())

      if(!Option(df).isEmpty){
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql(
          s"""
             |insert overwrite table hive_wlonl_acc_notes
             |select
             |notes_id                           ,
             |notes_tp                           ,
             |notes_at                           ,
             |trans_tm                           ,
             |trans_in_acc                       ,
             |trans_in_acc_tp                    ,
             |trans_out_acc                      ,
             |trans_out_acc_tp                   ,
             |mchnt_nm                           ,
             |notes_class                        ,
             |notes_class_nm                     ,
             |notes_class_child                  ,
             |notes_class_child_nm               ,
             |reimburse                          ,
             |members                            ,
             |currency_tp                        ,
             |pro_nm                             ,
             |user_id                            ,
             |rec_st                             ,
             |photo_url                          ,
             |remark                             ,
             |ext1                               ,
             |ext2                               ,
             |ext3                               ,
             |rec_crt_ts                         ,
             |rec_upd_ts
             |from spark_db2_tbl_wlonl_acc_notes  notes
        """.stripMargin)
        println("#### JOB_HV_87动态分区插入hive_wlonl_transfer_order成功！")
      }else{
        println("#### db2_tbl_wlonl_acc_notes 表中无数据！")
      }
    }

  }

  /**
    * JOB_HV_88  2017年3月31日
    * hive_ubp_order->tbl_ubp_order
    * @author liutao
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    */
  def JOB_HV_88(implicit sqlContext: HiveContext,start_dt:String,end_dt:String)={
    println("#### job_hv_88(hive_ubp_order->tbl_ubp_order)")

    DateUtils.timeCost("JOB_HV_88"){
      val start_day = start_dt.replace("-","")
      val end_day = end_dt.replace("-","")
      println("#### JOB_HV_88 落地增量抽取的时间范围: "+start_day+"--"+end_day)
      val df =sqlContext.readDB2_OrderWith4param(s"$schemas_mnsvcdb.tbl_ubp_order","order_dt",start_day,end_day)
      println("#### JOB_HV_88读取订单库的系统时间为:"+DateUtils.getCurrentSystemTime())

      df.registerTempTable("spark_db2_tbl_ubp_order")
      println("#### JOB_HV_88 注册临时表的系统时间为:"+DateUtils.getCurrentSystemTime())

      if(!Option(df).isEmpty){
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql(
          s"""
             |insert overwrite table hive_ubp_order partition (part_order_dt)
             |select
             |trim(order_id)                          ,
             |order_at                                ,
             |refund_at                               ,
             |trim(order_st)                          ,
             |case
             |when
             |substr(ubporder.order_dt,1,4) between '0001' and '9999' and substr(ubporder.order_dt,5,2) between '01' and '12' and
             |substr(ubporder.order_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(ubporder.order_dt,1,4),substr(ubporder.order_dt,5,2),substr(ubporder.order_dt,7,2))),9,2)
             |then concat_ws('-',substr(ubporder.order_dt,1,4),substr(ubporder.order_dt,5,2),substr(ubporder.order_dt,7,2))
             |else substr(ubporder.rec_crt_ts,1,10)
             |end as order_dt                         ,
             |trim(order_tm)                          ,
             |order_timeout                           ,
             |usr_id                                  ,
             |usr_ip                                  ,
             |trim(mer_id)                            ,
             |trim(sub_mer_id)                        ,
             |card_no                                 ,
             |bill_no                                 ,
             |trim(chnl_tp)                           ,
             |order_desc                              ,
             |access_order_id                         ,
             |access_reserved                         ,
             |trim(gw_tp)                             ,
             |notice_front_url                        ,
             |notice_back_url                         ,
             |trim(biz_tp)                            ,
             |trim(biz_map)                           ,
             |ext                                     ,
             |rec_crt_ts                              ,
             |rec_upd_ts                              ,
             |sub_biz_tp                              ,
             |trim(settle_id)                         ,
             |trim(ins_id_cd)                         ,
             |trim(access_md)                         ,
             |trim(mcc)                               ,
             |trim(submcc)                            ,
             |mer_name                                ,
             |mer_abbr                                ,
             |sub_mer_name                            ,
             |sub_mer_abbr                            ,
             |upoint_at                               ,
             |qr_code                                 ,
             |trim(payment_valid_tm)                  ,
             |trim(receive_ins_id_cd)                 ,
             |trim(term_id)                           ,
             |case
             |when
             |substr(ubporder.order_dt,1,4) between '0001' and '9999' and substr(ubporder.order_dt,5,2) between '01' and '12' and
             |substr(ubporder.order_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(ubporder.order_dt,1,4),substr(ubporder.order_dt,5,2),substr(ubporder.order_dt,7,2))),9,2)
             |then concat_ws('-',substr(ubporder.order_dt,1,4),substr(ubporder.order_dt,5,2),substr(ubporder.order_dt,7,2))
             |else substr(ubporder.rec_crt_ts,1,10)
             |end as part_order_dt
             |from spark_db2_tbl_ubp_order ubporder
        """.stripMargin)
        println("#### JOB_HV_88动态分区插入hive_ubp_order成功！")
      }else{
        println("#### db2_tbl_ubp_order 表中无数据！")
      }
    }

  }

  /**
    * JOB_HV_89 2017年3月20日
    * hive_mnsvc_business_instal_info->tbl_mnsvc_business_instal_info
    * @author liutao
    * @param sqlContext
    */
  def JOB_HV_89(implicit sqlContext: HiveContext)={
    println("#### job_hv_89(hive_mnsvc_business_instal_info->tbl_mnsvc_business_instal_info)")

    DateUtils.timeCost("JOB_HV_89"){
      //通过jdbc读取钱包的db2数据库的订单库的tbl_mnsvc_business_instal_info
      val df =sqlContext.readDB2_Order(s"$schemas_mnsvcdb.tbl_mnsvc_business_instal_info")
      println("#### JOB_HV_89读取订单库的系统时间为:"+DateUtils.getCurrentSystemTime())

      df.registerTempTable("spark_db2_tbl_mnsvc_business_instal_info")
      println("#### JOB_HV_89 注册临时表的系统时间为:"+DateUtils.getCurrentSystemTime())

      if(!Option(df).isEmpty){
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql(
          s"""
             |insert overwrite table hive_mnsvc_business_instal_info
             |select
             |instal_info_id                      ,
             |token_id                            ,
             |trim(user_id)                       ,
             |card_no                             ,
             |bank_cd                             ,
             |instal_amt                          ,
             |curr_num                            ,
             |period                              ,
             |trim(fee_option)                    ,
             |cred_no                             ,
             |prod_id                             ,
             |samt_pnt                            ,
             |apply_time                          ,
             |trim(instal_apply_st)               ,
             |trim(rec_st)                        ,
             |remark                              ,
             |rec_crt_ts                          ,
             |rec_upd_ts                          ,
             |ext1                                ,
             |ext2                                ,
             |ext3
             |from spark_db2_tbl_mnsvc_business_instal_info info
            """.stripMargin)
        println("#### JOB_HV_89动态分区插入hive_ubp_order成功！")
      }else{
        println("#### db2_tbl_mnsvc_business_instal_info 表中无数据！")
      }
    }

  }

}// ### END LINE ###
