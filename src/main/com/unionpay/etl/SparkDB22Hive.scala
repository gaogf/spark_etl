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
  // Xue update formatted date -_-
  private  lazy  val dateFormatter=DateTimeFormat.forPattern("yyyy-MM-dd")
  //指定HIVE数据库名
  private lazy val hive_dbname =ConfigurationManager.getProperty(Constants.HIVE_DBNAME)
  private lazy val schemas_accdb =ConfigurationManager.getProperty(Constants.SCHEMAS_ACCDB)
  private lazy val schemas_mgmdb =ConfigurationManager.getProperty(Constants.SCHEMAS_MGMDB)

 def main(args: Array[String]) {

    val conf = new SparkConf()
        .setAppName("SparkDB22Hive")
        .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
        .set("spark.Kryoserializer.buffer.max","1024m")
        .set("spark.yarn.driver.memoryOverhead","1024")
        .set("spark.yarn.executor.memoryOverhead","2000")
        .set("spark.newwork.buffer.timeout","300s")
        .set("spark.executor.heartbeatInterval","30s")
        .set("spark.driver.extraJavaOptions","-XX:+UseG1GC -XX:+UseCompressedOops")
        .set("spark.executor.extraJavaOptions","-XX:+UseG1GC -XX:+UseCompressedOops")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val sqlContext = new HiveContext(sc)


    //从数据库中获取当前JOB的执行起始和结束日期
    val rowParams=UPSQL_TIMEPARAMS_JDBC.readTimeParams(sqlContext)
    val start_dt=DateUtils.getYesterdayByJob(rowParams.getString(0))//获取开始日期：start_dt-1
    val end_dt=rowParams.getString(1)//结束日期
    val interval=DateUtils.getIntervalDays(start_dt,end_dt).toInt

    println(s"####The current Job start time：start_dt=$start_dt,end_dt=$end_dt####")


    val jobName = if(args.length>0) args(0) else None
    println(s"#### The current job name is ： $jobName ####")
    jobName match {
      /**
        * 每日模板job
        */
      case "JOB_HV_1"  => JOB_HV_1   //CODE BY YX
      case "JOB_HV_3"  => JOB_HV_3(sqlContext,start_dt,end_dt)   //CODE BY YX
      case "JOB_HV_4"  => JOB_HV_4(sqlContext,start_dt,end_dt)   //CODE BY XTP
      case "JOB_HV_8"  => JOB_HV_8(sqlContext,start_dt,end_dt)   //CODE BY XTP
      case "JOB_HV_9"  => JOB_HV_9   //CODE BY TZQ
      case "JOB_HV_10"  => JOB_HV_10  //CODE BY TZQ
      case "JOB_HV_11"  => JOB_HV_11  //CODE BY TZQ
      case "JOB_HV_12"  => JOB_HV_12  //CODE BY TZQ
      case "JOB_HV_13"  => JOB_HV_13  //CODE BY TZQ
      case "JOB_HV_14"  => JOB_HV_14  //CODE BY TZQ
      case "JOB_HV_16"  => JOB_HV_16  //CODE BY TZQ
      case "JOB_HV_18"  => JOB_HV_18(sqlContext,start_dt,end_dt)  //CODE BY YX
      case "JOB_HV_19"  => JOB_HV_19  //CODE BY YX
      case "JOB_HV_28"  => JOB_HV_28(sqlContext,start_dt,end_dt)  //CODE BY XTP
      case "JOB_HV_29"  => JOB_HV_29(sqlContext,start_dt,end_dt)  //CODE BY XTP
      case "JOB_HV_30"  => JOB_HV_30(sqlContext,start_dt,end_dt)  //CODE BY YX
      case "JOB_HV_32"  => JOB_HV_32(sqlContext,start_dt,end_dt)  //CODE BY XTP
      case "JOB_HV_33"  => JOB_HV_33(sqlContext,start_dt,end_dt)  //CODE BY XTP
      case "JOB_HV_36"  => JOB_HV_36  //CODE BY YX
      case "JOB_HV_43"  => JOB_HV_43(sqlContext,start_dt,end_dt)  //CODE BY YX
      case "JOB_HV_44"  => JOB_HV_44  //CODE BY TZQ
      case "JOB_HV_45"  => JOB_HV_45  //CODE BY YX
      case "JOB_HV_46"  => JOB_HV_46  //CODE BY XTP
      case "JOB_HV_47"  => JOB_HV_47  //CODE BY XTP
      case "JOB_HV_48"  => JOB_HV_48  //CODE BY TZQ
      case "JOB_HV_54"  => JOB_HV_54  //CODE BY TZQ
      case "JOB_HV_67"  => JOB_HV_67  //CODE BY TZQ
      case "JOB_HV_68"  => JOB_HV_68  //CODE BY TZQ
      case "JOB_HV_69"  => JOB_HV_69  //CODE BY XTP
      case "JOB_HV_70"  => JOB_HV_70  //CODE BY YX
      case "JOB_HV_79"  => JOB_HV_79  //CODE BY XTP


      /**
        * 指标套表job
        */
      case "JOB_HV_15"  => JOB_HV_15  //CODE BY TZQ  //测试出错，未解决
      case "JOB_HV_20_INI_I"  => JOB_HV_20_INI_I  //CODE BY YX
      //case "JOB_HV_20"  => JOB_HV_20  //CODE BY YX
      case "JOB_HV_23"  => JOB_HV_23  //CODE BY TZQ
      case "JOB_HV_24"  => JOB_HV_24  //CODE BY YX
      case "JOB_HV_25"  => JOB_HV_25  //CODE BY XTP
      case "JOB_HV_26"  => JOB_HV_26  //CODE BY TZQ
      case "JOB_HV_27"  => JOB_HV_27(sqlContext,start_dt,end_dt)  //CODE BY XTP
      case "JOB_HV_31"  => JOB_HV_31(sqlContext,start_dt,end_dt)  //CODE BY XTP
      case "JOB_HV_34"  => JOB_HV_34  //CODE BY XTP
      case "JOB_HV_35"  => JOB_HV_35  //CODE BY XTP
      case "JOB_HV_37"  => JOB_HV_37  //CODE BY TZQ
      case "JOB_HV_38"  => JOB_HV_38  //CODE BY TZQ
      case "JOB_HV_72"  => JOB_HV_72  //CODE BY TZQ
      case "JOB_HV_73"  => JOB_HV_73  //CODE BY TZQ
      case "JOB_HV_75"  => JOB_HV_75  //CODE BY XTP
      case "JOB_HV_76"  => JOB_HV_76  //CODE BY XTP

      case _ => println("Please input JobName")
    }

    sc.stop()
  }


  /**
    *hive-job-01/08-19
    *TBL_CHACC_CDHD_CARD_BIND_INF -> hive_card_bind_inf
    *
    * @author winslow yang
    * @param sqlContext
    * @return
    */
  def JOB_HV_1(implicit sqlContext: HiveContext) = {
    val currntTime =System.currentTimeMillis()
    println("###JOB_HV_1(tbl_chacc_cdhd_card_bind_inf -> hive_card_bind_inf)")
    val df = sqlContext.readDB2_ACC(s"$schemas_accdb.TBL_CHACC_CDHD_CARD_BIND_INF")
    df.registerTempTable("db2_card_bind_inf")
    val results = sqlContext.sql(
      """
        |select
        |cdhd_card_bind_seq,
        |trim(cdhd_usr_id) as cdhd_usr_id,
        |trim(bind_tp) as bind_tp,
        |trim(bind_card_no) as bind_card_no,
        |bind_ts,
        |unbind_ts,
        |trim(card_auth_st) as card_auth_st,
        |trim(card_bind_st) as card_bind_st,
        |trim(ins_id_cd) as ins_id_cd,
        |auth_ts,
        |trim(func_bmp) as func_bmp,
        |rec_crt_ts,                           rec_upd_ts,					                    ver_no,
        |sort_seq,						                  trim(cash_in) as cash_in,               trim(acct_point_ins_id_cd) as acct_point_ins_id_cd,
        |acct_owner,					                  bind_source,                            trim(card_media) as card_media,
        |backup_fld1,				                  backup_fld2,				                    trim(iss_ins_id_cd) as iss_ins_id_cd,
        |iss_ins_cn_nm,
        |case
        |when card_auth_st in ('1','2','3')
        |then min(rec_crt_ts)over(partition by cdhd_usr_id)
        |else null
        |end as frist_bind_ts
        |from db2_card_bind_inf
      """.stripMargin)

    println("###JOB_HV_1------>results:"+results.count())
    if(!Option(results).isEmpty){
      results.registerTempTable("spark_card_bind_inf")
      sqlContext.sql(s"use $hive_dbname")
      sqlContext.sql("truncate table hive_card_bind_inf")
      sqlContext.sql("insert into table hive_card_bind_inf select * from spark_card_bind_inf")
      println("insert into table hive_card_bind_inf successfully")
      val usedTime = System.currentTimeMillis() - currntTime
      val ss:Int =((System.currentTimeMillis() - currntTime)/1000).toInt
      val MM:Int = ss/60
      val hh:Int = MM/60
      val dd:Int = hh/24
      println("运行 JOB_HV_1 花费 :"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedTime+"毫秒")

    }else{
      println("加载的表tbl_chmgm_preferential_mchnt_inf中无数据！")
    }

  }

  /**
    * hive-job-03/08-19
    * tbl_chacc_cdhd_pri_acct_inf -> hive_pri_acct_inf
    *
    * @author winslow yang
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    */
  def JOB_HV_3(implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {
    val currntTime =System.currentTimeMillis()
    println("###JOB_HV_3(tbl_chacc_cdhd_pri_acct_inf)")
    val df1 = sqlContext.readDB2_ACC(s"$schemas_accdb.tbl_chacc_cdhd_pri_acct_inf")
    df1.registerTempTable("db2_pri_acct_inf")

    sqlContext.sql(s"use $hive_dbname")
    val results = sqlContext.sql(
      s"""
         |select
         |	trim(t1.cdhd_usr_id) as cdhd_usr_id,
         |	case
         |		when
         |			substr(t1.reg_dt,1,4) between '0001' and '9999' and substr(t1.reg_dt,5,2) between '01' and '12' and
         |			substr(t1.reg_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(t1.reg_dt,1,4),substr(t1.reg_dt,5,2),substr(t1.reg_dt,7,2))),9,2)
         |		then concat_ws('-',substr(t1.reg_dt,1,4),substr(t1.reg_dt,5,2),substr(t1.reg_dt,7,2))
         |		else null
         |	end as reg_dt,
         |	t1.usr_nm,											trim(t1.mobile) as mobile,										trim(t1.mobile_vfy_st) as mobile_vfy_st,
         |	t1.email_addr,							  	trim(t1.email_vfy_st) as email_vfy_st,				trim(t1.inf_source) as inf_source,
         |	t1.real_nm,											trim(t1.real_nm_st) as real_nm_st,						t1.nick_nm,
         |	trim(t1.certif_tp) as certif_tp,trim(t1.certif_id) as certif_id,              trim(t1.certif_vfy_st) as certif_vfy_st,
         |	case
         |		when
         |			substr(t1.birth_dt,1,4) between '0001' and '9999' and substr(t1.birth_dt,5,2) between '01' and '12' and
         |			substr(t1.birth_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(t1.birth_dt,1,4),substr(t1.birth_dt,5,2),substr(t1.birth_dt,7,2))),9,2)
         |		then concat_ws('-',substr(t1.birth_dt,1,4),substr(t1.birth_dt,5,2),substr(t1.birth_dt,7,2))
         |		else null
         |	end as birth_dt,
         |	trim(t1.sex) as sex,						        trim(t1.age) as age,										trim(t1.marital_st) as marital_st,
         |	trim(t1.home_mem_num) as home_mem_num,  trim(t1.cntry_cd) as cntry_cd,					trim(t1.gb_region_cd) as gb_region_cd,
         |	t1.comm_addr,														trim(t1.zip_cd) as zip_cd,							trim(t1.nationality) as nationality,
         |	trim(t1.ed_lvl) as ed_lvl,							t1.msn_no,															trim(t1.qq_no) as qq_no,
         |	t1.person_homepage,											trim(t1.industry_id) as industry_id,		trim(t1.annual_income_lvl) as annual_income_lvl,
         |	trim(t1.hobby) as hobby,								trim(t1.brand_prefer) as brand_prefer,	trim(t1.buss_dist_prefer) as buss_dist_prefer,
         |	trim(t1.head_pic_file_path) as head_pic_file_path,															trim(t1.pwd_cue_ques) as pwd_cue_ques,
         |	trim(t1.pwd_cue_answ) as pwd_cue_answ,	trim(t1.usr_eval_lvl) as usr_eval_lvl,	trim(t1.usr_class_lvl)as usr_class_lvl,
         |	trim(t1.usr_st) as usr_st,							t1.open_func,														t1.rec_crt_ts,
         |	t1.rec_upd_ts,													trim(t1.mobile_new) as mobile_new,			t1.email_addr_new,
         |	t1.activate_ts,													t1.activate_pwd,												trim(t1.region_cd) as region_cd,
         |	t1.ver_no,															trim(t1.func_bmp) as func_bmp,					t1.point_pre_open_ts,
         |	t1.refer_usr_id,												t1.vendor_fk,														t1.phone,
         |	t1.vip_svc,															t1.user_lvl_id,													t1.auto_adjust_lvl_in,
         |	case
         |		when
         |			substr(t1.lvl_begin_dt,1,4) between '0001' and '9999' and substr(t1.lvl_begin_dt,5,2) between '01' and '12' and
         |			substr(t1.lvl_begin_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(t1.lvl_begin_dt,1,4),substr(t1.lvl_begin_dt,5,2),substr(t1.lvl_begin_dt,7,2))),9,2)
         |		then concat_ws('-',substr(t1.lvl_begin_dt,1,4),substr(t1.lvl_begin_dt,5,2),substr(t1.lvl_begin_dt,7,2))
         |		else null
         |	end  lvl_begin_dt,
         |	t1.customer_title,											t1.company,															t1.dept,
         |	t1.duty,																t1.resv_phone,													t1.join_activity_list,
         |	t1.remark,															t1.note,
         |	case
         |		when
         |			substr(t1.usr_lvl_expire_dt,1,4) between '0001' and '9999' and substr(t1.usr_lvl_expire_dt,5,2) between '01' and '12' and
         |			substr(t1.usr_lvl_expire_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(t1.usr_lvl_expire_dt,1,4),substr(t1.usr_lvl_expire_dt,5,2),substr(t1.usr_lvl_expire_dt,7,2))),9,2)
         |		then concat_ws('-',substr(t1.usr_lvl_expire_dt,1,4),substr(t1.usr_lvl_expire_dt,5,2),substr(t1.usr_lvl_expire_dt,7,2))
         |		else null
         |	end as usr_lvl_expire_dt,
         |	trim(t1.reg_card_no) as reg_card_no,		trim(t1.reg_tm) as reg_tm,							t1.activity_source,
         |	trim(t1.chsp_svc_in) as chsp_svc_in,		trim(t1.accept_sms_in) as accept_sms_in,
         |	trim(t1.prov_division_cd) as prov_division_cd,																	trim(t1.city_division_cd) as city_division_cd,
         |	trim(t1.vid_last_login) as vid_last_login,																			trim(t1.pay_pwd) as pay_pwd,
         |	trim(t1.pwd_set_st) as pwd_set_st,
         | trim(t1.realnm_in),
         |	case
         |		when length(t1.certif_id)=15 then concat('19',substr(t1.certif_id,7,6))
         |		when length(t1.certif_id)=18 and substr(t1.certif_id,7,2) in ('19','20') then substr(t1.certif_id,7,8)
         |		else null
         |	end as birthday,
         |	case
         |    when length(trim(t1.certif_id)) in (15,18) then t2.name
         |    else null
         |  end as province_card,
         |  case
         |    when length(trim(t1.certif_id)) in (15,18) then t3.name
         |    else null
         |	end as city_card,
         |	case
         |		when length(trim(t1.mobile)) >= 11
         |		then t4.name
         |		else null
         |	end as mobile_provider,
         |	case
         |		when length(t1.certif_id)=15
         |		then
         |			case
         |				when int(substr(t1.certif_id,15,1))%2 = 0 then '女'
         |				when int(substr(t1.certif_id,15,1))%2 = 1 then '男'
         |				else null
         |			end
         |		when length(t1.certif_id)=18
         |		then
         |			case
         |				when int(substr(t1.certif_id,17,1))%2 = 0 then '女'
         |				when int(substr(t1.certif_id,15,1))%2 = 1 then '男'
         |				else null
         |			end
         |		else null
         |	end as sex_card,
         | 	case
         |		when trim(t1.city_division_cd)='210200' then '大连'
         |		when trim(t1.city_division_cd)='330200' then '宁波'
         |		when trim(t1.city_division_cd)='350200' then '厦门'
         |		when trim(t1.city_division_cd)='370200' then '青岛'
         |		when trim(t1.city_division_cd)='440300' then '深圳'
         |		when trim(t1.prov_division_cd) like '11%' then '北京'
         |		when trim(t1.prov_division_cd) like '12%' then '天津'
         |		when trim(t1.prov_division_cd) like '13%' then '河北'
         |		when trim(t1.prov_division_cd) like '14%' then '山西'
         |		when trim(t1.prov_division_cd) like '15%' then '内蒙古'
         |		when trim(t1.prov_division_cd) like '21%' then '辽宁'
         |		when trim(t1.prov_division_cd) like '22%' then '吉林'
         |		when trim(t1.prov_division_cd) like '23%' then '黑龙江'
         |		when trim(t1.prov_division_cd) like '31%' then '上海'
         |		when trim(t1.prov_division_cd) like '32%' then '江苏'
         |		when trim(t1.prov_division_cd) like '33%' then '浙江'
         |		when trim(t1.prov_division_cd) like '34%' then '安徽'
         |		when trim(t1.prov_division_cd) like '35%' then '福建'
         |		when trim(t1.prov_division_cd) like '36%' then '江西'
         |		when trim(t1.prov_division_cd) like '37%' then '山东'
         |		when trim(t1.prov_division_cd) like '41%' then '河南'
         |		when trim(t1.prov_division_cd) like '42%' then '湖北'
         |		when trim(t1.prov_division_cd) like '43%' then '湖南'
         |		when trim(t1.prov_division_cd) like '44%' then '广东'
         |		when trim(t1.prov_division_cd) like '45%' then '广西'
         |		when trim(t1.prov_division_cd) like '46%' then '海南'
         |		when trim(t1.prov_division_cd) like '50%' then '重庆'
         |		when trim(t1.prov_division_cd) like '51%' then '四川'
         |		when trim(t1.prov_division_cd) like '52%' then '贵州'
         |		when trim(t1.prov_division_cd) like '53%' then '云南'
         |		when trim(t1.prov_division_cd) like '54%' then '西藏'
         |		when trim(t1.prov_division_cd) like '61%' then '陕西'
         |		when trim(t1.prov_division_cd) like '62%' then '甘肃'
         |		when trim(t1.prov_division_cd) like '63%' then '青海'
         |		when trim(t1.prov_division_cd) like '64%' then '宁夏'
         |		when trim(t1.prov_division_cd) like '65%' then '新疆'
         |		else '总公司'
         |  end as phone_location,
         |  t5.relate_id as relate_id
         |from
         |	db2_pri_acct_inf t1
         |left join
         |	hive_province_card t2 on trim(substr(t1.certif_id,1,2)) = trim(t2.id)
         |left join
         |	hive_city_card t3 on trim(substr(t1.certif_id,1,4)) = trim(t3.id)
         |left join
         |	hive_ct t4 on trim(substr(substr(t1.mobile,-11,11),1,4)) = trim(t4.id)
         |left join
         |	hive_ucbiz_cdhd_bas_inf t5 on trim(t1.cdhd_usr_id) = trim(t5.usr_id)
       """.stripMargin)

    println("###JOB_HV_3------>results:"+results.count())

    if(!Option(results).isEmpty){
      results.registerTempTable("spark_pri_acct_inf")
      sqlContext.sql(s"use $hive_dbname")
      sqlContext.sql("truncate table hive_pri_acct_inf")
      sqlContext.sql("insert into table hive_pri_acct_inf select * from spark_pri_acct_inf")
      println("###### insert into table hive_pri_acct_inf successfule ######")
      val usedTime = System.currentTimeMillis() - currntTime
      val ss:Int =((System.currentTimeMillis() - currntTime)/1000).toInt
      val MM:Int = ss/60
      val hh:Int = MM/60
      val dd:Int = hh/24
      println("运行 JOB_HV_3 花费 :"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedTime+"毫秒")

    }else{
      println("加载的表hive_pri_acct_inf中无数据！")
    }
  }

  /**
    * JOB_HV_4/10-14
    * hive_acc_trans->viw_chacc_acc_trans_dtl
    * Code by Xue
    *
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    */
  def JOB_HV_4 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {
    val currntTime =System.currentTimeMillis()

    val start_day = start_dt.replace("-","")
    val end_day = end_dt.replace("-","")

    val df2_1 = sqlContext.readDB2_ACC_4para(s"$schemas_accdb.viw_chacc_acc_trans_dtl","trans_dt",s"$start_day",s"$end_day")
    df2_1.registerTempTable("viw_chacc_acc_trans_dtl")

    val df2_2 = sqlContext.readDB2_ACC_4para(s"$schemas_accdb.viw_chacc_acc_trans_log","msg_settle_dt",s"$start_day",s"$end_day")
    df2_2.registerTempTable("viw_chacc_acc_trans_log")

    val df2_3 = sqlContext.readDB2_MGM_4para(s"$schemas_mgmdb.viw_chmgm_swt_log","trans_dt",s"$start_day",s"$end_day")
    df2_3.registerTempTable("viw_chmgm_swt_log")


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
         |ta.trans_proc_start_ts  as frist_trans_proc_start_ts,
         |td.second_trans_proc_start_ts as second_trans_proc_start_ts,
         |td.third_trans_proc_start_ts as third_trans_proc_start_ts,
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
         |trim(tb.msg_tp) as msg_tp,
         |trim(tb.cdhd_fk) as cdhd_fk,
         |trim(tb.bill_tp) as bill_tp,
         |trim(tb.bill_bat_no) as bill_bat_no,
         |tb.bill_inf as bill_inf,
         |trim(tb.proc_cd) as proc_cd,
         |trim(tb.trans_curr_cd) as trans_curr_cd,
         |case
         |	when length(trim(translate(trim(tb.settle_at),'-0123456789',' ')))=0 then trim(tb.settle_at)
         |	else null
         |end as settle_at,
         |trim(tb.settle_curr_cd) as settle_curr_cd,
         |trim(tb.card_accptr_local_tm) as card_accptr_local_tm,
         |trim(tb.card_accptr_local_dt) as card_accptr_local_dt,
         |trim(tb.expire_dt) as expire_dt,
         |case
         |	when
         |		substr(tb.msg_settle_dt,1,4) between '0001' and '9999' and substr(tb.msg_settle_dt,5,2) between '01' and '12' and
         |		substr(tb.msg_settle_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(tb.msg_settle_dt,1,4),substr(tb.msg_settle_dt,5,2),substr(tb.msg_settle_dt,7,2))),9,2)
         |	then concat_ws('-',substr(tb.msg_settle_dt,1,4),substr(tb.msg_settle_dt,5,2),substr(tb.msg_settle_dt,7,2))
         |	else null
         |end as msg_settle_dt,
         |trim(tb.pos_cond_cd) as pos_cond_cd,
         |trim(tb.pos_pin_capture_cd) as pos_pin_capture_cd,
         |trim(tb.retri_ref_no) as retri_ref_no,
         |trim(tb.auth_id_resp_cd) as auth_id_resp_cd,
         |trim(tb.notify_st) as notify_st,
         |tb.addn_private_data as addn_private_data,
         |trim(tb.addn_at) as addn_at,
         |tb.acct_id_1 as acct_id_1,
         |tb.acct_id_2 as acct_id_2,
         |tb.resv_fld as resv_fld,
         |tb.cdhd_auth_inf as cdhd_auth_inf,
         |case
         |	when
         |		substr(tb.sys_settle_dt,1,4) between '0001' and '9999' and substr(tb.sys_settle_dt,5,2) between '01' and '12' and
         |		substr(tb.sys_settle_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(tb.sys_settle_dt,1,4),substr(tb.sys_settle_dt,5,2),substr(tb.sys_settle_dt,7,2))),9,2)
         |	then concat_ws('-',substr(tb.sys_settle_dt,1,4),substr(tb.sys_settle_dt,5,2),substr(tb.sys_settle_dt,7,2))
         |	else null
         |end as sys_settle_dt,
         |trim(tb.recncl_in) as recncl_in,
         |trim(tb.match_in) as match_in,
         |trim(tb.sec_ctrl_inf) as sec_ctrl_inf,
         |trim(tb.card_seq) as card_seq,
         |tb.dtl_inq_data as dtl_inq_data,
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
         |from (select * from viw_chacc_acc_trans_dtl where um_trans_id<>'AC02202000') ta
         |left join  (select * from viw_chacc_acc_trans_log
         |where um_trans_id<>'AC02202000') tb
         |on trim(ta.trans_tfr_tm)=trim(tb.trans_tfr_tm) and trim(ta.sys_tra_no)=trim(tb.sys_tra_no) and trim(ta.acpt_ins_id_cd)=trim(tb.acpt_ins_id_cd) and trim(ta.fwd_ins_id_cd)=trim(tb.fwd_ins_id_cd)
         |left join  ( select * from viw_chmgm_swt_log ) tc
         |on trim(ta.trans_tfr_tm)=trim(tc.tfr_dt_tm)  and  trim(ta.sys_tra_no)=trim(tc.sys_tra_no) and trim(ta.acpt_ins_id_cd)=trim(tc.acpt_ins_id_cd) and trim(ta.fwd_ins_id_cd)=trim(tc.msg_fwd_ins_id_cd)
         |left join
         |(
         |select
         |trim(tempc.trans_tfr_tm) as trans_tfr_tm,
         |trim(tempc.sys_tra_no) as sys_tra_no,
         |trim(tempc.acpt_ins_id_cd) as acpt_ins_id_cd,
         |trim(tempc.fwd_ins_id_cd) as fwd_ins_id_cd,
         |max(case when tempc.rank=2 then tempc.trans_proc_start_ts else null end) as second_trans_proc_start_ts,
         |max(case when tempc.rank=2 then tempc.trans_proc_start_ts else null end) as third_trans_proc_start_ts
         |from
         |(select
         |tempa.trans_tfr_tm as trans_tfr_tm,
         |tempa.sys_tra_no as sys_tra_no,
         |tempa.acpt_ins_id_cd as acpt_ins_id_cd,
         |tempa.fwd_ins_id_cd as fwd_ins_id_cd,
         |tempa.trans_proc_start_ts as trans_proc_start_ts,
         |row_number() over (order by tempa.trans_proc_start_ts) rank
         |from (select * from viw_chacc_acc_trans_dtl) tempa,
         |(select * from viw_chacc_acc_trans_log) tempb
         |where tempa.um_trans_id='AC02202000' and tempb.um_trans_id='AC02202000'
         |group by tempa.trans_tfr_tm,tempa.sys_tra_no,tempa.acpt_ins_id_cd,tempa.fwd_ins_id_cd,tempa.trans_proc_start_ts) tempc
         |group by tempc.trans_tfr_tm,tempc.sys_tra_no,tempc.acpt_ins_id_cd,tempc.fwd_ins_id_cd
         |)td
         |on trim(ta.trans_tfr_tm)=trim(td.trans_tfr_tm) and trim(ta.sys_tra_no)=trim(td.sys_tra_no) and trim(ta.acpt_ins_id_cd)=trim(td.acpt_ins_id_cd) and trim(ta.fwd_ins_id_cd)=trim(td.fwd_ins_id_cd)
         |
         | """.stripMargin)

    results.registerTempTable("spark_acc_trans")
    println("JOB_HV_4------>results:"+results.count())

    if(!Option(results).isEmpty){
      println("加载的表spark_acc_trans中有数据！")
    }else{
      println("加载的表spark_acc_trans中无数据！")
    }

    // Xue create function about partition by date ^_^
    def PartitionFun_JOB_HV_4(start_dt: String, end_dt: String)  {

      var sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val start = LocalDate.parse(start_dt, dateFormatter)
      val end = LocalDate.parse(end_dt, dateFormatter)
      val days = Days.daysBetween(start,end).getDays
      val dateStrs = for (day <- 0 to days) {
        val insertofTime = System.currentTimeMillis()
        val currentDay = (start.plusDays(day).toString(dateFormatter))
        println(s"=========插入'$currentDay'分区的数据=========")
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql(s"alter table hive_acc_trans drop partition (part_trans_dt='$currentDay')")
        println(s"alter table hive_acc_trans drop partition (part_trans_dt='$currentDay') successfully!")
        sqlContext.sql(s"insert into hive_acc_trans partition (part_trans_dt='$currentDay') select * from spark_acc_trans htempa where htempa.trans_dt = '$currentDay'")
        println(s"insert into hive_acc_trans partition (part_trans_dt='$currentDay') successfully!")
        val usedInsertofTime = System.currentTimeMillis() - insertofTime
        val ss:Int =((System.currentTimeMillis() - insertofTime)/1000).toInt
        val MM:Int = ss/60
        val hh:Int = MM/60
        val dd:Int = hh/24
        println("运行插入分区花费的时间是:"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedInsertofTime+"毫秒")

      }
    }

    PartitionFun_JOB_HV_4 (start_dt,end_dt)
    val usedTime = System.currentTimeMillis() - currntTime
    val ss:Int =((System.currentTimeMillis() - currntTime)/1000).toInt
    val MM:Int = ss/60
    val hh:Int = MM/60
    val dd:Int = hh/24
    println("运行 JOB_HV_4 花费 :"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedTime+"毫秒")

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
    val currntTime =System.currentTimeMillis()
    val df2_1 = sqlContext.readDB2_MGM(s"$schemas_mgmdb.TBL_CHMGM_STORE_TERM_RELATION")
    println("分区数为:" + {
      df2_1.rdd.getNumPartitions
    })
    df2_1.printSchema()
    df2_1.registerTempTable("TBL_CHMGM_STORE_TERM_RELATION")

    sqlContext.sql(s"use $hive_dbname")
    val HIVE_ACC_TRANS = sqlContext.sql(s"select * from HIVE_ACC_TRANS where part_trans_dt>='$start_dt' and part_trans_dt<='$end_dt'")
    HIVE_ACC_TRANS.registerTempTable("HIVE_ACC_TRANS")


    val results = sqlContext.sql(
      """
        |select
        |tempa.REC_ID AS REC_ID,
        |tempa.MCHNT_CD AS MCHNT_CD,
        |tempa.TERM_ID AS TERM_ID,
        |tempa.THIRD_PARTY_INS_FK AS THIRD_PARTY_INS_FK,
        |tempa.REC_UPD_USR_ID AS REC_UPD_USR_ID,
        |tempa.REC_UPD_TS AS REC_UPD_TS,
        |tempa.REC_CRT_USR_ID AS REC_CRT_USR_ID,
        |tempa.REC_CRT_TS AS REC_CRT_TS,
        |tempa.THIRD_PARTY_INS_ID AS THIRD_PARTY_INS_ID,
        |'0' AS IS_TRANS_TP
        |from
        |(
        |select
        |A.REC_ID AS REC_ID,
        |A.MCHNT_CD AS MCHNT_CD,
        |A.TERM_ID AS TERM_ID,
        |A.THIRD_PARTY_INS_FK AS THIRD_PARTY_INS_FK,
        |A.REC_UPD_USR_ID AS REC_UPD_USR_ID,
        |A.REC_UPD_TS AS REC_UPD_TS,
        |A.REC_CRT_USR_ID AS REC_CRT_USR_ID,
        |A.REC_CRT_TS AS REC_CRT_TS,
        |A.THIRD_PARTY_INS_ID AS THIRD_PARTY_INS_ID,
        |B.CARD_ACCPTR_CD AS CARD_ACCPTR_CD,
        |B.CARD_ACCPTR_TERM_ID AS CARD_ACCPTR_TERM_ID
        |from
        |TBL_CHMGM_STORE_TERM_RELATION A
        |LEFT JOIN
        |(
        |SELECT DISTINCT
        |CARD_ACCPTR_TERM_ID,
        |CARD_ACCPTR_CD
        |FROM
        |HIVE_ACC_TRANS
        |) B
        |ON A.MCHNT_CD = B.CARD_ACCPTR_CD AND A.TERM_ID = B.CARD_ACCPTR_TERM_ID
        |) tempa
        |where tempa.CARD_ACCPTR_CD is null AND tempa.CARD_ACCPTR_TERM_ID is null
        |
        |UNION ALL
        |
        |
        |select
        |tempb.REC_ID AS REC_ID,
        |tempb.MCHNT_CD AS MCHNT_CD,
        |tempb.TERM_ID AS TERM_ID,
        |tempb.THIRD_PARTY_INS_FK AS THIRD_PARTY_INS_FK,
        |tempb.REC_UPD_USR_ID AS REC_UPD_USR_ID,
        |tempb.REC_UPD_TS AS REC_UPD_TS,
        |tempb.REC_CRT_USR_ID AS REC_CRT_USR_ID,
        |tempb.REC_CRT_TS AS REC_CRT_TS,
        |tempb.THIRD_PARTY_INS_ID AS THIRD_PARTY_INS_ID,
        |'1' AS IS_TRANS_TP
        |from
        |(
        |select
        |A.REC_ID AS REC_ID,
        |A.MCHNT_CD AS MCHNT_CD,
        |A.TERM_ID AS TERM_ID,
        |A.THIRD_PARTY_INS_FK AS THIRD_PARTY_INS_FK,
        |A.REC_UPD_USR_ID AS REC_UPD_USR_ID,
        |A.REC_UPD_TS AS REC_UPD_TS,
        |A.REC_CRT_USR_ID AS REC_CRT_USR_ID,
        |A.REC_CRT_TS AS REC_CRT_TS,
        |A.THIRD_PARTY_INS_ID AS THIRD_PARTY_INS_ID,
        |B.CARD_ACCPTR_CD AS CARD_ACCPTR_CD,
        |B.CARD_ACCPTR_TERM_ID AS CARD_ACCPTR_TERM_ID
        |from
        |TBL_CHMGM_STORE_TERM_RELATION A
        |LEFT JOIN
        |(
        |SELECT DISTINCT
        |CARD_ACCPTR_TERM_ID,
        |CARD_ACCPTR_CD
        |FROM
        |HIVE_ACC_TRANS
        |) B
        |ON A.MCHNT_CD = B.CARD_ACCPTR_CD AND A.TERM_ID = B.CARD_ACCPTR_TERM_ID
        |) tempb
        |where tempb.CARD_ACCPTR_CD is not null  AND tempb.CARD_ACCPTR_TERM_ID is not null
        | """.stripMargin)
    results.registerTempTable("spark_tbl_chmgm_store_term_relation")

    println("JOB_HV_8------>results:"+results.count())

    if(!Option(results).isEmpty){
      results.registerTempTable("spark_tbl_chmgm_store_term_relation")
      sqlContext.sql(s"use $hive_dbname")
      sqlContext.sql("truncate table  HIVE_STORE_TERM_RELATION")
      sqlContext.sql("insert into table HIVE_STORE_TERM_RELATION select * from spark_tbl_chmgm_store_term_relation")
      println("insert into table HIVE_STORE_TERM_RELATION successfully!")
      val usedTime = System.currentTimeMillis() - currntTime
      val ss:Int =((System.currentTimeMillis() - currntTime)/1000).toInt
      val MM:Int = ss/60
      val hh:Int = MM/60
      val dd:Int = hh/24
      println("运行 JOB_HV_8 花费 :"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedTime+"毫秒")

    }else{
      println("加载的表spark_tbl_chmgm_store_term_relation中无数据！")
    }

  }


  /**
    * JOB_HV_9/08-23
    * hive_preferential_mchnt_inf->tbl_chmgm_preferential_mchnt_inf
    *
    * @author tzq
    * @param sqlContext
    * @return
    */
  def JOB_HV_9(implicit sqlContext: HiveContext) = {
    val currntTime =System.currentTimeMillis()
    println("###JOB_HV_9(hive_preferential_mchnt_inf --->tbl_chmgm_preferential_mchnt_inf)")
    val df = sqlContext.readDB2_ACC(s"$schemas_mgmdb.TBL_CHMGM_PREFERENTIAL_MCHNT_INF")
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
      sqlContext.sql(s"use $hive_dbname")
      sqlContext.sql("truncate table  hive_preferential_mchnt_inf")
      sqlContext.sql("insert into table hive_preferential_mchnt_inf select * from spark_tbl_chmgm_preferential_mchnt_inf")
      println("insert into table hive_preferential_mchnt_inf successfully!")
      val usedTime = System.currentTimeMillis() - currntTime
      val ss:Int =((System.currentTimeMillis() - currntTime)/1000).toInt
      val MM:Int = ss/60
      val hh:Int = MM/60
      val dd:Int = hh/24
      println("运行 JOB_HV_9 花费 :"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedTime+"毫秒")

    }else{
      println("加载的表tbl_chmgm_preferential_mchnt_inf中无数据！")
    }

  }


  /**
    * JOB_HV_10/08-23
    * hive_access_bas_inf->tbl_chmgm_access_bas_inf
    *
    * @author tzq
    * @param sqlContext
    * @return
    */
  def JOB_HV_10(implicit sqlContext: HiveContext) = {
    val currntTime =System.currentTimeMillis()

    println("###JOB_HV_10(hive_access_bas_inf->tbl_chmgm_access_bas_inf)")
    val df = sqlContext.readDB2_MGM(s"$schemas_mgmdb.TBL_CHMGM_ACCESS_BAS_INF")
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
      sqlContext.sql(s"use $hive_dbname")
      sqlContext.sql("truncate table  hive_access_bas_inf")
      sqlContext.sql("insert into table hive_access_bas_inf select * from spark_tbl_chmgm_access_bas_inf")
      println("insert into table hive_access_bas_inf successfully!")
      val usedTime = System.currentTimeMillis() - currntTime
      val ss:Int =((System.currentTimeMillis() - currntTime)/1000).toInt
      val MM:Int = ss/60
      val hh:Int = MM/60
      val dd:Int = hh/24
      println("运行 JOB_HV_10 花费 :"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedTime+"毫秒")

    }else{
      println("加载的表tbl_chmgm_access_bas_inf中无数据！")
    }
  }


  /**
    * JOB_HV_11/08-23
    * hive_ticket_bill_bas_inf->tbl_chacc_ticket_bill_bas_inf
    *
    * @author tzq
    * @param sqlContext
    * @return
    */
  def JOB_HV_11(implicit sqlContext: HiveContext) = {
    val currntTime =System.currentTimeMillis()

    println("###JOB_HV_11(hive_ticket_bill_bas_inf->tbl_chacc_ticket_bill_bas_inf)")
    val df = sqlContext.readDB2_ACC(s"$schemas_accdb.TBL_CHACC_TICKET_BILL_BAS_INF")

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
      sqlContext.sql(s"use $hive_dbname")
      sqlContext.sql("truncate table  hive_ticket_bill_bas_inf")
      sqlContext.sql("insert into table hive_ticket_bill_bas_inf select * from spark_db2_tbl_chacc_ticket_bill_bas_inf")
      println("insert into table hive_ticket_bill_bas_inf successfully!")
      val usedTime = System.currentTimeMillis() - currntTime
      val ss:Int =((System.currentTimeMillis() - currntTime)/1000).toInt
      val MM:Int = ss/60
      val hh:Int = MM/60
      val dd:Int = hh/24
      println("运行 JOB_HV_11 花费 :"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedTime+"毫秒")

    }else{
      println("加载的表TBL_CHMGM_TICKET_BILL_BAS_INF中无数据！")
    }
  }

  /**
    * JOB_HV_12/08-23
    * hive_chara_grp_def_bat->tbl_chmgm_chara_grp_def_bat
    *
    * @author tzq
    * @param sqlContext
    * @return
    */
  def JOB_HV_12(implicit sqlContext: HiveContext) = {
    val currntTime =System.currentTimeMillis()

    println("###JOB_HV_12(hive_chara_grp_def_bat->tbl_chmgm_chara_grp_def_bat)")
    val df = sqlContext.readDB2_MGM(s"$schemas_mgmdb.TBL_CHMGM_CHARA_GRP_DEF_BAT")
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
      sqlContext.sql(s"use $hive_dbname")
      sqlContext.sql("truncate table  hive_chara_grp_def_bat")
      sqlContext.sql("insert into table hive_chara_grp_def_bat select * from spark_tbl_chmgm_chara_grp_def_bat")
      println("insert into table hive_chara_grp_def_bat successfully!")
      val usedTime = System.currentTimeMillis() - currntTime
      val ss:Int =((System.currentTimeMillis() - currntTime)/1000).toInt
      val MM:Int = ss/60
      val hh:Int = MM/60
      val dd:Int = hh/24
      println("运行 JOB_HV_12 花费 :"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedTime+"毫秒")

    }else{
      println("加载的表tbl_chmgm_chara_grp_def_bat中无数据！")
    }

  }


  /**
    * hive-job-13/08-22
    * hive_card_bin->tbl_chmgm_card_bin
    *
    * @author tzq
    * @param sqlContext
    * @return
    */
  def JOB_HV_13(implicit sqlContext: HiveContext) = {
    val currntTime =System.currentTimeMillis()

    println("###JOB_HV_13(hive_card_bin->tbl_chmgm_card_bin)")
    val df = sqlContext.readDB2_MGM(s"$schemas_mgmdb.TBL_CHMGM_CARD_BIN")

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
      sqlContext.sql(s"use $hive_dbname")
      sqlContext.sql("truncate table  hive_card_bin")
      sqlContext.sql("insert into table hive_card_bin select * from spark_db2_tbl_chmgm_card_bin")
      println("insert into table hive_card_bin successfully!")
      val usedTime = System.currentTimeMillis() - currntTime
      val ss:Int =((System.currentTimeMillis() - currntTime)/1000).toInt
      val MM:Int = ss/60
      val hh:Int = MM/60
      val dd:Int = hh/24
      println("运行 JOB_HV_13 花费 :"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedTime+"毫秒")

    }else{
      println("加载的表TBL_CHMGM_CARD_BIN中无数据！")
    }
  }


  /**
    * hive-job-14/08-22
    * hive_inf_source_dtl->tbl_inf_source_dtl
    *
    * @author tzq
    * @param sqlContext
    * @return
    */
  def JOB_HV_14(implicit sqlContext: HiveContext) = {
    val currntTime =System.currentTimeMillis()

    println("###JOB_HV_14(hive_inf_source_dtl->tbl_inf_source_dtl)")
    val df = sqlContext.readDB2_ACC(s"$schemas_accdb.TBL_INF_SOURCE_DTL")
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
      sqlContext.sql(s"use $hive_dbname")
      sqlContext.sql("truncate table  hive_inf_source_dtl")
      sqlContext.sql("insert into table hive_inf_source_dtl select * from spark_db2_tbl_inf_source_dtl")
      println("insert into table hive_inf_source_dtl successfully!")
      val usedTime = System.currentTimeMillis() - currntTime
      val ss:Int =((System.currentTimeMillis() - currntTime)/1000).toInt
      val MM:Int = ss/60
      val hh:Int = MM/60
      val dd:Int = hh/24
      println("运行 JOB_HV_14 花费 :"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedTime+"毫秒")

    }else{
      println("加载的表TBL_INF_SOURCE_DTL中无数据！")
    }

  }

  /**
    * hive-job-15
    * hive_undefine_store_inf-->  hive_acc_trans + hive_store_term_relation
    *
    * @author tzq
    * @param sqlContext
    * @return
    *
    */
  def JOB_HV_15(implicit sqlContext: HiveContext) = {
    val currntTime =System.currentTimeMillis()

    println("###JOB_HV_15(hive_undefine_store_inf-->  hive_acc_trans + hive_store_term_relation)")
    sqlContext.sql(s"use $hive_dbname")
    val results = sqlContext.sql(
      """
        |select
        |c.card_accptr_cd as mchnt_cd,
        |c.card_accptr_term_id as term_id,
        |nvl(store_cd,null) as store_cd,
        |nvl(store_grp_cd,null) as store_grp_cd,
        |nvl(brand_id,0) as brand_id
        |from
        |(select
        |a.card_accptr_cd,a.card_accptr_term_id
        |from
        |(select
        |card_accptr_term_id,
        |card_accptr_cd
        |from hive_acc_trans
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

    if(!Option(results).isEmpty){
      results.registerTempTable("hive_undefine_store_inf_temp")
      sqlContext.sql("truncate table  hive_undefine_store_inf")
      sqlContext.sql("insert into table hive_undefine_store_inf select * from hive_undefine_store_inf_temp")
      println("insert into table hive_undefine_store_inf successfully!")
      val usedTime = System.currentTimeMillis() - currntTime
      val ss:Int =((System.currentTimeMillis() - currntTime)/1000).toInt
      val MM:Int = ss/60
      val hh:Int = MM/60
      val dd:Int = hh/24
      println("运行 JOB_HV_15 花费 :"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedTime+"毫秒")

    }else{
      println("没有数据插入到指定表hive_undefine_store_inf ！")
    }
  }

  /**
    * JOB_HV_16/08-23
    * hive_mchnt_inf_wallet->tbl_chmgm_mchnt_inf/TBL_CHMGM_STORE_TERM_RELATION/TBL_CHMGM_ACCESS_BAS_INF
    *
    * @author tzq
    * @param sqlContext
    * @return
    */
  def JOB_HV_16(implicit sqlContext: HiveContext) = {
    val currntTime =System.currentTimeMillis()

    println("###JOB_HV_16(hive_mchnt_inf_wallet->tbl_chmgm_mchnt_inf/TBL_CHMGM_STORE_TERM_RELATION/TBL_CHMGM_ACCESS_BAS_INF)")
    val df = sqlContext.readDB2_MGM(s"$schemas_mgmdb.TBL_CHMGM_MCHNT_INF")
    df.registerTempTable("db2_tbl_chmgm_mchnt_inf")

    val df1 = sqlContext.readDB2_MGM(s"$schemas_mgmdb.TBL_CHMGM_STORE_TERM_RELATION")
    df1.registerTempTable("db2_tbl_chmgm_store_term_relation")

    val df2 = sqlContext.readDB2_MGM(s"$schemas_mgmdb.TBL_CHMGM_ACCESS_BAS_INF")
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

    println("JOB_HV_16-----results:"+results.count())

    if(!Option(results).isEmpty){
      results.registerTempTable("spark_tbl_chmgm_mchnt_inf")
      sqlContext.sql(s"use $hive_dbname")
      sqlContext.sql("truncate table  hive_mchnt_inf_wallet")
      sqlContext.sql("insert into table hive_mchnt_inf_wallet select * from spark_tbl_chmgm_mchnt_inf")
      println("insert into table hive_mchnt_inf_wallet successfully!")
      val usedTime = System.currentTimeMillis() - currntTime
      val ss:Int =((System.currentTimeMillis() - currntTime)/1000).toInt
      val MM:Int = ss/60
      val hh:Int = MM/60
      val dd:Int = hh/24
      println("运行 JOB_HV_16 花费 :"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedTime+"毫秒")

    }else{
      println("指定表hive_mchnt_inf_wallet无数据插入！")
    }

  }


  /**
    * hive-job-18 2016-08-26
    * viw_chmgm_trans_his -> hive_download_trans
    *
    * @author winslow yang
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    */
  def JOB_HV_18(implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {
    println("###JOB_HV_18(viw_chmgm_trans_his -> hive_download_trans)")

    val start_day = start_dt.replace("-","")
    val end_day = end_dt.replace("-","")
    val currntTime =System.currentTimeMillis()

    val df = sqlContext.readDB2_MGM_4para(s"$schemas_mgmdb.VIW_CHMGM_TRANS_HIS","trans_dt",s"$start_day",s"$end_day")
    df.registerTempTable("db2_trans_his")

    sqlContext.sql(s"use $hive_dbname")
    val results = sqlContext.sql(
      s"""
         |select
         |	seq_id,
         |	trim(cdhd_usr_id)    as cdhd_usr_id,			trim(pri_acct_no)    as pri_acct_no,
         |	trim(acpt_ins_id_cd) as acpt_ins_id_cd,		trim(fwd_ins_id_cd)  as fwd_ins_id_cd,
         |	trim(sys_tra_no)     as sys_tra_no,				trim(tfr_dt_tm)      as tfr_dt_tm,
         |	trim(rcv_ins_id_cd)  as rcv_ins_id_cd,		onl_trans_tra_no,
         |	trim(mchnt_cd)       as mchnt_cd,					mchnt_nm,
         |	case
         |		when
         |			substr(trans_dt,1,4) between '0001' and '9999' and substr(trans_dt,5,2) between '01' and '12' and
         |			substr(trans_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(trans_dt,1,4),substr(trans_dt,5,2),substr(trans_dt,7,2))),9,2)
         |		then concat_ws('-',substr(trans_dt,1,4),substr(trans_dt,5,2),substr(trans_dt,7,2))
         |		else null
         |	end as trans_dt,
         |	trim(trans_tm) as trans_tm,
         |	case
         |  when length(trim(translate(trim(trans_at),'0123456789',' ')))=0 then trans_at
         |		else null
         |	end as trans_at,
         |	trim(buss_tp) 			as buss_tp,						trim(um_trans_id) 	as um_trans_id,
         |	trim(swt_right_tp) 	as swt_right_tp,			trim(swt_right_nm) 	as swt_right_nm,
         |	trim(bill_id) as bill_id,									bill_nm,
         |	trim(chara_acct_tp) as chara_acct_tp,			point_at,
         |	bill_num,																	trim(chara_acct_nm) as chara_acct_nm,
         |	plan_id,																	trim(sys_det_cd) 		as sys_det_cd,
         |	trim(acct_addup_tp) as acct_addup_tp,			remark,
         |	trim(bill_item_id) 	as bill_item_id,			trim(trans_st) 			as trans_st,
         |	case
         |   when length(trim(translate(trim(discount_at),'0123456789',' ')))=0 then discount_at
         |		else null
         |	end as discount_at,
         |	trim(booking_st) 		as booking_st,				plan_nm,
         |	trim(bill_acq_md) 	as bill_acq_md,				trim(oper_in) 			as oper_in,
         |	rec_crt_ts,					rec_upd_ts,						trim(term_id) 			as term_id,
         |	trim(pos_entry_md_cd) as pos_entry_md_cd,	orig_data_elemnt,
         |	udf_fld,																	trim(card_accptr_nm_addr) as card_accptr_nm_addr,
         | trim(token_card_no) as token_card_no
         |from
         | db2_trans_his
         |where
         | trim(um_trans_id) in ('12','17')
      """.stripMargin)
    results.registerTempTable("spark_trans_his")

    println("###JOB_HV_18------>results:"+results.count())

    def InsertPart_JOB_HV_18(start_dt: String, end_dt: String)  {

      var sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val start = LocalDate.parse(start_dt, dateFormatter)
      val end = LocalDate.parse(end_dt, dateFormatter)
      val days = Days.daysBetween(start, end).getDays
      val dateStrs = for (day <- 0 to days) {
        val insertofTime = System.currentTimeMillis()
        val currentDay = (start.plusDays(day).toString(dateFormatter))
        println(s"=========插入'$currentDay'分区的数据=========")

        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql(s"alter table hive_download_trans drop partition (part_trans_dt='$currentDay')")
        println(s"alter table hive_download_trans drop partition (part_trans_dt='$currentDay') successfully!")

        sqlContext.sql(
          s"""
             |insert into hive_download_trans partition (part_trans_dt='$currentDay')
             |select * from spark_trans_his where trans_dt = '$currentDay'
           """.stripMargin)
        println(s"insert into hive_download_trans partition (part_trans_dt='$currentDay') successfully!")
        val usedInsertofTime = System.currentTimeMillis() - insertofTime
        val ss:Int =((System.currentTimeMillis() - insertofTime)/1000).toInt
        val MM:Int = ss/60
        val hh:Int = MM/60
        val dd:Int = hh/24
        println("运行插入分区花费的时间是:"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedInsertofTime+"毫秒")

      }
    }

    InsertPart_JOB_HV_18 (start_dt,end_dt)
    val usedTime = System.currentTimeMillis() - currntTime
    val ss:Int =((System.currentTimeMillis() - currntTime)/1000).toInt
    val MM:Int = ss/60
    val hh:Int = MM/60
    val dd:Int = hh/24
    println("运行 JOB_HV_18 花费 :"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedTime+"毫秒")

  }

  /**
    * hive-job-19
    * TBL_CHMGM_INS_INF -> HIVE_INS_INF
    *
    * @author winslow yang
    * @param sqlContext
    */
  def JOB_HV_19(implicit sqlContext: HiveContext) = {
    val currntTime =System.currentTimeMillis()
    println("###JOB_HV_19(tbl_chmgm_ins_inf -> hive_ins_inf)")
    sqlContext.sql(s"use $hive_dbname")
    val df = sqlContext.readDB2_MGM(s"$schemas_mgmdb.TBL_CHMGM_INS_INF")
    df.registerTempTable("db2_ins_inf")

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
        |from db2_ins_inf
      """.stripMargin
    )
    println("JOB_HV_19------>results:"+results.count())
    if(!Option(results).isEmpty){
      results.registerTempTable("spark_ins_inf")
      sqlContext.sql("truncate table hive_ins_inf")
      sqlContext.sql("insert into table hive_ins_inf select * from spark_ins_inf")
      println("insert into table hive_ins_inf successfully!")
      val usedTime = System.currentTimeMillis() - currntTime
      val ss:Int =((System.currentTimeMillis() - currntTime)/1000).toInt
      val MM:Int = ss/60
      val hh:Int = MM/60
      val dd:Int = hh/24
      println("运行 JOB_HV_19 花费 :"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedTime+"毫秒")

    }else{
      println("加载的表HIVE_INS_INF中无数据！")
    }
  }


  /**
    * hive-job-20-ini-1 2016年10月31日
    * tbl_chmgm_term_inf -> hive_term_inf
    *
    * @author winslow yang
    * @para sqlContext
    * */
  def JOB_HV_20_INI_I(implicit sqlContext: HiveContext) = {
    val currntTime =System.currentTimeMillis()

    println("####JOB_HV_20_INI_I(tbl_chmgm_term_inf -> hive_term_inf)")

    val df = sqlContext.readDB2_MGM(s"$schemas_mgmdb.TBL_CHMGM_TERM_INF")
    df.registerTempTable("spark_tbl_chmgm_term_inf")

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
          |spark_tbl_chmgm_term_inf
  """.stripMargin)
    println("####JOB_HV_20_INI_I------>results: " + results.count())

    if(!Option(results).isEmpty){
      results.registerTempTable("spark_hive_term_inf")
      sqlContext.sql(s"use $hive_dbname")
      sqlContext.sql("truncate table hive_term_inf")
      println("#### truncate table hive_term_inf successful ####")
      sqlContext.sql(s"insert into table hive_term_inf select * from spark_hive_term_inf")
      println("#### insert into table hive_term_inf successful ####")
      val usedTime = System.currentTimeMillis() - currntTime
      val ss:Int =((System.currentTimeMillis() - currntTime)/1000).toInt
      val MM:Int = ss/60
      val hh:Int = MM/60
      val dd:Int = hh/24
      println("运行 JOB_HV_ 花费 :"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedTime+"毫秒")

    } else {
      println("#### 加载的表TBL_CHMGM_TERM_INF中无数据 ####")
    }
  }


  /**
    * hive-job-23  2016年10月9日
    * hive_brand_inf-->TBL_CHMGM_BRAND_INF
    *
    * @author tzq
    * @param sqlContext
    * @return
    */
  def JOB_HV_23(implicit sqlContext: HiveContext) = {
    val currntTime =System.currentTimeMillis()

    println("###JOB_HV_23(hive_brand_inf->tbl_chmgm_brand_inf)")

    val df = sqlContext.readDB2_MGM(s"$schemas_mgmdb.TBL_CHMGM_BRAND_INF")
    df.registerTempTable("db2_tbl_chmgm_brand_inf")
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
    println("###JOB_HV_23---------->results:"+results.count())

    if(!Option(results).isEmpty){
      results.registerTempTable("spark_db2_tbl_chmgm_brand_inf")
      sqlContext.sql(s"use $hive_dbname")
      sqlContext.sql("truncate table  hive_brand_inf")
      sqlContext.sql("insert into table hive_brand_inf select * from spark_db2_tbl_chmgm_brand_inf")
      println("insert into table hive_brand_inf successfully!")
      val usedTime = System.currentTimeMillis() - currntTime
      val ss:Int =((System.currentTimeMillis() - currntTime)/1000).toInt
      val MM:Int = ss/60
      val hh:Int = MM/60
      val dd:Int = hh/24
      println("运行 JOB_HV_23 花费 :"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedTime+"毫秒")

    }else{
      println("加载的表TBL_CHMGM_BRAND_INF中无数据！")
    }

  }


  /**
    * hive-job-24 2016-09-18
    * tbl_chmgm_mchnt_para -> hive_mchnt_para
    *
    * @author winslow yang
    * @param sqlContext
    * @return
    */
  def JOB_HV_24(implicit sqlContext: HiveContext) = {
    val currntTime =System.currentTimeMillis()

    println("###JOB_HV_24(tbl_chmgm_mchnt_para -> hive_mchnt_para)")

    sqlContext.sql(s"use $hive_dbname")
    val df = sqlContext.readDB2_MGM(s"$schemas_mgmdb.TBL_CHMGM_MCHNT_PARA")
    df.registerTempTable("db2_mchnt_para")
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
        |db2_mchnt_para
      """.stripMargin
    )

    println("JOB_HV_24------>results:"+results.count())
    if(!Option(results).isEmpty){
      results.registerTempTable("spark_mchnt_para")
      sqlContext.sql("truncate table hive_mchnt_para")
      sqlContext.sql("insert into table hive_mchnt_para select * from spark_mchnt_para")
      println("insert into table hive_mchnt_para successfully!")
      val usedTime = System.currentTimeMillis() - currntTime
      val ss:Int =((System.currentTimeMillis() - currntTime)/1000).toInt
      val MM:Int = ss/60
      val hh:Int = MM/60
      val dd:Int = hh/24
      println("运行 JOB_HV_ 花费 :"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedTime+"毫秒")

    }else{
      println("加载的表tbl_chmgm_mchnt_para中无数据！")
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
    val currntTime =System.currentTimeMillis()

    val df2_1 = sqlContext.readDB2_MGM(s"$schemas_mgmdb.tbl_mcmgm_mchnt_tp")

    df2_1.registerTempTable("tbl_mcmgm_mchnt_tp")
    val results = sqlContext.sql(
      """
        |select
        |trim(ta.MCHNT_TP) as MCHNT_TP,
        |trim(ta.MCHNT_TP_GRP) as MCHNT_TP_GRP,
        |ta.MCHNT_TP_DESC_CN as MCHNT_TP_DESC_CN,
        |ta.MCHNT_TP_DESC_EN as MCHNT_TP_DESC_EN,
        |ta.REC_ID as REC_ID,
        |ta.REC_ST as REC_ST,
        |ta.MCC_TYPE as MCC_TYPE,
        |ta.LAST_OPER_IN as LAST_OPER_IN,
        |trim(ta.REC_UPD_USR_ID) as REC_UPD_USR_ID,
        |ta.REC_UPD_TS as REC_UPD_TS,
        |ta.REC_CRT_TS as REC_CRT_TS,
        |ta.SYNC_ST as SYNC_ST,
        |ta.SYNC_BAT_NO as SYNC_BAT_NO,
        |ta.SYNC_TS as SYNC_TS
        |
        |from TBL_MCMGM_MCHNT_TP ta
        |
        | """.stripMargin)

    results.registerTempTable("spark_hive_mchnt_tp")
    println("JOB_HV_25------>results:"+results.count())

    if(!Option(results).isEmpty){
      results.registerTempTable("spark_hive_mchnt_tp")
      sqlContext.sql(s"use $hive_dbname")
      sqlContext.sql("truncate table  HIVE_MCHNT_TP")
      sqlContext.sql("insert into table HIVE_MCHNT_TP select * from spark_hive_mchnt_tp")
      println("insert into table HIVE_MCHNT_TP successfully!")
      val usedTime = System.currentTimeMillis() - currntTime
      val ss:Int =((System.currentTimeMillis() - currntTime)/1000).toInt
      val MM:Int = ss/60
      val hh:Int = MM/60
      val dd:Int = hh/24
      println("运行 JOB_HV_25 花费 :"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedTime+"毫秒")

    }else{
      println("加载的表spark_hive_mchnt_tp中无数据！")
    }
  }

  /**
    * hive-job-26
    * hive_mchnt_tp_grp-->tbl_mcmgm_mchnt_tp_grp
    *
    * @author tzq
    * @param sqlContext
    * @return
    */
  def JOB_HV_26(implicit sqlContext: HiveContext) = {
    val currntTime =System.currentTimeMillis()

    println("###JOB_HV_26(hive_mchnt_tp_grp-->tbl_mcmgm_mchnt_tp_grp)")
    val df = sqlContext.readDB2_MGM(s"$schemas_mgmdb.TBL_MCMGM_MCHNT_TP_GRP")
    df.registerTempTable("db2_tbl_mcmgm_mchnt_tp_grp")
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
    println("###JOB_HV_26---------->results:"+results.count())

    if(!Option(results).isEmpty){
      results.registerTempTable("spark_db2_tbl_mcmgm_mchnt_tp_grp")
      sqlContext.sql(s"use $hive_dbname")
      sqlContext.sql("truncate table  hive_mchnt_tp_grp")
      sqlContext.sql("insert into table hive_mchnt_tp_grp select * from spark_db2_tbl_mcmgm_mchnt_tp_grp")
      println("insert into table hive_mchnt_tp_grp successfully!")
      val usedTime = System.currentTimeMillis() - currntTime
      val ss:Int =((System.currentTimeMillis() - currntTime)/1000).toInt
      val MM:Int = ss/60
      val hh:Int = MM/60
      val dd:Int = hh/24
      println("运行 JOB_HV_26 花费 :"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedTime+"毫秒")

    }else{
      println("加载的表TBL_MCMGM_MCHNT_TP_GRP中无数据！")
    }

  }


  /**
    * JOB_HV_27/10-28
    * hive_serach_trans->VIW_CHACC_ACC_TRANS_LOG,VIW_CHMGM_SWT_LOG
    * Code by Xue
    *
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    */
  def JOB_HV_27 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {
    val currntTime =System.currentTimeMillis()

    val start_day = start_dt.replace("-","")
    val end_day = end_dt.replace("-","")

    val df2_1 = sqlContext.readDB2_ACC_4para(s"$schemas_accdb.VIW_CHACC_ACC_TRANS_LOG","trans_dt",s"$start_day",s"$end_day")
    df2_1.registerTempTable("VIW_CHACC_ACC_TRANS_LOG")

    val df2_2 = sqlContext.readDB2_MGM_4para(s"$schemas_mgmdb.VIW_CHMGM_SWT_LOG","MSG_SETTLE_DT",s"$start_day",s"$end_day")
    df2_2.registerTempTable("VIW_CHMGM_SWT_LOG")


    val results = sqlContext.sql(
      s"""
         |SELECT
         |NVL(B.TFR_DT_TM,A.TRANS_TFR_TM) as TFR_DT_TM,
         |NVL(B.SYS_TRA_NO,A.SYS_TRA_NO) as SYS_TRA_NO,
         |NVL(B.ACPT_INS_ID_CD,A.ACPT_INS_ID_CD) as ACPT_INS_ID_CD,
         |NVL(B.MSG_FWD_INS_ID_CD,A.FWD_INS_ID_CD) as FWD_INS_ID_CD,
         |B.PRI_KEY1 as PRI_KEY1,
         |B.FWD_CHNL_HEAD as FWD_CHNL_HEAD,
         |B.CHSWT_PLAT_SEQ as CHSWT_PLAT_SEQ,
         |trim(B.TRANS_TM) as TRANS_TM,
         |case when
         |substr(trim(B.TRANS_DT),1,4) between '0001' and '9999' and substr(trim(B.TRANS_DT),5,2) between '01' and '12' and
         |substr(trim(B.TRANS_DT),7,2) between '01' and substr(last_day(concat_ws('-',substr(trim(B.TRANS_DT),1,4),substr(trim(B.TRANS_DT),5,2),substr(trim(B.TRANS_DT),7,2))),9,2)
         |then concat_ws('-',substr(B.TRANS_DT,1,4),substr(B.TRANS_DT,5,2),substr(B.TRANS_DT,7,2))
         |else null end as TRANS_DT,
         |case when
         |substr(NVL(trim(B.CSWT_SETTLE_DT),trim(A.SYS_SETTLE_DT)) ,1,4) between '0001' and '9999' and substr(NVL(trim(B.CSWT_SETTLE_DT),trim(A.SYS_SETTLE_DT)),5,2) between '01' and '12' and
         |substr(NVL(trim(B.CSWT_SETTLE_DT),trim(A.SYS_SETTLE_DT)),7,2) between '01' and substr(last_day(concat_ws('-',substr(NVL(trim(B.CSWT_SETTLE_DT),trim(A.SYS_SETTLE_DT)),1,4),substr(NVL(trim(B.CSWT_SETTLE_DT),trim(A.SYS_SETTLE_DT)),5,2),substr(NVL(trim(B.CSWT_SETTLE_DT),trim(A.SYS_SETTLE_DT)),7,2))),9,2)
         |then concat_ws('-',substr(NVL(trim(B.CSWT_SETTLE_DT),trim(A.SYS_SETTLE_DT)),1,4),substr(NVL(trim(B.CSWT_SETTLE_DT),trim(A.SYS_SETTLE_DT)),5,2),substr(NVL(trim(B.CSWT_SETTLE_DT),trim(A.SYS_SETTLE_DT)),7,2))
         |else null end as CSWT_SETTLE_DT,
         |trim(B.INTERNAL_TRANS_TP) as INTERNAL_TRANS_TP,
         |trim(B.SETTLE_TRANS_ID) as SETTLE_TRANS_ID,
         |trim(B.TRANS_TP) as TRANS_TP,
         |trim(B.CUPS_SETTLE_DT) as CUPS_SETTLE_DT,
         |NVL(B.MSG_TP,A.MSG_TP) as MSG_TP,
         |trim(B.PRI_ACCT_NO) as PRI_ACCT_NO,
         |trim(B.CARD_BIN) as CARD_BIN,
         |NVL(B.PROC_CD,A.PROC_CD) as PROC_CD,
         |B.REQ_TRANS_AT as REQ_TRANS_AT,
         |B.RESP_TRANS_AT as RESP_TRANS_AT,
         |NVL(B.TRANS_CURR_CD,A.TRANS_CURR_CD) as TRANS_CURR_CD,
         |B.TRANS_TOT_AT as TRANS_TOT_AT,
         |trim(B.ISS_INS_ID_CD) as ISS_INS_ID_CD,
         |trim(B.LAUNCH_TRANS_TM) as LAUNCH_TRANS_TM,
         |trim(B.LAUNCH_TRANS_DT) as LAUNCH_TRANS_DT,
         |NVL(B.MCHNT_TP,A.MCHNT_TP) as MCHNT_TP,
         |trim(B.POS_ENTRY_MD_CD) as POS_ENTRY_MD_CD,
         |NVL(B.CARD_SEQ_ID,A.CARD_SEQ) as CARD_SEQ_ID,
         |trim(B.POS_COND_CD) as POS_COND_CD,
         |NVL(B.POS_PIN_CAPTURE_CD,A.POS_PIN_CAPTURE_CD) as POS_PIN_CAPTURE_CD,
         |NVL(B.RETRI_REF_NO,A.RETRI_REF_NO) as RETRI_REF_NO,
         |NVL(B.TERM_ID,A.CARD_ACCPTR_TERM_ID) as TERM_ID,
         |NVL(B.MCHNT_CD,A.CARD_ACCPTR_CD) as MCHNT_CD,
         |NVL(B.CARD_ACCPTR_NM_LOC,A.CARD_ACCPTR_NM_ADDR) as CARD_ACCPTR_NM_LOC,
         |NVL(B.SEC_RELATED_CTRL_INF,A.SEC_CTRL_INF) as SEC_RELATED_CTRL_INF,
         |NVL(B.ORIG_DATA_ELEMTS,A.ORIG_DATA_ELEMNT) as ORIG_DATA_ELEMTS,
         |NVL(B.RCV_INS_ID_CD,A.RCV_INS_ID_CD) as RCV_INS_ID_CD,
         |trim(B.FWD_PROC_IN) as FWD_PROC_IN,
         |trim(B.RCV_PROC_IN) as RCV_PROC_IN,
         |trim(B.PROJ_TP) as PROJ_TP,
         |B.USR_ID as USR_ID,
         |B.CONV_USR_ID as CONV_USR_ID,
         |trim(B.TRANS_ST) as TRANS_ST,
         |B.INQ_DTL_REQ as INQ_DTL_REQ,
         |B.INQ_DTL_RESP as INQ_DTL_RESP,
         |B.ISS_INS_RESV as ISS_INS_RESV,
         |B.IC_FLDS as IC_FLDS,
         |B.CUPS_DEF_FLD as CUPS_DEF_FLD,
         |trim(B.ID_NO) as ID_NO,
         |B.CUPS_RESV as CUPS_RESV,
         |B.ACPT_INS_RESV as ACPT_INS_RESV,
         |trim(B.ROUT_INS_ID_CD) as ROUT_INS_ID_CD,
         |trim(B.SUB_ROUT_INS_ID_CD) as SUB_ROUT_INS_ID_CD,
         |trim(B.RECV_ACCESS_RESP_CD) as RECV_ACCESS_RESP_CD,
         |trim(B.CHSWT_RESP_CD) as CHSWT_RESP_CD,
         |trim(B.CHSWT_ERR_CD) as CHSWT_ERR_CD,
         |B.RESV_FLD1 as RESV_FLD1,
         |B.RESV_FLD2 as RESV_FLD2,
         |B.TO_TS as TO_TS,
         |NVL(B.REC_UPD_TS,A.REC_UPD_TS) as REC_UPD_TS,
         |B.REC_CRT_TS as REC_CRT_TS,
         |NVL(B.SETTLE_AT,A.SETTLE_AT) as SETTLE_AT,
         |B.EXTERNAL_AMT as EXTERNAL_AMT,
         |B.DISCOUNT_AT as DISCOUNT_AT,
         |B.CARD_PAY_AT as CARD_PAY_AT,
         |B.RIGHT_PURCHASE_AT as RIGHT_PURCHASE_AT,
         |trim(B.RECV_SECOND_RESP_CD) as RECV_SECOND_RESP_CD,
         |B.REQ_ACPT_INS_RESV as REQ_ACPT_INS_RESV,
         |trim(B.LOG_ID) as LOG_ID,
         |trim(B.CONV_ACCT_NO) as CONV_ACCT_NO,
         |trim(B.INNER_PRO_IND) as INNER_PRO_IND,
         |trim(B.ACCT_PROC_IN) as ACCT_PROC_IN,
         |B.ORDER_ID as ORDER_ID,
         |A.SEQ_ID as SEQ_ID,
         |trim(A.OPER_MODULE) as OPER_MODULE,
         |trim(A.UM_TRANS_ID) as UM_TRANS_ID,
         |trim(A.CDHD_FK) as CDHD_FK,
         |trim(A.BILL_ID) as BILL_ID,
         |trim(A.BILL_TP) as BILL_TP,
         |trim(A.BILL_BAT_NO) as BILL_BAT_NO,
         |A.BILL_INF as BILL_INF,
         |trim(A.CARD_NO) as CARD_NO,
         |case
         |	when length((translate(trim(A.TRANS_AT),'-0123456789',' ')))=0 then trim(A.TRANS_AT)
         |	else null
         |end as TRANS_AT,
         |trim(A.SETTLE_CURR_CD) as SETTLE_CURR_CD,
         |trim(A.CARD_ACCPTR_LOCAL_TM) as CARD_ACCPTR_LOCAL_TM,
         |trim(A.CARD_ACCPTR_LOCAL_DT) as CARD_ACCPTR_LOCAL_DT,
         |trim(A.EXPIRE_DT) as EXPIRE_DT,
         |case when
         |substr(trim(A.MSG_SETTLE_DT),1,4) between '0001' and '9999' and substr(trim(A.MSG_SETTLE_DT),5,2) between '01' and '12' and
         |substr(trim(A.MSG_SETTLE_DT),7,2) between '01' and substr(last_day(concat_ws('-',substr(trim(A.MSG_SETTLE_DT),1,4),substr(trim(A.MSG_SETTLE_DT),5,2),substr(trim(A.MSG_SETTLE_DT),7,2))),9,2)
         |then concat_ws('-',substr(trim(A.MSG_SETTLE_DT),1,4),substr(trim(A.MSG_SETTLE_DT),5,2),substr(trim(A.MSG_SETTLE_DT),7,2))
         |else null end as MSG_SETTLE_DT,
         |trim(A.AUTH_ID_RESP_CD) as AUTH_ID_RESP_CD,
         |trim(A.RESP_CD) as RESP_CD,
         |trim(A.NOTIFY_ST) as NOTIFY_ST,
         |A.ADDN_PRIVATE_DATA as ADDN_PRIVATE_DATA,
         |A.UDF_FLD as UDF_FLD,
         |trim(A.ADDN_AT) as ADDN_AT,
         |A.ACCT_ID_1 as ACCT_ID_1,
         |A.ACCT_ID_2 as ACCT_ID_2,
         |A.RESV_FLD as RESV_FLD,
         |A.CDHD_AUTH_INF as CDHD_AUTH_INF,
         |trim(A.RECNCL_IN) as RECNCL_IN,
         |trim(A.MATCH_IN) as MATCH_IN,
         |A.TRANS_PROC_START_TS as TRANS_PROC_START_TS,
         |A.TRANS_PROC_END_TS as TRANS_PROC_END_TS,
         |trim(A.SYS_DET_CD) as SYS_DET_CD,
         |trim(A.SYS_ERR_CD) as SYS_ERR_CD,
         |A.DTL_INQ_DATA as DTL_INQ_DATA
         |FROM
         |(select * from VIW_CHACC_ACC_TRANS_LOG where UM_TRANS_ID='AC02003065' ) A
         |FULL JOIN (SELECT * FROM VIW_CHMGM_SWT_LOG  WHERE SETTLE_TRANS_ID='S38') B
         |on A.TRANS_TFR_TM=B.TFR_DT_TM and A.SYS_TRA_NO=B.SYS_TRA_NO and A.ACPT_INS_ID_CD=B.ACPT_INS_ID_CD and A.FWD_INS_ID_CD=B.MSG_FWD_INS_ID_CD
         |
         | """.stripMargin)

    results.registerTempTable("spark_hive_search_trans")
    println("JOB_HV_27------>results:"+results.count())

    if(!Option(results).isEmpty){
      println("加载的表spark_hive_search_trans中有数据！")
    }else{
      println("加载的表spark_hive_search_trans中无数据！")
    }

    // Xue create function about partition by date ^_^
    def PartitionFun_JOB_HV_27(start_dt: String, end_dt: String)  {
      var sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val start = LocalDate.parse(start_dt, dateFormatter)
      val end = LocalDate.parse(end_dt, dateFormatter)
      val days = Days.daysBetween(start, end).getDays
      val dateStrs = for (day <- 0 to days) {
        val insertofTime = System.currentTimeMillis()

        val currentDay = (start.plusDays(day).toString(dateFormatter))
        println(s"=========插入'$currentDay'分区的数据=========")
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql(s"alter table hive_search_trans drop partition (part_settle_dt='$currentDay')")
        println(s"alter table hive_search_trans drop partition (part_settle_dt='$currentDay') successfully!")
        sqlContext.sql(s"insert into hive_search_trans partition (part_settle_dt='$currentDay') select * from spark_hive_search_trans htempa where htempa.MSG_SETTLE_DT = '$currentDay'")
        println(s"insert into hive_search_trans partition (part_settle_dt='$currentDay') successfully!")
        val usedInsertofTime = System.currentTimeMillis() - insertofTime
        val ss:Int =((System.currentTimeMillis() - insertofTime)/1000).toInt
        val MM:Int = ss/60
        val hh:Int = MM/60
        val dd:Int = hh/24
        println("运行插入分区花费的时间是:"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedInsertofTime+"毫秒")

      }
    }

    PartitionFun_JOB_HV_27 (start_dt,end_dt)
    val usedTime = System.currentTimeMillis() - currntTime
    val ss:Int =((System.currentTimeMillis() - currntTime)/1000).toInt
    val MM:Int = ss/60
    val hh:Int = MM/60
    val dd:Int = hh/24
    println("运行 JOB_HV_27 花费 :"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedTime+"毫秒")

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
    val currntTime =System.currentTimeMillis()
    val start_day = start_dt.replace("-","")
    val end_day = end_dt.replace("-","")

    val df2_1 = sqlContext.readDB2_ACC_4para(s"$schemas_accdb.viw_chacc_online_point_trans_inf","trans_dt",s"$start_day",s"$end_day")
    df2_1.registerTempTable("viw_chacc_online_point_trans_inf")

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
         |else '总公司' end as cup_branch_ins_id_nm
         |
         |from viw_chacc_online_point_trans_inf ta
         |where  ta.trans_tp in ('03','09','11','18','28')
         | """.stripMargin)


    println("JOB_HV_28------>results:"+results.count())
    if(!Option(results).isEmpty){
      results.registerTempTable("spark_hive_online_point_trans")
    }else{
      println("加载的表spark_hive_online_point_trans中无数据！")
    }
    // Xue create function about partition by date ^_^
    def PartitionFun_JOB_HV_28(start_dt: String, end_dt: String)  {
      var sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val start = LocalDate.parse(start_dt, dateFormatter)
      val end = LocalDate.parse(end_dt, dateFormatter)
      val days = Days.daysBetween(start, end).getDays
      val dateStrs = for (day <- 0 to days) {
        val insertofTime = System.currentTimeMillis()
        val currentDay = (start.plusDays(day).toString(dateFormatter))
        println(s"=========insert data to partition = '$currentDay'=========")
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql(s"alter table hive_online_point_trans drop partition (part_trans_dt='$currentDay')")
        println(s"alter table hive_online_point_trans drop partition (part_trans_dt='$currentDay') successfully!")
        sqlContext.sql(s"insert into hive_online_point_trans partition (part_trans_dt='$currentDay') select * from spark_hive_online_point_trans htempa where htempa.trans_dt = '$currentDay'")
        println(s"insert into hive_online_point_trans partition (part_trans_dt='$currentDay') successfully!")
        val usedInsertofTime = System.currentTimeMillis() - insertofTime
        val ss:Int =((System.currentTimeMillis() - insertofTime)/1000).toInt
        val MM:Int = ss/60
        val hh:Int = MM/60
        val dd:Int = hh/24
        println("运行插入分区花费的时间是:"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedInsertofTime+"毫秒")
      }
    }

    PartitionFun_JOB_HV_28 (start_dt,end_dt)
    val usedTime = System.currentTimeMillis() - currntTime
    val ss:Int =((System.currentTimeMillis() - currntTime)/1000).toInt
    val MM:Int = ss/60
    val hh:Int = MM/60
    val dd:Int = hh/24
    println("运行 JOB_HV_28 花费 :"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedTime+"毫秒")

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
    val currntTime =System.currentTimeMillis()

    val start_day = start_dt.replace("-","")
    val end_day = end_dt.replace("-","")

    val df2_1 = sqlContext.readDB2_ACC_4para(s"$schemas_accdb.tbl_chacc_cdhd_point_addup_dtl","trans_dt",s"$start_day",s"$end_day")
    df2_1.registerTempTable("tbl_chacc_cdhd_point_addup_dtl")

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
         |concat_ws('-',substr(trim(ta.trans_dt),1,4),substr(trim(ta.trans_dt),5,2),substr(trim(ta.trans_dt),7,2)) as trans_dt,
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
         |trim(ta.retri_ref_no) as retri_ref_no
         |
         |from tbl_chacc_cdhd_point_addup_dtl ta
         |where ta.um_trans_id in('AD00000002','AD00000003','AD00000004','AD00000005','AD00000006','AD00000007')
         | """.stripMargin)

    println("JOB_HV_29------>results:"+results.count())
    if(!Option(results).isEmpty){
      results.registerTempTable("spark_hive_offline_point_trans")
      results.show(10)
    }else{
      println("加载的表spark_hive_offline_point_trans中无数据！")
    }
    // Xue create function about partition by date ^_^
    def PartitionFun_JOB_HV_29(start_dt: String, end_dt: String)  {
      var sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val start = LocalDate.parse(start_dt, dateFormatter)
      val end = LocalDate.parse(end_dt, dateFormatter)
      val days = Days.daysBetween(start, end).getDays
      val dateStrs = for (day <- 0 to days) {
        val insertofTime = System.currentTimeMillis()

        val currentDay = (start.plusDays(day).toString(dateFormatter))
        println(s"=========插入'$currentDay'分区的数据=========")
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql(s"alter table hive_offline_point_trans drop partition (part_trans_dt='$currentDay')")
        println(s"alter table hive_offline_point_trans drop partition (part_trans_dt='$currentDay') successfully!")
        sqlContext.sql(s"insert into hive_offline_point_trans partition (part_trans_dt='$currentDay') select * from spark_hive_offline_point_trans htempa where htempa.trans_dt= '$currentDay'")
        println(s"insert into hive_offline_point_trans partition (part_trans_dt='$currentDay') successfully!")
        val usedInsertofTime = System.currentTimeMillis() - insertofTime
        val ss:Int =((System.currentTimeMillis() - insertofTime)/1000).toInt
        val MM:Int = ss/60
        val hh:Int = MM/60
        val dd:Int = hh/24
        println("运行插入分区花费的时间是:"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedInsertofTime+"毫秒")
      }
    }

    PartitionFun_JOB_HV_29 (start_dt,end_dt)
    val usedTime = System.currentTimeMillis() - currntTime
    val ss:Int =((System.currentTimeMillis() - currntTime)/1000).toInt
    val MM:Int = ss/60
    val hh:Int = MM/60
    val dd:Int = hh/24
    println("运行 JOB_HV_29 花费 :"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedTime+"毫秒")

  }


  /**
    * hive-job-30 2016-08-22
    * viw_chacc_code_pay_tran_dtl -> hive_passive_code_pay_trans
    *
    * @author yangxue
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    */
  def JOB_HV_30(implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {
    println("###JOB_HV_30(viw_chacc_code_pay_tran_dtl -> hive_passive_code_pay_trans)")

    val currntTime =System.currentTimeMillis()
    val start_day = start_dt.replace("-","")
    val end_day = end_dt.replace("-","")

    sqlContext.sql(s"use $hive_dbname")
    val df = sqlContext.readDB2_ACC_4para(s"$schemas_accdb.VIW_CHACC_CODE_PAY_TRAN_DTL","trans_dt",s"$start_day",s"$end_day")
    df.registerTempTable("db2_code_pay_tran_dtl")
    val results = sqlContext.sql(
      s"""
         |select
         | trim(trans_seq),
         | cdhd_card_bind_seq,
         | trim(cdhd_usr_id),
         | trim(card_no),
         | trim(tran_mode),
         | trim(trans_tp),
         | tran_certi,
         | trim(trans_rdm_num),
         | trans_expire_ts,
         | order_id,
         | device_cd,
         | trim(mchnt_cd),
         | mchnt_nm,
         | trim(sub_mchnt_cd),
         | sub_mchnt_nm,
         | trim(settle_dt),
         | trans_at,
         | discount_at,
         | discount_info,
         | refund_at,
         | trim(orig_trans_seq),
         | trim(trans_st),
         | rec_crt_ts,
         | rec_upd_ts,
         | case
         | 	when
         | 		substr(trans_dt,1,4) between '0001' and '9999' and substr(trans_dt,5,2) between '01' and '12' and
         | 		substr(trans_dt,7,2) between '01' and last_day(concat_ws('-',substr(trans_dt,1,4),substr(trans_dt,5,2),substr(trans_dt,7,2)))
         | 	then concat_ws('-',substr(trans_dt,1,4),substr(trans_dt,5,2),substr(trans_dt,7,2))
         | 	else null
         | end as trans_dt,
         | trim(resp_code),
         | resp_msg,
         | trim(out_trade_no),
         | trim(body),
         | trim(terminal_id),
         | extend_params
         |from
         | db2_code_pay_tran_dtl
      """.stripMargin
    )
    results.registerTempTable("spark_code_pay_tran_dtl")

    println("###JOB_HV_30------>results:"+results.count())

    def InsertPart_JOB_HV_30(start_dt: String, end_dt: String)  {
      var sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val start = LocalDate.parse(start_dt, dateFormatter)
      val end = LocalDate.parse(end_dt, dateFormatter)
      val days = Days.daysBetween(start, end).getDays
      val dateStrs = for (day <- 0 to days) {
        val insertofTime = System.currentTimeMillis()
        val currentDay = (start.plusDays(day).toString(dateFormatter))
        println(s"=========插入'$currentDay'分区的数据=========")

        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql(s"alter table hive_passive_code_pay_trans drop partition (part_trans_dt='$currentDay')")
        println(s"alter table hive_passive_code_pay_trans drop partition (part_trans_dt='$currentDay') successfully!")

        sqlContext.sql(
          s"""
             |insert into hive_passive_code_pay_trans partition (part_trans_dt='$currentDay')
             |select * from spark_code_pay_tran_dtl where trans_dt = '$currentDay'
           """.stripMargin)
        println(s"insert into hive_passive_code_pay_trans partition (part_trans_dt='$currentDay') successfully!")
        val usedInsertofTime = System.currentTimeMillis() - insertofTime
        val ss:Int =((System.currentTimeMillis() - insertofTime)/1000).toInt
        val MM:Int = ss/60
        val hh:Int = MM/60
        val dd:Int = hh/24
        println("运行插入分区花费的时间是:"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedInsertofTime+"毫秒")

      }
    }

    InsertPart_JOB_HV_30 (start_dt,end_dt)
    val usedTime = System.currentTimeMillis() - currntTime
    val ss:Int =((System.currentTimeMillis() - currntTime)/1000).toInt
    val MM:Int = ss/60
    val hh:Int = MM/60
    val dd:Int = hh/24
    println("运行 JOB_HV_30 花费 :"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedTime+"毫秒")

  }

  /**
    * JOB_HV_31/10-14
    * HIVE_BILL_ORDER_TRANS->VIW_CHMGM_BILL_ORDER_AUX_INF
    * Code by Xue
    *
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    */
  def JOB_HV_31 (implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {
    val currntTime =System.currentTimeMillis()

    val start_day = start_dt.replace("-","")
    val end_day = end_dt.replace("-","")

    val df2_1 = sqlContext.readDB2_MGM_4para(s"$schemas_mgmdb.VIW_CHMGM_BILL_ORDER_AUX_INF","trans_dt",s"$start_day",s"$end_day")
    df2_1.registerTempTable("VIW_CHMGM_BILL_ORDER_AUX_INF")

    val results = sqlContext.sql(
      s"""
         |select
         |trim(ta.BILL_ORDER_ID) as BILL_ORDER_ID,
         |trim(ta.MCHNT_CD) as MCHNT_CD,
         |ta.MCHNT_NM as MCHNT_NM,
         |trim(ta.SUB_MCHNT_CD) as SUB_MCHNT_CD,
         |trim(ta.CDHD_USR_ID) as CDHD_USR_ID,
         |ta.SUB_MCHNT_NM as SUB_MCHNT_NM,
         |ta.RELATED_USR_ID as RELATED_USR_ID,
         |trim(ta.CUPS_TRACE_NUMBER) as CUPS_TRACE_NUMBER,
         |trim(ta.TRANS_TM) as TRANS_TM,
         |case
         |	when
         |		substr(ta.trans_dt,1,4) between '0001' and '9999' and substr(ta.trans_dt,5,2) between '01' and '12' and
         |		substr(ta.trans_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(ta.trans_dt,1,4),substr(ta.trans_dt,5,2),substr(ta.trans_dt,7,2))),9,2)
         |	then concat_ws('-',substr(ta.trans_dt,1,4),substr(ta.trans_dt,5,2),substr(ta.trans_dt,7,2))
         |	else null
         |end as trans_dt,
         |trim(ta.ORIG_TRANS_SEQ) as ORIG_TRANS_SEQ,
         |trim(ta.TRANS_SEQ) as TRANS_SEQ,
         |trim(ta.MOBILE_ORDER_ID) as MOBILE_ORDER_ID,
         |trim(ta.ACP_ORDER_ID) as ACP_ORDER_ID,
         |trim(ta.DELIVERY_PROV_CD) as DELIVERY_PROV_CD,
         |trim(ta.DELIVERY_CITY_CD) as DELIVERY_CITY_CD,
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
         |else '总公司' end) as prov_division_cd,
         |trim(ta.DELIVERY_DISTRICT_CD) as DELIVERY_DISTRICT_CD,
         |trim(ta.DELIVERY_ZIP_CD) as DELIVERY_ZIP_CD,
         |ta.DELIVERY_ADDRESS as DELIVERY_ADDRESS,
         |trim(ta.RECEIVER_NM) as RECEIVER_NM,
         |trim(ta.RECEIVER_MOBILE) as RECEIVER_MOBILE,
         |ta.DELIVERY_TIME_DESC as DELIVERY_TIME_DESC,
         |ta.INVOICE_DESC as INVOICE_DESC,
         |ta.TRANS_AT as TRANS_AT,
         |ta.REFUND_AT as REFUND_AT,
         |trim(ta.ORDER_ST) as ORDER_ST,
         |ta.ORDER_CRT_TS as ORDER_CRT_TS,
         |ta.ORDER_TIMEOUT_TS as ORDER_TIMEOUT_TS,
         |trim(ta.CARD_NO) as CARD_NO,
         |trim(ta.ORDER_CHNL) as ORDER_CHNL,
         |trim(ta.ORDER_IP) as ORDER_IP,
         |ta.DEVICE_INF as DEVICE_INF,
         |ta.REMARK as REMARK,
         |ta.REC_CRT_TS as REC_CRT_TS,
         |trim(ta.CRT_CDHD_USR_ID) as CRT_CDHD_USR_ID,
         |ta.REC_UPD_TS as REC_UPD_TS,
         |trim(ta.UPD_CDHD_USR_ID) as UPD_CDHD_USR_ID
         |
         |from VIW_CHMGM_BILL_ORDER_AUX_INF ta
         | """.stripMargin)

    println("JOB_HV_31------>results:"+results.count())
    if(!Option(results).isEmpty){
      results.registerTempTable("spark_hive_bill_order_trans")
    }else{
      println("加载的表spark_hive_bill_order_trans中无数据！")
    }


    // Xue create function about partition by date ^_^
    def PartitionFun_JOB_HV_31(start_dt: String, end_dt: String)  {
      var sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val start = LocalDate.parse(start_dt, dateFormatter)
      val end = LocalDate.parse(end_dt, dateFormatter)
      val days = Days.daysBetween(start, end).getDays
      val dateStrs = for (day <- 0 to days) {
        val insertofTime = System.currentTimeMillis()

        val currentDay = (start.plusDays(day).toString(dateFormatter))
        println(s"=========插入'$currentDay'分区的数据=========")
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql(s"alter table HIVE_BILL_ORDER_TRANS drop partition (part_trans_dt='$currentDay')")
        println(s"alter table HIVE_BILL_ORDER_TRANS drop partition (part_trans_dt='$currentDay') successfully!")
        sqlContext.sql(s"alter table HIVE_BILL_ORDER_TRANS add partition (part_trans_dt='$currentDay')")
        println(s"alter table HIVE_BILL_ORDER_TRANS add partition (part_trans_dt='$currentDay') successfully!")
        sqlContext.sql(s"insert into HIVE_BILL_ORDER_TRANS partition (part_trans_dt='$currentDay') select * from spark_hive_bill_order_trans htempa where htempa.trans_dt = '$currentDay'")
        println(s"insert into HIVE_BILL_ORDER_TRANS partition (part_trans_dt='$currentDay') successfully!")
        val usedInsertofTime = System.currentTimeMillis() - insertofTime
        val ss:Int =((System.currentTimeMillis() - insertofTime)/1000).toInt
        val MM:Int = ss/60
        val hh:Int = MM/60
        val dd:Int = hh/24
        println("运行插入分区花费的时间是:"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedInsertofTime+"毫秒")

      }
    }

    PartitionFun_JOB_HV_31 (start_dt,end_dt)
    val usedTime = System.currentTimeMillis() - currntTime
    val ss:Int =((System.currentTimeMillis() - currntTime)/1000).toInt
    val MM:Int = ss/60
    val hh:Int = MM/60
    val dd:Int = hh/24
    println("运行 JOB_HV_31 花费 :"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedTime+"毫秒")

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
    val currntTime =System.currentTimeMillis()

    val start_day = start_dt.replace("-","")
    val end_day = end_dt.replace("-","")

    val df2_1 = sqlContext.readDB2_MGM_4para(s"$schemas_mgmdb.tbl_umsvc_prize_discount_result","settle_dt",s"$start_day",s"$end_day")
    df2_1.registerTempTable("tbl_umsvc_prize_discount_result")

    val results = sqlContext.sql(
      s"""
         |select
         |ta.PRIZE_RESULT_SEQ as PRIZE_RESULT_SEQ,
         |trim(ta.TRANS_ID) as TRANS_ID,
         |trim(ta.SYS_TRA_NO) as SYS_TRA_NO,
         |trim(ta.SYS_TRA_NO_CONV) as SYS_TRA_NO_CONV,
         |trim(ta.PRI_ACCT_NO) as PRI_ACCT_NO,
         |ta.TRANS_AT as TRANS_AT,
         |ta.TRANS_AT_CONV as TRANS_AT_CONV,
         |ta.TRANS_POS_AT as TRANS_POS_AT,
         |trim(ta.TRANS_DT_TM) as TRANS_DT_TM,
         |trim(ta.LOC_TRANS_DT) as LOC_TRANS_DT,
         |trim(ta.LOC_TRANS_TM) as LOC_TRANS_TM,
         |case
         |	when
         |		substr(ta.SETTLE_DT,1,4) between '0001' and '9999' and substr(ta.SETTLE_DT,5,2) between '01' and '12' and
         |		substr(ta.SETTLE_DT,7,2) between '01' and substr(last_day(concat_ws('-',substr(ta.SETTLE_DT,1,4),substr(ta.SETTLE_DT,5,2),substr(ta.SETTLE_DT,7,2))),9,2)
         |	then concat_ws('-',substr(ta.SETTLE_DT,1,4),substr(ta.SETTLE_DT,5,2),substr(ta.SETTLE_DT,7,2))
         |	else null
         |end as SETTLE_DT,
         |trim(MCHNT_TP) as MCHNT_TP,
         |trim(ACPT_INS_ID_CD) as ACPT_INS_ID_CD,
         |trim(ISS_INS_ID_CD) as ISS_INS_ID_CD,
         |trim(ACQ_INS_ID_CD) as ACQ_INS_ID_CD,
         |CUP_BRANCH_INS_ID_CD as CUP_BRANCH_INS_ID_CD,
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
         |trim(ta.MCHNT_CD) as MCHNT_CD,
         |trim(ta.TERM_ID) as TERM_ID,
         |trim(ta.TRANS_CURR_CD) as TRANS_CURR_CD,
         |trim(ta.TRANS_CHNL) as TRANS_CHNL,
         |trim(ta.PROD_IN) as PROD_IN,
         |ta.AGIO_APP_ID as AGIO_APP_ID,
         |trim(ta.AGIO_INF) as AGIO_INF,
         |ta.PRIZE_APP_ID as PRIZE_APP_ID,
         |ta.PRIZE_ID as PRIZE_ID,
         |ta.PRIZE_LVL as PRIZE_LVL,
         |case
         |	when
         |		substr(ta.REC_CRT_DT,1,4) between '0001' and '9999' and substr(ta.REC_CRT_DT,5,2) between '01' and '12' and
         |		substr(ta.REC_CRT_DT,7,2) between '01' and substr(last_day(concat_ws('-',substr(ta.REC_CRT_DT,1,4),substr(ta.REC_CRT_DT,5,2),substr(ta.REC_CRT_DT,7,2))),9,2)
         |	then concat_ws('-',substr(ta.REC_CRT_DT,1,4),substr(ta.REC_CRT_DT,5,2),substr(ta.REC_CRT_DT,7,2))
         |	else null
         |end as REC_CRT_DT,
         |trim(ta.IS_MATCH_IN) as IS_MATCH_IN,
         |trim(ta.FWD_INS_ID_CD) as FWD_INS_ID_CD,
         |trim(ta.ORIG_TRANS_TFR_TM) as ORIG_TRANS_TFR_TM,
         |trim(ta.ORIG_SYS_TRA_NO) as ORIG_SYS_TRA_NO,
         |trim(ta.ORIG_ACPT_INS_ID_CD) as ORIG_ACPT_INS_ID_CD,
         |trim(ta.ORIG_FWD_INS_ID_CD) as ORIG_FWD_INS_ID_CD,
         |trim(ta.SUB_CARD_NO) as SUB_CARD_NO,
         |trim(ta.IS_PROCED) as IS_PROCED,
         |ta.ENTITY_CARD_NO as ENTITY_CARD_NO,
         |ta.CLOUD_PAY_IN as CLOUD_PAY_IN,
         |trim(ta.CARD_MEDIA) as CARD_MEDIA
         |
         |from TBL_UMSVC_PRIZE_DISCOUNT_RESULT ta
         | """.stripMargin)

    println("JOB_HV_32------>results:"+results.count())
    if(!Option(results).isEmpty){
      results.registerTempTable("spark_hive_prize_discount_result")
    }else{
      println("加载的表spark_hive_prize_discount_result中无数据！")
    }


    // Xue create function about partition by date ^_^
    def PartitionFun_JOB_HV_32(start_dt: String, end_dt: String)  {
      var sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val start = LocalDate.parse(start_dt, dateFormatter)
      val end = LocalDate.parse(end_dt, dateFormatter)
      val days = Days.daysBetween(start, end).getDays
      val dateStrs = for (day <- 0 to days) {
        val insertofTime = System.currentTimeMillis()

        val currentDay = (start.plusDays(day).toString(dateFormatter))
        println(s"=========插入'$currentDay'分区的数据=========")
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql(s"alter table hive_prize_discount_result drop partition (part_settle_dt='$currentDay')")
        println(s"alter table hive_prize_discount_result drop partition (part_settle_dt='$currentDay') successfully!")
        sqlContext.sql(s"insert into hive_prize_discount_result partition (part_settle_dt='$currentDay') select * from spark_hive_prize_discount_result htempa where htempa.settle_dt = '$currentDay'")
        println(s"insert into hive_prize_discount_result partition (part_settle_dt='$currentDay') successfully!")
        val usedInsertofTime = System.currentTimeMillis() - insertofTime
        val ss:Int =((System.currentTimeMillis() - insertofTime)/1000).toInt
        val MM:Int = ss/60
        val hh:Int = MM/60
        val dd:Int = hh/24
        println("运行插入分区花费的时间是:"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedInsertofTime+"毫秒")
      }
    }

    PartitionFun_JOB_HV_32 (start_dt, end_dt)
    val usedTime = System.currentTimeMillis() - currntTime
    val ss:Int =((System.currentTimeMillis() - currntTime)/1000).toInt
    val MM:Int = ss/60
    val hh:Int = MM/60
    val dd:Int = hh/24
    println("运行 JOB_HV_32 花费 :"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedTime+"毫秒")

  }

  /**
    * JOB_HV_33/10-19
    * hive_bill_sub_order_trans->VIW_CHMGM_BILL_SUB_ORDER_DETAIL_INF
    * Code by Xue
    *
    * @param start_dt
    * @param end_dt
    * @param sqlContext
    * @return
    */
  def JOB_HV_33(implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {
    val currntTime =System.currentTimeMillis()

    val start_day = start_dt.replace("-","")
    val end_day = end_dt.replace("-","")

    val df2_1 = sqlContext.readDB2_MGM_4para(s"$schemas_mgmdb.viw_chmgm_bill_sub_order_detail_inf","trans_dt",s"$start_day",s"$end_day")
    df2_1.registerTempTable("VIW_CHMGM_BILL_SUB_ORDER_DETAIL_INF")

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
         |ta.refund_reason as trans_seq,
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
         |ta.response_msg as response_msg
         |from viw_chmgm_bill_sub_order_detail_inf ta
         | """.stripMargin)

    println("JOB_HV_33------>results:" + results.count())
    if (!Option(results).isEmpty) {
      results.registerTempTable("spark_hive_bill_sub_order_trans")
    } else {
      println("加载的表spark_hive_bill_sub_order_trans中无数据！")
    }


    // Xue create function about partition by date ^_^
    def PartitionFun_JOB_HV_33(start_dt: String, end_dt: String) {
      var sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val start = LocalDate.parse(start_dt, dateFormatter)
      val end = LocalDate.parse(end_dt, dateFormatter)
      val days = Days.daysBetween(start, end).getDays
      val dateStrs = for (day <- 0 to days) {
        val insertofTime = System.currentTimeMillis()

        val currentDay = (start.plusDays(day).toString(dateFormatter))
        println(s"=========插入'$currentDay'分区的数据=========")
        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql(s"alter table hive_bill_sub_order_trans drop partition (part_trans_dt='$currentDay')")
        println(s"alter table hive_bill_sub_order_trans drop partition (part_trans_dt='$currentDay') successfully!")
        sqlContext.sql(s"insert into hive_bill_sub_order_trans partition (part_trans_dt='$currentDay') select * from spark_hive_bill_sub_order_trans htempa where htempa.trans_dt = '$currentDay'")
        println(s"insert into hive_bill_sub_order_trans partition (part_trans_dt='$currentDay') successfully!")
        val usedInsertofTime = System.currentTimeMillis() - insertofTime
        val ss:Int =((System.currentTimeMillis() - insertofTime)/1000).toInt
        val MM:Int = ss/60
        val hh:Int = MM/60
        val dd:Int = hh/24
        println("运行插入分区花费的时间是:"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedInsertofTime+"毫秒")

      }
    }

    PartitionFun_JOB_HV_33(start_dt, end_dt)
    val usedTime = System.currentTimeMillis() - currntTime
    val ss:Int =((System.currentTimeMillis() - currntTime)/1000).toInt
    val MM:Int = ss/60
    val hh:Int = MM/60
    val dd:Int = hh/24
    println("运行 JOB_HV_33 花费 :"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedTime+"毫秒")

  }

  /**
    * hive-job-36 2016-08-26
    * TBL_CHACC_CDHD_BILL_ACCT_INF -> HIVE_BUSS_DIST
    *
    * @author Xue
    * @param sqlContext
    */
  def JOB_HV_34(implicit sqlContext: HiveContext) = {
    val currntTime =System.currentTimeMillis()

    println("###JOB_HV_34(TBL_CHMGM_CHARA_ACCT_DEF_TMP -> HIVE_BUSS_DIST)")
    sqlContext.sql(s"use $hive_dbname")
    val df = sqlContext.readDB2_MGM(s"$schemas_mgmdb.TBL_CHMGM_CHARA_ACCT_DEF_TMP")
    df.registerTempTable("TBL_CHMGM_CHARA_ACCT_DEF_TMP")
    val results = sqlContext.sql(
      """
        |SELECT
        |ta.REC_ID as REC_ID,
        |trim(ta.CHARA_ACCT_TP) as CHARA_ACCT_TP,
        |trim(ta.CHARA_ACCT_ATTR) as CHARA_ACCT_ATTR,
        |ta.CHARA_ACCT_DESC as CHARA_ACCT_DESC,
        |trim(ta.ENTRY_GRP_CD) as ENTRY_GRP_CD,
        |trim(ta.ENTRY_BLKBILL_IN) as ENTRY_BLKBILL_IN,
        |trim(ta.OUTPUT_GRP_CD)  as OUTPUT_GRP_CD,
        |trim(ta.OUTPUT_BLKBILL_IN) as OUTPUT_BLKBILL_IN,
        |trim(ta.CHARA_ACCT_ST) as CHARA_ACCT_ST,
        |trim(ta.AUD_USR_ID) as AUD_USR_ID,
        |ta.AUD_IDEA as AUD_IDEA,
        |ta.AUD_TS as AUD_TS,
        |trim(ta.AUD_IN) as AUD_IN,
        |trim(ta.OPER_ACTION) as OPER_ACTION,
        |ta.REC_CRT_TS as REC_CRT_TS,
        |ta.REC_UPD_TS as REC_UPD_TS,
        |trim(ta.REC_CRT_USR_ID) as REC_CRT_USR_ID,
        |trim(ta.REC_UPD_USR_ID) as REC_UPD_USR_ID,
        |ta.VER_NO as VER_NO,
        |trim(ta.CHARA_ACCT_NM) as CHARA_ACCT_NM,
        |ta.CH_INS_ID_CD as CH_INS_ID_CD,
        |trim(ta.UM_INS_TP) as UM_INS_TP,
        |ta.INS_NM  as INS_NM,
        |trim(ta.OPER_IN) as OPER_IN,
        |ta.EVENT_ID as EVENT_ID,
        |trim(ta.SYNC_ST) as SYNC_ST,
        |ta.CUP_BRANCH_INS_ID_CD as CUP_BRANCH_INS_ID_CD,
        |ta.SCENE_USAGE_IN as SCENE_USAGE_IN
        |FROM TBL_CHMGM_CHARA_ACCT_DEF_TMP ta
      """.stripMargin
    )

    println("JOB_HV_34------>results:"+results.count())
    if(!Option(results).isEmpty){
      results.registerTempTable("spark_hive_buss_dist")
      sqlContext.sql("truncate table hive_buss_dist")
      sqlContext.sql("insert into table hive_buss_dist select * from spark_hive_buss_dist")
      println("###JOB_HV_36(insert into table hive_buss_dist successful) ")
      val usedTime = System.currentTimeMillis() - currntTime
      val ss:Int =((System.currentTimeMillis() - currntTime)/1000).toInt
      val MM:Int = ss/60
      val hh:Int = MM/60
      val dd:Int = hh/24
      println("运行 JOB_HV_34 花费 :"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedTime+"毫秒")

    }else{
      println("加载的表TBL_CHACC_CDHD_BILL_ACCT_INF中无数据！")
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
    val currntTime =System.currentTimeMillis()

    println("###JOB_HV_35(TBL_CHACC_CDHD_BILL_ACCT_INF -> HIVE_CDHD_BILL_ACCT_INF)")
    sqlContext.sql(s"use $hive_dbname")
    val df = sqlContext.readDB2_ACC(s"$schemas_accdb.TBL_CHACC_CDHD_BILL_ACCT_INF")
    df.registerTempTable("TBL_CHACC_CDHD_BILL_ACCT_INF")
    val results = sqlContext.sql(
      """
        |select
        |ta.SEQ_ID as SEQ_ID,
        |trim(ta.CDHD_USR_ID) as CDHD_USR_ID,
        |trim(ta.CDHD_FK) as CDHD_FK,
        |trim(ta.BILL_ID) as BILL_ID,
        |trim(ta.BILL_BAT_NO) as BILL_BAT_NO,
        |trim(ta.BILL_TP) as BILL_TP,
        |ta.MEM_NM as MEM_NM,
        |ta.BILL_NUM as BILL_NUM,
        |ta.USAGE_NUM as USAGE_NUM,
        |trim(ta.ACCT_ST) as ACCT_ST,
        |ta.REC_CRT_TS as REC_CRT_TS,
        |ta.REC_UPD_TS as REC_UPD_TS,
        |ta.VER_NO as VER_NO,
        |trim(ta.BILL_RELATED_CARD_NO) as BILL_RELATED_CARD_NO,
        |trim(ta.SCENE_ID) as SCENE_ID,
        |ta.FREEZE_BILL_NUM  as FREEZE_BILL_NUM
        |from TBL_CHACC_CDHD_BILL_ACCT_INF ta
      """.stripMargin
    )

    println("JOB_HV_35------>results:"+results.count())
    if(!Option(results).isEmpty){
      results.registerTempTable("spark_hive_cdhd_bill_acct_inf")
      sqlContext.sql("truncate table hive_cdhd_bill_acct_inf")
      sqlContext.sql("insert into table hive_cdhd_bill_acct_inf select * from spark_hive_cdhd_bill_acct_inf")
      println("###JOB_HV_35(insert into table hive_cdhd_bill_acct_inf successful) ")
      val usedTime = System.currentTimeMillis() - currntTime
      val ss:Int =((System.currentTimeMillis() - currntTime)/1000).toInt
      val MM:Int = ss/60
      val hh:Int = MM/60
      val dd:Int = hh/24
      println("运行 JOB_HV_35 花费 :"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedTime+"毫秒")

    }else{
      println("加载的表TBL_CHACC_CDHD_BILL_ACCT_INF中无数据！")
    }
  }


  /**
    * hive-job-36 2016-08-26
    * tbl_umsvc_discount_bas_inf -> hive_discount_bas_inf
    *
    * @author winslow yang
    * @param sqlContext
    */
  def JOB_HV_36(implicit sqlContext: HiveContext) = {
    val currntTime =System.currentTimeMillis()

    println("###JOB_HV_36(tbl_umsvc_discount_bas_inf -> hive_discount_bas_inf)")
    sqlContext.sql(s"use $hive_dbname")
    val df = sqlContext.readDB2_MGM(s"$schemas_mgmdb.TBL_UMSVC_DISCOUNT_BAS_INF")
    df.registerTempTable("db2_dis_bas_inf")
    val results = sqlContext.sql(
      """
        |select
        |loc_activity_id,					loc_activity_nm,									loc_activity_desc,
        |	case
        |		when
        |			substr(activity_begin_dt,1,4) between '0001' and '9999' and substr(activity_begin_dt,5,2) between '01' and '12' and
        |			substr(activity_begin_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(activity_begin_dt,1,4),substr(activity_begin_dt,5,2),substr(activity_begin_dt,7,2))),9,2)
        |		then concat_ws('-',substr(activity_begin_dt,1,4),substr(activity_begin_dt,5,2),substr(activity_begin_dt,7,2))
        |		else null
        |	end as activity_begin_dt,
        |	case
        |		when
        |			substr(activity_end_dt,1,4) between '0001' and '9999' and substr(activity_end_dt,5,2) between '01' and '12' and
        |			substr(activity_end_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(activity_end_dt,1,4),substr(activity_end_dt,5,2),substr(activity_end_dt,7,2))),9,2)
        |		then concat_ws('-',substr(activity_end_dt,1,4),substr(activity_end_dt,5,2),substr(activity_end_dt,7,2))
        |		else null
        |	end as activity_end_dt,
        |	agio_mchnt_num,						eff_mchnt_num,										sync_bat_no,
        |	trim(sync_st) as sync_st,	trim(rule_upd_in) as rule_upd_in,	rule_grp_id,
        |	rec_crt_ts,								trim(rec_crt_usr_id) as rec_crt_usr_id,
        |	trim(rec_crt_ins_id_cd) as rec_crt_ins_id_cd,
        | 	case
        |   when trim(rec_crt_ins_id_cd)='00011000' then '北京'
        |		when trim(rec_crt_ins_id_cd)='00011100' then '天津'
        |		when trim(rec_crt_ins_id_cd)='00011200' then '河北'
        |		when trim(rec_crt_ins_id_cd)='00011600' then '山西'
        |		when trim(rec_crt_ins_id_cd)='00011900' then '内蒙古'
        |		when trim(rec_crt_ins_id_cd)='00012210' then '辽宁'
        |		when trim(rec_crt_ins_id_cd)='00012220' then '大连'
        |		when trim(rec_crt_ins_id_cd)='00012400' then '吉林'
        |		when trim(rec_crt_ins_id_cd)='00012600' then '黑龙江'
        |		when trim(rec_crt_ins_id_cd)='00012900' then '上海'
        |		when trim(rec_crt_ins_id_cd)='00013000' then '江苏'
        |		when trim(rec_crt_ins_id_cd)='00013310' then '浙江'
        |		when trim(rec_crt_ins_id_cd)='00013320' then '宁波'
        |		when trim(rec_crt_ins_id_cd)='00013600' then '安徽'
        |		when trim(rec_crt_ins_id_cd)='00013900' then '福建'
        |		when trim(rec_crt_ins_id_cd)='00013930' then '厦门'
        |		when trim(rec_crt_ins_id_cd)='00014200' then '江西'
        |		when trim(rec_crt_ins_id_cd)='00014500' then '山东'
        |		when trim(rec_crt_ins_id_cd)='00014520' then '青岛'
        |		when trim(rec_crt_ins_id_cd)='00014900' then '河南'
        |		when trim(rec_crt_ins_id_cd)='00015210' then '湖北'
        |		when trim(rec_crt_ins_id_cd)='00015500' then '湖南'
        |		when trim(rec_crt_ins_id_cd)='00015800' then '广东'
        |		when trim(rec_crt_ins_id_cd)='00015840' then '深圳'
        |		when trim(rec_crt_ins_id_cd)='00016100' then '广西'
        |		when trim(rec_crt_ins_id_cd)='00016400' then '海南'
        |		when trim(rec_crt_ins_id_cd)='00016500' then '四川'
        |		when trim(rec_crt_ins_id_cd)='00016530' then '重庆'
        |		when trim(rec_crt_ins_id_cd)='00017000' then '贵州'
        |		when trim(rec_crt_ins_id_cd)='00017310' then '云南'
        |		when trim(rec_crt_ins_id_cd)='00017700' then '西藏'
        |		when trim(rec_crt_ins_id_cd)='00017900' then '陕西'
        |		when trim(rec_crt_ins_id_cd)='00018200' then '甘肃'
        |		when trim(rec_crt_ins_id_cd)='00018500' then '青海'
        |		when trim(rec_crt_ins_id_cd)='00018700' then '宁夏'
        |		when trim(rec_crt_ins_id_cd)='00018800' then '新疆'
        |		else '总公司'
        |	end as cup_branch_ins_id_nm,
        |	rec_upd_ts,								trim(rec_upd_usr_id) as rec_upd_usr_id,
        |	trim(rec_upd_ins_id_cd) as rec_upd_ins_id_cd,
        |	trim(del_in) as del_in,		ver_no,				trim(run_st) as run_st,
        |	trim(posp_from_in) as posp_from_in,			trim(group_id) as group_id
        |from
        |  db2_dis_bas_inf
      """.stripMargin
    )

    println("JOB_HV_36------>results:"+results.count())
    if(!Option(results).isEmpty){
      results.registerTempTable("spark_dis_bas_inf")
      sqlContext.sql("truncate table hive_discount_bas_inf")
      sqlContext.sql("insert into table hive_discount_bas_inf select * from spark_dis_bas_inf")
      println("###JOB_HV_36(insert into table hive_discount_bas_inf successful) ")
      val usedTime = System.currentTimeMillis() - currntTime
      val ss:Int =((System.currentTimeMillis() - currntTime)/1000).toInt
      val MM:Int = ss/60
      val hh:Int = MM/60
      val dd:Int = hh/24
      println("运行 JOB_HV_36 花费 :"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedTime+"毫秒")

    }else{
      println("加载的表tbl_umsvc_discount_bas_inf中无数据！")
    }
  }

  /**
    * hive-job-37  2016年9月21日 星期三
    * hive_filter_app_det-->tbl_umsvc_filter_app_det
    *
    * @author tzq
    * @param sqlContext
    * @return
    */
  def JOB_HV_37(implicit sqlContext: HiveContext) = {
    val currntTime =System.currentTimeMillis()

    println("###JOB_HV_37(hive_filter_app_det-->tbl_umsvc_filter_app_det)")
    val df = sqlContext.readDB2_MGM(s"$schemas_mgmdb.TBL_UMSVC_FILTER_APP_DET")
    df.registerTempTable("db2_tbl_umsvc_filter_app_det")
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

    println("###JOB_HV_37---------->results:"+results.count())
    if(!Option(results).isEmpty){
      results.registerTempTable("spark_db2_tbl_umsvc_filter_app_det")
      sqlContext.sql(s"use $hive_dbname")
      sqlContext.sql("truncate table  hive_filter_app_det")
      sqlContext.sql("insert into table hive_filter_app_det select * from spark_db2_tbl_umsvc_filter_app_det")
      val usedTime = System.currentTimeMillis() - currntTime
      val ss:Int =((System.currentTimeMillis() - currntTime)/1000).toInt
      val MM:Int = ss/60
      val hh:Int = MM/60
      val dd:Int = hh/24
      println("运行 JOB_HV_37 花费 :"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedTime+"毫秒")
    }else{
      println("加载的表TBL_UMSVC_FILTER_APP_DET中无数据！")
    }

  }


  /**
    * hive-job-38  2016年9月21日 星期三
    * hive_filter_rule_det-->tbl_umsvc_filter_rule_det
    *
    * @author tzq
    * @param sqlContext
    * @return
    */
  def JOB_HV_38(implicit sqlContext: HiveContext) = {
    val currntTime =System.currentTimeMillis()

    println("###JOB_HV_38(hive_filter_rule_det-->tbl_umsvc_filter_rule_det)")
    val df = sqlContext.readDB2_MGM(s"$schemas_mgmdb.TBL_UMSVC_FILTER_RULE_DET")
    df.registerTempTable("db2_tbl_umsvc_filter_rule_det")
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

    println("###JOB_HV_38---------->results:"+results.count())

    if(!Option(results).isEmpty){
      results.registerTempTable("spark_db2_tbl_umsvc_filter_rule_det")
      sqlContext.sql(s"use $hive_dbname")
      sqlContext.sql("truncate table  hive_filter_rule_det")
      sqlContext.sql("insert into table hive_filter_rule_det select * from db2_tbl_umsvc_filter_rule_det")
      val usedTime = System.currentTimeMillis() - currntTime
      val ss:Int =((System.currentTimeMillis() - currntTime)/1000).toInt
      val MM:Int = ss/60
      val hh:Int = MM/60
      val dd:Int = hh/24
      println("运行 JOB_HV_38 花费 :"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedTime+"毫秒")

    }else{
      println("加载的表TBL_UMSVC_FILTER_RULE_DET中无数据！")
    }

  }


  /**
    *  hive-job-43 2016-09-12
    *  viw_chmgm_swt_log -> hive_switch_point_trans
    *
    * @author winslow yang
    * @param sqlContext
    * @param start_dt
    * @param end_dt
    */
  def JOB_HV_43(implicit sqlContext: HiveContext,start_dt:String,end_dt:String) = {

    println("###JOB_HV_43(viw_chmgm_swt_log -> hive_switch_point_trans)")

    val currntTime =System.currentTimeMillis()
    val start_day = start_dt.replace("-","")
    val end_day = end_dt.replace("-","")

    sqlContext.sql(s"use $hive_dbname")
    val df = sqlContext.readDB2_MGM_4para(s"$schemas_mgmdb.VIW_CHMGM_SWT_LOG","trans_dt",s"$start_day",s"$end_day")
    df.registerTempTable("db2_swt_log")
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
         |db2_swt_log
         |where
         |trans_tp in ('S370000000','S380000000')
      """.stripMargin
    )
    results.registerTempTable("spark_swt_log")

    println("###JOB_HV_43------>results:"+results.count())

    def InsertPart_JOB_HV_43 (start_dt: String, end_dt: String)  {
      var sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val start = LocalDate.parse(start_dt, dateFormatter)
      val end = LocalDate.parse(end_dt, dateFormatter)
      val days = Days.daysBetween(start, end).getDays
      val dateStrs = for (day <- 0 to days) {
        val insertofTime = System.currentTimeMillis()

        val currentDay = (start.plusDays(day).toString(dateFormatter))
        println(s"=========insert data to partition= '$currentDay'=========")

        sqlContext.sql(s"use $hive_dbname")
        sqlContext.sql(s"alter table hive_switch_point_trans drop partition (part_trans_dt='$currentDay')")
        println(s"alter table hive_switch_point_trans drop partition (part_trans_dt='$currentDay') successfully!")

        sqlContext.sql(
          s"""
             |insert into hive_switch_point_trans partition (part_trans_dt='$currentDay')
             |select * from spark_swt_log where trans_dt = '$currentDay'
           """.stripMargin)
        println(s"insert into hive_switch_point_trans partition (part_trans_dt='$currentDay') successfully!")
        val usedInsertofTime = System.currentTimeMillis() - insertofTime
        val ss:Int =((System.currentTimeMillis() - insertofTime)/1000).toInt
        val MM:Int = ss/60
        val hh:Int = MM/60
        val dd:Int = hh/24
        println("运行插入当前分区花费的时间是:"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedInsertofTime+"毫秒")

      }
    }

    InsertPart_JOB_HV_43 (start_dt,end_dt)
    val usedTime = System.currentTimeMillis() - currntTime
    val ss:Int =((System.currentTimeMillis() - currntTime)/1000).toInt
    val MM:Int = ss/60
    val hh:Int = MM/60
    val dd:Int = hh/24
    println("运行 JOB_HV_43 花费 :"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedTime+"毫秒")

  }

  /**
    * hive-job-44/08-22
    * hive_cdhd_cashier_maktg_reward_dtl->viw_chacc_cdhd_cashier_maktg_reward_dtl
    *
    * @author tzq
    * @param sqlContext
    * @return
    */
  def JOB_HV_44(implicit sqlContext: HiveContext) = {
    val currntTime =System.currentTimeMillis()

    println("###JOB_HV_44(viw_chacc_cdhd_cashier_maktg_reward_dtl)")

    val df= sqlContext.readDB2_ACC(s"$schemas_accdb.VIW_CHACC_CDHD_CASHIER_MAKTG_REWARD_DTL")

    df.registerTempTable("db2_cdhd_cashier_maktg_reward_dtl")

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

    println("###JOB_HV_44---------results 条目数: "+results.count())
    if(!Option(results).isEmpty){
      results.registerTempTable("spark_cdhd_cashier_maktg_reward_dtl")
      sqlContext.sql(s"use $hive_dbname")
      sqlContext.sql("truncate table  hive_cdhd_cashier_maktg_reward_dtl")
      sqlContext.sql("insert into table hive_cdhd_cashier_maktg_reward_dtl select * from spark_cdhd_cashier_maktg_reward_dtl")
      println("insert into table hive_cdhd_cashier_maktg_reward_dtl successfully!")
      val usedTime = System.currentTimeMillis() - currntTime
      val ss:Int =((System.currentTimeMillis() - currntTime)/1000).toInt
      val MM:Int = ss/60
      val hh:Int = MM/60
      val dd:Int = hh/24
      println("运行 JOB_HV_44 花费 :"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedTime+"毫秒")

    }else{
      println("加载视图表VIW_CHACC_CDHD_CASHIER_MAKTG_REWARD_DTL中无数据！")
    }

  }


  /**
    * JOB_HV_45 2016-10-28
    * TBL_CUP_BRANCH_ACPT_INS_INF -> HIVE_BRANCH_ACPT_INS_INF
    *
    * @author winslow yang
    * @param sqlContext
    * */
  def JOB_HV_45 (implicit sqlContext: HiveContext) = {
    val currntTime =System.currentTimeMillis()

    println("###JOB_HV_45(hive_branch_acpt_ins_inf)")

    val df = sqlContext.readDB2_ACC(s"$schemas_accdb.TBL_CUP_BRANCH_ACPT_INS_INF")
    df.registerTempTable("spark_tbl_cup_branch_acpt_ins_inf")

    val results = sqlContext.sql(
      """
        |select
        |trim(cup_branch_ins_id_cd),
        |trim(ins_id_cd),
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
        |spark_tbl_cup_branch_acpt_ins_inf
      """.stripMargin)
    println("JOB_HV_45------>results:"+results.count())
    results.registerTempTable("spark_hive_branch_acpt_ins_inf")

    if(!Option(results).isEmpty){
      sqlContext.sql(s"use $hive_dbname")
      sqlContext.sql("truncate table hive_branch_acpt_ins_inf")
      sqlContext.sql("insert into table hive_branch_acpt_ins_inf select * from spark_hive_branch_acpt_ins_inf")
      println("###JOB_HV_45(insert into hive_branch_acpt_ins_inf successful)")
      val usedTime = System.currentTimeMillis() - currntTime
      val ss:Int =((System.currentTimeMillis() - currntTime)/1000).toInt
      val MM:Int = ss/60
      val hh:Int = MM/60
      val dd:Int = hh/24
      println("运行 JOB_HV_45 花费 :"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedTime+"毫秒")

    }else{
      println("加载的表spark_hive_branch_acpt_ins_inf中无数据！")
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
    val currntTime =System.currentTimeMillis()

    val df2_1 = sqlContext.readDB2_MGM(s"$schemas_mgmdb.tbl_umsvc_prize_activity_bas_inf")
    df2_1.registerTempTable("tbl_umsvc_prize_activity_bas_inf")

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

    println("JOB_HV_46------>results:"+results.count())
    if(!Option(results).isEmpty){
      results.registerTempTable("spark_hive_prize_activity_bas_inf")
      sqlContext.sql(s"use $hive_dbname")
      sqlContext.sql("truncate table  hive_prize_activity_bas_inf")
      sqlContext.sql("insert into table hive_prize_activity_bas_inf select * from spark_hive_prize_activity_bas_inf")
      println("insert into table hive_prize_activity_bas_inf successfully!")
      val usedTime = System.currentTimeMillis() - currntTime
      val ss:Int =((System.currentTimeMillis() - currntTime)/1000).toInt
      val MM:Int = ss/60
      val hh:Int = MM/60
      val dd:Int = hh/24
      println("运行 JOB_HV_46 花费 :"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedTime+"毫秒")

    }else{
      println("加载的表spark_hive_prize_activity_bas_inf中无数据！")
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
    val currntTime =System.currentTimeMillis()

    val df2_1 = sqlContext.readDB2_MGM(s"$schemas_mgmdb.tbl_umsvc_prize_lvl_add_rule")
    df2_1.registerTempTable("tbl_umsvc_prize_lvl_add_rule")
    val results = sqlContext.sql(
      """
        |select
        |ta.PRIZE_ID_LVL as BILL_ID,
        |ta.PRIZE_LVL as PRIZE_LVL,
        |ta.REWARD_POINT_RATE as REWARD_POINT_RATE,
        |ta.REWARD_POINT_MAX as REWARD_POINT_MAX,
        |trim(ta.BILL_ID) as BILL_ID,
        |ta.REWARD_BILL_NUM as REWARD_BILL_NUM,
        |trim(ta.CHD_REWARD_MD) as CHD_REWARD_MD,
        |trim(ta.CSH_REWARD_MD) as CSH_REWARD_MD,
        |ta.CSH_REWARD_POINT_RATE as CSH_REWARD_POINT_RATE,
        |ta.CSH_REWARD_POINT_MAX as CSH_REWARD_POINT_MAX,
        |ta.CSH_REWARD_DAY_MAX as CSH_REWARD_DAY_MAX,
        |trim(ta.BUSS_TP) as BUSS_TP
        |
        |from TBL_UMSVC_PRIZE_LVL_ADD_RULE ta
        |
        | """.stripMargin)

    println("JOB_HV_47------>results:"+results.count())
    if(!Option(results).isEmpty){
      results.registerTempTable("spark_hive_prize_lvl_add_rule")
      sqlContext.sql(s"use $hive_dbname")
      sqlContext.sql("truncate table  hive_prize_lvl_add_rule")
      sqlContext.sql("insert into table hive_prize_lvl_add_rule select * from spark_hive_prize_lvl_add_rule")
      println("insert into table hive_prize_lvl_add_rule successfully!")
      val usedTime = System.currentTimeMillis() - currntTime
      val ss:Int =((System.currentTimeMillis() - currntTime)/1000).toInt
      val MM:Int = ss/60
      val hh:Int = MM/60
      val dd:Int = hh/24
      println("运行 JOB_HV_47 花费 :"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedTime+"毫秒")

    }else{
      println("加载的表spark_hive_prize_lvl_add_rule中无数据！")
    }
  }


  /**
    * hive-job-48/08-29
    * hive_prize_bas->tbl_umsvc_prize_bas
    *
    * @author tzq
    * @param sqlContext
    * @return
    */
  def JOB_HV_48(implicit sqlContext: HiveContext) = {
    val currntTime =System.currentTimeMillis()

    println("###JOB_HV_48(hive_prize_bas->tbl_umsvc_prize_bas)")
    val df = sqlContext.readDB2_MGM(s"$schemas_mgmdb.TBL_UMSVC_PRIZE_BAS")

    df.registerTempTable("db2_tbl_umsvc_prize_bas")
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
    println("###JOB_HV_48--------->results : "+results.count())

    if(!Option(results).isEmpty){
      results.registerTempTable("spark_tbl_umsvc_prize_bas")
      sqlContext.sql(s"use $hive_dbname")
      sqlContext.sql("truncate table  hive_prize_bas")
      sqlContext.sql("insert into table hive_prize_bas select * from spark_tbl_umsvc_prize_bas")
      println("insert into table hive_prize_bas successfully!")
      val usedTime = System.currentTimeMillis() - currntTime
      val ss:Int =((System.currentTimeMillis() - currntTime)/1000).toInt
      val MM:Int = ss/60
      val hh:Int = MM/60
      val dd:Int = hh/24
      println("运行 JOB_HV_48 花费 :"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedTime+"毫秒")

    }else{
      println("加载的表TBL_UMSVC_PRIZE_BAS中无数据！")
    }

  }

  /**
    * hive-job-54/08-29
    * hive_cashier_bas_inf->tbl_chacc_cashier_bas_inf
    *
    * @author tzq
    * @param sqlContext
    * @return
    */
  def JOB_HV_54(implicit sqlContext: HiveContext) = {
    val currntTime =System.currentTimeMillis()

    println("###JOB_HV_54(tbl_chacc_cashier_bas_inf)")
    val df = sqlContext.readDB2_ACC(s"$schemas_accdb.TBL_CHACC_CASHIER_BAS_INF")

    df.registerTempTable("db2_tbl_chacc_cashier_bas_inf")
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

    println("###JOB_HV_54--------->results："+results.count())
    if(!Option(results).isEmpty){
      results.registerTempTable("spark_bl_chacc_cashier_bas_inf")
      sqlContext.sql(s"use $hive_dbname")
      sqlContext.sql("truncate table  hive_cashier_bas_inf")
      sqlContext.sql("insert into table hive_cashier_bas_inf select * from spark_bl_chacc_cashier_bas_inf")
      println("insert into table hive_cashier_bas_inf successfully!")
      val usedTime = System.currentTimeMillis() - currntTime
      val ss:Int =((System.currentTimeMillis() - currntTime)/1000).toInt
      val MM:Int = ss/60
      val hh:Int = MM/60
      val dd:Int = hh/24
      println("运行 JOB_HV_54 花费 :"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedTime+"毫秒")

    }else{
      println("加载的表tbl_chacc_cashier_bas_inf中无数据！")
    }

  }



  /**
    * hive-job-67
    * Hive_signer_log -->tbl_umtxn_signer_log
    *
    *@author tzq
    * @param sqlContext
    * @return
    */
  def JOB_HV_67(implicit sqlContext: HiveContext) = {
    val currntTime =System.currentTimeMillis()

    println("###JOB_HV_67(Hive_signer_log -->tbl_umtxn_signer_log)")
    val df = sqlContext.readDB2_ACC(s"$schemas_accdb.TBL_UMTXN_SIGNER_LOG")

    df.registerTempTable("db2_tbl_umtxn_signer_log")
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

    println("###JOB_HV_67---------->results："+results.count())

    if(!Option(results).isEmpty){
      results.registerTempTable("spark_db2_tbl_umtxn_signer_log")
      sqlContext.sql(s"use $hive_dbname")
      sqlContext.sql("truncate table  hive_signer_log")
      sqlContext.sql("insert into table hive_signer_log select * from spark_db2_tbl_umtxn_signer_log")
      println("insert into table hive_signer_log successfully!")
      val usedTime = System.currentTimeMillis() - currntTime
      val ss:Int =((System.currentTimeMillis() - currntTime)/1000).toInt
      val MM:Int = ss/60
      val hh:Int = MM/60
      val dd:Int = hh/24
      println("运行 JOB_HV_67 花费 :"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedTime+"毫秒")

    }else{
      println("加载的表tbl_umtxn_signer_log中无数据！")
    }

  }


  /**
    * hive-job-68
    * hive_cashier_point_acct_oper_dtl-->tbl_umtxn_cashier_point_acct_oper_dtl
    *
    * @author tzq
    * @param sqlContext
    * @return
    */
  def JOB_HV_68(implicit sqlContext: HiveContext) = {
    val currntTime =System.currentTimeMillis()

    println("###JOB_HV_68(tbl_umtxn_cashier_point_acct_oper_dtl)")
    val df = sqlContext.readDB2_ACC(s"$schemas_accdb.TBL_UMTXN_CASHIER_POINT_ACCT_OPER_DTL")
    df.registerTempTable("db2_tbl_umtxn_cashier_point_acct_oper_dtl")
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
    println("###JOB_HV_68---------->results："+results.count())

    if(!Option(results).isEmpty){
      results.registerTempTable("spark_db2_tbl_umtxn_cashier_point_acct_oper_dtl")
      sqlContext.sql(s"use $hive_dbname")
      sqlContext.sql("truncate table  hive_cashier_point_acct_oper_dtl")
      sqlContext.sql("insert into table hive_cashier_point_acct_oper_dtl select * from spark_db2_tbl_umtxn_cashier_point_acct_oper_dtl")
      println("insert into table hive_cashier_point_acct_oper_dtl successfully!")
      val usedTime = System.currentTimeMillis() - currntTime
      val ss:Int =((System.currentTimeMillis() - currntTime)/1000).toInt
      val MM:Int = ss/60
      val hh:Int = MM/60
      val dd:Int = hh/24
      println("运行 JOB_HV_68 花费 :"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedTime+"毫秒")

    }else{
      println("加载的表TBL_UMTXN_CASHIER_POINT_ACCT_OPER_DTL中无数据！")
    }
  }

  /**
    * JOB_HV_69/10-14
    * HIVE_PRIZE_LVL->tbl_umsvc_prize_lvl
    * Code by Xue
    *
    * @param sqlContext
    * @return
    */
  def JOB_HV_69 (implicit sqlContext: HiveContext) = {
    val currntTime =System.currentTimeMillis()

    val df2_1 = sqlContext.readDB2_MGM("ch_mgmdb.tbl_umsvc_prize_lvl")
    df2_1.registerTempTable("tbl_umsvc_prize_lvl")
    val results = sqlContext.sql(
      """
        |select
        |ta.LOC_ACTIVITY_ID as LOC_ACTIVITY_ID,
        |trim(ta.PRIZE_TP) as PRIZE_TP,
        |ta.PRIZE_LVL as  PRIZE_LVL,
        |ta.PRIZE_ID_LVL as PRIZE_ID_LVL,
        |trim(ta.ACTIVITY_PLAT) as ACTIVITY_PLAT,
        |ta.PRIZE_ID as PRIZE_ID,
        |trim(ta.RUN_ST) as RUN_ST,
        |trim(ta.SYNC_ST) as SYNC_ST,
        |ta.SYNC_BAT_NO as SYNC_BAT_NO,
        |ta.LVL_PRIZE_NUM as LVL_PRIZE_NUM,
        |trim(ta.PRIZE_LVL_DESC) as PRIZE_LVL_DESC,
        |trim(ta.REPRIZE_LIMIT) as REPRIZE_LIMIT,
        |ta.PRIZE_PAY_TP as PRIZE_PAY_TP,
        |ta.CYCLE_PRIZE_NUM as CYCLE_PRIZE_NUM,
        |ta.CYCLE_SPAN as CYCLE_SPAN,
        |trim(ta.CYCLE_UNIT) as CYCLE_UNIT,
        |trim(ta.PROGRS_IN) as PROGRS_IN,
        |ta.SEG_PRIZE_NUM as SEG_PRIZE_NUM,
        |ta.MIN_PRIZE_TRANS_AT  as MIN_PRIZE_TRANS_AT,
        |ta.MAX_PRIZE_TRANS_AT as MAX_PRIZE_TRANS_AT,
        |ta.PRIZE_AT as PRIZE_AT,
        |trim(ta.OPER_IN) as OPER_IN,
        |ta.EVENT_ID as EVENT_ID,
        |ta.REC_ID as REC_ID,
        |trim(ta.REC_UPD_USR_ID) as REC_UPD_USR_ID,
        |ta.REC_UPD_TS as REC_UPD_TS,
        |ta.REC_CRT_TS as REC_CRT_TS,
        |ta.VER_NO as VER_NO
        |from TBL_UMSVC_PRIZE_LVL ta
        | """.stripMargin)
    println("JOB_HV_69------>results:"+results.count())
    if(!Option(results).isEmpty){
      results.registerTempTable("spark_hive_prize_lvl")
      sqlContext.sql(s"use $hive_dbname")
      sqlContext.sql("truncate table  HIVE_PRIZE_LVL")
      sqlContext.sql("insert into table HIVE_PRIZE_LVL select * from spark_hive_prize_lvl")
      println("insert into table HIVE_PRIZE_LVL successfully!")
      val usedTime = System.currentTimeMillis() - currntTime
      val ss:Int =((System.currentTimeMillis() - currntTime)/1000).toInt
      val MM:Int = ss/60
      val hh:Int = MM/60
      val dd:Int = hh/24
      println("运行 JOB_HV_69 花费 :"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedTime+"毫秒")

    }else{
      println("加载的表spark_hive_prize_lvl中无数据！")
    }
  }


  /**
    *  hive-job-70 2016-09-12
    *  tbl_inf_source_class -> hive_inf_source_class
    *
    * @author winslow yang
    * @param sqlContext
    */
  def JOB_HV_70(implicit sqlContext: HiveContext) = {
    val currntTime =System.currentTimeMillis()

    println("###JOB_HV_70(tbl_inf_source_class -> hive_inf_source_class)")
    sqlContext.sql(s"use $hive_dbname")
    val df = sqlContext.readDB2_ACC(s"$schemas_accdb.TBL_INF_SOURCE_CLASS")
    df.registerTempTable("db2_inf_source_class")
    val results = sqlContext.sql(
      """
        |select
        |access_nm,
        |class
        |from
        |db2_inf_source_class
      """.stripMargin
    )
    println("JOB_HV_70------>results:"+results.count())
    if(!Option(results).isEmpty){
      results.registerTempTable("spark_inf_source_class")
      sqlContext.sql("truncate table hive_inf_source_class")
      sqlContext.sql("insert into table hive_inf_source_class select * from spark_inf_source_class")
      println("insert into table hive_inf_source_class successfully!")
      val usedTime = System.currentTimeMillis() - currntTime
      val ss:Int =((System.currentTimeMillis() - currntTime)/1000).toInt
      val MM:Int = ss/60
      val hh:Int = MM/60
      val dd:Int = hh/24
      println("运行 JOB_HV_70 花费 :"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedTime+"毫秒")

    }else{
      println("加载的表tbl_inf_source_class中无数据！")
    }
  }


  /**
    *  hive-job-72 2016-11-2
    *  hive_bill_order_aux_inf->viw_chmgm_bill_order_aux_inf
    *
    * @author tzq
    * @param sqlContext
    */
  def JOB_HV_72(implicit sqlContext: HiveContext) = {
    val currntTime =System.currentTimeMillis()

    println("###JOB_HV_72(hive_bill_order_aux_inf->viw_chmgm_bill_order_aux_inf)")
    sqlContext.sql(s"use $hive_dbname")
    val df = sqlContext.readDB2_MGM(s"$schemas_mgmdb.viw_chmgm_bill_order_aux_inf")
    df.registerTempTable("db2_viw_chmgm_bill_order_aux_inf")
    val results = sqlContext.sql(
      """
        |select
        |trim(bill_order_id),
        |trim(mchnt_cd),
        |mchnt_nm,
        |trim(sub_mchnt_cd),
        |trim(cdhd_usr_id),
        |sub_mchnt_nm,
        |related_usr_id,
        |trim(cups_trace_number),
        |trim(trans_tm),
        |case
        |	when
        |		substr(trans_dt,1,4) between '0001' and '9999' and substr(trans_dt,5,2) between '01' and '12' and
        |		substr(trans_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(trans_dt,1,4),substr(trans_dt,5,2),substr(trans_dt,7,2))),9,2)
        |	then concat_ws('-',substr(trans_dt,1,4),substr(trans_dt,5,2),substr(trans_dt,7,2))
        |	else null
        |end as trans_dt,
        |trim(orig_trans_seq),
        |trim(trans_seq),
        |trim(mobile_order_id),
        |trim(acp_order_id),
        |trim(delivery_prov_cd),
        |trim(delivery_city_cd),
        |trim(delivery_district_cd),
        |trim(delivery_zip_cd),
        |delivery_address,
        |trim(receiver_nm),
        |trim(receiver_mobile),
        |delivery_time_desc,
        |invoice_desc,
        |trans_at,
        |refund_at,
        |trim(order_st),
        |order_crt_ts,
        |order_timeout_ts,
        |trim(card_no),
        |trim(order_chnl),
        |trim(order_ip),
        |device_inf,
        |remark,
        |rec_crt_ts,
        |trim(crt_cdhd_usr_id),
        |rec_upd_ts,
        |trim(upd_cdhd_usr_id)
        |
        |from
        |db2_viw_chmgm_bill_order_aux_inf
        |
      """.stripMargin
    )
    println("JOB_HV_72------>results:"+results.count())
    if(!Option(results).isEmpty){
      results.registerTempTable("spark_db2_viw_chmgm_bill_order_aux_inf")
      sqlContext.sql("truncate table hive_bill_order_aux_inf")
      sqlContext.sql("insert into table hive_bill_order_aux_inf select * from spark_db2_viw_chmgm_bill_order_aux_inf")
      println("insert into table hive_bill_order_aux_inf successfully!")
      val usedTime = System.currentTimeMillis() - currntTime
      val ss:Int =((System.currentTimeMillis() - currntTime)/1000).toInt
      val MM:Int = ss/60
      val hh:Int = MM/60
      val dd:Int = hh/24
      println("运行 JOB_HV_72 花费 :"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedTime+"毫秒")

    }else{
      println("加载的视图：viw_chmgm_bill_order_aux_inf 中无数据！")
    }
  }

  /**
    *  hive-job-73 2016-11-2
    * hive_bill_sub_order_detail_inf->viw_chmgm_bill_sub_order_detail_inf
    *
    * @author tzq
    * @param sqlContext
    */
  def JOB_HV_73(implicit sqlContext: HiveContext) = {
    val currntTime =System.currentTimeMillis()

    println("###JOB_HV_73(hive_bill_sub_order_detail_inf->viw_chmgm_bill_sub_order_detail_inf)")
    sqlContext.sql(s"use $hive_dbname")
    val df = sqlContext.readDB2_MGM(s"$schemas_mgmdb.viw_chmgm_bill_sub_order_detail_inf")
    df.registerTempTable("db2_viw_chmgm_bill_sub_order_detail_inf")
    val results = sqlContext.sql(
      """
        |select
        |bill_sub_order_id,
        |trim(bill_order_id),
        |trim(mchnt_cd),
        |mchnt_nm,
        |trim(sub_mchnt_cd),
        |sub_mchnt_nm,
        |trim(bill_id),
        |bill_price,
        |trim(trans_seq),
        |refund_reason,
        |trim(order_st),
        |rec_crt_ts,
        |trim(crt_cdhd_usr_id),
        |rec_upd_ts,
        |trim(upd_cdhd_usr_id),
        |order_timeout_ts,
        |case
        |	when
        |		substr(trans_dt,1,4) between '0001' and '9999' and substr(trans_dt,5,2) between '01' and '12' and
        |		substr(trans_dt,7,2) between '01' and substr(last_day(concat_ws('-',substr(trans_dt,1,4),substr(trans_dt,5,2),substr(trans_dt,7,2))),9,2)
        |	then concat_ws('-',substr(trans_dt,1,4),substr(trans_dt,5,2),substr(trans_dt,7,2))
        |	else null
        |end as trans_dt,
        |related_usr_id,
        |trans_process,
        |response_code,
        |response_msg
        |from
        |db2_viw_chmgm_bill_sub_order_detail_inf
        |
      """.stripMargin
    )
    println("JOB_HV_73------>results:"+results.count())
    if(!Option(results).isEmpty){
      results.registerTempTable("spark_db2_viw_chmgm_bill_sub_order_detail_inf")
      sqlContext.sql("truncate table hive_bill_sub_order_detail_inf")
      sqlContext.sql("insert into table hive_bill_sub_order_detail_inf select * from spark_db2_viw_chmgm_bill_sub_order_detail_inf")
      println("insert into table hive_bill_sub_order_detail_inf successfully!")
      val usedTime = System.currentTimeMillis() - currntTime
      val ss:Int =((System.currentTimeMillis() - currntTime)/1000).toInt
      val MM:Int = ss/60
      val hh:Int = MM/60
      val dd:Int = hh/24
      println("运行 JOB_HV_73 花费 :"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedTime+"毫秒")

    }else{
      println("加载的视图：viw_chmgm_bill_sub_order_detail_inf 中无数据！")
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
    val currntTime =System.currentTimeMillis()

    val df2_1 = sqlContext.readDB2_MGM(s"$schemas_mgmdb.tbl_chmgm_access_static_inf ")
    df2_1.registerTempTable("tbl_chmgm_access_static_inf")

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


    println("JOB_HV_75------>results:"+results.count())
    if(!Option(results).isEmpty){
      results.registerTempTable("spark_hive_access_static_inf")
      sqlContext.sql(s"use $hive_dbname")
      sqlContext.sql("truncate table  hive_access_static_inf")
      sqlContext.sql("insert into table hive_access_static_inf select * from spark_hive_access_static_inf")
      println("insert into table hive_access_static_inf successfully!")
      val usedTime = System.currentTimeMillis() - currntTime
      val ss:Int =((System.currentTimeMillis() - currntTime)/1000).toInt
      val MM:Int = ss/60
      val hh:Int = MM/60
      val dd:Int = hh/24
      println("运行 JOB_HV_75 花费 :"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedTime+"毫秒")

    }else{
      println("加载的表spark_hive_access_static_inf中无数据！")
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
    val currntTime =System.currentTimeMillis()

    val df2_1 = sqlContext.readDB2_MGM(s"$schemas_mgmdb.tbl_chmgm_region_cd ")
    df2_1.registerTempTable("tbl_chmgm_region_cd")

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


    println("JOB_HV_76------>results:"+results.count())
    if(!Option(results).isEmpty){
      results.registerTempTable("spark_hive_region_cd")
      sqlContext.sql(s"use $hive_dbname")
      sqlContext.sql("truncate table  hive_region_cd")
      sqlContext.sql("insert into table hive_region_cd select * from spark_hive_region_cd")
      println("insert into table hive_region_cd successfully!")
      val usedTime = System.currentTimeMillis() - currntTime
      val ss:Int =((System.currentTimeMillis() - currntTime)/1000).toInt
      val MM:Int = ss/60
      val hh:Int = MM/60
      val dd:Int = hh/24
      println("运行 JOB_HV_76 花费 :"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedTime+"毫秒")

    }else{
      println("加载的表spark_hive_region_cd中无数据！")
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
    val currntTime =System.currentTimeMillis()

    println("###JOB_HV_79(tbl_aconl_ins_bas)")
    val df = sqlContext.readDB2_ACC(s"$schemas_accdb.tbl_aconl_ins_bas")

    df.registerTempTable("tbl_aconl_ins_bas")
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

    println("###JOB_HV_79--------->results："+results.count())
    if(!Option(results).isEmpty){
      results.registerTempTable("spark_tbl_aconl_ins_bas")
      sqlContext.sql(s"use $hive_dbname")
      sqlContext.sql("truncate table  hive_aconl_ins_bas")
      sqlContext.sql("insert into table hive_aconl_ins_bas select * from spark_tbl_aconl_ins_bas")
      println("insert into table hive_aconl_ins_bas successfully!")
      val usedTime = System.currentTimeMillis() - currntTime
      val ss:Int =((System.currentTimeMillis() - currntTime)/1000).toInt
      val MM:Int = ss/60
      val hh:Int = MM/60
      val dd:Int = hh/24
      println("运行 JOB_HV_79 花费 :"+dd+"天"+(hh-dd*24)+"时"+(MM-hh*60)+"分"+(ss-MM*60)+"秒 , 合计："+usedTime+"毫秒")

    }else{
      println("加载的表spark_tbl_aconl_ins_bas中无数据！")
    }

  }



}// ### END LINE ###
