package com.unionpay.constant

/**
  * 常量定义
  * Created by tzq on 2016/10/10.
  */
object Constants {

  //PARAMS FOR ETL
  val TODAY_DT="today_dt"
  val START_DT="start_dt"
  val END_DT="end_dt"

  //UPSQL Connection
  val UPSQL_USER="upsql.user"
  val UPSQL_PASSWORD="upsql.password"
  val UPSQL_DRIVER = "upsql.driver"
  val UPSQL_URL="upsql.url"

  //upsql time scheduler params
  val UPSQL_PARAMS_URL="upsql.params.url"
  val UPSQL_PARAMS_TABLE="upsql.params.table"

  //Db2 connection
  val DB2_USER_SWT="db2.user.swt"
  val DB2_USER_ACC="db2.user.acc"
  val DB2_USER_MGM="db2.user.mgm"
  val DB2_USER_UPOUP="db2.user.upoup"
  val DB2_USER_MNSVC="db2.user.mnsvc"
  val DB2_USER_WLONL="db2.user.wlonl"


  val DB2_PASSWORD_SWT ="db2.password.swt"
  val DB2_PASSWORD_ACC ="db2.password.acc"
  val DB2_PASSWORD_MGM ="db2.password.mgm"
  val DB2_PASSWORD_UPOUP="db2.password.upoup"
  val DB2_PASSWORD_MNSVC="db2.password.mnsvc"
  val DB2_PASSWORD_WLONL="db2.password.wlonl"

  val DB2_URL_SWTDB="db2.url_swtdb"
  val DB2_URL_ACCDB="db2.url_accdb"
  val DB2_URL_MGMDB="db2.url_mgmdb"
  val DB2_URL_UPOUPDB="db2.url_upoupdb"
  val DB2_URL_MNSVCDB="db2.url_mnsvcdb"
  val DB2_URL_WLONLDB="db2.url_wlonldb"

  val DB2_DRIVER="db2.driver"

  val SCHEMAS_SWTDB="db2.swtdb_schemas_name"
  val SCHEMAS_ACCDB="db2.accdb_schemas_name"
  val SCHEMAS_MGMDB="db2.mgmdb_schemas_name"
  val SCHEMAS_UPOUPDB="db2.upoupdb_schemas_name"
  val SCHEMAS_MNSVCDB="db2.mnsvcdb_schemas_name"
  val SCHEMAS_WLONLDB="db2.wlonldb_schemas_name"

  //Hive Database_name
  val HIVE_DBNAME = "hive.dbname"

  val UPW_PROP="upw.prop"

  //SRC Hive Connectio
  val SRCHIVE_USER ="srchive.user"
  val SRCHIVE_PASSWORD ="srchive.password"
  val SRCHIVE_URL_HBKDB="srchive.url_hbkdb"
  val SRCHIVE_DRIVER ="srchive.driver"


  //Config UP Hive repository
  val UP_NAMENODE ="up.namenode"
  val UP_HIVEDATAROOT ="up.hivedataroot"

}