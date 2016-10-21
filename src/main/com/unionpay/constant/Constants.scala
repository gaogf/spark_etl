package com.unionpay.constant

/**
  * 相关常量定义
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
  val DB2_USER="db2.user"
  val DB2_PASSWORD ="db2.password"
  val DB2_URL_ACCDB="db2.url_accdb"
  val DB2_URL_MGMDB="db2.url_mgmdb"
  val DB2_DRIVER="db2.driver"

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