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


  //Db2 Connection
  val DB2_USER="db2.user"
  val DB2_PASSWORD ="db2.password"
  val DB2_URL_ACCDB="db2.url_accdb"
  val DB2_URL_MGMDB="db2.url_mgmdb"
  val DB2_DRIVER="db2.driver"



  //SRC Hive Connectio
  val SRCHIVE_USER ="srchive.user"
  val SRCHIVE_PASSWORD ="srchive.password"
  val SRCHIVE_URL_HBKDB="srchive.url_hbkdb"
  val SRCHIVE_DRIVER ="srchive.driver"

}