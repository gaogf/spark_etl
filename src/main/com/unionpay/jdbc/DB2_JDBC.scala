package com.unionpay.jdbc

import java.sql.DriverManager

import com.unionpay.conf.ConfigurationManager
import com.unionpay.constant.Constants
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * DB2 CONNECTION INFO
  * Created by tzq on 2016/10/10.
  */
object DB2_JDBC {
  private lazy val user =ConfigurationManager.getProperty(Constants.DB2_USER)
  private lazy val password =ConfigurationManager.getProperty(Constants.DB2_PASSWORD)
  private lazy val url_accdb: String =ConfigurationManager.getProperty(Constants.DB2_URL_ACCDB)
  private lazy val url_mgmdb: String =ConfigurationManager.getProperty(Constants.DB2_URL_MGMDB)
  private lazy val driver =ConfigurationManager.getProperty(Constants.DB2_DRIVER)

  implicit class ReadDB2(sqlContext: SQLContext) {
    def jdbc_accdb_DF(table: String, numPartitions: Int = 1): DataFrame = {
      val map = Map(
        "url" -> url_accdb,
        "user" -> user,
        "password" -> password,
        "driver" -> driver,
        "dbtable" -> table,
        "numPartitions" -> s"$numPartitions")
      sqlContext.read.format("jdbc").options(map).load()
    }
    def jdbc_mgmdb_DF(table: String, numPartitions: Int = 1): DataFrame = {
      val map = Map(
        "url" -> url_mgmdb,
        "user" -> user,
        "password" -> password,
        "driver" -> driver,
        "dbtable" -> table,
        "numPartitions" -> s"$numPartitions")
      sqlContext.read.format("jdbc").options(map).load()
    }
  }
}
