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
  private lazy val user_acc =ConfigurationManager.getProperty(Constants.DB2_USER_ACC)
  private lazy val password_acc =ConfigurationManager.getProperty(Constants.DB2_PASSWORD_ACC)
  private lazy val user_mgm =ConfigurationManager.getProperty(Constants.DB2_USER_MGM)
  private lazy val password_mgm =ConfigurationManager.getProperty(Constants.DB2_PASSWORD_MGM)
  private lazy val url_accdb: String =ConfigurationManager.getProperty(Constants.DB2_URL_ACCDB)
  private lazy val url_mgmdb: String =ConfigurationManager.getProperty(Constants.DB2_URL_MGMDB)
  private lazy val driver =ConfigurationManager.getProperty(Constants.DB2_DRIVER)

  implicit class ReadDB2_WithUR(sqlContext: SQLContext) {
    def readDB2_ACC(table: String): DataFrame = {
      properties.put("user", user_acc)
      properties.put("password", password_acc)
      properties.put("driver", driver)
      sqlContext.read.jdbc(url_accdb, s"$table with ur --", properties)
    }

    def readDB2_MGM(table: String): DataFrame = {
      properties.put("user", user_mgm)
      properties.put("password", password_mgm)
      properties.put("driver", driver)
      sqlContext.read.jdbc(url_mgmdb, s"$table with ur --", properties)
    }

    def readDB2_ACC_4para(table: String, field: String, start_dt: String, end_dt: String): DataFrame = {
      properties.put("user", user_acc)
      properties.put("password", password_acc)
      properties.put("driver", driver)
      sqlContext.read.jdbc(url_accdb, s"$table where $field >= '$start_dt' and $field <= '$end_dt' with ur --", properties)
    }

    def readDB2_MGM_4para(table: String, field: String, start_dt: String, end_dt: String): DataFrame = {
      properties.put("user", user_mgm)
      properties.put("password", password_mgm)
      properties.put("driver", driver)
      sqlContext.read.jdbc(url_mgmdb, s"$table where $field >= '$start_dt' and $field <= '$end_dt' with ur --", properties)
    }
  }

  implicit class ReadDB2(sqlContext: SQLContext) {
    def jdbc_accdb_DF(table: String, numPartitions: Int = 1): DataFrame = {
      val map = Map(
        "url" -> url_accdb,
        "user" -> user_acc,
        "password" -> password_acc,
        "driver" -> driver,
        "dbtable" -> table,
        "numPartitions" -> s"$numPartitions")
      sqlContext.read.format("jdbc").options(map).load()
    }
    def jdbc_mgmdb_DF(table: String, numPartitions: Int = 1): DataFrame = {
      val map = Map(
        "url" -> url_mgmdb,
        "user" -> user_mgm,
        "password" -> password_mgm,
        "driver" -> driver,
        "dbtable" -> table,
        "numPartitions" -> s"$numPartitions")
      sqlContext.read.format("jdbc").options(map).load()
    }
  }

}
