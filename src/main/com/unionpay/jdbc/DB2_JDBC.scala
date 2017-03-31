package com.unionpay.jdbc

import java.util.Properties
import com.unionpay.conf.ConfigurationManager
import com.unionpay.constant.Constants
import org.apache.spark.sql.{DataFrame,SQLContext}

/**
  * DB2 CONNECTION INFO
  * Created by tzq on 2016/10/10.
  */
object DB2_JDBC {
  private lazy val user_acc =ConfigurationManager.getProperty(Constants.DB2_USER_ACC)
  private lazy val password_acc =ConfigurationManager.getProperty(Constants.DB2_PASSWORD_ACC)
  private lazy val user_mgm =ConfigurationManager.getProperty(Constants.DB2_USER_MGM)
  private lazy val password_mgm =ConfigurationManager.getProperty(Constants.DB2_PASSWORD_MGM)
  private lazy val user_upoup=ConfigurationManager.getProperty(Constants.DB2_USER_UPOUP)
  private lazy val password_upoup=ConfigurationManager.getProperty(Constants.DB2_PASSWORD_UPOUP)
  private lazy val user_mnsvc=ConfigurationManager.getProperty(Constants.DB2_USER_MNSVC)
  private lazy val  password_mnsvc=ConfigurationManager.getProperty(Constants.DB2_PASSWORD_MNSVC)
  private lazy  val user_wlonl=ConfigurationManager.getProperty(Constants.DB2_USER_WLONL)
  private lazy val  password_wlonl=ConfigurationManager.getProperty(Constants.DB2_PASSWORD_WLONL)

  private lazy val url_accdb: String =ConfigurationManager.getProperty(Constants.DB2_URL_ACCDB)
  private lazy val url_mgmdb: String =ConfigurationManager.getProperty(Constants.DB2_URL_MGMDB)
  private lazy val url_upoupdb: String =ConfigurationManager.getProperty(Constants.DB2_URL_UPOUPDB)
  private lazy val url_mnsvcdb:String =ConfigurationManager.getProperty(Constants.DB2_URL_MNSVCDB)
  private lazy val url_wlonldb:String =ConfigurationManager.getProperty(Constants.DB2_URL_WLONLDB)

  private lazy val driver =ConfigurationManager.getProperty(Constants.DB2_DRIVER)
  private lazy val properties = new Properties()

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
    //读取财务库
    def readDB2_ACC_4para(table: String, field: String, start_dt: String, end_dt: String): DataFrame = {
      properties.put("user", user_acc)
      properties.put("password", password_acc)
      properties.put("driver", driver)
      sqlContext.read.jdbc(url_accdb, s"$table where $field >= '$start_dt' and $field <= '$end_dt' with ur --", properties)
    }
    //读取管理库
    def readDB2_MGM_4para(table: String, field: String, start_dt: String, end_dt: String): DataFrame = {
      properties.put("user", user_mgm)
      properties.put("password", password_mgm)
      properties.put("driver", driver)
      sqlContext.read.jdbc(url_mgmdb, s"$table where $field >= '$start_dt' and $field <= '$end_dt' with ur --", properties)
    }
    //读取营销库
    def readDB2_MarketingWith4param(table:String,field:String,start_dt:String,end_dt:String):DataFrame={
      properties.put("user", user_upoup)
      properties.put("password", password_upoup)
      properties.put("driver", driver)
      sqlContext.read.jdbc(url_upoupdb, s"$table where $field >= '$start_dt' and $field <= '$end_dt' with ur --", properties)
    }
    //读取订单库
    def readDB2_OrderWith4param(table:String,field:String,start_dt:String,end_dt:String) : DataFrame={
      properties.put("user", user_mnsvc)
      properties.put("password", password_mnsvc)
      properties.put("driver", driver)
      sqlContext.read.jdbc(url_mnsvcdb, s"$table where $field >= '$start_dt' and $field <= '$end_dt' with ur --", properties)
    }
    def readDB2_Order(table:String):DataFrame ={
      properties.put("user", user_mnsvc)
      properties.put("password", password_mnsvc)
      properties.put("driver", driver)
      sqlContext.read.jdbc(url_mnsvcdb, s"$table with ur --", properties)
    }
    //读取手机后台库(联机库)
    def readDB2_MbgWith4param(table:String,field:String,start_dt:String,end_dt:String) :DataFrame ={
      properties.put("user", user_wlonl)
      properties.put("password", password_wlonl)
      properties.put("driver", driver)
      sqlContext.read.jdbc(url_wlonldb, s"$table where $field >= '$start_dt' and $field <= '$end_dt' with ur --", properties)
    }
    def readDB2_MbgWith3param(table:String,start_dt:String,end_dt:String): DataFrame ={
      properties.put("user", user_wlonl)
      properties.put("password", password_wlonl)
      properties.put("driver", driver)
      sqlContext.read.jdbc(url_wlonldb,s"$table with ur --",properties)
    }
    def readDB2_Mbg(table:String) :DataFrame ={
      properties.put("user", user_wlonl)
      properties.put("password", password_wlonl)
      properties.put("driver", driver)
      sqlContext.read.jdbc(url_wlonldb, s"$table with ur --", properties)
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
    def jdbc_makdb_DF(table :String,numPartitions: Int = 1) :DataFrame={
      val map = Map(
        "url" -> url_upoupdb,
        "user" -> user_upoup,
        "password" -> password_upoup,
        "driver" -> driver,
        "dbtable" -> table,
        "numPartitions" -> s"$numPartitions")
      sqlContext.read.format("jdbc").options(map).load()
    }
    def jdbc_orderdb_DF(table :String,numPartitions: Int = 1) :DataFrame ={
      val map = Map(
        "url" -> url_mnsvcdb,
        "user" -> user_mnsvc,
        "password" -> password_mnsvc,
        "driver" -> driver,
        "dbtable" -> table,
        "numPartitions" -> s"$numPartitions")
      sqlContext.read.format("jdbc").options(map).load()
    }
    def jdbc_mbgdb_DF(table :String,numPartitions: Int = 1):DataFrame ={
      val map = Map(
        "url" -> url_wlonldb,
        "user" -> user_wlonl,
        "password" -> password_wlonl,
        "driver" -> driver,
        "dbtable" -> table,
        "numPartitions" -> s"$numPartitions")
      sqlContext.read.format("jdbc").options(map).load()

    }
  }

}
