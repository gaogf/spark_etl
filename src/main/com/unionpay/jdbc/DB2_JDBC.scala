package com.unionpay.jdbc

import java.sql.DriverManager
import java.util.Properties
import java.sql.{Connection, DriverManager}

import com.unionpay.conf.ConfigurationManager
import com.unionpay.constant.Constants
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * DB2 CONNECTION INFO
  * Created by tzq on 2016/10/10.
  */
object DB2_JDBC {
  //账户库
  private lazy val url_accdb: String =ConfigurationManager.getProperty(Constants.DB2_URL_ACCDB)
  private lazy val user_acc =ConfigurationManager.getProperty(Constants.DB2_USER_ACC)
  private lazy val password_acc =ConfigurationManager.getProperty(Constants.DB2_PASSWORD_ACC)
  //管理库
  private lazy val url_mgmdb: String =ConfigurationManager.getProperty(Constants.DB2_URL_MGMDB)
  private lazy val user_mgm =ConfigurationManager.getProperty(Constants.DB2_USER_MGM)
  private lazy val password_mgm =ConfigurationManager.getProperty(Constants.DB2_PASSWORD_MGM)
  //营销库
  private lazy val url_makdb: String =ConfigurationManager.getProperty(Constants.DB2_URL_MAKDB)
  private lazy val user_mak=ConfigurationManager.getProperty(Constants.DB2_USER_MAK)
  private lazy val password_mak=ConfigurationManager.getProperty(Constants.DB2_PASSWORD_MAk)
  //订单库
  private lazy val user_order=ConfigurationManager.getProperty(Constants.DB2_USER_ORDER)
  private lazy val url_orderdb:String =ConfigurationManager.getProperty(Constants.DB2_URL_ORDERDB)
  private lazy val  password_order=ConfigurationManager.getProperty(Constants.DB2_PASSWORD_ORDER)
  //手机后台库
  private lazy val url_mbgdb:String =ConfigurationManager.getProperty(Constants.DB2_URL_MBGDB)
  private lazy  val user_mbg=ConfigurationManager.getProperty(Constants.DB2_USER_MBG)
  private lazy val  password_mbg=ConfigurationManager.getProperty(Constants.DB2_PASSWORD_MBG)

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
    def readDB2_MarketingWith4param(table:String,field:String,start_dt:String,end_dt:String):DataFrame={
      properties.put("user", user_mak)
      properties.put("password", password_mak)
      properties.put("driver", driver)
      sqlContext.read.jdbc(url_makdb, s"$table where $field >= '$start_dt' and $field <= '$end_dt' with ur --", properties)
    }
    //读取订单库
    def readDB2_OrderWith4param(table:String,field:String,start_dt:String,end_dt:String) : DataFrame={
      properties.put("user", user_order)
      properties.put("password", password_order)
      properties.put("driver", driver)
      sqlContext.read.jdbc(url_orderdb, s"$table where $field >= '$start_dt' and $field <= '$end_dt' with ur --", properties)
    }
    def readDB2_Order(table:String):DataFrame ={
      properties.put("user", user_order)
      properties.put("password", password_order)
      properties.put("driver", driver)
      sqlContext.read.jdbc(url_orderdb, s"$table with ur --", properties)
    }
    //读取手机后台库(联机库)
    def readDB2_MbgWith4param(table:String,field:String,start_dt:String,end_dt:String) :DataFrame ={
      properties.put("user", user_mbg)
      properties.put("password", password_mbg)
      properties.put("driver", driver)
      sqlContext.read.jdbc(url_mbgdb, s"$table where $field >= '$start_dt' and $field <= '$end_dt' with ur --", properties)
    }
    def readDB2_MbgWith3param(table:String,start_dt:String,end_dt:String): DataFrame ={
      properties.put("user", user_mbg)
      properties.put("password", password_mbg)
      properties.put("driver", driver)
      sqlContext.read.jdbc(url_mbgdb,s"$table with ur --",properties)
    }
    def readDB2_Mbg(table:String) :DataFrame ={
      properties.put("user", user_mbg)
      properties.put("password", password_mbg)
      properties.put("driver", driver)
      sqlContext.read.jdbc(url_mbgdb, s"$table with ur --", properties)
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
        "url" -> url_makdb,
        "user" -> user_mak,
        "password" -> password_mak,
        "driver" -> driver,
        "dbtable" -> table,
        "numPartitions" -> s"$numPartitions")
      sqlContext.read.format("jdbc").options(map).load()
    }
    def jdbc_orderdb_DF(table :String,numPartitions: Int = 1) :DataFrame ={
      val map = Map(
        "url" -> url_orderdb,
        "user" -> user_order,
        "password" -> password_order,
        "driver" -> driver,
        "dbtable" -> table,
        "numPartitions" -> s"$numPartitions")
      sqlContext.read.format("jdbc").options(map).load()
    }
    def jdbc_mbgdb_DF(table :String,numPartitions: Int = 1):DataFrame ={
      val map = Map(
        "url" -> url_mbgdb,
        "user" -> user_mbg,
        "password" -> password_mbg,
        "driver" -> driver,
        "dbtable" -> table,
        "numPartitions" -> s"$numPartitions")
      sqlContext.read.format("jdbc").options(map).load()

    }
  }

}
