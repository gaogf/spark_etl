package com.unionpay.jdbc

import java.sql.DriverManager

import com.unionpay.conf.ConfigurationManager
import com.unionpay.constant.Constants

/**
  * 从源Hive仓库中抽取数据
  * Created by tzq on 2016/10/13.
  */
object SRCHIVE_JDBC {

  private lazy val user =ConfigurationManager.getProperty(Constants.SRCHIVE_USER)
  private lazy val password = ConfigurationManager.getProperty(Constants.SRCHIVE_PASSWORD)
  private lazy val url_hbkdb: String = ConfigurationManager.getProperty(Constants.SRCHIVE_URL_HBKDB)
  private lazy val driver =ConfigurationManager.getProperty(Constants.SRCHIVE_DRIVER)

  def read_hbkdb(tableName:String,partField:String,PartBeginTime:String,PartEndTime:String): Unit ={

    Class.forName(driver)
    val connection  = DriverManager.getConnection(url_hbkdb,user,password)
    val sql: String = s"select * from $tableName where $partField >= '$PartBeginTime' and $partField <= '$PartEndTime'"

    println("######" + sql + "######")

    try {
      val statement  = connection.prepareStatement(sql)
      statement.executeQuery()
      println("### 执行查询成功！###")

    } catch {
      case e:Exception =>e.printStackTrace
    } finally {
      connection.close
    }
  }

}
