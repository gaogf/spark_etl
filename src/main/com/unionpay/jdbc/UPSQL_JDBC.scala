package com.unionpay.jdbc

import java.sql.DriverManager
import java.util.Properties
import com.unionpay.conf.ConfigurationManager
import com.unionpay.constant.Constants
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
  * UPSQL CONNECTON INFO
  * Created by tzq on 2016/10/10.
  */
object UPSQL_JDBC {

  private lazy val url=ConfigurationManager.getProperty(Constants.UPSQL_URL)
  private lazy val driver=ConfigurationManager.getProperty(Constants.UPSQL_DRIVER)
  private lazy val user=ConfigurationManager.getProperty(Constants.UPSQL_USER)
  private lazy val password=ConfigurationManager.getProperty(Constants.UPSQL_PASSWORD)
  private lazy val properties = new Properties()
  properties.put("user", user)
  properties.put("password", password)
  properties.put("driver", driver)

  /**
    * 将DataFrame组成的临时表插入到数据库中
    * @param df
    */
  implicit class DataFrame2Mysql(df: DataFrame) {
    def save2Mysql(table: String, numPartitions: Option[Int] = None) = {
      numPartitions match {
        case Some(num) => df.repartition(num).write.mode(SaveMode.Append).jdbc(url, table, properties)
        case None => df.write.mode(SaveMode.Append).jdbc(url, table, properties)
      }
    }
  }

  /**
    * 根据表名、字段名、和指定的时间范围来进行删除
    * @param tableName
    * @param field
    * @param beginTime
    * @param endTime
    */
  def delete(tableName:String,field:String,beginTime:String,endTime:String): Unit ={

    Class.forName(driver)
    val conn = DriverManager.getConnection(url,user,password)
    val sql: String = s"delete from $tableName where $field >= '$beginTime' and $field <= '$endTime'"
    println("###### " + sql + "######")
    try {
      val stat = conn.prepareStatement(sql)
      val rs = stat.executeUpdate()
      println("###### Affect Rows：" + rs + "  ######")
    } catch {
      case e:Exception =>e.printStackTrace
    } finally {
      conn.close
    }
  }

}