package com.unionpay.jdbc

import java.sql.{DriverManager, ResultSet}
import java.util.Properties

import com.unionpay.conf.ConfigurationManager
import com.unionpay.constant.Constants
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode}

/**
  * 获取时间调度的参数start_dt 和end_dt
  * Created by tzq on 2016/10/20.
  */
object UPSQL_TIMEPARAMS_JDBC {

  private lazy val url=ConfigurationManager.getProperty(Constants.UPSQL_PARAMS_URL)
  private lazy val driver=ConfigurationManager.getProperty(Constants.UPSQL_DRIVER)
  private lazy val user=ConfigurationManager.getProperty(Constants.UPSQL_USER)
  private lazy val password=ConfigurationManager.getProperty(Constants.UPSQL_PASSWORD)
  private lazy val tableName=ConfigurationManager.getProperty(Constants.UPSQL_PARAMS_TABLE)
  private lazy val properties = new Properties()
  properties.put("user", user)
  properties.put("password", password)
  properties.put("driver", driver)

  /**
    * 根据表名来取时间参数
    * @param sqlContext
    * @return
    */
  def readTimeParams(sqlContext:HiveContext):Row={
   sqlContext.read.jdbc(url, tableName, properties).select("start_dt","end_dt").first()
  }

  /**
    * 根据表名来取时间参数
    * @return
    */
  def getTimeParams(): ResultSet ={

    Class.forName(driver)
    val conn = DriverManager.getConnection(url,user,password)
    val sql: String = s"select start_dt,end_dt from $tableName"
    println("###### " + sql + " ######")
    val stat = conn.prepareStatement(sql)
    stat.executeQuery()
  }

  /**
    * 将调度的时间参数插入到mysql中（overwrite）
    * @param sc
    * @param sqlContext
    * @param start_dt 开始日期 (2016-10-20)
    * @param end_dt 结束日期
    * @param numPartitions 可选
    */
  def save2Mysql(sc: SparkContext, sqlContext: HiveContext, start_dt: String, end_dt: String, numPartitions: Option[Int] = None): Unit = {

    val schema = StructType(StructField("start_dt", StringType) :: StructField("end_dt", StringType) :: Nil)
    val data = sc.parallelize(List((start_dt, end_dt))).map(line => Row.apply(line._1, line._2))
    val df = sqlContext.createDataFrame(data, schema)

    numPartitions match {
      case Some(num) => df.repartition(num).write.mode(SaveMode.Overwrite).jdbc(url, tableName, properties)
      case None => df.write.mode(SaveMode.Overwrite).jdbc(url, tableName, properties)
    }
    println("### insert to table (upw_time_params) successful ! ###")
  }


}