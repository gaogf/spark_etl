package com.unionpay.etl


import java.io.FileOutputStream
import java.io.File
import java.util.{Date, Properties}

import com.typesafe.config.Config
import com.unionpay.conf.ConfigurationManager
import com.unionpay.constant.Constants
import com.unionpay.utils.DateUtils

/**
  * 时间调度器
  * Created by tzq on 2016/10/19.
  */
object TimeScheduler {
  private lazy val prop =new Properties()

  def main(args: Array[String]): Unit = {
    val filePath=this.getClass.getClassLoader.getResource(ConfigurationManager.getProperty(Constants.UPW_PROP)).getPath()
    //若未传值则取当前系统时间减一天（即：昨天）
    val start_dt=if(args.length>1) args(0) else DateUtils.getYesterdayByJob(DateUtils.dateFormat.format(new Date()))
    val end_dt=if(args.length>1) args(1) else DateUtils.getYesterdayByJob(DateUtils.dateFormat.format(new Date()))

    load()//加载配置文件

    writeProperties(Constants.START_DT,start_dt,filePath)
    writeProperties(Constants.END_DT,end_dt,filePath)
  }

  def load(): Unit ={
    val in=this.getClass.getClassLoader.getResourceAsStream(ConfigurationManager.getProperty(Constants.UPW_PROP))
    try{
      prop.load(in)
    }catch {
      case e: Exception => e.printStackTrace()
    }finally {
      in.close()
    }

  }

  /**
    * 更新配置文件
    * @param key
    * @param value
    * @param filePath
    */
  def writeProperties(key:String,value:String,filePath:String): Unit ={
    val fos=new FileOutputStream(filePath,false)
    try {
      prop.setProperty(key,value)
      prop.store(fos, "SET SCHEDULER TIME")
      println(s"当前修改的键为 : $key 值为：$value")
    } catch {
      case e: Exception => e.printStackTrace()
    }finally {
      fos.close()
    }
  }
}
