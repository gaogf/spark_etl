package com.unionpay.conf

import java.util.Properties

/**
  * 配置管理类
  * Created by tzq on 2016/10/10.
  */
object ConfigurationManager {
  val prop=new Properties()

 try{
  lazy val in=ConfigurationManager.getClass.getClassLoader.getResourceAsStream("upw.properties")
   prop.load(in)
 }catch {
   case ex: Exception => println(ex)
 }

  def getProperty(key:String)={
    prop.getProperty(key)
  }
}
