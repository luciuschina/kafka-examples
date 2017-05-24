package com.haozhuo.spark

import org.apache.spark.sql.SparkSession

/**
 * Created by hadoop on 5/23/17.
 */
case class Users2(id:Integer,name:String)
case class User11(id:Integer,name:String,salary:String)
object App {
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .config("spark.app.name", "xx")
      //.config("spark.sql.warehouse.dir","hdfs://namenode:9000/user/hive/warehouse")
      .config("spark.master", "local[4]")
     // .config("hive.metastore.uris","thrift://192.168.1.150:9083")
      .enableHiveSupport
      .getOrCreate()

  }
}
