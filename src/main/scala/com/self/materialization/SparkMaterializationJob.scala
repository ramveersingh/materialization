package com.self.materialization

import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.slf4j.LoggerFactory

/**
  * Created by ramsingh on 3/1/2018.
  */
object SparkMaterializationJob extends App {
  val logger = LoggerFactory.getLogger(getClass)
  logger.info("Loading Configuration")

  System.setProperty("java.io.tmpdir", "/tmp/spark")

  val config = ConfigFactory.load()
  println(config.getString("spark.test"))

  val conf = new SparkConf().setMaster("local[2]")
            .setAppName("NetworkWordCount")
  val sc = new SparkContext(conf)
  val hiveContext = new HiveContext(sc)
  hiveContext.setConf("hive.metastore.uris", "thrift://10.207.94.138:9083")


  //val employees = hiveContext.sql("SELECT * FROM attunity.employee")
  //employees.show()

  val tables = hiveContext.tableNames()
  tables.foreach(table => println(table))

  val employee = hiveContext.table("employee")
  employee.show()



}
