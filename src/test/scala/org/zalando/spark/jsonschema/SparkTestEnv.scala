package org.zalando.spark.jsonschema

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SQLContext

object SparkTestEnv {

  lazy val sparkContext: SparkContext = {
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")

    new SparkContext(new SparkConf().setMaster("local").setAppName("testapp"))
  }

  lazy val sqlContext: SQLContext = new org.apache.spark.sql.SQLContext(sparkContext)
}
