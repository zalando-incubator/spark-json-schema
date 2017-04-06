package org.zalando.spark.jsonschema

import org.apache.spark.sql.SQLContext
import org.apache.spark.{ SparkConf, SparkContext }

import scala.io.Source

object SparkTestEnv {

  lazy val sparkContext: SparkContext = {
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")

    new SparkContext(new SparkConf().setMaster("local").setAppName("testapp"))
  }

  lazy val sqlContext: SQLContext = new SQLContext(sparkContext)

  def getTestResourceContent(relativePath: String): String = {
    val relPath = getClass.getResource(relativePath)
    require(relPath != null, s"Path can not be reached: $relativePath")
    Source.fromURL(relPath).mkString
  }

}
