package org.zalando.spark.jsonschema

import org.apache.spark.sql.SparkSession

import scala.io.Source

object SparkTestEnv {

  lazy val sparkSession: SparkSession = {
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")

    SparkSession.builder()
      .master("local")
      .appName("testapp")
      .config("spark.ui.enabled", value = false)
      .getOrCreate()
  }

  def getTestResourceContent(relativePath: String): String = {
    Option(getClass.getResource(relativePath)) match {
      case Some(relPath) => Source.fromURL (relPath).mkString
      case None => throw new IllegalArgumentException(s"Path can not be reached: $relativePath")
    }
  }

}
