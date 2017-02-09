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
    val inputStream = getClass.getResourceAsStream(relativePath)
    try Source.fromInputStream(inputStream).mkString
    catch {
      case _: NullPointerException =>
        throw new NullPointerException(s"No content found in $relativePath")
    } finally inputStream.close()
  }

}
