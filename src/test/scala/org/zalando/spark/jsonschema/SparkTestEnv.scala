package org.zalando.spark.jsonschema

import org.apache.spark.sql.SparkSession

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

}
