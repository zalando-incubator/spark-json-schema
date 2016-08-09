package de.zalando.payana.schema

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.types._
import org.scalatest.FunSuite
import play.api.libs.json.Json
import SparkTestEnv._

class SchemaConverterTest extends FunSuite {
  
  def testSchema: StructType = {
    val schemaPath = "src/test/resources/testJsonSchema.json"
    SchemaConverter.convert(schemaPath)
  }
  
  test("should convert schema.json into spark StructType") {
    val expectedStruct = StructType(Array(
      StructField("object", StructType(Array(
        StructField("item1", StringType, nullable = false),
        StructField("item2", StringType, nullable = false)
      )), nullable = false),
      StructField("array", ArrayType(StructType(Array(
        StructField("itemProperty1", StringType, nullable = false),
        StructField("itemProperty2", DoubleType, nullable = false)
      ))), nullable = false),
      StructField("structure", StructType(Array(
        StructField("nestedArray", ArrayType(StructType(Array(
          StructField("key", StringType, nullable = false),
          StructField("value", LongType, nullable = false)
        ))), nullable = false)
      )), nullable = false),
      StructField("integer", LongType, nullable = false),
      StructField("string", StringType, nullable = false),
      StructField("number", DoubleType, nullable = false),
      StructField("nullable", DoubleType, nullable = true),
      StructField("boolean", BooleanType, nullable = false),
      StructField("additionalProperty", StringType, nullable = false)
    ))

    assert(testSchema === expectedStruct)
  }


test("data fields with only nulls shouldn't be removed") {
  val schema = SchemaConverter.convert(Json.parse("""{
    "$schema": "smallTestSchema",
    "id": "smallTestSchema",
    "type": "object",
    "name": "/",
    "properties": {
      "name": {
        "id": "smallTestSchema/name",
        "type": "string",
        "name": "name"
      },
      "address": {
        "id": "smallTestSchema/address/",
        "type": "object",
        "name": "address",
        "properties": {
          "zip": {
            "id": "smallTestSchema/address/zip",
            "type": "string",
            "name": "zip"
  }}}}}"""))
  val jsonString = sparkContext.parallelize(Seq(
    """{"name": "aaa", "address": {}, "foo": "bar"}""",
    """{"name": "bbb", "address": {}}"""
  ))

  // without SchemaConverter
  val db = sqlContext.read.json(jsonString)
  assert(db.schema != schema)
  assert(db.schema === StructType(Array(
    StructField("foo", StringType, nullable = true),
    StructField("name", StringType, nullable = true)
  )))
  assert(db.select("name").collect()(0)(0) === "aaa")
  intercept[AnalysisException] { db.select("address") }
  assert(db.select("foo").collect()(0)(0) === "bar")

  // with SchemaConverter
  val dbSchema = sqlContext.read.schema(schema).json(jsonString)
  assert(dbSchema.schema === schema)
  assert(dbSchema.select("name").collect()(0)(0) === "aaa")
  assert(dbSchema.select("address.zip").collect()(0)(0) === null)
  intercept[AnalysisException] { dbSchema.select("foo") }
}
}
