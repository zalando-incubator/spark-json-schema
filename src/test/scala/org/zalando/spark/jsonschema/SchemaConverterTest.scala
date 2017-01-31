package org.zalando.spark.jsonschema

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.types._
import org.scalatest.{FunSuite, Matchers}
import org.zalando.spark.jsonschema.SparkTestEnv._

class SchemaConverterTest extends FunSuite with Matchers {

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
    StructField("float", FloatType, nullable = false),
    StructField("nullable", DoubleType, nullable = true),
    StructField("boolean", BooleanType, nullable = false),
    StructField("additionalProperty", StringType, nullable = false)
  ))

  test("should convert schema.json into spark StructType") {
    val testSchema = SchemaConverter.convert(getClass.getResource("/testJsonSchema.json").getPath)
    assert(testSchema === expectedStruct)
  }

  test("should convert schema.json content into spark StructType") {
    val testSchema = SchemaConverter.convertContent(getTestResourceContent("/testJsonSchema.json"))
    assert(testSchema === expectedStruct)
  }

  test("data fields with only nulls shouldn't be removed") {
    val schema = SchemaConverter.convert(getClass.getResource("/testJsonSchema2.json").getPath)

    val jsonString = sparkContext.parallelize(Seq(
      """{"name": "aaa", "address": {}, "foo": "bar"}""",
      """{"name": "bbb", "address": {}}"""
    ))

    // without SchemaConverter
    val dbNoSchema = sqlContext.read.json(jsonString)
    assert(dbNoSchema.schema != schema)
    assert(dbNoSchema.schema === StructType(Array(
      StructField("foo", StringType, nullable = true),
      StructField("name", StringType, nullable = true)
    )))
    assert(dbNoSchema.select("name").collect()(0)(0) === "aaa")
    intercept[AnalysisException] {
      dbNoSchema.select("address")
    }
    assert(dbNoSchema.select("foo").collect()(0)(0) === "bar")

    // with SchemaConverter
    val dbWithSchema = sqlContext.read.schema(schema).json(jsonString)
    assert(dbWithSchema.schema === schema)
    assert(dbWithSchema.select("name").collect()(0)(0) === "aaa")
    assert(dbWithSchema.select("address.zip").collect()(0)(0) === null)
    intercept[AnalysisException] {
      dbWithSchema.select("foo")
    }
  }
}