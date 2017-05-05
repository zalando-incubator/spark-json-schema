package org.zalando.spark.jsonschema

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.types._
import org.scalatest.{ FunSuite, Matchers }
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
    val testSchema = SchemaConverter.convert("/testJsonSchema.json")
    assert(testSchema === expectedStruct)
  }

  test("should convert schema.json content into spark StructType") {
    val testSchema = SchemaConverter.convertContent(getTestResourceContent("/testJsonSchema.json"))
    assert(testSchema === expectedStruct)
  }

  // 'id' and 'name' are optional according to http://json-schema.org/latest/json-schema-core.html
  test("should support optional 'id' and 'name' properties") {
    val testSchema = SchemaConverter.convert("/testJsonSchema3.json")
    assert(testSchema === expectedStruct)
  }

  test("data fields with only nulls shouldn't be removed") {
    val schema = SchemaConverter.convert("/testJsonSchema2.json")

    val jsonString = sparkSession.sparkContext.parallelize(Seq(
      """{"name": "aaa", "address": {}, "foo": "bar"}""",
      """{"name": "bbb", "address": {}}"""
    ))

    // without SchemaConverter
    val dbNoSchema = sparkSession.read.json(jsonString)
    assert(dbNoSchema.schema != schema)
    assert(dbNoSchema.schema === StructType(Array(
      StructField("foo", StringType, nullable = true),
      StructField("name", StringType, nullable = true)
    )))
    assert(dbNoSchema.select("name").collect()(0)(0) === "aaa")
    intercept[AnalysisException] { dbNoSchema.select("address") }
    assert(dbNoSchema.select("foo").collect()(0)(0) === "bar")

    // with SchemaConverter
    val dbWithSchema = sparkSession.read.schema(schema).json(jsonString)
    assert(dbWithSchema.schema === schema)
    assert(dbWithSchema.select("name").collect()(0)(0) === "aaa")
    assert(dbWithSchema.select("address.zip").collect()(0)(0) === null)
    intercept[AnalysisException] { dbWithSchema.select("foo") }
  }

  test("schema should support references") {
    val schema = SchemaConverter.convert("/testJsonSchema4.json")

    val expected = StructType(Array(
      StructField("name", StringType, nullable = false),
      StructField("addressA", StructType(Array(
        StructField("zip", StringType, nullable = true)
      )), nullable = false),
      StructField("addressB", StructType(Array(
        StructField("zip", StringType, nullable = true)
      )), nullable = false)
    ))

    assert(schema === expected)
  }

  test("Empty object should be possible") {
    val schema = SchemaConverter.convertContent(
      """
        {
          "$schema": "smallTestSchema",
          "type": "object",
          "properties": {
            "address": {
              "type": "object"
            }
          }
        }
      """
    )
    val expected = StructType(Array(
      StructField("address", StructType(Seq.empty), nullable = false)
    ))

    assert(schema === expected)
  }

  test("Known primitive type array should be an array of this type") {
    val typeMap = Map(
      "string" -> StringType,
      "number" -> DoubleType,
      "float" -> FloatType,
      "integer" -> LongType,
      "boolean" -> BooleanType
    )
    typeMap.foreach {
      case p @ (key, datatype) =>
        val schema = SchemaConverter.convertContent(
          s"""
          {
            "$$schema": "smallTestSchema",
            "type": "object",
            "properties": {
              "array" : {
                "type" : "array",
                "items": {
                  "type": "$key"
                }
              }
            }
          }
        """
        )
        val expected = StructType(Array(
          StructField("array", ArrayType(datatype), nullable = false)
        ))

        assert(schema === expected)
    }
  }

  test("Array of array should be an array of array") {

    val schema = SchemaConverter.convertContent(
      """
          {
            "$$schema": "smallTestSchema",
            "type": "object",
            "properties": {
              "array" : {
                "type" : "array",
                "items": {
                  "type": "array",
                  "items": {
                    "type": "string"
                  }
                }
              }
            }
          }
        """
    )
    val expected = StructType(Array(
      StructField("array", ArrayType(ArrayType(StringType)), nullable = false)
    ))

    assert(schema === expected)
  }

  test("Array of object should be an array of object") {

    val schema = SchemaConverter.convertContent(
      """
          {
            "$$schema": "smallTestSchema",
            "type": "object",
            "properties": {
              "array" : {
                "type" : "array",
                "items": {
                  "type": "object"
                }
              }
            }
          }
        """
    )
    val expected = StructType(Array(
      StructField("array", ArrayType(StructType(Seq.empty)), nullable = false)
    ))

    assert(schema === expected)
  }

  test("Array of object with properties should be an array of object with these properties") {

    val schema = SchemaConverter.convertContent(
      """
          {
            "$$schema": "smallTestSchema",
            "type": "object",
            "properties": {
              "array" : {
                "type" : "array",
                "items": {
                  "type": "object",
                  "properties": {
                    "name" : {
                      "type" : "string"
                    }
                  }
                }
              }
            }
          }
        """
    )
    val expected = StructType(Array(
      StructField("array", ArrayType(StructType(Seq(StructField("name", StringType, nullable = false )))), nullable = false)
    ))

    assert(schema === expected)
  }

  test("Array of unknown type should be an array of object") {

    val schema = SchemaConverter.convertContent(
      """
          {
            "$$schema": "smallTestSchema",
            "type": "object",
            "properties": {
              "array" : {
                "type" : "array",
                "items" : {}
              }
            }
          }
        """
    )
    val expected = StructType(Array(
      StructField("array", ArrayType(StructType(Seq.empty)), nullable = false)
    ))

    assert(schema === expected)
  }

  test("Array of various type should be an array of object") {

    val schema = SchemaConverter.convertContent(
      """
          {
            "$$schema": "smallTestSchema",
            "type": "object",
            "properties": {
              "array" : {
                "type" : "array",
                "items" : {
                  "type" : ["string", "integer"]
                }
              }
            }
          }
        """
    )
    val expected = StructType(Array(
      StructField("array", ArrayType(StructType(Seq.empty)), nullable = false)
    ))

    assert(schema === expected)
  }

}
