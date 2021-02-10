package org.zalando.spark.jsonschema

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.types._
import org.scalatest.{ BeforeAndAfter, FunSuite, Matchers }
import org.zalando.spark.jsonschema.SparkTestEnv._

import scala.util.Try

class SchemaConverterTest extends FunSuite with Matchers with BeforeAndAfter {

  val expectedStruct = StructType(Array(
    StructField("object", StructType(Array(
      StructField("item1", StringType, nullable = false),
      StructField("item2", StringType, nullable = false)
    )), nullable = false),
    StructField("array", ArrayType(StructType(Array(
      StructField("itemProperty1", StringType, nullable = false),
      StructField("itemProperty2", DoubleType, nullable = false)
    )), containsNull = false), nullable = false),
    StructField("structure", StructType(Array(
      StructField("nestedArray", ArrayType(StructType(Array(
        StructField("key", StringType, nullable = false),
        StructField("value", LongType, nullable = false)
      )), containsNull = false), nullable = false)
    )), nullable = false),
    StructField("integer", LongType, nullable = false),
    StructField("string", StringType, nullable = false),
    StructField("number", DoubleType, nullable = false),
    StructField("float", FloatType, nullable = false),
    StructField("nullable", DoubleType, nullable = true),
    StructField("boolean", BooleanType, nullable = false),
    StructField("decimal", DecimalType(38, 18), nullable = false),
    StructField("decimal_default", DecimalType(10, 0), nullable = false),
    StructField("decimal_nullable", DecimalType(38, 18), nullable = true),
    StructField("timestamp", DataTypes.TimestampType, nullable = false),
    StructField("additionalProperty", StringType, nullable = false)
  ))

  before {
    SchemaConverter.enableStrictTyping()
  }

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
    assert(Try(dbWithSchema.select("address.zip").collect()(0)(0)).isSuccess)
    assert(Try(dbWithSchema.select("address.foo").collect()(0)(0)).isFailure)
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
          StructField("array", ArrayType(datatype, containsNull = false), nullable = false)
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
      StructField("array", ArrayType(ArrayType(StringType, containsNull = false), containsNull = false), nullable = false)
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
      StructField("array", ArrayType(StructType(Seq.empty), containsNull = false), nullable = false)
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
      StructField("array", ArrayType(StructType(Seq(StructField("name", StringType, nullable = false))), containsNull = false), nullable = false)
    ))

    assert(schema === expected)
  }

  test("Array of unknown type should fail") {

    assertThrows[IllegalArgumentException] {
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
    }
  }

  test("Array of various type should fail") {
    assertThrows[IllegalArgumentException] {
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
    }
  }

  test("Array of nullable type should be an array of nullable type") {
    val typeMap = Map(
      "string" -> StringType,
      "number" -> DoubleType,
      "float" -> FloatType,
      "integer" -> LongType,
      "boolean" -> BooleanType
    )
    typeMap.foreach {
      case (name, atype) =>
        val schema = SchemaConverter.convertContent(
          s"""
          {
            "$$schema": "smallTestSchema",
            "type": "object",
            "properties": {
              "array" : {
                "type" : "array",
                "items" : {
                  "type" : ["$name", "null"]
                }
              }
            }
          }
        """
        )

        val expected = StructType(Array(
          StructField("array", ArrayType(atype, containsNull = true), nullable = false)
        ))

        assert(schema === expected)
    }
  }

  test("Array of non-nullable type should be an array of non-nullable type") {
    val schema = SchemaConverter.convertContent(
      """
          {
            "$$schema": "smallTestSchema",
            "type": "object",
            "properties": {
              "array" : {
                "type" : "array",
                "items" : {
                  "type" : ["string"]
                }
              }
            }
          }
        """
    )

    val expected = StructType(Array(
      StructField("array", ArrayType(StringType, containsNull = false), nullable = false)
    ))

    assert(schema === expected)
  }

  test("Array of nullable object should be an array of nullable object") {
    val schema = SchemaConverter.convertContent(
      """
          {
            "$$schema": "smallTestSchema",
            "type": "object",
            "properties": {
              "array" : {
                "type" : "array",
                "items" : {
                  "type" : ["object", "null"],
                  "properties" : {
                    "prop" : {
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
      StructField("array", ArrayType(
        StructType(Seq(StructField("prop", StringType, nullable = false))), containsNull = true
      ), nullable = false)
    ))

    assert(schema === expected)
  }

  test("Nullable array should be an array or a null value") {
    val schema = SchemaConverter.convertContent(
      """
          {
            "$$schema": "smallTestSchema",
            "type": "object",
            "properties": {
              "array" : {
                "type" : ["array", "null"],
                "items" : {
                  "type" : "string"
                }
              }
            }
          }
        """
    )

    val expected = StructType(Array(
      StructField("array", ArrayType(StringType, containsNull = false), nullable = true)
    ))

    assert(schema === expected)
  }

  test("Multiple types should fail with strict typing") {
    assertThrows[IllegalArgumentException] {
      val schema = SchemaConverter.convertContent(
        """
          {
            "$$schema": "smallTestSchema",
            "type": "object",
            "properties": {
              "prop" : {
                "type" : ["integer", "float"]
              }
            }
          }
        """
      )

    }
  }

  test("Multiple types should default to string without strict typing") {
    val schema = SchemaConverter.disableStrictTyping().convertContent(
      """
          {
            "$$schema": "smallTestSchema",
            "type": "object",
            "properties": {
              "prop" : {
                "type" : ["integer", "float"]
              }
            }
          }
        """
    )

    val expected = StructType(Array(
      StructField("prop", StringType, nullable = false)
    ))

    assert(schema === expected)
  }

  test("null type only should fail") {
    assertThrows[AssertionError] {
      val schema = SchemaConverter.convertContent(
        """
          {
            "$$schema": "smallTestSchema",
            "type": "object",
            "properties": {
              "prop" : {
                "type" : "null"
              }
            }
          }
        """
      )
    }
  }

  test("null type only should fail event as a single array element") {
    assertThrows[IllegalArgumentException] {
      val schema = SchemaConverter.convertContent(
        """
          {
            "$$schema": "smallTestSchema",
            "type": "object",
            "properties": {
              "prop" : {
                "type" : ["null"]
              }
            }
          }
        """
      )
    }
  }

  test("decimal type with only one of precision or range should fail") {
    assertThrows[IllegalArgumentException] {
      val schema = SchemaConverter.convertContent(
        """
          {
            "type": "object",
            "properties": {
              "decimal": {
                "type": "decimal",
                "range": 18
              }
            }
          }
        """
      )
    }
    assertThrows[IllegalArgumentException] {
      val schema = SchemaConverter.convertContent(
        """
          {
            "type": "object",
            "properties": {
              "decimal": {
                "type": "decimal",
                "precision": 38
              }
            }
          }
        """
      )
    }
  }

}
