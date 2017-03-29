package org.zalando.spark.jsonschema

import org.apache.spark.sql.types._
import play.api.libs.json._

import scala.annotation.tailrec
import scala.io.Source

/**
 * Schema Converter for getting schema in json format into a spark Structure
 *
 * The given schema for spark has almost no validity checks, so it will make sense
 * to combine this with the schema-validator. For loading data with schema, data is converted
 * to the type given in the schema. If this is not possible the whole row will be null (!).
 * A field can be null if its type is a 2-element array, one of which is "null". The converted
 * schema doesn't check for 'enum' fields, i.e. fields which are limited to a given set.
 * It also doesn't check for required fields or if additional properties are set to true
 * or false. If a field is specified in the schema, than you can select it and it will
 * be null if missing. If a field is not in the schema, it cannot be selected even if
 * given in the dataset.
 *
 */
case class SchemaType(typeName: String, nullable: Boolean)

object SchemaConverter {

  val SchemaFieldName = "name"
  val SchemaFieldType = "type"
  val SchemaFieldId = "id"
  val SchemaStructContents = "properties"
  val SchemaArrayContents = "items"
  val SchemaRoot = "/"
  val TypeMap = Map(
    "string" -> StringType,
    "number" -> DoubleType,
    "float" -> FloatType,
    "integer" -> LongType,
    "boolean" -> BooleanType,
    "object" -> StructType,
    "array" -> ArrayType
  )

  def convertContent(schemaContent: String): StructType = convert(parseSchemaJson(schemaContent))

  def convert(inputPath: String): StructType = convert(loadSchemaJson(inputPath))

  def convert(inputSchema: JsObject): StructType = {
    val name = getJsonName(inputSchema).getOrElse("/")
    val typeName = getJsonType(inputSchema, name).typeName
    if (name == SchemaRoot && typeName == "object") {
      val properties = (inputSchema \ SchemaStructContents).as[JsObject]
      convertJsonStruct(new StructType, properties, properties.keys.toList)
    } else {
      throw new IllegalArgumentException(
        s"schema needs root level called <$SchemaRoot> and root type <object>. " +
          s"Current root is <$name> and type is <$typeName>"
      )
    }
  }

  def getJsonName(json: JsValue): Option[String] = (json \ SchemaFieldName).asOpt[String]

  def getJsonId(json: JsValue): Option[String] = (json \ SchemaFieldId).asOpt[String]

  def getJsonType(json: JsObject, name: String): SchemaType = {
    val id = getJsonId(json).getOrElse(name)

    (json \ SchemaFieldType).getOrElse(JsNull) match {
      case JsString(s) => SchemaType(s, nullable = false)
      case JsArray(array) if array.size == 2 =>
        array.find(_ != JsString("null"))
          .map(i => SchemaType(i.as[String], nullable = true))
          .getOrElse {
            throw new IllegalArgumentException(
              s"Incorrect definition of a nullable parameter at <$id>"
            )
          }
      case JsNull => throw new IllegalArgumentException(s"No <$SchemaType> in schema at <$id>")
      case t => throw new IllegalArgumentException(
        s"Unsupported type <${t.toString}> in schema at <$id>"
      )
    }
  }

  private def parseSchemaJson(schemaContent: String) = Json.parse(schemaContent).as[JsObject]

  def loadSchemaJson(filePath: String): JsObject = {
    val relPath = getClass.getResource(filePath)
    require(relPath != null, s"Path can not be reached: $filePath")
    parseSchemaJson(Source.fromURL(relPath).mkString)
  }

  @tailrec
  private def convertJsonStruct(schema: StructType, json: JsObject, jsonKeys: List[String]): StructType = {
    jsonKeys match {
      case Nil => schema
      case head :: tail =>
        val enrichedSchema = addJsonField(schema, (json \ head).as[JsObject], head)
        convertJsonStruct(enrichedSchema, json, tail)
    }
  }

  private def addJsonField(schema: StructType, json: JsObject, name: String): StructType = {
    val fieldType = getJsonType(json, name)
    val (dataType, nullable) = TypeMap(fieldType.typeName) match {

      case dataType: DataType =>
        (dataType, fieldType.nullable)

      case ArrayType =>
        val dataType = ArrayType(getDataType(json, JsPath \ SchemaArrayContents \ SchemaStructContents))
        (dataType, getJsonType(json, name).nullable)

      case StructType =>
        val dataType = getDataType(json, JsPath \ SchemaStructContents)
        (dataType, getJsonType(json, name).nullable)
    }

    schema.add(getJsonName(json).getOrElse(name), dataType, nullable = nullable)
  }

  private def getDataType(json: JsValue, contentPath: JsPath): DataType = {
    val content = contentPath.asSingleJson(json).as[JsObject]
    convertJsonStruct(new StructType, content, content.keys.toList)
  }
}
