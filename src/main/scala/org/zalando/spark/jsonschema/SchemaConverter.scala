package org.zalando.spark.jsonschema

import org.apache.spark.sql.types._
import play.api.libs.json._

import scala.annotation.tailrec
import scala.collection.Set

/**
  * Schema Converter for getting schema in json format into a spark Structure
  *
  * The given schema for spark has almost no validity checks, so it will make sense
  * to combine this with the schema-validator. For loading data with schema, data is converted
  * to the type given in the schema. If this is not possible the whole row will be null (!).
  * Fields can be null, no matter if the schema has nullable true or false. The converted
  * schema doesn't check for 'enum' fields, i.e. fields which are limited to a given set.
  * It also doesn't check for required fields or if additional properties are set to true
  * or false. If a field is specified in the schema, than you can select it and it will
  * be null if missing. If a field is not in the schema, it cannot be selected even if
  * given in the dataset.
  *
  */
object SchemaConverter {

  val SchemaFieldName = "name"
  val SchemaFieldType = "type"
  val SchemaFieldId = "id"
  val SchemaStructContents = "properties"
  val SchemaArrayContents = "items"
  val SchemaRoot = "/"
  val typeMap = Map(
    "string" -> StringType,
    "number" -> DoubleType,
    "integer" -> LongType,
    "boolean" -> BooleanType,
    "object" -> StructType,
    "array" -> ArrayType
  )

  def convert(inputPath: String): StructType = convert(loadSchemaJson(inputPath))
  def convert(inputSchema: JsValue): StructType = {
    val name = getJsonName(inputSchema)
    val typeName = getJsonType(inputSchema).typeName
    if (name == SchemaRoot && typeName == "object") {
      val properties = (inputSchema \ SchemaStructContents).as[JsObject]
      convertJsonStruct(new StructType, properties, properties.keys)
    } else {
      throw new IllegalArgumentException(
        s"schema needs root level called <$SchemaRoot> and root type <object>. " +
          s"Current root is <$name> and type is <$typeName>"
      )
    }
  }

  case class SchemaType(typeName: String, nullable: Boolean)
  def getJsonName(json: JsValue): String = (json \ SchemaFieldName).as[String]
  def getJsonId(json: JsValue): String = (json \ SchemaFieldId).as[String]
  def getJsonType(json: JsValue): SchemaType = {
    val id = getJsonId(json)
    (json \ SchemaFieldType).getOrElse(JsNull) match {
      case JsString(s) => SchemaType(s, nullable = false)
      case JsArray(a) if a.size == 2 && a.count(_ != JsString("null")) == 1 =>
        a.filter(_ != JsString("null")).map(i => SchemaType(i.as[String], nullable = true)).head
      case JsNull => throw new IllegalArgumentException(s"No <$SchemaType> in schema at <$id>")
      case t => throw new IllegalArgumentException(
        s"Unsupported type <${t.toString}> in schema at <$id>"
      )
    }
  }
  
  def loadSchemaJson(filePath: String): JsValue = {
    val inputStream = getClass.getResourceAsStream("/schema/json/trainingDataJsonSchema.json")
    val jsonString = scala.io.Source.fromInputStream(inputStream).mkString
    try Json.parse(jsonString)
    finally inputStream.close()
  }

  @tailrec
  def convertJsonStruct(schema: StructType, json: JsValue, jsonKeys: Set[String]): StructType = {
    if (jsonKeys.nonEmpty) {
      val enrichedSchema = addJsonField(schema, (json \ jsonKeys.head).as[JsValue])
      convertJsonStruct(enrichedSchema, json, jsonKeys.drop(1))
    } else { schema }
  }

  def addJsonField(schema: StructType, json: JsValue): StructType = {
    val fieldType = getJsonType(json)
    typeMap(fieldType.typeName) match {
      case d: DataType => schema.add(getJsonName(json), d, nullable = fieldType.nullable)
      case ArrayType =>
        addJsonArrayStructure(schema, json, JsPath \ SchemaArrayContents \ SchemaStructContents)
      case StructType => addJsonObjectStructure(schema, json, JsPath \ SchemaStructContents)
    }
  }

  private def addJsonStructure(schema: StructType, json: JsValue, contentPath: JsPath)(addStructure: StructType => DataType): StructType = {
    val content = contentPath.asSingleJson(json).as[JsObject]
    schema.add(
      getJsonName(json),
      addStructure(convertJsonStruct(new StructType, content, content.keys)),
      nullable = getJsonType(json).nullable
    )
  }

  def addJsonObjectStructure(schema: StructType, json: JsValue, contentPath: JsPath): StructType = {
    addJsonStructure(schema, json, contentPath)(structType => structType)
  }

  def addJsonArrayStructure(schema: StructType, json: JsValue, contentPath: JsPath): StructType = {
    addJsonStructure(schema, json, contentPath)(structType => ArrayType(structType))
  }
}
