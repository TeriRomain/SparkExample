package com.spark.project.utils

import cats.implicits._
import com.spark.project.exception.IngestionException
import com.spark.project.exception.IngestionException.{CanNotParseSchemaException, DateRefSchemaException, InvalidDataTypeException, PrimaryKeySchemaException}
import com.spark.project.logger.Logging
import com.spark.project.model.{DatasetField, DatasetFieldRaw}
import io.circe.generic.auto._
import io.circe.parser._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import com.spark.project.utils.ExtensionMethodUtils._

import scala.util.Try

case class SchemaUtils(tableName: String) extends Logging {

  def getSchemaAsString(schemaPath: String)(implicit spark: SparkSession): String = {
    spark.sparkContext.textFile(schemaPath).collect.mkString("\n")
  }

  def schemaToFieldList(schema: Vector[DatasetFieldRaw]): Either[IngestionException, Vector[DatasetField]] = {
    schema.map(_.toDatasetField).traverse
  }

  def fieldsToStructType(schema: Vector[DatasetField]): StructType = {
    val fields = schema.map(field => StructField(field.Name, field.Type.parquetType))
    StructType(fields)
  }

  def getSchema(StringSchema: String): (Vector[DatasetField], StructType) = {
    val fields = for {
      datasetFieldRaws <- decodeSchema(StringSchema)
      fields <- schemaToFieldList(datasetFieldRaws)
    } yield fields

    fields match {
      case Right(fields) => (fields, fieldsToStructType(fields))
      case Left(exception) =>
        exception match {
          case _: InvalidDataTypeException => logger.error(s"Error on table $tableName. Invalid data type")
          case _: CanNotParseSchemaException => logger.error(s"Error on table $tableName. Error when parsing schema")
          case error: IngestionException => logger.error(s"Error on table $tableName. ${error.getMessage}")
        }
        throw exception
    }
  }

  def decodeSchema(schema: String): Either[IngestionException, Vector[DatasetFieldRaw]] = {
    decode[Vector[DatasetFieldRaw]](schema)
      .leftMap(f => CanNotParseSchemaException(f.getMessage))
  }

  def getDateRefColumn(schema: Vector[DatasetField]): Either[IngestionException, Seq[String]] = {
    val dateRefColumns = schema.filter(_.IsDateRef == true)
      .map(_.Name)

    if (dateRefColumns.length > 1) Left(DateRefSchemaException("You must define one max DateRef column"))
    else Right(dateRefColumns)
  }

  def getPrimaryKeyColumns(schema: Vector[DatasetField]): Either[IngestionException, Seq[String]] = {
    val primaryKeyColumns = schema.filter(_.IsPrimaryKey == true)
      .map(_.Name)

    if (primaryKeyColumns.isEmpty) Left(PrimaryKeySchemaException("Schema does not contain PrimaryKey Column"))
    else Right(primaryKeyColumns)
  }

  def getSpecialsColumns(schemaString: String): (Seq[String], Seq[String]) = {
    val specialsColumns = for {
      schemaItems <- decodeSchema(schemaString)
      schema <- schemaToFieldList(schemaItems)
      primaryKeys <- getPrimaryKeyColumns(schema)
      datesRef <- getDateRefColumn(schema)
    } yield (primaryKeys, datesRef)

    specialsColumns match {
      case Right((primaryKeys, datesRef)) => (primaryKeys, datesRef)
      case Left(exception) =>
        logger.error(exception.getMessage)
        throw exception
    }
  }
}
