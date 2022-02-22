package com.spark.project.utils

import com.spark.project.exception.IngestionException
import com.spark.project.exception.IngestionException.{CanNotParseSchemaException, DateRefSchemaException, InvalidDataTypeException, PrimaryKeySchemaException}
import com.spark.project.logger.Logging
import com.spark.project.model.{DatasetField, DatasetFieldRaw, SQLRequest}
import io.circe.{Json, JsonNumber, ParsingFailure}
import io.circe.parser.{decode, parse}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StructField, StructType}

case class BusinessModelUtils(modelName: String) extends Logging {

  def getBusinessModelSources(filePath: String)(implicit spark: SparkSession): Array[String] ={
    import spark.implicits._

    spark.read.option("multiline", "true").json(filePath).map(row => {
      row.getAs[String]("source_name")
    }).collect()
  }

  def getBusinessModelRequests(filePath: String)(implicit spark: SparkSession): Array[SQLRequest] ={
    import spark.implicits._

    spark.read.option("multiline", "true").json(filePath).map(row => {
      SQLRequest(row.getAs[String]("table_name"), row.getAs[String]("table_type"), row.getAs[String]("request"))
    }).collect()
  }
}
