package com.spark.project.job

import com.spark.project.logger.Logging
import com.spark.project.transformer.{AddTechnicalUpdateColumnTransformer, ParseDateRefColumnTransformer, Pipeline}
import com.spark.project.utils.{FileSystemUtils, SchemaUtils}
import org.apache.spark.sql.functions.{col, input_file_name}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types._

case class DataPreparationJob(tableName: String) extends Logging{

  val rawDir = s"warehouse/raw/$tableName"
  val preparedDir = s"warehouse/prepared/$tableName"
  val schemaPath = s"schemas/$tableName"
  val errorDir = s"warehouse/error/$tableName"

  val schemaUtils: SchemaUtils = SchemaUtils(tableName)

  val ERROR_COLUMN_NAME = "error_column"

  def process()(implicit spark: SparkSession): Unit = {

    val stringSchema = schemaUtils.getSchemaAsString(s"$schemaPath/$tableName.schema")

    val dataFrame = readDataFrame(rawDir, stringSchema)

    val transformedDataFrame = Pipeline(
      AddTechnicalUpdateColumnTransformer().applyIf(dataFrame.columns.contains(FileSystemUtils.FILE_INFORMATION_UPDATE_TIME_COLUMN)) ,
      ParseDateRefColumnTransformer(stringSchema, schemaUtils)
    ).transform(dataFrame)

    writeDataFrame(transformedDataFrame)
  }

  def readDataFrame(inputDirectory: String, stringSchema: String)(implicit spark: SparkSession): DataFrame = {

    val dfFilesInfo = FileSystemUtils.listFilesInformationAsDataFrame(inputDirectory)

    val schema = schemaUtils.getSchema(stringSchema)
    val schemaWithErrorCol = StructType(schema._2 :+ StructField(ERROR_COLUMN_NAME, StringType))

    val dataFrame = spark.read
      .option("header", "true")
      .option("columnNameOfCorruptdRecord", ERROR_COLUMN_NAME)
      .schema(schemaWithErrorCol)
      .csv(inputDirectory)

    dataFrame.withColumn(FileSystemUtils.FILE_INFORMATION_FILENAME_COLUMN, input_file_name())
      .join(dfFilesInfo, FileSystemUtils.FILE_INFORMATION_FILENAME_COLUMN)
  }

  def writeDataFrame(dataFrame: DataFrame)(implicit spark: SparkSession): Unit ={

    val dfError = dataFrame.filter(col(ERROR_COLUMN_NAME).isNotNull)
    if (!dfError.isEmpty) {
      dfError.write.csv(errorDir)
    }

    val dfWithoutErrors = dataFrame.filter(col(ERROR_COLUMN_NAME).isNull).drop(ERROR_COLUMN_NAME)

    dfWithoutErrors.show()

    val writer = dfWithoutErrors.write.mode(SaveMode.Overwrite)

    if(dfWithoutErrors.columns.contains(ParseDateRefColumnTransformer.getDateRefColName())) {
      writer.partitionBy(ParseDateRefColumnTransformer.getDateRefColName())
    }

   writer.parquet(preparedDir)
  }
}
