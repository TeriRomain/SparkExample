package com.spark.project.job

import com.google.gson.JsonArray
import com.spark.project.logger.Logging
import com.spark.project.model.SQLRequest
import com.spark.project.utils.{BusinessModelUtils, FileSystemUtils}
import io.circe.{Json, JsonObject}
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

case class BusinessComputeJob(dataModel: String) extends Logging{

  val businessModelConfigPath = s"business_model"

  val preparedDir = s"warehouse/prepared"
  val errorDir = s"warehouse/error"

  val businessDataDir = s"warehouse/business/$dataModel"

  val businessModelUtils: BusinessModelUtils = BusinessModelUtils(dataModel)

  def process()(implicit spark: SparkSession): Unit = {
    val businessModelSources = businessModelUtils.getBusinessModelSources(s"$businessModelConfigPath/sources/$dataModel.json")
    loadDataSourcesInSqlContext(businessModelSources)

    val businessModelRequests = businessModelUtils.getBusinessModelRequests(s"$businessModelConfigPath/processing/$dataModel.json")

    processBusinessModelRequests(businessModelRequests)
  }

  def loadDataSourcesInSqlContext(sources: Array[String])(implicit spark: SparkSession): Unit = {
    sources.foreach(source => spark.read.parquet(s"$preparedDir/$source").createOrReplaceTempView(source))
  }

  def processBusinessModelRequests(businessModelRequests: Array[SQLRequest])(implicit spark: SparkSession): Unit ={
    businessModelRequests.foreach(request => {
      executeRequest(request)
    })
  }

  def executeRequest(request: SQLRequest)(implicit spark: SparkSession): Unit ={
    val tableName = request.tableName
    logger.info(s"Processing data model, execute request : $tableName")

    val dfResult = spark.sql(request.request)
    dfResult.createOrReplaceTempView(tableName)
    dfResult.show()

    if (request.tableType.equals("FINAL")){
      dfResult.coalesce(1).write.option("header","true").mode(SaveMode.Overwrite).csv(s"$businessDataDir/$tableName")
    }
  }
}
