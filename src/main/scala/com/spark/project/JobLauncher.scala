package com.spark.project

import org.apache.spark.sql.SparkSession
import com.spark.project.job.{BusinessComputeJob, DataPreparationJob}
import com.spark.project.logger.Logging

import scala.util.{Failure, Try}

object JobLauncher extends Logging{
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder.
      appName("sparkJob").
      master("local[*]").getOrCreate()

    val tablesToPrepare = List("customer", "items", "orders", "products")
    val jobList = tablesToPrepare.map(tableToPrepare => DataPreparationJob(tableToPrepare))
    val preparationExecutions = jobList.map {
      job => Try(job.process()(spark))
    }

    val dataModelToProcess = List("repeaters")
    val businessJobList = dataModelToProcess.map(businessModel => BusinessComputeJob(businessModel))
    val businessExecutions = businessJobList.map {
      job => Try(job.process()(spark))
    }

    spark.stop()
  }
}
