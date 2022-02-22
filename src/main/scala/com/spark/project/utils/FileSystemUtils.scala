package com.spark.project.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path}
import org.apache.spark.sql
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

import java.net.URI

object FileSystemUtils {

  val FILE_INFORMATION_FILENAME_COLUMN = "file_information_filename"
  val FILE_INFORMATION_UPDATE_TIME_COLUMN = "file_information_update_time"

  def fileInformationSchema(): StructType = {
    StructType(Array(
      StructField(FILE_INFORMATION_FILENAME_COLUMN, StringType, nullable = true),
      StructField(FILE_INFORMATION_UPDATE_TIME_COLUMN, LongType, nullable = true)
    ))
  }

  def listFilesInPath[T](path: String)(f: LocatedFileStatus => T)(implicit spark: SparkSession): Seq[T] = {
    val fileSystem = FileSystem.get(URI.create(path), new Configuration(spark.sparkContext.hadoopConfiguration))
    val filesList = fileSystem.listFiles(new Path(path), true)
    var rowData = Seq.empty[T]
    while (filesList.hasNext) {
      val fileStatus = filesList.next()
      if (fileStatus.isFile)
        rowData = rowData :+ f(fileStatus)
    }
    rowData
  }

  def listFilesInformationAsDataFrame(path: String)(implicit spark: SparkSession): sql.DataFrame = {
    val rowData = listFilesInPath(path) { fileStatus => Row(fileStatus.getPath.toUri.toString, fileStatus.getModificationTime) }
    spark.createDataFrame(spark.sparkContext.parallelize(rowData), fileInformationSchema())
  }

  def listFilenamesAndModificationTimeAsList(path: String)(implicit spark: SparkSession): List[(String, Long)] = {
    listFilesInPath(path) { fileStatus =>
      (fileStatus.getPath.toString, fileStatus.getModificationTime)
    }.toList
  }
}
