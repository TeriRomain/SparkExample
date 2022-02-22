package com.spark.project.transformer
import com.spark.project.utils.FileSystemUtils
import org.apache.spark.sql.functions.{coalesce, col, lit}
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{Column, DataFrame}

case class AddTechnicalUpdateColumnTransformer() extends Transformer {
  val COLUMN_NAME = "tech_update_date"

  override def transform(dataFrame: DataFrame): DataFrame = {
    dataFrame.withColumn(COLUMN_NAME, createUpdatedColumn(dataFrame))
      .drop(FileSystemUtils.FILE_INFORMATION_FILENAME_COLUMN)
      .drop(FileSystemUtils.FILE_INFORMATION_UPDATE_TIME_COLUMN)
  }

  def createUpdatedColumn(dataFrame: DataFrame): Column ={
    val column = if (dataFrame.columns.contains(COLUMN_NAME)) col(COLUMN_NAME) else lit(null)
    coalesce(column, (col(FileSystemUtils.FILE_INFORMATION_UPDATE_TIME_COLUMN) / 1000).cast(TimestampType))
  }
}
