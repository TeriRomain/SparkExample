package com.spark.project.transformer

import com.spark.project.utils.SchemaUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, to_date, udf}
import org.apache.spark.sql.types.TimestampType

case class ParseDateRefColumnTransformer(schemaString: String, schemaUtils: SchemaUtils) extends Transformer {



  override def transform(dataFrame: DataFrame): DataFrame = {
    val (primaryKeyColumns, dateRefColumn) = schemaUtils.getSpecialsColumns(schemaString)

    ParseDateRefColumnTransformer.parseDateAndAddColumns(dataFrame, dateRefColumn)
  }


}

object ParseDateRefColumnTransformer {
  val DATE_REF_COLUMN_NAME = "date_ref"

  def parseDateAndAddColumns(dataFrame: DataFrame, dateRefColumn: Seq[String]): DataFrame = {
    dateRefColumn.foldLeft(dataFrame)((df, dateCol) =>
      df.withColumn(DATE_REF_COLUMN_NAME, to_date(col(dateCol), "yyyy-MM-dd"))
    )
  }

  def getDateRefColName(): String = {
    DATE_REF_COLUMN_NAME
  }
}
