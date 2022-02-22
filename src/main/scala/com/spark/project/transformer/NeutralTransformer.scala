package com.spark.project.transformer

import org.apache.spark.sql.DataFrame

case class NeutralTransformer() extends Transformer {
  override def transform(dataFrame: DataFrame): DataFrame = dataFrame
}
