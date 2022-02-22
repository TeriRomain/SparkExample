package com.spark.project.transformer

import org.apache.spark.sql.DataFrame

case class Pipeline(transformers: Transformer*) {
  def transform(initialDf: DataFrame): DataFrame = {
    transformers.foldLeft(initialDf) {
      (df, transformer) => transformer.transform(df)
    }
  }
}
