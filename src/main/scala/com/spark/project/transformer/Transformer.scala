package com.spark.project.transformer

import org.apache.spark.sql.DataFrame

trait Transformer {
  def transform(dataFrame: DataFrame): DataFrame

  def applyIf(condition: Boolean): Transformer = {
    if(condition) this
    else NeutralTransformer()
  }
}
