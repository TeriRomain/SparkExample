package com.spark.project.model

import com.spark.project.exception.IngestionException
import cats.implicits._

case class DatasetFieldRaw(Name: String,
                           Type: String,
                           IsPrimaryKey: Option[Boolean] = None,
                           IsDateRef: Option[Boolean] = None) {

  def toDatasetField: Either[IngestionException, DatasetField] = {
    DataTypes.stringToDataTypes(Type).map { dataType: DataTypes =>
      DatasetField(
        Name = Name,
        Type = dataType,
        IsPrimaryKey = IsPrimaryKey.getOrElse(false),
        IsDateRef = IsDateRef.getOrElse(false)
      )
    }
  }
}
