package com.spark.project.model

import com.spark.project.exception.IngestionException
import com.spark.project.exception.IngestionException.InvalidDataTypeException
import org.apache.spark.sql.types
import org.apache.spark.sql.types.{DataType, DateType}

sealed trait DataTypes {
  def parquetType: types.DataType
  def isDate: Boolean = false
}

object DataTypes {

  case object StringType extends DataTypes {
    override def parquetType: types.DataType = types.StringType
  }

  case object IntegerType extends DataTypes {
    override def parquetType: types.DataType = types.IntegerType
  }

  case object LongType extends DataTypes {
    override def parquetType: types.DataType = types.LongType
  }

  case object DoubleType extends DataTypes {
    override def parquetType: types.DataType = types.DoubleType
  }

  case object FloatType extends DataTypes {
    override def parquetType: types.DataType = types.FloatType
  }

  case object TimestampType extends DataTypes {
    override def parquetType: DataType = types.TimestampType
  }


  def stringToDataTypes(_type: String): Either[IngestionException, DataTypes] = _type match {
    case "string" => Right(StringType)
    case "int" => Right(IntegerType)
    case "bigint" => Right(LongType)
    case "long" => Right(LongType)
    case "double" => Right(DoubleType)
    case "float" => Right(FloatType)
    case "timestamp" => Right(TimestampType)

    case invalidType => Left(InvalidDataTypeException(s"Type $invalidType is invalid"))
  }

  def sparkDataTypesToDatalakeDataTypes(_type: types.DataType): DataTypes = _type match {
    case types.StringType => StringType
    case types.IntegerType => IntegerType
    case types.LongType => LongType
    case types.DoubleType => DoubleType
    case types.TimestampType => TimestampType
    case types.FloatType => FloatType
    case _: types.DecimalType => FloatType
    case _ => StringType
  }

  def parquetTypesToString(_type: types.DataType): String = _type match {
    case types.StringType => "string"
    case types.IntegerType => "int"
    case types.LongType => "bigint"
    case types.DoubleType => "double"
    case types.FloatType => "float"
    case types.TimestampType => "timestamp"
    case _: types.DecimalType => "decimal"
    case _ => "string"
  }

}
