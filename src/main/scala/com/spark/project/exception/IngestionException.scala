package com.spark.project.exception

sealed abstract class IngestionException(message: String) extends Exception(message)

object IngestionException {
  case class InvalidDataTypeException(message: String) extends IngestionException(message)

  case class SchemaDoesNotExistsException(message: String) extends IngestionException(message)

  case class SparkSchemaException(message: String) extends IngestionException(message)

  case class DateRefSchemaException(message: String) extends IngestionException(message)

  case class PrimaryKeySchemaException(message: String) extends IngestionException(message)

  case class CanNotParseSchemaException(message: String) extends IngestionException(message)

  case class WronglyFormattedPartition(message: String) extends IngestionException(message)

  case class WrongSchemaStrategyException(message: String) extends IngestionException(message)

  case class CanNotFormatColumnsException(message: String) extends IngestionException(message)

  case class WrapperException(message: String) extends IngestionException(message)

}
