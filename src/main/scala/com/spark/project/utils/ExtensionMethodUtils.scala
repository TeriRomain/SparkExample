package com.spark.project.utils

import com.spark.project.exception.IngestionException

import scala.util.{Failure, Success, Try}

object ExtensionMethodUtils {

  implicit class TryOps[T](t: Try[T]) {
    def toEither(f: String => IngestionException): Either[IngestionException, T] = t match {
      case Success(s) => Right(s)
      case Failure(exception) => Left(f(exception.getMessage))
    }
  }

  implicit class EitherVectorOps[L, R](e: Vector[Either[L, R]]) {
    /**
     * Transform a Vector of Either in a Either of Vector
     * If at least one of the Either is a Left, the output Either is a Left.
     * If there is only Rights, the output Either is a Right
     * @return The Either of Vector
     */
    def traverse: Either[L, Vector[R]] = {
      e.find(_.isLeft) match {
        case Some(left) => Left(left.left.get)
        case None => Right(e.map(_.right.get))
      }
    }
  }

  implicit class StringOps(s: String) {
    def emptyOrNull(): Boolean = {
      s == null || s.isEmpty
    }
  }
}
