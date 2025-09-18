package link.rdcn.dacp.user

import enumeratum._

import scala.collection.immutable

/**
 * @Author renhao
 * @Description:
 * @Data 2025/7/8 18:14
 * @Modified By:
 */

sealed trait DataOperationType extends EnumEntry

object DataOperationType extends Enum[DataOperationType] {
  val values: immutable.IndexedSeq[DataOperationType] = findValues

  case object Map extends DataOperationType

  case object Filter extends DataOperationType

  case object Select extends DataOperationType

  case object Reduce extends DataOperationType

  case object Join extends DataOperationType

  case object GroupBy extends DataOperationType

  case object Sort extends DataOperationType

}