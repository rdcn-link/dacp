package link.rdcn.dacp.client

import link.rdcn.Logging
import link.rdcn.dacp.struct.DataFrameDocument
import link.rdcn.operation._
import link.rdcn.struct.{ClosableIterator, DataFrame, Row, StructType}

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/10 17:24
 * @Modified By:
 */

case class RemoteGetDataFrameProxy(
                                    operation: TransformOp,
                                    getRows: String => (StructType, ClosableIterator[Row]),
                                    sourceSchema: StructType,
                                    sourceDocument: DataFrameDocument
                                  )
  extends DataFrame with Logging
{

  override lazy val schema: StructType = {
    operation match {
      case SourceOp(dataFrameUrl) => sourceSchema
      case _ => schemaAndRows._1
    }
  }

  val document: DataFrameDocument = {
    operation match {
      case SourceOp(dataFrameUrl) => sourceDocument
      case _ => DataFrameDocument.empty()
    }
  }

  override def filter(f: Row => Boolean): DataFrame = {
    val genericFunctionCall = SingleRowCall(new SerializableFunction[Row, Boolean] {
      override def apply(v1: Row): Boolean = f(v1)
    })
    val filterOp = FilterOp(FunctionWrapper.getJavaSerialized(genericFunctionCall), operation)
    copy(operation = filterOp)
  }

  override def select(columns: String*): DataFrame = {
    copy(operation = SelectOp(operation, columns: _*))
  }

  override def limit(n: Int): DataFrame = copy(operation = LimitOp(n, operation))

  override def map(f: Row => Row): DataFrame = {
    val genericFunctionCall = SingleRowCall(new SerializableFunction[Row, Row] {
      override def apply(v1: Row): Row = f(v1)
    })
    val mapOperationNode = MapOp(FunctionWrapper.getJavaSerialized(genericFunctionCall), operation)
    copy(operation = mapOperationNode)
  }

  override def foreach(f: Row => Unit): Unit = records.foreach(f)

  override def collect(): List[Row] = records.toList

  private def records(): Iterator[Row] = schemaAndRows._2

  private lazy val schemaAndRows = getRows(operation.toJsonString)

  override def mapIterator[T](f: ClosableIterator[Row] => T): T = f(getRows(operation.toJsonString)._2)
}




