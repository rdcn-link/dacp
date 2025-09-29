package link.rdcn.dacp.optree.fifo

import link.rdcn.struct.{ClosableIterator, DataFrame, DefaultDataFrame, Row, StructType}

/**
 * @Author renhao
 * @Description:
 * @Data 2025/9/26 18:18
 * @Modified By:
 */
class FifoFileDataFrame(namedPipe: NamedPipe) extends DataFrame {
  override val schema: StructType = ???

  override def mapIterator[T](f: ClosableIterator[Row] => T): T = ???

  override def map(f: Row => Row): DataFrame = ???

  override def filter(f: Row => Boolean): DataFrame = ???

  override def select(columns: String*): DataFrame = ???

  override def limit(n: Int): DataFrame = ???

  override def reduce(f: ((Row, Row)) => Row): DataFrame = ???

  override def foreach(f: Row => Unit): Unit = ???

  override def collect(): List[Row] = ???
}
