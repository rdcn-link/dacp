package link.rdcn.dacp.received

import link.rdcn.struct.{DataFrame, StructType}

trait DataReceiver extends AutoCloseable {
  /** Called once before receiving any rows */
  def start(): Unit

  /** Called for each received batch of rows */
  def receiveRow(dataFrame: DataFrame): Unit

  /** Called after all batches are received successfully */
  def finish(): Unit

  /** Called on error to clean up */
  override def close(): Unit = {}
}
