package link.rdcn.dacp.received

import link.rdcn.struct.{DataFrame, StructType}

trait DataReceiver{
  def receive(dataFrame: DataFrame): Unit
}
