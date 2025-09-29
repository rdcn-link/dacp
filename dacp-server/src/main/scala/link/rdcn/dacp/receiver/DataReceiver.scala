package link.rdcn.dacp.receiver

import link.rdcn.struct.{DataFrame, StructType}

trait DataReceiver{
  def receive(dataFrame: DataFrame): Unit
}
