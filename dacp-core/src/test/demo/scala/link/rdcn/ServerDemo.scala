/**
 * @Author Yomi
 * @Description:
 * @Data 2025/7/22 10:49
 * @Modified By:
 */
package link.rdcn

import link.rdcn.TestBase.getResourcePath
import link.rdcn.dacp.received.DataReceiver
import link.rdcn.dacp.server.DacpServer
import link.rdcn.struct.DataFrame

import java.io.File
import java.nio.file.Paths


object ServerDemo {
  def main(args: Array[String]): Unit = {
    val provider = new TestDemoProvider
    val fairdHome = Paths.get(getResourcePath("tls")).toString
    val certPath = "server.crt"
    val keyPath = "server.pem"
    val tlsCertFile: File = Paths.get(fairdHome, certPath).toFile
    val tlsKeyFile: File = Paths.get(fairdHome, keyPath).toFile
    /**
     * 根据fairdHome自动读取配置文件
     * 非加密连接
     * val server = new FairdServer(provider.dataProvider, provider.authProvider, Paths.get(getResourcePath("")).toString())
     * tls加密连接
     */
    val server = new DacpServer(provider.dataProvider,new DataReceiver {
      /** Called once before receiving any rows */
      override def start(): Unit = {}

      /** Called for each received batch of rows */
      override def receiveRow(dataFrame: DataFrame): Unit = {}

      /** Called after all batches are received successfully */
      override def finish(): Unit = {}
    }, provider.authProvider)
    server.enableTLS(tlsCertFile, tlsKeyFile)
    server.start(fairdHome)
  }
}
