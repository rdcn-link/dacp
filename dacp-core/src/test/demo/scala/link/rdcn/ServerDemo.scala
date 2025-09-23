/**
 * @Author Yomi
 * @Description:
 * @Data 2025/7/22 10:49
 * @Modified By:
 */
package link.rdcn

import link.rdcn.TestBase._
import link.rdcn.dacp.{ConfigKeys, FairdConfig}
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
      override def receive(dataFrame: DataFrame): Unit = {}
    }, provider.authProvider)
    server.enableTLS(tlsCertFile, tlsKeyFile)
    val props = ConfigLoader.loadProperties(s"$fairdHome" + File.separator + "conf" + File.separator + "faird.conf")
    props.setProperty(ConfigKeys.FAIRD_HOME, fairdHome)
    val fairdConfig = FairdConfig.load(props)
    server.start(fairdConfig)
  }
}
