package link.rdcn.server

import link.rdcn.ConfigLoader
import link.rdcn.TestBase.{adminPassword, adminUsername, getResourcePath}
import link.rdcn.TestEmptyProvider._
import link.rdcn.dacp.client.DacpClient
import link.rdcn.dacp.receiver.DataReceiver
import link.rdcn.dacp.server.DacpServer
import link.rdcn.struct.DataFrame
import link.rdcn.user.UsernamePassword
import org.apache.arrow.flight.FlightRuntimeException
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

import java.io.{File, IOException}
import java.net.ConnectException


class ServerExceptionTest {

  //服务未启动
  @Test
  def testServerNotRunning(): Unit = {
    val exception = assertThrows(
      classOf[FlightRuntimeException],
      () => DacpClient.connect("dacp://1.1.1.1:3101", UsernamePassword(adminUsername, adminPassword))

    )
    assertTrue(exception.getCause.isInstanceOf[ConnectException])
  }

  //服务重复启动
  @Test()
  def testServerAlreadyStarted(): Unit = {
    //    flightServer.start(configCache)
    val serverException = assertThrows(
      classOf[IOException],
      () => {
        val flightServer = new DacpServer(emptyDataProvider, emptyDataReceiver, emptyAuthProvider)
        flightServer.start(configCache)
      }
    )
    assertTrue(serverException.isInstanceOf[IOException])
  }
  //访问时断开连接 server closed
  //url错误（端口和ip）

}
