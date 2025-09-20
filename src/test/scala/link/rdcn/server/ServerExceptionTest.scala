package link.rdcn.server

import link.rdcn.TestBase.{adminPassword, adminUsername}
import link.rdcn.TestEmptyProvider._
import link.rdcn.dacp.client.DacpClient
import link.rdcn.dacp.server.DacpServer
import link.rdcn.user.UsernamePassword
import org.apache.arrow.flight.FlightRuntimeException
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

import java.io.IOException
import java.net.ConnectException


class ServerExceptionTest {

  //服务未启动
  @Test
  def testServerNotRunning(): Unit = {
    val exception = assertThrows(
      classOf[FlightRuntimeException],
      () => DacpClient.connect("dacp://localhost:3101", UsernamePassword(adminUsername, adminPassword))

    )
    assertTrue(exception.getCause.isInstanceOf[ConnectException])
  }

  //端口被占用
  @Test()
  def testAddressAlreadyInUse(): Unit = {
    val flightServer1 = new DacpServer(emptyDataProvider, emptyDataReceiver, emptyAuthProvider)
    flightServer1.start(configCache)
    val flightServer2 = new DacpServer(emptyDataProvider, emptyDataReceiver, emptyAuthProvider)
    val serverException = assertThrows(
      classOf[IOException],
      () => flightServer2.start(configCache)
    )
    flightServer1.close()
    assertTrue(serverException.isInstanceOf[IOException])
    assertTrue(!serverException.isInstanceOf[ConnectException])
  }

  //服务重复启动
  @Test()
  def testServerAlreadyStarted(): Unit = {
    val flightServer = new DacpServer(emptyDataProvider, emptyDataReceiver, emptyAuthProvider)
    flightServer.start(configCache)
    val serverException = assertThrows(
      classOf[IllegalStateException],
      () => flightServer.start(configCache)
    )
    flightServer.close()
    assertTrue(serverException.isInstanceOf[IllegalStateException])
  }
  //访问时断开连接 server closed
  //url错误（端口和ip）

}
