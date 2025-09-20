package link.rdcn.server

import link.rdcn.TestBase.{adminPassword, adminUsername}
import link.rdcn.TestProvider
import link.rdcn.dacp.client.DacpClient
import link.rdcn.user.{Credentials, UsernamePassword}
import org.apache.arrow.flight.{CallStatus, FlightRuntimeException}
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.api.Test

class FlightServerAuthHandlerTest extends TestProvider {

  @Test()
  def testLoginWhenUsernameIsNotAdmin(): Unit = {
    // 模拟非admin用户的情况进行测试
    val serverException = assertThrows(
      classOf[FlightRuntimeException],
      () => DacpClient.connect("dacp://0.0.0.0:3101", UsernamePassword("NotAdmin", adminPassword))
    )

    assertEquals(CallStatus.UNAUTHORIZED.withDescription("User not logged in!").toRuntimeException, serverException)
  }

  @Test()
  def testInvalidCredentials(): Unit = {
    val serverException = assertThrows(
      classOf[FlightRuntimeException],
      () => DacpClient.connect("dacp://0.0.0.0:3101", UsernamePassword(adminUsername, "wrongPassword"))
    )

    assertEquals(CallStatus.UNAUTHENTICATED.code(), serverException.asInstanceOf[FlightRuntimeException].status().code())
  }

  //匿名访问DataFrame失败
  @Test
  def testAnonymousAccessDataFrameFalse(): Unit = {
    val dc = DacpClient.connect("dacp://0.0.0.0:3101", Credentials.ANONYMOUS)
    val serverException = assertThrows(
      classOf[FlightRuntimeException],
      () => dc.getByPath("/csv/data_1.csv").foreach(_ => ())
    )
    assertEquals(CallStatus.UNAUTHORIZED.withDescription("User unauthorized!").toRuntimeException, serverException.asInstanceOf[FlightRuntimeException])
  }

}
