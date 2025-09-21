package link.rdcn.client

import link.rdcn.TestProvider
import link.rdcn.TestProvider.dc
import org.apache.arrow.flight.{CallStatus, FlightRuntimeException}
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.api.Test

class ResourceUnavailableTest extends TestProvider {

  //df不存在
  @Test
  def testAccessInvalidDataFrame(): Unit = {
    val serverException = assertThrows(
      classOf[FlightRuntimeException],
      {
        val df = dc.getByPath("/csv/invalid_not.csv") // 假设没有该文件
        () => df.foreach(_ => {})
      }
    )
    assertEquals(CallStatus.UNAUTHORIZED, serverException.status())
  }
}
