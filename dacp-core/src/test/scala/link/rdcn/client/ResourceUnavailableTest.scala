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
        () => dc.getByPath("/csv/invalid_not.csv").foreach(_ => {})
    )
    assertEquals(CallStatus.UNAUTHORIZED.code(), serverException.status().code())
    assertEquals("access dataFrame /csv/invalid_not.csv Forbidden",serverException.status().description())
  }
}
