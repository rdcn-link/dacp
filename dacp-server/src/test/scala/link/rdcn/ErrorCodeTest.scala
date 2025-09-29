package link.rdcn

import link.rdcn.TestBase._
import org.apache.arrow.flight.{CallStatus, FlightRuntimeException}
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.api.Test

class ErrorCodeTest {
  @Test()
  def testErrorCodeNotExist(): Unit = {
    val ServerException = assertThrows(
      classOf[FlightRuntimeException],
      () => sendErrorWithFlightStatus(0,"unknown error")
    )
    assertEquals(CallStatus.UNKNOWN.code(),ServerException.status().code())
    assertEquals("unknown error",ServerException.status().description())
  }
}
