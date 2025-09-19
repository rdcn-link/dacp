package link.rdcn.client

import link.rdcn.TestProvider.dc
import link.rdcn.util.ExceptionHandler
import link.rdcn.{ErrorCode, TestProvider}
import org.apache.arrow.flight.FlightRuntimeException
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.api.Test

class ResourceUnavailableTest extends TestProvider {

  //df不存在
  @Test
  def testAccessInvalidDataFrame(): Unit = {
    val df = dc.getByPath("/csv/invalid.csv") // 假设没有该文件
    val serverException = assertThrows(
      classOf[FlightRuntimeException],
      () => df.foreach(_ => {})
    )
    assertEquals(ErrorCode.DATAFRAME_NOT_EXIST, ExceptionHandler.getErrorCode(serverException))
  }
}
