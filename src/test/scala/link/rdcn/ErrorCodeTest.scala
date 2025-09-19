package link.rdcn

import link.rdcn.client.exception.ClientException
import link.rdcn.server.exception.ServerException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class ErrorCodeTest {

  @Test()
  def testInvalidError(): Unit = {
    val exception = new ClientException(ErrorCode.UNKNOWN_ERROR)
    assertEquals(ErrorCode.UNKNOWN_ERROR, exception.getErrorCode)
  }

  @Test()
  def testErrorCodeNotExist(): Unit = {
    val exception = new ServerException() //未设置ErrorCode
    assertEquals(ErrorCode.ERROR_CODE_NOT_EXIST, exception.getErrorCode)
  }
}
