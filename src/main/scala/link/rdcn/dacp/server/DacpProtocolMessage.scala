package link.rdcn.dacp.server

import link.rdcn.operation.TransformOp
import link.rdcn.struct.DataFrame
import link.rdcn.user.UserPrincipal

/**
 * @Author renhao
 * @Description:
 * @Data 2025/9/16 16:41
 * @Modified By:
 */
trait CookRequest {
  def getTransformTree: TransformOp

  def getRequestUserPrincipal(): UserPrincipal
}

trait CookResponse {
  def sendDataFrame(dataFrame: DataFrame): Unit

  def sendError(code: Int, message: String): Unit
}