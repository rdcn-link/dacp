package link.rdcn.dacp.optree

import jep.SubInterpreter
import link.rdcn.operation.ExecutionContext
import link.rdcn.struct.DataFrame
import link.rdcn.user.Credentials

/**
 * @Author renhao
 * @Description:
 * @Data 2025/9/26 16:00
 * @Modified By:
 */
trait FlowExecutionContext extends ExecutionContext {

  val fairdHome: String

  def pythonHome: String

  def loadRemoteDataFrame(baseUrl: String, path:String, credentials: Credentials): Option[DataFrame]

  def loadSourceDataFrame(dataFrameNameUrl: String): Option[DataFrame]

  def getSubInterpreter(sitePackagePath: String, whlPath: String): Option[SubInterpreter] =
    Some(JepInterpreterManager.getJepInterpreter(sitePackagePath, whlPath, Some(pythonHome)))

  def getRepositoryClient(): Option[OperatorRepository]
}
