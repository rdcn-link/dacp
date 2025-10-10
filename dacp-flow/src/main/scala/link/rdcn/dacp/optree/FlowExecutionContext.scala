package link.rdcn.dacp.optree

import jep.{SharedInterpreter, SubInterpreter}
import link.rdcn.operation.TransformOp
import link.rdcn.struct.DataFrame
import link.rdcn.user.Credentials

import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

/**
 * @Author renhao
 * @Description:
 * @Data 2025/9/26 16:00
 * @Modified By:
 */
trait FlowExecutionContext extends link.rdcn.operation.ExecutionContext {

  private[this] val asyncResults = new ConcurrentHashMap[TransformOp, Future[DataFrame]]()
  implicit protected val asyncExecutionContext: ExecutionContext =
    ExecutionContext.fromExecutorService(java.util.concurrent.Executors.newWorkStealingPool(8))

  def registerAsyncResult(transformOp: TransformOp, future: Future[DataFrame]): Unit = {
    asyncResults.put(transformOp, future)

    future.onComplete {
      case Success(df) =>
      case Failure(e) =>
        asyncResults.remove(transformOp)
        throw new Exception(s"TransformOp $transformOp failed", e)
    }(asyncExecutionContext)
  }

  def getAsyncResult(transformOp: TransformOp): Option[Future[DataFrame]] = {
    Option(asyncResults.get(transformOp))
  }

  val fairdHome: String

  def pythonHome: String

  def isAsyncEnabled: Boolean = false

  def loadRemoteDataFrame(baseUrl: String, path:String, credentials: Credentials): Option[DataFrame]

  def loadSourceDataFrame(dataFrameNameUrl: String): Option[DataFrame]

  def getSubInterpreter(sitePackagePath: String, whlPath: String): Option[SubInterpreter] =
    Some(JepInterpreterManager.getJepInterpreter(sitePackagePath, whlPath, Some(pythonHome)))

  def getRepositoryClient(): Option[OperatorRepository]
}
