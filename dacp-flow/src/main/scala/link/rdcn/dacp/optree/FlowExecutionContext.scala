package link.rdcn.dacp.optree

import jep.SubInterpreter
import link.rdcn.struct.DataFrame
import link.rdcn.user.Credentials

import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.duration._
import scala.concurrent.{Await, Promise}

/**
 * @Author renhao
 * @Description:
 * @Data 2025/9/26 16:00
 * @Modified By:
 */
trait FlowExecutionContext extends link.rdcn.operation.ExecutionContext {

  private val fifoCreationPromises = new ConcurrentHashMap[String, Promise[Unit]]()

  /**
   * 获取或创建一个与特定 FIFO 文件路径关联的 Promise。
   * 这是线程安全的，确保对于同一个路径，我们只创建一个 Promise。
   */
  private def getOrCreateFifoPromise(path: String): Promise[Unit] = {
    fifoCreationPromises.computeIfAbsent(path, _ => Promise[Unit]())
  }

  /**
   * 消费者调用此方法来等待生产者发出信号。
   * 它会阻塞当前线程，直到对应的 FIFO 文件被创建。
   *
   * @param fifoPath 要等待的 FIFO 文件的绝对路径
   * @param timeout  等待的超时时间
   */
  def awaitFifoCreated(fifoPath: String, timeout: Duration): Unit = {
    println(s"INFO: [Consumer] Waiting for FIFO file to be created at: $fifoPath")
    val promise = getOrCreateFifoPromise(fifoPath)
    try {
      // Await.result 会阻塞，直到 promise.success(()) 被调用
      Await.result(promise.future, timeout)
      println(s"INFO: [Consumer] Signal received. FIFO file is ready at: $fifoPath")
    } catch {
      case e: Exception =>
        println(s"ERROR: [Consumer] Timed out while waiting for FIFO file at: $fifoPath")
        throw e // 超时后抛出异常
    }
  }

  /**
   * 生产者在创建完 FIFO 文件后调用此方法。
   * 唤醒正在等待的消费者。
   *
   * @param fifoPath 已创建的 FIFO 文件的绝对路径
   */
  def signalFifoCreated(fifoPath: String): Unit = {
    val promise = getOrCreateFifoPromise(fifoPath)
    promise.success(())
    println(s"INFO: [Producer] Signaled that FIFO file is ready at: $fifoPath")
  }

  val fairdHome: String

  def pythonHome: String

  def loadRemoteDataFrame(baseUrl: String, path:String, credentials: Credentials): Option[DataFrame]

  def loadSourceDataFrame(dataFrameNameUrl: String): Option[DataFrame]

  def getSubInterpreter(sitePackagePath: String, whlPath: String): Option[SubInterpreter] =
    Some(JepInterpreterManager.getJepInterpreter(sitePackagePath, whlPath, Some(pythonHome)))

  def getRepositoryClient(): Option[OperatorRepository]
}
