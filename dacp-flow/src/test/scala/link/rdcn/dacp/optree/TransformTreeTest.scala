package link.rdcn.dacp.optree

import link.rdcn.operation.SourceOp
import link.rdcn.struct.{DataFrame, DefaultDataFrame, StructType}
import link.rdcn.user.Credentials
import org.json.{JSONArray, JSONObject}
import org.junit.jupiter.api.Test

import java.nio.file.Paths
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class TransformTreeTest {
  private val JYG_CONTAINER_NAME = "jyg-container"

  private val HOST_DIR = "/data2/work/ncdc/faird/temp"
  private val CONTAINER_DIR = "/mnt/data"
  private val OP1_DIR = Paths.get(CONTAINER_DIR, "op1", "op1.py").toString
  private val OP1_OUTPUT = Paths.get(HOST_DIR, "op1", "gully_slop_fifo.csv").toString
  private val OP2_DIR = Paths.get(CONTAINER_DIR, "op3", "op3.py").toString
  private val OP2_OUTPUT = Paths.get(HOST_DIR, "op3", "suscep_hdyro_fifo.csv").toString

  private val ctx = new FlowExecutionContext {

    override val fairdHome: String = ""

    override def pythonHome: String = ""

    override def loadSourceDataFrame(dataFrameNameUrl: String): Option[DataFrame] = {
      Some(DefaultDataFrame(StructType.empty, Iterator.empty))
    }

    override def getRepositoryClient(): Option[OperatorRepository] = Some(new RepositoryClient("10.0.89.38", 8088))

    override def loadRemoteDataFrame(baseUrl: String, path: String, credentials: Credentials): Option[DataFrame] = ???
  }


  @Test
  def TransformTreeSimpleFIFOFlowTest(): Unit = {
    val jo1 = new JSONObject()
    val commandArray1 = new JSONArray()
    commandArray1.put("python")
    commandArray1.put(OP1_DIR)
    jo1.put("type", LangTypeV2.FILE_REPOSITORY_BUNDLE.name)
    jo1.put("command", commandArray1)
    jo1.put("outPutFilePath", OP1_OUTPUT)
    jo1.put("containerName", JYG_CONTAINER_NAME)

    val jo2 = new JSONObject()
    val commandArray2 = new JSONArray()
    val inputArray2 = new JSONArray()
    commandArray2.put("python")
    commandArray2.put(OP2_DIR)
    inputArray2.put(OP1_OUTPUT)
    jo2.put("type", LangTypeV2.FILE_REPOSITORY_BUNDLE.name)
    jo2.put("command", commandArray2)
    jo2.put("outPutFilePath", OP2_OUTPUT)
    jo2.put("containerName", JYG_CONTAINER_NAME)
    jo2.put("inputFifoPath", inputArray2)
    val transformTree2 = TransformerNode(TransformFunctionWrapper.fromJsonObject(jo2).asInstanceOf[FileRepositoryBundle], SourceOp(""))
    val transformTree1 = TransformerNode(TransformFunctionWrapper.fromJsonObject(jo1).asInstanceOf[FileRepositoryBundle], SourceOp(""))

    // 1. 将整个流水线操作封装在一个大的 Future 中，以便让它在后台异步运行。
    val pipelineFuture: Future[Unit] = Future {
      // 在这个 Future 内部，我们并行启动两个需要相互阻塞的脚本

      // 2. 在一个线程中启动生产者（它会阻塞直到消费者连接）
      val producerFuture = Future {
        println("生产者(transformTree1)启动中...")
        transformTree1.execute(ctx) // 这个调用会阻塞
        println("生产者(transformTree1)执行完毕。")
      }

      // 3. 在另一个线程中启动消费者（它会阻塞直到生产者写入）
      val consumerFuture = Future {
        println("消费者(transformTree2)启动中...")
        transformTree2.execute(ctx) // 这个调用会阻塞
        println("消费者(transformTree2)执行完毕。")
      }

      // 4. 等待这两个部分都完成
      //    用 for-comprehension 等待两个 future，确保整个 pipelineFuture 在两者都结束后才算完成
      val combined = for {
        _ <- producerFuture
        _ <- consumerFuture
      } yield ()

      // 在这里阻塞 Future 内部的线程，直到两个脚本都完成
      Await.result(combined, 5.minutes)
    }

    // 5. 在主线程中等待整个流水线的完成
    println("主线程等待整个流水线完成...")
    try {
      Await.result(pipelineFuture, 5.minutes)
      println("测试成功：生产者-消费者流水线成功执行完毕！")
    } catch {
      case e: Exception =>
        println(s"测试失败：流水线执行出错: ${e.getMessage}")
        e.printStackTrace()
    }
  }


}
