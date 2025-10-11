package link.rdcn.dacp.optree

import link.rdcn.operation.SourceOp
import link.rdcn.struct.{DataFrame, DefaultDataFrame, StructType}
import link.rdcn.user.Credentials
import org.json.{JSONArray, JSONObject}
import org.junit.jupiter.api.Test

import java.nio.file.Paths
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

class TransformTreeTest {
  private val JYG_CONTAINER_NAME = "jyg-container"
  private val JYG_IMAGE = "registry.cn-hangzhou.aliyuncs.com/cnic-piflow/siltdam-jyg:latest"

  private val HOST_DIR = "/data2/work/ncdc/faird/temp"
  private val CONTAINER_DIR = "/mnt/data"
  private val OP1_DIR = Paths.get(CONTAINER_DIR, "op1", "op1.py").toString
  private val OP1_OUTPUT = Paths.get(HOST_DIR, "op1", "gully_slop_fifo.csv").toString
  private val OP2_DIR = Paths.get(CONTAINER_DIR, "op3", "op3.py").toString
  private val OP2_OUTPUT = Paths.get(HOST_DIR, "op3", "suscep_hdyro_fifo.csv").toString

  private def ctx = new FlowExecutionContext {
    override val isAsyncEnabled = true

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
//    commandArray1.put("ls /mnt")
    commandArray1.put(OP1_DIR)
    jo1.put("type", LangTypeV2.FILE_REPOSITORY_BUNDLE.name)
    jo1.put("command", commandArray1)
    jo1.put("outPutFilePath", OP1_OUTPUT)
    jo1.put("containerName", JYG_CONTAINER_NAME)

    val jo2 = new JSONObject()
    val commandArray2 = new JSONArray()
    commandArray2.put("python")
    commandArray2.put(OP2_DIR)
    jo2.put("type", LangTypeV2.FILE_REPOSITORY_BUNDLE.name)
    jo2.put("command", commandArray2)
    jo2.put("outPutFilePath", OP2_OUTPUT)
    jo2.put("containerName", JYG_CONTAINER_NAME)
    val transformTree2 = TransformerNode(TransformFunctionWrapper.fromJsonObject(jo2).asInstanceOf[FileRepositoryBundle], SourceOp(""))
    val transformTree1 = TransformerNode(TransformFunctionWrapper.fromJsonObject(jo1).asInstanceOf[FileRepositoryBundle], SourceOp(""))

        val future1: Future[Unit] = Future {
          println("transformTree1 execution started...")
    transformTree1.execute(ctx) // Assuming this returns Unit
          println("transformTree1 execution finished.")
        }

        val future2: Future[Unit] = Future {
          println("transformTree2 execution started...")
          transformTree2.execute(ctx) // Assuming this returns Unit
          println("transformTree2 execution finished.")
        }
        val combinedFuture = for {
          _ <- future1
    //      _ <- future2
        } yield ()
        Thread.sleep(2000)
        // 阻塞主线程，直到 combinedFuture 完成，或者超时
        println("主线程正在等待所有异步任务完成...")
        try {
          // 设置一个合理的超时时间，例如1分钟。如果超过这个时间任务还未完成，将会抛出 TimeoutException
          Await.result(combinedFuture, 1.minute)
          println("所有异步任务均已成功完成！")
        } catch {
          case e: Exception =>
            println(s"等待任务完成时发生错误: ${e.getMessage}")
        }
      }


}
