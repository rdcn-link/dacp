package link.rdcn.optree

import jep.SharedInterpreter
import link.rdcn.ConfigLoader
import link.rdcn.TestBase.getResourcePath
import link.rdcn.TestProvider.dataProvider
import link.rdcn.dacp.FairdConfig
import link.rdcn.dacp.client.DacpClient.protocolSchema
import link.rdcn.dacp.optree.{CppBin, FlowExecutionContext, JavaJar, JepInterpreterManager, OperatorRepository, PythonBin, RepositoryClient}
import link.rdcn.dacp.util.DataFrameMountUtils.mountDataFrameToTempPath
import link.rdcn.log.LoggerFactory
import link.rdcn.operation.{FunctionWrapper, LangType, SharedInterpreterManager}
import link.rdcn.struct.ValueType.IntType
import link.rdcn.struct._
import org.json.JSONObject
import org.junit.jupiter.api.Test

import java.io.{BufferedReader, FileInputStream, InputStreamReader}
import java.nio.file.Paths
import java.util.Base64

/**
 * @Author renhao
 * @Description:
 * @Data 2025/7/16 15:42
 * @Modified By:
 */
class FunctionWrapperTest {
  val rows = Seq(Row.fromSeq(Seq(1, 2))).iterator
  val dataFrame = DefaultDataFrame(StructType.empty.add("col_1", ValueType.IntType).add("col_2", ValueType.IntType), ClosableIterator(rows)())
  val dataFrames = Seq(dataFrame)
  ConfigLoader.init(getResourcePath(""))
  LoggerFactory.setDftpConfig(ConfigLoader.fairdConfig)



  @Test
  def pythonBinTest(): Unit = {
    System.setProperty("java.library.path", "/opt/homebrew/lib/python3.11/site-packages/jep/")
    val whlPath = Paths.get(ConfigLoader.fairdConfig.fairdHome, "lib", "link-0.1-py3-none-any.whl").toString
    val pythonBin = PythonBin("normalize",whlPath)
    val df = pythonBin.applyToDataFrames(dataFrames, ctx)
    df.foreach(row => {
      assert(row._1 == 0.33)
      assert(row._2 == 0.67)
    })
  }

  @Test
  def javaJarTest(): Unit = {
    val javaJar = JavaJar(Paths.get(ctx.fairdConfig.fairdHome, "faird-plugin-impl-0.5.0-20250920.jar").toString(),"Transformer11")
    val newDataFrame = javaJar.applyToInput(dataFrames, ctx).asInstanceOf[DataFrame]
    newDataFrame.foreach(row => {
      assert(row._1 == 1)
      assert(row._2 == 2)
      assert(row._3 == 100)
    })
  }

  @Test
  def cppBinTest(): Unit = {
    val cppPath = Paths.get(ConfigLoader.fairdConfig.fairdHome, "lib", "cpp", "cpp_processor.exe").toString
    val cppBin = CppBin(cppPath)
    val newDf = cppBin.applyToInput(dataFrames, ctx).asInstanceOf[DataFrame]
    newDf.foreach(row => {
      assert(row._1 == true)
    })
  }

  @Test
  def dataFrameFuseMountTest(): Unit = {
    val rows = (0 until 5).toList.map { i =>
      Row.fromSeq(Seq(i + 1, i + 2))
    }.iterator

    val schema = StructType.empty
      .add("col_1", IntType)
      .add("col_2", IntType)

    val df = DefaultDataFrame(schema, ClosableIterator(rows)())

    mountDataFrameToTempPath(df, file => {
      if (file != null) {
        val reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)))
        try {
          var index = 0
          var line: String = reader.readLine()
          while (line != null) {
            println(line)
            try {
              //              val row = Row.fromJsonString(line)
              //              assert(row._2 == 1 + index)
              //              assert(row._1 == 2 + index)
            } catch {
              case ex: Throwable =>
                println(s"Error parsing line $index: ${ex.getMessage}")
                throw ex
            }
            index += 1
            try {
              line = reader.readLine()
            } catch {
              case e: Exception => throw e
            }
          }
        } finally {
          reader.close()
        }
      } else {
        println("No batch.json file found!")
      }
    })
  }

  @Test
  def consumeFuseMountDataTest(): Unit = {
    val rows = (0 until 1000).toList.map { i =>
      Row.fromSeq(Seq(i + 1, i + 2))
    }.iterator

    val schema = StructType.empty
      .add("col_1", IntType)
      .add("col_2", IntType)

    val df = DefaultDataFrame(schema, ClosableIterator(rows)())
    mountDataFrameToTempPath(df, file => {
      val inputPath = file.toPath.toString
      val outPutPath = "/home/renhao/IdeaProjects/faird-java/faird-core/src/test/resources/temp/temp.json"
      val cppPath = "/home/renhao/IdeaProjects/faird-java/faird-core/src/test/resources/lib/cpp/processor"
      runCppProcess(cppPath = cppPath, inputPath = inputPath, outputPath = outPutPath)
    })
  }

  private def runCppProcess(cppPath: String, inputPath: String, outputPath: String): Int = {
    val pb = new ProcessBuilder(cppPath, inputPath, outputPath)
    pb.inheritIO() // 继承当前进程的 stdout/stderr，调试时很有用

    val process = pb.start()
    val exitCode = process.waitFor()
    exitCode
  }

  def ctx = new FlowExecutionContext {
    override val fairdConfig: FairdConfig = ConfigLoader.fairdConfig
    override val pythonHome: String = fairdConfig.pythonHome
    var baseUrl: String = s"$protocolSchema://${fairdConfig.hostPosition}:${fairdConfig.hostPort}"

    override def getSharedInterpreter(): Option[SharedInterpreter] = Some(SharedInterpreterManager.getInterpreter)

    override def loadSourceDataFrame(dataFrameNameUrl: String): Option[DataFrame] = {
      val resourcePath = if (dataFrameNameUrl.startsWith(baseUrl)) dataFrameNameUrl.stripPrefix(baseUrl)
      else dataFrameNameUrl
      try {
        val dataStreamSource: DataStreamSource = dataProvider.getDataStreamSource(resourcePath)
        val dataFrame: DataFrame = DefaultDataFrame(dataStreamSource.schema, dataStreamSource.iterator)
        Some(dataFrame)
      } catch {
        case e: Exception =>
          None
      }
    }

    override def getRepositoryClient(): Option[OperatorRepository] = Some(new RepositoryClient("10.0.89.38", 8088))
  }

}

