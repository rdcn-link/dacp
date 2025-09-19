package link.rdcn.optree

import link.rdcn.TestBase.getResourcePath
import link.rdcn.dacp.optree.JepInterpreterManager
import link.rdcn.operation.SharedInterpreterManager
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

  @Test
  def getJepTest(): Unit = {
    val jep = SharedInterpreterManager.getInterpreter
  }

  @Test
  def javaCodeTest(): Unit = {
    val code =
      """
        |import java.util.*;
        |import link.rdcn.util.*;
        |import link.rdcn.client.dag.Transformer11;
        |import link.rdcn.struct.*;
        |
        |public class DynamicUDF implements Transformer11 {
        |    public DynamicUDF() {
        |        // 默认构造器，必须显式写出
        |    }
        |    @Override
        |    public link.rdcn.struct.DataFrame transform(final link.rdcn.struct.DataFrame dataFrame) {
        |            final scala.collection.Iterator<Row> iter = ((LocalDataFrame)dataFrame).stream();
        |            final scala.collection.Iterator<Row> rows =  new scala.collection.Iterator<Row>() {
        |            public boolean hasNext() {
        |                return iter.hasNext();
        |            }
        |            public Row next() {
        |                Row row = (Row)iter.next();
        |                return Row.fromJavaList(Arrays.asList(row.get(0), row.get(1), 100));
        |            }
        |        };
        |                return DataUtils.getDataFrameByStream(rows);
        |            }
        |}
        |""".stripMargin

    val compiler = new SimpleCompiler()
    compiler.cook(code)
    val clazz = compiler.getBytecodes.asScala.toMap
    val jo = new JSONObject()
    jo.put("type", LangType.JAVA_CODE.name)
    jo.put("clazz", Base64.getEncoder.encodeToString(SimpleSerializer.serialize(new java.util.HashMap[String, Array[Byte]](clazz.asJava))))
    val javaCode = FunctionWrapper(jo).asInstanceOf[JavaCode]
    val rows = DefaultDataFrame(StructType.empty.add("id", IntType).add("value", IntType), ClosableIterator(Seq(Row.fromSeq(Seq(1, 2))).iterator)())
    val newDataFrame = javaCode.applyToInput(rows.stream).asInstanceOf[DataFrame]
    newDataFrame.foreach(row => {
      assert(row._1 == 1)
      assert(row._2 == 2)
      assert(row._3 == 100)
    })
  }

  @Test
  def pythonBinTest(): Unit = {
    ConfigLoader.init(getResourcePath(""))
    val whlPath = Paths.get(ConfigLoader.fairdConfig.fairdHome, "lib", "link-0.1-py3-none-any.whl").toString
    val jo = new JSONObject()
    jo.put("type", LangType.PYTHON_BIN.name)
    jo.put("functionID", "aaa.bbb.id1")
    jo.put("functionName", "normalize")
    jo.put("whlPath", whlPath)
    val pythonBin = FunctionWrapper(jo).asInstanceOf[PythonBin]
    val rows = Seq(Row.fromSeq(Seq(1, 2))).iterator
    val jep = JepInterpreterManager.getJepInterpreter("id1", whlPath)
    val newRow = pythonBin.applyToInput(rows, Some(jep)).asInstanceOf[Iterator[Row]].next()
    assert(newRow._1 == 0.33)
    assert(newRow._2 == 0.67)
  }

  @Test
  def javaJarTest(): Unit = {
    ConfigLoader.init(getResourcePath(""))
    val jo = new JSONObject()
    jo.put("type", LangType.JAVA_JAR.name)
    jo.put("functionID", "aaa.bbb.id2")
    jo.put("fileName", "faird-plugin-impl-1.0-20250707.jar")
    val javaJar = FunctionWrapper(jo).asInstanceOf[JavaJar]
    val rows = Seq(Row.fromSeq(Seq(1, 2))).iterator
    val dataFrame = DefaultDataFrame(StructType.empty.add("col_1", ValueType.IntType).add("col_2", ValueType.IntType), ClosableIterator(rows)())
    val newDataFrame = javaJar.applyToInput(dataFrame).asInstanceOf[DataFrame]
    newDataFrame.foreach(row => {
      assert(row._1 == 1)
      assert(row._2 == 2)
      assert(row._3 == 100)
    })
  }

  @Test
  def cppBinTest(): Unit = {
    ConfigLoader.init(getResourcePath(""))
    val cppPath = Paths.get(ConfigLoader.fairdConfig.fairdHome, "lib", "cpp", "cpp_processor.exe").toString
    val jo = new JSONObject()
    jo.put("type", LangType.CPP_BIN.name)
    jo.put("functionID", "cpp_processor.exe")
    val cppBin = FunctionWrapper(jo).asInstanceOf[CppBin]
    val rows = Seq(Row.fromSeq(Seq(1, 2))).iterator
    val dataFrame = DefaultDataFrame(StructType.empty.add("col_1", ValueType.IntType).add("col_2", ValueType.IntType), ClosableIterator(rows)())
    val newDf = cppBin.applyToInput(dataFrame).asInstanceOf[DataFrame]
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

}

