package link.rdcn.client

import link.rdcn.TestBase.getOutputDir
import link.rdcn.TestProvider
import link.rdcn.TestProvider._
import link.rdcn.client.DataFrameOperationTest._
import link.rdcn.client.recipe.{Flow, SourceNode, Transformer11}
import link.rdcn.struct._
import link.rdcn.util.ExceptionHandler
import org.apache.arrow.flight.FlightRuntimeException
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.io.{PrintWriter, StringWriter}
import java.nio.file.Paths
import scala.io.Source

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/16 18:08
 * @Modified By:
 */

object DataFrameOperationTest {
  val udfB = (num: Int) => new Transformer11 {
    override def transform(dataFrame: DataFrame): DataFrame = {
      dataFrame.map(row => Row.fromTuple(row.getAs[Long](0) + num, row.get(1)))
    }
  }

  val udfC = (num: Int) => new Transformer11 {
    override def transform(dataFrame: DataFrame): DataFrame = {
      dataFrame.map(row => Row.fromTuple(row.getAs[Long](0), row.get(1), num))
    }
  }

  val udfD = new Transformer11 {
    override def transform(dataFrame: DataFrame): DataFrame = {
      dataFrame.map(row => Row.fromTuple(row.getAs[Long](0) * 2, row.get(1)))
    }
  }

  val udfE = new Transformer11 {
    override def transform(dataFrame: DataFrame): DataFrame = {
      dataFrame.map(row => Row(row.get(0)))
    }
  }


  def getLine(row: Row): String = {
    val delimiter = ","
    row.toSeq.map(_.toString).mkString(delimiter) + '\n'
  }

  def transformB(lines: Seq[String], transformationNum: Int): Seq[String] = {
    lines.map { line =>
      val cols = line.split(",")
      val id = cols(0).toLong
      val rest = cols.tail.mkString(",")
      s"${id + transformationNum},$rest"
    }
  }

  def transformC(lines: Seq[String], transformationNum: Int): Seq[String] = {
    lines.map { line =>
      val cols = line.split(",")
      val id = cols(0).toLong
      val rest = cols.tail.mkString(",")
      s"$id,$rest,$transformationNum"
    }
  }

  def transformD(lines: Seq[String]): Seq[String] = {
    lines.map { line =>
      val cols = line.split(",")
      val id = cols(0).toLong
      val rest = cols.tail.mkString(",")
      s"${id * 2},$rest"
    }
  }

  def transformE(lines: Seq[String]): Seq[String] = {
    lines.map { line =>
      val cols = line.split(",")
      val id = cols(0).toLong
      val rest = cols.tail.mkString(",")
      s"$id"
    }
  }

  def addLineBreak(lines: Seq[String]): Seq[String] = {
    lines.map { line => line + "\n" }
  }

}

class DataFrameOperationTest extends TestProvider {
  val outputDir = getOutputDir("test_output", "output")


  @Test
  def testDataFrameForEach(): Unit = {
    val expectedOutput = Source.fromFile(Paths.get(csvDir, "data_1.csv").toString).getLines().toSeq.tail.mkString("\n") + "\n"
    val df = dc.getByPath("/csv/data_1.csv")
    val stringWriter = new StringWriter()
    val printWriter = new PrintWriter(stringWriter)
    df.foreach { row =>
      printWriter.write(getLine(row))
    }
    printWriter.flush()
    val actualOutput = stringWriter.toString
    assertEquals(expectedOutput, actualOutput, "Unexpected output from foreach operation")
  }


  @ParameterizedTest
  @ValueSource(ints = Array(10))
  def testDataFrameLimit(num: Int): Unit = {
    val expectedOutput = Source.fromFile(Paths.get(csvDir, "data_1.csv").toString).getLines().toSeq.tail.take(num).mkString("\n") + "\n"
    val df = dc.getByPath("/csv/data_1.csv")
    val stringWriter = new StringWriter()
    val printWriter = new PrintWriter(stringWriter)
    df.limit(num).foreach { row =>
      printWriter.write(getLine(row))
    }
    printWriter.flush()
    val actualOutput = stringWriter.toString
    assertEquals(expectedOutput, actualOutput, "Unexpected output from limit operation")
  }
  //懒计算测试

  @ParameterizedTest
  @ValueSource(ints = Array(2))
  def testDataFrameFilter(id: Int): Unit = {
    val expectedOutput = Source.fromFile(Paths.get(csvDir, "data_1.csv").toString).getLines().toSeq(id) + "\n"
    val df = dc.getByPath("/csv/data_1.csv")
    val stringWriter = new StringWriter()
    val printWriter = new PrintWriter(stringWriter)
    val rowFilter: Row => Boolean = (row: Row) => row.getAs[Long](0) == id

    //匿名函数
    df.filter(rowFilter).foreach { row =>
      printWriter.write(getLine(row))
    }
    printWriter.flush()
    val actualOutput = stringWriter.toString
    assertEquals(expectedOutput, actualOutput, "Unexpected output from filter operation")
  }

  @ParameterizedTest
  @ValueSource(ints = Array(10))
  def testDataFrameMap(num: Long): Unit = {
    val expectedOutput = Source.fromFile(Paths.get(csvDir, "data_1.csv").toString).getLines()
      .toSeq
      .tail // 跳过标题行
      .map { line =>
        val cols = line.split(",") // 按逗号拆分列
        val id = cols(0).toLong + num // 第一列转Int并+1
        s"$id,${cols.tail.mkString}" // 拼接回剩余列
      }
      .mkString("\n") + "\n"
    val df = dc.getByPath("/csv/data_1.csv")
    val stringWriter = new StringWriter()
    val printWriter = new PrintWriter(stringWriter)
    val rowMapper: Row => Row = row => Row(row.getAs[Long](0) + num, row.get(1))

    try {
      df.map(rowMapper).foreach { row =>

        printWriter.write(getLine(row))
      }
    } catch {
      case e: FlightRuntimeException => println(ExceptionHandler.getErrorCode(e))
    }

    printWriter.flush()
    val actualOutput = stringWriter.toString
    assertEquals(expectedOutput, actualOutput, "Unexpected output from map operation")
  }

  @Test
  def testDataFrameMapColumn(): Unit = {
    val expectedOutput = Source.fromFile(Paths.get(csvDir, "data_1.csv").toString).getLines()
      .toSeq
      .tail // 跳过标题行
      .map { line =>
        val cols = line.split(",") // 按逗号拆分列
        s"${cols.tail.mkString}" + "\n" // 拼接剩余列
      }
      .mkString("")
    val df = dc.getByPath("/csv/data_1.csv")
    val stringWriter = new StringWriter()
    val printWriter = new PrintWriter(stringWriter)
    val rowMapper: Row => Row = row => Row(row.get(1))

    try {
      df.map(rowMapper).foreach { row =>
        printWriter.write(getLine(row))
      }
    } catch {
      case e: FlightRuntimeException => println(ExceptionHandler.getErrorCode(e))
    }

    printWriter.flush()
    val actualOutput = stringWriter.toString
    assertEquals(expectedOutput, actualOutput, "Unexpected output from map operation")
  }

  @Test
  def testDataFrameSelect(): Unit = {
    val expectedOutput = Source.fromFile(Paths.get(csvDir, "data_1.csv").toString).getLines()
      .toSeq
      .tail // 跳过标题行
      .map { line =>
        val cols = line.split(",") // 按逗号拆分列
        s"${cols.tail.mkString}" // 拼接剩余列
      }
      .mkString("\n") + "\n"
    val df = dc.getByPath("/csv/data_1.csv")
    val stringWriter = new StringWriter()
    val printWriter = new PrintWriter(stringWriter)

    try {
      df.select("value").foreach { row =>
        printWriter.write(getLine(row))
      }
    } catch {
      case e: FlightRuntimeException => println(ExceptionHandler.getErrorCode(e))
    }

    printWriter.flush()
    val actualOutput = stringWriter.toString
    assertEquals(expectedOutput, actualOutput, "Unexpected output from map operation")
  }

  //A --take--> B
  @ParameterizedTest
  @ValueSource(ints = Array(0, 10, 20000))
  def testDataFrameUDFTake(num: Int): Unit = {
    val lines = Source.fromFile(Paths.get(csvDir, "data_1.csv").toString).getLines().toSeq.tail
    val expectedOutput = lines.take(num).map(line => line + '\n').mkString("")

    val udf = new Transformer11 {
      override def transform(dataFrame: DataFrame): DataFrame = {
        dataFrame.limit(num)
      }
    }

    val transformerDAG = Flow(
      Map(
        "A" -> SourceNode("/csv/data_1.csv"),
        "B" -> udf
      ),
      Map(
        "A" -> Seq("B")
      )
    )
    val dfs: ExecutionResult = dc.execute(transformerDAG)
    val actualOutputs = dfs.map().map { case (_, df) =>
      val stringWriter = new StringWriter()
      val printWriter = new PrintWriter(stringWriter)

      df.foreach { row =>
        printWriter.write(getLine(row))
      }
      printWriter.flush()
      stringWriter.toString
    }
    assertEquals(expectedOutput, actualOutputs.head)
  }

  //A --filter--> B
  @ParameterizedTest
  @ValueSource(ints = Array(0, 10, 20000))
  def testDataFrameUDFFilter(num: Int): Unit = {
    val lines = Source.fromFile(Paths.get(csvDir, "data_1.csv").toString).getLines().toSeq.tail
    val expectedOutput = lines.take(num).map(line => line + '\n').mkString("")

    val udf = new Transformer11 {
      override def transform(dataFrame: DataFrame): DataFrame = {
        dataFrame.filter(row => row.getAs[Long](0) <= num)
      }
    }

    val transformerDAG = Flow(
      Map(
        "A" -> SourceNode("/csv/data_1.csv"),
        "B" -> udf
      ),
      Map(
        "A" -> Seq("B")
      )
    )
    val dfs: ExecutionResult = dc.execute(transformerDAG)
    val actualOutputs = dfs.map().map { case (_, df) =>
      val stringWriter = new StringWriter()
      val printWriter = new PrintWriter(stringWriter)

      df.foreach { row =>
        printWriter.write(getLine(row))
      }
      printWriter.flush()
      stringWriter.toString
    }
    assertEquals(expectedOutput, actualOutputs.head)
  }

  //A --map--> B
  @ParameterizedTest
  @ValueSource(ints = Array(10))
  def testDataFrameUDFMap(num: Int): Unit = {
    val lines = Source.fromFile(Paths.get(csvDir, "data_1.csv").toString).getLines().toSeq.tail
    val expectedOutput = addLineBreak(transformB(lines, num)).mkString("")

    val transformerDAG = Flow(
      Map(
        "A" -> SourceNode("/csv/data_1.csv"),
        "B" -> udfB(num)
      ),
      Map(
        "A" -> Seq("B")
      )
    )
    val dfs: ExecutionResult = dc.execute(transformerDAG)
    val actualOutputs = dfs.map().map { case (_, df) =>
      val stringWriter = new StringWriter()
      val printWriter = new PrintWriter(stringWriter)

      df.foreach { row =>
        printWriter.write(getLine(row))
      }
      printWriter.flush()
      stringWriter.toString
    }
    assertEquals(expectedOutput, actualOutputs.head)
  }

  //A --> B --> C
  @ParameterizedTest
  @ValueSource(ints = Array(10))
  def testDataFrameUDFLinearDAG(num: Int): Unit = {
    val lines = Source.fromFile(Paths.get(csvDir, "data_1.csv").toString).getLines().toSeq.tail
    val expectedOutput = addLineBreak(transformC(transformB(lines, num), num)).mkString("")


    val transformerDAG = Flow(
      Map(
        "A" -> SourceNode("/csv/data_1.csv"),
        "B" -> udfB(num),
        "C" -> udfC(num)
      ),
      Map(
        "A" -> Seq("B"),
        "B" -> Seq("C")
      )
    )
    val dfs: ExecutionResult = dc.execute(transformerDAG)
    val actualOutputs = dfs.map().map { case (_, df) =>
      val stringWriter = new StringWriter()
      val printWriter = new PrintWriter(stringWriter)

      df.foreach { row =>
        printWriter.write(getLine(row))
      }
      printWriter.flush()
      stringWriter.toString
    }
    assertEquals(expectedOutput, actualOutputs.head)
  }

  //    A
  //   / \
  //  B   C
  @ParameterizedTest
  @ValueSource(ints = Array(10))
  def testDataFrameUDFForkDAG(num: Int): Unit = {
    val lines = Source.fromFile(Paths.get(csvDir, "data_1.csv").toString).getLines().toSeq.tail
    val expectedOutputAB = addLineBreak(transformB(lines, num)).mkString("")
    val expectedOutputAC = addLineBreak(transformC(lines, num)).mkString("")

    val transformerDAG = Flow(
      Map(
        "A" -> SourceNode("/csv/data_1.csv"),
        "B" -> udfB(num),
        "C" -> udfC(num)
      ),
      Map(
        "A" -> Seq("B", "C"),
      )
    )
    val dfs: ExecutionResult = dc.execute(transformerDAG)
    val actualOutputs = dfs.map().map { case (_, df) =>
      val stringWriter = new StringWriter()
      val printWriter = new PrintWriter(stringWriter)

      df.foreach { row =>
        printWriter.write(getLine(row))
      }
      printWriter.flush()
      stringWriter.toString
    }
    val listActualOutput = actualOutputs.toList
    assertEquals(expectedOutputAB, listActualOutput(0))
    assertEquals(expectedOutputAC, listActualOutput(1))
  }


  //  A   B
  //   \ /
  //    C
  @ParameterizedTest
  @ValueSource(ints = Array(10))
  def testDataFrameUDFJoinDAG(num: Int): Unit = {
    val lines1 = Source.fromFile(Paths.get(csvDir, "data_1.csv").toString).getLines().toSeq.tail
    val lines2 = Source.fromFile(Paths.get(csvDir, "data_2.csv").toString).getLines().toSeq.tail
    val expectedOutputAC = addLineBreak(transformC(lines1, num)).mkString
    val expectedOutputBC = addLineBreak(transformC(lines2, num)).mkString

    val transformerDAG = Flow(
      Map(
        "A" -> SourceNode("/csv/data_1.csv"),
        "B" -> SourceNode("/csv/data_2.csv"),
        "C" -> udfC(num)
      ),
      Map(
        "A" -> Seq("C"),
        "B" -> Seq("C")
      )
    )
    val dfs: ExecutionResult = dc.execute(transformerDAG)
    val actualOutputs = dfs.map().map { case (_, df) =>
      val stringWriter = new StringWriter()
      val printWriter = new PrintWriter(stringWriter)

      df.foreach { row =>
        printWriter.write(getLine(row))
      }
      printWriter.flush()
      stringWriter.toString
    }
    val listActualOutput = actualOutputs.toList
    assertEquals(expectedOutputAC, listActualOutput(0))
    assertEquals(expectedOutputBC, listActualOutput(1))
  }


  //      A
  //     / \
  //    B   C
  //     \ /
  //      D --> E
  @ParameterizedTest
  @ValueSource(ints = Array(10))
  def testDataFrameUDFHybridDAG(num: Int): Unit = {
    val lines1 = Source.fromFile(Paths.get(csvDir, "data_1.csv").toString).getLines().toSeq.tail
    val lines2 = Source.fromFile(Paths.get(csvDir, "data_1.csv").toString).getLines().toSeq.tail
    val expectedOutputABDE = addLineBreak(transformE(transformD(transformB(lines1, num)))).mkString
    val expectedOutputACDE = addLineBreak(transformE(transformD(transformC(lines2, num)))).mkString

    val transformerDAG = Flow(
      Map(
        "A" -> SourceNode("/csv/data_1.csv"),
        "B" -> udfB(num),
        "C" -> udfC(num),
        "D" -> udfD,
        "E" -> udfE
      ),
      Map(
        "A" -> Seq("B", "C"),
        "B" -> Seq("D"),
        "C" -> Seq("D"),
        "D" -> Seq("E")
      )
    )
    val dfs: ExecutionResult = dc.execute(transformerDAG)
    val actualOutputs = dfs.map().map { case (_, df) =>
      val stringWriter = new StringWriter()
      val printWriter = new PrintWriter(stringWriter)

      df.foreach { row =>
        printWriter.write(getLine(row))
      }
      printWriter.flush()
      stringWriter.toString
    }
    val listActualOutput = actualOutputs.toList
    assertEquals(expectedOutputABDE, listActualOutput(0))
    assertEquals(expectedOutputACDE, listActualOutput(1))
  }

  @ParameterizedTest
  @ValueSource(ints = Array(10))
  def transformerDAGPathDetectionTest(num: Int): Unit = {
    val dagNoStartEnd = Flow(
      Map(
        "A" -> SourceNode("/csv/data_1.csv"),
        "B" -> udfB(num),
        "C" -> udfC(num)
      ),
      Map(
        "A" -> Seq("B", "C"),
        "B" -> Seq("C"),
        "C" -> Seq("A")
      )
    )
    val exceptionNoRoot = assertThrows(classOf[IllegalArgumentException], () => {
      dc.execute(dagNoStartEnd)
    })
    assertTrue(exceptionNoRoot.getMessage.contains("graph might contain cycles or be empty"))

    val dagCycle = Flow(
      Map(
        "A" -> SourceNode("/csv/data_1.csv"),
        "B" -> udfB(num),
        "C" -> udfC(num)
      ),
      Map(
        "A" -> Seq("B", "C"),
        "B" -> Seq("C"),
        "C" -> Seq("B")
      )
    )
    val exceptionCycle = assertThrows(classOf[IllegalArgumentException], () => {
      dc.execute(dagCycle)
    })
    assertTrue(exceptionCycle.getMessage.contains("Cycle detected"))

    val notBeginWithSource = Flow(
      Map(
        "A" -> SourceNode("/csv/data_1.csv"),
        "B" -> udfB(num),
        "C" -> udfC(num)
      ),
      Map(
        "B" -> Seq("A", "C"),
        "A" -> Seq("C"),
      )
    )
    val exceptionNotBeginWithSource = assertThrows(classOf[IllegalArgumentException], () => {
      dc.execute(notBeginWithSource)
    })
    assertTrue(exceptionNotBeginWithSource.getMessage.contains("not of type SourceOp"))

    val opNotFoundByKey = Flow(
      Map(
        "A" -> SourceNode("/csv/data_1.csv"),
        "B" -> udfB(num),
        "C" -> udfC(num)
      ),
      Map(
        "J" -> Seq("N", "C"),
        "F" -> Seq("C"),
      )
    )
    val exceptionOpNotFoundByKey = assertThrows(classOf[IllegalArgumentException], () => {
      dc.execute(opNotFoundByKey)
    })
    assertTrue(exceptionOpNotFoundByKey.getMessage.contains("not defined in the node map"))
  }

  @Test
  def testDataFrameRowIndexAccess(): Unit = {
    val lines = Source.fromFile(Paths.get(csvDir, "data_1.csv").toString).getLines().toSeq.tail
    val expectedOutput = lines.mkString("\n") + "\n"

    val df = dc.getByPath("/csv/data_1.csv")
    val stringWriter = new StringWriter()
    val printWriter = new PrintWriter(stringWriter)

    try {
      df.foreach { row: Row =>
        printWriter.write(s"${row._1},${row._2}\n")
      }
    } catch {
      case e: FlightRuntimeException => println(ExceptionHandler.getErrorCode(e))
    }

    printWriter.flush()
    val actualOutput = stringWriter.toString
    assertEquals(expectedOutput, actualOutput, "Unexpected output from map operation")
  }


  @Test
  def testDataFrameRowIndexOutOfBound(): Unit = {
    val df = dc.getByPath("/csv/data_1.csv")
    val exceptionRowIndexOutOfBound = assertThrows(classOf[java.lang.IndexOutOfBoundsException], () => {
      df.foreach { row: Row =>
        println(row._3)
      }
    })
    assertEquals(exceptionRowIndexOutOfBound.getMessage, "2")
  }

}
