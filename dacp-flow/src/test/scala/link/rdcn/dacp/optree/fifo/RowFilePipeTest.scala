package link.rdcn.dacp.optree.fifo

import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

import java.io.{File, PrintWriter}
import java.nio.file.{Files, Path}

class RowFilePipeTest {

  private var tempFile: File = _
  private var rowFilePipe: RowFilePipe = _

  @BeforeEach
  def setUp(): Unit = {
    // 为每个测试创建一个唯一的临时文件
    tempFile = Files.createTempFile("test_row_pipe_", ".txt").toFile
    rowFilePipe = new RowFilePipe(tempFile)
  }

  @AfterEach
  def tearDown(): Unit = {
    // 清理临时文件
    Files.deleteIfExists(tempFile.toPath)
  }

  @Test
  def testWriteAndRead_MultipleLines(): Unit = {
    val lines = List("hello world", "line 2", "测试第三行")

    // 执行写操作
    rowFilePipe.write(lines.iterator)

    // 执行读操作
    val readLines = rowFilePipe.read().toList

    // 验证：读取的内容应与写入的内容完全一致
    assertEquals(lines, readLines, "读取的内容与写入的内容不匹配")
  }

  @Test
  def testWrite_OverwritesExistingContent(): Unit = {
    // 第一次写入
    rowFilePipe.write(Iterator("initial content"))

    val newLines = List("new line 1", "new line 2")
    // 第二次写入，应覆盖旧内容
    rowFilePipe.write(newLines.iterator)

    val readLines = rowFilePipe.read().toList
    assertEquals(newLines, readLines, "write 方法未能正确覆盖已有内容")
  }

  @Test
  def testRead_EmptyFile(): Unit = {
    // 确保文件是空的
    val iterator = rowFilePipe.read()

    // 验证：对于空文件，hasNext 应为 false
    assertFalse(iterator.hasNext, "空文件的迭代器 hasNext 应返回 false")

    // 验证：读取结果应为空列表
    assertEquals(List.empty[String], iterator.toList, "读取空文件应返回空列表")
  }

  @Test
  def testRead_ThrowsExceptionAfterEnd(): Unit = {
    rowFilePipe.write(Iterator("one line"))
    val iterator = rowFilePipe.read()

    iterator.next()
    assertThrows(classOf[NoSuchElementException], () => {
      val _ = iterator.next()
    }, "在迭代器耗尽后调用 next() 应抛出 NoSuchElementException")
  }

  @Test
  def testFromExistFile_CopiesContentCorrectly(): Unit = {
    // 准备一个源文件并写入内容
    val sourceFile = Files.createTempFile("source_file_", ".txt").toFile
    val sourceLines = List("source line 1", "源文件第二行")
    val writer = new PrintWriter(sourceFile)
    try {
      sourceLines.foreach(writer.println)
    } finally {
      writer.close()
    }

    // 执行 fromExistFile 操作
    rowFilePipe.fromExistFile(sourceFile)

    // 读取 RowFilePipe 的内容并验证
    val copiedLines = rowFilePipe.read().toList
    assertEquals(sourceLines, copiedLines, "fromExistFile 未能正确复制文件内容")

    // 清理源文件
    Files.deleteIfExists(sourceFile.toPath)
  }

  @Test
  def testDataFrame_CreatesDataFrameWithCorrectSchemaAndData(): Unit = {
    val lines = List("line one", "line two")
    rowFilePipe.write(lines.iterator)

    // 执行 dataFrame() 方法
    val df = rowFilePipe.dataFrame()

    // 验证 Schema
    val expectedSchema = link.rdcn.struct.StructType.empty.add("content", link.rdcn.struct.ValueType.StringType)
    assertEquals(expectedSchema, df.schema, "DataFrame 的 Schema 不正确")

    // 验证数据内容
    val expectedData = lines.map(line => link.rdcn.struct.Row.fromSeq(line))
    val actualData = df.collect()
    assertEquals(expectedData.toString(), actualData.toString(), "DataFrame 的数据内容不正确")
  }

  @Test
  def testCompanionObject_CreateEmptyFile_CreatesFifoFile(): Unit = {
    // 注意: 此测试依赖于 `mkfifo` 命令, 只能在 Unix-like (Linux, macOS) 系统上运行
    val osName = System.getProperty("os.name").toLowerCase
    if (osName.contains("win")) {
      println("跳过 FIFO 文件创建测试，因为当前操作系统是 Windows。")
      return
    }

    var pipeFile: Path = null
    try {
      pipeFile = Files.createTempFile("test_fifo_", ".txt")
      Files.delete(pipeFile) // createTempFile 会创建一个常规文件，先删除它

      // 使用 String 路径测试
      val pipeFromString = RowFilePipe.createEmptyFile(pipeFile.toString)
      assertTrue(Files.exists(pipeFile), "createEmptyFile(String) 未能创建文件")
      // 注意：通过标准 Java/Scala API 很难直接判断一个文件是否为 FIFO。
      // 在这里，我们相信如果 `mkfifo` 命令没有报错且文件存在，那么它就是正确的。
      pipeFromString.delete()
      assertFalse(Files.exists(pipeFile), "delete() 未能删除由 String 路径创建的文件")

      // 使用 File 对象测试
      val pipeFromFile = RowFilePipe.createEmptyFile(pipeFile.toFile)
      assertTrue(Files.exists(pipeFile), "createEmptyFile(File) 未能创建文件")
      pipeFromFile.delete()
      assertFalse(Files.exists(pipeFile), "delete() 未能删除由 File 对象创建的文件")
    } finally {
      if (pipeFile != null) Files.deleteIfExists(pipeFile)
    }
  }
}