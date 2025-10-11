package link.rdcn.dacp.optree.fifo

import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import java.io.File
import java.nio.file.Files

class FilePipeTest {

  private var tempFile: File = _
  private var filePipe: FilePipe = _

  /**
   * FilePipe 的一个具体实现，用于测试。
   */
  private class TestFilePipe(file: File) extends FilePipe(file) {
    override def dataFrame(): link.rdcn.struct.DataFrame = {
      // 对于文件操作的测试，此方法的具体实现不重要。
      null
    }
  }

  /**
   * 在每个测试方法执行前运行。
   * 负责创建临时的文件路径，并确保测试环境干净。
   */
  @BeforeEach
  def setUp(): Unit = {
    // 为每个测试创建一个唯一的临时文件路径，避免相互干扰。
    val tempDir = System.getProperty("java.io.tmpdir")
    tempFile = new File(tempDir, s"test-pipe-${System.nanoTime()}")

    // 确保在测试开始前，该文件不存在。
    Files.deleteIfExists(tempFile.toPath)

    // 初始化被测对象。
    filePipe = new TestFilePipe(tempFile)
  }

  /**
   * 在每个测试方法执行后运行。
   * 负责清理测试过程中创建的文件。
   */
  @AfterEach
  def tearDown(): Unit = {
    // 确保测试结束后，清理文件，保持环境整洁。
    Files.deleteIfExists(tempFile.toPath)
  }

  @Test
  def testCreate_SuccessWhenFileDoesNotExist(): Unit = {
    // 前置条件：断言文件确实不存在。
    assertFalse(tempFile.exists(), s"测试前，临时文件 '${tempFile.getAbsolutePath}' 不应存在")

    // 执行操作
    filePipe.create()

    // 后置条件：断言命名管道文件已被创建。
    assertTrue(tempFile.exists(), s"调用 create() 后，命名管道文件 '${tempFile.getAbsolutePath}' 应被创建")
  }

  @Test
  def testCreate_ThrowsExceptionWhenFileExists(): Unit = {
    // 准备：手动在目标路径创建一个普通文件。
    tempFile.createNewFile()
    assertTrue(tempFile.exists(), "测试设置失败：无法创建用于测试的临时文件")

    // 执行并验证：断言当文件已存在时，调用 create() 会抛出异常。
    val exception = assertThrows(classOf[Exception], () => {
      filePipe.create()
    }, "当文件已存在时，create() 方法应抛出异常")

    // 验证异常信息是否符合预期。
    val expectedMessage = s"init RowFilePipe fail ${tempFile.getAbsolutePath} exists"
    assertEquals(expectedMessage, exception.getMessage, "异常信息不符合预期")
  }

  @Test
  def testDelete_DeletesExistingFile(): Unit = {
    // 准备：先创建命名管道文件。
    filePipe.create()
    assertTrue(tempFile.exists(), "测试设置失败：无法创建用于测试的命名管道")

    // 执行操作
    filePipe.delete()

    // 验证：断言文件已被成功删除。
    assertFalse(tempFile.exists(), s"调用 delete() 后，文件 '${tempFile.getAbsolutePath}' 应被删除")
  }

  @Test
  def testDelete_DoesNotThrowWhenFileDoesNotExist(): Unit = {
    // 前置条件：断言文件不存在。
    assertFalse(tempFile.exists(), "测试前，文件不应存在")

    try {
      filePipe.delete()
    } catch {
      case e: Exception =>
        fail(s"当文件不存在时，delete() 方法不应抛出异常，但它抛出了: ${e.getMessage}")
    }
  }

  @Test
  def testPath_ReturnsCorrectAbsolutePath(): Unit = {
    // 执行操作
    val actualPath = filePipe.path

    // 验证：返回的路径应与文件的绝对路径一致。
    val expectedPath = tempFile.getAbsolutePath
    assertEquals(expectedPath, actualPath, "path() 方法返回的路径不正确")
  }
}