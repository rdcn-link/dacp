package link.rdcn

import link.rdcn.ConfigLoaderTest.getResourcePath
import org.apache.logging.log4j.{Level, LogManager, Logger}
import org.apache.logging.log4j.core.LoggerContext
import org.apache.logging.log4j.core.appender.{ConsoleAppender, FileAppender}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.parallel.{Execution, ExecutionMode}

import java.io.File
import java.nio.file.{Files, Path, Paths}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.JavaConverters.collectionAsScalaIterableConverter


@Execution(ExecutionMode.SAME_THREAD)
class LoggerTest extends Logging {
  val expectedMessage = "Test message"
  val filePath = Paths.get(ExpectedLogger.getFileName)
  val regex = "^(?<timestamp>\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2})\\s+\\[(?<thread>\\S+)\\]\\s+(?<level>[A-Z]{4,5})\\s+(?<logger>\\S+)\\s+-\\s+(?<message>.*)$".r

  //测试builder是否正确设置
  @Test
  def testBuilderProperties(): Unit = {

    val loggerContext = LogManager.getContext(false).asInstanceOf[LoggerContext]
    val config = loggerContext.getConfiguration
    val consoleAppender: ConsoleAppender = config.getAppender("Console")
    val rootConfig = config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME)
    val fileAppender: FileAppender = config.getAppender("File")
    assertNotNull(consoleAppender)
    assertNotNull(fileAppender)
    assertNotNull(rootConfig)
    assertTrue(consoleAppender.isStarted)
    assertTrue(fileAppender.isStarted)
    assertEquals(ExpectedLogger.getLevel, rootConfig.getLevel)
    assertEquals(ExpectedLogger.getFileName, fileAppender.getFileName)
    assertEquals(ExpectedLogger.getConsoleLayout, consoleAppender.getLayout.toString)
    assertEquals(ExpectedLogger.getFileLayout, fileAppender.getLayout.toString)
  }

  //测试能否正确生成一条log（格式）
  @Test
  def testLoggerMessageCorrect(): Unit = {
    ConfigLoader.init(getResourcePath(""))
    val logger: Logger = LogManager.getLogger(getClass)
    logger.info(expectedMessage)
    val logLine = extractTargetLog(filePath)

    regex.findFirstMatchIn(logLine) match {
      case Some(m) =>
        assertTrue(validateTimestamp(m.group("timestamp")))
        assertTrue(validateLevel(m.group("level")))
        assertTrue(validateMessage(m.group("message")))
      case None => fail(s"No regex match for: $logLine")
    }

  }

  private def getResourcePath(resourceName: String): String = {
    val url = Option(getClass.getClassLoader.getResource(resourceName))
      .orElse(Option(getClass.getResource(resourceName)))
      .getOrElse(throw new RuntimeException(s"Resource not found: $resourceName"))
    url.getPath
  }

  private def extractTargetLog(filePath: Path): String = {
    Files.readAllLines(filePath).asScala
      .find(_.contains("Test message"))
      .getOrElse(fail("Target log entry not found"))
  }

  // 时间戳验证 (精确到秒)
  private def validateTimestamp(ts: String): Boolean = {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    try {
      LocalDateTime.parse(ts, formatter)
      true
    } catch {
      case _: Exception =>
        false
    }
  }

  // 日志级别验证
  private def validateLevel(level: String): Boolean = ExpectedLogger.getLevel.toString == level


  // 消息内容验证
  private def validateMessage(msg: String): Boolean = msg == expectedMessage

}

object ExpectedLogger {
  private val confPath = new File(getResourcePath("faird.conf")).toPath
  private val expectedConfig: Map[String, String] = Files.readAllLines(confPath).asScala
    .filter(_.contains("=")) // 过滤有效行
    .map(_.split("=", 2)) // 按第一个=分割
    .map(arr => arr(0).trim -> arr(1).trim) // 转换为键值对
    .toMap

  def getLevel: Level = Level.toLevel(expectedConfig("logging.level.root"))

  def getFileName: String = expectedConfig("logging.file.name")

  def getConsoleLayout: String = expectedConfig("logging.pattern.console")

  def getFileLayout: String = expectedConfig("logging.pattern.file")

}

