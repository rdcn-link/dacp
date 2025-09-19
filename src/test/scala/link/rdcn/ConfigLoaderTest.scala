package link.rdcn

import link.rdcn.ConfigLoaderTest.getResourcePath
import link.rdcn.TestEmptyProvider._
import org.apache.arrow.flight.Location
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.api.Test

import java.io.File
import java.nio.file.Files
import scala.collection.JavaConverters.asScalaBufferConverter


/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/17 13:39
 * @Modified By:
 */
object ConfigLoaderTest {
  def getResourcePath(resourceName: String): String = {
    val url = Option(getClass.getClassLoader.getResource(resourceName))
      .orElse(Option(getClass.getResource(resourceName)))
      .getOrElse(throw new RuntimeException(s"Resource not found: $resourceName"))
    url.getPath
  }
}

class ConfigLoaderTest {

  //是否加载配置文件
  @Test
  def initTest(): Unit = {
    val configPath = getResourcePath("")
    ConfigLoader.fairdConfig = configCache
    ConfigLoader.init(configPath)
    val config = ConfigBridge.getConfig

    assertEquals(ExpectedConfigLoader.getHostName, config.hostName)
    assertEquals(ExpectedConfigLoader.getHostTitle, config.hostTitle)
    assertEquals(ExpectedConfigLoader.getHostPort, config.hostPort)
    assertEquals(ExpectedConfigLoader.getHostDomain, config.hostDomain)
    assertEquals(ExpectedConfigLoader.getHostPosition, config.hostPosition)
  }


  //未加载Config
  @Test
  def testConfigNotInit(): Unit = {
    ConfigLoader.fairdConfig = null

    assertThrows(
      classOf[NullPointerException],
      () => Location.forGrpcInsecure(ConfigLoader.fairdConfig.hostPosition,
        ConfigLoader.fairdConfig.hostPort)
    )

  }

}

object ExpectedConfigLoader {

  private val confPath = new File(getResourcePath("faird.conf")).toPath
  private val expectedConfig: Map[String, String] = Files.readAllLines(confPath).asScala
    .filter(_.contains("=")) // 过滤有效行
    .map(_.split("=", 2)) // 按第一个=分割
    .map(arr => arr(0).trim -> arr(1).trim) // 转换为键值对
    .toMap

  def getHostName: String = expectedConfig("faird.host.name")

  def getHostTitle: String = expectedConfig("faird.host.title")

  def getHostPosition: String = expectedConfig("faird.host.position")

  def getHostDomain: String = expectedConfig("faird.host.domain")

  def getHostPort: Int = expectedConfig("faird.host.port").toInt

  def getCatdbPort: Int = expectedConfig("faird.catdb.port").toInt

}
