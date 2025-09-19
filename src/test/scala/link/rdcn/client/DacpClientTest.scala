package link.rdcn.client

import link.rdcn.TestBase.genModel
import link.rdcn.TestProvider
import link.rdcn.TestProvider._
import org.apache.jena.rdf.model.Model
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/16 18:08
 * @Modified By:
 */


class DacpClientTest extends TestProvider {
  val csvModel: Model = genModel
  val binModel: Model = genModel

  @Test
  def testListDataSetNames(): Unit = {
    assertEquals(dataProvider.listDataSetNames().toSet, dc.listDataSetNames().toSet, "ListDataSetNames接口输出与预期不符！")
  }

  @Test
  def testListDataFrameNames(): Unit = {
    assertEquals(dataProvider.listDataFrameNames("csv").toSet, dc.listDataFrameNames("csv").toSet, "ListDataFrameNames接口读取csv文件输出与预期不符！")
    assertEquals(dataProvider.listDataFrameNames("bin").toSet, dc.listDataFrameNames("bin").toSet, "ListDataFrameNames接口读取二进制文件输出与预期不符！")

  }

  @Test
  def testGetDataSetMetaData(): Unit = {
    //注入元数据
    dataProvider.getDataSetMetaData("csv", csvModel)
    assertTrue(csvModel.isIsomorphicWith(dc.getDataSetMetaData("csv")), "GetDataSetMetaData接口读取csv文件输出与预期不符！")
    dataProvider.getDataSetMetaData("bin", binModel)
    assertTrue(binModel.isIsomorphicWith(dc.getDataSetMetaData("bin")), "GetDataSetMetaData接口读取二进制文件输出与预期不符！")
  }

  @Test
  def testSchema(): Unit = {
    //注入元数据
    dataProvider.getDataSetMetaData("csv", csvModel)
    assertTrue(csvModel.isIsomorphicWith(dc.getDataSetMetaData("csv")), "GetDataSetMetaData接口读取csv文件输出与预期不符！")
    dataProvider.getDataSetMetaData("bin", binModel)
    assertTrue(binModel.isIsomorphicWith(dc.getDataSetMetaData("bin")), "GetDataSetMetaData接口读取二进制文件输出与预期不符！")
  }

  @Test
  def testGetHostInfo(): Unit = {
    val allKeys: Set[String] = Set(
      FAIRD_HOST_DOMAIN,
      FAIRD_HOST_TITLE,
      FAIRD_HOST_NAME,
      FAIRD_HOST_PORT,
      FAIRD_HOST_POSITION,
      LOGGING_FILE_NAME,
      LOGGING_LEVEL_ROOT,
      LOGGING_PATTERN_FILE,
      LOGGING_PATTERN_CONSOLE,
      FAIRD_TLS_ENABLED,
      FAIRD_TLS_CERT_PATH,
      FAIRD_TLS_KEY_PATH
    )
    val hostInfo = dc.getHostInfo
    allKeys.foreach(key => {
      assertTrue(hostInfo.contains(key), s"实际结果中缺少键：$key")
      assertEquals(expectedHostInfo(key), hostInfo(key), s"键 '$key' 的值与预期不符！")
    }
    )
  }

  @ParameterizedTest
  @ValueSource(strings = Array("/csv/data_1.csv"))
  def testGetDataFrameSize(dataFrameName: String): Unit = {
    assertEquals(getExpectedDataFrameSize(dataFrameName), dc.getDataFrameSize(dataFrameName), "GetDataFrameSize接口输出与预期不符！")
  }

  @Test
  def testGetServerResourceInfo(): Unit = {
    val expectedResourceInfo = dc.getServerResourceInfo

    val allKeys: Set[String] = Set(
      CPU_CORES,
      CPU_USAGE_PERCENT,
      JVM_TOTAL_MEMORY_MB,
      JVM_FREE_MEMORY_MB,
      JVM_USED_MEMORY_MB,
      JVM_MAX_MEMORY_MB,
      SYSTEM_MEMORY_USED_MB,
      SYSTEM_MEMORY_FREE_MB,
      SYSTEM_MEMORY_TOTAL_MB
    )
    val serverResouceInfo = dc.getServerResourceInfo
    allKeys.foreach(key => {
      assertTrue(serverResouceInfo.contains(key), s"实际结果中缺少键：$key")
    }
    )
  }
}
