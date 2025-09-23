/**
 * @Author Yomi
 * @Description:
 * @Data 2025/7/30 17:20
 * @Modified By:
 */
package link.rdcn.optree


import link.rdcn.ConfigLoader
import link.rdcn.TestBase.getResourcePath
import link.rdcn.dacp.optree.RepositoryClient
import link.rdcn.optree.RepositoryClientTest.{operatorClient, operatorDir}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

import java.io.File
import java.nio.file.Paths
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

object RepositoryClientTest {
  val operatorClient = new RepositoryClient("10.0.89.38", 8088)
  val operatorDir = Paths.get(getClass.getClassLoader.getResource("").toURI).toString
}

class RepositoryClientTest {
  ConfigLoader.init(getResourcePath(""))
  val jarFile: File = new File(Paths.get(ConfigLoader.fairdConfig.fairdHome,"lib","java").toString).listFiles().head
  val jarPath: String = jarFile.getAbsolutePath
  val jarName: String = jarFile.getName


  @Test
  def uploadPackageTest(): Unit = {
    val functionId = "aaa.bbb.id2"
    val responseBody = operatorClient.uploadPackage(jarPath, functionId, "JAVA_JAR", "Java Application", "Transformer11")
    assertTrue(Await.result(responseBody, 30.seconds).contains("success"), "Upload failed")
  }

  @Test
  def getOperatorInfoTest(): Unit = {
    ConfigLoader.init(getResourcePath(""))
    val functionId = "aaa.bbb.id2"
    val downloadFuture = operatorClient.getOperatorInfo(functionId)
    // 阻塞等待 Future 完成
    val jsonInfo = Await.result(downloadFuture, 30.seconds)
    assertEquals(functionId, jsonInfo.getString("id"))
    assertEquals(jarName, jsonInfo.getString("packageName"))
    assertEquals("JAVA_JAR", jsonInfo.getString("type"))
    assertEquals("Java Application", jsonInfo.getString("desc"))
    assertEquals("Transfomer11", jsonInfo.getString("functionName"))
  }

  @Test
  def downloadPackageTest(): Unit = {
    ConfigLoader.init(getResourcePath(""))
    val functionId = "aaa.bbb.id1"
    val downloadFuture = operatorClient.downloadPackage(functionId, operatorDir)
    // 阻塞等待 Future 完成
    Await.result(downloadFuture, 30.seconds)
    val downloadedFile = new File(Paths.get(operatorDir, "link-0.1-py3-none-any.whl").toString)
    assertTrue(downloadedFile.exists(), s"File $downloadedFile not exist")
    assertTrue(downloadedFile.length() > 0, "Empty File!")
  }


}
