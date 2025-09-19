/**
 * @Author Yomi
 * @Description:
 * @Data 2025/7/30 17:20
 * @Modified By:
 */
package link.rdcn.optree

import link.rdcn.ConfigLoader
import link.rdcn.TestBase.getResourcePath
import link.rdcn.optree.RepositoryClientTest.{operatorClient, operatorDir}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.{AfterAll, Test}

import java.io.File
import java.nio.file.Paths
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

object RepositoryClientTest {
  //  implicit val system: ActorSystem = ActorSystem("HttpClient")
  val operatorClient = new RepositoryClient("10.0.89.38", 8088)
  val operatorDir = Paths.get(getClass.getClassLoader.getResource("").toURI).toString

  @AfterAll
  def close(): Unit = {
    //    Await.result(system.terminate(), 10.seconds)
  }
}

class RepositoryClientTest {


  @Test
  def uploadPackageTest(): Unit = {
    ConfigLoader.init(getResourcePath(""))
    val jarPath = Paths.get(ConfigLoader.fairdConfig.fairdHome, "lib", "java", "faird-plugin-impl-1.0-20250707.jar").toString
    val functionId = "aaa.bbb.id2"
    val responseBody = operatorClient.uploadPackage(jarPath, functionId, "JAVA_JAR", "Java Application", "main")
    assertTrue(Await.result(responseBody, 30.seconds).contains("success"), "Upload failed")
  }

  @Test
  def getOperatorInfoTest(): Unit = {
    ConfigLoader.init(getResourcePath(""))
    val functionId = "my-java-app-2"
    val downloadFuture = operatorClient.getOperatorInfo(functionId)
    // 阻塞等待 Future 完成
    val jsonInfo = Await.result(downloadFuture, 30.seconds)
    assertEquals("my-java-app-2", jsonInfo.getString("id"))
    assertEquals("faird-plugin-1.0-20250707.jar", jsonInfo.getString("fileName"))
    assertEquals("jar", jsonInfo.getString("type"))
    assertEquals("Java Application", jsonInfo.getString("desc"))
    assertEquals("main", jsonInfo.getString("functionName"))
  }

  @Test
  def downloadPackageTest(): Unit = {
    ConfigLoader.init(getResourcePath(""))
    val functionId = "my-java-app-2"
    val downloadFuture = operatorClient.downloadPackage(functionId, operatorDir)
    // 阻塞等待 Future 完成
    Await.result(downloadFuture, 30.seconds)
    val downloadedFile = new File(Paths.get(operatorDir, functionId + ".jar").toString)
    assertTrue(downloadedFile.exists(), s"File $downloadedFile not exist")
    assertTrue(downloadedFile.length() > 0, "Empty File!")
  }

}
