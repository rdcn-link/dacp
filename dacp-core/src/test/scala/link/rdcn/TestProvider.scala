/**
 * @Author Yomi
 * @Description:
 * @Data 2025/7/29 17:30
 * @Modified By:
 */
package link.rdcn

import link.rdcn.TestBase._
import link.rdcn.dacp.ConfigKeys._
import link.rdcn.dacp.client.DacpClient
import link.rdcn.dacp.receiver.DataReceiver
import link.rdcn.dacp.server.DacpServer
import link.rdcn.dacp.user.{AuthProvider, DataOperationType}
import link.rdcn.server.BlobRegistry
import link.rdcn.struct.ValueType.{DoubleType, LongType}
import link.rdcn.struct.{DataFrame, StructType}
import link.rdcn.user.{UserPrincipal, Credentials, UsernamePassword}
import link.rdcn.util.DataUtils
import org.junit.jupiter.api.{AfterAll, BeforeAll}

import java.io.File
import java.nio.file.Paths

trait TestProvider {

}

object TestProvider {
  ConfigLoader.init(Paths.get(getResourcePath("")).toString)

  val prefix = "dacp://" + ConfigLoader.fairdConfig.hostPosition + ":" + ConfigLoader.fairdConfig.hostPort
  val permissions = Map(
    adminUsername -> Set("/csv/data_1.csv", "/bin",
      "/csv/data_2.csv", "/csv/data_1.csv", "/csv/invalid.csv", "/excel/data.xlsx")
//      .map(path => prefix + path)
  )

  val baseDir = getOutputDir("test_output")
  // 生成的临时目录结构
  val binDir = Paths.get(baseDir, "bin").toString
  val csvDir = Paths.get(baseDir, "csv").toString

  //必须在DfInfos前执行一次
  TestDataGenerator.generateTestData(binDir, csvDir, baseDir)

  //根据文件生成元信息
  lazy val csvDfInfos = listFiles(csvDir).map(file => {
    DataFrameInfo(Paths.get("/csv").resolve(file.getName).toString.replace("\\", "/"), Paths.get(file.getAbsolutePath).toUri, CSVSource(",", true), StructType.empty.add("id", LongType).add("value", DoubleType))
  })
  lazy val binDfInfos = Seq(
    DataFrameInfo(Paths.get("/").resolve(Paths.get(binDir).getFileName).toString.replace("\\", "/"), Paths.get(binDir).toUri, DirectorySource(false), StructType.binaryStructType))

  val dataSetCsv = DataSet("csv", "1", csvDfInfos.toList)
  val dataSetBin = DataSet("bin", "2", binDfInfos.toList)

  class TestAuthenticatedUser(userName: String, token: String) extends UserPrincipal {
    def getUserName: String = userName
  }

  val authProvider = new AuthProvider {

    override def authenticate(credentials: Credentials): UserPrincipal = {
      if (credentials.isInstanceOf[UsernamePassword]) {
        val usernamePassword = credentials.asInstanceOf[UsernamePassword]
        if (usernamePassword.username == null && usernamePassword.password == null) {
          sendErrorWithFlightStatus(404,"User not found!")
        }
        else if (usernamePassword.username == adminUsername && usernamePassword.password == adminPassword) {
          new TestAuthenticatedUser(adminUsername, genToken())
        } else if (usernamePassword.username == userUsername && usernamePassword.password == userPassword) {
          new TestAuthenticatedUser(adminUsername, genToken())
        }
        else if (usernamePassword.username != "admin") {
          sendErrorWithFlightStatus(403,"User unauthorized!")
        } else {
          sendErrorWithFlightStatus(0,"User authenticate unknown error!")
        }
      } else if (credentials == Credentials.ANONYMOUS) {
        new TestAuthenticatedUser(anonymousUsername, genToken())
      }
      else {
        sendErrorWithFlightStatus(400,"Invalid credentials!")
      }
    }

    override def checkPermission(user: UserPrincipal, dataFrameName: String, opList: List[DataOperationType]): Boolean = {
      val userName = user.asInstanceOf[TestAuthenticatedUser].getUserName
      if (userName == anonymousUsername)
        sendErrorWithFlightStatus(403,"User not logged in!")
      permissions.get(userName) match { // 用 get 避免 NoSuchElementException
        case Some(allowedFiles) => allowedFiles.contains(dataFrameName)
        case None => false // 用户不存在或没有权限
      }
    }
  }
  val dataProvider: DataProviderImpl = new DataProviderImpl() {
    override val dataSetsScalaList: List[DataSet] = List(dataSetCsv, dataSetBin)
    override val dataFramePaths: (String => String) = (relativePath: String) => {
      Paths.get(getOutputDir("test_output"), relativePath).toString
    }
  }
  val dataReceiver: DataReceiver = new DataReceiver {

    override def receive(dataFrame: DataFrame): Unit = ???

  }

  private var fairdServer: Option[DacpServer] = None
  var dc: DacpClient = _
  val configCache = ConfigLoader.fairdConfig
  var expectedHostInfo: Map[String, String] = _

  @BeforeAll
  def startServer(): Unit = {
    TestDataGenerator.generateTestData(binDir, csvDir, baseDir)
    getServer
    connectClient

  }

  @AfterAll
  def stop(): Unit = {
    stopServer()
    BlobRegistry.cleanUp()
    TestDataGenerator.cleanupTestData(baseDir)
  }

  def getServer: DacpServer = synchronized {
    if (fairdServer.isEmpty) {
      ConfigLoader.init(Paths.get(getResourcePath("")).toString)
      val s = new DacpServer(dataProvider, dataReceiver, authProvider)
      s.start(ConfigLoader.fairdConfig)
      //      println(s"Server (Location): Listening on port ${s.getPort}")
      fairdServer = Some(s)
      expectedHostInfo =
        Map(
          FAIRD_HOST_NAME -> ConfigLoader.fairdConfig.hostName,
          FAIRD_HOST_PORT -> ConfigLoader.fairdConfig.hostPort.toString,
          FAIRD_HOST_TITLE -> ConfigLoader.fairdConfig.hostTitle,
          FAIRD_HOST_POSITION -> ConfigLoader.fairdConfig.hostPosition,
          FAIRD_HOST_DOMAIN -> ConfigLoader.fairdConfig.hostDomain,
          FAIRD_TLS_ENABLED -> ConfigLoader.fairdConfig.useTLS.toString,
          FAIRD_TLS_CERT_PATH -> ConfigLoader.fairdConfig.certPath,
          FAIRD_TLS_KEY_PATH -> ConfigLoader.fairdConfig.keyPath,
          LOGGING_FILE_NAME -> ConfigLoader.fairdConfig.loggingFileName,
          LOGGING_LEVEL_ROOT -> ConfigLoader.fairdConfig.loggingLevelRoot,
          LOGGING_PATTERN_FILE -> ConfigLoader.fairdConfig.loggingPatternFile,
          LOGGING_PATTERN_CONSOLE -> ConfigLoader.fairdConfig.loggingPatternConsole

        )

    }
    fairdServer.get
  }

  def connectClient: Unit = synchronized {
    dc = DacpClient.connect("dacp://localhost:3101", UsernamePassword(adminUsername, adminPassword))
  }

  def stopServer(): Unit = synchronized {
    fairdServer.foreach(_.close())
    fairdServer = None
  }

  def getExpectedDataFrameSize(dataFrameName: String): Long = {
    dataProvider.dataSetsScalaList.foreach(ds => {
      val dfInfo = ds.getDataFrameInfo(dataFrameName)
      if (dfInfo.nonEmpty) return DataUtils.countLinesFast(new File(dfInfo.get.path))
    })
    -1L
  }

}
