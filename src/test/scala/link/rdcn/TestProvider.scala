/**
 * @Author Yomi
 * @Description:
 * @Data 2025/7/29 17:30
 * @Modified By:
 */
package link.rdcn

import link.rdcn.ErrorCode.{INVALID_CREDENTIALS, USER_NOT_FOUND, USER_NOT_LOGGED_IN}
import link.rdcn.TestBase._
import link.rdcn.ConfigKeys._
import link.rdcn.client.dacp.DacpClient
import link.rdcn.received.DataReceiver
import link.rdcn.server.dacp.DacpServer
import link.rdcn.server.dftp.BlobRegistry
import link.rdcn.server.exception.AuthorizationException
import link.rdcn.struct.{DataFrame, StructType}
import link.rdcn.struct.ValueType.{DoubleType, LongType}
import link.rdcn.user.{AuthProvider, AuthenticatedUser, Credentials, DataOperationType, UsernamePassword}
import link.rdcn.util.DataUtils
import link.rdcn.util.DataUtils.listFiles
import org.junit.jupiter.api.{AfterAll, BeforeAll}

import java.io.File
import java.nio.file.Paths

trait TestProvider {

}

object TestProvider {
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

  class TestAuthenticatedUser(userName: String, token: String) extends AuthenticatedUser {
    def getUserName: String = userName
  }

  val authprovider = new AuthProvider {

    override def authenticate(credentials: Credentials): AuthenticatedUser = {
      if (credentials.isInstanceOf[UsernamePassword]) {
        val usernamePassword = credentials.asInstanceOf[UsernamePassword]
        if (usernamePassword.username == null && usernamePassword.password == null) {
          throw new AuthorizationException(USER_NOT_FOUND)
        }
        else if (usernamePassword.username == adminUsername && usernamePassword.password == adminPassword) {
          new TestAuthenticatedUser(adminUsername, genToken())
        } else if (usernamePassword.username == userUsername && usernamePassword.password == userPassword) {
          new TestAuthenticatedUser(adminUsername, genToken())
        }
        else if (usernamePassword.username == anonymousUsername) {
          new TestAuthenticatedUser(anonymousUsername, genToken())
        }
        else if (usernamePassword.username != adminUsername) {
          throw new AuthorizationException(USER_NOT_FOUND)
        } else {
          throw new AuthorizationException(INVALID_CREDENTIALS)
        }
      } else if (credentials == Credentials.ANONYMOUS) {
        new TestAuthenticatedUser(anonymousUsername, genToken())
      }
      else {
        throw new AuthorizationException(INVALID_CREDENTIALS)
      }
    }

    override def checkPermission(user: AuthenticatedUser, dataFrameName: String, opList: List[DataOperationType]): Boolean = {
      val userName = user.asInstanceOf[TestAuthenticatedUser].getUserName
      if (userName == anonymousUsername)
        throw new AuthorizationException(USER_NOT_LOGGED_IN)
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
    /** Called once before receiving any rows */
    override def start(): Unit = ???

    /** Called for each received batch of rows */
    override def receiveRow(dataFrame: DataFrame): Unit = ???

    /** Called after all batches are received successfully */
    override def finish(): Unit = ???
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
      val s = new DacpServer(dataProvider, dataReceiver, authprovider)
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
    dc = DacpClient.connect("dacp://0.0.0.0:3101", UsernamePassword(adminUsername, adminPassword))
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
