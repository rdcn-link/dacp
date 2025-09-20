package link.rdcn

import link.rdcn.TestBase._
import link.rdcn.dacp.user.{AuthProvider, DataOperationType}
import link.rdcn.struct.ValueType.{DoubleType, IntType, LongType}
import link.rdcn.struct._
import link.rdcn.user._

import java.io.File
import java.nio.file.Paths

/**
 * @Author Yomi
 * @Description:
 * @Data 2025/7/16 14:19
 * @Modified By:
 */

//用于Demo的Provider
class TestDemoProvider(baseDirString: String = demoBaseDir, subDirString: String = "data") {
  ConfigLoader.init(Paths.get(getResourcePath("")).toString)

  val prefix = "dacp://" + ConfigLoader.fairdConfig.hostPosition + ":" + ConfigLoader.fairdConfig.hostPort
  val permissions = Map(
    adminUsername -> Set("/listDataFrames/bin","/listDataFrames/csv","/listHostInfo","/listDataSets","/csv/data_1.csv", "/bin",
      "/csv/data_2.csv", "/csv/data_1.csv", "/csv/invalid.csv", "/excel/data.xlsx").map(path => prefix + path)
  )



  val baseDir = getOutputDir(baseDirString, subDirString)
  // 生成的临时目录结构
  val binDir = getOutputDir(baseDirString, Seq(subDirString, "bin").mkString(File.separator))
  val csvDir = getOutputDir(baseDirString, Seq(subDirString, "csv").mkString(File.separator))
  val excelDir = getOutputDir(baseDirString, Seq(subDirString, "excel").mkString(File.separator))

  //根据文件生成元信息
  lazy val csvDfInfos = listFiles(csvDir).map(file => {
    DataFrameInfo(Paths.get("/csv").resolve(file.getName).toString.replace("\\", "/"), Paths.get(file.getAbsolutePath).toUri, CSVSource(",", true), StructType.empty.add("id", LongType).add("value", DoubleType))
  })
  lazy val binDfInfos = Seq(
    DataFrameInfo(Paths.get("/").resolve(Paths.get(binDir).getFileName).toString.replace("\\", "/"), Paths.get(binDir).toUri, DirectorySource(false), StructType.binaryStructType))
  lazy val excelDfInfos = listFiles(excelDir).map(file => {
    DataFrameInfo(Paths.get("/excel").resolve(file.getName).toString.replace("\\", "/"), Paths.get(file.getAbsolutePath).toUri, ExcelSource(), StructType.empty.add("id", IntType).add("value", IntType))
  })

  val dataSetCsv = DataSet("csv", "1", csvDfInfos.toList)
  val dataSetBin = DataSet("bin", "2", binDfInfos.toList)
  val dataSetExcel = DataSet("excel", "3", excelDfInfos.toList)

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
    override val dataSetsScalaList: List[DataSet] = List(dataSetCsv, dataSetBin, dataSetExcel)
    override val dataFramePaths: (String => String) = (relativePath: String) => {
      Paths.get(baseDir, relativePath).toString
    }

  }

  // 默认构造函数
  def this() = {
    this(demoBaseDir, "data") // 调用主构造函数
  }


}

