package link.rdcn

import link.rdcn.TestBase.{getOutputDir, getResourcePath}
import link.rdcn.dacp.receiver.DataReceiver
import link.rdcn.dacp.user.{AuthProvider, DataOperationType}
import link.rdcn.user.{Credentials, UserPrincipal}
import link.rdcn.struct.{DataFrame, DataStreamSource, StructType}
import org.apache.arrow.flight.Location
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}

/**
 * @Author Yomi
 * @Description:
 * @Data 2025/7/16 13:34
 * @Modified By:
 */
trait TestEmptyProvider {

}

/** *
 * 用于不需要生成数据的测试的Provider
 */
object TestEmptyProvider {
  ConfigLoader.init(getResourcePath(""))

  val outputDir = getOutputDir("test_output", "output")

  val location = Location.forGrpcInsecure(ConfigLoader.fairdConfig.hostPosition, ConfigLoader.fairdConfig.hostPort)
  val allocator: BufferAllocator = new RootAllocator()
  val emptyAuthProvider = new AuthProvider {
    override def authenticate(credentials: Credentials): UserPrincipal = {
      null
    }

    /**
     * 判断用户是否具有某项权限
     */
    override def checkPermission(user: UserPrincipal, dataFrameName: String, opList: List[DataOperationType]): Boolean = true
  }

  val emptyDataProvider: DataProviderImpl = new DataProviderImpl() {
    override val dataSetsScalaList: List[DataSet] = List.empty
    override val dataFramePaths: (String => String) = (relativePath: String) => {
      null
    }

    override def getDataStreamSource(dataFrameName: String): DataStreamSource = ???

    override def getSchema(dataFrameName: String): StructType = ???
  }

  val emptyDataReceiver: DataReceiver = new DataReceiver {

    override def receive(dataFrame: DataFrame): Unit = ???

  }

  val configCache = ConfigLoader.fairdConfig

  class TestAuthenticatedUser(userName: String, token: String) extends UserPrincipal {
    def getUserName: String = userName
  }


}
