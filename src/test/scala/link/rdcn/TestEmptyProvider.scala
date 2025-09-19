package link.rdcn

import link.rdcn.TestBase.{getOutputDir, getResourcePath}
import link.rdcn.received.DataReceiver
import link.rdcn.user.{AuthProvider, AuthenticatedUser, Credentials, DataOperationType}
import link.rdcn.struct.{DataFrame, DataStreamSource}
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
    override def authenticate(credentials: Credentials): AuthenticatedUser = {
      null
    }

    /**
     * 判断用户是否具有某项权限
     */
    override def checkPermission(user: AuthenticatedUser, dataFrameName: String, opList: List[DataOperationType]): Boolean = true
  }

  val emptyDataProvider: DataProviderImpl = new DataProviderImpl() {
    override val dataSetsScalaList: List[DataSet] = List.empty
    override val dataFramePaths: (String => String) = (relativePath: String) => {
      null
    }

    override def getDataStreamSource(dataFrameName: String): DataStreamSource = ???
  }

  val emptyDataReceiver: DataReceiver = new DataReceiver {
    /** Called once before receiving any rows */
    override def start(): Unit = ???

    /** Called for each received batch of rows */
    override def receiveRow(dataFrame: DataFrame): Unit = ???

    /** Called after all batches are received successfully */
    override def finish(): Unit = ???
  }

  val configCache = ConfigLoader.fairdConfig

  class TestAuthenticatedUser(userName: String, token: String) extends AuthenticatedUser {
    def getUserName: String = userName
  }


}
