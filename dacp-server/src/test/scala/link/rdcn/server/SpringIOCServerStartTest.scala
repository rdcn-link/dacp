package link.rdcn.server

import link.rdcn.ConfigLoader
import link.rdcn.dacp.FairdConfig
import link.rdcn.dacp.client.DacpClient
import link.rdcn.dacp.provider.DataProvider
import link.rdcn.dacp.receiver.DataReceiver
import link.rdcn.dacp.server.DacpServer
import link.rdcn.dacp.user.{AuthProvider, DataOperationType}
import link.rdcn.dacp.struct.{DataFrameDocument, DataFrameStatistics}
import link.rdcn.struct.ValueType.StringType
import link.rdcn.struct.{ClosableIterator, DataFrame, DataStreamSource, Row, StructType}
import link.rdcn.user.{Credentials, UserPrincipal, UsernamePassword}
import org.apache.jena.rdf.model.Model
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.xml.XmlBeanFactory
import org.springframework.core.io.ClassPathResource

import java.util
import java.util.Arrays

class SpringIOCServerStartTest {
  @Test
  def serverStart(): Unit = {
    val f = new XmlBeanFactory(new ClassPathResource("faird.xml"))
    val dataReceiver = f.getBean("dataReceiver").asInstanceOf[DataReceiver]
    val dataProvider = f.getBean("dataProvider").asInstanceOf[DataProvider]
    val authProvider = f.getBean("authProvider").asInstanceOf[AuthProvider]
    val fairdHome = getClass.getClassLoader.getResource("").getPath

    val server: DacpServer = new DacpServer(dataProvider, dataReceiver, authProvider)

    ConfigLoader.init(fairdHome)
    server.start(ConfigLoader.fairdConfig)
    val client = DacpClient.connect("dacp://0.0.0.0:3101", Credentials.ANONYMOUS)
    assert(client.listDataSetNames().head == "dataSet1")
    assert(client.listDataFrameNames("dataSet1").head == "dataFrame1")
  }

  @Test
  def serverDstpTest(): Unit = {
    val dacpServer = new DacpServer(new DataProviderTest, new DataReceiverTest, new AuthorProviderTest)
    dacpServer.start(new FairdConfig)

    val dacpClient = DacpClient.connect("dacp://0.0.0.0:3101", Credentials.ANONYMOUS)
    val dfDataSets = dacpClient.get("dacp://0.0.0.0:3101/listDataSetNames")
    println("#########DataSet List")
    dfDataSets.foreach(println)
    val dfNames = dacpClient.get("dacp://0.0.0.0:3101/listDataFrameNames/csv")
    println("#########DataFrame List")
    dfNames.foreach(println)
    val df = dacpClient.get("dacp://0.0.0.0:3101/get/csv")
    println("###########println DataFrame")
    val s: StructType = df.schema
    df.foreach(println)

  }

}

class DataReceiverTest extends DataReceiver {
  override def receive(dataFrame: DataFrame): Unit = ???
}

class DataProviderTest extends DataProvider {

  /**
   * 列出所有数据集名称
   *
   * @return java.util.List[String]
   */
  override def listDataSetNames(): util.List[String] = {
    Arrays.asList("dataSet1")
  }

  /**
   * 获取数据集的 RDF 元数据，填充到传入的 rdfModel 中
   *
   * @param dataSetId 数据集 ID
   * @param rdfModel  RDF 模型（由调用者传入，方法将其填充）
   */
  override def getDataSetMetaData(dataSetId: String, rdfModel: Model): Unit = {
  }

  /**
   * 列出指定数据集下的所有数据帧名称
   *
   * @param dataSetId 数据集 ID
   * @return java.util.List[String]
   */
  override def listDataFrameNames(dataSetId: String): util.List[String] = Arrays.asList("dataFrame1")

  /**
   * 获取数据帧的数据流
   *
   * @param dataFrameName 数据帧名（如 mnt/a.csv)
   * @return 数据流源
   */
  override def getDataStreamSource(dataFrameName: String): DataStreamSource = new DataStreamSource {
    override def rowCount: Long = -1

    override def schema: StructType = StructType.empty.add("col1", StringType)

    override def iterator: ClosableIterator[Row] = {
      val rows = Seq.range(0, 10).map(index => Row.fromSeq(Seq("id" + index))).toIterator
      ClosableIterator(rows)()
    }
  }

  /**
   * 获取数据帧详细信息
   *
   * @param dataFrameName 数据帧名
   * @return 数据帧的DataFrameDocument
   */
  override def getDocument(dataFrameName: String): DataFrameDocument = ???

  /** *
   * 获取数据帧统计信息
   *
   * @param dataFrameName 数据帧名
   * @return 数据帧的DataFrameStatistics
   */
  override def getStatistics(dataFrameName: String): DataFrameStatistics = ???

  /**
   * 获取数据集的 RDF 元数据，填充到传入的 rdfModel 中
   *
   * @param dataFrameName 数据帧名（如 /mnt/a.csv)
   * @param rdfModel      RDF 模型（由调用者传入，方法将其填充）
   */
  override def getDataFrameMetaData(dataFrameName: String, rdfModel: Model): Unit = {
  }
}

case class TokenAuthenticatedUser(token: String) extends UserPrincipal

class AuthorProviderTest extends AuthProvider {
  /**
   * 用户认证，成功返回认证后的保持用户登录状态的凭证
   *
   * @throws AuthorizationException
   */
  override def authenticate(credentials: Credentials): UserPrincipal = {
    val token: String = {
      credentials match {
        case UsernamePassword("Admin", "Ano") => "1"
        case _ => "2"
      }
    }
    TokenAuthenticatedUser(token)
  }

  /**
   * 判断用户是否具有某项权限
   *
   * @param user          已认证用户
   * @param dataFrameName 数据帧名称
   * @param opList        操作类型列表（Java List）
   * @return 是否有权限
   */
  override def checkPermission(user: UserPrincipal, dataFrameName: String, opList: List[DataOperationType]): Boolean = {
    user match {
      case a: TokenAuthenticatedUser => if (a.token == "1") true else false
      case _ => false
    }
  }
}
