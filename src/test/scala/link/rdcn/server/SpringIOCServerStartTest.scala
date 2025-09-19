package link.rdcn.server

import link.rdcn.{ConfigLoader, FairdConfig, struct}
import link.rdcn.client.dacp.DacpClient
import link.rdcn.provider.{DataFrameDocument, DataFrameStatistics, DataProvider}
import link.rdcn.struct.ValueType.{LongType, StringType}
import link.rdcn.struct.{DataFrame, DataStreamSource, Row, StructType}
import link.rdcn.user.{AuthProvider, AuthenticatedUser, Credentials, DataOperationType, UsernamePassword}
import link.rdcn.util.ClosableIterator
import link.rdcn.received.DataReceiver
import link.rdcn.server.dacp.DacpServer
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
  /** Called once before receiving any rows */
  override def start(): Unit = ???

  /** Called for each received batch of rows */
  override def receiveRow(dataFrame: DataFrame): Unit = ???

  /** Called after all batches are received successfully */
  override def finish(): Unit = ???
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
}

case class TokenAuthenticatedUser(token: String) extends AuthenticatedUser

class AuthorProviderTest extends AuthProvider {
  /**
   * 用户认证，成功返回认证后的保持用户登录状态的凭证
   *
   * @throws AuthorizationException
   */
  override def authenticate(credentials: Credentials): AuthenticatedUser = {
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
  override def checkPermission(user: AuthenticatedUser, dataFrameName: String, opList: List[DataOperationType]): Boolean = {
    user match {
      case a: TokenAuthenticatedUser => if (a.token == "1") true else false
      case _ => false
    }
  }
}
