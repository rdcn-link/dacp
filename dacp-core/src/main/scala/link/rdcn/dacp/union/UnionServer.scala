package link.rdcn.dacp.union

import link.rdcn.client.UrlValidator
import link.rdcn.dacp.client.DacpClient
import link.rdcn.dacp.optree.RemoteSourceProxyOp
import link.rdcn.dacp.receiver.DataReceiver
import link.rdcn.dacp.server.{CookRequest, CookResponse, DacpServer, KeyBasedAuthUtils}
import link.rdcn.dacp.user.{AuthProvider, DataOperationType, KeyAuthProvider, KeyCredentials}
import link.rdcn.operation.{SourceOp, TransformOp}
import link.rdcn.provider.{DataFrameDocument, DataFrameStatistics, DataProvider}
import link.rdcn.server.{GetRequest, GetResponse}
import link.rdcn.struct.{DFRef, DataFrame, DataStreamSource, DefaultDataFrame, Row}
import link.rdcn.user.{Credentials, UserPrincipal}
import link.rdcn.util.CodecUtils
import org.apache.jena.rdf.model.Model
import org.json.JSONObject

import java.util
import java.time.format.DateTimeFormatter
import java.time.{ZoneId, ZonedDateTime}
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable

/**
 * @Author renhao
 * @Description:
 * @Date 2025/8/20 13:57
 * @Modified By:
 */
class UnionServer(dataProvider: DataProvider, dataReceiver: DataReceiver, authProvider: AuthProvider) extends DacpServer(dataProvider, dataReceiver, authProvider) {

  private val endpoints = mutable.ListBuffer[Endpoint]()

  private val endpointClientsMap = mutable.Map[String, DacpClient]()
  private val endpointMap = mutable.Map[String, Endpoint]()

  private val authProviderWithKey = KeyAuthProvider(authProvider)

  private val formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
  private val recipeCounter = new AtomicLong(0L)

  def addEndpoint(endpoints: Endpoint*): Unit = {
    endpoints.foreach(endpoint => {
      this.endpointClientsMap.put(endpoint.url, endpoint.getDacpClient(createSignature(-1)))
      this.endpoints.append(endpoint)
      this.endpointMap.put(endpoint.url, endpoint)
    })
  }

  override def doListDataSets(): DataFrame = {
    val dataSets = endpointClientsMap.toList.map(kv => kv._2.get(s"${kv._1}/listDataSets"))
    mergeDataFrame(dataSets)
  }

  override def doListDataFrames(listDataFrameUrl: String): DataFrame = {
    val dataFrames: mutable.Iterable[DataFrame] = endpointClientsMap.map(kv => (kv._2, kv._2.get(s"${kv._1}/listDataSets")))
      .map(kv => {
        mergeDataFrame(kv._2.mapIterator[Seq[DataFrame]](rows => rows.map(row => kv._1.get(row.getAs[DFRef](3).url)).toSeq).toList)
      })
    mergeDataFrame(dataFrames.toList)
  }

  override def doListHostInfo(): DataFrame = {
    val dataFrames = endpointClientsMap.toList.map(kv => kv._2.get(s"${kv._1}/listHostInfo"))
    mergeDataFrame(dataFrames)
  }

  override def doCook(request: CookRequest, response: CookResponse): Unit = {
    val transformTree = request.getTransformTree
    val userPrincipal = request.getRequestUserPrincipal()

    transformTree match
    {
      case s: SourceOp =>
        val baseUrlAndPath: (String, String) = UrlValidator.extractBaseUrlAndPath(s.dataFrameUrl) match {
          case Right((baseUrl, path)) => (baseUrl, path)
          case Left(message) => (this.getBaseUrl(), s.dataFrameUrl)
        }
        if (baseUrlAndPath._1 == this.getBaseUrl())
        {
          baseUrlAndPath._2 match {
            case "/listDataSets" => response.sendDataFrame(doListDataSets())
            case "/listHostInfo" => response.sendDataFrame(doListHostInfo())
            case "/listDataFrames" => response.sendDataFrame(doListDataFrames(s.dataFrameUrl))
            case other => response.sendError(404, s"not found resource ${this.getBaseUrl()}${other}")
          }
        }else
        {
          if(authProviderWithKey.checkPermission(userPrincipal, baseUrlAndPath._2)){
            try {
              val client = endpointClientsMap.getOrElse(baseUrlAndPath._1
                , throw new Exception(s"Access to FaridServer ${s.dataFrameUrl} is denied"))
              response.sendDataFrame(client.executeTransformTree(transformTree))
            } catch {
              case e: Exception =>
                logger.error(e)
                response.sendError(500, e.getMessage)
            }
          }else response.sendError(403, s"access ${s.dataFrameUrl} forbidden")
        }
      case other: TransformOp =>
        other.sourceUrlList.find(url => {
          !authProviderWithKey.checkPermission(userPrincipal, getUrlPath(url))
        })match {
          case Some(url) => response.sendError(403, s"access ${url} forbidden")
          case None => response.sendDataFrame(createRecipe(other).execute())
        }
    }
  }

  override def doGet(request: GetRequest, response: GetResponse): Unit = {
    if(Seq("/listDataSets", "/listHostInfo", "/listDataFrames").contains(request.getRequestURI())) super.doGet(request, response)
    else response.sendError(404, s"not found resource ${this.getBaseUrl()}${request.getRequestURI()}")
  }

  private def createSignature(expirationTime: Long): KeyCredentials = {
    val privateKey = getFairdConfig().privateKey
    if (privateKey.isEmpty) throw new Exception("Private key not found. Please configure private key information for this UnionServer.")
    else {
      val nonce = UUID.randomUUID().toString
      val issueTime = System.currentTimeMillis()
      val jo = new JSONObject().put("serverId", this.getBaseUrl())
        .put("nonce", nonce)
        .put("issueTime", issueTime)
        .put("validTo", issueTime + expirationTime)

      val signature = KeyBasedAuthUtils.signData(privateKey.get, CodecUtils.encodeString(jo.toString))
      KeyCredentials(this.getBaseUrl(), nonce, issueTime, issueTime + expirationTime, signature)
    }
  }

  private def rebuildOperation(transformOp: TransformOp, baseUrl: String): TransformOp = {
    val inputs = transformOp.inputs.map(operation => operation match {
      case s: SourceOp =>
        if (s.dataFrameUrl.startsWith(baseUrl)) s else {
          val certificate = createSignature(60L * 60 * 1000) //default expirationTime 1h
          RemoteSourceProxyOp(s.dataFrameUrl, certificate.toJson().toString)
        }
      case other: TransformOp => rebuildOperation(other, baseUrl)
    })
    transformOp.setInputs(inputs: _*)
  }

  private def getRecipeId(): String = {
    val timestamp = ZonedDateTime.now(ZoneId.of("UTC")).format(formatter)
    val seq = recipeCounter.incrementAndGet()
    s"job-${timestamp}-${seq}"
  }

  private case class Recipe(
                             jobId: String,
                             transformOp: TransformOp,
                             executionNodeBaseUrl: String,
                             attributes: Map[String, String] = Map()
                           )
  {
    def execute(): DataFrame = {
      val client = endpointClientsMap.getOrElse(executionNodeBaseUrl
        , throw new Exception(s"Access to FaridServer ${executionNodeBaseUrl} is denied"))
      client.executeTransformTree(transformOp)
    }

    //TODO kill Job
    def close(): Unit = {}
  }

  //  TODO 根据计算资源判定执行节点
  //可强制指定执行节点
  private def createRecipe(transformOp: TransformOp, executionNodeBaseUrl: Option[String] = None): Recipe = {
    if (executionNodeBaseUrl.isEmpty) {
      val baseUrl = transformOp.sourceUrlList.toList
        .map(UrlValidator.extractBase(_).get._1)
        .groupBy(identity)
        .mapValues(_.size)
        .maxBy(_._2)._1
      val reOperation = rebuildOperation(transformOp, baseUrl)
      Recipe(getRecipeId(), reOperation, baseUrl)
    } else {
      Recipe(getRecipeId(), rebuildOperation(transformOp, executionNodeBaseUrl.get), executionNodeBaseUrl.get)
    }
  }

  private def mergeDataFrame(dataFrames: List[DataFrame]): DataFrame = {
    val stream: Iterator[Row] = dataFrames.map(df => df.mapIterator[Iterator[Row]](iter => iter)).reduce(_ ++ _)
    val schema = dataFrames.head.schema
    DefaultDataFrame(schema, stream)
  }
}

object UnionServer {

  def apply(endpoints: List[Endpoint]): UnionServer = {
    val unionServer = new UnionServer(dataProvider, dataReceiver, authProvider)
    unionServer.addEndpoint(endpoints: _*)
    unionServer
  }

  def initCluster(urls: String*): UnionServer = {
    val endpoints = urls.map(url => {
      UrlValidator.validate(url) match {
        case Right(value) => Endpoint(value._2, value._3.getOrElse(3101))
        case Left(message) => throw new IllegalArgumentException(message)
      }
    }).toList
    UnionServer(endpoints)
  }

  private val authProvider = new AllowAllAuthProvider

  private val dataProvider = new DataProvider {
    override def listDataSetNames(): util.List[String] = ???

    override def getDataSetMetaData(dataSetId: String, rdfModel: Model): Unit = ???

    override def listDataFrameNames(dataSetId: String): util.List[String] = ???

    override def getDataStreamSource(dataFrameName: String): DataStreamSource = ???

    override def getDocument(dataFrameName: String): DataFrameDocument = ???

    override def getStatistics(dataFrameName: String): DataFrameStatistics = ???
  }
  private val dataReceiver = new DataReceiver {
    override def receive(dataFrame: DataFrame): Unit = ???
  }
}

case class Endpoint(
                     host: String,
                     port: Int
                   ) {
  // 解析Url获取的key
  def url: String = s"${DacpClient.protocolSchema}://${host}:${port}"

  def getDacpClient(credentials: Credentials): DacpClient = DacpClient.connect(url, credentials)

}

class AllowAllAuthProvider extends AuthProvider {

  override def authenticate(credentials: Credentials): UserPrincipal = new UserPrincipal {}

  override def checkPermission(user: UserPrincipal, dataFrameName: String, opList: List[DataOperationType]): Boolean = true
}


