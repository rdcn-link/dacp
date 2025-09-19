package link.rdcn.dacp.server

import com.sun.management.OperatingSystemMXBean
import link.rdcn.dacp.ConfigKeys.{FAIRD_HOST_DOMAIN, FAIRD_HOST_NAME, FAIRD_HOST_PORT, FAIRD_HOST_POSITION, FAIRD_HOST_TITLE, FAIRD_TLS_CERT_PATH, FAIRD_TLS_ENABLED, FAIRD_TLS_KEY_PATH, LOGGING_FILE_NAME, LOGGING_LEVEL_ROOT, LOGGING_PATTERN_CONSOLE, LOGGING_PATTERN_FILE}
import link.rdcn.dacp.{ConfigKeys, FairdConfig}
import link.rdcn.dacp.optree.{FlowExecutionContext, OperationTree, OperatorRepository}
import link.rdcn.dacp.received.DataReceiver
import link.rdcn.dacp.user.{AuthProvider, DataOperationType, KeyAuthenticatedUser, SignatureAuth}
import link.rdcn.operation.{ExecutionContext, Operation}
import link.rdcn.{DftpConfig, Logging}
import link.rdcn.provider.DataProvider
import link.rdcn.server.{ActionRequest, ActionResponse, DftpServiceHandler, GetRequest, GetResponse, PutRequest, PutResponse}
import link.rdcn.server.dftp.DftpServer
import link.rdcn.struct.ValueType.{LongType, RefType, StringType}
import link.rdcn.struct.{DFRef, DataFrame, DataStreamSource, DefaultDataFrame, Row, StructType}
import link.rdcn.user.{AuthenticatedProvider, AuthenticatedUser, Credentials}
import link.rdcn.util.{CodecUtils, DataUtils}
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.json.{JSONArray, JSONObject}

import java.io.{File, FileInputStream, InputStreamReader, StringWriter}
import java.lang.management.ManagementFactory
import java.util.Properties
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters.asScalaBufferConverter

/**
 * @Author renhao
 * @Description:
 * @Data 2025/9/16 16:43
 * @Modified By:
 */
case class DacpServer(dataProvider: DataProvider, dataReceiver: DataReceiver, authProvider: AuthProvider)
  extends DftpServer with Logging{

  override def setProtocolSchema(protocolSchema: String): DftpServer = super.setProtocolSchema(protocolSchema)

  override def setAuthHandler(authenticatedProvider: AuthenticatedProvider): DftpServer =
    super.setAuthHandler(authProvider)

  override def setServiceHandler(dftpServiceHandler: DftpServiceHandler): DftpServer = {
    val dftpServiceHandler =  new DftpServiceHandler {
      override def doAction(request: ActionRequest, response: ActionResponse): Unit = response.sendError(501, s"${request.getActionName()} Not Implemented")

      override def doGet(request: GetRequest, response: GetResponse): Unit = DacpServer.this.doGet(request, response)

      override def doPut(request: PutRequest, response: PutResponse): Unit = {
        val dataFrame = request.getDataFrame()
        try {
          dataReceiver.start()
          dataReceiver.receiveRow(dataFrame)
          dataReceiver.finish()
          dataReceiver.close()
        } catch {
          case e: Exception => response.sendError(500, e.getMessage)
        }
        response.sendMessage(new JSONObject().put("status","success").toString())
      }
    }
    super.setServiceHandler(dftpServiceHandler)
  }

  override def buildStream(authenticatedUser: AuthenticatedUser, ticket: Array[Byte]): Either[DataFrame, (Int, String)] = {
    val ticketInfo = CodecUtils.decodeTicket(ticket)
    if(ticketInfo._1 == COOK_STREAM){
      var result: Either[DataFrame, (Int, String)] = null
      val request = new CookRequest {
        override def getOperation: String = ticketInfo._2

        override def getRequestAuthenticated(): AuthenticatedUser = authenticatedUser
      }
      val response = new CookResponse {
        override def sendDataFrame(dataFrame: DataFrame): Unit = result = Left(dataFrame)

        override def sendError(code: Int, message: String): Unit = result = Right(code, message)
      }
      doCook(request, response)
      result
    }else {
      super.buildStream(authenticatedUser, ticket)
    }
  }

  override def authenticate(credentials: Credentials): AuthenticatedUser = {
    credentials match {
      case sig: SignatureAuth =>
        KeyAuthenticatedUser(fairdConfig.pubKeyMap.get(sig.serverId), sig.serverId, sig.nonce, sig.issueTime, sig.validTo, sig.signature)
      case _ => authProvider.authenticate(credentials)
    }
  }

  override def start(dftpConfig: DftpConfig): Unit = {
    require(dftpConfig.isInstanceOf[FairdConfig])
    this.fairdConfig = dftpConfig.asInstanceOf[FairdConfig]
    super.start(dftpConfig)
  }

  def start(fairdHome: String): Unit = {
    val props = loadProperties(s"$fairdHome" + File.separator + "conf" + File.separator + "faird.conf")
    props.setProperty(ConfigKeys.FAIRD_HOME, fairdHome)
    fairdConfig = FairdConfig.load(props)
    start(fairdConfig)
  }

  def doCook(request: CookRequest, response: CookResponse): Unit = {
    val operationStr = request.getOperation
    val authenticatedUser = request.getRequestAuthenticated()
    val verifyResult = verifyOperationPermission(operationStr, authenticatedUser)
    if(verifyResult._1){
      try{
        response.sendDataFrame(verifyResult._2.execute(ctx))
      }catch {
        case e: Exception =>
          logger.error(e)
          response.sendError(500, e.getMessage)
      }
    }else{
      response.sendError(403, s"access dataFrame ${verifyResult._3.get} forbidden")
    }
  }

  def verifyOperationPermission(operationStr: String, authenticatedUser: AuthenticatedUser): (Boolean, Operation, Option[String]) = {
    val sourceList = new ListBuffer[String]
    val operation = OperationTree.fromJsonString(operationStr, sourceList)
    val keyPermission: Option[Boolean] = authenticatedUser match {
      case keyAuthenticatedUser: KeyAuthenticatedUser => Some(keyAuthenticatedUser.checkPermission())
      case _ => None
    }
    sourceList.find(dataFrameName => {
      if (keyPermission.nonEmpty) !keyPermission.get else
        !authProvider.checkPermission(authenticatedUser, dataFrameName, List.empty)
    }) match {
      case Some(dataFrame) => (false, operation, Some(dataFrame))
      case None => (true, operation, None)
    }
  }

  def doGet(request: GetRequest, response: GetResponse): Unit = {
    request.getRequestedPath() match {
      case "/listDataSets" =>
        try {
          response.sendDataFrame(doListDataSets())
        } catch {
          case e: Exception =>
            logger.error("Error while listDataSets", e)
            response.sendError(500, e.getMessage)
        }
      case path if path.startsWith("/listDataFrames") => {
        try {
          response.sendDataFrame(doListDataFrames(request.getRequestedPath()))
        } catch {
          case e: Exception =>
            logger.error("Error while listDataFrames", e)
            response.sendError(500, e.getMessage)
        }
      }
      case "/listHostInfo" => {
        try {
          response.sendDataFrame(doListHostInfo)
        } catch {
          case e: Exception =>
            logger.error("Error while listHostInfo", e)
            response.sendError(500, e.getMessage)
        }
      }
      case otherPath =>
        try {
          val authenticatedUser = request.getRequestAuthenticated()
          if(authProvider.checkPermission(authenticatedUser, otherPath)){
            val dataStreamSource: DataStreamSource = dataProvider.getDataStreamSource(otherPath)
            val dataFrame: DataFrame = DefaultDataFrame(dataStreamSource.schema, dataStreamSource.iterator)
            response.sendDataFrame(dataFrame)
          }else{
            response.sendError(403, s"access dataFrame $otherPath Forbidden")
          }
        } catch {
          case e: Exception =>
            logger.error(s"Error while get resource $otherPath", e)
            response.sendError(500, e.getMessage)
        }
    }
  }

  /**
   * 输入链接（实现链接）： dacp://0.0.0.0:3101/listDataSets
   * 返回链接： dacp://0.0.0.0:3101/listDataFrames/dataSetName
   * */
  def doListDataSets(): DataFrame = {
    val stream = dataProvider.listDataSetNames().asScala.map(dsName => {
      val model: Model = ModelFactory.createDefaultModel
      dataProvider.getDataSetMetaData(dsName, model)
      val writer = new StringWriter();
      model.write(writer, "RDF/XML");
      val dataSetInfo = new JSONObject().put("name", dsName).toString
      Row.fromTuple((dsName, writer.toString
        , dataSetInfo, DFRef(s"${baseUrl}/listDataFrames/$dsName")))
    }).toIterator
    val schema = StructType.empty.add("name", StringType)
      .add("meta", StringType).add("DataSetInfo", StringType).add("dataFrames", RefType)
    DefaultDataFrame(schema, stream)
  }

  /**
   * 输入链接（实现链接）： dacp://0.0.0.0:3101/listDataFrames/dataSetName
   * 返回链接： dacp://0.0.0.0:3101/dataFrameName
   * */
  def doListDataFrames(listDataFrameUrl: String): DataFrame = {
    val dataSetName = listDataFrameUrl.stripPrefix("/listDataFrames/")
    val schema = StructType.empty.add("name", StringType).add("size", LongType)
      .add("document", StringType).add("schema", StringType).add("statistics", StringType)
      .add("dataFrame", RefType)
    val stream: Iterator[Row] = dataProvider.listDataFrameNames(dataSetName).asScala
      .map(dfName => {
        (dfName, dataProvider.getDataStreamSource(dfName).rowCount
          , getDataFrameDocumentJsonString(dfName), getDataFrameSchemaString(dfName)
          , getDataFrameStatisticsString(dfName), DFRef(s"${baseUrl}/$dfName"))
      })
      .map(Row.fromTuple(_)).toIterator
    DefaultDataFrame(schema, stream)
  }

  /**
   * 输入链接(实现链接)： dacp://0.0.0.0:3101/getHost
   * */
  def doListHostInfo(): DataFrame = {
    val schema = StructType.empty.add("name", StringType).add("hostInfo", StringType).add("resourceInfo", StringType)
    val hostName = fairdConfig.hostName
    val stream = Seq((hostName, getHostInfoString(), getHostResourceString()))
      .map(Row.fromTuple(_)).toIterator
    DefaultDataFrame(schema, stream)
  }

  var baseUrl: String = s"$protocolSchema://${fairdConfig.hostPosition}:${fairdConfig.hostPort}"

  private def loadProperties(path: String): Properties = {
    val props = new Properties()
    val fis = new InputStreamReader(new FileInputStream(path), "UTF-8")
    try props.load(fis) finally fis.close()
    props
  }

  private def ctx = new FlowExecutionContext {
    override val pythonHome: String = ???
    override val fairdConfig: FairdConfig = fairdConfig

    override def loadSourceDataFrame(dataFrameNameUrl: String): Option[DataFrame] = {
      val resourcePath = if(dataFrameNameUrl.startsWith(baseUrl)) dataFrameNameUrl.stripPrefix(baseUrl)
      else dataFrameNameUrl
      try{
        val dataStreamSource: DataStreamSource = dataProvider.getDataStreamSource(resourcePath)
        val dataFrame: DataFrame = DefaultDataFrame(dataStreamSource.schema, dataStreamSource.iterator)
        Some(dataFrame)
      }catch {
        case e: Exception =>
          logger.error(e)
          None
      }
    }

    override def getRepositoryClient(): Option[OperatorRepository] = ???
  }

  private def getHostInfoString(): String = {
    val hostInfo = Map(s"$FAIRD_HOST_NAME" -> s"${fairdConfig.hostName}",
      s"$FAIRD_HOST_TITLE" -> s"${fairdConfig.hostTitle}",
      s"$FAIRD_HOST_POSITION" -> s"${fairdConfig.hostPosition}",
      s"$FAIRD_HOST_DOMAIN" -> s"${fairdConfig.hostDomain}",
      s"$FAIRD_HOST_PORT" -> s"${fairdConfig.hostPort}",
      s"$FAIRD_TLS_ENABLED" -> s"${fairdConfig.useTLS}",
      s"$FAIRD_TLS_CERT_PATH" -> s"${fairdConfig.certPath}",
      s"$FAIRD_TLS_KEY_PATH" -> s"${fairdConfig.keyPath}",
      s"$LOGGING_FILE_NAME" -> s"${fairdConfig.loggingFileName}",
      s"$LOGGING_LEVEL_ROOT" -> s"${fairdConfig.loggingLevelRoot}",
      s"$LOGGING_PATTERN_CONSOLE" -> s"${fairdConfig.loggingPatternConsole}",
      s"$LOGGING_PATTERN_FILE" -> s"${fairdConfig.loggingPatternFile}")
    val jo = new JSONObject()
    hostInfo.foreach(kv => jo.put(kv._1, kv._2))
    jo.toString()
  }

  private def getHostResourceString(): String = {
    val jo = new JSONObject()
    getResourceStatusString.foreach(kv => jo.put(kv._1, kv._2))
    jo.toString()
  }

  private def getDataFrameDocumentJsonString(dataFrameName: String): String = {
    val document = dataProvider.getDocument(dataFrameName)
    val schema = StructType.empty.add("url", StringType).add("alias", StringType).add("title", StringType)
    val stream = getSchema(dataFrameName).columns.map(col => col.name).map(name => Seq(document.getColumnURL(name).getOrElse("")
        , document.getColumnAlias(name).getOrElse(""), document.getColumnTitle(name).getOrElse("")))
      .map(seq => link.rdcn.struct.Row.fromSeq(seq))
    val ja = new JSONArray()
    stream.map(_.toJsonObject(schema)).foreach(ja.put(_))
    ja.toString()
  }

  private def getDataFrameSchemaString(dataFrameName: String): String = {
    val structType = getSchema(dataFrameName)
    val schema = StructType.empty.add("name", StringType).add("valueType", StringType).add("nullable", StringType)
    val stream = structType.columns.map(col => Seq(col.name, col.colType.name, col.nullable.toString))
      .map(seq => Row.fromSeq(seq))
    val ja = new JSONArray()
    stream.map(_.toJsonObject(schema)).foreach(ja.put(_))
    ja.toString()
  }

  private def getDataFrameStatisticsString(dataFrameName: String): String = {
    val statistics = dataProvider.getStatistics(dataFrameName)
    val jo = new JSONObject()
    jo.put("byteSize", statistics.byteSize)
    jo.put("rowCount", statistics.rowCount)
    jo.toString()
  }

  private def getSchema(dataFrameName: String): StructType = {
    val dataStreamSource: DataStreamSource = dataProvider.getDataStreamSource(dataFrameName)
    var structType = dataStreamSource.schema
    if (structType.isEmpty()) {
      val dataStreamSource: DataStreamSource = dataProvider.getDataStreamSource(dataFrameName)
      val iter = dataStreamSource.iterator
      if (iter.hasNext) {
        structType = DataUtils.inferSchemaFromRow(iter.next())
      }
    }
    structType
  }

  private def getResourceStatusString(): Map[String, String] = {
    val osBean = ManagementFactory.getOperatingSystemMXBean
      .asInstanceOf[OperatingSystemMXBean]
    val runtime = Runtime.getRuntime

    val cpuLoadPercent = (osBean.getSystemCpuLoad * 100).formatted("%.2f")
    val availableProcessors = osBean.getAvailableProcessors

    val totalMemory = runtime.totalMemory() / 1024 / 1024 // MB
    val freeMemory = runtime.freeMemory() / 1024 / 1024 // MB
    val maxMemory = runtime.maxMemory() / 1024 / 1024 // MB
    val usedMemory = totalMemory - freeMemory

    val systemMemoryTotal = osBean.getTotalPhysicalMemorySize / 1024 / 1024 // MB
    val systemMemoryFree = osBean.getFreePhysicalMemorySize / 1024 / 1024 // MB
    val systemMemoryUsed = systemMemoryTotal - systemMemoryFree
    Map(
      "cpu.cores" -> s"$availableProcessors",
      "cpu.usage.percent" -> s"$cpuLoadPercent%",
      "jvm.memory.max.mb" -> s"$maxMemory MB",
      "jvm.memory.total.mb" -> s"$totalMemory MB",
      "jvm.memory.used.mb" -> s"$usedMemory MB",
      "jvm.memory.free.mb" -> s"$freeMemory MB",
      "system.memory.total.mb" -> s"$systemMemoryTotal MB",
      "system.memory.used.mb" -> s"$systemMemoryUsed MB",
      "system.memory.free.mb" -> s"$systemMemoryFree MB"
    )
  }

  protected var fairdConfig: FairdConfig = _
  private val protocolSchema = "dacp"

  private val BLOB_STREAM: Byte = 1
  private val GET_STREAM: Byte = 2
  private val COOK_STREAM: Byte = 3
}

class AllowAllAuthProvider extends AuthProvider {

  /**
   * 用户认证，成功返回认证后的保持用户登录状态的凭证
   *
   * @throws AuthorizationException
   */
  override def authenticate(credentials: Credentials): AuthenticatedUser = new AuthenticatedUser {}

  /**
   * 判断用户是否具有某项权限
   *
   * @param user          已认证用户
   * @param dataFrameName 数据帧名称
   * @param opList        操作类型列表（Java List）
   * @return 是否有权限
   */
  override def checkPermission(user: AuthenticatedUser, dataFrameName: String, opList: List[DataOperationType]): Boolean = true
}
