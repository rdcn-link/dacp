package link.rdcn.dacp.server

import com.sun.management.OperatingSystemMXBean
import link.rdcn.DftpConfig
import link.rdcn.client.UrlValidator
import link.rdcn.dacp.ConfigKeys.{FAIRD_HOME, FAIRD_HOST_DOMAIN, FAIRD_HOST_NAME, FAIRD_HOST_PORT, FAIRD_HOST_POSITION, FAIRD_HOST_TITLE, FAIRD_TLS_CERT_PATH, FAIRD_TLS_ENABLED, FAIRD_TLS_KEY_PATH, LOGGING_FILE_NAME, LOGGING_LEVEL_ROOT, LOGGING_PATTERN_CONSOLE, LOGGING_PATTERN_FILE}
import link.rdcn.dacp.{ConfigKeys, FairdConfig}
import link.rdcn.dacp.optree.{FlowExecutionContext, OperatorRepository, RepositoryClient, TransformTree}
import link.rdcn.dacp.receiver.DataReceiver
import link.rdcn.dacp.user.{AuthProvider, KeyAuthProvider}
import link.rdcn.log.Logging
import link.rdcn.operation.TransformOp
import link.rdcn.provider.DataProvider
import link.rdcn.server.{ActionRequest, ActionResponse, BlobTicket, DftpMethodService, DftpTicket, GetRequest, GetResponse, GetTicket, PutRequest, PutResponse}
import link.rdcn.server.dftp.DftpServer
import link.rdcn.struct.ValueType.{LongType, RefType, StringType}
import link.rdcn.struct.{DFRef, DataFrame, DataStreamSource, DefaultDataFrame, Row, StructType}
import link.rdcn.user.{AuthenticationService, UserPrincipal}
import link.rdcn.util.{CodecUtils, DataUtils}
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.json.{JSONArray, JSONObject}
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader
import org.springframework.context.support.GenericApplicationContext
import org.springframework.core.io.ClassPathResource

import java.io.{File, FileInputStream, InputStreamReader, StringWriter}
import java.lang.management.ManagementFactory
import java.nio.charset.StandardCharsets
import java.util.Properties
import scala.collection.JavaConverters.asScalaBufferConverter

/**
 * @Author renhao
 * @Description:
 * @Data 2025/9/20 14:12
 * @Modified By:
 */
class DacpServer(dataProvider: DataProvider, dataReceiver: DataReceiver, authProvider: AuthProvider) extends Logging{

  private var fairdConfig: FairdConfig = _
  private val protocolScheme = "dacp"
  private var baseUrl: String = _

  private val authProviderWithKey = KeyAuthProvider(authProvider)
  private val dftpMethodService: DftpMethodService = new DftpMethodService {
    override def doGet(request: GetRequest, response: GetResponse): Unit = DacpServer.this.doGet(request, response)

    override def doPut(request: PutRequest, response: PutResponse): Unit = {
      val dataFrame = request.getDataFrame()
      try {
        dataReceiver.receive(dataFrame)
      } catch {
        case e: Exception => response.sendError(500, e.getMessage)
      }
      response.send(CodecUtils.encodeString(new JSONObject().put("status","success").toString))
    }

    override def doAction(request: ActionRequest, response: ActionResponse): Unit =
      response.sendError(501, s"${request.getActionName()} Not Implemented")
  }
  private val server: DacpServerProducer = new DacpServerProducer(authProviderWithKey, dftpMethodService)

  def start(dftpConfig: DftpConfig): Unit = {
    require(dftpConfig.isInstanceOf[FairdConfig])
    this.fairdConfig = dftpConfig.asInstanceOf[FairdConfig]
    authProviderWithKey.setFairdConfig(fairdConfig)
    baseUrl = s"$protocolScheme://${fairdConfig.hostPosition}:${fairdConfig.hostPort}"
    server.start(dftpConfig)
  }

  def close(): Unit = server.close()

  def enableTLS(tlsCertFile: File, tlsKeyFile: File): DftpServer =
    server.enableTLS(tlsCertFile, tlsKeyFile)

  def disableTLS(): DftpServer = server.disableTLS()

  def doCook(request: CookRequest, response: CookResponse): Unit = {
    val transformTree = request.getTransformTree
    val userPrincipal = request.getRequestUserPrincipal()
    transformTree.sourceUrlList.find(
      dataFrameUrl => !authProviderWithKey.checkPermission(userPrincipal, getUrlPath(dataFrameUrl))
    )match {
      case Some(dataFrameUrl) => response.sendError(403, s"access dataFrame ${dataFrameUrl} forbidden")
      case None => response.sendDataFrame(transformTree.execute(ctx))
    }
  }

  def doGet(request: GetRequest, response: GetResponse): Unit = {
    request.getRequestURI() match {
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
          response.sendDataFrame(doListDataFrames(request.getRequestURI()))
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
        val userPrincipal = request.getUserPrincipal()
        if(authProvider.checkPermission(userPrincipal, otherPath)){
          val dataStreamSource: DataStreamSource = dataProvider.getDataStreamSource(otherPath)
          val dataFrame: DataFrame = DefaultDataFrame(dataStreamSource.schema, dataStreamSource.iterator)
          response.sendDataFrame(dataFrame)
        }else{
          response.sendError(403, s"access dataFrame $otherPath Forbidden")
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

  def getProtocolScheme(): String = protocolScheme

  def getFairdConfig(): FairdConfig = fairdConfig

  def getBaseUrl(): String = baseUrl

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
    val schema = StructType.empty.add("url", StringType).add("alias", StringType).add("title", StringType).add("dataFrameTitle", StringType)
    val stream = getSchema(dataFrameName).columns.map(col => col.name).map(name => Seq(document.getColumnURL(name).getOrElse("")
        , document.getColumnAlias(name).getOrElse(""), document.getColumnTitle(name).getOrElse(""), document.getDataFrameTitle().getOrElse("")))
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

  def getUrlPath(dataFrameUrl: String): String = {
    val urlValidator = UrlValidator(protocolScheme)
    urlValidator.extractPath(dataFrameUrl) match {
      case Right(path) => path
      case Left(message) => dataFrameUrl
    }
  }

  private class DacpServerProducer(userAuthenticationService: AuthenticationService, dftpMethod: DftpMethodService)
    extends DftpServer(userAuthenticationService, dftpMethod)
  {
    this.setProtocolSchema(protocolScheme)

    override def parseTicket(bytes: Array[Byte]): DftpTicket =
    {
      val BLOB_TICKET: Byte = 1
      val GET_TICKET: Byte = 2
      val COOK_TICKET: Byte = 3

      val buffer = java.nio.ByteBuffer.wrap(bytes)
      val typeId: Byte = buffer.get()
      val len = buffer.getInt()
      val b = new Array[Byte](len)
      buffer.get(b)
      val ticketContent = new String(b, StandardCharsets.UTF_8)
      typeId match {
        case BLOB_TICKET => BlobTicket(ticketContent)
        case GET_TICKET => GetTicket(ticketContent)
        case COOK_TICKET => CookTicket(ticketContent)
        case _ => throw new Exception("parse fail")
      }
    }

    override def getStreamByTicket(userPrincipal: UserPrincipal,
                                   ticket: Array[Byte],
                                   response: GetResponse): Unit =
    {
      val dftpTicket = parseTicket(ticket)
      dftpTicket match
      {
        case CookTicket(ticketContent) =>
        {
          val cookRequest: CookRequest = new CookRequest
          {
            override def getTransformTree: TransformOp = TransformTree.fromJsonString(ticketContent)

            override def getRequestUserPrincipal(): UserPrincipal = userPrincipal
          }
          val cookResponse: CookResponse = new CookResponse
          {
            override def sendDataFrame(dataFrame: DataFrame): Unit =
              response.sendDataFrame(dataFrame)

            override def sendError(code: Int, message: String): Unit =
              response.sendError(code, message)
          }
          doCook(cookRequest, cookResponse)
        }
        case other => super.getStreamByTicket(userPrincipal, ticket, response)
      }
    }
  }

  private def ctx = new FlowExecutionContext {

    override val fairdConfig: FairdConfig = getFairdConfig()

    //TODO pythonHome from env
    override def pythonHome: String = this.fairdConfig.pythonHome

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
    //TODO Repository config
    override def getRepositoryClient(): Option[OperatorRepository] = Some(new RepositoryClient("10.0.89.38", 8088))
  }
}

object DacpServer {
  var server: DacpServer = _

  def start(fairdHome: File): Unit = {
    val context: GenericApplicationContext = new GenericApplicationContext();
    val reader: XmlBeanDefinitionReader = new XmlBeanDefinitionReader(context);
    reader.loadBeanDefinitions(new ClassPathResource(s"${fairdHome.getAbsolutePath}" + File.separator + "conf" + File.separator + "faird.xml"));
    context.refresh();
    val dataReceiver = context.getBean("dataReceiver").asInstanceOf[DataReceiver]
    val dataProvider = context.getBean("dataProvider").asInstanceOf[DataProvider]
    val authProvider = context.getBean("authProvider").asInstanceOf[AuthProvider]

    val server: DacpServer = new DacpServer(dataProvider, dataReceiver, authProvider)
    val props = loadProperties(fairdHome.getAbsolutePath + File.separator + "conf" + File.separator + "faird.conf")
    props.setProperty(ConfigKeys.FAIRD_HOME, fairdHome.getAbsolutePath)
    val fairdConfig = FairdConfig.load(props)
    server.start(fairdConfig)
  }

  def start(fairdHome: File, dataProvider: DataProvider, dataReceiver: DataReceiver, authProvider: AuthProvider): Unit = {
    val server: DacpServer = new DacpServer(dataProvider, dataReceiver, authProvider)
    val props = loadProperties(fairdHome.getAbsolutePath + File.separator + "conf" + File.separator + "faird.conf")
    props.setProperty(ConfigKeys.FAIRD_HOME, fairdHome.getAbsolutePath)
    val fairdConfig = FairdConfig.load(props)
    server.start(fairdConfig)
  }

  def close(): Unit = {
    if(server!=null) server.close()
  }

  private def loadProperties(path: String): Properties = {
    val props = new Properties()
    val fis = new InputStreamReader(new FileInputStream(path), "UTF-8")
    try props.load(fis) finally fis.close()
    props
  }
}
