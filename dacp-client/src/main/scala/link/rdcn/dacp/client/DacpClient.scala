package link.rdcn.dacp.client

import link.rdcn.client.{DftpClient, RemoteDataFrameProxy, UrlValidator}
import link.rdcn.dacp.optree.{LangTypeV2, RepositoryOperator, TransformFunctionWrapper, TransformerNode}
import link.rdcn.dacp.recipe.{ExecutionResult, Flow, FlowPath, RepositoryNode, SourceNode, Transformer11, Transformer21}
import link.rdcn.dacp.struct.{CookTicket, DataFrameDocument, DataFrameStatistics}
import link.rdcn.operation.{DataFrameCall11, DataFrameCall21, SerializableFunction, SourceOp, TransformOp}
import link.rdcn.struct.{ClosableIterator, DFRef, DataFrame, DefaultDataFrame, Row, StructType}
import link.rdcn.user.Credentials
import org.apache.arrow.flight.Ticket
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.json.{JSONArray, JSONObject}

import java.io.{File, StringReader}
import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.mutable

/**
 * @Author renhao
 * @Description:
 * @Date 2025/8/18 16:30
 * @Modified By:
 */
class DacpClient(host: String, port: Int, useTLS: Boolean = false) extends DftpClient(host, port, useTLS) {

  override val prefixSchema: String = "dacp"

  def listDataSetNames(): Seq[String] = getDataSetInfoMap.keys.toSeq

  def listDataFrameNames(dsName: String): Seq[String] = {
    val url = getDataSetInfoMap.get(dsName).getOrElse(return Seq.empty)._3.url
    get(url).collect().map(row => row.getAs[String](0))
  }

  def getDataSetMetaData(dsName: String): Model = {
    val model = ModelFactory.createDefaultModel()
    val rdfString = getDataSetInfoMap.get(dsName).getOrElse(return model)._1
    val reader = new StringReader(rdfString)
    model.read(reader, null, "RDF/XML")
    model
  }

  def executeTransformTree(transformOp: TransformOp): DataFrame = {
    val schemaAndRow = getCookRows(transformOp.toJsonString)
    DefaultDataFrame(schemaAndRow._1, schemaAndRow._2)
  }

  //TODO: Remove this method and update related test cases accordingly
  def getByPath(path: String): DataFrame = {
    super.get(dacpUrlPrefix + path)
  }

  //执行 recipe
  def execute(recipe: Flow): ExecutionResult = {
    val executePaths: Seq[FlowPath] = recipe.getExecutionPaths()
    val dfs: Seq[DataFrame] = executePaths.map(path => RemoteDataFrameProxy(transformFlowToOperation(path), getCookRows))
    new ExecutionResult() {
      override def single(): DataFrame = dfs.head

      override def get(name: String): DataFrame = dfs(name.toInt - 1)

      override def map(): Map[String, DataFrame] = dfs.zipWithIndex.map {
        case (dataFrame, id) => (id.toString, dataFrame)
      }.toMap
    }
  }

  private def transformFlowToOperation(path: FlowPath): TransformOp = {
    path.node match {
      case f: Transformer11 =>
        val genericFunctionCall = DataFrameCall11(new SerializableFunction[DataFrame, DataFrame] {
          override def apply(v1: DataFrame): DataFrame = f.transform(v1)
        })
        val transformerNode: TransformerNode = TransformerNode(TransformFunctionWrapper.getJavaSerialized(genericFunctionCall), transformFlowToOperation(path.children.head))
        transformerNode
      case f: Transformer21 =>
        val genericFunctionCall = DataFrameCall21(new SerializableFunction[(DataFrame, DataFrame), DataFrame] {
          override def apply(v1: (DataFrame, DataFrame)): DataFrame = f.transform(v1._1, v1._2)
        })
        val leftInput = transformFlowToOperation(path.children.head)
        val rightInput = transformFlowToOperation(path.children.last)
        val transformerNode: TransformerNode = TransformerNode(TransformFunctionWrapper.getJavaSerialized(genericFunctionCall), leftInput, rightInput)
        transformerNode
      case node: RepositoryNode =>
        val jo = new JSONObject()
        jo.put("type", LangTypeV2.REPOSITORY_OPERATOR.name)
        jo.put("functionID", node.functionId)
        val transformerNode: TransformerNode = TransformerNode(TransformFunctionWrapper.fromJsonObject(jo).asInstanceOf[RepositoryOperator], transformFlowToOperation(path.children.head))
        transformerNode
      case s: SourceNode => SourceOp(s.dataFrameName)
      case other => throw new IllegalArgumentException(s"This FlowNode ${other} is not supported please extend Transformer11 trait")
    }
  }

  def getCookRows(transformOpStr: String): (StructType, ClosableIterator[Row]) = {
    val schemaAndIter = getStream(flightClient, new Ticket(CookTicket(transformOpStr).encodeTicket()))
    val stream = schemaAndIter._2.map(seq => Row.fromSeq(seq))
    (schemaAndIter._1, ClosableIterator(stream)())
  }

  def getDocument(dataFrameName: String): DataFrameDocument = {
    val jsonString: String = getDataFrameInfoMap.get(dataFrameName).map(_._2).getOrElse("[]")
    val jo = new JSONArray(jsonString).getJSONObject(0)
    new DataFrameDocument {
      override def getSchemaURL(): Option[String] = Some("[SchemaURL defined by provider]")

      override def getDataFrameTitle(): Option[String] = Some(jo.getString("title"))

      override def getColumnURL(colName: String): Option[String] = Some(jo.getString("url"))

      override def getColumnAlias(colName: String): Option[String] = Some(jo.getString("alias"))

      override def getColumnTitle(colName: String): Option[String] = Some(jo.getString("title"))
    }
  }

  def getStatistics(dataFrameName: String): DataFrameStatistics = {
    val jo = new JSONObject(getDataFrameInfoMap.get(dataFrameName).map(_._4).getOrElse(""))
    new DataFrameStatistics {
      override def rowCount: Long = jo.getLong("rowCount")

      override def byteSize: Long = jo.getLong("byteSize")
    }
  }

  def getDataFrameSize(dataFrameName: String): Long = {
    getDataFrameInfoMap.get(dataFrameName).map(_._1).getOrElse(0L)
  }

  def getHostInfo: Map[String, String] = {
    val jo = new JSONObject(getHostInfoMap().head._2._1)
    jo.keys().asScala.map { key =>
      key -> jo.getString(key)
    }.toMap
  }

  def getServerResourceInfo: Map[String, String] = {
    val jo = new JSONObject(getHostInfoMap().head._2._2)
    jo.keys().asScala.map { key =>
      key -> jo.getString(key)
    }.toMap
  }

  private val dacpUrlPrefix: String = s"$prefixSchema://$host:$port"

  //dataSetName -> (metaData, dataSetInfo, dataFrames)
  def getDataSetInfoMap: Map[String, (String, String, DFRef)] = {
    val result = mutable.Map[String, (String, String, DFRef)]()
    get(dacpUrlPrefix + "/listDataSets").mapIterator(rows => rows.foreach(row => {
      result.put(row.getAs[String](0), (row.getAs[String](1), row.getAs[String](2), row.getAs[DFRef](3)))
    }))
    result.toMap
  }

  //dataFrameName -> (size,document,schema,statistic,dataFrame)
  def getDataFrameInfoMap: Map[String, (Long, String, String, String, DFRef)] = {
    val result = mutable.Map[String, (Long, String, String, String, DFRef)]()
    getDataSetInfoMap.values.map(v => get(v._3.url)).foreach(df => {
      df.mapIterator(iter => iter.foreach(row => {
        result.put(row.getAs[String](0), (row.getAs[Long](1), row.getAs[String](2), row.getAs[String](3), row.getAs[String](4), row.getAs[DFRef](5)))
      }))
    })
    result.toMap
  }

  //hostName -> (hostInfo, resourceInfo)
  def getHostInfoMap(): Map[String, (String, String)] = {
    val result = mutable.Map[String, (String, String)]()
    get(dacpUrlPrefix + "/listHostInfo").mapIterator(iter => iter.foreach(row => {
      result.put(row.getAs[String](0), (row.getAs[String](1), row.getAs[String](2)))
    }))
    result.toMap
  }
}

object DacpClient {
  val protocolSchema = "dacp"
  private val urlValidator = UrlValidator(protocolSchema)

  def connect(url: String, credentials: Credentials = Credentials.ANONYMOUS): DacpClient = {
    urlValidator.validate(url) match {
      case Right(parsed) =>
        val client = new DacpClient(parsed._1, parsed._2.getOrElse(3101))
        client.login(credentials)
        client
      case Left(err) =>
        throw new IllegalArgumentException(s"Invalid DACP URL: $err")
    }
  }

  def connectTLS(url: String, file: File, credentials: Credentials = Credentials.ANONYMOUS): DacpClient = {
    System.setProperty("javax.net.ssl.trustStore", file.getAbsolutePath)
    urlValidator.validate(url) match {
      case Right(parsed) =>
        val client = new DacpClient(parsed._1, parsed._2.getOrElse(3101), true)
        client.login(credentials)
        client
      case Left(err) =>
        throw new IllegalArgumentException(s"Invalid DACP URL: $err")
    }
  }
}
