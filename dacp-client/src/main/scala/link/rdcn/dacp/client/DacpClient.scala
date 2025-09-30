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
import scala.collection.mutable.ArrayBuffer

/**
 * @Author renhao
 * @Description:
 * @Date 2025/8/18 16:30
 * @Modified By:
 */
class DacpClient(host: String, port: Int, useTLS: Boolean = false) extends DftpClient(host, port, useTLS) {

  override val prefixSchema: String = "dacp"

  def listDataSetNames(): Seq[String] = {
    val result = ArrayBuffer[String]()
    get(dacpUrlPrefix + "/listDataSets").mapIterator(rows => rows.foreach(row => {
      result+=(row.getAs[String](0))
    }))
    result
  }

  def listDataFrameNames(dsName: String): Seq[String] = {
    val allDataSets = get(dacpUrlPrefix + "/listDataSets")
      .mapIterator(rows => rows.find(row => row.getAs[String](0) == dsName))
    allDataSets.map { row =>
      val url = row.getAs[DFRef](3).url
      get(url).collect().map(dataRow => dataRow.getAs[String](0))
    }.getOrElse(Seq.empty)
  }

  def getDataSetMetaData(dsName: String): Model = {
    val model = ModelFactory.createDefaultModel()
    val rdfString = new String(doAction(s"/getDataSetMetaData/${dsName}"), "UTF-8").trim
    rdfString match {
      case s if s.nonEmpty =>
        val reader = new StringReader(s)
        model.read(reader, null, "RDF/XML")
      case _ =>
    }
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
    val jsonString: String = {
      new String(doAction(s"/getDocument/${dataFrameName}"), "UTF-8").trim match {
        case s if s.nonEmpty => s
        case _ => "[]"
      }
    }
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
    val jsonString: String = {
      new String(doAction(s"/getStatistics/${dataFrameName}"), "UTF-8").trim match {
        case s if s.isEmpty => ""
        case s => s
      }
    }
    val jo = new JSONObject(jsonString)
    new DataFrameStatistics {
      override def rowCount: Long = jo.getLong("rowCount")

      override def byteSize: Long = jo.getLong("byteSize")
    }
  }

  def getDataFrameSize(dataFrameName: String): Long = {
      new String(doAction(s"/getDataFrameSize/${dataFrameName}"), "UTF-8").trim match {
        case s if s.nonEmpty => s.asInstanceOf[Long]
        case _ => 0L
      }
  }

  def getHostInfo: Map[String, String] = {
    val result = mutable.Map[String, String]()
    get(dacpUrlPrefix + "/listHostInfo").mapIterator(iter => iter.foreach(row => {
      result.put(row.getAs[String](0), row.getAs[String](1))
    }))
    val jo = new JSONObject(result.toMap.head._2)
    jo.keys().asScala.map { key =>
      key -> jo.getString(key)
    }.toMap
  }

  def getServerResourceInfo: Map[String, String] = {
    val result = mutable.Map[String, String]()
    get(dacpUrlPrefix + "/listHostInfo").mapIterator(iter => iter.foreach(row => {
      result.put(row.getAs[String](0), row.getAs[String](2))
    }))
    val jo = new JSONObject(result.toMap.head._2)
    jo.keys().asScala.map { key =>
      key -> jo.getString(key)
    }.toMap
  }

  private val dacpUrlPrefix: String = s"$prefixSchema://$host:$port"
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
