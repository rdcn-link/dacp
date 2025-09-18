package link.rdcn.dacp.optree

import jep.SubInterpreter
import link.rdcn.client.UrlValidator
import link.rdcn.dacp.FairdConfig
import link.rdcn.dacp.client.DacpClient
import link.rdcn.operation.{ExecutionContext, FilterOp, FunctionWrapper, LimitOp, MapOp, Operation, SelectOp, SharedInterpreterManager, SourceOp}
import link.rdcn.struct.DataFrame
import link.rdcn.user.TokenAuth
import org.json.{JSONArray, JSONObject}

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable.ListBuffer

/**
 * @Author renhao
 * @Description:
 * @Data 2025/9/18 10:53
 * @Modified By:
 */
trait FlowExecutionContext extends ExecutionContext {

  val pythonHome: String

  val fairdConfig: FairdConfig

  def loadSourceDataFrame(dataFrameNameUrl: String): Option[DataFrame]

  def getSubInterpreter(sitePackagePath: String, whlPath: String): Option[SubInterpreter] =
    Some(JepInterpreterManager.getJepInterpreter(sitePackagePath, whlPath, Some(pythonHome)))

  def getRepositoryClient(): Option[OperatorRepository]
}

object OperationTree {
  def fromJsonString(json: String, sourceList: ListBuffer[String]): Operation = {
    val parsed: JSONObject = new JSONObject(json)
    val opType = parsed.getString("type")
    if (opType == "SourceOp") {
      sourceList.append(parsed.getString("dataFrameName"))
      SourceOp(parsed.getString("dataFrameName"))
    } else if (opType == "RemoteSourceProxyOp") {
      RemoteSourceProxyOp(parsed.getString("baseUrl") + parsed.getString("path"), parsed.getString("token"))
    } else {
      val ja: JSONArray = parsed.getJSONArray("input")
      val inputs = (0 until ja.length).map(ja.getJSONObject(_).toString()).map(fromJsonString(_, sourceList))
      opType match {
        case "Map" => MapOp(FunctionWrapper(parsed.getJSONObject("function")), inputs: _*)
        case "Filter" => FilterOp(FunctionWrapper(parsed.getJSONObject("function")), inputs: _*)
        case "Limit" => LimitOp(parsed.getJSONArray("args").getInt(0), inputs: _*)
        case "Select" => SelectOp(inputs.head, parsed.getJSONArray("args").toList.asScala.map(_.toString): _*)
        case "TransformerNode" => TransformerNode(TransformFunctionWrapper.fromJsonObject(parsed.getJSONObject("function")), inputs: _*)
      }
    }
  }
}

case class RemoteSourceProxyOp(url: String, certificate: String) extends Operation {

  val baseUrlAndPath = UrlValidator.extractBaseUrlAndPath(url)
  var baseUrl: String = _
  var path: String = _
  baseUrlAndPath match {
    case Right(value) =>
      baseUrl = value._1
      path = value._2
    case Left(message) => throw new IllegalArgumentException(message)
  }

  override var inputs: Seq[Operation] = Seq.empty

  override def operationType: String = "RemoteSourceProxyOp"

  override def toJson: JSONObject = new JSONObject().put("type", operationType)
    .put("baseUrl", baseUrl).put("path", path).put("token", certificate)

  override def execute(ctx: ExecutionContext): DataFrame = {
    val dacpClient = DacpClient.connect(baseUrl, TokenAuth(certificate))
    dacpClient.get(baseUrl + path)
  }
}

case class TransformerNode(transformFunctionWrapper: TransformFunctionWrapper, inputOperations: Operation*) extends Operation {

  override var inputs: Seq[Operation] = inputOperations

  override def operationType: String = "TransformerNode"

  override def toJson: JSONObject = {
    val ja = new JSONArray()
    inputs.foreach(in => ja.put(in.toJson))
    new JSONObject().put("type", operationType)
      .put("function", transformFunctionWrapper.toJson)
      .put("input", ja)
  }

  override def execute(ctx: ExecutionContext): DataFrame = {
    transformFunctionWrapper.applyToDataFrames(inputs.map(_.execute(ctx)), ctx.asInstanceOf[FlowExecutionContext])
  }
}
