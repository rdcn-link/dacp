package link.rdcn.dacp.optree

import link.rdcn.client.UrlValidator
import link.rdcn.operation._
import link.rdcn.struct.DataFrame
import link.rdcn.user.TokenAuth
import org.json.{JSONArray, JSONObject}

import scala.collection.JavaConverters.asScalaBufferConverter

/**
 * @Author renhao
 * @Description:
 * @Data 2025/9/18 10:53
 * @Modified By:
 */

/**
 * Tree-structured operation collection with hierarchical execution
 * Example:
 * op1 op2 op3
 *  \   /  /
 *   op4 op5
 *     \ /
 *     op6
 */
object TransformTree {
  def fromJsonString(json: String): TransformOp = {
    val parsed: JSONObject = new JSONObject(json)
    val opType = parsed.getString("type")
    if (opType == "SourceOp") {
      SourceOp(parsed.getString("dataFrameName"))
    } else if (opType == "RemoteSourceProxyOp") {
      RemoteSourceProxyOp(parsed.getString("baseUrl") + parsed.getString("path"), parsed.getString("token"))
    } else {
      val ja: JSONArray = parsed.getJSONArray("input")
      val inputs = (0 until ja.length).map(ja.getJSONObject(_).toString()).map(fromJsonString(_))
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

case class RemoteSourceProxyOp(url: String, certificate: String) extends TransformOp {

  val baseUrlAndPath = UrlValidator.extractBaseUrlAndPath(url)
  var baseUrl: String = _
  var path: String = _
  baseUrlAndPath match {
    case Right(value) =>
      baseUrl = value._1
      path = value._2
    case Left(message) => throw new IllegalArgumentException(message)
  }

  override var inputs: Seq[TransformOp] = Seq.empty

  override def sourceUrlList: Set[String] = Set.empty

  override def operationType: String = "RemoteSourceProxyOp"

  override def toJson: JSONObject = new JSONObject().put("type", operationType)
    .put("baseUrl", baseUrl).put("path", path).put("token", certificate)

  override def execute(ctx: ExecutionContext): DataFrame = {
    require(ctx.isInstanceOf[FlowExecutionContext])
    ctx.asInstanceOf[FlowExecutionContext].loadRemoteDataFrame(baseUrl, path, TokenAuth(certificate))
      .getOrElse(throw new Exception(s"get remote DataFrame ${baseUrl+path} fail"))
  }
}

case class TransformerNode(transformFunctionWrapper: TransformFunctionWrapper, inputTransforms: TransformOp*) extends TransformOp {

  override var inputs: Seq[TransformOp] = inputTransforms

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
