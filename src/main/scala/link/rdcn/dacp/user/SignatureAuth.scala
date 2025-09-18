package link.rdcn.dacp.user

import link.rdcn.user.{Credentials}
import org.json.JSONObject

import java.util.Base64

/**
 * @Author renhao
 * @Description:
 * @Data 2025/9/18 18:38
 * @Modified By:
 */
case class SignatureAuth(
                          serverId: String,
                          nonce: String,
                          issueTime: Long, //签发时间
                          validTo: Long, //过期时间
                          signature: Array[Byte] // UnionServer 私钥签名
                        ) extends Credentials {
  def toJson(): JSONObject = {
    val json = new JSONObject()
    json.put("serverId", serverId)
    json.put("nonce", nonce)
    json.put("issueTime", issueTime)
    json.put("validTo", validTo)
    json.put("signature", Base64.getEncoder.encodeToString(signature))
    json
  }
}

object SignatureAuth {
  def fromJson(json: JSONObject): SignatureAuth = {
    SignatureAuth(
      serverId = json.getString("serverId"),
      nonce = json.getString("nonce"),
      issueTime = json.getLong("issueTime"),
      validTo = json.getLong("validTo"),
      signature = Base64.getDecoder.decode(json.getString("signature"))
    )
  }
}
