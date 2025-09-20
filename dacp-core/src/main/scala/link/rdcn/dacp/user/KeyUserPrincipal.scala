package link.rdcn.dacp.user

import link.rdcn.dacp.server.KeyBasedAuthUtils
import link.rdcn.user.UserPrincipal
import link.rdcn.util.CodecUtils
import org.json.JSONObject

import java.security.PublicKey

/**
 * @Author renhao
 * @Description:
 * @Data 2025/9/18 18:27
 * @Modified By:
 */
case class KeyUserPrincipal(
                                 publicKey: Option[PublicKey],
                                 serverId: String,
                                 nonce: String,
                                 issueTime: Long, //签发时间
                                 validTo: Long, //过期时间
                                 signature: Array[Byte] // UnionServer 私钥签名
                               ) extends UserPrincipal {
  def checkPermission(): Boolean = {
    if (publicKey.isEmpty) false else {
      if (validTo > issueTime) {
        KeyBasedAuthUtils.verifySignature(publicKey.get, getChallenge(), signature) && System.currentTimeMillis() < validTo
      } else {
        KeyBasedAuthUtils.verifySignature(publicKey.get, getChallenge(), signature)
      }
    }
  }

  private def getChallenge(): Array[Byte] = {
    val jo = new JSONObject().put("serverId", serverId)
      .put("nonce", nonce)
      .put("issueTime", issueTime)
      .put("validTo", validTo)
    CodecUtils.encodeString(jo.toString)
  }
}
