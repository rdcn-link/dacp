package link.rdcn.server.union

import link.rdcn.util.KeyBasedAuthUtils.{generateKeyPair, signData, verifySignature}
import org.junit.jupiter.api.Test

import java.util.Base64

/**
 * @Author renhao
 * @Description:
 * @Date 2025/9/4 15:49
 * @Modified By:
 */
class KeyBasedAuthUtilsTest {

  @Test
  def SSHStyleAuthUtilTest(): Unit = {
    // 假设这是Server端的密钥对
    val serverKeyPair = generateKeyPair()
    val serverPublicKey = serverKeyPair.getPublic
    val serverPrivateKey = serverKeyPair.getPrivate

    // Client 先保存 Server 的公钥（就像 SSH known_hosts）
    val knownServerPublicKey = serverPublicKey

    // Client 发一个随机 challenge 给 Server
    val challenge = "hello-secure-world".getBytes("UTF-8")

    // Server 用私钥签名
    val signature = signData(serverPrivateKey, challenge)

    println(s"Signature: ${Base64.getEncoder.encodeToString(signature)}")

    // Client 用 Server 公钥验证
    val ok = verifySignature(knownServerPublicKey, challenge, signature)

    assert(ok)
  }
}
