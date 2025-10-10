package link.rdcn.dacp.client

import org.json.JSONObject

import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.Base64


/**
 * @Author renhao
 * @Description:
 * @Data 2025/10/10 14:41
 * @Modified By:
 */
object AuthPlatform {

  private var clientToken: Option[String] = None



  def main(args: Array[String]): Unit = {
    val clientToken =  "faird-client1"+":"+"tcqi54cnp3cewj94nd9uop2q"
    val basic = "Basic " + Base64.getUrlEncoder().encodeToString(clientToken.getBytes())

    val paramMap = new JSONObject().put("username", "faird-user1")
      .put("password", "user1@cnic.cn")
      .put("grantType","password")

    val httpClient = HttpClient.newHttpClient()
    val request = HttpRequest.newBuilder()
      .uri(URI.create("https://api.opendatachain.cn/auth/oauth/token"))
      .header("Authorization", basic)
      .header("Content-Type", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(paramMap.toString()))
      .build()

    val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
    println(new JSONObject(response.body()).getString("data"))
  }
}
