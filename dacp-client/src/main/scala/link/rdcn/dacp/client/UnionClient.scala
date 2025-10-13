package link.rdcn.dacp.client

import link.rdcn.client.{RemoteDataFrameProxy, UrlValidator}
import link.rdcn.operation.SourceOp
import link.rdcn.struct.DataFrame
import link.rdcn.user.{AnonymousCredentials, Credentials, UsernamePassword}

/**
 * @Author renhao
 * @Description:
 * @Date 2025/8/28 09:23
 * @Modified By:
 */
class UnionClient private(host: String, port: Int, useTLS: Boolean = false) extends DacpClient(host, port, useTLS) {

  override def get(url: String): DataFrame = {
    val urlValidator = new UrlValidator(prefixSchema)
    if (urlValidator.isPath(url)) RemoteDataFrameProxy(SourceOp(url), super.getRows) else {
      urlValidator.validate(url) match {
        case Right(value) => RemoteDataFrameProxy(SourceOp(url), getRows)
        case Left(message) => throw new IllegalArgumentException(message)
      }
    }
  }
}

object UnionClient {
  val protocolSchema = "dacp"
  private val urlValidator = UrlValidator(protocolSchema)

  def connect(url: String, credentials: Credentials = Credentials.ANONYMOUS,  useUnifiedLogin: Boolean = false): UnionClient = {
    urlValidator.validate(url) match {
      case Right(parsed) =>
        val client = new UnionClient(parsed._1, parsed._2.getOrElse(3101))
        if(useUnifiedLogin){
          credentials match {
            case AnonymousCredentials => client.login(credentials)
            case c: UsernamePassword => client.login(AuthPlatform.authenticate(c))
            case _ => throw new IllegalArgumentException(s"the $credentials is not supported")
          }
        }else client.login(credentials)
        client
      case Left(err) =>
        throw new IllegalArgumentException(s"Invalid DACP URL: $err")
    }
  }

  def connectTLS(url: String, credentials: Credentials = Credentials.ANONYMOUS,  useUnifiedLogin: Boolean = false): UnionClient = {
    urlValidator.validate(url) match {
      case Right(parsed) =>
        val client = new UnionClient(parsed._1, parsed._2.getOrElse(3101), true)
        if(useUnifiedLogin){
          credentials match {
            case AnonymousCredentials => client.login(credentials)
            case c: UsernamePassword => client.login(AuthPlatform.authenticate(c))
            case _ => throw new IllegalArgumentException(s"the $credentials is not supported")
          }
        }else client.login(credentials)
        client
      case Left(err) =>
        throw new IllegalArgumentException(s"Invalid DACP URL: $err")
    }
  }
}
