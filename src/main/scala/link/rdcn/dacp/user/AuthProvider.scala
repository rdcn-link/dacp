package link.rdcn.dacp.user

import link.rdcn.dacp.FairdConfig
import link.rdcn.user.{AuthenticationService, Credentials, UserPrincipal}

/**
 * @Author renhao
 * @Description:
 * @Data 2025/7/9 17:08
 * @Modified By:
 */

trait AuthProvider extends AuthenticationService{

  /**
   * 用户认证，成功返回认证后的保持用户登录状态的凭证
   */
  def authenticate(credentials: Credentials): UserPrincipal

  /**
   * 判断用户是否具有某项权限
   *
   * @param user          已认证用户
   * @param dataFrameName 数据帧名称
   * @param opList        操作类型列表（Java List）
   * @return 是否有权限
   */
  def checkPermission(user: UserPrincipal,
                      dataFrameName: String,
                      opList: List[DataOperationType] = List.empty): Boolean
}

case class KeyAuthProvider(authProvider: AuthProvider) extends AuthProvider {

  var fairdConfig: FairdConfig = _

  def setFairdConfig(fairdConfig: FairdConfig): Unit = this.fairdConfig = fairdConfig

  override def authenticate(credentials: Credentials): UserPrincipal = {
    credentials match {
      case sig: KeyCredentials =>
        KeyUserPrincipal(fairdConfig.pubKeyMap.get(sig.serverId), sig.serverId, sig.nonce, sig.issueTime, sig.validTo, sig.signature)
      case other => authProvider.authenticate(other)
    }
  }

  override def checkPermission(user: UserPrincipal, dataFrameName: String, opList: List[DataOperationType] = List.empty): Boolean = {
    user match {
      case key: KeyUserPrincipal => key.checkPermission()
      case other => authProvider.checkPermission(other, dataFrameName, opList)
    }
  }
}