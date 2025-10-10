package link.rdcn.dacp

import link.rdcn.DftpConfig

import java.nio.file.Paths
import java.security.{PrivateKey, PublicKey}
import scala.beans.BeanProperty

/**
 * @Author renhao
 * @Description:
 * @Data 2025/7/9 17:23
 * @Modified By:
 */
//TODO 修改为scala风格，兼容spring IOC
class FairdConfig() extends DftpConfig {
  @BeanProperty var fairdHome: String = {
    val resource = getClass.getClassLoader.getResource("")
    if(resource != null) {
      resource.getPath
    }else{
      System.getProperty("user.dir")
    }
  }
  @BeanProperty var hostName: String = s"${hostPosition}:${hostPort}"
  @BeanProperty var hostTitle: String = ""
  @BeanProperty var hostPosition: String = "0.0.0.0"
  @BeanProperty var hostDomain: String = ""
  @BeanProperty var hostPort: Int = 3101
  @BeanProperty var useTLS: Boolean = false
  @BeanProperty var certPath: String = ""
  @BeanProperty var keyPath: String = ""
  @BeanProperty var publicKeyPath: String = ""
  @BeanProperty var privateKeyPath: String = ""
  @BeanProperty var loggingFileName: String = fairdHome + "./access.log"
  @BeanProperty var loggingLevelRoot: String = "INFO"
  @BeanProperty var loggingPatternConsole: String = "%d{HH:mm:ss} %-5level %logger{36} - %msg%n"
  @BeanProperty var loggingPatternFile: String = "%d{yyyy-MM-dd HH:mm:ss} [%thread] %-5level %logger - %msg%n"
  @BeanProperty var pythonHome: String = null

  override def toString: String = s"FairdConfig($fairdHome, $hostName, $hostTitle, ...)"

  override def host: String = hostPosition

  override def port: Int = hostPort

  override def pubKeyMap: Map[String, PublicKey] = KeyBasedAuthUtils.loadPublicKeys(Paths.get(fairdHome, publicKeyPath).toAbsolutePath.toString)

  override def privateKey: Option[PrivateKey] = Some(KeyBasedAuthUtils.loadPrivateKey(Paths.get(fairdHome, privateKeyPath).toAbsolutePath.toString))

  override def logFilePath: String = loggingFileName

  override def rootLogLevel: String = loggingLevelRoot

  override def consoleLogPattern: String = loggingPatternConsole

  override def fileLogPattern: String = loggingPatternFile
}

object FairdConfig {

  def apply(fairdHome: String,
            hostName: String,
            hostTitle: String,
            hostPosition: String,
            hostDomain: String,
            hostPort: Int,
            useTLS: Boolean,
            certPath: String,
            keyPath: String,
            publicKeyPath: String,
            privateKeyPath: String,
            loggingFileName: String,
            loggingLevelRoot: String,
            loggingPatternConsole: String,
            loggingPatternFile: String,
            pythonHome: String): FairdConfig = {
    val fairdConfig = new FairdConfig
    fairdConfig.fairdHome = fairdHome
    fairdConfig.hostName = hostName
    fairdConfig.hostTitle = hostTitle
    fairdConfig.hostPosition = hostPosition
    fairdConfig.hostDomain = hostDomain
    fairdConfig.hostPort = hostPort
    fairdConfig.useTLS = useTLS
    fairdConfig.certPath = certPath
    fairdConfig.keyPath = keyPath
    fairdConfig.publicKeyPath = publicKeyPath
    fairdConfig.privateKeyPath = privateKeyPath
    fairdConfig.loggingFileName = loggingFileName
    fairdConfig.loggingLevelRoot = loggingLevelRoot
    fairdConfig.loggingPatternConsole = loggingPatternConsole
    fairdConfig.loggingPatternFile = loggingPatternFile
    fairdConfig.pythonHome = pythonHome
    fairdConfig
  }

  /** 从 java.util.Properties 加载配置生成 FairdConfig 实例 */
  def load(props: java.util.Properties): FairdConfig = {
    def getOrDefault(key: String, default: String): String =
      Option(props.getProperty(key))
        .getOrElse(default)

    FairdConfig(
      fairdHome = getOrDefault(ConfigKeys.FAIRD_HOME, ""),
      hostName = getOrDefault(ConfigKeys.FAIRD_HOST_NAME, s"${getOrDefault(ConfigKeys.FAIRD_HOST_POSITION, "0.0.0.0")}:${getOrDefault(ConfigKeys.FAIRD_HOST_PORT, "3101")}"),
      hostTitle = getOrDefault(ConfigKeys.FAIRD_HOST_TITLE, ""),
      hostPosition = getOrDefault(ConfigKeys.FAIRD_HOST_POSITION, "0.0.0.0"),
      hostDomain = getOrDefault(ConfigKeys.FAIRD_HOST_DOMAIN, ""),
      hostPort = getOrDefault(ConfigKeys.FAIRD_HOST_PORT, "3101").trim.toInt,
      useTLS = getOrDefault(ConfigKeys.FAIRD_TLS_ENABLED, "false").trim.toBoolean,
      certPath = getOrDefault(ConfigKeys.FAIRD_TLS_CERT_PATH, "server.crt"),
      keyPath = getOrDefault(ConfigKeys.FAIRD_TLS_KEY_PATH, "server.pem"),
      publicKeyPath = getOrDefault(ConfigKeys.FAIRD_PUBLIC_KEY_PATH, "server.pub"),
      privateKeyPath = getOrDefault(ConfigKeys.FAIRD_PRIVATE_KEY_PATH, "server.key"),
      loggingFileName = getOrDefault(ConfigKeys.LOGGING_FILE_NAME, "./access.log"),
      loggingLevelRoot = getOrDefault(ConfigKeys.LOGGING_LEVEL_ROOT, "INFO"),
      loggingPatternConsole = getOrDefault(ConfigKeys.LOGGING_PATTERN_CONSOLE, "%d{HH:mm:ss} %-5level %logger{36} - %msg%n"),
      loggingPatternFile = getOrDefault(ConfigKeys.LOGGING_PATTERN_FILE, "%d{yyyy-MM-dd HH:mm:ss} [%thread] %-5level %logger - %msg%n"),
      pythonHome = getOrDefault(ConfigKeys.PYTHON_HOME, null),
    )
  }
}