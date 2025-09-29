package link.rdcn

import link.rdcn.dacp.provider.DataProvider
import link.rdcn.dacp.struct.{DataFrameDocument, DataFrameStatistics}
import link.rdcn.dacp.{ConfigKeys, FairdConfig}
import link.rdcn.struct.ValueType.{BinaryType, BlobType, BooleanType, DoubleType, FloatType, IntType, LongType, RefType, StringType}
import link.rdcn.struct._
import link.rdcn.user.UserPrincipal
import org.apache.arrow.flight.CallStatus
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.vocabulary.RDF

import java.io.{File, FileInputStream, InputStreamReader}
import java.net.URI
import java.nio.file.{Files, Path, Paths}
import java.util.{Collections, Properties, UUID}
import scala.collection.JavaConverters.seqAsJavaListConverter


/** *
 * 所有测试用Provider相关公共类和变量
 */
trait TestBase {

}

object TestBase {

  // 文件数量配置
  val binFileCount = 3
  val csvFileCount = 3

  val adminUsername = "admin@instdb.cn"
  val adminPassword = "admin001"
  val userUsername = "user"
  val userPassword = "user"
  val anonymousUsername = "ANONYMOUS"

  //生成Token
  val genToken = () => UUID.randomUUID().toString
  val resourceUrl = getClass.getProtectionDomain.getCodeSource.getLocation
  val testClassesDir = new File(resourceUrl.toURI)
  val demoBaseDir = Paths.get( "src", "test", "resources").toString

  def genModel: Model = {
    ModelFactory.createDefaultModel()
  }

  def getOutputDir(subDirs: String*): String = {
    val outDir = Paths.get(testClassesDir.getParentFile.getParentFile.getAbsolutePath, subDirs: _*) // 项目根路径
    Files.createDirectories(outDir)
    outDir.toString
  }

  /**
   *
   * @param resourceName
   * @return test下名为resourceName的文件夹
   */
  def getResourcePath(resourceName: String): String = {
    val url = Option(getClass.getClassLoader.getResource(resourceName))
      .orElse(Option(getClass.getResource(resourceName))) // 先到test-classes中查找，然后到classes中查找
      .getOrElse(throw new RuntimeException(s"Resource not found: $resourceName"))
    val nativePath: Path = Paths.get(url.toURI())
    nativePath.toString
  }

  def listFiles(directoryPath: String): Seq[File] = {
    val dir = new File(directoryPath)
    if (dir.exists() && dir.isDirectory) {
      dir.listFiles().filter(_.isFile).toSeq
    } else {
      Seq.empty
    }
  }

  def convertStructTypeToArrowSchema(structType: StructType): Schema = {
    val fields: List[Field] = structType.columns.map { column =>
      val arrowFieldType = column.colType match {
        case IntType =>
          new FieldType(column.nullable, new ArrowType.Int(32, true), null)
        case LongType =>
          new FieldType(column.nullable, new ArrowType.Int(64, true), null)
        case FloatType =>
          new FieldType(column.nullable, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), null)
        case DoubleType =>
          new FieldType(column.nullable, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), null)
        case StringType =>
          new FieldType(column.nullable, ArrowType.Utf8.INSTANCE, null)
        case BooleanType =>
          new FieldType(column.nullable, ArrowType.Bool.INSTANCE, null)
        case BinaryType =>
          new FieldType(column.nullable, new ArrowType.Binary(), null)
        case RefType =>
          val metadata = new java.util.HashMap[String, String]()
          metadata.put("logicalType", "Url")
          new FieldType(column.nullable, ArrowType.Utf8.INSTANCE, null, metadata)
        case BlobType =>
          val metadata = new java.util.HashMap[String, String]()
          metadata.put("logicalType", "blob")
          new FieldType(column.nullable, new ArrowType.Binary(), null, metadata)
        case _ =>
          throw new UnsupportedOperationException(s"Unsupported type: ${column.colType}")
      }

      new Field(column.name, arrowFieldType, Collections.emptyList())
    }.toList

    new Schema(fields.asJava)
  }

  def sendErrorWithFlightStatus(code: Int, message: String): UserPrincipal = {
    val status = code match {
      case 400 => CallStatus.INVALID_ARGUMENT
      case 401 => CallStatus.UNAUTHENTICATED
      case 403 => CallStatus.UNAUTHORIZED
      case 404 => CallStatus.NOT_FOUND
      case 408 => CallStatus.TIMED_OUT
      case 409 => CallStatus.ALREADY_EXISTS
      case 500 => CallStatus.INTERNAL
      case 501 => CallStatus.UNIMPLEMENTED
      case 503 => CallStatus.UNAVAILABLE
      case _ => CallStatus.UNKNOWN
    }
    throw status.withDescription(message).toRuntimeException
  }

}


abstract class DataProviderImpl extends DataProvider {
  val dataSetsScalaList: List[DataSet]
  val dataFramePaths: (String => String)

  def listDataSetNames(): java.util.List[String] = {
    dataSetsScalaList.map(_.dataSetName).asJava
  }

  def getDataSetMetaData(dataSetName: String, rdfModel: Model): Unit = {
    val dataSet: DataSet = dataSetsScalaList.find(_.dataSetName == dataSetName).getOrElse(return rdfModel)
    dataSet.getMetadata(rdfModel)
  }

  def listDataFrameNames(dataSetName: String): java.util.List[String] = {
    val dataSet: DataSet = dataSetsScalaList.find(_.dataSetName == dataSetName).getOrElse(return new java.util.ArrayList)
    dataSet.dataFrames.map(_.name).asJava
  }

  def getDataStreamSource(dataFrameName: String): DataStreamSource = {
    val dataFrameInfo: DataFrameInfo = getDataFrameInfo(dataFrameName).getOrElse(return new DataStreamSource {
      override def rowCount: Long = -1

      override def schema: StructType = StructType.empty

      override def iterator: ClosableIterator[Row] = ClosableIterator(Iterator.empty)()
    })
    dataFrameInfo.inputSource match {
      case _: CSVSource => DataStreamSource.csv(new File(dataFrameInfo.path))
      case _: DirectorySource => DataStreamSource.filePath(new File(dataFrameInfo.path))
      case _: ExcelSource => DataStreamSource.excel(dataFrameInfo.path.toString)
      case _: InputSource => ???
    }

  }

  //若使用config，客户端也需要初始化因为是不同进程
  override def getDocument(dataFrameName: String): DataFrameDocument = {
    new DataFrameDocument {
      override def getSchemaURL(): Option[String] = Some("[SchemaURL defined by provider]")

      override def getDataFrameTitle(): Option[String] = Some("[title]")

      override def getColumnURL(colName: String): Option[String] = Some("[ColumnURL defined by provider]")

      override def getColumnAlias(colName: String): Option[String] = Some("[ColumnAlias defined by provider]")

      override def getColumnTitle(colName: String): Option[String] = Some("[ColumnTitle defined by provider]")
    }
  }

  override def getStatistics(dataFrameName: String): DataFrameStatistics = {
    val rowCountResult = getDataStreamSource(dataFrameName).rowCount

    new DataFrameStatistics {
      override def rowCount: Long = rowCountResult

      override def byteSize: Long = 0L
    }
  }

  private def getDataFrameInfo(dataFrameName: String): Option[DataFrameInfo] = {
    dataSetsScalaList.foreach(ds => {
      val dfInfo = ds.getDataFrameInfo(dataFrameName)
      if (dfInfo.nonEmpty) return dfInfo
    })
    None
  }

}


case class DataFrameInfo(
                          name: String,
                          path: URI,
                          inputSource: InputSource,
                          schema: StructType
                        ) {
}

case class DataSet(
                    dataSetName: String,
                    dataSetId: String,
                    dataFrames: List[DataFrameInfo]
                  ) {
  /** 生成 RDF 元数据模型 */
  def getMetadata(model: Model): Unit = {
    val datasetURI = s"dacp://${ConfigLoader.fairdConfig.hostName}:${ConfigLoader.fairdConfig.hostPort}/" + dataSetId
    val datasetRes = model.createResource(datasetURI)

    val hasFile = model.createProperty(datasetURI + "/hasFile")
    val hasName = model.createProperty(datasetURI + "/name")

    datasetRes.addProperty(RDF.`type`, model.createResource(datasetURI + "/DataSet"))
    datasetRes.addProperty(hasName, dataSetName)

    dataFrames.foreach { df =>
      datasetRes.addProperty(hasFile, df.name)
    }
  }

  def getDataFrameInfo(dataFrameName: String): Option[DataFrameInfo] = {
    dataFrames.find { dfInfo =>
      val normalizedDfPath: String = dfInfo.path.toString
      normalizedDfPath.contains(dataFrameName)
    }
  }
}

sealed trait InputSource

case class CSVSource(
                      delimiter: String = ",",
                      head: Boolean = false
                    ) extends InputSource

case class JSONSource(
                       multiline: Boolean = false
                     ) extends InputSource

case class DirectorySource(
                            recursive: Boolean = true
                          ) extends InputSource

case class StructuredSource() extends InputSource

case class ExcelSource() extends InputSource

object ConfigLoader {
  var fairdConfig: FairdConfig = _
  def init(fairdHome: String): Unit = synchronized {
    val props = loadProperties(s"$fairdHome" + File.separator + "conf" + File.separator + "faird.conf")
    props.setProperty(ConfigKeys.FAIRD_HOME, fairdHome)
    fairdConfig = FairdConfig.load(props)
  }

  def loadProperties(path: String): Properties = {
    val props = new Properties()
    val fis = new InputStreamReader(new FileInputStream(path), "UTF-8")
    try props.load(fis) finally fis.close()
    props
  }

  def getConfig(): FairdConfig = fairdConfig

}



