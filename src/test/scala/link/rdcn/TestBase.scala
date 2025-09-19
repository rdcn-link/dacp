package link.rdcn

import link.rdcn.provider._
import link.rdcn.struct.{Row, StructType, DataStreamSource, DataStreamSourceFactory}
import link.rdcn.util.ClosableIterator
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.vocabulary.RDF

import java.io.File
import java.net.URI
import java.nio.file.{Files, Path, Paths}
import java.util.UUID
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

  val demoBaseDir = Paths.get("/faird-core", "src", "test", "demo").toString

  //权限
  val permissions = Map(
    adminUsername -> Set("/csv/data_1.csv", "/bin",
      "/csv/data_2.csv", "/csv/data_1.csv", "/csv/invalid.csv", "/excel/data.xlsx")
  )

  def genModel: Model = {
    ModelFactory.createDefaultModel()
  }

  def getOutputDir(subDirs: String*): String = {
    val outDir = Paths.get(System.getProperty("user.dir"), subDirs: _*) // 项目根路径
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
      case _: CSVSource => DataStreamSourceFactory.createCsvDataStreamSource(new File(dataFrameInfo.path))
      case _: DirectorySource => DataStreamSourceFactory.createFileListDataStreamSource(new File(dataFrameInfo.path))
      case _: ExcelSource => DataStreamSourceFactory.createExcelDataStreamSource(Paths.get(dataFrameInfo.path).toString)
      case _: InputSource => ???
    }

  }

  //若使用config，客户端也需要初始化因为是不同进程
  override def getDocument(dataFrameName: String): DataFrameDocument = {
    new DataFrameDocument {
      override def getSchemaURL(): Option[String] = Some("[SchemaURL defined by provider]")

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


