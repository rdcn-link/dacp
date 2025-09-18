package link.rdcn.provider

/**
 * @Author renhao
 * @Description:
 * @Data 2025/7/10 16:21
 * @Modified By:
 */
trait DataFrameDocument extends Serializable {
  def getSchemaURL(): Option[String]

  def getColumnURL(colName: String): Option[String]

  def getColumnAlias(colName: String): Option[String]

  def getColumnTitle(colName: String): Option[String]
}
