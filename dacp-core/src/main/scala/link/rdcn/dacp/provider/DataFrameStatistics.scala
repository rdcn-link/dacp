/**
 * @Author Yomi
 * @Description:
 * @Data 2025/7/22 13:49
 * @Modified By:
 */
package link.rdcn.provider

trait DataFrameStatistics extends Serializable {
  def rowCount: Long

  def byteSize: Long
}
