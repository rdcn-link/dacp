package link.rdcn.operators

import link.rdcn.dacp.recipe.Transformer11
import link.rdcn.struct.{DataFrame, Row}

/**
 * @Author renhao
 * @Description:
 * @Data 2025/7/25 15:50
 * @Modified By:
 */
class DataProcessor extends Transformer11 {
  override def transform(dataFrame: DataFrame): DataFrame = {
    dataFrame.map(row => Row.fromTuple((row._1, row._2, 100)))
  }
}
