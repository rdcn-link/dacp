/**
 * @Author Yomi
 * @Description:
 * @Data 2025/7/28 18:45
 * @Modified By:
 */
package link.rdcn.client

import link.rdcn.dacp.recipe.{FlowNode, RepositoryNode, SourceNode}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class FlowNodeTest {
  @Test
  def testSource(): Unit = {
    assertEquals(SourceNode("/csv/data_1.csv"), FlowNode.source("/csv/data_1.csv"))
  }

  @Test
  def testStocked(): Unit = {
    assertEquals(RepositoryNode("my-java-app-2"), FlowNode.stocked("my-java-app-2"))
  }


}
