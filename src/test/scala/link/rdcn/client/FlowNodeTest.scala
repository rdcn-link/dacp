/**
 * @Author Yomi
 * @Description:
 * @Data 2025/7/28 18:45
 * @Modified By:
 */
package link.rdcn.client

import link.rdcn.client.recipe._
import org.codehaus.janino.SimpleCompiler
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

import scala.collection.JavaConverters.mapAsScalaMapConverter

class FlowNodeTest {
  @Test
  def testSource(): Unit = {
    assertEquals(SourceNode("/csv/data_1.csv"), FlowNode.source("/csv/data_1.csv"))
  }

  @Test
  def testFromJavaClass(): Unit = {
    val javaCode =
      """
        |import java.util.*;
        |import link.rdcn.util.*;
        |import link.rdcn.client.dag.Transformer11;
        |import link.rdcn.struct.*;
        |
        |public class DynamicUDF implements Transformer11 {
        |    public DynamicUDF() {
        |        // 默认构造器，必须显式写出
        |    }
        |    @Override
        |    public link.rdcn.struct.DataFrame transform(final link.rdcn.struct.DataFrame dataFrame) {
        |            final scala.collection.Iterator<Row> iter = ((DefaultDataFrame)dataFrame).stream();
        |            final scala.collection.Iterator<Row> rows =  new scala.collection.Iterator<Row>() {
        |            public boolean hasNext() {
        |                return iter.hasNext();
        |            }
        |            public Row next() {
        |                Row row = (Row)iter.next();
        |                return Row.fromJavaList(Arrays.asList(row.get(0), row.get(1), 100));
        |            }
        |        };
        |                return DataUtils.getDataFrameByStream(rows);
        |            }
        |}
        |""".stripMargin
    val compiler = new SimpleCompiler()
    compiler.cook(javaCode)
    val clazz = compiler.getBytecodes.asScala.toMap
  }

  @Test
  def testStocked(): Unit = {
    assertEquals(RepositoryNode("my-java-app-2"), FlowNode.stocked("my-java-app-2"))
  }


}
