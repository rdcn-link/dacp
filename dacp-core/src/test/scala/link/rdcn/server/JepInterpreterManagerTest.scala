/**
 * @Author Yomi
 * @Description:
 * @Data 2025/8/27 09:40
 * @Modified By:
 */
package link.rdcn.server

import link.rdcn.dacp.optree.JepInterpreterManager
import link.rdcn.operation.SharedInterpreterManager
import org.junit.jupiter.api.Test

class JepInterpreterManagerTest {

  @Test
  def getJepTest(): Unit = {
    val jep = SharedInterpreterManager.getInterpreter
    jep.close()
  }
}
