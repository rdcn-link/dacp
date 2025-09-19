/**
 * @Author Yomi
 * @Description:
 * @Data 2025/7/24 18:08
 * @Modified By:
 */
package link.rdcn


import link.rdcn.TestBase.getResourcePath
import link.rdcn.client.dacp.DacpClient
import link.rdcn.client.recipe._
import link.rdcn.struct.{DataFrame, ExecutionResult, Row}
import link.rdcn.user.UsernamePassword

object ClientMultiLanguageCodeDemo {

  def main(args: Array[String]): Unit = {
    // 连接Faird服务
    val dc: DacpClient = DacpClient.connectTLS("dacp://localhost:3101", UsernamePassword("admin@instdb.cn", "admin001"))

    //构建数据源节点
    val sourceNode: FlowNode = FlowNode.source("/csv/data_1.csv")
    //构建自定义操作节点
    val transformer11Node = FlowNode.ofTransfomer11(new Transformer11 {
      override def transform(dataFrame: DataFrame): DataFrame = {
        dataFrame.map(row => Row.fromTuple(row.getAs[Long](0), row.get(1), 100))
      }
    })
    val transformerDAGJavaCode: Flow = Flow.pipe(sourceNode, transformer11Node)
    val javaDAGDfs: ExecutionResult = dc.execute(transformerDAGJavaCode)
    println("--------------打印通过自定义操作的数据帧--------------")
    javaDAGDfs.map().foreach { case (_, df) => df.limit(3).foreach(row => println(row)) }

    // 使用算子库指定id的算子对数据帧进行操作
    ConfigLoader.init(getResourcePath(""))
    val repositoryOperator = FlowNode.stocked("aaa.bbb.id4")
    val transformerDAGRepositoryOperator: Flow = Flow.pipe(sourceNode, repositoryOperator)
    val RepositoryOperatorDAGDfs: ExecutionResult = dc.execute(transformerDAGRepositoryOperator)
    println("--------------打印通过算子库指定id的算子操作的数据帧--------------")
    RepositoryOperatorDAGDfs.map().foreach { case (_, df) => df.limit(3).foreach(row => println(row)) }

  }

}
