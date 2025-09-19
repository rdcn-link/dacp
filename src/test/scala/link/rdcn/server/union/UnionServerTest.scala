package link.rdcn.server.union

import link.rdcn.provider.{DataFrameDocument, DataFrameStatistics, DataProvider}
import link.rdcn.client.dacp.DacpClient
import link.rdcn.client.recipe.{FlowNode, SourceNode, Flow, Transformer21}
import link.rdcn.client.union.UnionClient
import link.rdcn.ConfigLoader
import link.rdcn.server.DataReceiverTest
import link.rdcn.server.dacp.DacpServer
import link.rdcn.server.dftp.AllowAllAuthProvider
import link.rdcn.struct.ValueType.StringType
import link.rdcn.struct.{DataFrame, DataStreamSource, DefaultDataFrame, Row, StructType}
import link.rdcn.user.Credentials
import link.rdcn.util.ClosableIterator
import org.apache.arrow.flight.FlightRuntimeException
import org.apache.jena.rdf.model.Model
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test}
import org.junit.jupiter.api.Assertions._

import java.nio.file.Paths
import java.util
import java.util.Arrays

object UnionServerTest {
  var unionServer: UnionServer = _
  var dacpServer1: DacpServer = _
  var dacpServer2: DacpServer = _

  @BeforeAll
  def startServer(): Unit = {
    val unionServerHome = getClass.getResource("/").getPath
    val node1Home = Paths.get(unionServerHome, "node1/").toAbsolutePath.toString
    val node2Home = Paths.get(unionServerHome, "node2/").toAbsolutePath.toString

    val dataProviderTest1 = new DataProvider {

      override def listDataSetNames(): util.List[String] = Arrays.asList("dataSet1")

      override def getDataSetMetaData(dataSetId: String, rdfModel: Model): Unit = {
      }

      override def listDataFrameNames(dataSetId: String): util.List[String] = Arrays.asList("/dataFrame1")

      override def getDataStreamSource(dataFrameName: String): DataStreamSource = {
        if (dataFrameName == "/dataFrame1") {
          new DataStreamSource {
            override def rowCount: Long = -1

            override def schema: StructType = StructType.empty.add("col1", StringType)

            override def iterator: ClosableIterator[Row] = {
              val rows = Seq.range(0, 10).map(index => Row.fromSeq(Seq("id" + index))).toIterator
              ClosableIterator(rows)()
            }
          }
        } else {
          throw new Exception(s"$dataFrameName not found")
        }

      }

      override def getDocument(dataFrameName: String): DataFrameDocument = new DataFrameDocument {
        override def getSchemaURL(): Option[String] = None

        override def getColumnURL(colName: String): Option[String] = None

        override def getColumnAlias(colName: String): Option[String] = None

        override def getColumnTitle(colName: String): Option[String] = None
      }

      override def getStatistics(dataFrameName: String): DataFrameStatistics = new DataFrameStatistics {
        override def rowCount: Long = -1

        override def byteSize: Long = -1
      }
    }

    val dataProviderTest2 = new DataProvider {

      override def listDataSetNames(): util.List[String] = Arrays.asList("dataSet2")

      override def getDataSetMetaData(dataSetId: String, rdfModel: Model): Unit = {
      }

      override def listDataFrameNames(dataSetId: String): util.List[String] = Arrays.asList("/dataFrame2")

      override def getDataStreamSource(dataFrameName: String): DataStreamSource = {
        if (dataFrameName == "/dataFrame2") {
          new DataStreamSource {
            override def rowCount: Long = -1

            override def schema: StructType = StructType.empty.add("col1", StringType)

            override def iterator: ClosableIterator[Row] = {
              val rows = Seq.range(0, 10).map(index => Row.fromSeq(Seq("id" + index))).toIterator
              ClosableIterator(rows)()
            }
          }
        } else {
          throw new Exception(s"$dataFrameName not found")
        }

      }

      override def getDocument(dataFrameName: String): DataFrameDocument = new DataFrameDocument {
        override def getSchemaURL(): Option[String] = None

        override def getColumnURL(colName: String): Option[String] = None

        override def getColumnAlias(colName: String): Option[String] = None

        override def getColumnTitle(colName: String): Option[String] = None
      }

      override def getStatistics(dataFrameName: String): DataFrameStatistics = new DataFrameStatistics {
        override def rowCount: Long = -1

        override def byteSize: Long = -1
      }
    }


    ConfigLoader.init(node1Home)
    dacpServer1 = new DacpServer(dataProviderTest1, new DataReceiverTest, new AllowAllAuthProvider)
    println(s"启动DacpServer1 bind ${ConfigLoader.fairdConfig.host}:" + ConfigLoader.fairdConfig.port)
    dacpServer1.start(ConfigLoader.fairdConfig)

    ConfigLoader.init(node2Home)
    dacpServer2 = new DacpServer(dataProviderTest2, new DataReceiverTest, new AllowAllAuthProvider)
    println(s"启动DacpServer2 bind ${ConfigLoader.fairdConfig.host}:" + ConfigLoader.fairdConfig.port)
    dacpServer2.start(ConfigLoader.fairdConfig)

    ConfigLoader.init(unionServerHome)
    unionServer = UnionServer.initCluster("dacp://0.0.0.0:3102", "dacp://0.0.0.0:3103")
    println(s"启动UnionServer bind ${ConfigLoader.fairdConfig.host}:" + ConfigLoader.fairdConfig.port)
    unionServer.start(ConfigLoader.fairdConfig)
  }

  @AfterAll
  def close(): Unit = {
    dacpServer1.close()
    dacpServer2.close()
    unionServer.close()
  }
}

class UnionServerTest {

  @Test
  def unionServerTest(): Unit = {
    val nodeClient1 = DacpClient.connect("dacp://0.0.0.0:3102", Credentials.ANONYMOUS)
    val nodeClient2 = DacpClient.connect("dacp://0.0.0.0:3103", Credentials.ANONYMOUS)
    val dataSetList1 = nodeClient1.listDataSetNames()
    val dataSetList2 = nodeClient2.listDataSetNames()
    val client = UnionClient.connect("dacp://0.0.0.0:3104", Credentials.ANONYMOUS)
    val dataSetUnionList = client.listDataSetNames()
    assert((dataSetList1.toSet union dataSetList2.toSet) == dataSetUnionList.toSet)
    assert(client.listDataFrameNames("dataSet1").toSet == nodeClient1.listDataFrameNames("dataSet1").toSet)
  }

  @Test
  def notFoundDataFrameTest(): Unit = {
    val client = UnionClient.connect("dacp://0.0.0.0:3104", Credentials.ANONYMOUS)
    val ex = assertThrows(classOf[FlightRuntimeException], () => {
      client.get("dacp://0.0.0.0:3104/dataFrame1")
    })
    assert(ex.getMessage.contains("not found resource"))
  }

  @Test
  def getDataFrameTest(): Unit = {
    val client = UnionClient.connect("dacp://0.0.0.0:3104", Credentials.ANONYMOUS)
    val df = client.get("dacp://0.0.0.0:3102/dataFrame1")
    assert(df.collect().length == 10)
  }

  @Test
  def recipeTest(): Unit = {
    val client = UnionClient.connect("dacp://0.0.0.0:3104", Credentials.ANONYMOUS)
    val nodeMap = scala.collection.mutable.Map[String, FlowNode]()
    val edgesMap = scala.collection.mutable.Map[String, Seq[String]]()
    nodeMap.put("A", SourceNode("dacp://0.0.0.0:3102/dataFrame1"))
    nodeMap.put("B", SourceNode("dacp://0.0.0.0:3103/dataFrame2"))
    nodeMap.put("C", new Transformer21 {
      override def transform(leftDataFrame: DataFrame, rightDataFrame: DataFrame): DataFrame = {
        val stream: Iterator[Row] = leftDataFrame.mapIterator[Iterator[Row]](iter => iter) ++ rightDataFrame.mapIterator[Iterator[Row]](iter => iter)
        val schema = leftDataFrame.schema
        DefaultDataFrame(schema, stream)
      }
    })
    edgesMap.put("A", Seq("C"))
    edgesMap.put("B", Seq("C"))
    val flow = Flow(nodeMap.toMap, edgesMap.toMap)
    assert(client.execute(flow).single().collect().length == 20)
  }


}
