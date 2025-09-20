package link.rdcn;

import link.rdcn.provider.*;
import link.rdcn.dacp.received.DataReceiver;
import link.rdcn.dacp.server.DacpServer;
import link.rdcn.struct.*;
import link.rdcn.dacp.user.AuthProvider;
import link.rdcn.user.UserPrincipal;
import link.rdcn.user.Credentials;
import link.rdcn.dacp.user.DataOperationType;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.RDF;
import scala.Option;
import scala.collection.JavaConverters;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/26 10:07
 * @Modified By:
 */
public class JProviderTest {

    public static void main(String[] args) throws IOException, InterruptedException {

        InputSource inputSource = new CSVSource(",", false);
        DataFrameInfo dfInfo = new DataFrameInfo("file_1.csv", Paths.get("/home/dd/file_1.csv").toUri(), inputSource, StructType.empty().add("id", ValueTypeHelper.getIntType(), true)
                .add("name", ValueTypeHelper.getStringType(), true));
        List<DataFrameInfo> listD = new ArrayList<DataFrameInfo>();
        listD.add(dfInfo);
        DataSet ds1 = new DataSet("dd", "1", JavaConverters.asScalaBufferConverter(listD).asScala().toList());

        // 创建AuthenticatedUser的实现类，用于存储用户登陆凭证，这里通过记录token作为示例
        class TestAuthenticatedUser implements UserPrincipal {
            private final String token;

            /**
             * 构造方法，用于初始化成员变量token
             * @param token
             */
            TestAuthenticatedUser(String token) {
                this.token = token;
            }
        }

        // 创建AuthProvider的实现类，用于处理用户认证和授权逻辑
        AuthProvider authProvider = new AuthProvider() {
            /**
             * 使用提供的凭证进行用户身份验证
             *
             * @param credentials 用户凭证对象，包含用户的登录信息。
             * @return 一个经过验证的用户对象。
             * @throws org.apache.arrow.flight.FlightRuntimeException 如果身份验证失败，则抛出此类异常，需要Provider按需求实现。
             */
            @Override
            public UserPrincipal authenticate(Credentials credentials) throws org.apache.arrow.flight.FlightRuntimeException {
                return new TestAuthenticatedUser(UUID.randomUUID().toString());
            }

            /**
             * 授权方法，用于判断用户是否有权限访问指定的DataFrame。
             *
             * @param user          已认证的用户对象
             * @param dataFrameName 需要访问的DataFrame名称
             * @return 如果用户有权限访问指定的DataFrame，则返回true；否则返回false，需要Provider按需求实现。
             */
            @Override
            public boolean checkPermission(UserPrincipal user, String dataFrameName, scala.collection.immutable.List<DataOperationType> opList) {
                return true;
            }
        };


        // 创建DataProvider的实例，用于处理DataSet和DataFrame的获取逻辑
        link.rdcn.provider.DataProvider dataProvider = new link.rdcn.provider.DataProvider() {
            private List<DataSet> dataSetsJavaList;


            /**
             * 获取数据集名称列表
             * Provider实现该方法以提供DataSet名称列表
             *
             * @return 返回数据集名称的列表
             */
            @Override
            public List<String> listDataSetNames() {


                return Collections.emptyList();

            }

            /**
             * 获取DataSet元数据
             * Provider通过配置文件和DataSetName获得URI和DataSetID
             * Provider提供DataSet和空RDF模型对象，并需要实现对RDF信息的注入
             * 元数据将从rdfModel获取
             *
             * @param dataSetName DataSet名称
             * @param rdfModel    空RDF模型对象
             */
            @Override
            public void getDataSetMetaData(String dataSetName, Model rdfModel) {
                String hostname = ConfigLoader.getConfig().hostName(); //配置文件中 faird.hostName=cerndc
                int port = ConfigLoader.getConfig().hostPort(); //faird.hostPort=3101
                String dataSetID = "根据dataSetName拿ID";
                String datasetURI = "dacp://" + hostname + ":" + port + "/" + dataSetID;
                Resource datasetRes = rdfModel.createResource(datasetURI);

                Property hasFile = rdfModel.createProperty(datasetURI + "/hasFile");
                Property hasName = rdfModel.createProperty(datasetURI + "/name");

                datasetRes.addProperty(RDF.type, rdfModel.createResource("DataSet"));
                datasetRes.addProperty(hasName, dataSetName);
                List<String> dataFrameNames = new ArrayList<>();
                dataFrameNames.add("123123");
                dataFrameNames.add("dsfsdf");
                for (String dataFrameName : dataFrameNames) {
                    datasetRes.addProperty(hasFile, dataFrameName);
                }
            }

            /**
             * 列出指定DataSet的DataFrame名称列表
             *
             * @param dataSetName 数据集名称
             * @return 数据帧名称列表，如果数据集不存在或数据帧列表为空，则返回空列表
             */
            @Override
            public List<String> listDataFrameNames(String dataSetName) {
                return Collections.emptyList();
            }

            /**
             * 通过DataFrame的名称获得文件位置和文件类型，根据DataStreamSourceFactory获取DataStreamSource
             * 这里使用辅助函数getDataFrameInfo获得名称到文件位置的映射，Provider应根据实际情况实现该映射
             * Provider应当实现DataStreamSourceFactory静态类，根据不同的文件类型获得DataStreamSource
             * 注意：Factory应该设计针对读取一个文件夹下二进制文件的情况，此时DataFrame名称应为文件夹名称而非文件名
             * 并且Schema应该定义为：[name, byteSize, 文件类型, 创建时间, 最后修改时间, 最后访问时间, file]
             * 对于其他文件类型，Row的每列应与Schema一一对应
             * DataStreamSource类提供将数据处理为Iterator[Row]的方式，Provider也需要实现此类
             * 此处示例为通过Name、Schema、文件类型（csv，json，bin...）获得DataStreamSource
             *
             * DataStreamSource的实现类，用于流式提供DataFrame
             * createDataFrame方法用于组装DataFrame对象，实例中包含schema和迭代器作为示例
             *
             * @param dataFrameName DataFrame的名称
             * @return DataStreamSource
             */
            @Override
            public DataStreamSource getDataStreamSource(String dataFrameName) {
                DirectorySource directorySource = new DirectorySource(false);
                DataFrameInfo dataFrameInfo = getDataFrameInfo(dataFrameName).getOrElse(null);
                return DataStreamSourceFactory.createFileListDataStreamSource(new File(dataFrameInfo.path()), false);
            }

            /**
             * 根据DataFrame名称获取DataFrameDocument
             * DataFrameDocument用于封装DataFrameSchemaURL等元数据
             *
             * @param dataFrameName DataFrame的名称
             * @return DataFrameDocument
             */
            @Override
            public link.rdcn.provider.DataFrameDocument getDocument(String dataFrameName) {
                return null;
            }

            /**
             * 根据DataFrame名称获取DataFrameStatistics
             * DataFrameStatistics用于封装rowCount等DataFrame统计信息
             * 此处示例为返回null，实际根据Provider需要实现
             * @param dataFrameName
             * @return DataFrameStatistics
             */
            @Override
            public link.rdcn.provider.DataFrameStatistics getStatistics(String dataFrameName) {
                return null;
            }


            /**
             * 根据DataFrame名称获取DataFrameInfo，包含DataFrame位置和类型等信息
             * 此处示例为遍历DataSet列表，查找DataFrameInfo，实际根据Provider需要实现
             * @param dataFrameName
             * @return
             */
            private Option<DataFrameInfo> getDataFrameInfo(String dataFrameName) {
                // 遍历 dataSetsJavaList
                for (DataSet ds : dataSetsJavaList) {
                    Option<DataFrameInfo> dfInfo = ds.getDataFrameInfo(dataFrameName);
                    if (dfInfo.isEmpty()) {
                        return dfInfo;
                    }
                }
                return Option.empty();
            }
        };

        DataReceiver dataReceiver = new DataReceiver() {
            @Override
            public void start() {

            }

            @Override
            public void receiveRow(DataFrame dataFrame) {

            }

            @Override
            public void finish() {

            }

            @Override
            public void close() {
                DataReceiver.super.close();
            }
        };

        //设置一个路径存放faird相关外部文件，其中faird.conf 存放到 $fairdHome/conf 路径下
        String fairdHome = "/Users/renhao/IdeaProjects/faird-java/target/test-classes/";
        DacpServer fairdServer = new DacpServer(dataProvider, dataReceiver, authProvider);
        ConfigLoader.init(fairdHome);
        //启动faird服务
        fairdServer.start(ConfigLoader.fairdConfig());
        //关闭faird服务
        fairdServer.close();
    }

}
