package link.rdcn;


import link.rdcn.client.recipe.*;
import link.rdcn.struct.Blob;
import link.rdcn.struct.DataFrame;
import link.rdcn.struct.DefaultDataFrame;
import link.rdcn.struct.Row;
import link.rdcn.user.UsernamePassword;
import link.rdcn.util.DataUtils;
import org.apache.commons.io.IOUtils;
import org.apache.jena.rdf.model.Model;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;
import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @Author Yomi
 * @Description:
 * @Data 2025/7/14 19:38
 * @Modified By:
 */
public class JClientDemo {

    private static final Logger log = LoggerFactory.getLogger(JClientDemo.class);

    public static void main(String[] args) {
        // 通过用户名密码非加密连接FairdClient
//        FairdClient dc = FairdClient.connect("dacp://localhost:3101", new UsernamePassword("admin@instdb.cn", "admin001"));
        // 通过用户名密码tls加密连接FairdClient 需要用户端进行相关配置
        // 客户端证书路径配置
        JFairdClient dc = JFairdClient.connectTLS("dacp://localhost:3101", new UsernamePassword("admin@instdb.cn", "admin001"));
        // 匿名连接FairdClient
//        FairdClient dcAnonymous = FairdClient.connect("dacp://localhost:3101", Credentials.ANONYMOUS());

        //获得所有的数据集名称
        System.out.println("--------------打印数据集列表--------------");
        List<String> dataSetNames = dc.listDataSetNames();
        for (String name : dataSetNames) {
            System.out.println(name);
        }

        //获得指定数据集的所有的数据帧名称
        System.out.println("--------------打印数据集 csv 所有数据帧名称--------------");
        List<String> frameNames = dc.listDataFrameNames("csv");
        for (String frameName : frameNames) {
            System.out.println(frameName);
        }

        //获得指定数据集的元数据信息
        System.out.println("--------------打印数据集 csv 的元数据信息--------------");
        Model metaData = dc.getDataSetMetaData("csv");
        metaData.write(System.out, "TURTLE");

        //获得host基本信息
        System.out.println("--------------打印host基本信息--------------");
        Map<String, String> hostInfo = dc.getHostInfo();
        System.out.println(hostInfo.get(ConfigKeys.FAIRD_HOST_NAME()));
        System.out.println(hostInfo.get(ConfigKeys.FAIRD_HOST_TITLE()));
        System.out.println(hostInfo.get(ConfigKeys.FAIRD_HOST_PORT()));
        System.out.println(hostInfo.get(ConfigKeys.FAIRD_HOST_POSITION()));
        System.out.println(hostInfo.get(ConfigKeys.FAIRD_HOST_DOMAIN()));
        System.out.println(hostInfo.get(ConfigKeys.FAIRD_TLS_ENABLED()));
        System.out.println(hostInfo.get(ConfigKeys.FAIRD_TLS_CERT_PATH()));
        System.out.println(hostInfo.get(ConfigKeys.FAIRD_TLS_KEY_PATH()));
        System.out.println(hostInfo.get(ConfigKeys.LOGGING_FILE_NAME()));
        System.out.println(hostInfo.get(ConfigKeys.LOGGING_LEVEL_ROOT()));
        System.out.println(hostInfo.get(ConfigKeys.LOGGING_PATTERN_CONSOLE()));
        System.out.println(hostInfo.get(ConfigKeys.LOGGING_PATTERN_FILE()));

        //获得服务器资源信息
        System.out.println("--------------打印服务器资源信息--------------");
        Map<String, String> serverResourceInfo = dc.getServerResourceInfo();
        System.out.println(serverResourceInfo.get(ResourceKeys.CPU_CORES()));
        System.out.println(serverResourceInfo.get(ResourceKeys.CPU_USAGE_PERCENT()));
        System.out.println(serverResourceInfo.get(ResourceKeys.JVM_FREE_MEMORY_MB()));
        System.out.println(serverResourceInfo.get(ResourceKeys.JVM_USED_MEMORY_MB()));
        System.out.println(serverResourceInfo.get(ResourceKeys.JVM_TOTAL_MEMORY_MB()));
        System.out.println(serverResourceInfo.get(ResourceKeys.JVM_MAX_MEMORY_MB()));
        System.out.println(serverResourceInfo.get(ResourceKeys.SYSTEM_MEMORY_FREE_MB()));
        System.out.println(serverResourceInfo.get(ResourceKeys.SYSTEM_MEMORY_USED_MB()));
        System.out.println(serverResourceInfo.get(ResourceKeys.SYSTEM_MEMORY_TOTAL_MB()));

        //打开非结构化数据的文件列表数据帧
        DataFrame dfBin = dc.get("/bin");

        //获得数据帧的Document，包含由Provider定义的SchemaURI等信息
        //用户可以控制没有信息时输出的字段
        System.out.println("--------------打印数据帧Document--------------");
//        DataFrameDocument dataFrameDocument = ((RemoteDataFrameProxy) dfBin).getDocument();
//        String schemaURL = convertToJavaOptional(dataFrameDocument.getSchemaURL()).orElse("schemaURL not found");
//        String columnURL = convertToJavaOptional(dataFrameDocument.getColumnURL("file_name")).orElse("columnURL not found");
//        String columnAlias = convertToJavaOptional(dataFrameDocument.getColumnAlias("file_name")).orElse("columnAlias not found");
//        String columnTitle = convertToJavaOptional(dataFrameDocument.getColumnTitle("file_name")).orElse("columnTitle not found");
//        System.out.println(schemaURL);
//        System.out.println(columnURL);
//        System.out.println(columnAlias);
//        System.out.println(columnTitle);
//        System.out.println(dfBin.schema());

        //获得数据帧大小
        System.out.println("--------------打印数据帧大小--------------");
//        Long dataFrameSize = ((RemoteDataFrameProxy) dfBin).getStatistics().byteSize();
//        Long dataFrameRowCount = ((RemoteDataFrameProxy) dfBin).getStatistics().rowCount();
//        System.out.println(dataFrameSize);
//        System.out.println(dataFrameRowCount);

        //可以对数据帧进行操作 比如foreach 每行数据为一个Row对象，可以通过Tuple风格访问每一列的值
        System.out.println("--------------打印非结构化数据文件列表数据帧--------------");
        dfBin.foreach(row -> {
            //通过Tuple风格访问
            String name = (String) row._1();
            //通过下标访问
            Blob blob = (Blob) row.get(6);
            //除此之外列值支持的类型还包括：Integer, Long, Float, Double, Boolean, byte[]
            //offerStream用于接受一个用户编写的处理blob InputStream的函数并确保其关闭
            blob.offerStream(inputStream -> {
                try {
                    FileOutputStream outputStream = new FileOutputStream(Paths.get("src", "test", "demo", "data", "output", name).toFile());
                    IOUtils.copy(inputStream, outputStream);
                    outputStream.close();
                    return null;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            System.out.println(row);
            System.out.println(name);
            return null;
        });

        //获取数据
        //对数据进行collect操作可以将数据帧的所有行收集到内存中，但是要注意内存溢出的问题
        //limit操作可以限制返回的数据行数，防止内存溢出
        //还可以打开CSV文件数据帧
        DataFrame dfCsv = dc.get("/csv/data_1.csv");
        List<Row> rowsCollect = convertToJavaList(dfCsv.limit(1).collect());
        System.out.println("--------------打印结构化数据 /csv/data_1.csv 数据帧--------------");
        for (Row row : rowsCollect) {
            System.out.println(row);
        }

        //编写map算子的匿名函数对数据帧进行操作
        //Function1是scala函数类接口，需要实现apply方法并在其中编写函数体
        //SerializableFunction是FairdClient提供的序列化函数接口，继承自Function1
        Function1<Row, Row> mapFunction = new SerializableFunction<Row, Row>() {

            @Override
            public Row apply(Row v1) {
                return Row.apply(convertToScalaSeq(Arrays.asList(v1._1())));
            }

            //两个函数的从右到左组合，一般不用特殊处理
            @Override
            public <A> Function1<A, Row> compose(Function1<A, Row> g) {
                return SerializableFunction.super.compose(g);
            }

            //两个函数的从左到右组合，一般不用特殊处理
            @Override
            public <A> Function1<Row, A> andThen(Function1<Row, A> g) {
                return SerializableFunction.super.andThen(g);
            }
        };
        List<Row> rowsMap = convertToJavaList(dfCsv.map(mapFunction).collect());
        System.out.println("--------------打印结构化数据 /csv/data_1.csv 经过map操作后的数据帧--------------");
        for (Row row : rowsMap.stream().limit(10).collect(Collectors.toList())) {
            System.out.println(row);
        }

        //编写filter算子的匿名函数对数据帧进行操作
        Function1<Row, Object> filterFunction = new SerializableFunction<Row, Object>() {

            @Override
            public Object apply(Row row) {
                long id = (long) row._1();
                return id <= 1;
            }

            @Override
            public <A> Function1<A, Object> compose(Function1<A, Row> g) {
                return SerializableFunction.super.compose(g);
            }

            @Override
            public <A> Function1<Row, A> andThen(Function1<Object, A> g) {
                return SerializableFunction.super.andThen(g);
            }
        };
        List<Row> rowsFilter = convertToJavaList(dfCsv.filter(filterFunction).collect());
        System.out.println("--------------打印结构化数据 /csv/data_1.csv 经过filter操作后的数据帧--------------");
        for (Row row : rowsFilter) {
            System.out.println(row);
        }

        //select可以通过列名得到指定列的数据
        List<Row> selectedRows = convertToJavaList(dfCsv.select("id").collect());
        System.out.println("--------------打印结构化数据 /csv/data_1.csv 经过select操作后的数据帧--------------");
        for (Row row : selectedRows.stream().limit(10).collect(Collectors.toList())) {
            System.out.println(row);
        }

        //自定义算子和DAG执行图对数据帧进行操作
        //构建数据源节点
        FlowNode sourceNodeA = new SourceNode("/csv/data_1.csv");
        FlowNode sourceNodeB = new SourceNode("/csv/data_2.csv");
        //构建自定义算子节点对象
        //自定义一个map算子 比如对第一列加1
        FlowNode udfMap = new Transformer11() {
            @Override
            public DataFrame transform(DataFrame dataFrame) {
                return dataFrame.map(row -> {
                    long value = (long) row._1();
                    return Row.fromJavaList(Arrays.asList(value + 1, row._2()));
                });
            }
        };
        //自定义一个过滤算子 比如只保留小于等于3的行
        FlowNode udfFilter2 = new Transformer11() {
            @Override
            public DataFrame transform(DataFrame dataFrame) {
                return dataFrame.filter(row -> {
                    long value = (long) row._1();
                    return value <= 3;
                });
            }
        };

        FlowNode udfFilter = new Transformer11() {
            @Override
            public DataFrame transform(DataFrame dataFrame) {
                scala.collection.Iterator<Row> newRow = ((DefaultDataFrame)dataFrame).stream();


                return DataUtils.getDataFrameByStream(newRow);
            }

        };

        //构建节点Map，节点名对应节点对象，可以是数据源节点或者自定义算子节点
        //至少一个节点
        Map<String, FlowNode> javaNodesMapMin = new HashMap<>();
        javaNodesMapMin.put("A", sourceNodeA);
        //构建边Map，可以没有边，对应不进行操作
        Map<String, List<String>> javaEdgesMapMin = new HashMap<>();
        //通过边和节点Map构建DAG执行图
        Flow flowMin = Flow.apply(convertToScalaNodesMap(javaNodesMapMin), convertToScalaEdgesMap(javaEdgesMapMin));
        //执行DAG图，返回一个数据帧列表
        List<DataFrame> minDAGDfs = new ArrayList<>(dc.execute(flowMin).map().values());
        System.out.println("--------------打印最小DAG直接获取的数据帧--------------");
        for (DataFrame df : minDAGDfs) {
            df.limit(3).foreach(row ->
            {
                System.out.println(row);
                return null;
            });
        }
        //对于线性依赖也可以简化为通过节点链pipe构造DAG
        Flow flowSeq = Flow.pipe(new SourceNode("/csv/data_1.csv"));
        List<DataFrame> seqDAGDfs = new ArrayList<>(dc.execute(flowSeq).map().values());
        System.out.println("--------------打印自定义filter算子操作后的数据帧--------------");
        for (DataFrame df : seqDAGDfs) {
            df.limit(3).foreach(row ->
            {
                System.out.println(row);
                return null;
            });
        }

        //构建节点Map，节点名对应节点对象，可以是数据源节点或者自定义算子节点
        Map<String, FlowNode> javaNodesMap = new HashMap<>();
        javaNodesMap.put("A", sourceNodeA);
        javaNodesMap.put("B", udfFilter);
        //构建边Map，一个节点可以有多个下游节点
        Map<String, List<String>> javaEdgesMap = new HashMap<>();
        javaEdgesMap.put("A", Arrays.asList("B"));
        //通过边和节点Map构建DAG执行图
        Flow flow = Flow.apply(convertToScalaNodesMap(javaNodesMap), convertToScalaEdgesMap(javaEdgesMap));
        List<DataFrame> simpleDfs = new ArrayList<>(dc.execute(flow).map().values());
        System.out.println("--------------打印自定义filter算子操作后的数据帧--------------");
        for (DataFrame df : simpleDfs) {
            df.limit(3).foreach(row ->
            {
                System.out.println(row);
                return null;
            });
        }

        //可以构建更复杂的多数据源节点和操作的DAG
        //多对一
        //  A   B
        //   \ /
        //    C
        Flow transformerMergeDAG = Flow.apply(convertToScalaNodesMap(
                new HashMap<String, FlowNode>() {{
            put("A", sourceNodeA);
            put("B", sourceNodeB);
            put("C", udfFilter);
        }}), convertToScalaEdgesMap(
                new HashMap<String, List<String>>() {{
            put("A", Arrays.asList("C"));
            put("B", Arrays.asList("C"));
        }}));
        List<DataFrame> mergeDfs = new ArrayList<>(dc.execute(transformerMergeDAG).map().values());
        System.out.println("--------------打印执行自定义DAG后的数据帧--------------");
        for (DataFrame df : mergeDfs) {
            df.limit(3).foreach(row ->
            {
                System.out.println(row);
                return null;
            });
        }

        //多对一
        //   A
        //  / \
        // B   C
        Flow transformerForkDAG = Flow.apply(convertToScalaNodesMap(
                new HashMap<String, FlowNode>() {{
                    put("A", sourceNodeA);
                    put("B", udfMap);
                    put("C", udfFilter);
                }}), convertToScalaEdgesMap(
                new HashMap<String, List<String>>() {{
                    put("A", Arrays.asList("B","C"));
                }}));
        List<DataFrame> forkDfs = new ArrayList<>(dc.execute(transformerForkDAG).map().values());
        System.out.println("--------------打印执行自定义DAG后的数据帧--------------");
        for (DataFrame df : forkDfs) {
            df.limit(3).foreach(row ->
            {
                System.out.println(row);
                return null;
            });
        }

        //多对多
        //   A  B
        //   |/\|
        //   C  D
        Flow transformerComplexDAG = Flow.apply(convertToScalaNodesMap(
                new HashMap<String, FlowNode>() {{
                    put("A", sourceNodeA);
                    put("B", sourceNodeB);
                    put("C", udfFilter);
                    put("D", udfMap);
                }}), convertToScalaEdgesMap(
                new HashMap<String, List<String>>() {{
                    put("A", Arrays.asList("C","D"));
                    put("B", Arrays.asList("C","D"));
                }}));
        List<DataFrame> complexDfs = new ArrayList<>(dc.execute(transformerComplexDAG).map().values());
        System.out.println("--------------打印执行自定义DAG后的数据帧--------------");
        for (DataFrame df : complexDfs) {
            df.limit(3).foreach(row ->
            {
                System.out.println(row);
                return null;
            });
        }

    }



    public static scala.collection.immutable.Map<String, FlowNode> convertToScalaNodesMap(Map<String, FlowNode> javaMap) {
        return JavaConverters.mapAsScalaMapConverter(javaMap).asScala().toMap(
                scala.Predef.$conforms() // This provides evidence for implicit conversion to scala.Tuple2
        );
    }

    public static scala.collection.immutable.Map<String, Seq<String>> convertToScalaEdgesMap(Map<String, List<String>> javaMap) {
        return JavaConverters.mapAsScalaMapConverter(javaMap).asScala().toMap(scala.Predef.$conforms()).mapValues(
                list -> JavaConverters.asScalaBufferConverter(list).asScala().toSeq()
        );
    }

    public static <T> java.util.List<T> convertToJavaList(Seq<T> scalaSeq) {
        return JavaConverters.seqAsJavaListConverter(scalaSeq).asJava();
    }

    public static <T> Seq<T> convertToScalaSeq(java.util.List<T> javaList) {
        return JavaConverters.asScalaBufferConverter(javaList).asScala().toSeq();
    }

    public static <T> java.util.Optional<T> convertToJavaOptional(Option<T> scalaOption) {
        return Optional.ofNullable(scalaOption.getOrElse(null));
    }

}

