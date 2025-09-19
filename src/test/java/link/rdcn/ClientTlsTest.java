package link.rdcn;

import link.rdcn.dacp.client.DacpClient
import link.rdcn.user.Credentials;
import scala.collection.Iterator;
import scala.collection.Seq;

/**
 * ServerTest Description
 *
 * @author 郭志斌
 * @version faird-java 1.0.0
 * <b> Creation Time:</b> 2025/7/14 14:10
 */
public class ClientTlsTest {
    public static void main(String[] args) {
        System.setProperty("javax.net.ssl.trustStore", "/Users/renhao/Downloads/faird-java-http2/src/main/resources/conf/faird");
        DacpClient dc = DacpClient.connectTLS("dacp://localhost:3101", Credentials.ANONYMOUS());
        Seq<String> stringSeq = dc.listDataSetNames();
        Iterator<String> iterator = stringSeq.iterator();

        System.out.println(stringSeq.size());

        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }
        scala.collection.immutable.Map<String, String> serverResourceInfo = dc.getServerResourceInfo();
        System.out.println(serverResourceInfo);
    }
}
