/**
 * @Author Yomi
 * @Description:
 * @Data 2025/7/15 10:57
 * @Modified By:
 */
package link.rdcn;

import link.rdcn.received.DataReceiver;
import link.rdcn.server.dacp.DacpServer;
import link.rdcn.struct.DataFrame;

public class JServerDemo {
    public static void main(String[] args) {
        TestDemoProvider provider = new TestDemoProvider();
        //根据fairdHome自动读取配置文件
        DacpServer server = new DacpServer(provider.dataProvider(), new DataReceiver() {
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
        },provider.authProvider());
        server.start(ConfigLoader.fairdConfig());
    }
}
