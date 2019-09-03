package utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

/**  
 * @Author : Erich ErichLee@qq.com    
 * @Date   : 2019年3月20日 
 * @Comment: 
 *            
 */
public class HBaseConn {
	private static final HBaseConn INSTANCE = new HBaseConn();
    private static  Configuration configuration; //hbase配置
    private static  Connection connection; //hbase connection
    private HBaseConn(){
        try{
            if (configuration==null){
                 configuration = HBaseConfiguration.create();
                 configuration.set("hbase.zookeeper.quorum","192.168.100.120:2181,192.168.100.121:2181,192.168.100.122:2181,");
//                 configuration.set("hbase.zookeeper.property.clientPort", "2181");
                 configuration.set("hbase.rpc.timeout", "24000000");
                 configuration.set("hbase.client.scanner.timeout.period", "24000000");
            }

        }catch (Exception e){
            e.printStackTrace();
        }
    }
    private  Connection getConnection(){
        if (connection==null || connection.isClosed()){
            try{
                connection = ConnectionFactory.createConnection(configuration);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        return connection;
    }
    public static Connection getHBaseConn(){
        return INSTANCE.getConnection();
    }
    public static Table getTable(String tableName) throws IOException {
        return INSTANCE.getConnection().getTable(TableName.valueOf(tableName));
    }
    public static void closeConn(){
        if (connection!=null){
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
