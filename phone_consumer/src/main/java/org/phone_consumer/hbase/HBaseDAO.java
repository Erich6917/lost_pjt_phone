package org.phone_consumer.hbase;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.phone_consumer.utils.ConnectionInstance;
import org.phone_consumer.utils.HBaseUtil;
import org.phone_consumer.utils.PropertiesUtil;

public class HBaseDAO {
    public static final Configuration CONF;
    private String namespace;
    private int regions;
    private String tableName;
    private HTable table;
    private Connection connection;
    private SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddHHmmss");

    //用来存放“一小堆30行”数据，用于优化
    private List<Put> cacheList = new ArrayList<>();

    static {
        CONF = HBaseConfiguration.create();
    }

    /**
     * alter + inster
     * 用于构造命名空间和表
     */
    public HBaseDAO() {
        try {
            namespace = PropertiesUtil.getProperty("hbase.calllog.namespace");
            tableName = PropertiesUtil.getProperty("hbase.calllog.tablename");
            regions = Integer.valueOf(PropertiesUtil.getProperty("hbase.calllog.regions"));

            if (!HBaseUtil.isExistTable(CONF, tableName)) {
                HBaseUtil.initNameSpace(CONF, namespace);
                HBaseUtil.createTable(CONF, tableName, regions, "f1", "f2");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * @param value 17325302007,18503558939,2019-02-05 18:13:53,0456
     */
    public void put(String value) {

        try {
            if (cacheList.size() == 0) {
                connection = ConnectionInstance.getConnection(CONF);
                table = (HTable) connection.getTable(TableName.valueOf(tableName));
                table.setAutoFlushTo(false);
                table.setWriteBufferSize(2 * 1024 * 1024);
            }

            //数组下标越界异常
            String[] splitValue = value.split(",");
            String caller = splitValue[0];
            String callee = splitValue[1];
            String buildTime = splitValue[2];
            String duration = splitValue[3];

            String regionCode = HBaseUtil.genRegionCode(caller, buildTime, regions);

            //这个变量用于插入到HBase的列中
            String buildTimeRe = sdf2.format(sdf1.parse(buildTime));
            //做为rowkey所需的参数
            String BuildTimeTS = null;

            BuildTimeTS = String.valueOf(sdf1.parse(buildTime).getTime());

            String roekey = HBaseUtil.genRowkey(regionCode, caller, BuildTimeTS, callee, "1", duration);


            Put put = new Put(Bytes.toBytes(roekey));
            //通过put对象添加rowkey和列值   参数说明：[(列簇：f1),(列名:caller),(列值：caller)]
            //快捷键：ctrl + d
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("caller"), Bytes.toBytes(caller));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("callee"), Bytes.toBytes(callee));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("buildTimeRe"), Bytes.toBytes(buildTimeRe));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("BuildTimeTS"), Bytes.toBytes(BuildTimeTS));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("flag"), Bytes.toBytes("1"));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("duration"), Bytes.toBytes(duration));

            //把rowkey、列簇、列名、列值放到到cacheList的对象中
            cacheList.add(put);

            if (cacheList.size() >= 30) {
                table.put(cacheList);
                table.flushCommits();

                table.close();
                cacheList.clear();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


}
