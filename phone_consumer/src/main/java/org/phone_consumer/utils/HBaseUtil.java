package org.phone_consumer.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.TreeSet;

public class HBaseUtil {

    /**
     * 初始化命名空间
     *
     * @param conf
     * @param namespace
     */
    public static void initNameSpace(Configuration conf, String namespace) throws IOException {
        //获取connection
        Connection connection = ConnectionFactory.createConnection(conf);
        //获取admin对象
        Admin admin = connection.getAdmin();

        //创建命名空间
        NamespaceDescriptor nd = NamespaceDescriptor
                .create(namespace)
                //add配置不强制加
                .addConfiguration("create_time", String.valueOf(System.currentTimeMillis()))
                .build();

        //通过admin对象创建namespace
        admin.createNamespace(nd);

        //关闭对象
        close(admin, connection);
    }

    /**
     * 初始化表
     *
     * @param conf
     * @param tableName
     * @param regions
     * @param columnFamily
     */
    public static void createTable(Configuration conf, String tableName, int regions, String... columnFamily) throws IOException {
        //获取connection
        Connection connection = ConnectionFactory.createConnection(conf);
        //获取admin对象
        Admin admin = connection.getAdmin();

        //如果表已存在，就返回
        if (isExistTable(conf, tableName)) {
            return;
        }

        //创建表对象
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));

        for (String cf : columnFamily) {
            htd.addFamily(new HColumnDescriptor(cf));
        }

        //添加协处理器的全类名
//        htd.addCoprocessor("hbase.CalleeWriteObserver");
        //通过admin创建表(htd(列簇),分裂得regions)
        admin.createTable(htd, genSplitKeys(regions));

        //关闭
        close(admin, connection);
    }

    /**
     * 分区
     *
     * @param regions
     * @return
     */
    private static byte[][] genSplitKeys(int regions) {
        //第一步:定义分区键数组 keys:00| 01| 02| 03| 04| 05|
        String[] keys = new String[regions];
        //分区位数“格式化”
        DecimalFormat df = new DecimalFormat("00");

        //00| 01| 02| 03| 04| 05|
        for (int i = 0; i < regions; i++) {
            keys[i] = df.format(i) + "|";
        }

        //第二步
        byte[][] splitsKeys = new byte[regions][];
        //分区键有序
        TreeSet<byte[]> treeSet = new TreeSet<>(Bytes.BYTES_COMPARATOR);
        for (int i = 0; i < regions; i++) {
            treeSet.add(Bytes.toBytes(keys[i]));
        }


        //第三步
        Iterator<byte[]> splitKeysIterator = treeSet.iterator();
        int index = 0;
        while (splitKeysIterator.hasNext()) {
            byte[] next = splitKeysIterator.next();
            splitsKeys[index++] = next;
        }

        return splitsKeys;
    }

    /**
     * 判断表是否存在
     *
     * @param conf
     * @param tableName
     */
    public static boolean isExistTable(Configuration conf, String tableName) throws IOException {
        //获取connection
        Connection connection = ConnectionFactory.createConnection(conf);
        //获取admin对象
        Admin admin = connection.getAdmin();
        //判断表API
        boolean b = admin.tableExists(TableName.valueOf(tableName));
        //关闭
        close(admin, connection);
        return b;
    }

    /**
     * 关闭
     *
     * @param admin
     * @param connection
     */
    public static void close(Admin admin, Connection connection) throws IOException {
        if (admin != null) {
            admin.close();
        }

        if (connection != null) {
            connection.close();
        }

    }

    /**
     * regionCode, caller. buildTime, callee. flag, duration
     * rowkey前的离散串，caller. buildTime, callee. flag, duration
     * <p>
     * 主叫（flag:1）：19379884788,17325302007,2019-03-01 20:48:57,1584    ===> f1 列簇
     * 被叫（flag:0）：17325302007，19379884788,2019-03-01 20:48:57,1584   ===> f2 列簇
     * 面试常问rowkey相关的问题：你们公司如何设计的rowKey? 怎么设计rowKey才能避免热点问题？
     *
     * @param regionCode
     * @param caller
     * @param callee
     * @param duration
     * @return
     */
    public static String genRowkey(String regionCode, String caller, String buildTime, String callee,String flag, String duration) {
        StringBuilder sb = new StringBuilder();
        sb.append(regionCode + "_")
                .append(caller + "_")
                .append(buildTime + "_")
                .append(callee + "_")
                .append(flag + "_")
                .append(duration);
        return sb.toString();
    }

    /**
     * 当数据进入HBase的Region的时候足够的离散
     *
     * @param caller
     * @param buildTime
     * @param regions
     */
    public static String  genRegionCode(String caller, String buildTime, int regions) {
        //取出主叫的后四位 lastPhone：caller的后四位
        String lastPhone = caller.substring(caller.length() - 4);

        //取出年月  从2019-03-01 20:48:57中取出年月 ，
        String yearMonth = buildTime.replaceAll("-", "").substring(0, 6);

        //离散操作1：做异或处理 ^
        Integer x = Integer.valueOf(lastPhone) ^ Integer.valueOf(yearMonth);

        //离散操作2：把离散1的值再做hashcode
        int y = x.hashCode();

        //最终想要的分区号
        int regionCode = y % regions;

        DecimalFormat df = new DecimalFormat("00");

        return df.format(regionCode);
    }


}
