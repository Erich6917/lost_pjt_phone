项目流程



1、准备、本地打包测试

​	项目右键>run as > Maven clean

​	项目右键>run as > Maven build

​	本地测试 java -cp phone_producer-0.0.1.jar org.phone_producer.producer.ProductLog

​	上传至linux服务器，同样的命令测试，同级目录生成calllog.csv文件，标识测试成功

2、运行jar包后，模拟生产数据不断生成过程，生成calllog.csv

3、启动zookeeper集群

4、启动kafka集群

​	topic由flume启动时创建，kafka启动时不用单独启动

```
bin/kafka-console-consumer.sh
	--bootstrap-server bigdata121:9092 --from-beginning --topic calllog
```

3、启动flume采集数据

flume启动单台即可

​      配置主要信息如下

```
	a1.sources.r1.command = tail -F -c +0 /opt/moudle/lost/phone/source/calllog.csv
​	a1.sinks.k1.type = org.apache.flume.sink.kafka.KafkaSink
​	a1.sinks.k1.brokerList = master:9092,bigdata121:9092,bigdata122:9092
​	a1.sinks.k1.topic = calllog
```

​	启动后自动创建topic  calllog

4、kafka单出口消费topic

```
flume-ng agent --conf /opt/moudle/flume-1.7.0/conf/ --name a1 --conf-file /opt/moudle/lost/phone/config/flume-kafka.conf
```

​	一切正常可以看到同步信息生成

5、java代码实现，代替kafka窗口实现消费topic，实现控制台打印信息

​	HBaseConsumer

6、java代码实现，将打印的信息同步到hbase

7、hbase信息内容连接pg数据库





业务指标:

​	a) 用户每天主叫通话个数统计，通话时间统计。

​                      读取Hbase数据，MR计算，显示，保存至pg

​        b) 用户每月通话记录统计，通话时间统计。



​        c) 用户之间亲密关系统计。（通话次数与通话时间体现用户亲密关系）









​	