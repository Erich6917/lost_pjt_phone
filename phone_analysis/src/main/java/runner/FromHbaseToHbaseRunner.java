package runner;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FromHbaseToHbaseRunner {
	// 表中的数据是：
	// hello jack
	// hello world
	// hello tom
	// hello lmc
	// Text 单词 IntWritable是总数
	public static class HMapper extends TableMapper<Text, IntWritable> {
		// map端的输出的值是将单词 拆分 (hello 1) (hello 1)(jack 1) ...........
		IntWritable outval = new IntWritable(1);// 输出的每个单词数量都是1

		@Override
		protected void map(ImmutableBytesWritable key, Result value, Context context)
				throws IOException, InterruptedException {
//            // 1.不需要key值 因为key是row_key 表中自带的 不变的那个值
//            byte[] val = value.getValue("info".getBytes(), "word".getBytes());
//            String word = new String(val);
//            String[] split = word.split(" ");
//            for (String str : split) {
//                context.write(new Text(str), outval);
//            }

			String rowkey = Bytes.toString(key.get());

			context.write(new Text(rowkey), new IntWritable(1));
		}
	}

	public static class HReducer extends TableReducer<Text, IntWritable, NullWritable> {
		// 重写reduce方法
		private NullWritable outputKey = NullWritable.get();
		private Put outputValue;

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable value : values) {
//				count++;
				System.out.println(value.toString());
				count += value.get();
			}
			outputValue = new Put(Bytes.toBytes(key.toString()));
			outputValue.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("count"), Bytes.toBytes(String.valueOf(count)));
			context.write(outputKey, outputValue);
		}
	}

	public static void main(String[] args) throws Exception {
//		Configuration config=new Configuration();
//		config.addResource("hbase-site.xml");
//		config.set("hbase.zookeeper.quorum", "192.168.100.120:2181,192.168.100.121:2181,192.168.100.122:2181,");

//		Configuration configuration = new HBaseConfiguration.create(config); // hbase配置
		Configuration configuration = HBaseConfiguration.create();
		configuration.addResource("hbase-site.xml");
		configuration.set("hbase.zookeeper.quorum", "192.168.100.120:2181,192.168.100.121:2181,192.168.100.122:2181,");
//            configuration.set("hbase.zookeeper.property.clientPort", "2181");
		configuration.set("hbase.rpc.timeout", "24000000");
		configuration.set("hbase.client.scanner.timeout.period", "24000000");

		Job job = Job.getInstance(configuration);
		job.setJarByClass(FromHbaseToHbaseRunner.class);
		Scan scan = new Scan();
		TableMapReduceUtil.initTableMapperJob("phone_calllog", scan, HMapper.class, Text.class, IntWritable.class, job);

		job.setReducerClass(HReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Mutation.class);

		TableMapReduceUtil.initTableReducerJob("phone_call_out", HReducer.class, job);

		/**
		 * output phone_call_out > f1 > key,val
		 * 
		 */
		job.waitForCompletion(true);
	}
}
