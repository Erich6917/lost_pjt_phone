package runner;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import mybatis.MemberPhone;
import outputformat.PgOutputFormat;

/**
 * 
 * @Author	Erich ErichLee@qq.com    
 * @Date	2019年9月3日 
 * @Comment	操作步骤	
 *  		从 HBase 》 PG
 *			读：需要 extends TableMapper
 *			写：正常 extends Reducer，但是实体类要 实现DBWritable, Writable
 *			另外是pg驱动问题，全路径执行如下
 *			/opt/moudle/hadoop-2.7.3/bin/yarn jar /opt/moudle/lost/phone/source/phone_analysis-0.0.1-SNAPSHOT.jar runner.FromHbaseToPgRunner -libjars /opt/moudle/hadoop-2.7.3/lib/postgresql-42.2.2.jar
 *
 */

public class FromHbaseToPgRunner2 extends Configured implements Tool {
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

	public static class PgReducer extends Reducer<Text, IntWritable, MemberPhone, NullWritable> {
		private MemberPhone member = new MemberPhone();

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable value : values) {
//				count++;
				System.out.println(value.toString());
				count += value.get();
			}
			member.setMember_id(key.toString());
			member.setMember_name(String.valueOf(count));

			context.write(member, null);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration configuration = HBaseConfiguration.create();
		configuration.addResource("hbase-site.xml");
		configuration.set("hbase.zookeeper.quorum", "192.168.100.120:2181,192.168.100.121:2181,192.168.100.122:2181,");
		configuration.set("hbase.rpc.timeout", "24000000");
		configuration.set("hbase.client.scanner.timeout.period", "24000000");
		int re = ToolRunner.run(configuration, new FromHbaseToPgRunner2(), args);
		System.exit(re);
	}

	@Override
	public int run(String[] args) throws Exception {
        Configuration configuration = this.getConf();
        //数据库配置
        DBConfiguration.configureDB(configuration, "org.postgresql.Driver","jdbc:postgresql://bigdata121:5432/lost","lostopr", "paic134");

		Job job = Job.getInstance(configuration);
		job.setJarByClass(FromHbaseToPgRunner2.class);
		Scan scan = new Scan();
		TableMapReduceUtil.initTableMapperJob("phone_calllog", scan, HMapper.class, Text.class, IntWritable.class, job);

		// 设置reduce数量，最少一个
		job.setNumReduceTasks(1);// in uber mode
		job.setReducerClass(PgReducer.class);
		job.setOutputKeyClass(MemberPhone.class);
		job.setOutputValueClass(NullWritable.class);
		
		
		job.setOutputFormatClass(PgOutputFormat.class);
//		DBOutputFormat.setOutput(job, "phone_member", "member_id", "member_name");
//		job.setOutputFormatClass(DBOutputFormat.class);

		job.waitForCompletion(true);

		return 0;
	}
}
