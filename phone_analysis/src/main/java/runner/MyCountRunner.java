package runner;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import kv.key.ComDimension;
import kv.key.ContactDimension;
import kv.key.DateDimension;
import kv.value.CountDurationValue;
import mapper.CountDurationMapper;

/**
 * @Author Erich ErichLee@qq.com
 * @Date 2019年8月29日
 * @Comment 读取hbase内容显示打印即可，为了数据显示或存储做准备
 * 
 */
public class MyCountRunner implements Tool {
	private Configuration conf = null;

	@Override
	public void setConf(Configuration conf) {
		this.conf = HBaseConfiguration.create(conf);
	}

	@Override
	public Configuration getConf() {
		return this.conf;
	}

	@Override
	public int run(String[] args) throws Exception {
		// 得到conf
		Configuration conf = this.getConf();
		// 实例化Job
		Job job = Job.getInstance(conf);
		job.setJarByClass(CountDurationRunner.class);
		// 组装Mapper InputForamt
		initHBaseInputConfig(job);
		// 组装Reducer Outputformat
//        initReducerOutputConfig(job);

		FileOutputFormat.setOutputPath(job, new Path(args[0]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	private void initHBaseInputConfig(Job job) {
		Connection connection = null;
		Admin admin = null;
		try {
			String tableName = "phone_calllog";
			connection = ConnectionFactory.createConnection(job.getConfiguration());
			admin = connection.getAdmin();
			if (!admin.tableExists(TableName.valueOf(tableName))) {
				throw new RuntimeException("无法找到目标表.");
			}
			Scan scan = new Scan();
			// 可以优化
			TableMapReduceUtil.initTableMapperJob(tableName, scan, CountDurationMapper.class, ComDimension.class,
					Text.class, job, true);

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (admin != null) {
					admin.close();
				}
				if (connection != null && !connection.isClosed()) {
					connection.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	public class MyCountMapper extends TableMapper<ComDimension, Text> {
		private ComDimension comDimension = new ComDimension();
		private Text durationText = new Text();
		private Map<String, String> phoneMap;

		@Override
		protected void map(ImmutableBytesWritable key, Result value, Context context)
				throws IOException, InterruptedException {
			// super.map(key, value, context);
			// 05_19902496992_20180312154840_15542823911_1_1288
			String rowkey = Bytes.toString(key.get());
			String[] splits = rowkey.split("_");
			if ("0".equals(splits[4])) {
				return;
			}

			// 聚合的是主叫数据
			String caller = splits[1];
			String callee = splits[3];
			String buildTime = splits[2];
			String duration = splits[5];
			durationText.set(duration);

			String year = buildTime.substring(0, 4);
			String month = buildTime.substring(4, 6);
			String day = buildTime.substring(6, 8);

			DateDimension yearDimension = new DateDimension(year, "-1", "-1");
			DateDimension monthDimension = new DateDimension(year, month, "-1");
			DateDimension dayDimension = new DateDimension(year, month, day);

			// 主叫callerContactDimension
			ContactDimension callerContactDimension = new ContactDimension(caller, phoneMap.get(caller));

			comDimension.setContactDimension(callerContactDimension);
			// 年
			comDimension.setDateDimension(yearDimension);
			context.write(comDimension, durationText);
			// 月
			comDimension.setDateDimension(monthDimension);
			context.write(comDimension, durationText);
			// 日
			comDimension.setDateDimension(dayDimension);
			context.write(comDimension, durationText);

			// 被叫calleeContactDimension
			ContactDimension calleeContactDimension = new ContactDimension(callee, phoneMap.get(callee));

			comDimension.setContactDimension(calleeContactDimension);
			// 年
			comDimension.setDateDimension(yearDimension);
			context.write(comDimension, durationText);
			// 月
			comDimension.setDateDimension(monthDimension);
			context.write(comDimension, durationText);
			// 日
			comDimension.setDateDimension(dayDimension);
			context.write(comDimension, durationText);
		}

	}

	public static class MyCountReducer extends Reducer<ComDimension, Text, ComDimension, CountDurationValue> {
		private CountDurationValue countDurationValue = new CountDurationValue();

		@Override
		protected void reduce(ComDimension key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// super.reduce(key, values, context);
			int callSum = 0;
			int callDuration = 0;
			for (Text t : values) {
				callSum++;
				callDuration += Integer.valueOf(t.toString());
			}
			countDurationValue.setCallSum(String.valueOf(callSum));
			countDurationValue.setCallDurationSum(String.valueOf(callDuration));

			context.write(key, countDurationValue);
		}
	}

	public static void main(String[] args) {
		try {
			int status = ToolRunner.run(new CountDurationRunner(), args);
			System.exit(status);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
