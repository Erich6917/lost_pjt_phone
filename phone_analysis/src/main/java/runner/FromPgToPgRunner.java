//package runner;
//
//import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
//import org.apache.hadoop.mapreduce.lib.db.DBWritable;
//
//import lombok.AllArgsConstructor;
//import lombok.Getter;
//import lombok.NoArgsConstructor;
//import lombok.Setter;
//
//import java.io.DataInput;
//import java.io.DataOutput;
//import java.io.IOException;
//import java.sql.PreparedStatement;
//import java.sql.ResultSet;
//import java.sql.SQLException;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.client.Result;
//import org.apache.hadoop.hbase.client.Scan;
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
//import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
//import org.apache.hadoop.hbase.mapreduce.TableMapper;
//import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.NullWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.Writable;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Reducer;
//
///**
// * @Author Erich ErichLee@qq.com
// * @Date 2019年8月31日
// * @Comment
// * 
// */
//public class FromPgToPgRunner {
//	@Getter
//	@Setter
//	@NoArgsConstructor
//	@AllArgsConstructor
//	private static class MysqlRecord implements DBWritable, Writable {
//		private String member_id; // 主键 id
//		private String member_name; // 姓名
//		private String created_by;
//		private String created_date;
//		private String updated_by;
//		private String updated_date;
//
//		@Override
//		public void write(PreparedStatement statement) throws SQLException {
//			statement.setString(1, this.getMember_id());
//			statement.setString(2, this.getMember_name());
//		}
//
//		@Override
//		public void readFields(ResultSet resultSet) throws SQLException {
//			this.member_id = resultSet.getString(1);
//			this.member_name = resultSet.getString(2);
//		}
//
//		@Override
//		public void readFields(DataInput in) throws IOException {
//			this.member_id = in.readUTF();
//			this.member_name = in.readUTF();
//		}
//
//		@Override
//		public void write(DataOutput out) throws IOException {
//			out.writeUTF(this.member_id);
//			out.writeUTF(this.member_name);
//		}
//
//		@Override
//		public String toString() {
//			return "DBWritable Member{" + "id=" + member_id + ", name='" + member_name + "}";
//		}
//
//	}
//
//	private static class MySQLStatistic implements DBWritable, Writable {
//		private Date date;
//		private int nums;
//
//		MySQLStatistic(Date date, int nums) {
//			this.date = date;
//			this.nums = nums;
//		}
//
//		MySQLStatistic(String date, int nums) {
//			this.date = Date.valueOf(date);
//			this.nums = nums;
//		}
//
//		@Override
//		public void write(DataOutput dataOutput) throws IOException {
//			Text.writeString(dataOutput, date.toString());
//			dataOutput.writeInt(nums);
//		}
//
//		@Override
//		public void readFields(DataInput dataInput) throws IOException {
//			date = Date.valueOf(Text.readString(dataInput));
//			nums = dataInput.readInt();
//		}
//
//		@Override
//		public void write(PreparedStatement preparedStatement) throws SQLException {
//			preparedStatement.setDate(1, date);
//			preparedStatement.setInt(2, nums);
//		}
//
//		@Override
//		public void readFields(ResultSet resultSet) throws SQLException {
//			date = resultSet.getDate(1);
//			nums = resultSet.getInt(2);
//		}
//	}
//
//	private static class SQLMapper extends Mapper<LongWritable, MysqlRecord, Text, IntWritable> {
//		@Override
//		protected void map(LongWritable key, MysqlRecord value, Context context)
//				throws IOException, InterruptedException {
//			Date d = value.time;
//			int output = value.output_speed;
//			context.write(new Text(d.toString()), new IntWritable(output));
//		}
//	}
//
//	private static class SQLReducer extends Reducer<Text, IntWritable, MySQLStatistic, NullWritable> {
//		@Override
//		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
//				throws IOException, InterruptedException {
//			int sum = 0;
//			for (IntWritable v : values) {
//				sum += v.get();
//			}
//			MySQLStatistic res = new MySQLStatistic(key.toString(), sum);
//			context.write(res, NullWritable.get());
//		}
//	}
//
//	public static void main(String[] args) throws Exception {
//		Configuration conf = new Configuration();
//		DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306/microblog_spider",
//				"root", "admin");
//
//		Job job = Job.getInstance(conf, "mysql test");
//		job.setJarByClass(mysql_test.class);
//		job.setMapperClass(SQLMapper.class);
//		job.setReducerClass(SQLReducer.class);
//
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(IntWritable.class);
//		job.setOutputKeyClass(MySQLStatistic.class);
//		job.setOutputValueClass(NullWritable.class);
//		job.setInputFormatClass(DBInputFormat.class);
//		job.setOutputFormatClass(DBOutputFormat.class);
//
//		String[] fields = { "size", "time", "input_speed", "output_speed" };
//		DBInputFormat.setInput(job, // job
//				MysqlRecord.class, // input class
//				"proxy_table", // table name
//				null, // condition
//				"time", // order by
//				fields); // fields
//
//		DBOutputFormat.setOutput(job, // job
//				"hadoop_out", // output table name
//				"date", "nums" // fields
//		);
//
//		job.waitForCompletion(true);
//	}
//
//}
