package runner;

 /**  
 * @Author	Erich ErichLee@qq.com    
 * @Date	2019年9月2日 
 * @Comment	 
 *            
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by multiangle on 16-11-12.
 */

public class mysql_test_demo {
    private static class MysqlRecord implements DBWritable,Writable {
        protected int size ;
        protected Date time ;
        protected int input_speed ;
        protected int output_speed ;

        @Override
        public void write(PreparedStatement preparedStatement) throws SQLException {
            preparedStatement.setInt(1,this.size);
            preparedStatement.setDate(2,this.time);
            preparedStatement.setInt(3,this.input_speed);
            preparedStatement.setInt(4,this.output_speed);
        }

        @Override
        public void readFields(ResultSet resultSet) throws SQLException {
            this.size = resultSet.getInt(1) ;
            this.time = resultSet.getDate(2) ;
            this.input_speed = resultSet.getInt(3) ;
            this.output_speed = resultSet.getInt(4) ;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeInt(this.size);
            Text.writeString(dataOutput, this.time.toString()) ;
            dataOutput.writeInt(this.input_speed);
            dataOutput.writeInt(this.output_speed);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.size = dataInput.readInt() ;
            this.time = Date.valueOf(Text.readString(dataInput)) ;
            this.input_speed = dataInput.readInt() ;
            this.output_speed = dataInput.readInt() ;
        }

        @Override
        public String toString() {
            return String.format("%d\t%s\t%d\t%d",size,time.toString(),input_speed,output_speed);
        }
    }

    private static class MySQLStatistic implements DBWritable,Writable{
        private Date date ;
        private int nums ;

        MySQLStatistic(Date date, int nums){
            this.date = date ;
            this.nums = nums ;
        }
        MySQLStatistic(String date, int nums){
            this.date = Date.valueOf(date) ;
            this.nums = nums ;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            Text.writeString(dataOutput,date.toString()) ;
            dataOutput.writeInt(nums);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            date = Date.valueOf(Text.readString(dataInput)) ;
            nums = dataInput.readInt() ;
        }

        @Override
        public void write(PreparedStatement preparedStatement) throws SQLException {
            preparedStatement.setDate(1, date);
            preparedStatement.setInt(2, nums);
        }

        @Override
        public void readFields(ResultSet resultSet) throws SQLException {
            date = resultSet.getDate(1) ;
            nums = resultSet.getInt(2) ;
        }
    }

    private static class SQLMapper extends Mapper<LongWritable, MysqlRecord, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, MysqlRecord value, Context context)
                throws IOException, InterruptedException {
            Date d = value.time ;
            int output = value.output_speed ;
            context.write(new Text(d.toString()),new IntWritable(output));
        }
    }

    private static class SQLReducer extends Reducer<Text, IntWritable, MySQLStatistic, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0 ;
            for (IntWritable v:values){
                sum += v.get() ;
            }
            MySQLStatistic res = new MySQLStatistic(key.toString(),sum) ;
            context.write(res, NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration() ;
        DBConfiguration.configureDB(
                conf, "com.mysql.jdbc.Driver",
                "jdbc:mysql://localhost:3306/microblog_spider",
                "root","admin");


        Job job = Job.getInstance(conf,"mysql test") ;
        job.setJarByClass(mysql_test_demo.class);
        job.setMapperClass(SQLMapper.class);
        job.setReducerClass(SQLReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(MySQLStatistic.class);
        job.setOutputValueClass(NullWritable.class);
        job.setInputFormatClass(DBInputFormat.class);
        job.setOutputFormatClass(DBOutputFormat.class);

        String[] fields = {"size","time","input_speed","output_speed"} ; 
        DBInputFormat.setInput(
                job,                // job
                MysqlRecord.class,  // input class
                "proxy_table",      // table name
                null,               // condition
                "time",             // order by
                fields);            // fields

        DBOutputFormat.setOutput(
                job,                // job
                "hadoop_out",       // output table name
                "date","nums"       // fields
        );

        job.waitForCompletion(true) ;
    }

}
