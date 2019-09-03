package outputformat;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import mybatis.MemberPhone;

/**
 * @Author Erich ErichLee@qq.com
 * @Date 2019年9月3日
 * @Comment pg连接mybatis 入库
 * 
 */
public class PgOutputFormat extends OutputFormat<MemberPhone, NullWritable> {
	private OutputCommitter committer = null;

	@Override
	public RecordWriter<MemberPhone, NullWritable> getRecordWriter(TaskAttemptContext context) {
		// 初始化JDBC连接器对象
		return new PgRecordWriter();
	}

	@Override
	public void checkOutputSpecs(JobContext context) throws InterruptedException {
		// 输出校检
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {
		// 此方法点击Outputformat按ctrl + F10 的源码，复制getOutputCommitter，在FlieOutputFormat
		if (committer == null) {
			String name = context.getConfiguration().get(FileOutputFormat.OUTDIR);
			Path outputPath = name == null ? null : new Path(name);
			committer = new FileOutputCommitter(outputPath, context);
		}
		return committer;
	}

	static class PgRecordWriter extends RecordWriter<MemberPhone, NullWritable> {

		@Override
		public void write(MemberPhone key, NullWritable value) throws IOException, InterruptedException {
			String resource = "mybatis-config.xml";
			InputStream inputStream = Resources.getResourceAsStream(resource);
			// 2.根据配置文件构建 SqlSessionFactory
			SqlSessionFactory sqlSessionFactory = new SqlSessionFactoryBuilder().build(inputStream);
			// 3.通过 SqlSessionFactory 创建 SqlSession
			SqlSession sqlSession = sqlSessionFactory.openSession();
			// 4.SqlSession 执行映射文件中定义的 SQL ，并返回映射结果
			MemberPhone member = sqlSession.selectOne("saveMember", key);
			// 打印输出结果
			System.out.println(member.toString());
			// 5.关闭 SqlSession
			sqlSession.close();
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
		}

	}
}
