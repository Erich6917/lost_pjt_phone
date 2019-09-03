package mybatis;

import java.io.InputStream;

import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.junit.Test;

/**
 * @Author Erich ErichLee@qq.com
 * @Date 2019年8月30日
 * @Comment
 * 
 */
public class MybatisTest {
	@Test
	public void findMemberByIdTest() throws Exception {
		// 1.读取配置文件
		String resource = "mybatis-config.xml";
		InputStream inputStream = Resources.getResourceAsStream(resource);
		// 2.根据配置文件构建 SqlSessionFactory
		SqlSessionFactory sqlSessionFactory = new SqlSessionFactoryBuilder().build(inputStream);
		// 3.通过 SqlSessionFactory 创建 SqlSession
		SqlSession sqlSession = sqlSessionFactory.openSession();
		// 4.SqlSession 执行映射文件中定义的 SQL ，并返回映射结果
		Member member = sqlSession.selectOne("findMemberById", "1");
		// 打印输出结果
		System.out.println(member.toString());
		// 5.关闭 SqlSession
		sqlSession.close();
	}
}
