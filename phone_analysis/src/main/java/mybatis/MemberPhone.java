package mybatis;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * @Author Erich ErichLee@qq.com
 * @Date 2019年8月31日
 * @Comment
 * 
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class MemberPhone implements DBWritable, Writable {
	private String member_id; // 主键 id
	private String member_name; // 姓名
	private String created_by;
	private String created_date;
	private String updated_by;
	private String updated_date;

	@Override
	public String toString() {
		return "DBWritable Member{" + "id=" + member_id + ", name='" + member_name + "}";
	}

	@Override
	public void write(PreparedStatement statement) throws SQLException {
		statement.setString(1, this.getMember_id());
		statement.setString(2, this.getMember_name());
	}

	@Override
	public void readFields(ResultSet resultSet) throws SQLException {
		this.member_id = resultSet.getString(1);
		this.member_name = resultSet.getString(2);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.member_id);
		out.writeUTF(this.member_name);

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.member_id = in.readUTF();
		this.member_name = in.readUTF();

	}
}
