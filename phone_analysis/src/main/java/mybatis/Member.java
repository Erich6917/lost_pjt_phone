package mybatis;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * @Author Erich ErichLee@qq.com
 * @Date 2019年8月30日
 * @Comment
 * 
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class Member {
	private String member_id; // 主键 id
	private String member_name; // 姓名
	private String created_by;
	private String created_date;
	private String updated_by;
	private String updated_date;

	@Override
	public String toString() {
		return "Member{" + "id=" + member_id + ", name='" + member_name + "}";
	}
}
