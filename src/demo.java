import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import pers.jues.SQL.MySQL;

public class demo {

	public static void main(String[] args) {
		MySQL mysql = new MySQL("172.16.2.147", 3306, "test", "", "test", "123456");
		//
		try {
			List<Map<String, Object>> res;
			Map<String, Object> row;
			String table = "user";
			//
			System.out.println("insert-------------------------------------------");
			row = new HashMap<String, Object>();
			row.put("name", "jues笔记");
			row.put("url", "http://note.jues.org.cn");
			// insert
			long id = mysql.insert(table, row);
			// select
			res = mysql.select(table, null, null, null);

			System.out.println(res);

			System.out.println("update-------------------------------------------");
			row = new HashMap<String, Object>();
			row.put("name", "Java简单操作MySQL");
			row.put("url", "http://note.jues.org.cn/node/137");

			// update
			mysql.update(table, row, "`id`=" + String.valueOf(id));
			// select
			res = mysql.select(table, null, null, null);

			System.out.println(res);

			System.out.println("delete-------------------------------------------");
			// delete
			mysql.delete(table, "`id`=" + String.valueOf(id));
			// select
			res = mysql.select(table, null, null, null);

			System.out.println(res);

		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
