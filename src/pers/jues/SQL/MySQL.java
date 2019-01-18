package pers.jues.SQL;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.PreparedStatement;

public class MySQL {
	final String JDBS_DRIVER = "com.mysql.jdbc.Driver";
	String db_url;
	String username;
	String password;

	String table_pre;

	Connection conn = null;

	// MyData
	public MySQL(String host, int port, String database, String table_pre, String username, String password) {
		// init url
		this.db_url = String.format("jdbc:mysql://%s:%d/%s?useUnicode=true&characterEncoding=utf-8", host, port,
				database);

		// init username
		this.username = username;
		this.password = password;

		//
		this.table_pre = table_pre;
	}

	// connect
	public boolean connect() throws SQLException, ClassNotFoundException {

		// init driver
		Class.forName(this.JDBS_DRIVER);

		// connect
		this.conn = (Connection) DriverManager.getConnection(this.db_url, this.username, this.password);

		return true;
	}

	// reconnect
	public boolean reconnect() throws SQLException, ClassNotFoundException {
		if (null != this.conn && false == this.conn.isClosed()) {
			return true;
		}

		return this.connect();
	}

	// disconnect
	public void disconnect() throws SQLException {
		this.conn.close();
	}

	//
	PreparedStatement prepare(String sql) throws ClassNotFoundException, SQLException {
		if (false == this.reconnect()) {
			return null;
		}
		//
		PreparedStatement stmt = (PreparedStatement) this.conn.prepareStatement(sql);
		//
		return stmt;
	}

	//
	String fieldName2String(Set<String> keys) {
		if (null == keys || 0 >= keys.size()) {
			return "*";
		}

		String text = "";
		//
		for (String key : keys) {
			if (0 < text.length()) {
				text += ",";
			}
			//
			text += "`" + key + "`";
		}
		//
		return text;
	}

	//
	String fieldValue2String(Collection<Object> values) {
		String text = "";
		//
		for (@SuppressWarnings("unused")
		Object value : values) {
			if (0 < text.length()) {
				text += ",";
			}
			//
			text += "?";
		}
		//
		return text;
	}

	//
	int fieldValueSet(Collection<Object> values, PreparedStatement stmt) throws SQLException {
		int i = 1;
		for (Object value : values) {
			stmt.setObject(i, value);
			i++;
		}
		//
		return i;
	}

	//
	public long insert(String table, Map<String, Object> row) throws ClassNotFoundException, SQLException {
		// sql format
		Set<String> keys = row.keySet();
		Collection<Object> values = row.values();
		String sql = "INSERT INTO `" + this.table_pre + table + "` (" + this.fieldName2String(keys) + ")" + " VALUES("
				+ this.fieldValue2String(values) + ")";

		// sql values
		PreparedStatement stmt = this.prepare(sql);
		this.fieldValueSet(values, stmt);

		// execute
		stmt.execute();
		long last_id = stmt.getLastInsertID();
		stmt.close();
		//
		return last_id;
	}

	//
	public int update(String table, Map<String, Object> row, String where) throws ClassNotFoundException, SQLException {
		int line = 0;

		// sql format
		String fields = "";
		for (Entry<String, Object> entry : row.entrySet()) {
			if (0 < fields.length()) {
				fields += ",";
			}
			//
			fields += "`" + entry.getKey() + "`" + "=" + "'" + String.valueOf(entry.getValue()) + "'";
		}
		String sql_where = (null == where) ? "" : (" WHERE " + where);
		String sql = "UPDATE `" + this.table_pre + table + "` SET " + fields + sql_where;

		// sql values
		PreparedStatement stmt = this.prepare(sql);

		// execute
		line = stmt.executeUpdate();
		stmt.close();
		//
		return line;
	}

	//
	public boolean delete(String table, String where) throws ClassNotFoundException, SQLException {
		String sql_where = (null == where) ? "" : (" WHERE " + where);
		String sql = "DELETE FROM `" + this.table_pre + table + "` " + sql_where;
		PreparedStatement stmt = this.prepare(sql);
		stmt.execute();
		stmt.close();
		//
		return true;
	}

	//
	public List<Map<String, Object>> select(String table, Set<String> fields, String where, String order)
			throws ClassNotFoundException, SQLException {
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		String sql_where = (null == where) ? "" : (" WHERE " + where);
		String sql_order = (null == order) ? "" : (" ORDER BY " + order);
		String sql = "SELECT " + this.fieldName2String(fields) + " FROM `" + this.table_pre + table + "` " + sql_where
				+ sql_order;
		//
		PreparedStatement stmt = this.prepare(sql);
		ResultSet ret = stmt.executeQuery();
		//
		while (ret.next()) {
			Map<String, Object> row = new HashMap<String, Object>();
			//
			java.sql.ResultSetMetaData meta = ret.getMetaData();
			for (int i = 1; meta.getColumnCount() >= i; i++) {
				String field = meta.getColumnName(i);
				//
				row.put(field, ret.getObject(i));
			}
			//
			list.add(row);
		}
		//
		stmt.close();
		//
		return list;
	}

}