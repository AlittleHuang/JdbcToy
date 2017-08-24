package jdbcutil;

import java.util.Arrays;

import jdbcutil.JdbcUtil.SqlType;

/**
 * 查询语句与参数
 * @author HuangChengwei
 *
 */
public class SqlAndParam {

	private SqlType sqlType;		//操作类型(增删改查)
	private String tableName;		//表名
	private Object[] parameters;	//参数
	private String sql;				//sql语句

	public SqlAndParam(String sql, Object[] parameters, SqlType type, String table) {
		this.parameters = parameters;
		this.sql = sql;
		this.sqlType = type;
		this.tableName = table;
	}

	public SqlAndParam(String sql, Object parameters, SqlType type, String table) {
		this(sql, new Object[] { parameters }, type, table);
	}

	public Object[] getParameters() {
		return parameters;
	}

	public String getSql() {
		return sql;
	}

	public SqlType getSqlType() {
		return sqlType;
	}

	public void setSqlType(SqlType type) {
		this.sqlType = type;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String table) {
		this.tableName = table;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(parameters);
		result = prime * result + ((sql == null) ? 0 : sql.hashCode());
		result = prime * result + ((sqlType == null) ? 0 : sqlType.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SqlAndParam other = (SqlAndParam) obj;
		if (!Arrays.equals(parameters, other.parameters))
			return false;
		if (sql == null) {
			if (other.sql != null)
				return false;
		} else if (!sql.equals(other.sql))
			return false;
		if (sqlType != other.sqlType)
			return false;
		return true;
	}

	

}