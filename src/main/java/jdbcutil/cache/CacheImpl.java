package jdbcutil.cache;

import static jdbcutil.JdbcUtil.printArrays;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.sql.DataSource;

import jdbcutil.SqlAndParam;

public class CacheImpl implements Cache<SqlAndParam, Object> {

	private DataSource dataSource;

	Map<String, DataPool<SqlAndParam, Object>> selectOne = new ConcurrentHashMap<>();
	Map<String, DataPool<SqlAndParam, Object>> selectList = new ConcurrentHashMap<>();

	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
	}

	private PreparedStatement getPrepareStatement(SqlAndParam key) {
		try {
			String sql = key.getSql();
			Object[] args = key.getParameters();
			Connection connection = dataSource.getConnection();
			PreparedStatement prepareStatement = connection.prepareStatement(sql);
			for (int i = 1; i <= args.length; i++) {
				prepareStatement.setObject(i, args[i - 1]);
			}
			return prepareStatement;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public Object get(SqlAndParam key) {
		Object ret = null;
		switch (key.getSqlType()) {

		case SELECT_ONE:
			ret = getRet(selectOne, key);
			break;
		case SELECT_LIST:
			ret = getRet(selectList, key);
			break;
		case COUNT:
			ret = executeQuery(key);
			break;
		case INSERT:
			ret = executeUpdate(key);
			if ((int)ret > 0) {
				selectList.remove(key.getTableName());
			}
			break;
		case UPDATE:
		case DELETE:
			ret = executeUpdate(key);
			if ((int)ret > 0) {
				selectList.remove(key.getTableName());
				selectOne.remove(key.getTableName());
			}
			break;
		default:
			break;
		}

		return ret;
	}

	private Object getRet(Map<String, DataPool<SqlAndParam, Object>> map, SqlAndParam key) {

		String tableName = key.getTableName();
		DataPool<SqlAndParam, Object> pool = map.get(tableName);
		if (pool == null) {
			pool = new DataPool<>();
			map.put(tableName, pool);
		}

		Object ret;
		if (pool.containsKey(key)) {
			ret = pool.get(key);
		} else {
			ret = executeQuery(key);
			pool.put(key, ret);
		}
		return ret;
	}

	private ResultSet executeQuery(SqlAndParam key) {
		try {
			ResultSet ret = getPrepareStatement(key).executeQuery();
			System.out.println(key.getSql());
			printArrays(key.getParameters());
			return ret;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	private int executeUpdate(SqlAndParam key) {
		try {
			int ret = getPrepareStatement(key).executeUpdate();
			System.out.println(key.getSql());
			printArrays(key.getParameters());
			return ret;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return 0;
	}

}
