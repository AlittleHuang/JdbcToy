package jdbcutil.cache;

import static jdbcutil.JdbcUtil.getDeleteQuery;
import static jdbcutil.JdbcUtil.getFindByIdQuery;
import static jdbcutil.JdbcUtil.getIdName;
import static jdbcutil.JdbcUtil.getIds;
import static jdbcutil.JdbcUtil.getInsertQuery;
import static jdbcutil.JdbcUtil.getTable;
import static jdbcutil.JdbcUtil.getUpdateQuery;
import static jdbcutil.JdbcUtil.toBean;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import jdbcutil.JdbcUtil;
import jdbcutil.QueryCriteria;
import jdbcutil.SqlAndParam;

public class JdbcTemplate<T> {

//	private DataSource dataSource;
	
	CacheImpl cache;
	
	public void setDataSource(DataSource dataSource) {
//		this.dataSource = dataSource;
		cache = new CacheImpl();
		cache.setDataSource(dataSource);
	}

	@SuppressWarnings("unchecked")
	public T selectOne(T record) {
		Object id = null;
		try {
			id = getIds(record)[0];
		} catch (Exception e) {
			e.printStackTrace();
		}
		return selectByPrimaryKey((Class<T>)record.getClass(), id);
	}

	public List<T> select(QueryCriteria<T> criteria) {
		List<T> list = null;
		try {
			list = queryForList(criteria);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}

	public long selectCount(QueryCriteria<T> criteria) {
		try {
			ResultSet resultSet = executeQuery(criteria.toCountQuery());
			List<Map<String, Object>> list = resultSetToList(resultSet);
			return (long) list.get(0).get("COUNT(*)");
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0L;
	}

	public T selectByPrimaryKey(Class<T> clazz, Object key) {
		try {
			SqlAndParam findByIdQuery = getFindByIdQuery(clazz,key);
			ResultSet resultSet = executeQuery(findByIdQuery);
			List<Map<String, Object>> list = resultSetToList(resultSet);
			if (list.size() == 1) {
				Map<String, Object> map = list.get(0);
				return (T) toBean(map,clazz);
			}else if(list.size()>1){
				throw new RuntimeException("结果不唯一");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public int insert(T record) {
		try {
			SqlAndParam sqlAndParameters = getInsertQuery(record);
			return executeUpdate(sqlAndParameters);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}

	public int delete(T record) {
		try {
			SqlAndParam sqlAndParameters = getDeleteQuery(record);
			return executeUpdate(sqlAndParameters);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}

	public int deleteByPrimaryKey(Class<T> clazz, Object key) {
		try {
			String table = getTable(clazz);
			String nId = getIdName(clazz)[0];
			SqlAndParam sqlAndParameters = getDeleteQuery(table, nId, key);
			return executeUpdate(sqlAndParameters);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}

	public int updateByPrimaryKey(T record) {
		try {
			SqlAndParam sqlAndParameters = getUpdateQuery(record);
			return executeUpdate(sqlAndParameters);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}
	
	public List<T> queryForList(QueryCriteria<T> restrictions) throws Exception {
		SqlAndParam query = restrictions.toFindQuery();
		List<Map<String, Object>> list = queryForList(query);
		List<T> ret = new ArrayList<>();
		for (Map<String, Object> map : list) {
			T bean = JdbcUtil.toBean(map, restrictions.getEntityClass());
			ret.add(bean);
		}
		return ret;
	}
	
	public List<Map<String, Object>> queryForList(SqlAndParam sqlAndParameters) throws SQLException {
		ResultSet resultSet = executeQuery(sqlAndParameters);
		List<Map<String, Object>> list = JdbcUtil.resultSetToList(resultSet);
		return list;
	}

	public static List<Map<String, Object>> resultSetToList(ResultSet resultSet)
			throws SQLException {
		if (resultSet == null) {
			return null;
		}
		ResultSetMetaData metaData = resultSet.getMetaData();
		int columnCount = metaData.getColumnCount();
		List<Map<String, Object>> list = new ArrayList<>();
		Map<String, Object> rowData = new HashMap<>();
		while (resultSet.next()) {
			rowData = new HashMap<String, Object>(columnCount);
			for (int i = 1; i <= columnCount; i++) {
				rowData.put(metaData.getColumnName(i), resultSet.getObject(i));
			}
			list.add(rowData);
		}
		return list;
	}

	private ResultSet executeQuery(SqlAndParam sqlAndParameters) throws SQLException {
//		String sql = sqlAndParameters.getSql();
//		Object[] args = sqlAndParameters.getParameters();
//		PreparedStatement prepareStatement = getPrepareStatement(sqlAndParameters);
//		ResultSet ret = prepareStatement.executeQuery();
//		System.out.println(sql);
//		printArrays(args);
		Object ret = cache.get(sqlAndParameters);
		return (ResultSet) ret;
	}
	
	private int executeUpdate(SqlAndParam sqlAndParameters) throws SQLException {
//		String sql = sqlAndParameters.getSql();
//		Object[] args = sqlAndParameters.getParameters();
//		PreparedStatement prepareStatement = getPrepareStatement(sqlAndParameters);
//		int ret = prepareStatement.executeUpdate();
//		System.out.println(sql);
//		printArrays(args);
		Object ret = cache.get(sqlAndParameters);
		return (int) ret;
	}

//	private PreparedStatement getPrepareStatement(SqlAndParam sqlAndParameters)
//			throws SQLException {
//		String sql = sqlAndParameters.getSql();
//		Object[] args = sqlAndParameters.getParameters();
//		Connection connection = dataSource.getConnection();
//		PreparedStatement prepareStatement = connection.prepareStatement(sql);
//		for (int i = 1; i <= args.length; i++) {
//			prepareStatement.setObject(i, args[i - 1]);
//		}
//		return prepareStatement;
//	}

}
