package jdbcutil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class QueryCriteria<T> {

	Class<T> clazz;
	Set<Criteria> aKey = new HashSet<>();
	Set<Criteria> oKey = new HashSet<>();
	private String limit;
	
	private class Criteria{
		private String criteria;
		private Object[] parameters;
		public Criteria(String key,String criteria, Object... parameters) {
			this.criteria ="`"+ key + "` " + criteria + " ?";
			this.parameters = parameters;
		}
		public Criteria(String criteria, Object... parameters) {
			this.criteria = criteria;
			this.parameters = parameters;
		}
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((criteria == null) ? 0 : criteria.hashCode());
			result = prime * result + Arrays.hashCode(parameters);
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
			@SuppressWarnings("unchecked")
			Criteria other = (Criteria) obj;
			if (criteria == null) {
				if (other.criteria != null)
					return false;
			} else if (!criteria.equals(other.criteria))
				return false;
			if (!Arrays.equals(parameters, other.parameters))
				return false;
			return true;
		}
	}

	public QueryCriteria(Class<T> clazz) {
		this.clazz = clazz;
	}

	public Class<T> getEntityClass() {
		return clazz;
	}

	public void setClazz(Class<T> clazz) {
		this.clazz = clazz;
	}

	public Set<Criteria> getaKey() {
		return aKey;
	}

	public void setaKey(Set<Criteria> aKey) {
		this.aKey = aKey;
	}

	public Set<Criteria> getoKey() {
		return oKey;
	}

	public void setoKey(Set<Criteria> oKey) {
		this.oKey = oKey;
	}

	public String getLimit() {
		return limit;
	}

	public void setLimit(String limit) {
		this.limit = limit;
	}

	public QueryCriteria<T> orEq(String k, Object v) {
		oKey.add(new Criteria(k, "=", v));
		return this;
	}

	public QueryCriteria<T> orAllEq(Map<String, Object> map) {
		Set<Entry<String, Object>> entrySet = map.entrySet();
		for (Entry<String, Object> entry : entrySet) {
			orEq(entry.getKey(), entry.getValue());
		}
		return this;
	}

	public QueryCriteria<T> orGt(String k, Object v) {
		oKey.add(new Criteria(k, ">", v));
		return this;
	}

	public QueryCriteria<T> orGe(String k, Object v) {
		oKey.add(new Criteria(k, ">=", v));
		return this;
	}

	public QueryCriteria<T> orLt(String k, Object v) {
		oKey.add(new Criteria(k, "<", v));
		return this;
	}

	public QueryCriteria<T> orLe(String k, Object v) {
		oKey.add(new Criteria(k, "<=", v));
		return this;
	}

	public QueryCriteria<T> orBetween(String k, Object l, Object h) {
		Object[] value = new Object[] { l, h };
		oKey.add(new Criteria("`" + k + "` BETWEEN ? AND ?", value));
		return this;
	}

	public QueryCriteria<T> orLike(String k, Object v) {
		oKey.add(new Criteria(k, "like", v));
		return this;
	}

	public QueryCriteria<T> orin(String k, Object[] v) {
		StringBuilder sb = new StringBuilder();
		sb.append(k).append(" IN ");
		JdbcUtil.placeholder(v.length, sb);
		oKey.add(new Criteria(sb.toString(), v));
		return this;
	}

	public QueryCriteria<T> eq(String k, Object v) {
		aKey.add(new Criteria(k, "=", v));
		return this;
	}

	public QueryCriteria<T> allEq(Map<String, Object> map) {
		for (Entry<String, Object> entry : map.entrySet()) {
			eq(entry.getKey(), entry.getValue());
		}
		return this;
	}

	public QueryCriteria<T> gt(String k, Object v) {
		aKey.add(new Criteria(k, ">", v));
		return this;
	}

	public QueryCriteria<T> ge(String k, Object v) {
		aKey.add(new Criteria(k, ">=", v));
		return this;
	}

	public QueryCriteria<T> lt(String k, Object v) {
		aKey.add(new Criteria(k, "<", v));
		return this;
	}

	public QueryCriteria<T> le(String k, Object v) {
		aKey.add(new Criteria(k, "<=", v));
		return this;
	}

	public QueryCriteria<T> between(String k, Object l, Object h) {
		Object[] value = { l, h };
		aKey.add(new Criteria("`" + k + "` BETWEEN ? AND ?", value));
		return this;
	}

	public QueryCriteria<T> like(String k, Object v) {
		aKey.add(new Criteria(k, "like", v));
		return this;
	}

	public QueryCriteria<T> in(String k, Object[] v) {
		StringBuilder sb = new StringBuilder();
		sb.append(k).append(" IN ");
		JdbcUtil.placeholder(v.length, sb);
		aKey.add(new Criteria(sb.toString(), v));
		return this;
	}

	public QueryCriteria<T> limit(int begn, int totle) {
		// TODO
		return this;
	}

	public QueryCriteria<T> limit(int totle) {
		limit(0, totle);
		return this;
	}

	public void clear() {
		oKey.clear();
		aKey.clear();
		limit = null;
	}

	// -----------------------------------------------------

	public SqlAndParam toFindQuery() throws Exception {
		return getQueryByRestrictions(JdbcUtil.SqlType.SELECT_LIST);
	}

	public SqlAndParam toCountQuery() throws Exception {
		return getQueryByRestrictions(JdbcUtil.SqlType.COUNT);
	}

	public SqlAndParam getQueryByRestrictions(JdbcUtil.SqlType type) throws Exception {
		StringBuilder sb = new StringBuilder();
		String table = JdbcUtil.getTable(this.getEntityClass());
		sb.append(JdbcUtil.SELECT);
		if (type==JdbcUtil.SqlType.COUNT) {
			sb.append(JdbcUtil.COUNT);
		} else {
			Object[] keys = JdbcUtil.getKeys(this.getEntityClass());
			JdbcUtil.formatArr(keys, "", ",\n    ", "", sb);
		}
		sb.append(JdbcUtil.FROM).append('`').append(table).append('`');
		Set<Criteria> aKey = this.getaKey();
		Set<Criteria> oKey = this.getoKey();
		List<Object> paramList = new ArrayList<>();
//		Map<String, Object[]> ak = JdbcUtil.formatAsKVArr(aKey);
//		Map<String, Object[]> ok = JdbcUtil.formatAsKVArr(oKey);
		if (aKey.size() + oKey.size() > 0) {
			sb.append(JdbcUtil.WHERE);
		}
		boolean first = true;
		for (Criteria k : aKey) {
			if (first) {
				first = false;
			} else {
				sb.append(JdbcUtil.AND);
			}
			sb.append(k.criteria);
			for (Object parameter : k.parameters) {
				paramList.add(parameter);
			}
		}
		for (Criteria k : oKey) {
			if (first) {
				first = false;
			} else {
				sb.append(JdbcUtil.OR);
			}
			sb.append(k.criteria);
			for (Object parameter : k.parameters) {
				paramList.add(parameter);
			}
		}
		String sql = sb.toString();
		Object[] ps = new Object[paramList.size()];
		paramList.toArray(ps);

		return new SqlAndParam(sql, ps, type, table);
	}

}
