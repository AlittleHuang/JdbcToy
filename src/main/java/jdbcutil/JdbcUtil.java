package jdbcutil;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;

public class JdbcUtil {

	public final static String COLUMNS = "COLUMNS";
	public final static String PARAMETERDS = "PARAMETERDS";

	public final static String INSERT_INTO = " INSERT INTO ";
	public final static String SELECT = " SELECT\n     ";
	public final static String UPDATE = " UPDATE\n     ";
	public final static String DELETE = " DELETE";
	public final static String COUNT = " COUNT(*)";
	public final static String FROM = "\n FROM\n     ";
	public final static String SET = "\n SET\n     ";
	public final static String WHERE = "\n WHERE\n     ";
	public final static String VALUES = "\n VALUES\n     ";
	public final static String AND = "\n AND ";
	public final static String OR = "\n OR  ";

	public static enum SqlType {
		SELECT_ONE, SELECT_LIST, UPDATE, DELETE, INSERT, COUNT
	}

	public static SqlAndParam getFindByIdQuery(String table, String nId, Object vId) {
		String sql = SELECT + "*" + FROM + "`" + table + "`" + WHERE + "`" + nId + "`" + "= ?";
		return new SqlAndParam(sql, vId, SqlType.SELECT_ONE, table);
	}

	public static SqlAndParam getFindByIdQuery(Class<?> clazz, Object vId) throws Exception {
		Object[] keys = getKeys(clazz);
		String table = getTable(clazz);
		String nId = getIdName(clazz)[0];
		return getFindByIdQuery(keys, table, nId, vId);
	}

	public static SqlAndParam getFindByIdQuery(Object[] keys, String table, String nId, Object vId) {
		StringBuilder sb = new StringBuilder();
		sb.append(SELECT);
		formatArr(keys, "", ",\n     ", "", sb);
		sb.append(FROM).append('`').append(table).append('`');
		sb.append(WHERE).append('`').append(nId).append("` = ?");
		return new SqlAndParam(sb.toString(), vId, SqlType.SELECT_ONE, table);
	}

	public static SqlAndParam getInsertQuery(String table, Map<String, Object> map) {
		Map<String, Object[]> kv = formatAsKVArr(map);
		StringBuilder sb = new StringBuilder();
		sb.append(INSERT_INTO);
		fKey(table, sb);
		formatKVs(kv.get(COLUMNS), sb).append(VALUES);
		placeholder(map.size(), sb);
		String sql = sb.toString();
		return new SqlAndParam(sql, kv.get(PARAMETERDS), SqlType.INSERT, table);
	}

	public static SqlAndParam getInsertQuery(Object entity) throws Exception {
		return getInsertQuery(getTable(entity), toMap(entity));
	}

	public static SqlAndParam getUpdateQuery(String table, Map<String, Object> map, String nid,
			Object vId) {
		Map<String, Object[]> kv = formatAsKVArr(map);
		Object[] column = kv.get(COLUMNS);
		StringBuilder sb = new StringBuilder();

		sb.append(UPDATE);
		fKey(table, sb).append(SET);
		formatEq(column, sb).append(WHERE);
		fKey(nid, sb).append(" = ?");

		Object[] parameter = concat(kv.get(PARAMETERDS), vId);

		return new SqlAndParam(sb.toString(), parameter, SqlType.UPDATE, table);
	}

	public static SqlAndParam getUpdateQuery(Object entity) throws Exception {
		String table = getTable(entity);
		Map<String, Object> map = toMap(entity);
		String nId = getIdName(entity)[0];
		Object vId = map.get(nId);
		return getUpdateQuery(table, map, nId, vId);
	}

	public static SqlAndParam getDeleteQuery(String table, String nid, Object id) {
		String sql = DELETE + FROM +"`" + table + "`" + WHERE + "`" + nid + "` = ?";
		return new SqlAndParam(sql, id, SqlType.DELETE, table);
	}

	public static SqlAndParam getDeleteQuery(Object entity) throws Exception {
		String table = getTable(entity);
		String nId = getIdName(entity)[0];
		Object id = getIds(entity)[0];
		return getDeleteQuery(table, nId, id);
	}

	public static SqlAndParam getUpdateQuery(String table, Map<String, Object> map, String idName) {
		Object id = map.remove(idName);
		return getUpdateQuery(table, map, idName, id);
	}

	/**
	 * StringBuilder后增加 INSERT 类型sql语句的占位符(?,?,?...)
	 * @param i
	 * @param sb
	 * @return
	 */
	public static StringBuilder placeholder(int i, StringBuilder sb) {
		sb.append("(?");
		for (int j = 1; j < i; j++) {
			sb.append(", ?");
		}
		sb.append(")");
		return sb;
	}

	public static Object[] concat(Object[] a, Object[] b) {
		Object[] c = new Object[a.length + b.length];
		System.arraycopy(a, 0, c, 0, a.length);
		System.arraycopy(b, 0, c, a.length, b.length);
		return c;
	}

	private static Object[] concat(Object[] a, Object b) {
		Object[] c = new Object[a.length + 1];
		System.arraycopy(a, 0, c, 0, a.length);
		c[a.length] = b;
		return c;
	}

	private static StringBuilder formatEq(Object[] keys, StringBuilder sb) {
		boolean first = true;
		for (Object key : keys) {
			if (first) {
				first = false;
			} else {
				sb.append(",\n     ");
			}
			sb.append('`');
			sb.append(key);
			sb.append('`');
			sb.append(" = ?");
		}
		return sb;
	}

	public static Map<String, Object[]> formatAsKVArr(Map<String, Object> map) {
		if (map == null)
			return null;

		Set<Entry<String, Object>> entrySet = map.entrySet();
		String[] ks = new String[map.size()];
		List<Object> vsList = new ArrayList<>();
		int i = 0;
		for (Entry<String, Object> entry : entrySet) {
			Object v = entry.getValue();
			if (entry.getValue() != null) {
				if (v instanceof Object[]) {
					Object[] valuse = (Object[]) v;
					for (Object o : valuse) {
						vsList.add(o);
					}
				} else
					vsList.add(entry.getValue());
			} else {
				vsList.add(null);
			}
			ks[i++] = entry.getKey().toString();
		}
		HashMap<String, Object[]> hashMap = new HashMap<>();
		hashMap.put(COLUMNS, ks);
		Object[] vs = new Object[vsList.size()];
		vsList.toArray(vs);
		hashMap.put(PARAMETERDS, vs);
		return hashMap;
	}

	public static StringBuilder formatArr(Object[] kvs, String a, String b, String c,
			StringBuilder sb) {
		if (kvs != null && kvs.length != 0) {
			int length = kvs.length;
			int i;
			sb.append(a);
			for (i = 0; i < length - 1; i++) {
				sb.append('`').append(kvs[i]).append('`').append(b);
			}
			sb.append('`').append(kvs[i]).append('`').append(c);
		}
		return sb;
	}

	private static StringBuilder formatKVs(Object[] keys, StringBuilder sb) {
		return formatArr(keys, "(", ",", ")", sb);
	}

	private static StringBuilder fKey(String key, StringBuilder sb) {
		return sb.append('`').append(key).append('`');
	}

	public static Map<String, Object> toMap(Object obj) {

		if (obj == null) {
			return null;
		}
		Map<String, Object> map = new HashMap<String, Object>();
		try {
			BeanInfo beanInfo = Introspector.getBeanInfo(obj.getClass());
			PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();
			for (PropertyDescriptor property : propertyDescriptors) {
				String key = property.getName();
				if (!key.equals("class")) {
					Method getter = property.getReadMethod();
					Object value = getter.invoke(obj);
					map.put(key, value);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return map;
	}

	public static Map<String, Object> entityToMap(Object obj) {

		if (obj == null) {
			return null;
		}

		Map<String, Object> map = new HashMap<String, Object>();
		Class<? extends Object> cls = obj.getClass();
		try {
			BeanInfo beanInfo = Introspector.getBeanInfo(obj.getClass());
			PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();
			for (PropertyDescriptor property : propertyDescriptors) {
				String key = property.getName();
				if (!key.equals("class")) {

					Field field = cls.getDeclaredField(key);
					Column column = field.getAnnotation(Column.class);
					if (column != null) {
						if (!"".equals(column.name())) {
							key = column.name();
						}
						Method getter = property.getReadMethod();
						Object value = getter.invoke(obj);
						map.put(key, value);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return map;
	}

	public static <T> T toBean(Map<String, Object> map, Class<T> cls) throws Exception {
		T bean = cls.newInstance();
		try {
			BeanInfo beanInfo = Introspector.getBeanInfo(cls);
			PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();

			for (PropertyDescriptor property : propertyDescriptors) {
				String key = property.getName();
				if ("class".equals(key))
					continue;
				Field field = cls.getDeclaredField(key);

				Column clumn = field.getAnnotation(Column.class);
				if (clumn != null && isNotBlank(clumn.name())) {
					key = clumn.name();
				}

				if (map.containsKey(key)) {
					Object value = map.get(key);
					Method setter = property.getWriteMethod();
					setter.invoke(bean, value);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return bean;
	}

	private static Map<Class<?>, String[]> idNameMap = new HashMap<>();

	public static String[] getIdName(Object entity) throws Exception {
		return getIdName(entity.getClass());
	}

	public static String[] getIdName(Class<?> cls) throws Exception {
		if (idNameMap.containsKey(cls))
			return idNameMap.get(cls);
		List<String> idList = new ArrayList<>();
		BeanInfo beanInfo = Introspector.getBeanInfo(cls);
		PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();
		for (PropertyDescriptor property : propertyDescriptors) {
			String key = property.getName();
			if (!key.equals("class")) {
				Field field = cls.getDeclaredField(key);
				boolean isId = field.getAnnotation(Id.class) != null;
				if (isId)
					idList.add(key);
			}
		}
		String[] idName = new String[idList.size()];
		idList.toArray(idName);
		return idName;
	}

	public static String[] getKeys(Class<?> beanClass) throws Exception {
		BeanInfo beanInfo = Introspector.getBeanInfo(beanClass);
		PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();
		String[] keys = new String[propertyDescriptors.length - 1];
		int i = 0;
		for (PropertyDescriptor propertyDescriptor : propertyDescriptors) {
			String name = propertyDescriptor.getName();
			if ("class".equals(name)) {
				continue;
			}
			keys[i++] = name;
		}

		return keys;
	}

	public static List<Map<String, Object>> resultSetToList(ResultSet resultSet)
			throws SQLException {
		if (resultSet == null)
			return null;
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

	private static Map<Class<?>, String> tablesMap = new HashMap<>();

	public static String getTable(Class<?> class1) {
		if (tablesMap.containsKey(class1))
			return tablesMap.get(class1);
		Table table = class1.getAnnotation(Table.class);
		if (table != null && isNotBlank(table.name()))
			return table.name();
		Entity ett = class1.getAnnotation(Entity.class);
		if (ett != null && isNotBlank(ett.name()))
			return ett.name();
		return class1.getSimpleName();
	}

	public static String getTable(Object entity) {
		return getTable(entity.getClass());
	}

	private static Map<Class<?>, Method[]> idmethodsMap = new HashMap<>();

	public static Object[] getIds(Object entity) throws Exception {
		Class<?> cls = entity.getClass();
		Method[] methods;
		if (idmethodsMap.containsKey(entity)) {
			methods = idmethodsMap.get(entity);
		} else {

			List<Method> idMethodList = new ArrayList<>();

			BeanInfo beanInfo = Introspector.getBeanInfo(cls);
			PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();
			for (PropertyDescriptor property : propertyDescriptors) {
				String key = property.getName();
				if (!key.equals("class")) {
					Field field = cls.getDeclaredField(key);
					boolean isId = field.getAnnotation(Id.class) != null;
					if (isId)
						idMethodList.add(property.getReadMethod());
				}
			}

			methods = new Method[idMethodList.size()];
			idMethodList.toArray(methods);
			idmethodsMap.put(entity.getClass(), methods);
		}
		Object[] values = new Object[methods.length];

		for (int i = 0; i < values.length; i++) {
			values[i] = methods[i].invoke(entity);
		}

		return values;
	}
	
	public static void printArrays(Object[] objects) {
		if (objects == null || objects.length == 0)
			return;
		for (int i = 0; i < objects.length; i++) {
			if (i == 0)
				System.out.print("parameters:[" + objects[i]);
			else
				System.out.print("," + objects[i]);
		}
		System.out.println("]");
	}

	@org.jetbrains.annotations.Contract("null -> true")
	public static boolean isBlank(String str) {
		return str == null || "".equals(str.trim());
	}

	public static boolean isNotBlank(String str) {
		return !isBlank(str);
	}

}
