package test;

import org.junit.Test;

import jdbcutil.QueryCriteria;
import jdbcutil.cache.JdbcTemplate;

public class Test001 {
	
	@Test
	public void test01(){
		JdbcTemplate<TestBean> template = new JdbcTemplate<TestBean>();
		template.setDataSource(C3P0Utils.getDataSource());
		Class<TestBean> clazz = TestBean.class;
		QueryCriteria<TestBean> criteria = new QueryCriteria<TestBean>(clazz);
		criteria.eq("id", 36).orEq("id", 22).orBetween("id", 0, 99);
		
//		long count = template.selectCount(criteria);
//		template.select(criteria);
		TestBean user = new TestBean();
		user.setId(3);
		user.setUsername("");
		user.setPassword("");
//		template.delete(user);
		template.select(criteria);
		template.select(criteria);
//		List<Map<String, Object>> resultSetToList = JdbcUtil.resultSetToList(query);
//		System.out.println(bean);
	}

}
