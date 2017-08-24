package jdbcutil.cache;

public interface Cache<K,V> {
	
	V get(K key);// throws Exception;
	
}
