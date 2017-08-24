package jdbcutil.cache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 缓存策略:根据命中调整数据在队列中的位置,提升量由step控制
 * 
 * @param <K>
 *            获取缓存数据的关键字类型
 * @param <V>
 *            缓存数据类型
 */
public class DataPool<K, V> {

	private Map<K, DataNode<K, V>> dataMap = new ConcurrentHashMap<>();

	private DataNode<K, V> head; // 头结点
	private DataNode<K, V> tail; // 尾节点
	private int size; // 链表数量
	private int maxSize = 250; // 最大缓存数据量
	private int step = maxSize / 5; // 命中数据前进的数量
	private int insertStep = maxSize / 5; // 新数据插入的位置

	public boolean containsKey(K key) {
		return dataMap.containsKey(key);
	}

	/**
	 * 缓存节点
	 * 
	 * @param <K>
	 *            获取缓存数据的关键字类型
	 * @param <K,
	 *            V> 缓存数据类型
	 */
	private static class DataNode<K, V> {
		private K key;
		private V data;
		private DataNode<K, V> prev;
		private DataNode<K, V> next;

		public V getData() {
			return data;
		}

		public DataNode(K key, V data) {
			this.key = key;
			this.data = data;
		}

		public K getKey() {
			return key;
		}
	}

	public DataPool() {
	}

	public DataPool(int maxSize) {
		this.maxSize = maxSize;
		step = maxSize / 5;
		insertStep = maxSize / 5;
	}

	/**
	 * 插入新数据
	 * 
	 * @param e
	 *            插入的数据
	 */

	synchronized public void put(K key, V data) {
		DataNode<K, V> dataNode = new DataNode<K, V>(key, data);
		if (head == null) {
			head = dataNode;
			tail = dataNode;
		} else {
			int x = insertStep - maxSize + size;
			DataNode<K, V> cur = head;

			for (int i = 0; i < x && cur.next != null; i++) {
				cur = cur.next;
			}

			dataNode.next = cur;
			dataNode.prev = cur.prev;

			if (cur != head) {
				cur.prev.next = dataNode;
			} else {
				head = dataNode;
			}
			cur.prev = dataNode;

		}

		if (size == maxSize) {
			dataMap.remove(tail.getKey());
			tail = tail.prev;
			tail.next = null;
		} else {
			size++;
		}
		dataMap.put(dataNode.getKey(), dataNode);
	}

	public V get(K key) {
		V value = null;
		if (dataMap.containsKey(key)) {
			DataNode<K, V> dataNode = dataMap.get(key);
			hit(dataNode);
			value = dataNode.getData();
		}
		return value;
	}

	synchronized public void hit(DataNode<K, V> hit) {
		if (hit == head)
			return;
		DataNode<K, V> cur = hit;
		for (int i = 0; i < step && cur != head; i++) {
			cur = cur.prev;
		}

		hit.prev.next = hit.next;
		if(hit != tail){
			hit.next.prev = hit.prev;
		}else{
			tail = hit.prev;
		}
		
		if(cur != head){
			cur.prev.next = hit;
		}else{
			head = hit;
		}
		
		hit.prev = cur.prev;
		hit.next = cur;
		cur.prev = hit;
	}

}
