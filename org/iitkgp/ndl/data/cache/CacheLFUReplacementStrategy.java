package org.iitkgp.ndl.data.cache;

import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

/**
 * This class describes LFU(least frequently used) strategy for replacement algorithm for a fixed size cache
 * @param <T> Which kind value to be cached, key is always {@link String}
 * @author Debasis
 */
public class CacheLFUReplacementStrategy<T> implements CacheReplacementStrategy<T> {
	
	// replacement algorithm is LFU (least frequently used)
	
	int fixedSize;
	// cache hit/miss counter
	protected long cacheHit = 0, cacheMiss = 0;
	
	/**
	 * Fixed size cache manager
	 * @param size fixed cache size
	 */
	public CacheLFUReplacementStrategy(int size) {
		this.fixedSize = size;
	}
	
	// key frequency class, key and it's associated with access frequency
	class KeyFrequency {
		T value;
		long frequency;
		
		public KeyFrequency(long frequency, T value) {
			this.frequency = frequency;
			this.value = value;
		}
	}
	
	// TODO frequencies should be done using PriorotyQueue (min heap)
	// but java PQ does not offer any API like update(node, value)
	Map<String, KeyFrequency> cacheDetail = new TreeMap<String, KeyFrequency>();
	TreeMap<Long, Set<String>> cacheFrequencies = new TreeMap<Long, Set<String>>();
	
	/**
	 * Gets cache hit count
	 * @return returns cache hit count
	 */
	public long getCacheHit() {
		return cacheHit;
	}
	
	/**
	 * Gets cache miss count
	 * @return returns cache miss count
	 */
	public long getCacheMiss() {
		return cacheMiss;
	}
	
	/**
	 * Adds to cache, key and associated <b>T</b> type value
	 * @param key key name
	 * @param value associated value
	 */
	public void add(String key, T value) {
		if(cacheDetail.size() == fixedSize) {
			// cache is full, so less frequently used element removed
			String leastFrequent = removeLeastFrequent();
			cacheDetail.remove(leastFrequent);
		}
		cacheDetail.put(key, new KeyFrequency(0, value)); // cache
		updateFrequency(key, 0); // update frequencies
	}
	
	/**
	 * Gets values for a given key
	 * @param key key name
	 * @return returns associated value, NULL if not found
	 */
	public T get(String key) {
		// track access frequency
		KeyFrequency kf = cacheDetail.get(key);
		if(kf != null) {
			cacheHit++; // cache hit
			// add to `cacheFrequencies`
			updateFrequency(key, kf.frequency);
			kf.frequency++; // increase frequency
			return kf.value;
		} else {
			// not found
			cacheMiss++; // cache miss
			return null;
		}
	}
	
	/**
	 * Checks whether key cached or not
	 * @param key key to check
	 * @return returns true if cached otherwise NULL
	 */
	public boolean contains(String key) {
		return cacheDetail.containsKey(key);
	}
	
	/**
	 * Checks whether key exists in cache or not
	 * @param key key to check
	 * @return returns true if exists otherwise false
	 */
	public boolean exists(String key) {
		return cacheDetail.get(key) != null;
	}
	
	// updates frequency (current+1) to key when key is accessed
	void updateFrequency(String key, long frequency) {
		// removes frequency entry
		if(frequency > 0) {
			// non-zero case
			removeFrequencyEntry(frequency, key);
		}
		// add entry
		long nextFrequency = frequency + 1;
		Set<String> keys = cacheFrequencies.get(nextFrequency);
		if(keys == null) {
			// first time entry
			keys = new HashSet<String>(2);
			cacheFrequencies.put(nextFrequency, keys);
		}
		keys.add(key);
	}
	
	// removes least frequently used key to make new place for new entry
	String removeLeastFrequent() {
		Entry<Long, Set<String>> min = cacheFrequencies.firstEntry();
		long frequency = min.getKey();
		String removed = removeFrequencyEntry(frequency, null);
		return removed;
	}
	
	// removes frequency entry accordingly
	String removeFrequencyEntry(long frequency, String key) {
		Set<String> keys = cacheFrequencies.get(frequency);
		// first element/or given key to remove
		String removed = key == null ? keys.iterator().next() : key;
		keys.remove(removed); // remove element
		// safe remove
		if(keys.isEmpty()) {
			// no entry then remove the whole frequency-entry
			cacheFrequencies.remove(frequency);
		}
		return removed;
	}
}