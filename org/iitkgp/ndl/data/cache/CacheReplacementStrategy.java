package org.iitkgp.ndl.data.cache;

/**
 * This contract describes strategy for replacement algorithm for a fixed size cache
 * @param <T> Which kind value to be cached, key is always {@link String}
 * @author Debasis
 */
public interface CacheReplacementStrategy<T> {
	
	/**
	 * Adds to cache, key and associated <b>T</b> type value
	 * @param key key name
	 * @param value associated value
	 */
	void add(String key, T value);
	
	/**
	 * Gets values for a given key
	 * @param key key name
	 * @return returns associated value, NULL if not found
	 */
	T get(String key);
	
	/**
	 * Checks whether key cached or not
	 * @param key key to check
	 * @return returns true if cached otherwise NULL
	 */
	boolean contains(String key);
	
	/**
	 * Checks whether key exists in cache or not
	 * @param key key to check
	 * @return returns true if exists otherwise false
	 */
	boolean exists(String key);
	
	/**
	 * Gets cache hit count
	 * @return returns cache hit count
	 */
	long getCacheHit();
	
	/**
	 * Gets cache miss count
	 * @return returns cache miss count
	 */
	long getCacheMiss();
}