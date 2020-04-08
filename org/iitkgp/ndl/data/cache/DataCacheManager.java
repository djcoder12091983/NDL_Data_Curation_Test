package org.iitkgp.ndl.data.cache;

import org.iitkgp.ndl.data.normalizer.NDLDDCNormalizer;
import org.iitkgp.ndl.data.normalizer.NDLDataNormalizer;
import org.iitkgp.ndl.data.normalizer.NDLLanguageNormalizer;

/**
 * <pre>Data cache management for costly computed data, a value is cached for a given key.</pre>
 * <pre>When a key is accessed manager checks whether it's in cache then returns from cache otherwise computes
 * and stores it in cache for future use.</pre>
 * <pre>The manager maintains a fixed size cache when cache is full then
 * least frequently used key is removed and new key is added to cache.</pre>
 * @param <T> Which kind value to be cached, key is always {@link String}
 * @see NDLDataNormalizer
 * @see NDLDDCNormalizer
 * @see NDLLanguageNormalizer
 * @see CacheReplacementStrategy
 * @author Debasis
 */
public abstract class DataCacheManager<T> {
	// fixed size data cache manager
	
	protected CacheReplacementStrategy<T> strategy = null;
	
	/**
	 * Gets cache hit count
	 * @return returns cache hit count
	 */
	public long getCacheHit() {
		return strategy.getCacheHit();
	}
	
	/**
	 * Gets cache miss count
	 * @return returns cache miss count
	 */
	public long getCacheMiss() {
		return strategy.getCacheMiss();
	}
	
	/**
	 * Adds to cache, key and associated <b>T</b> type value
	 * @param key key name
	 * @param value associated value
	 */
	public void add(String key, T value) {
		strategy.add(key, value);
	}
	
	/**
	 * Gets values for a given key
	 * @param key key name
	 * @return returns associated value, NULL if not found
	 */
	public T get(String key) {
		return strategy.get(key);
	}
	
	/**
	 * Checks whether key cached or not
	 * @param key key to check
	 * @return returns true if cached otherwise NULL
	 */
	public boolean contains(String key) {
		return strategy.contains(key);
	}
	
	/**
	 * Checks whether key exists in cache or not
	 * @param key key to check
	 * @return returns true if exists otherwise false
	 */
	public boolean exists(String key) {
		return strategy.exists(key);
	}
}