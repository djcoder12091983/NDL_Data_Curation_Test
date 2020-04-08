package org.iitkgp.ndl.data.normalizer;

import org.iitkgp.ndl.context.NDLConfigurationContext;
import org.iitkgp.ndl.data.cache.DataCacheManager;
import org.iitkgp.ndl.data.cache.LFUDataCacheManager;

/**
 * <pre>This is an extension {@link NDLDataNormalizer} and it offers caching facility speed up the normalization process.</pre>
 * Note: Caching is required when computation cost is high, typically network call.
 * @param <T> see {@link DataCacheManager} for more details
 * @see DataCacheManager
 * @author Debasis
 */
public abstract class NDLDataNormalizerWithCache<T> extends NDLDataNormalizer {
	
	static {
		NDLConfigurationContext.init(); // initialization of global context
	}
	
	// cache manager
	protected DataCacheManager<T> cacheManager = new LFUDataCacheManager<T>(
			Integer.parseInt(NDLConfigurationContext.getConfiguration("ndl.data.normalization.cache.fixed.size")));
	
	/**
	 * default constructor
	 */
	public NDLDataNormalizerWithCache() {
		// default
	}
	
	/**
	 * Sets multiple value separator if any
	 * @param multivalueSeparator multiple value separator
	 */
	public NDLDataNormalizerWithCache(char multivalueSeparator) {
		super(multivalueSeparator);
	}
	
	/**
	 * Gets cache hit count
	 * @return returns cache hit count
	 */
	public long getCacheHit() {
		return cacheManager.getCacheHit();
	}
	
	/**
	 * Gets cache miss count
	 * @return returns cache miss count
	 */
	public long getCacheMiss() {
		return cacheManager.getCacheMiss();
	}
	
	/**
	 * Adds to cache for further ease access
	 * @param key key
	 * @param value associated value
	 */
	protected void addToCache(String key, T value) {
		cacheManager.add(key, value); // cache it
	}
	
	/**
	 * Gets cached value if cached otherwise NULL
	 * @param key returns cached value if cached otherwise NULL
	 * @return returns value from cache
	 */
	protected T getFromCache(String key) {
		return cacheManager.get(key);
	}
	
	/**
	 * Checks whether key cached or not
	 * @param key key to check
	 * @return returns true if cached otherwise NULL
	 */
	protected boolean contains(String key) { 
		return cacheManager.contains(key);
	}
}