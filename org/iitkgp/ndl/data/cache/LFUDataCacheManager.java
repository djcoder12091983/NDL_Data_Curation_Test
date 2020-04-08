package org.iitkgp.ndl.data.cache;

/**
 * Data cache (fixed size) manager with replacement strategy {@link CacheLFUReplacementStrategy}
 * @param <T> Which kind value to be cached, key is always {@link String}
 * @see DataCacheManager
 * @see CacheReplacementStrategy
 * @author Debasis
 */
public class LFUDataCacheManager<T> extends DataCacheManager<T> {
	
	/**
	 * Constructor to initialize fixed size cache
	 * @param fixedSize fixed size (cache size, how many elements it can store at most)
	 */
	public LFUDataCacheManager(int fixedSize) {
		strategy = new CacheLFUReplacementStrategy<>(fixedSize);
	}
}