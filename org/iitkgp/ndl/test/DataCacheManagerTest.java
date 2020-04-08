package org.iitkgp.ndl.test;

import static org.junit.Assert.assertEquals;

import org.iitkgp.ndl.data.cache.DataCacheManager;
import org.iitkgp.ndl.data.cache.LFUDataCacheManager;
import org.junit.Test;

/**
 * Test cases of {@link DataCacheManager}
 * @author Aurghya, Debasis
 */
public class DataCacheManagerTest {
	
	// cache
	DataCacheManager<String> lfuCache = new LFUDataCacheManager<String>(5);
	
	/**
	 * test add and get 
	 */
	@Test
	public void test() {
		lfuCache.add("ndl1", "ndldata1");
		lfuCache.add("ndl2", "ndldata2");
		lfuCache.add("ndl3", "ndldata3");
		lfuCache.add("ndl4", "ndldata4");
		lfuCache.add("ndl5", "ndldata5");
		// access frequencies
		for(int i=0; i<5; i++) {
			assertEquals("ndldata5", lfuCache.get("ndl5"));
		}
		for(int i=0; i<4; i++) {
			assertEquals("ndldata4", lfuCache.get("ndl4"));
		}
		for(int i=0; i<3; i++) {
			assertEquals("ndldata3", lfuCache.get("ndl3"));
		}
		for(int i=0; i<2; i++) {
			assertEquals("ndldata2", lfuCache.get("ndl2"));
		}
		for(int i=0; i<1; i++) {
			assertEquals("ndldata1", lfuCache.get("ndl1"));
		}
		assertEquals(true, lfuCache.exists("ndl1"));
		assertEquals(true, lfuCache.exists("ndl2"));
		assertEquals(true, lfuCache.exists("ndl3"));
		assertEquals(true, lfuCache.exists("ndl4"));
		assertEquals(true, lfuCache.exists("ndl5"));
		lfuCache.add("ndl6", "ndldata6"); // new entry
		assertEquals(false, lfuCache.exists("ndl1")); // removes 1 because of low frequency access
		assertEquals(true, lfuCache.exists("ndl2"));
		assertEquals(true, lfuCache.exists("ndl3"));
		assertEquals(true, lfuCache.exists("ndl4"));
		assertEquals(true, lfuCache.exists("ndl5"));
		assertEquals(true, lfuCache.exists("ndl6"));
		assertEquals("ndldata6", lfuCache.get("ndl6")); // access 6
	}
	
}