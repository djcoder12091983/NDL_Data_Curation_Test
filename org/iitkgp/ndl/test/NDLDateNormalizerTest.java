package org.iitkgp.ndl.test;

import static org.junit.Assert.assertEquals;

import java.util.Collection;

import org.iitkgp.ndl.data.normalizer.NDLDateNormalizer;
import org.junit.Test;

/**
 * Test cases of {@link NDLDateNormalizer}
 * @author Debasis
 */
public class NDLDateNormalizerTest {
	
	/**
	 * test
	 * @throws Exception throws error if any error occurs
	 */
	@Test
	public void test() throws Exception {
		NDLDateNormalizer normalizer = new NDLDateNormalizer('|');
		// single date normalization
		Collection<String> values = normalizer.normalize("12/jun/2018");
		assertEquals(1, normalizer.getCacheMiss()); // all cache miss
		assertEquals(0, normalizer.getCacheHit()); // no cache hit
		assertEquals(1, values.size());
		assertEquals(true, values.contains("2018-06-12"));
		// multiple date normalization
		values = normalizer.transform("12/jun/2018|23-jul-2017");
		assertEquals(2, normalizer.getCacheMiss()); // 2 cache miss
		assertEquals(1, normalizer.getCacheHit()); // 1 cache hit
		assertEquals(2, values.size());
		assertEquals(true, values.contains("2018-06-12"));
		assertEquals(true, values.contains("2017-07-23"));
	}
}
