package org.iitkgp.ndl.test;

import static org.junit.Assert.assertEquals;

import java.util.Collection;

import org.iitkgp.ndl.data.normalizer.NDLLanguageNormalizer;
import org.junit.Test;

/**
 * Test cases of {@link NDLLanguageNormalizer}
 * @author Debasis
 */
public class NDLLanguageNormalizerTest {
	
	/**
	 * test
	 * @throws Exception throws error if any error occurs
	 */
	@Test
	public void test() throws Exception {
		NDLLanguageNormalizer normalizer = new NDLLanguageNormalizer('|');
		Collection<String> values = normalizer.normalize("en|fa"); // first try
		assertEquals(2, normalizer.getCacheMiss()); // all cache miss
		assertEquals(0, normalizer.getCacheHit()); // no cache hit
		assertEquals(2, values.size());
		assertEquals(true, values.contains("fas"));
		assertEquals(true, values.contains("eng"));
		values = normalizer.normalize("por|en"); // next try
		assertEquals(3, normalizer.getCacheMiss()); // 3 cache miss
		assertEquals(1, normalizer.getCacheHit()); // 1 cache hit
		assertEquals(2, values.size());
		assertEquals(true, values.contains("por"));
		assertEquals(true, values.contains("eng"));
	}
}