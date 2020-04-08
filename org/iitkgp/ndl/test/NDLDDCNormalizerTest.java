package org.iitkgp.ndl.test;

import static org.junit.Assert.assertEquals;

import java.util.Collection;

import org.iitkgp.ndl.data.normalizer.NDLDDCNormalizer;
import org.junit.Test;

/**
 * Test cases of {@link NDLDDCNormalizer}
 * @author Debasis
 */
public class NDLDDCNormalizerTest {
	
	/**
	 * test
	 * @throws Exception throws error if any error occurs
	 */
	@Test
	public void test() throws Exception {
		NDLDDCNormalizer normalizer = new NDLDDCNormalizer('|');
		Collection<String> ddcvalues = normalizer.normalize("540|616"); // first try
		assertEquals(5, ddcvalues.size());
		assertEquals(true, ddcvalues.contains("500::Natural sciences & mathematics"));
		assertEquals(true, ddcvalues.contains("540::Chemistry & allied sciences"));
		assertEquals(true, ddcvalues.contains("600::Technology"));
		assertEquals(true, ddcvalues.contains("610::Medicine & health"));
		assertEquals(true, ddcvalues.contains("616::Diseases"));
		assertEquals(2, normalizer.getCacheMiss()); // all cache miss
		assertEquals(0, normalizer.getCacheHit()); // no cache hit
		ddcvalues = normalizer.normalize("540|636"); // next try
		assertEquals(3, normalizer.getCacheMiss()); // 3 cache miss
		assertEquals(1, normalizer.getCacheHit()); // 1 cache hit
		assertEquals(5, ddcvalues.size());
		assertEquals(true, ddcvalues.contains("500::Natural sciences & mathematics"));
		assertEquals(true, ddcvalues.contains("540::Chemistry & allied sciences"));
		assertEquals(true, ddcvalues.contains("600::Technology"));
		assertEquals(true, ddcvalues.contains("630::Agriculture & related technologies"));
		assertEquals(true, ddcvalues.contains("636::Animal husbandry"));
	}
}
