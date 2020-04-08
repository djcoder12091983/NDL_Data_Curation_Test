package org.iitkgp.ndl.test;

import static org.junit.Assert.assertEquals;

import java.util.Set;

import org.iitkgp.ndl.data.normalizer.NDLDataNormalizationPool;
import org.junit.Test;

/**
 * Test cases of {@link NDLDataNormalizationPool}
 * @author Debasis
 */
public class NDLDataNormalizationPoolTest {
	
	/**
	 * test
	 * @throws Exception throws error if any error occurs
	 */
	@Test
	public void test() throws Exception {
		NDLDataNormalizationPool normalizer = new NDLDataNormalizationPool();
		Set<String> ddcvalues = normalizer.normalize("dc.subject.ddc", '|', "540|616");
		// ddc test (normalizer registered)
		assertEquals(5, ddcvalues.size());
		assertEquals(true, ddcvalues.contains("500::Natural sciences & mathematics"));
		assertEquals(true, ddcvalues.contains("540::Chemistry & allied sciences"));
		assertEquals(true, ddcvalues.contains("600::Technology"));
		assertEquals(true, ddcvalues.contains("610::Medicine & health"));
		assertEquals(true, ddcvalues.contains("616::Diseases"));
		// other field normalization which has no normalizer registered
		// default behavior is to split the values according to separator
		Set<String> values = normalizer.normalize("dc.contributor.author", '|', "Paul, T.|Das, K.");
		assertEquals(2, values.size());
		assertEquals(true, values.contains("Paul, T."));
		assertEquals(true, values.contains("Das, K."));
	}
	
	/**
	 * test custom normalizer register
	 * @throws Exception throws error if any error occurs
	 */
	@Test
	public void testCustomNormalizer() throws Exception {
		NDLDataNormalizationPool normalizer = new NDLDataNormalizationPool();
		normalizer.addNormalizer("dc.subject.ddc", "org.iitkgp.ndl.test.CustomDataNormalizerTest"); // override
		// test
		Set<String> ddcvalues = normalizer.normalize("dc.subject.ddc", '|', "100|200");
		assertEquals(2, ddcvalues.size());
		assertEquals(true, ddcvalues.contains("200"));
		assertEquals(true, ddcvalues.contains("300"));
	}
}