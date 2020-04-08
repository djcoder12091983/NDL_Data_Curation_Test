package org.iitkgp.ndl.test;

import static org.junit.Assert.assertEquals;

import java.util.Collection;

import org.iitkgp.ndl.data.normalizer.NDLInitialCapitalNormalizer;
import org.junit.Test;

/**
 * Test cases of {@link NDLInitialCapitalNormalizer}
 * @author Debasis
 */
public class NDLInitialCapitalNormalizerTest {
	
	/**
	 * test
	 */
	@Test
	public void test() {
		NDLInitialCapitalNormalizer normalizer = new NDLInitialCapitalNormalizer('|');
		Collection<String> values = normalizer.normalize(" ndl data |new ndl data ");
		assertEquals(2, values.size());
		assertEquals(true, values.contains("Ndl data"));
		assertEquals(true, values.contains("New ndl data"));
	}
}