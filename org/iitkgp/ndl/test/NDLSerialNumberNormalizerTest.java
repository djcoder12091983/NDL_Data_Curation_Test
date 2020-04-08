package org.iitkgp.ndl.test;

import static org.junit.Assert.assertEquals;

import java.util.Collection;

import org.iitkgp.ndl.data.normalizer.NDLSerialNumberNormalizer;
import org.junit.Test;

/**
 * Test cases of {@link NDLSerialNumberNormalizer}
 * @author Debasis
 */
public class NDLSerialNumberNormalizerTest {
	
	/**
	 * test
	 */
	@Test
	public void test() {
		NDLSerialNumberNormalizer normalizer = new NDLSerialNumberNormalizer('|');
		Collection<String> values = normalizer.normalize(" 1234-5678 |1045-78934 ");
		assertEquals(2, values.size());
		assertEquals(true, values.contains("12345678"));
		assertEquals(true, values.contains("104578934"));
	}
}