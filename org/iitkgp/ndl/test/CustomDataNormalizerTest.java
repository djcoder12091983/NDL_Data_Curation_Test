package org.iitkgp.ndl.test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.iitkgp.ndl.data.normalizer.NDLDataNormalizer;

/**
 * Custom data normalizer class for testing
 * @see NDLDataNormalizationPoolTest
 * @author Debasis
 */
public class CustomDataNormalizerTest extends NDLDataNormalizer {
	
	/**
	 * default constructor
	 */
	public CustomDataNormalizerTest() {
		// default
	}
	
	/**
	 * Sets multiple value separator if any
	 * @param multivalueSeparator multiple value separator
	 */
	public CustomDataNormalizerTest(char multivalueSeparator) {
		super(multivalueSeparator);
	}

	/**
	 * custom data normalizer, adds 100
	 */
	@Override
	public Set<String> transform(String input) {
		Set<String> values = new HashSet<String>(2);
		List<String> tokens = splitValues(input); // split
		for(String token : tokens) {
			// 100 add
			values.add(String.valueOf(Long.valueOf(token) + 100));
		}
		return values;
	}
}