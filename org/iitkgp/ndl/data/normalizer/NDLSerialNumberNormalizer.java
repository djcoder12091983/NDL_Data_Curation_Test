package org.iitkgp.ndl.data.normalizer;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * NDL ISSN/ISBN or other serial numbers are normalized 
 * @author Debasis
 */
public class NDLSerialNumberNormalizer extends NDLDataNormalizer {
	
	/**
	 * default constructor
	 */
	public NDLSerialNumberNormalizer() {
		// default
	}
	
	/**
	 * Sets multiple value separator if any
	 * @param multivalueSeparator multiple value separator
	 */
	public NDLSerialNumberNormalizer(char multivalueSeparator) {
		super(multivalueSeparator);
	}

	/**
	 * Normalization process
	 */
	@Override
	public Set<String> transform(String input) {
		List<String> values = splitValues(input);
		// normalization
		Set<String> normalizedValues = new HashSet<String>(4);
		for(String value : values) {
			// remove hyphen
			String modifiedValue = value.replaceAll("-", "").replace('x', 'X');
			normalizedValues.add(normalizeMore(modifiedValue));
		}
		return normalizedValues;
	}

	/**
	 * Normalizes serial number if needed further
	 * @param serial serial number to normalize
	 * @return returns normalized serial number
	 */
	public String normalizeMore(String serial) {
		// blank implementation
		return serial; // no change
	}
}
