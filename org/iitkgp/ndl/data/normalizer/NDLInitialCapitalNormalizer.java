package org.iitkgp.ndl.data.normalizer;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.iitkgp.ndl.util.NDLDataUtils;

/**
 * The text is splitted into tokens by some separator {@link #splitValues(String)}
 * and each token contains initial Capital
 * @author Debasis
 */
public class NDLInitialCapitalNormalizer extends NDLDataNormalizer {
	
	/**
	 * default constructor
	 */
	public NDLInitialCapitalNormalizer() {
		// default
	}
	
	/**
	 * Sets multiple value separator if any
	 * @param multivalueSeparator multiple value separator
	 */
	public NDLInitialCapitalNormalizer(char multivalueSeparator) {
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
			normalizedValues.add(NDLDataUtils.initCap(value));
		}
		return normalizedValues;
	}

}