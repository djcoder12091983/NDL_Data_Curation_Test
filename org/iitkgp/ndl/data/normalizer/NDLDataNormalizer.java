package org.iitkgp.ndl.data.normalizer;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

import org.iitkgp.ndl.data.Transformer;
import org.iitkgp.ndl.data.normalizer.exception.DataNormalizationException;

/**
 * NDL data normalizer base class which responsible for splitting values into tokens sends to target transformer
 * @author Debasis
 */
public abstract class NDLDataNormalizer implements Transformer<String, String> {
	
	char multivalueSeparator;
	boolean multivalueSeparatorExists = false;
	
	/**
	 * default constructor
	 */
	public NDLDataNormalizer() {
		// default
	}
	
	/**
	 * Sets multiple value separator if any
	 * @param multivalueSeparator multiple value separator
	 */
	public NDLDataNormalizer(char multivalueSeparator) {
		this.multivalueSeparator = multivalueSeparator;
		multivalueSeparatorExists = true;
	}

	/**
	 * Splitting values into tokens
	 * @param input input to split
	 * @return returns splitted multiple tokens by separator if any
	 */
	protected List<String> splitValues(String input) {
		List<String> values = new LinkedList<String>();
		if(multivalueSeparatorExists) {
			// multiple value separator exists
			StringTokenizer tokens = new StringTokenizer(input, String.valueOf(multivalueSeparator));
			while(tokens.hasMoreTokens()) {
				values.add(tokens.nextToken().trim());
			}
		} else {
			values.add(input.trim());
		}
		return values;
	}
	
	/**
	 * normalization takes place according to target transformer
	 * @param value value to be normalized
	 * @return returns normalized value
	 * @throws DataNormalizationException throws error normalization fails
	 */
	public Collection<String> normalize(String value) throws DataNormalizationException {
		try {
			// transformation logic
			return transform(value);
		} catch(Exception ex) {
			// error
			// ex.printStackTrace(System.out);
			throw new DataNormalizationException(ex.getMessage(), ex.getCause());
		}
	}
}