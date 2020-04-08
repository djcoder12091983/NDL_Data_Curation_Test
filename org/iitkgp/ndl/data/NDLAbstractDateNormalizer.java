package org.iitkgp.ndl.data;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.data.normalizer.NDLDataNormalizerWithCache;
import org.iitkgp.ndl.data.normalizer.exception.DataNormalizationException;

/**
 * NDL abstract date normalizer, date normalization logic is abstract
 * @author Debasis
 */
public abstract class NDLAbstractDateNormalizer extends NDLDataNormalizerWithCache<String> {
	
	/**
	 * default constructor
	 */
	public NDLAbstractDateNormalizer() {
		// default
	}
	
	/**
	 * Sets multiple value separator if any
	 * @param multivalueSeparator multiple value separator
	 */
	public NDLAbstractDateNormalizer(char multivalueSeparator) {
		super(multivalueSeparator);
	}
	
	/**
	 * normalizes date into NDL compatible format
	 */
	@Override
	public Set<String> transform(String input) {
		List<String> dates = splitValues(input); // split values with multiple-value-separator
		// normalization
		Set<String> normalizedDates = new HashSet<String>(2);
		for(String date : dates) {
			String normalized = getFromCache(date); // try to get it from cache
			if(normalized == null) {
				// not cached yet
				try {
					normalized = dateNormalize(date);
					addToCache(date, normalized); // cache it
				} catch(Exception ex) {
					ex.printStackTrace(System.out);
					// error
					throw new DataNormalizationException(ex.getMessage(), ex.getCause());
				}
			}
			if(StringUtils.isNotBlank(normalized)) {
				normalizedDates.add(normalized); // add result if conversion successful
			}
		}
		return normalizedDates;
	}
	
	/**
	 * Normalize function
	 * @param date date to normalize
	 * @return returns normalized values
	 * @throws Exception throws exception in case date normalization errors occur
	 */
	public abstract String dateNormalize(String date) throws Exception;
}