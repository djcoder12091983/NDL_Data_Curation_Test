package org.iitkgp.ndl.context.custom.normalizer;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.iitkgp.ndl.data.normalizer.NDLDataNormalizerWithCache;
import org.iitkgp.ndl.data.normalizer.exception.DataNormalizationException;
import org.iitkgp.ndl.util.NDLDataUtils;
import org.iitkgp.ndl.util.NDLServiceUtils;

/**
 * <pre>NDL DDC normalizer by valid codes (300, 400 etc.). DDC normalization returns full hierarchy with text in sorted order.</pre>
 * <pre>Note: DDC codes are applicable only but not TEXT</pre>
 * @author Debasis
 */
public class NDLCareerVerticalDDCNormalizer extends NDLDataNormalizerWithCache<Set<String>> {

	/**
	 * default constructor
	 */
	public NDLCareerVerticalDDCNormalizer() {
		// default
	}
	
	/**
	 * Sets multiple value separator if any
	 * @param multivalueSeparator multiple value separator
	 */
	public NDLCareerVerticalDDCNormalizer(char multivalueSeparator) {
		super(multivalueSeparator);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<String> transform(String input) {
		List<String> ddcCodes = splitValues(input);
		Set<String> codelist = new HashSet<>(2); // maintain order
		for(String ddcCode : ddcCodes) {
			Set<String> normalizedDDC = getFromCache(ddcCode); // try to get it from cache
			if(normalizedDDC == null) {
				// not cached yet
				try {
					normalizedDDC = NDLServiceUtils.normalizeDDC2Codes(ddcCode);
					addToCache(ddcCode, normalizedDDC); // cache it
				} catch(Exception ex) {
					// error
					throw new DataNormalizationException(ex.getMessage(), ex.getCause());
				}
			}
			codelist.add(NDLDataUtils.join(normalizedDDC, '/')); // add result
		}
		
		return codelist;
	}
}
