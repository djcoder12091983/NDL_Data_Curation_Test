package org.iitkgp.ndl.data.normalizer;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.iitkgp.ndl.data.normalizer.exception.DataNormalizationException;
import org.iitkgp.ndl.util.NDLServiceUtils;

/**
 * <pre>NDL DDC normalizer by valid codes (300, 400 etc.). DDC normalization returns full hierarchy with text in sorted order.</pre>
 * See {@link NDLServiceUtils#DDC_COMAPRATOR} for DDC hierarchy text sorting logic
 * @author Debasis
 */
public class NDLDDCNormalizer extends NDLDataNormalizerWithCache<Set<String>> {
	
	/**
	 * default constructor
	 */
	public NDLDDCNormalizer() {
		// default
	}
	
	/**
	 * Sets multiple value separator if any
	 * @param multivalueSeparator multiple value separator
	 */
	public NDLDDCNormalizer(char multivalueSeparator) {
		super(multivalueSeparator);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<String> transform(String input) {
		List<String> ddcCodes = splitValues(input);
		// normalization
		Set<String> normalizedDDCCodes = new TreeSet<String>(NDLServiceUtils.DDC_COMAPRATOR);
		for(String ddcCode : ddcCodes) {
			Set<String> normalizedDDC = getFromCache(ddcCode); // try to get it from cache
			if(normalizedDDC == null) {
				// not cached yet
				try {
					normalizedDDC = NDLServiceUtils.normalizeDDC(ddcCode);
					addToCache(ddcCode, normalizedDDC); // cache it
				} catch(Exception ex) {
					// error
					throw new DataNormalizationException(ex.getMessage(), ex.getCause());
				}
			}
			normalizedDDCCodes.addAll(normalizedDDC); // add result
		}
		return normalizedDDCCodes;
	}

}