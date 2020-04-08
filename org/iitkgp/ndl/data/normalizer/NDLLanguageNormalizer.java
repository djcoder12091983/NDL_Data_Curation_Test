package org.iitkgp.ndl.data.normalizer;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.data.normalizer.exception.DataNormalizationException;
import org.iitkgp.ndl.util.NDLServiceUtils;

/**
 * NDL language normalizer
 * @author Debasis
 */
public class NDLLanguageNormalizer extends NDLDataNormalizerWithCache<String> {
	
	/**
	 * default constructor
	 */
	public NDLLanguageNormalizer() {
		// default
	}
	
	/**
	 * Sets multiple value separator if any
	 * @param multivalueSeparator multiple value separator
	 */
	public NDLLanguageNormalizer(char multivalueSeparator) {
		super(multivalueSeparator);
	}
	
	/**
	 * NDL language normalization process
	 */
	@Override
	public Set<String> transform(String input) {
		List<String> languages = splitValues(input);
		// normalization
		Set<String> normalizedLanguages = new HashSet<String>(2);
		for(String lang : languages) {
			String normalized = getFromCache(lang); // try to get it from cache
			if(normalized == null) {
				// not cached yet
				try {
					normalized = NDLServiceUtils.normalilzeLanguage(lang);
					addToCache(lang, normalized); // cache it
				} catch(Exception ex) {
					// error
					throw new DataNormalizationException(ex.getMessage(), ex.getCause());
				}
			}
			if(StringUtils.isNotBlank(normalized)) {
				normalizedLanguages.add(normalized); // add result if conversion successful
			}
		}
		return normalizedLanguages;
	}

}