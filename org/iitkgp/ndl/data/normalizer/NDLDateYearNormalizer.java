package org.iitkgp.ndl.data.normalizer;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.data.NDLAbstractDateNormalizer;
import org.iitkgp.ndl.util.NDLServiceUtils;

/**
 * NDL date(only year part) normalizer
 * @author Debasis
 */
public class NDLDateYearNormalizer extends NDLAbstractDateNormalizer {

	/**
	 * default constructor
	 */
	public NDLDateYearNormalizer() {
		// default
	}
	
	/**
	 * Sets multiple value separator if any
	 * @param multivalueSeparator multiple value separator
	 */
	public NDLDateYearNormalizer(char multivalueSeparator) {
		super(multivalueSeparator);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public String dateNormalize(String date) throws Exception {
		String normalized =  NDLServiceUtils.normalilzeDate(date);
		if(StringUtils.isBlank(normalized)) {
			// wrong date
			return date;
		} else {
			// only year
			return normalized.substring(0, 4);
		}
	}
}