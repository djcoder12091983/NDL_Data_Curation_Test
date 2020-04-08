package org.iitkgp.ndl.data.normalizer;

import org.iitkgp.ndl.data.NDLAbstractDateNormalizer;
import org.iitkgp.ndl.util.NDLServiceUtils;

/**
 * NDL date normalizer
 * @author Debasis
 */
public class NDLDateNormalizer extends NDLAbstractDateNormalizer {
	
	/**
	 * default constructor
	 */
	public NDLDateNormalizer() {
		// default
	}
	
	/**
	 * Sets multiple value separator if any
	 * @param multivalueSeparator multiple value separator
	 */
	public NDLDateNormalizer(char multivalueSeparator) {
		super(multivalueSeparator);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public String dateNormalize(String date) throws Exception {
		return NDLServiceUtils.normalilzeDate(date);
	}
}