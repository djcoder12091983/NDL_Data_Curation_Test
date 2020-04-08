package org.iitkgp.ndl.data.normalizer;

import org.apache.commons.lang3.StringUtils;

/**
 * NDL ISSN/ISBN or other serial numbers are normalized
 * <pre>Note: If given length not met then padding with zero on left side</pre>
 * @see NDLSerialNumberNormalizer
 * @author Debasis
 */
public class NDLSerialNumberNormalizerWithLeftZeroPadding extends NDLSerialNumberNormalizer {

	int fixedLength;
	
	/**
	 * default constructor
	 * @param fixedLength fixed length for serial number
	 */
	public NDLSerialNumberNormalizerWithLeftZeroPadding(int fixedLength) {
		this.fixedLength = fixedLength;
	}
	
	/**
	 * Sets multiple value separator if any
	 * @param multivalueSeparator multiple value separator
	 * @param fixedLength serial number fixed length
	 */
	public NDLSerialNumberNormalizerWithLeftZeroPadding(char multivalueSeparator, int fixedLength) {
		super(multivalueSeparator);
		this.fixedLength = fixedLength;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public String normalizeMore(String serial) {
		if(serial.length() < fixedLength) {
			// further normalization
			return StringUtils.leftPad(serial, fixedLength, '0');
		} else {
			// no change
			return serial;
		}
	}
}