package org.iitkgp.ndl.data.normalizer;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.util.NDLDataUtils;

/**
 * Simple name normalizer
 * TODO Assumptions
 * @author Debasis
 * @see NDLDataUtils#normalizeSimpleNameByWrongNameTokens(String, Set, boolean)
 * @see NDLDataUtils#normalizeSimpleNameByWrongNameTokens(String, Set)
 */
public class NDLSimpleNameNormalizer extends NDLDataNormalizer {
	
	// wrong tokens during name normalization
	Set<String> wrongNameTokens = new HashSet<String>(2);
	Set<String> wrongNames = new HashSet<String>(2);
	
	/**
	 * default constructor
	 */
	public NDLSimpleNameNormalizer() {
		// default
	}
	
	/**
	 * Sets multiple value separator if any
	 * @param multivalueSeparator multiple value separator
	 */
	public NDLSimpleNameNormalizer(char multivalueSeparator) {
		super(multivalueSeparator);
	}
	
	/**
	 * Adds wrong name tokens
	 * @param wrongNameTokens given wrong name tokens
	 * @see NDLDataUtils#normalizeSimpleNameByWrongNameTokens(String, Set, boolean)
	 * @see NDLDataUtils#normalizeSimpleNameByWrongNameTokens(String, Set)
	 */
	public void addWrongNameToken(String ... wrongNameTokens) {
		for(String wrongNameToken : wrongNameTokens) {
			this.wrongNameTokens.add(wrongNameToken);
		}
	}
	
	/**
	 * Adds wrong names
	 * @param wrongNames given wrong names
	 */
	public void addWrongName(String ... wrongNames) {
		for(String wrongName : wrongNames) {
			this.wrongNames.add(wrongName);
		}
	}
	
	/**
	 * Adds wrong name tokens
	 * @param wrongNameTokens given wrong name tokens
	 * @see NDLDataUtils#normalizeSimpleNameByWrongNameTokens(String, Set, boolean)
	 * @see NDLDataUtils#normalizeSimpleNameByWrongNameTokens(String, Set)
	 */
	public void addWrongNameToken(Collection<String> wrongNameTokens) {
		this.wrongNameTokens.addAll(wrongNameTokens);
	}
	
	/**
	 * Adds wrong names
	 * @param wrongNames given wrong names
	 */
	public void addWrongName(Collection<String> wrongNames) {
		this.wrongNames.addAll(wrongNames);
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
			String modifiedName = NDLDataUtils.normalizeSimpleName(value, wrongNameTokens, wrongNames, true);
			if(StringUtils.isNotBlank(modifiedName)) {
				normalizedValues.add(modifiedName);
			}
		}
		return normalizedValues;
	}

}