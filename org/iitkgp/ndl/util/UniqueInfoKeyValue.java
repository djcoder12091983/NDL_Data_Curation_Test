package org.iitkgp.ndl.util;

/**
 *  ndl.sourceMeta.uniqueInfo key value pair details
 * @author Debasis
 */
public class UniqueInfoKeyValue {
	
	String key;
	String value;
	
	/**
	 * Sets key
	 * @param key key
	 */
	public void setKey(String key) {
		this.key = key;
	}
	
	/**
	 * Sets value
	 * @param value value
	 */
	public void setValue(String value) {
		this.value = value;
	}
	
	/**
	 * Gets key
	 * @return returns key
	 */
	public String getKey() {
		return key;
	}
	
	/**
	 * Gets value
	 * @return returns value
	 */
	public String getValue() {
		return value;
	}
}