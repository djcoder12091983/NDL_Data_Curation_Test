package org.iitkgp.ndl.data.duplicate.checker;

import java.util.Collection;

/**
 * Duplicate document item
 * @author Debasis
 */
public class DuplicateDocument {
	
	String value;
	Collection<String> ndli_id;
	
	/**
	 * Sets value
	 * @param value value
	 */
	public void setValue(String value) {
		this.value = value;
	}
	
	/**
	 * Sets NDL id details
	 * @param ndli_id NDL id details
	 */
	public void setNdli_id(Collection<String> ndli_id) {
		this.ndli_id = ndli_id;
	}
	
	/**
	 * gets value
	 * @return returns value
	 */
	public String getValue() {
		return value;
	}
	
	/**
	 * gets NDL id list
	 * @return returns NDL list
	 */
	public Collection<String> getNdli_id() {
		return ndli_id;
	}
}