package org.iitkgp.ndl.data.validator;

import java.util.Map;
import java.util.Set;

/**
 * NDL field-wise schema detail
 * @author Debasis
 */
public class NDLFieldSchemaDetail {
	
	Map<String, NDLFieldDetail> response;
	
	/**
	 * JSON response text
	 * @param response JSON response text
	 */
	public void setResponse(Map<String, NDLFieldDetail> response) {
		this.response = response;
	}
	
	/**
	 * JSON response loaded or not
	 * @return true if so otherwise false
	 */
	public boolean available() {
		return response != null;
	}
	
	/**
	 * Gets NDL field detail
	 * @param field field name
	 * @return returns field detail
	 */
	public NDLFieldDetail getDetail(String field) {
		return response.get(field);
	}
	
	/**
	 * Whether field contains in schema or not
	 * @param field field name to check
	 * @return returns true if so otherwise false
	 */
	public boolean containsField(String field) {
		return response.containsKey(field);
	}
	
	/**
	 * Gets controlled values for given field
	 * @param field field name
	 * @return returns control values
	 */
	public Set<String> getControlledValues(String field) {
		return response.get(field).getValues();
	}
	
	/**
	 * Gets controlled keys for given field
	 * @param field field name
	 * @return returns control keys
	 */
	public Set<String> getControlledKeys(String field) {
		return response.get(field).getKeys();
	}

}