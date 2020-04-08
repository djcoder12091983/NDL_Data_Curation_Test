package org.iitkgp.ndl.data.validator;

import java.util.Set;

/**
 * NDL field constraint detail
 * @see NDLFieldType
 * @author Debasis
 *
 */
public class NDLFieldDetail {
	
	String type;
	boolean multi;
	boolean required;
	boolean unique;
	Set<String> keys;
	Set<String> values;
	
	/**
	 * Sets field type
	 * @param type field type
	 */
	public void setType(String type) {
		this.type = type;
	}
	
	/**
	 * Sets multiple flag, whether a field supports multiple value or not
	 * @param multi multiple flag
	 */
	public void setMulti(boolean multi) {
		this.multi = multi;
	}
	
	/**
	 * Sets required/mandate flag, whether a field mandate or not
	 * @param required required/mandate flag
	 */
	public void setRequired(boolean required) {
		this.required = required;
	}
	
	/**
	 * Sets unique flag, whether a field is unique across source or not
	 * @param unique unique flag
	 */
	public void setUnique(boolean unique) {
		this.unique = unique;
	}
	
	/**
	 * Sets control keys for field 
	 * @param keys control keys
	 */
	public void setKeys(Set<String> keys) {
		this.keys = keys;
	}
	
	/**
	 * Sets control values for field
	 * @param values control values
	 */
	public void setValues(Set<String> values) {
		this.values = values;
	}
	
	/**
	 * Gets field type
	 * @return returns field type
	 */
	public String getType() {
		return type;
	}
	
	/**
	 * Whether the field supports multiple value or not
	 * @return returns true if supports otherwise false
	 */
	public boolean isMulti() {
		return multi;
	}
	
	/**
	 * Whether the field is mandate or not
	 * @return returns true if mandate otherwise false
	 */
	public boolean isRequired() {
		return required;
	}
	
	/**
	 * Whether the field is unique is across source or not
	 * @return returns true if so otherwise false
	 */
	public boolean isUnique() {
		return unique;
	}
	
	/**
	 * Gets control keys
	 * @return returns control keys
	 */
	public Set<String> getKeys() {
		return keys;
	}
	
	/**
	 * Control Keys available or not
	 * @return returns true if so otherwise false
	 */
	public boolean keysAvailable() {
		return keys == null || keys.isEmpty();
	}
	
	/**
	 * Gets control values
	 * @return returns control values
	 */
	public Set<String> getValues() {
		return values;
	}
	
	/**
	 * Control values available or not
	 * @return returns true if so otherwise false
	 */
	public boolean valuesAvailable() {
		return values == null || values.isEmpty();
	}
}