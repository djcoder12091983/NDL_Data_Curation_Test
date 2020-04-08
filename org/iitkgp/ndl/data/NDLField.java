package org.iitkgp.ndl.data;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.context.NDLDataValidationContext;
import org.iitkgp.ndl.util.NDLDataValidationUtils;
import org.iitkgp.ndl.validator.exception.NDLDataValidationException;

/**
 * <pre>NDL field details encapsulation class, it holds schema, element, qualifier, json-key etc.</pre>
 * <pre>Example: <b>dc.contributor.author</b> returns
 * <b>{schema=dc, element=contrubutor, qualifier=author}</b></pre>
 * <pre>Example: <b>dc.identifier.other:volume</b> returns
 * <b>{schema=dc, element=identifier, qualifier=other, json-key=volume}</b></pre>
 * Note: getXXX API(s) returns NULL if not found, schema and element always exists
 * @author Debasis
 */
public class NDLField implements Comparable<NDLField> {
	
	String field = null;
	String schema = null;
	String element = null;
	String qualifier = null;
	String jsonKey = null;
	boolean status = true;
	
	/**
	 * Constructor
	 * @param field full qualified field name
	 */
	public NDLField(String field) {
		int colon = field.indexOf(':');
		if(colon != -1) {
			this.field = field.substring(0, colon);
			jsonKey = field.substring(colon+1);
		} else {
			this.field = field;
		}
		String tokens[] = this.field.split("\\.");
		if(tokens.length < 2) {
			// error
			throw new NDLDataValidationException(field + " is not a valid field.");
		}
		schema = tokens[0];
		element = tokens[1];
		if(tokens.length == 3) {
			qualifier =  tokens[2];
		}
		
		// validation
		if(StringUtils.isNotBlank(jsonKey)) {
			// field with json key
			status = NDLDataValidationUtils.validate(this.field, jsonKey);
		} else {
			// normal field
			status = NDLDataValidationUtils.validate(this.field);
		}
		if(!status) {
			// error, display it
			NDLDataValidationContext.displayError(this);
		}
	}
	
	/**
	 * Gets field name (exclusion of json-key)
	 * @return returns field name (exclusion of json-key)
	 */
	public String getField() {
		return field;
	}
	
	/**
	 * Gets schema name 
	 * @return returns schema name
	 */
	public String getSchema() {
		return schema;
	}
	
	/**
	 * Gets element name
	 * @return returns element name
	 */
	public String getElement() {
		return element;
	}
	
	/**
	 * Checks whether qualifier exists or not
	 * @return returns true if present otherwise false
	 */
	public boolean hasQualifier() {
		return StringUtils.isNotBlank(qualifier);
	}
	
	/**
	 * Gets qualifier name
	 * @return returns qualifier name
	 */
	public String getQualifier() {
		return qualifier;
	}
	
	/**
	 * Checks whether JOSN key exists or not
	 * @return returns true if present otherwise false
	 */
	public boolean hasJsonKey() {
		return StringUtils.isNotBlank(jsonKey);
	}
	
	/**
	 * Gets json key
	 * @return returns json key
	 */
	public String getJsonKey() {
		return jsonKey;
	}
	
	/**
	 * Checks whether field is valid or not
	 * @return returns field validation status
	 */
	public boolean isValid() {
		return status;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public int compareTo(NDLField other) {
		int c = other.field.compareTo(this.field);
		if(c == 0 && other.hasJsonKey()) {
			return other.jsonKey.compareTo(this.jsonKey);
		} else {
			return c;
		}
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(Object obj) {
		if(obj == null) {
			return false;
		}
		if(obj == this) {
			return true;
		}
		NDLField other = (NDLField)obj;
		boolean f = other.field.equals(this.field);
		if(!f) {
			return f;
		} else if(other.hasJsonKey()) {
			return other.jsonKey.equals(this.jsonKey);
		} else {
			return f;
		}
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {
		int h = 13;
		h += h * this.field.hashCode() * 17;
		if(this.hasJsonKey()) {
			h += h * this.jsonKey.hashCode() * 17;
		}
		return h;
	}
}