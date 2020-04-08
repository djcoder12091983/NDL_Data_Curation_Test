package org.iitkgp.ndl.data.validator;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.data.NDLField;
import org.iitkgp.ndl.data.log.TextLogger;

/**
 * It avoids same error message agaian and again
 * @author Debasis
 */
public class UniqueErrorTracker {
	
	// unique errors
	Set<String> errors = new HashSet<String>(2);
	
	/**
	 * Displays error as text (whole text message)
	 * @param message error text message
	 * @return returns logged message
	 */
	public String displayError(String message) {
		return displayError(message, null);
	}
	
	/**
	 * Displays error as text (whole text message)
	 * @param message error text message
	 * @param logger logger if externally logging required
	 * @return returns logged message
	 */
	public String displayError(String message, TextLogger logger) {
		if(!errors.contains(message)) {
			if(logger != null) {
				// if logger is mentioned
				try {
					logger.log(message);
				} catch(IOException ex) {
					// suppress error
				}
			}
			System.err.println(message);
			errors.add(message); // track
		}
		return message;
	}
	
	/**
	 * Display error field along with value if any
	 * @param field field detail
	 * @param value associated value
	 * @return returns error message
	 */
	public String displayError(NDLField field, String value) {
		return displayError(field, value, null); // logger is NULL
	}
	
	/**
	 * Display error field along with value if any
	 * @param field field detail
	 * @param value associated value
	 * @param logger logger to log error message
	 * @return returns error message
	 */
	public String displayError(NDLField field, String value, String id, TextLogger logger) {
		// invalid field
		String name = getName(field, value);
		String message = (StringUtils.isNotBlank(id) ? ("ID: " + id + " ") : "") + "Field: \"" + name
				+ "\" is not NDL registered.";
		if(!errors.contains(name)) {
			// new error
			if(logger != null) {
				// if logger is mentioned
				try {
					logger.log(message);
				} catch(IOException ex) {
					// suppress error
				}
			}
			System.err.println(message);
			errors.add(name); // track
		}
		return message;
	}
	
	/**
	 * Display error field along with value if any
	 * @param field field detail
	 * @param value associated value
	 * @param logger logger to log error message
	 * @return returns error message
	 */
	public String displayError(NDLField field, String value, TextLogger logger) {
		return displayError(field, value, null, logger);
	}
	/**
	 * Display error field along with value if any
	 * @param field field detail
	 * @return returns error message
	 */
	public String displayError(NDLField field) {
		return displayError(field, null);
	}
	
	// gets name as key to track error
	String getName(NDLField field, String value) {
		StringBuilder fullName = new StringBuilder(field.getSchema()).append(".").append(field.getElement());
		String q = field.getQualifier();
		if(StringUtils.isNotBlank(q)) {
			fullName.append(".").append(q);
		}
		String k = field.getJsonKey();
		if(StringUtils.isNotBlank(k)) {
			fullName.append(":").append(k);
		}
		if(StringUtils.isNotBlank(value)) {
			fullName.append("=").append(value);
		}
		return fullName.toString();
	}

}