package org.iitkgp.ndl.data.exception;

/**
 * If multivalued field is accessed for single value extraction
 * @author Debasis
 */
public class NDLMultivaluedException extends RuntimeException {
	
	/**
	 * Constructor
	 * @param message Error message
	 */
	public NDLMultivaluedException(String message) {
		super(message);
	}
	
	/**
	 * Constructor
	 * @param message Error message
	 * @param cause Error cause
	 */
	public NDLMultivaluedException(String message, Throwable cause) {
		super(message, cause);
	}

}
