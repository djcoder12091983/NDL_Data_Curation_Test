package org.iitkgp.ndl.validator.exception;

/**
 * Invalid NDL field is accessed
 * @author Debasis
 */
public class NDLFieldNotFoundException extends RuntimeException {
	
	/**
	 * Constructor
	 * @param message Error message
	 */
	public NDLFieldNotFoundException(String message) {
		super(message);
	}
	
	/**
	 * Constructor
	 * @param message Error message
	 * @param cause Error cause
	 */
	public NDLFieldNotFoundException(String message, Throwable cause) {
		super(message, cause);
	}

}