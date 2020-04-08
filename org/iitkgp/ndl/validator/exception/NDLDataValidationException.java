package org.iitkgp.ndl.validator.exception;

/**
 * NDL data validation exception
 * @author Debasis
 */
public class NDLDataValidationException extends RuntimeException {
	
	/**
	 * Constructor
	 * @param message Error message
	 */
	public NDLDataValidationException(String message) {
		super(message);
	}
	
	/**
	 * Constructor
	 * @param message Error message
	 * @param cause Error cause
	 */
	public NDLDataValidationException(String message, Throwable cause) {
		super(message, cause);
	}
}