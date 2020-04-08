package org.iitkgp.ndl.data.exception;

/**
 * This exception is thrown when NDL field is not JSON-Keyed
 * @author Debasis
 */
public class FieldIsNotJSONKeyedException extends RuntimeException {
	
	/**
	 * Constructor
	 * @param message Error message
	 */
	public FieldIsNotJSONKeyedException(String message) {
		super(message);
	}
	
	/**
	 * Constructor
	 * @param message Error message
	 * @param cause Error cause
	 */
	public FieldIsNotJSONKeyedException(String message, Throwable cause) {
		super(message, cause);
	}

}