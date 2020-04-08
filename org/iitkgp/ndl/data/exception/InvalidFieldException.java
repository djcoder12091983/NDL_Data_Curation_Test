package org.iitkgp.ndl.data.exception;

/**
 * This exception throws when invalid NDL field is accessed.
 * @author Debasis
 */
public class InvalidFieldException extends RuntimeException {
	
	/**
	 * Constructor
	 * @param message Error message
	 */
	public InvalidFieldException(String message) {
		super(message);
	}
	
	/**
	 * Constructor
	 * @param message Error message
	 * @param cause Error cause
	 */
	public InvalidFieldException(String message, Throwable cause) {
		super(message, cause);
	}

}
