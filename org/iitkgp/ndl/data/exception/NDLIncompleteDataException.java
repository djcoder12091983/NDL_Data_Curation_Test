package org.iitkgp.ndl.data.exception;

/**
 * This exception throws when incomplete data found
 * @author Debasis
 */
public class NDLIncompleteDataException extends RuntimeException {
	
	/**
	 * Constructor
	 * @param message Error message
	 */
	public NDLIncompleteDataException(String message) {
		super(message);
	}
	
	/**
	 * Constructor
	 * @param message Error message
	 * @param cause Error cause
	 */
	public NDLIncompleteDataException(String message, Throwable cause) {
		super(message, cause);
	}

}