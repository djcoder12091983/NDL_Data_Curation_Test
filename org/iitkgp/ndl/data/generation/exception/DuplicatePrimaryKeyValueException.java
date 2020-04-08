package org.iitkgp.ndl.data.generation.exception;

/**
 * This exception is thrown when primary value is duplicate 
 * @author Debasis
 */
public class DuplicatePrimaryKeyValueException extends RuntimeException {
	
	/**
	 * Constructor
	 * @param message Error message
	 */
	public DuplicatePrimaryKeyValueException(String message) {
		super(message);
	}
	
	/**
	 * Constructor
	 * @param message Error message
	 * @param cause Error cause
	 */
	public DuplicatePrimaryKeyValueException(String message, Throwable cause) {
		super(message, cause);
	}
}