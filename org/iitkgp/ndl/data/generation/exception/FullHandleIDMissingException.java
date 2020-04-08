package org.iitkgp.ndl.data.generation.exception;

/**
 * This exception is thrown when full handle id is missing
 * @author Debasis
 */
public class FullHandleIDMissingException extends RuntimeException {
	
	/**
	 * Constructor
	 * @param message Error message
	 */
	public FullHandleIDMissingException(String message) {
		super(message);
	}
	
	/**
	 * Constructor
	 * @param message Error message
	 * @param cause Error cause
	 */
	public FullHandleIDMissingException(String message, Throwable cause) {
		super(message, cause);
	}
}