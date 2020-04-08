package org.iitkgp.ndl.context.custom.exception;

/**
 * Exception used to throw context switch load related error occurs
 * @author Debasis
 */
public class NDLContextSwitchLoadException extends RuntimeException {

	/**
	 * Constructor
	 * @param message Error message
	 */
	public NDLContextSwitchLoadException(String message) {
		super(message);
	}
	
	/**
	 * Constructor
	 * @param message Error message
	 * @param cause Error cause
	 */
	public NDLContextSwitchLoadException(String message, Throwable cause) {
		super(message, cause);
	}
}