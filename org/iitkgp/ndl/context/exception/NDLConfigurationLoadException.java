package org.iitkgp.ndl.context.exception;

/**
 * Exception used to throw context load related error occurs
 * @author Debasis
 */
public class NDLConfigurationLoadException extends RuntimeException {
	
	/**
	 * Constructor
	 * @param message Error message
	 */
	public NDLConfigurationLoadException(String message) {
		super(message);
	}
	
	/**
	 * Constructor
	 * @param message Error message
	 * @param cause Error cause
	 */
	public NDLConfigurationLoadException(String message, Throwable cause) {
		super(message, cause);
	}
}