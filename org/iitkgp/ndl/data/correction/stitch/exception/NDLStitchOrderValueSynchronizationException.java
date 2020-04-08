package org.iitkgp.ndl.data.correction.stitch.exception;

/**
 * This exception is thrown when level ordering happens with some assigned level order value,
 * and if order values are not synchronized 
 * @author Debasis
 */
public class NDLStitchOrderValueSynchronizationException extends RuntimeException {
	
	/**
	 * Constructor
	 * @param message Error message
	 */
	public NDLStitchOrderValueSynchronizationException(String message) {
		super(message);
	}
	
	/**
	 * Constructor
	 * @param message Error message
	 * @param cause Error cause
	 */
	public NDLStitchOrderValueSynchronizationException(String message, Throwable cause) {
		super(message, cause);
	}
}