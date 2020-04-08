package org.iitkgp.ndl.data.correction.stitch.exception;

/**
 * This exception is thrown when title is missing
 * @author Debasis
 */
public class NDLSIPStitchBlankTitleException extends RuntimeException {
	
	/**
	 * Constructor
	 * @param message Error message
	 */
	public NDLSIPStitchBlankTitleException(String message) {
		super(message);
	}
	
	/**
	 * Constructor
	 * @param message Error message
	 * @param cause Error cause
	 */
	public NDLSIPStitchBlankTitleException(String message, Throwable cause) {
		super(message, cause);
	}

}
