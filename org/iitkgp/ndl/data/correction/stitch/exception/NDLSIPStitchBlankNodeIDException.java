package org.iitkgp.ndl.data.correction.stitch.exception;

/**
 * This exception is thrown when intermediate node ID is missing
 * @author Debasis
 */
public class NDLSIPStitchBlankNodeIDException extends RuntimeException {
	
	/**
	 * Constructor
	 * @param message Error message
	 */
	public NDLSIPStitchBlankNodeIDException(String message) {
		super(message);
	}
	
	/**
	 * Constructor
	 * @param message Error message
	 * @param cause Error cause
	 */
	public NDLSIPStitchBlankNodeIDException(String message, Throwable cause) {
		super(message, cause);
	}
}