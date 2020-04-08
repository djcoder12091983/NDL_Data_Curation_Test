package org.iitkgp.ndl.data.correction.stitch.exception;

/**
 * This exception is thrown when existing node not found
 * @author Debasis
 */
public class NDLSIPStitchExistingNodeNotFoundException extends RuntimeException {
	
	/**
	 * Constructor
	 * @param message Error message
	 */
	public NDLSIPStitchExistingNodeNotFoundException(String message) {
		super(message);
	}
	
	/**
	 * Constructor
	 * @param message Error message
	 * @param cause Error cause
	 */
	public NDLSIPStitchExistingNodeNotFoundException(String message, Throwable cause) {
		super(message, cause);
	}

}
