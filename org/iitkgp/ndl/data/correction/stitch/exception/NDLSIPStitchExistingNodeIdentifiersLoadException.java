package org.iitkgp.ndl.data.correction.stitch.exception;

/**
 * This exception is thrown when SIP stitching related existing node linking required
 * and existing nodes identifier file loading fails
 * @author Debasis
 */
public class NDLSIPStitchExistingNodeIdentifiersLoadException extends RuntimeException {
	
	/**
	 * Constructor
	 * @param message Error message
	 */
	public NDLSIPStitchExistingNodeIdentifiersLoadException(String message) {
		super(message);
	}
	
	/**
	 * Constructor
	 * @param message Error message
	 * @param cause Error cause
	 */
	public NDLSIPStitchExistingNodeIdentifiersLoadException(String message, Throwable cause) {
		super(message, cause);
	}
}