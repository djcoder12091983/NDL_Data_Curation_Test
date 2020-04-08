package org.iitkgp.ndl.data.correction.stitch.exception;

/**
 * This exception is thrown when invalid hierarchy information provided
 * @author Debasis
 */
public class NDLStitchInvalidHierarchyInformationException extends RuntimeException {
	
	/**
	 * Constructor
	 * @param message Error message
	 */
	public NDLStitchInvalidHierarchyInformationException(String message) {
		super(message);
	}
	
	/**
	 * Constructor
	 * @param message Error message
	 * @param cause Error cause
	 */
	public NDLStitchInvalidHierarchyInformationException(String message, Throwable cause) {
		super(message, cause);
	}

}
