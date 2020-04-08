package org.iitkgp.ndl.data.correction.stitch.exception;

import org.iitkgp.ndl.data.correction.stitch.AbstractNDLSIPStitchingContainer;

/**
 * This exception is thrown when for a single source cross prefix ID found.
 * For more details see {@link AbstractNDLSIPStitchingContainer#turnOnFullHandleIDConsideration()}
 * @author Debasis
 */
public class NDLSIPStitchCrossPrefixIDException extends RuntimeException {
	
	/**
	 * Constructor
	 * @param message Error message
	 */
	public NDLSIPStitchCrossPrefixIDException(String message) {
		super(message);
	}
	
	/**
	 * Constructor
	 * @param message Error message
	 * @param cause Error cause
	 */
	public NDLSIPStitchCrossPrefixIDException(String message, Throwable cause) {
		super(message, cause);
	}

}
