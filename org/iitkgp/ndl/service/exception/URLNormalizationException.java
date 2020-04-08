package org.iitkgp.ndl.service.exception;

/**
 * Throws when URL normalization fails
 * @author Debasis
 */
public class URLNormalizationException extends RuntimeException {
	
	/**
	 * Constructor
	 * @param message Error message
	 */
	public URLNormalizationException(String message) {
		super(message);
	}
	
	/**
	 * Constructor
	 * @param message Error message
	 * @param cause Error cause
	 */
	public URLNormalizationException(String message, Throwable cause) {
		super(message, cause);
	}

}