package org.iitkgp.ndl.service.exception;

/**
 * Throws when NDL date normalization fails
 * @author Debasis
 */
public class DateNormalizationException extends RuntimeException {
	
	/**
	 * Constructor
	 * @param message Error message
	 */
	public DateNormalizationException(String message) {
		super(message);
	}
	
	/**
	 * Constructor
	 * @param message Error message
	 * @param cause Error cause
	 */
	public DateNormalizationException(String message, Throwable cause) {
		super(message, cause);
	}

}