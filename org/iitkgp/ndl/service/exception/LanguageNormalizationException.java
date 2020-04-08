package org.iitkgp.ndl.service.exception;

/**
 * Throws when NDL language normalization fails
 * @author Debasis
 */
public class LanguageNormalizationException extends RuntimeException {
	
	/**
	 * Constructor
	 * @param message Error message
	 */
	public LanguageNormalizationException(String message) {
		super(message);
	}
	
	/**
	 * Constructor
	 * @param message Error message
	 * @param cause Error cause
	 */
	public LanguageNormalizationException(String message, Throwable cause) {
		super(message, cause);
	}

}