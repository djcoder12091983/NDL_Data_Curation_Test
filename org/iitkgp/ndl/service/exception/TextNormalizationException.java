package org.iitkgp.ndl.service.exception;

/**
 * Throws when NDL text normalization fails
 * @author Debasis
 */
public class TextNormalizationException extends RuntimeException {
	
	/**
	 * Constructor
	 * @param message Error message
	 */
	public TextNormalizationException(String message) {
		super(message);
	}
	
	/**
	 * Constructor
	 * @param message Error message
	 * @param cause Error cause
	 */
	public TextNormalizationException(String message, Throwable cause) {
		super(message, cause);
	}

}