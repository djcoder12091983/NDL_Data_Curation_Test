package org.iitkgp.ndl.service.exception;

/**
 * Throws when NDL DDC normalization fails
 * @author Debasis
 */
public class DDCNormalizationException extends RuntimeException {
	
	/**
	 * Constructor
	 * @param message Error message
	 */
	public DDCNormalizationException(String message) {
		super(message);
	}
	
	/**
	 * Constructor
	 * @param message Error message
	 * @param cause Error cause
	 */
	public DDCNormalizationException(String message, Throwable cause) {
		super(message, cause);
	}

}
