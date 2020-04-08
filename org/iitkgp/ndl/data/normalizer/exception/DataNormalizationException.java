package org.iitkgp.ndl.data.normalizer.exception;

/**
 * This exception throws when data normalization process fails
 * @author Debasis
 */
public class DataNormalizationException extends RuntimeException {
	
	/**
	 * Constructor
	 * @param message Error message
	 */
	public DataNormalizationException(String message) {
		super(message);
	}
	
	/**
	 * Constructor
	 * @param message Error message
	 * @param cause Error cause
	 */
	public DataNormalizationException(String message, Throwable cause) {
		super(message, cause);
	}

}