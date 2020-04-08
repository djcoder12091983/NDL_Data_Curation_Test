package org.iitkgp.ndl.data.normalizer.exception;

/**
 * This exception throws when normalizer class loading fails  
 * @author Debasis
 */
public class DataNormalizationLoadException extends RuntimeException {
	
	/**
	 * Constructor
	 * @param message Error message
	 */
	public DataNormalizationLoadException(String message) {
		super(message);
	}
	
	/**
	 * Constructor
	 * @param message Error message
	 * @param cause Error cause
	 */
	public DataNormalizationLoadException(String message, Throwable cause) {
		super(message, cause);
	}

}