package org.iitkgp.ndl.data.exception;

/**
 * This exception is thrown when configuration merging mismatch happens
 * @author Debasis
 */
public class ConfigurationMergeException extends RuntimeException {
	
	/**
	 * Constructor
	 * @param message Error message
	 */
	public ConfigurationMergeException(String message) {
		super(message);
	}
	
	/**
	 * Constructor
	 * @param message Error message
	 * @param cause Error cause
	 */
	public ConfigurationMergeException(String message, Throwable cause) {
		super(message, cause);
	}

}