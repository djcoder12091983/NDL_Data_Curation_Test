package org.iitkgp.ndl.data.exception;

/**
 * Throws exception when invalid key is given to access configuration data
 * @author Debasis, Aurghya
 */
public class InvalidConfigurationKeyException extends RuntimeException {
	
	/**
	 * Constructor
	 * @param message Error message
	 */
	public InvalidConfigurationKeyException(String message) {
		super(message);
	}
	
	/**
	 * Constructor
	 * @param message Error message
	 * @param cause Error cause
	 */
	public InvalidConfigurationKeyException(String message, Throwable cause) {
		super(message, cause);
	}

}