package org.iitkgp.ndl.data.exception;

import org.iitkgp.ndl.data.container.ConfigurationData;
import org.iitkgp.ndl.data.container.ConfigurationPool;

/**
 * If primary column not found to load mapping configuration
 * @see ConfigurationPool
 * @see ConfigurationData
 * @author Debasis
 */
public class PrimaryIndexNotFoundException extends RuntimeException {
	
	/**
	 * Constructor
	 * @param message Error message
	 */
	public PrimaryIndexNotFoundException(String message) {
		super(message);
	}
	
	/**
	 * Constructor
	 * @param message Error message
	 * @param cause Error cause
	 */
	public PrimaryIndexNotFoundException(String message, Throwable cause) {
		super(message, cause);
	}

}