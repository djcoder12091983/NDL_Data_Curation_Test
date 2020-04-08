package org.iitkgp.ndl.validator.exception;

import org.iitkgp.ndl.data.validator.AbstractNDLDataValidator;

/**
 * This exception throws when NDL schema loading exception. See {@link AbstractNDLDataValidator} for more details.
 * @author Debasis
 */
public class NDLSchemaDetailLoadException extends RuntimeException {
	
	/**
	 * Constructor
	 * @param message Error message
	 */
	public NDLSchemaDetailLoadException(String message) {
		super(message);
	}
	
	/**
	 * Constructor
	 * @param message Error message
	 * @param cause Error cause
	 */
	public NDLSchemaDetailLoadException(String message, Throwable cause) {
		super(message, cause);
	}

}