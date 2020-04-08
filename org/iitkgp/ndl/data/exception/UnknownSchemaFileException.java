package org.iitkgp.ndl.data.exception;

/**
 * This exception is thrown when unknown schema file is identified
 * @author Debasis
 */
public class UnknownSchemaFileException extends RuntimeException {
	
	/**
	 * Constructor
	 * @param message Error message
	 */
	public UnknownSchemaFileException(String message) {
		super(message);
	}
	
	/**
	 * Constructor
	 * @param message Error message
	 * @param cause Error cause
	 */
	public UnknownSchemaFileException(String message, Throwable cause) {
		super(message, cause);
	}

}