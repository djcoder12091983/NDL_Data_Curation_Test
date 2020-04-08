package org.iitkgp.ndl.json.exception;

/**
 * This exception throws when expression json path is invalid in loaded json object
 * @author Debasis, Vishal
 */
public class InvalidJSONExpressionException extends RuntimeException {
	
	/**
	 * Constructor
	 * @param message Error message
	 */
	public InvalidJSONExpressionException(String message) {
		super(message);
	}
	
	/**
	 * Constructor
	 * @param message Error message
	 * @param cause Error cause
	 */
	public InvalidJSONExpressionException(String message, Throwable cause) {
		super(message, cause);
	}

}