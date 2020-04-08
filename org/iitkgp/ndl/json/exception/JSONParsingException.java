package org.iitkgp.ndl.json.exception;

/**
 * Json parsing error exception
 * @author Debasis
 */
public class JSONParsingException extends RuntimeException {
	
	/**
	 * Constructor
	 * @param message Error message
	 */
	public JSONParsingException(String message) {
		super(message);
	}
	
	/**
	 * Constructor
	 * @param message Error message
	 * @param cause Error cause
	 */
	public JSONParsingException(String message, Throwable cause) {
		super(message, cause);
	}

}