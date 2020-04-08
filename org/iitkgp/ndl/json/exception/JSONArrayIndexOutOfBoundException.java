package org.iitkgp.ndl.json.exception;

/**
 * This exception throws when invalid array index is accessed in loaded json object
 * @author Debasis
 */
public class JSONArrayIndexOutOfBoundException extends RuntimeException {
	
	/**
	 * Constructor
	 * @param message Error message
	 */
	public JSONArrayIndexOutOfBoundException(String message) {
		super(message);
	}
	
	/**
	 * Constructor
	 * @param message Error message
	 * @param cause Error cause
	 */
	public JSONArrayIndexOutOfBoundException(String message, Throwable cause) {
		super(message, cause);
	}

}