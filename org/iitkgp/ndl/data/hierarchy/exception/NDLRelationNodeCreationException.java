package org.iitkgp.ndl.data.hierarchy.exception;

/**
 * This exception is thrown when primary value is duplicate 
 * @author Debasis
 */
public class NDLRelationNodeCreationException extends RuntimeException {
	
	/**
	 * Constructor
	 * @param message Error message
	 */
	public NDLRelationNodeCreationException(String message) {
		super(message);
	}
	
	/**
	 * Constructor
	 * @param message Error message
	 * @param cause Error cause
	 */
	public NDLRelationNodeCreationException(String message, Throwable cause) {
		super(message, cause);
	}

}