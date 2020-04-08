package org.iitkgp.ndl.core.exception;

import org.iitkgp.ndl.core.NDLFieldTokenSplitter;
import org.iitkgp.ndl.core.NDLFieldTokenSplittingLoader;

/**
 * This exception is thrown when splitter class loading error occurs
 * @see NDLFieldTokenSplittingLoader
 * @see NDLFieldTokenSplitter 
 * @author Debasis
 */
public class NDLFieldTokenSplittingConfigurationLoadingException extends RuntimeException {
	
	/**
	 * Constructor
	 * @param message Error message
	 */
	public NDLFieldTokenSplittingConfigurationLoadingException(String message) {
		super(message);
	}
	
	/**
	 * Constructor
	 * @param message Error message
	 * @param cause Error cause
	 */
	public NDLFieldTokenSplittingConfigurationLoadingException(String message, Throwable cause) {
		super(message, cause);
	}

}