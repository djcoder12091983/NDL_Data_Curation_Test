package org.iitkgp.ndl.service.exception;

/**
 * Throws when NDL service request fails
 * @author Debasis 
 */
public class ServiceRequestException extends RuntimeException {
	
	String serviceURL;
	
	/**
	 * Constructor
	 * @param serviceURL associated service URL
	 * @param message Error message
	 */
	public ServiceRequestException(String serviceURL, String message) {
		super(message);
		this.serviceURL = serviceURL;
	}
	
	/**
	 * Constructor
	 * @param serviceURL associated service URL
	 * @param message Error message
	 * @param cause Error cause
	 */
	public ServiceRequestException(String serviceURL, String message, Throwable cause) {
		super(message, cause);
		this.serviceURL = serviceURL;
	}
	
	/**
	 * Gets service URL
	 * @return returns service URL
	 */
	public String getServiceURL() {
		return serviceURL;
	}

}