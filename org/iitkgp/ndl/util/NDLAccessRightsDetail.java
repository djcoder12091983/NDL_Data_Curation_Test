package org.iitkgp.ndl.util;

/**
 * NDL access rights detail class
 * For more details see getAccessRights section of
 * <a href="http://www.dataentry.ndl.iitkgp.ac.in/helper/#data-services">Services</a>
 * @author Debasis
 */
public class NDLAccessRightsDetail {
	
	String accessRights = null;
	int numFiles = 0;
	int statusCode = 0;
	
	/**
	 * Constructor
	 * @param accessRights access rights
	 */
	public NDLAccessRightsDetail(String accessRights) {
		this.accessRights = accessRights;
	}
	
	/**
	 * Gets access rights
	 * @return returns access rights
	 */
	public String getAccessRights() {
		return accessRights;
	}
	
	/**
	 * Sets numFiles
	 * @param numFiles numFiles
	 */
	public void setNumFiles(int numFiles) {
		this.numFiles = numFiles;
	}
	
	/**
	 * Gets numFiles
	 * @return returns numFiles
	 */
	public int getNumFiles() {
		return numFiles;
	}
	
	/**
	 * Sets statusCode
	 * @param statusCode statusCode
	 */
	public void setStatusCode(int statusCode) {
		this.statusCode = statusCode;
	}
	
	/**
	 * Gets statusCode
	 * @return returns statusCode
	 */
	public int getStatusCode() {
		return statusCode;
	}

}