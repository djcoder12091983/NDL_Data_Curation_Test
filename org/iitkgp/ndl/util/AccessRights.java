package org.iitkgp.ndl.util;

/**
 * Access rights names
 * @author Debasis
 */
public enum AccessRights {
	
	/**
	 * open access right
	 */
	OPEN("open"),
	
	/**
	 * authorized access right
	 */
	AUTHORIZED("authorized"),
	
	/**
	 * limited access right
	 */
	LIMITED("limited"),
	
	/**
	 * NDL access right
	 */
	NDL("ndl"),
	
	/**
	 * subscribed access right
	 */
	SUBSCRIBED("subscribed");
	
	String accessRights;
	
	// constructor
	private AccessRights(String accessRights) {
		this.accessRights = accessRights;
	}
	
	/**
	 * Gets access rights
	 * @return returns access rights
	 */
	public String getAccessRights() {
		return accessRights;
	}
}