package org.iitkgp.ndl.util;

/**
 * NDL URL types (DSPACE speicifc)
 * @author Debasis
 */
public enum NDLURLType {
	
	/**
	 * XML URL type
	 */
	XMLUI("xmlui"),
	
	/**
	 * JSP UI URL type
	 */
	JSPUI("jspui"),
	
	/**
	 * EPRINTS URL type
	 */
	EPRINTS("eprints"),
	
	/**
	 * Unknown URL type
	 */
	NULL("");
	
	String type;
	
	// constructor
	private NDLURLType(String type) {
		this.type = type;
	}
	
	/**
	 * Gets URL type
	 * @return returns URL type
	 */
	public String getType() {
		return type;
	}
}