package org.iitkgp.ndl.data.validator;

/**
 * NDL field type
 * @author Debasis
 */
public enum NDLFieldType {
	
	/**
	 * table of contents
	 */
	TABLE_OF_CONTENTS("tableOfContents"),
	
	/**
	 * free data type
	 */
	FREE("free"),
	
	/**
	 * control fields
	 */
	CTRL("ctrl"),
	
	/**
	 * fields with controlled key
	 */
	CTRL_KEY("ctrlKey"),
	
	/**
	 * prerequisiteTopic
	 */
	PREREQUISITE_TOPIC("prerequisiteTopic"),
	
	/**
	 * boolean data type
	 */
	BOOL("bool");

	// type text
	String type;
	
	/**
	 * Constructor
	 * @param type type text
	 */
	private NDLFieldType(String type) {
		this.type = type;
	}
	
	/**
	 * Gets field type
	 * @return returns field type
	 */
	public String getType() {
		return type;
	}

}