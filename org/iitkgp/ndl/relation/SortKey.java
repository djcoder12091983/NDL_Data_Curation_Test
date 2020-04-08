package org.iitkgp.ndl.relation;

/**
 * NDL Stitching related sort-key class
 * @author Debasis
 */
public class SortKey {
	
	String type;
	String value;
	String key;
	
	/**
	 * Sort key text type
	 */
	public static String STRING_TYPE = "String";
	
	/**
	 * Sort key numeric(int) type
	 */
	public static String INTEGER_TYPE = "Integer";
	
	/**
	 * Sort key big numeric(long) type
	 */
	public static String LONG_TYPE = "Long";
	
	/**
	 * Constructor
	 * @param type sort key type
	 * @param value sort value
	 * @param key sort key
	 */
	public SortKey(String type, String value, String key) {
		this.type = type;
		this.value = value;
		this.key = key;
	}
	
	/**
	 * Gets sort key type
	 * @return returns sort key type
	 */
	public String getType() {
		return type;
	}
	
	/**
	 * Gets sort value
	 * @return returns sort value
	 */
	public String getValue() {
		return value;
	}
	
	/**
	 * Returns sort key
	 * @return returns sort key
	 */
	public String getKey() {
		return key;
	}
}