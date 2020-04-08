package org.iitkgp.ndl.data.iterator;

/**
 * Data item class to encapsulate entry name and it's byte contents
 * @author Debasis
 */
public interface DataItem {
	
	/**
	 * Gets entry name
	 * @return returns entry name
	 */
	public String getEntryName();
	
	/**
	 * Returns byte array contents
	 * @return returns associated byte array contents
	 */
	public byte[] getContents();

}
