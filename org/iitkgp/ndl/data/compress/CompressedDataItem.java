package org.iitkgp.ndl.data.compress;

import org.iitkgp.ndl.data.iterator.DataItem;

/**
 * Compressed data item class to encapsulate entry name and it's byte contents
 * @author Debasis
 */
public class CompressedDataItem implements DataItem {
	
	String entryName = null;
	byte[] contents = null;
	
	/**
	 * Constructor
	 * @param entryName entry name
	 * @param contents byte array contents
	 */
	public CompressedDataItem(String entryName, byte[] contents) {
		this.entryName = entryName;
		this.contents = contents;
	}
	
	/**
	 * Gets entry name
	 * @return returns entry name
	 */
	public String getEntryName() {
		return entryName;
	}
	
	/**
	 * Returns byte array contents
	 * @return returns associated byte array contents
	 */
	public byte[] getContents() {
		return contents;
	}
}