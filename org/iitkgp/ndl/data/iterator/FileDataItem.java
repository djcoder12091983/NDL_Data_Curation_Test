package org.iitkgp.ndl.data.iterator;

import java.io.File;

import org.apache.commons.io.FileUtils;

/**
 * File data item class to encapsulate entry name and it's byte contents
 * @author Debasis
 */
public class FileDataItem implements DataItem {
	
	String entryName = null;
	byte[] contents = null;
	
	/**
	 * Constructor
	 * @param item file item
	 */
	public FileDataItem(File item) {
		this.entryName = item.getAbsolutePath();
		try {
			this.contents = FileUtils.readFileToByteArray(item);
		} catch(Exception ex) {
			// error
			throw new IllegalStateException("Error loading file: " + ex.getMessage());
		}
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