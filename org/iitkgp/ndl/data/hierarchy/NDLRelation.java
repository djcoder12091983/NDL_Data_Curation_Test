package org.iitkgp.ndl.data.hierarchy;

import org.iitkgp.ndl.util.NDLDataUtils;

/**
 * NDL relation detail (stitching or any other relations)
 * @author Debasis
 */
public class NDLRelation {
	
	String title;
	String handle;
	
	/**
	 * Constructor
	 * @param handle handle id to set, internally it stores handle suffix id
	 * @param title corresponding title
	 */
	public NDLRelation(String handle, String title) {
		this.handle = NDLDataUtils.getHandleSuffixID(handle);
		this.title = title;
	}
	
	/**
	 * Gets title
	 * @return returns title
	 */
	public String getTitle() {
		return title;
	}
	
	/**
	 * Gets handle suffix id
	 * @return returns handle suffix id
	 */
	public String getHandle() {
		return handle;
	}
}