package org.iitkgp.ndl.relation;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.data.exception.NDLIncompleteDataException;

/**
 * NDL Stitching related is-partof class
 * @author Debasis
 */
public class IsPartOf {
	
	String handle;
	String title;
	
	/**
	 * Constructor
	 * @param handle handle id
	 * @param title title
	 */
	public IsPartOf(String handle, String title) {
		if(StringUtils.isBlank(handle) || StringUtils.isBlank(title)) {
			// error
			throw new NDLIncompleteDataException("ISPART: Handle/Title is missing.");
		}
		this.handle = handle;
		this.title = title;
	}
	
	/**
	 * Sets handle id, use it carefully
	 * @param handle handle id to set
	 */
	public void setHandle(String handle) {
		this.handle = handle;
	}
	
	/**
	 * Gets handle id
	 * @return returns handle id
	 */
	public String getHandle() {
		return handle;
	}
	
	/**
	 * Gets title
	 * @return returns title
	 */
	public String getTitle() {
		return title;
	}
	
	/**
	 * Sets title
	 * @param title title to set
	 */
	public void setTitle(String title) {
		this.title = title;
	}
}
