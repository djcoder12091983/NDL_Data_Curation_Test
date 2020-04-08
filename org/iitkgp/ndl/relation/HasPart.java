package org.iitkgp.ndl.relation;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.data.exception.NDLIncompleteDataException;

/**
 * NDL Stitching related haspart class
 * @author Debasis
 */
public class HasPart {
	
	SortKey sortKey;
	
	boolean visible;
	boolean expandable;
	String handle;
	String title;

	/**
	 * Constructor
	 * @param title title
	 * @param handle handle id
	 * @param expandable whether point is expandable
	 * @param visible whether point is visible or not
	 */
	public HasPart(String title, String handle, boolean expandable, boolean visible) {
		if(StringUtils.isBlank(handle) || StringUtils.isBlank(title)) {
			// error
			throw new NDLIncompleteDataException("HASPART: Handle/Title is missing.");
		}
		this.title = title;
		this.handle = handle;
		this.expandable = expandable;
		this.visible = visible;
	}
	
	/**
	 * Constructor
	 * @param title title
	 * @param handle handle id
	 * @param expandable whether point is expandable
	 * @param visible whether point is visible or not
	 * @param sortKey see {@link SortKey}
	 */
	public HasPart(String title, String handle, boolean expandable, boolean visible, SortKey sortKey) {
		if(StringUtils.isBlank(handle) || StringUtils.isBlank(title)) {
			// error
			throw new NDLIncompleteDataException("HASPART: Handle/Title is missing.");
		}
		this.title = title;
		this.handle = handle;
		this.expandable = expandable;
		this.visible = visible;
		
		this.sortKey = sortKey;
	}
	
	/**
	 * Gets {@link SortKey} detail
	 * @return returns {@link SortKey} detail
	 */
	public SortKey getSortKey() {
		return sortKey;
	}
	
	/**
	 * Returns whether point is expandable or not
	 * @return Returns true point is expandable otherwise false
	 */
	public boolean isExpandable() {
		return expandable;
	}
	
	/**
	 * Returns whether point is visible or not
	 * @return returns true if point is visible otherwise false
	 */
	public boolean isVisible() {
		return visible;
	}
	
	/**
	 * Sets visible true/false
	 * @param visible visible flag true/false
	 */
	public void setVisible(boolean visible) {
		this.visible = visible;
	}
	
	/**
	 * Gets associated handle id
	 * @return returns handle id
	 */
	public String getHandle() {
		return handle;
	}
	
	/**
	 * Sets handle id
	 * @param handle handle id
	 */
	public void setHandle(String handle) {
		this.handle = handle;
	}
	
	/**
	 * Gets title
	 * @return returns title
	 */
	public String getTitle() {
		return title;
	}
	
	/**
	 * Sets associated title
	 * @param title title to set
	 */
	public void setTitle(String title) {
		this.title = title;
	}

}