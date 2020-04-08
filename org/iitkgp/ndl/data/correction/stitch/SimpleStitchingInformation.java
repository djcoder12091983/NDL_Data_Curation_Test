package org.iitkgp.ndl.data.correction.stitch;

import org.iitkgp.ndl.data.DataOrder;

/**
 * Simple stitching information detail.
 * It works with {@link AbstractNDLSIPExistingNodeLinkingStitchingContainer}
 * @author debasis
 */
public class SimpleStitchingInformation {
	
	String title; // title used in stitching only
	String parentFullHandleID; // parent full handle ID
	int order; // numeric order information
	
	/**
	 * Constructor
	 * @param title title used in stitching only
	 * @param parentFullHandleID parent full handle ID
	 * @param order numeric order information
	 * @param orderType order type
	 */
	public SimpleStitchingInformation(String title, String parentFullHandleID, int order, DataOrder orderType) {
		this.title = title;
		this.parentFullHandleID = parentFullHandleID;
		this.order = order;
		if(orderType == DataOrder.DESCENDING) {
			// reverse ordering
			order *= -1;
		}
	}
	
	/**
	 * Constructor
	 * @param title title used in stitching only
	 * @param parentFullHandleID parent full handle ID
	 * @param order numeric order information (default is ascending)
	 */
	public SimpleStitchingInformation(String title, String parentFullHandleID, int order) {
		this(title, parentFullHandleID, order, DataOrder.ASCENDING);
	}
	
	// getters
	
	public String getTitle() {
		return title;
	}
	
	public int getOrder() {
		return order;
	}
	
	public String getParentFullHandleID() {
		return parentFullHandleID;
	}
}