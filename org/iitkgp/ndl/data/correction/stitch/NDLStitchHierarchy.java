package org.iitkgp.ndl.data.correction.stitch;

import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.data.SIPDataItem;

/**
 * NDL stitch hierarchy detail
 * @see NDLStitchHierarchyNode
 * @author Debasis
 */
public class NDLStitchHierarchy {
	
	List<NDLStitchHierarchyNode> hierarchy = new LinkedList<>();
	boolean orphanNode = true;
	
	// mention leaf title if needed
	String leafTitle;
	
	/**
	 * Adds hierarchy information in order
	 * @param node hierarchy/intermediate node
	 * @return returns object itself
	 */
	public NDLStitchHierarchy add(NDLStitchHierarchyNode node) {
		hierarchy.add(node);
		orphanNode = false; // part of stitching so it's not orphan node
		return this;
	}
	
	/**
	 * Returns hierarchy path length
	 * @return returns hierarchy path length
	 */
	public int size() {
		return hierarchy.size();
	}
	
	/**
	 * Returns hierarchy is not defined
	 * @return returns hierarchy is not defined
	 */
	public boolean isEmpty() {
		return hierarchy.isEmpty();
	}
	
	/**
	 * returns string representation
	 */
	@Override
	public String toString() {
		StringBuilder txt = new StringBuilder();
		for(NDLStitchHierarchyNode h : hierarchy) {
			txt.append(h.toString()).append('>');
		}
		return txt.deleteCharAt(txt.length() - 1).toString();
	}
	
	/**
	 * returns whether it's orphan node
	 * @return returns true if orphan node otherwise false
	 */
	public boolean isOrphanNode() {
		return orphanNode;
	}
	
	/**
	 * sets orphan node flag (in case of override default behavior)
	 * @param orphanNode given orphan node flag
	 */
	public void setOrphanNode(boolean orphanNode) {
		this.orphanNode = orphanNode;
	}
	
	/**
	 * Sets leaf title, otherwise it reads from title field
	 * @param leafTitle sets leaf title
	 */
	public void setLeafTitle(String leafTitle) {
		this.leafTitle = leafTitle;
	}
	
	/**
	 * gets leaf title, if {@link #setLeafTitle(String)} is not called then
	 * it uses default title (read from title field) 
	 * @param sip SIP item to read default title from title field
	 * in case of {@link #setLeafTitle(String)} not called
	 * @return returns leaf title
	 */
	public String getLeafTitle(SIPDataItem sip) {
		if(StringUtils.isNotBlank(leafTitle)) {
			return leafTitle;
		}
		// default title value read from title field
		return sip.getSingleValue(AbstractNDLSIPStitchingContainer.TITLE_FIELD);
	}
}