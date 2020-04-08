package org.iitkgp.ndl.data.correction.stitch;

import java.util.HashMap;
import java.util.Map;

/**
 * Simple stitching node holding parent information and children information with order if any
 * @author debasis
 */
public class SimpleStitchingNode {
	
	// handle prefix suffix
	String parentHandlePrefix;
	String parentHandleSuffix;
	
	// children information with order information
	Map<String, Map<String, Integer>> children = new HashMap<>(2);
	
	/**
	 * sets parent information
	 * @param parentHandlePrefix parent handle prefix
	 * @param parentHandleSuffix parent handle suffix
	 */
	public void setParent(String parentHandlePrefix, String parentHandleSuffix) {
		this.parentHandlePrefix = parentHandlePrefix;
		this.parentHandleSuffix = parentHandleSuffix;
	}
	
	/**
	 * Adds child information
	 * @param childHandlePrefix child handle prefix
	 * @param childHandleSuffix child handle suffix
	 * @param order children sequence
	 */
	public void add(String childHandlePrefix, String childHandleSuffix, int order) {
		Map<String, Integer> detail = children.get(childHandlePrefix);
		if(detail == null) {
			detail = new HashMap<>(2);
			children.put(childHandlePrefix, detail);
		}
		detail.put(childHandleSuffix, order);
	}
}