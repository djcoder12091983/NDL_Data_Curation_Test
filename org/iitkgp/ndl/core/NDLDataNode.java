package org.iitkgp.ndl.core;

/**
 * This is NDL SIP/AIP data node interface
 * @author Debasis
 */
public interface NDLDataNode {

	/**
	 * Gets wrapped text content
	 * @return returns text value
	 */
	String getTextContent();
	
	/**
	 * Gets parent node
	 * @return returns parent node, otherwise NULL if not found
	 */
	NDLDataNode getParentNode();
	
	/**
	 * Removes child for a given node reference
	 * @param node given node reference
	 */
	void removeChild(NDLDataNode node);
	
	/**
	 * Sets text value
	 * @param value text value
	 */
	void setTextContent(String value);
	
	/**
	 * Removes the data node
	 */
	void remove();
}