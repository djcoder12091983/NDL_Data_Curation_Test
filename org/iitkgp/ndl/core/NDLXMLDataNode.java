package org.iitkgp.ndl.core;

import org.w3c.dom.Node;

/**
 * XML node decorator
 * @author Debasis
 */
public class NDLXMLDataNode implements NDLDataNode {

	Node xmlnode;
	
	public NDLXMLDataNode(Node node) {
		this.xmlnode = node;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public NDLDataNode getParentNode() {
		Node parent = xmlnode.getParentNode();
		if(parent != null) {
			return new NDLXMLDataNode(parent);
		} else {
			 return null;
		}
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getTextContent() {
		return xmlnode.getTextContent();
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void removeChild(NDLDataNode node) {
		if(node instanceof NDLXMLDataNode) {
			// valid node to remove
			NDLXMLDataNode xmlnode = (NDLXMLDataNode)node;
			this.xmlnode.removeChild(xmlnode.xmlnode);
		} else {
			throw new IllegalStateException("NDLXMLDataNode expected");
		}
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void setTextContent(String value) {
		xmlnode.setTextContent(value);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void remove() {
		this.getParentNode().removeChild(this);
	}
}