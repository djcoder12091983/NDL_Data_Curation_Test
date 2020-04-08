package org.iitkgp.ndl.core;

import java.util.LinkedList;
import java.util.List;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * NDL {@link NodeList}
 * @author Debasis
 */
public class NDLNodeList implements NodeList {

	List<Node> nodes = new LinkedList<Node>();
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getLength() {
		return nodes.size();
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public Node item(int index) {
		return nodes.get(index);
	}
	
	/**
	 * Adds given node
	 * @param node given node
	 */
	public void addNode(Node node) {
		nodes.add(node);
	}
}