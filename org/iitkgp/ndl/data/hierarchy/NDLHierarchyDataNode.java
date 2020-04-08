package org.iitkgp.ndl.data.hierarchy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.iitkgp.ndl.data.RowData;

/**
 * NDL data node used data hierarchy build
 * @author Debasis
 */
public class NDLHierarchyDataNode {
	
	String id; // data node identifier
	String name; // name or title
	NDLHierarchyDataNode parent; // parent
	Map<String, NDLHierarchyDataNode> children = null; // child nodes by mapped by ID
	RowData data = null;
	
	/**
	 * Constructor
	 * @param id data node ID
	 */
	public NDLHierarchyDataNode(String id) {
		this.id = id;
	}
	
	/**
	 * Constructor
	 * @param id data node ID
	 * @param name data node name
	 */
	public NDLHierarchyDataNode(String id, String name) {
		this.id = id;
		this.name = name;
	}
	
	/**
	 * Constructor
	 * @param id data node ID
	 * @param name data node name
	 * @param parent parent ID
	 */
	public NDLHierarchyDataNode(String id, String name, NDLHierarchyDataNode parent) {
		this.id = id;
		this.name = name;
		this.parent = parent;
	}
	
	/**
	 * Gets ID
	 * @return returns ID
	 */
	public String getId() {
		return id;
	}
	
	/**
	 * Gets node name
	 * @return returns node name
	 */
	public String getName() {
		return name;
	}
	
	/**
	 * Gets parent if any otherwise null
	 * @return returns parent if any otherwise null
	 */
	public NDLHierarchyDataNode getParent() {
		return parent;
	}
	
	/**
	 * Gets data associated with given key
	 * @param key given key
	 * @return returns associated data if any, otherwise null
	 */
	public Collection<String> getData(String key) {
		return data.getData(key);
	}
	
	/**
	 * Adds child to data node
	 * @param child child to add
	 */
	public void addChild(NDLHierarchyDataNode child) {
		if(children == null) {
			children = new HashMap<String, NDLHierarchyDataNode>(2);
		}
		children.put(child.id, child); // parent to child
		child.parent = this; // child to parent
	}
	
	/**
	 * Gets child by id
	 * @param id given ID
	 * @return returns child if found otherwise NULL
	 */
	public NDLHierarchyDataNode getChild(String id) {
		return children != null ? children.get(id) : null;
	}
	
	/**
	 * Adds data to node
	 * @param key attribute
	 * @param value associated value
	 */
	public void addData(String key, String value) {
		if(data != null) {
			data = new RowData();
		}
		data.addData(key, value);
	}
	
	/**
	 * Whether this node belongs to a path by parent name
	 * @param name parent name
	 * @return returns true if belongs otherwise false
	 */
	public boolean belongToName(String name) {
		NDLHierarchyDataNode node = parent;
		while(node != null) {
			if(name.equals(node.name)) {
				// match found
				return true;
			}
			node = node.parent; // next parent
		}
		return false; // full path traversed, but match not found
	}
	
	/**
	 * Whether this node belongs to a path by parent id
	 * @param id parent id
	 * @return returns true if belongs otherwise false
	 */
	public boolean belongToID(String id) {
		NDLHierarchyDataNode node = parent;
		while(node != null) {
			if(id.equals(node.id)) {
				// match found
				return true;
			}
			node = node.parent; // next parent
		}
		return false; // full path traversed, but match not found
	}
	
	/**
	 * Gets parent path for this data node 
	 * @return returns parent in list by deep to root
	 */
	public List<NDLHierarchyDataNode> getParents() {
		List<NDLHierarchyDataNode> parents = new ArrayList<NDLHierarchyDataNode>(2);
		NDLHierarchyDataNode node = parent;
		while(node != null) {
			parents.add(node);
			node = node.parent; // next parent
		}
		return parents;
	}
}