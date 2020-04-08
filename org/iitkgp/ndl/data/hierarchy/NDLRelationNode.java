package org.iitkgp.ndl.data.hierarchy;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.iitkgp.ndl.data.RowData;
import org.iitkgp.ndl.util.NDLDataUtils;

/**
 * NDL relation detail (stitching or any other relations)
 * It contains both way information (parent as well child)
 * @author Debasis
 */
public class NDLRelationNode {

	RowData self = new RowData(); // self object
	String handle;
	String title;
	String folder;
	NDLRelationNode parent; // parent reference
	Map<String, NDLRelationNode> children = new HashMap<String, NDLRelationNode>(2); // child reference
	
	/**
	 * Constructor
	 * @param handle handle id to set, internally it stores handle suffix id
	 * @param title corresponding title
	 */
	public NDLRelationNode(String handle, String title) {
		this.handle = NDLDataUtils.getHandleSuffixID(handle);
		this.title = title;
	}
	
	/**
	 * Constructor
	 * @param handle handle id to set, internally it stores handle suffix id
	 * @param title corresponding title
	 * @param parent corresponding parent node
	 */
	public NDLRelationNode(String handle, String title, NDLRelationNode parent) {
		this(handle, title);
		this.parent = parent;
	}
	
	/**
	 * Gets self handle id
	 * @return returns self handle id
	 */
	public String getId() {
		return handle;
	}
	
	/**
	 * Gets self title
	 * @return returns self title
	 */
	public String getTitle() {
		return title;
	}
	
	/**
	 * Sets parent detail
	 * @param parent parent reference
	 */
	public void setParent(NDLRelationNode parent) {
		this.parent = parent;
	}
	
	/**
	 * Sets parent detail
	 * @param parentHandle parent handle id to set, internally it stores handle suffix id
	 * @param parentTitle corresponding parent title
	 */
	public void setParent(String parentHandle, String parentTitle) {
		this.parent = new NDLRelationNode(parentHandle, parentTitle);
	}
	
	/**
	 * Sets parent detail
	 * @param parentHandle parent handle id to set, internally it stores handle suffix id
	 */
	public void setParent(String parentHandle) {
		this.parent = new NDLRelationNode(parentHandle, null);
	}
	
	/**
	 * Gets parent reference
	 * @return returns parent
	 */
	public NDLRelationNode getParent() {
		return parent;
	}
	
	/**
	 * Adds child detail if any
	 * @param childHandle parent child handle id to set, internally it stores handle suffix id
	 * @param childTitle corresponding child title
	 */
	public void addChild(String childHandle, String childTitle) {
		children.put(childHandle, new NDLRelationNode(childHandle, childTitle));
	}
	
	/**
	 * Adds child detail if any
	 * @param childHandle parent child handle id to set, internally it stores handle suffix id
	 */
	public void addChild(String childHandle) {
		children.put(childHandle, new NDLRelationNode(childHandle, null));
	}
	
	/**
	 * Gets child handles
	 * @return returns child handles
	 */
	public Collection<String> getChildren() {
		return children.keySet(); 
	}
	
	/**
	 * Gets child node by child handle id
	 * @param childHandle returns child node by child handle id
	 * @return returns gets child node by handle ID
	 */
	public NDLRelationNode getChildByHandle(String childHandle) {
		return children.get(childHandle);
	}
	
	/**
	 * Deletes child handle by child handle id
	 * @param childHandle child handle
	 */
	public void deleteChildByHandle(String childHandle) {
		children.remove(childHandle);
	}
	
	/**
	 * Determines whether node is leaf or not
	 * @return returns true if leaf otherwise false
	 */
	public boolean isLeaf() {
		return children.isEmpty();
	}
	
	/**
	 * Adds value to self item
	 * @param field values added to field
	 * @param values given values to add
	 */
	public void addValue(String field, String ... values) {
		self.addData(field, values);
	}
	
	/**
	 * Adds value to self item
	 * @param field values added to field
	 * @param values given values to add
	 */
	public void addValue(String field, Collection<String> values) {
		self.addData(field, values);
	}
	
	/**
	 * Sets folder for self item
	 * @param folder folder to keep item
	 */
	public void setFolder(String folder) {
		this.folder = folder;
	}
	
	/**
	 * Gets single value for given field
	 * @param field given field name
	 * @return returns value if exists otherwise NULL
	 */
	public String getSingleValue(String field) {
		return self.getSingleValue(field);
	}
	
	/**
	 * Get value/values for given field name
	 * @param field given field name
	 * @return returns value/values if exists otherwise NULL
	 */
	public Collection<String> getValue(String field) {
		return self.getData(field);
	}
	
	/**
	 * gets self item
	 * @return returns self item
	 */
	public RowData getItem() {
		return self;
	}
}