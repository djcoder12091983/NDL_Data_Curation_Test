package org.iitkgp.ndl.data.correction.stitch;

/**
 * Stitching node creation result
 * @see NDLStitchHierarchyTreeNode
 * @author Debasis
 */
public class NDLStitchHierarchyTreeNodeCreation {

	NDLStitchHierarchyTreeNode node; // created node
	boolean created = true;
	int bytes4Creation = 0;
	boolean invalidHandle = false; // handle ID validation
	// this flags are required for: if a node is already marked LEAF but now it would be part of INTERMEDIATE_NODE
	// then existing node will be replaced by new node
	boolean forceRemoved = false;
	String removedHandleID;
	
	/**
	 * Constructor
	 * @param node created node
	 * @param created creation flag true/false
	 * @param bytes bytes consumed for this node
	 */
	public NDLStitchHierarchyTreeNodeCreation(NDLStitchHierarchyTreeNode node, boolean created, int bytes) {
		this.node = node;
		this.created = created;
		this.bytes4Creation = bytes;
	}
	
	/**
	 * Constructor
	 * @param node created node
	 * @param created creation flag true/false
	 * @param bytes bytes consumed for this node
	 * @param invalidHandle whether handle id is valid or not
	 */
	public NDLStitchHierarchyTreeNodeCreation(NDLStitchHierarchyTreeNode node, boolean created, int bytes,
			boolean invalidHandle) {
		this(node, created, bytes);
		this.invalidHandle = invalidHandle;
	}
	
	/**
	 * gets created node
	 * @return returns created node
	 */
	public NDLStitchHierarchyTreeNode getNode() {
		return node;
	}
	
	/**
	 * gets creation flag
	 * @return returns creation flag true/false
	 */
	public boolean isCreated() {
		return created;
	}
	
	/**
	 * Returns handle validation status
	 * @return returns true false accordingly
	 */
	public boolean isInvalidHandle() {
		return invalidHandle;
	}
	
	/**
	 * returns whether node forcefully removed or not
	 * @return returns true/false
	 */
	public boolean isForceRemoved() {
		return forceRemoved;
	}
	
	/**
	 * returns removed handle ID if forcefully removed
	 * @return returns removed handle ID
	 */
	public String getRemovedHandleID() {
		return removedHandleID;
	}
}