package org.iitkgp.ndl.data.correction.stitch;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.data.correction.stitch.context.NDLStitchingContext;
import org.iitkgp.ndl.data.correction.stitch.exception.NDLSIPStitchBlankNodeIDException;
import org.iitkgp.ndl.data.exception.NDLIncompleteDataException;
import org.iitkgp.ndl.util.NDLDataUtils;

/**
 * NDL SIP stitching tree node
 * @author Debasis
 */
public class NDLStitchHierarchyTreeNode {
	
	NDLStitchHierarchyNode data; // underlying data
	Set<Character> retainHandleIDCharacters = new HashSet<>(2);
	
	/**
	 * Constructor
	 * @param data associated underlying data
	 */
	public NDLStitchHierarchyTreeNode(NDLStitchHierarchyNode data) {
		this.data = data;
		retainHandleIDCharacters.add('_'); // retain underscore
	}
	
	/**
	 * Constructor
	 * @param data associated underlying data
	 * @param retainHandleIDCharacters retain specific characters for handle ID
	 */
	public NDLStitchHierarchyTreeNode(NDLStitchHierarchyNode data, char ... retainHandleIDCharacters) {
		this(data);
		// add extra retaining handle ID characters
		for(char ch : retainHandleIDCharacters) {
			this.retainHandleIDCharacters.add(ch);
		}
	}
	
	// children
	NDLStitchHierarchyTreeNode parent;
	Map<String, NDLStitchHierarchyTreeNode> children = new HashMap<>(2);
	long childrenBytes = 0; // children occupied memory
	
	// merge additional meta-data
	void merge(Map<String, Collection<String>> metadata, Map<String, Collection<String>> additionalData) {
		// original existing data
		Map<String, Collection<String>> souce1 = data.metadata;
		Map<String, Collection<String>> souce2 = data.additionalData;
		
		// merge metadata
		for(String k : souce1.keySet()) {
			if(metadata.containsKey(k)) {
				// merging
				souce1.get(k).addAll(metadata.get(k));
			}
			
		}
		// merge additional data
		for(String k : souce2.keySet()) {
			if(additionalData.containsKey(k)) {
				// merging
				souce2.get(k).addAll(additionalData.get(k));
			}
		}
	}
	
	/**
	 * Adds child to current node 
	 * @param child given child to add
	 * @param context this context helps to solve exceptions
	 * @param hpath hierarchy path for auto-generation of handle
	 * @param handle prefix if any
	 * @return returns added child creation result
	 * @throws NDLSIPStitchBlankNodeIDException throws error if ID is missing
	 */
	NDLStitchHierarchyTreeNodeCreation add(NDLStitchHierarchyNode child, NDLStitchingContext context, String hpath,
			String hdlpfx) {
		if(StringUtils.isBlank(child.id)) {
			throw new NDLSIPStitchBlankNodeIDException("ID is missing for intermediate node(" + child.title + ")");
		}
		NDLStitchHierarchyTreeNode cnode = children.get(child.id);
		boolean forceRemoved = false;
		String removeHandleID = null;
		if(cnode != null) {
			// this condition is check if a node is already marked LEAF but now it would be part of INTERMEDIATE_NODE
			// then existing node will be replaced by new node
			if(cnode.data.leaf && !child.leaf) {
				// forcefully removed
				forceRemoved = true;
				removeHandleID = cnode.data.handle;
				children.remove(child.id); // remove the entry TODO may be redundant
			} else {
				// reuse case
				// returns existing node
				// 0 bytes consumed
				// copy existing_node flag if needed
				cnode.data.existingNode = child.existingNode;
				return new NDLStitchHierarchyTreeNodeCreation(cnode, false, 0);
			}
		}
		
		boolean hdlpfxf = StringUtils.isNotBlank(hdlpfx);
		
		cnode = new NDLStitchHierarchyTreeNode(child);
		// add parent information
		NDLStitchHierarchyNode pnode = new NDLStitchHierarchyNode(this.data.id, this.data.title, false, false);
		pnode.handle = this.data.handle;
		pnode.level = -1; // for parent detail no level information required
		cnode.parent = new NDLStitchHierarchyTreeNode(pnode);
		if(StringUtils.isBlank(child.handle)) {
			// intermediate node if handle ID is missing
			child.handle = (hdlpfxf ? (hdlpfx + "/") : "") + "IN_"
					+ NDLDataUtils.normalizeAsHandleID(hpath, retainHandleIDCharacters) + "_"
					+ context.nextAutoGenerateHandleID();
			child.size += child.handle.length(); // handle ID size count
		}
		children.put(child.id, cnode);
		int size = child.size() + pnode.size() + child.id.length();
		childrenBytes += size;
		// adds handle validity
		String handle = child.handle;
		if(StringUtils.isNotBlank(hdlpfx)) {
			// already prefix added to handle ID
			handle = NDLDataUtils.getHandleSuffixID(handle);
		}
		NDLStitchHierarchyTreeNodeCreation res = new NDLStitchHierarchyTreeNodeCreation(cnode, true, size,
				!NDLDataUtils.validateHandle(handle));
		res.forceRemoved = forceRemoved;
		if(forceRemoved) {
			// removed handle ID
			res.removedHandleID = removeHandleID;
		}
		return res;
	}
	
	/**
	 * Adds child to current node 
	 * @param child given child to add
	 * @param context this context helps to solve exceptions
	 * @param handle prefix if any
	 * @return returns added child creation result
	 * @throws NDLSIPStitchBlankNodeIDException throws error if ID is missing
	 */
	NDLStitchHierarchyTreeNodeCreation add(NDLStitchHierarchyNode child, NDLStitchingContext context, String hdlpfx) {
		if(StringUtils.isBlank(child.handle)) {
			throw new NDLIncompleteDataException("HANDLE ID is missing. To use this API handle ID must be defined.");
		}
		return add(child, context, null, hdlpfx);
	}
	
	/**
	 * Adds child to current node 
	 * @param child given child to add
	 * @param context this context helps to solve exceptions
	 * @param handle prefix if any
	 * @return returns added child creation result
	 * @throws NDLSIPStitchBlankNodeIDException throws error if ID is missing
	 */
	NDLStitchHierarchyTreeNodeCreation add(NDLStitchHierarchyNode child, NDLStitchingContext context) {
		return add(child, context, null, null);
	}
	
	/**
	 * Sets parent information
	 * @param parent parent information
	 */
	public void setParent(NDLStitchHierarchyTreeNode parent) {
		this.parent = parent;
	}
	
	/**
	 * returns size in bytes
	 * @return returns size in bytes
	 */
	public long size() {
		return data.size() + childrenBytes;
	}
	
	/**
	 * Gets associated data
	 * @return returns associated data
	 */
	public NDLStitchHierarchyNode getData() {
		return data;
	}
}