package org.iitkgp.ndl.data.correction;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.data.AIPDataItem;
import org.iitkgp.ndl.data.NDLDataItem;
import org.iitkgp.ndl.data.container.DataContainerNULLConfiguration;
import org.iitkgp.ndl.data.hierarchy.NDLRelationNode;
import org.iitkgp.ndl.data.iterator.AIPDataIterator;
import org.iitkgp.ndl.util.NDLDataUtils;

/**
 * <pre>Responsible for NDL data AIP to AIP correction/curation process, logging, assets add etc.</pre>
 * <pre>This class takes care of normalization if any new value added.</pre>
 * <ul>
 * <li>To add custom logic see {@link #correctTargetItem(NDLDataItem)}</li>
 * <li>To add initialization logic see {@link #preProcessData()} {@link #init(DataContainerNULLConfiguration)}</li>
 * <li>To add destroy/complete logic see {@link #postProcessData()} {@link #close()}</li>
 * <li>To add text logging file use
 * {@link #addTextLogger(String)}
 * {@link #addTextLogger(String, String)}
 * </li>
 * <li>To add CSV logging file use
 * {@link #addCSVLogger(String, org.iitkgp.ndl.data.CSVConfiguration)}
 * {@link #addCSVLogger(String)}
 * {@link #addCSVLogger(String, String[], org.iitkgp.ndl.data.CSVConfiguration)}
 * {@link #addCSVLogger(String, String[])}
 * </li>
 * <li>
 * To add mapping resource use
 * {@link #addMappingResource(java.io.File, String)}
 * {@link #addMappingResource(java.io.File, String, String)}
 * </li>
 * </ul>
 * <pre><b>When override a method then don't forget to call super method.</b></pre>
 * <pre>To correct data try to use {@link #add(String, String)} {@link #add(String, String, char)}
 * {@link #deleteIfContains(String, java.util.Set)} {@link #deleteIfNotContains(String, java.util.Set)}
 * {@link #normalize(String...)} {@link #normalize(String, Character)}</pre>
 * <pre>To use more correction AIP(s) see {@link NDLDataItem} methods</pre>
 * <pre><b>Example:</b> {@code
 * // sample AIP to AIP correction
 * public class NDLAIPCorrectionContainerTest extends NDLAIPCorrectionContainer {
 * 	// constructor
 * 	public NDLAIPCorrectionContainerTest(String input, String logLocation, String outputLocation, String name) {
 * 		super(input, logLocation, outputLocation, name);
 * 	}
 * 	// correction logic
 * 	protected boolean correctTargetItem(AIPDataItem target) throws Exception {
 * 		// some sample corrections
 * 		add("dc.xxx.yyy", "some value"); // add some value to field dc.xxx.yyy
 * 		move("dc.xxx.yy1", "dc.xxx.yy2"); // move dc.xxx.yy1 to dc.xxx.yy2
 * 		deleteIfContains("dc.xxx.yy3", "wrong_value1", "wrong_value2"); // delete values with some filters
 * 		delete("dc.xxx.yy4"); // dc.xxx.yy4 field delete
 * 		// etc.
 * 		// success correction
 * 		return true;
 * 	}
 * 	// testing
 * 	public static void main(String[] args) throws Exception {
 * 		String input = "input source"; // flat AIP location or compressed AIP location
 * 		String logLocation = "log location"; // log location if any
 * 		String outputLocation = "output location where to write the data";
 * 		String name = "logical source name";
 * 		NDLAIPCorrectionContainerTest p = new NDLAIPCorrectionContainerTest(input, logLocation, outputLocation, name);
 * 		p.correctData(); // corrects data
 * 	}
 * }
 * }</pre>
 * 
 * @author Debasis
 */
public abstract class NDLAIPCorrectionContainer
		extends AbstractNDLDataCorrectionContainer<AIPDataItem, AIPDataIterator> {
	
	// delete item relation details
	Map<String, Set<String>> deleteAIPItemRelations = new HashMap<String, Set<String>>(2);
	Map<String, NDLRelationNode> aipRelations = new HashMap<String, NDLRelationNode>(2);
	
	// remove permission flag
	boolean removePermissionFlag = true;
	
	/**
	 * Constructor
	 * @param input input source
	 * @param logLocation log location to generate validation logs
	 * @param outputLocation output location where corrected data to be stored
	 * @param name logical data source name, by which logging file(s) etc. prefixed
	 */
	public NDLAIPCorrectionContainer(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, name, outputLocation, false);
		dataReader = new AIPDataIterator(input);
	}
	
	/**
	 * Constructor
	 * @param input input source
	 * @param logLocation log location to generate validation logs
	 * @param outputLocation output location where corrected data to be stored
	 * @param name logical data source name, by which logging file(s) etc. prefixed
	 * @param validationFlag validation flag for data validation
	 */
	public NDLAIPCorrectionContainer(String input, String logLocation, String outputLocation, String name,
			boolean validationFlag) {
		super(input, logLocation, name, outputLocation, false, validationFlag);
		dataReader = new AIPDataIterator(input);
	}
	
	/**
	 * Turns off remove permission flag
	 */
	public void turnOffRemovePermissionFlag() {
		removePermissionFlag = false;
	}
	
	// gets associated handles for a given handle
	// basically it returns all child handles including self item
	Collection<String> getAIPAssociatedHandles(String handle) {
		Set<String> associates = new HashSet<String>(2);
		associates.add(handle);
		Queue<String> nodes = new LinkedList<String>();
		nodes.add(handle);
		// BFS scan to get all nodes of sub-tree
		while(nodes.isEmpty()) {
			NDLRelationNode node = aipRelations.get(nodes.poll());
			Collection<String> children = node.getChildren();
			for(String child : children) {
				associates.add(child);
				nodes.add(child);
			}
		}
		
		return associates;
	}
	
	/**
	 * Source type specific collection community wise hierarchy information loading
	 */
	@Override
	public void preProcessData() throws Exception {
		// super call
		super.preProcessData();
		if(loadHierarchyFlag) {
			System.out.println("Loading AIP hierarchy informations ...");
			// process delete informations if any
			if(!discardItems.isEmpty()) {
				for(String discardItem : discardItems) {
					deletedItems.addAll(getAssociatedHandles(discardItem));
					NDLRelationNode parent = aipRelations.get(discardItem).getParent();
					if(parent != null) {
						// bottom up approach
						NDLRelationNode parentNode;
						do {
							// delete nodes till non-leaf item found
							String parentID = parent.getId();
							parentNode = aipRelations.get(parentID);
							parentNode.deleteChildByHandle(discardItem);
							// delete mapping
							Set<String> deletes = deleteAIPItemRelations.get(parentID);
							if(deletes == null) {
								deletes = new HashSet<String>(2);
								deleteAIPItemRelations.put(parentID, deletes);
							}
							deletes.add(discardItem);
							
							// next
							discardItem = parentID;
							parent = aipRelations.get(discardItem).getParent();
						} while(parent != null && parentNode.isLeaf());
					}
				}
			}
			if(!accessHierarchyFlag) {
				// flag is OFF, destroy relations
				aipRelations.clear();
			}
		}
	}
	
	// gets relation node
	NDLRelationNode getRelationNode(AIPDataItem item) {
		NDLRelationNode node = new NDLRelationNode(item.getId(), item.getSingleValue("dc.title"));
		for(String childHandle : item.getChildHandles()) {
			node.addChild(NDLDataUtils.getHandleSuffixID(childHandle));
		}
		String parentID = item.getParentId();
		if(StringUtils.isNotBlank(parentID)) {
			node.setParent(NDLDataUtils.getHandleSuffixID(parentID));
		}
		return node;
	}
	
	/**
	 * Source type specific item wise collection community information loading
	 */
	@Override
	protected void loadCustomRelationNode(AIPDataItem item) throws Exception {
		// super call
		super.loadCustomRelationNode(item);
		// map
		String id = NDLDataUtils.getHandleSuffixID(item.getId());
		aipRelations.put(id, getRelationNode(item));
	}
	
	/**
	 * Source type specific parent item update if required for deleting an item
	 */
	@Override
	protected void handleParentOnDelete(AIPDataItem parent) throws Exception {
		// super call
		super.handleParentOnDelete(parent);
		// handle hierarchy part if required
		String id = NDLDataUtils.getHandleSuffixID(parent.getId());
		parent.removeChildEntries(deleteAIPItemRelations.get(id));
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void preProcessItem(AIPDataItem item) throws Exception {
		super.preProcessItem(item); // let super call do it's job
		if(escapeHTMLFlag) {
			item.turnOnEscapeHTMLFlag();
		}
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean processItem(AIPDataItem item) throws Exception {
		// remove permission flag if exists
		if(removePermissionFlag) {
			// if permission block to be deleted
			item.removePermissionBlock();
		}
		if(item.isItem()) {
			// only for ITEM
			return super.processItem(item);
		} else {
			// parent items, default true
			return true;
		}
	}
	
	/**
	 * Gets AIP hierarchical information for current item
	 * @param separator information separated by, default is -&gt;
	 * @return returns hierarchical information
	 */
	public String getAIPHierarchyInformation(String separator) {
		AIPDataItem aip = getCurrentTargetItem();
		String parent = NDLDataUtils.getHandleSuffixID(aip.getParentId());
		Stack<String> hierarchy = new Stack<String>();
		while(StringUtils.isNotBlank(parent)) {
			// till parent exists
			NDLRelationNode parentNode = aipRelations.get(parent);
			hierarchy.push(parentNode.getTitle());
			
			NDLRelationNode parentRelation = parentNode.getParent();
			if(parentRelation != null) {
				parent = parentRelation.getId();
			} else {
				// no parent available
				parent = null;
			}
		}
		return NDLDataUtils.join(hierarchy, separator);
	}
	
	/**
	 * Gets AIP hierarchical information separated by '-&gt;' for current item
	 * @return returns hierarchical information
	 * @see #getAIPHierarchyInformation(String)
	 */
	public String getAIPHierarchyInformation() {
		return getAIPHierarchyInformation("->");
	}
	
	/**
	 * Checks whether current item belongs to given hierarchy path
	 * <pre>Path can be partial and it can be case insensitive</pre> 
	 * @param hierarchyName hierarchy path name
	 * @return returns true if belongs to otherwise false
	 */
	public boolean itemBelongsTo(String hierarchyName) {
		AIPDataItem aip = getCurrentTargetItem();
		String parent = NDLDataUtils.getHandleSuffixID(aip.getParentId());
		while(StringUtils.isNotBlank(parent)) {
			// till parent exists
			NDLRelationNode parentNode = aipRelations.get(parent);
			if(StringUtils.containsIgnoreCase(parentNode.getTitle(), hierarchyName)) {
				// membership found
				return true;
			}
			NDLRelationNode parentRelation = parentNode.getParent();
			if(parentRelation != null) {
				parent = parentRelation.getId();
			} else {
				// no parent available
				parent = null;
			}
		}
		return false; // membership not found
	}
	
	/**
	 * Checks whether current item belongs to given collection name
	 * @param collection collection name
	 * @return returns true if belongs to collection otherwise false
	 */
	public boolean itemBelongsToCollection(String collection) {
		return StringUtils.containsIgnoreCase(getCurrentCollectionName(), collection);
	}
	
	/**
	 * Gets current item collection name
	 * @return returns collection name for current item
	 */
	public String getCurrentCollectionName() {
		AIPDataItem aip = getCurrentTargetItem();
		String parent = NDLDataUtils.getHandleSuffixID(aip.getParentId());
		NDLRelationNode parentNode = aipRelations.get(parent);
		return parentNode.getTitle();
	}
	
	/**
	 * Gets current item collection ID
	 * @return returns collection ID for current item
	 */
	public String getCurrentCollectionID() {
		AIPDataItem aip = getCurrentTargetItem();
		return aip.getParentId();
	}
	
	/**
	 * Gets current item collection suffix ID (after slash)
	 * @return returns collection suffix ID (after slash) for current item
	 */
	public String getCurrentCollectionSuffixId() {
		AIPDataItem aip = getCurrentTargetItem();
		return NDLDataUtils.getHandleSuffixID(aip.getParentId());
	}
	
	/**
	 * Gets current item suffix id (after slash)
	 * @return returns suffix ID (after slash) for current item
	 */
	public String getCurrentItemSuffixId() {
		AIPDataItem aip = getCurrentTargetItem();
		return NDLDataUtils.getHandleSuffixID(aip.getId());
	}
}