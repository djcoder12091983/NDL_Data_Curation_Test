package org.iitkgp.ndl.data.correction;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.data.AIPDataItem;
import org.iitkgp.ndl.relation.HasPart;
import org.iitkgp.ndl.relation.IsPartOf;
import org.iitkgp.ndl.util.NDLDataUtils;

import com.opencsv.CSVReader;

/**
 * NDL AIP stitching template.
 * Here use 'hierarachy.tree' generated by {@link NDLAIPPreStitching}
 * <pre>Default assumption is parent-information is kept in 'dc.relation.ispartof' field,
 * developer can override this information.</pre>
 * <pre>Note: AIP data count should be less because whole hierarchy tree required to be placed in memory.</pre>
 * @see NDLAIPPreStitching
 * @author Debasis
 */
public class NDLAIPStitchingCorrection extends NDLAIPCorrectionContainer {
	
	boolean deleteOrderField = false;
	boolean deleteParentInformationField = false;
	
	String hierarchyFile;
	String parentInformationField = "dc.relation.ispartof";
	String orderField;
	
	// hierarchical nodes
	Map<String, Node> nodes = new HashMap<String, Node>();
	
	// hierarchy NODE
	class Node {
		String id;
		String title;
		
		Node(String id, String title) {
			this.id = id;
			this.title = title;
		}
		
		Node(String id, String title, Node parent) {
			this.id = id;
			this.title = title;
			this.parent = parent;
		}
		
		Node parent;
		Map<Integer, Node> children = new TreeMap<Integer, Node>();
		
		void addChild(Node child, int order) {
			children.put(order, child);
		}
		
		boolean isLeaf() {
			return children.isEmpty();
		}
	}

	/**
	 * Constructor
	 * @param input input source
	 * @param logLocation log location to generate validation logs
	 * @param outputLocation output location where corrected data to be stored
	 * @param name logical data source name, by which logging file(s) etc. prefixed
	 * @param hierarchyFile hierarchy information file
	 * @param orderField the order information field
	 */
	public NDLAIPStitchingCorrection(String input, String logLocation, String outputLocation, String name,
			String hierarchyFile, String orderField) {
		super(input, logLocation, outputLocation, name);
		this.hierarchyFile = hierarchyFile;
		this.orderField = orderField;
	}
	
	/**
	 * Sets the parent information (parent handle id) field;
	 * @param parentInformationField parent information field name
	 */
	public void setParentInformationField(String parentInformationField) {
		this.parentInformationField = parentInformationField;
	}
	
	/**
	 * Turns on delete order field
	 */
	public void turnOnDeleteOrderField() {
		deleteOrderField = true;
	}
	
	/**
	 * Turns on delete parent-information field
	 */
	public void turnOnDeleteParentInformationField() {
		deleteParentInformationField = true;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void preProcessData() throws Exception {
		super.preProcessData();
		
		// form tree
		CSVReader reader = NDLDataUtils.readCSV(new File(hierarchyFile), 1);
		
		System.out.println("Constructing tree...");
		
		String tokens[] = null;
		while((tokens = reader.readNext()) != null) {
			String pid = tokens[0];
			if(StringUtils.isBlank(pid)) {
				// invalid parent ID
				continue;
			}
			String cid = tokens[2];
			Node parent = nodes.get(pid);
			if(parent == null) {
				// not created
				parent = new Node(pid, tokens[1]);
				nodes.put(pid, parent);
			}
			Node child = nodes.get(cid);
			if(child == null) {
				child = new Node(cid, tokens[3]);
				nodes.put(cid, child);
			}
			// link established
			parent.addChild(child, Integer.valueOf(tokens[4]));
			child.parent = parent;
		}
		
		reader.close();
		System.out.println("Constructing tree done.");
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	protected boolean correctTargetItem(AIPDataItem target) throws Exception {
		
		if(target.isItem()) {
			// valid item
			String id = target.getId();
			String handlePrefix = NDLDataUtils.getHandlePrefixID(id);
			String handleSuffix = NDLDataUtils.getHandleSuffixID(id);
			
			if(deleteOrderField) {
				target.delete(orderField);
			}
			if(deleteParentInformationField) {
				target.delete(parentInformationField);
			}
			
			Node node = nodes.get(handleSuffix);
			if(node != null) {
				// has-part is-part
				Node parent = node.parent;
				if(parent != null) {
					// is-part
					target.updateSingleValue("dc.relation.ispartof",
							NDLDataUtils.serializeIsPartOf(new IsPartOf(handlePrefix + "/" + parent.id, parent.title)));
				}
				if(!node.isLeaf()) {
					// has-part
					List<HasPart> parts = new LinkedList<HasPart>();
					for(Node child : node.children.values()) {
						Node childref = nodes.get(child.id);
						boolean expandable = !childref.isLeaf();
						// TODO it's assumed that virtual nodes have no URL
						parts.add(new HasPart(child.title, handlePrefix + "/" + child.id, expandable, !expandable));
					}
					target.add("dc.relation.haspart", NDLDataUtils.serializeHasPart(parts));
				}
			}
		}
		
		return true;
	}
}