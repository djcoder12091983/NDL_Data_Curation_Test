package org.iitkgp.ndl.test.source;

import java.util.Map;

import org.apache.commons.lang3.CharUtils;
import org.iitkgp.ndl.data.DataType;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.stitch.AbstractNDLSIPStitchingContainer;
import org.iitkgp.ndl.data.correction.stitch.NDLStitchHierarchy;
import org.iitkgp.ndl.data.correction.stitch.NDLStitchHierarchyNode;
import org.iitkgp.ndl.util.NDLDataUtils;

// CK 12 stitching
public class CK12Stitching extends AbstractNDLSIPStitchingContainer {
	
	//static final Pattern BOOK_PATTERN = Pattern.compile(".*");
	
	Map<String, String> virtualNodes = null;
	//Map<String, String> pNodesTitles = null;

	public CK12Stitching(String input, String logLocation, String outputLocation, String logicalName) throws Exception {
		super(input, logLocation, outputLocation, logicalName);
	}
	
	// create vitual node
	NDLStitchHierarchyNode createVirtualNode(String title, String type, String acr) {
		NDLStitchHierarchyNode node = new NDLStitchHierarchyNode(title, title);
		node.addMetadata("dc.type", type);
		node.addMetadata("dc.rights.accessRights", acr);
		return node;
	}
	
	String getOrder(String text) {
		int l = text.length();
		StringBuilder o = new StringBuilder();
		for(int i = l - 1; i >= 0 ; i--) {
			char ch = text.charAt(i);
			if(CharUtils.isAsciiNumeric(ch)) {
				o.append(ch);
			} else {
				break;
			}
		}
		if(o.length() == 0) {
			return String.valueOf(Integer.MAX_VALUE);
		}
		return o.reverse().toString();
	}
	
	// leaf order
	@Override
	protected String itemOrder(SIPDataItem item) {
		// last numeric value (from right)
		return getOrder(NDLDataUtils.getHandleSuffixID(item.getId()));
	}
	
	// hierarchical path for each item if exists
	@Override
	protected NDLStitchHierarchy hierarchy(SIPDataItem item) throws Exception {
		String id = NDLDataUtils.getHandleSuffixID(item.getId());
		
		NDLStitchHierarchy h = new NDLStitchHierarchy();
		// book stitching
		if(item.contains("lrmi.learningResourceType", "book")) {
			// top node
			if(virtualNodes.containsKey(id)) {
				String title = virtualNodes.get(id);
				h.add(createVirtualNode(title, "text", "open"));
			}
		} else if(id.contains("_")) {
			// intermediate nodes
			String tokens[] = id.split("_");
			// top node
			String top = tokens[0];
			if(virtualNodes.containsKey(top)) {
				String title = virtualNodes.get(top);
				h.add(createVirtualNode(title, "text", "open"));
			}
			NDLStitchHierarchyNode l1node = getExistingNodes(top);
			l1node.setOrder(getOrder(top));
			h.add(l1node);
			// next nodes
			int l = tokens.length;
			for(int i = 1; i < l - 1; i++) {
				String t = top + "_" + tokens[i];
				NDLStitchHierarchyNode lnode = getExistingNodes(t);
				lnode.setOrder(tokens[i]);
				h.add(lnode);
			}
			//System.out.println(id + " => " + h);
		} else {
			// simulations stitching
			String title = null;
			if(id.startsWith("psim")) {
				// physics
				title = "Physics Simulation";
			} else if(id.startsWith("csim")) {
				// chemistry
				title = "Chemistry Simulation";
			}
			if(title != null) {
				h.add(createVirtualNode(title, "simulation", "authorized"));
			}
		}
		
		return h;
	}
	
	public static void main(String[] args) throws Exception {
		
		String input = "/home/dspace/debasis/NDL/NDL_sources/CK-12/in/2019.Oct.22.12.14.42.CK12_After_Curation_22_10_18.Corrected.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/NDL_sources/CK-12/out";
		String outputLocation = "/home/dspace/debasis/NDL/NDL_sources/CK-12/out";
		String logicalName = "ck12.stitich";
		
		String pnodefile = "/home/dspace/debasis/NDL/NDL_sources/CK-12/conf/pnodes.titles.csv";
		String vnodefile = "/home/dspace/debasis/NDL/NDL_sources/CK-12/conf/virtual.nodes.csv";
		
		CK12Stitching ck12 = new CK12Stitching(input, logLocation, outputLocation, logicalName);
		
		//ck12.pNodesTitles = NDLDataUtils.loadKeyValue(pnodefile);
		ck12.virtualNodes = NDLDataUtils.loadKeyValue(vnodefile);
		ck12.turnOnExistingNodeLinking(pnodefile);
		
		ck12.turnOnLogRelationDetails();
		ck12.setLeafIsPartLogging(-1);
		ck12.turnOnOrphanNodesLogging();
		ck12.turnOnDuplicateHandlesChecking();
		ck12.addLevelOrder(2, DataType.INTEGER);
		ck12.addLevelOrder(3, DataType.INTEGER);
		ck12.addLevelOrder(4, DataType.INTEGER);
		
		ck12.addGlobalMetadata("dc.description.searchVisibility", "false");
		
		// abbreviated handle id generation strategy
		ck12.setDefaultAbbreviatedHandleIDGenerationStrategy(1);
		// stitch starts
		ck12.stitch();
		
		System.out.println("Done.");
	}
}