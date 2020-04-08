package org.iitkgp.ndl.test.source;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.context.NDLConfigurationContext;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;
import org.iitkgp.ndl.relation.HasPart;
import org.iitkgp.ndl.relation.IsPartOf;
import org.iitkgp.ndl.util.NDLDataUtils;

import com.opencsv.CSVReader;

public class RBStitching1 extends NDLSIPCorrectionContainer {
	
	static final String HANDLE_PREFIX = "12345678_rjsthnbrd/";
	
	String hierarchyFile;
	// hierarchical nodes
	Map<String, Node> nodes = new HashMap<String, Node>();

	public RBStitching1(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
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
	
	@Override
	public void preProcessData() throws Exception {
		super.preProcessData();
		
		turnOffLoadHierarchyFlag();
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
	
	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		
		String id = target.getId();
		String handleSuffix = NDLDataUtils.getHandleSuffixID(id);
		
		if(handleSuffix.startsWith("P")) {
			target.add("dc.source", "Raj-eGyan");
			target.add("dc.source.uri", "http://egyan.rajasthan.gov.in");
		}
		
		delete("dc.relation", "dc.relation.ispartof");
		
		Node node = nodes.get(handleSuffix);
		if(node != null) {
			// has-part is-part
			Node parent = node.parent;
			if(parent != null) {
				// is-part
				String part = NDLDataUtils.serializeIsPartOf(new IsPartOf(HANDLE_PREFIX + parent.id, parent.title)); 
				target.updateSingleValue("dc.relation.ispartof", part);
				log("rb.stitching.log", id + " => " + part);
			}
			if(!node.isLeaf()) {
				// has-part
				List<HasPart> parts = new LinkedList<HasPart>();
				for(Node child : node.children.values()) {
					Node childref = nodes.get(child.id);
					boolean expandable = !childref.isLeaf();
					// TODO it's assumed that virtual nodes have no URL
					parts.add(new HasPart(child.title, HANDLE_PREFIX + child.id, expandable, !expandable));
				}
				String hpart = NDLDataUtils.serializeHasPart(parts);
				target.add("dc.relation.haspart", hpart);
				
				log("rb.stitching.log", id + " => " + hpart);
			}
		}
		
		return true;
	}
	
	public static void main(String[] args) throws Exception {
		
		String input = "/home/dspace/debasis/NDL/NDL_sources/RAJ/in/2019.May.16.20.05.58.rb.v2.Corrected.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/NDL_sources/RAJ/logs";
		String outputLocation = "/home/dspace/debasis/NDL/NDL_sources/RAJ/output";
		String name = "rb.stitch.v1";
		String hierarchyFile = "/home/dspace/debasis/NDL/NDL_sources/RAJ/conf/rb.hierarchy.csv";
		
		NDLConfigurationContext.addConfiguration("compressed.data.process.buffer.size", "10");
		NDLConfigurationContext.addConfiguration("process.display.threshold.limit", "50");
		
		RBStitching1 p = new RBStitching1(input, logLocation, outputLocation, name);
		p.turnOffLoadHierarchyFlag();
		p.hierarchyFile = hierarchyFile;
		p.addTextLogger("rb.stitching.log");
		p.processData();
		
		System.out.println("Done.");
	}
}