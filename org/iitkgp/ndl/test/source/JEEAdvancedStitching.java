package org.iitkgp.ndl.test.source;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.data.AIPDataItem;
import org.iitkgp.ndl.data.compress.CompressedDataItem;
import org.iitkgp.ndl.data.compress.TarGZCompressedFileReader;
import org.iitkgp.ndl.data.compress.TarGZCompressedFileWriter;
import org.iitkgp.ndl.relation.HasPart;
import org.iitkgp.ndl.relation.IsPartOf;
import org.iitkgp.ndl.util.CommonUtilities;
import org.iitkgp.ndl.util.NDLDataUtils;

import com.opencsv.CSVReader;

public class JEEAdvancedStitching {
	
static String HANDLE_PREFIX = "123456789_iitjeeadv";
	
	String hierarchyFile;
	String input;
	String outputLocation;
	
	public JEEAdvancedStitching(String hierarchyFile, String input, String outputLocation) {
		this.hierarchyFile = hierarchyFile;
		this.input = input;
		this.outputLocation = outputLocation;
	}
	
	Map<String, Node> nodes = new HashMap<String, Node>();
	
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
	
	void process() throws Exception {
		// form tree
		CSVReader reader = NDLDataUtils.readCSV(new File(hierarchyFile), 1);
		
		System.out.println("Constructing tree...");
		
		String tokens[] = null;
		while((tokens = reader.readNext()) != null) {
			String pid = tokens[0];
			if(StringUtils.isBlank(pid) || StringUtils.equals(pid, "0")) {
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
		
		System.out.println("Stitching starts....");
		
		// reflect stitching to AIP
		stitch();
	}
	
	void stitch() throws Exception {
		TarGZCompressedFileReader reader = new TarGZCompressedFileReader(new File(input));
		TarGZCompressedFileWriter writer = new TarGZCompressedFileWriter(outputLocation, "jee_advanced.stitching");
		reader.init();
		writer.init();
		
		CompressedDataItem item;
		// aip reader
		int c = 0;
		long start = System.currentTimeMillis();
		while((item = reader.next()) != null) {
			Map<String, byte[]> contents = new HashMap<String, byte[]>(2);
			contents.put(item.getEntryName(), item.getContents());
			AIPDataItem aip = new AIPDataItem();
			aip.load(contents);
			
			if(aip.isItem()) {
				
				aip.delete("ndl.sequenceNo"); // delete sequence no
				
				String id = NDLDataUtils.getHandleSuffixID(aip.getId());
				Node node = nodes.get(id);
				// has-part is-part
				Node parent = node.parent;
				if(parent != null) {
					// is-part
					aip.updateSingleValue("dc.relation.ispartof",
							NDLDataUtils.serializeIsPartOf(new IsPartOf(HANDLE_PREFIX + "/" + parent.id, parent.title)));
				}
				if(!node.isLeaf()) {
					// has-part
					List<HasPart> parts = new LinkedList<HasPart>();
					for(Node child : node.children.values()) {
						Node childref = nodes.get(child.id);
						boolean expandable = !childref.isLeaf();
						parts.add(new HasPart(child.title, HANDLE_PREFIX + "/" + child.id, expandable, !expandable));
					}
					aip.add("dc.relation.haspart", NDLDataUtils.serializeHasPart(parts));
				}
			}
			
			// write
			writer.write(aip.getContents());
			
			if(++c % 1000 == 0) {
				long end = System.currentTimeMillis();
				
				System.out.println("Processed: " + c);
				System.out.println(CommonUtilities.durationMessage(end - start));
			}
		}
		reader.close();
		writer.close();
		
		System.out.println("Total processed: " + c);
		long end = System.currentTimeMillis();
		System.out.println(CommonUtilities.durationMessage(end - start));
	}
	
	public static void main(String[] args) throws Exception {
		
		String input = "/home/dspace/debasis/NDL/generated_xml_data/JEE_advanced/out/2019.Mar.19.12.19.57.jee_advanced_v1/2019.Mar.19.12.19.57.jee_advanced_v1.Corrected.tar.gz";
		String outputLocation = "/home/dspace/debasis/NDL/generated_xml_data/JEE_advanced/out";
		String hierarchyFile = "/home/dspace/debasis/NDL/generated_xml_data/JEE_advanced/logs/hierarachy.tree.csv";
		
		JEEAdvancedStitching p = new JEEAdvancedStitching(hierarchyFile, input, outputLocation);
		p.process();
		
		System.out.println("Done.");
	}
}