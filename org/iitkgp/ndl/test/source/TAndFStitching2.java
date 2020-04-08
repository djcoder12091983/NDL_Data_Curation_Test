package org.iitkgp.ndl.test.source;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;
import org.iitkgp.ndl.relation.HasPart;
import org.iitkgp.ndl.relation.IsPartOf;
import org.iitkgp.ndl.util.NDLDataUtils;
import org.xml.sax.SAXException;

import com.opencsv.CSVReader;

public class TAndFStitching2 extends NDLSIPCorrectionContainer {
	
	String volLocation;
	String issueLocation;
	
	class Item {
		String handle;
		String title;
		
		Item(String handle, String title) {
			this.handle = handle;
			this.title = title;
		}
	}
	
	//Map<String, String> journals = new HashMap<String, String>();
	Map<String, String> jcollections = new HashMap<String, String>();
	Map<String, List<Item>> items = new HashMap<String, List<Item>>();
	Map<String, Item> phandles = new HashMap<String, Item>();
	Map<String, NDLStitchingNode> jnodes = new HashMap<String, NDLStitchingNode>();
	
	// TODO could be moved to somewhere else for reuse
	// stitching node detail
	class NDLStitchingNode {
		SIPDataItem item = null;
		Map<String, NDLStitchingNode> children = new LinkedHashMap<String, NDLStitchingNode>();
		NDLStitchingNode parent = null;
		
		public NDLStitchingNode() {
			// default
		}
		
		public NDLStitchingNode(SIPDataItem item) {
			this.item = item;
		}
		
		public NDLStitchingNode(String handle) throws SAXException, IOException {
			// create by handle ID
			item = NDLDataUtils.createBlankSIP(handle);
		}
		
		public NDLStitchingNode(String handle, NDLStitchingNode parent) throws SAXException, IOException {
			// create by handle ID
			item = NDLDataUtils.createBlankSIP(handle, true);
			this.parent = parent;
		}
		
		void resetChildren() {
			children = new LinkedHashMap<String, NDLStitchingNode>();
		}
		
		void removeChild(String key) {
			children.remove(key);
		}
		
		void addChild(String key, NDLStitchingNode child) {
			children.put(key, child);
		}
		
		NDLStitchingNode getChildByKey(String key) {
			return children.get(key);
		}
		
		void addValue(String field, String ... values) {
			item.add(field, values);
		}
		
		void addValue(String field, Collection<String> values) {
			item.add(field, values);
		}
		
		void setFolder(String folder) {
			item.setFolder(folder);
		}
		
		String getSingleValue(String field) {
			return item.getSingleValue(field);
		}
		
		List<String> getValue(String field) {
			return item.getValue(field);
		}
		
		SIPDataItem getItem() {
			return item;
		}
		
		boolean isEmpty() {
			return children.size() == 0;
		}
	}

	public TAndFStitching2(String input, String logLocation, String outputLocation, String volLocation,
			String issueLocation, String name) {
		super(input, logLocation, outputLocation, name);
		this.volLocation = volLocation;
		this.issueLocation = issueLocation;
	}
	
	void load(String conf) throws Exception {
		File files[] = new File(conf).listFiles();
		for(File file : files) {
			// scan configurations
			CSVReader reader = NDLDataUtils.readCSV(file);
			String tokens[];
			while((tokens = reader.readNext()) != null) {
				String itext = tokens[0];
				List<Item> leaves = items.get(itext);
				if(leaves == null) {
					leaves = new LinkedList<Item>();
					items.put(itext, leaves);
				}
				leaves.add(new Item(tokens[1], tokens[2]));
				phandles.put(tokens[1], new Item(tokens[3], tokens[4]));
			}
			
			reader.close();
		}
	}

	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		
		// move has-part
		if(target.exists("dc.relation.haspart")) {
			// journal
			target.move("dc.relation.haspart", "ndl.sourceMeta.additionalInfo:thumbnail");
			// remove the item later on it will be saved
			String jcode = target.getSingleValue("dc.rights.uri").split("/")[2];
			jnodes.put(jcode, new NDLStitchingNode(target));
			return false;
		}
		
		// leaf parent link
		Item pitem = phandles.get(target.getId());
		if(pitem == null) {
			// handle missing case
			System.err.println("ID: " + target.getId() + " URL: " + target.getSingleValue("dc.identifier.uri"));
		} else {
			// found
			String isPartOfJson = NDLDataUtils.serializeIsPartOf(new IsPartOf(pitem.handle, pitem.title));
			log("stitching.log", target.getId() + " : " + isPartOfJson); // logging
			target.add("dc.relation.ispartof", isPartOfJson);
		}
		
		return true;
	}
	
	@Override
	public void postProcessData() throws Exception {
		super.postProcessData();
		
		int limit = 10000; // stitching limit
		int counter = 0;
		NDLStitchingNode root = new NDLStitchingNode(); // stitching root node
		
		// parent folder counter
		//long parentFolder = 0;
		
		System.out.println("Writing data for stitching ...");
		File jfiles[] = new File(volLocation).listFiles();
		for(File jfile : jfiles) {
			// scan each journal
			String jcode = jfile.getName();
			if(counter >= limit) {
				// new journal encounters, flush the items
				System.out.println("Flushing ... " + counter + " items");
				writeStitchingData(root); // persist

				// reset
				root.resetChildren();
				counter = 0;
			}
			
			// first node journal
			NDLStitchingNode jnode = root.getChildByKey(jcode);
			if(jnode == null) {
				// not exist
				// create
				jnode = jnodes.get(jcode);
				jnode.item.add("lrmi.learningResourceType", "journal");
				jnodes.remove(jcode); // remove the key TODO need to see
				root.addChild(jcode, jnode);
			}
			
			File vfile = jfile.listFiles()[0];
			CSVReader vreader = NDLDataUtils.readCSV(vfile);
			for(String vtokens[] : vreader.readAll()) {
				// scan all volumes
				String v[] = vtokens[0].split(" +");
				String vtext = v[1] + " " + v[2];
				if(v[2].contains("-")) {
					// take latest year
					v[2] = v[2].substring(5);
				}
				String volKey = jcode + "_" + v[1] + "_" + v[2];
				NDLStitchingNode vnode = jnode.getChildByKey(volKey);
				if(vnode == null) {
					// not exist
					// create
					vnode = new NDLStitchingNode("tandfonline/V_" + volKey, jnode);
					jnode.addChild(volKey, vnode);
					
					// add value
					vnode.setFolder(jcollections.get(jcode) + "/V_" + volKey);
					vnode.addValue("dc.title", "Volume: " + vtext);
					vnode.addValue("dc.source", "Taylor & Francis Online");
					vnode.addValue("dc.source.uri", "https://www.tandfonline.com");
					// TODO URI needs to be added
					vnode.addValue("dc.description.searchVisibility", "false");
				}
				
				// scan all issues
				File ilocation = new File(issueLocation, jcode + "/vol_" + v[1] + "_" + v[2]);
				if(!ilocation.exists()) {
					//System.err.println("Issue location not found: " + ilocation);
					log("stitching.error.log", "Issue location not found: " + ilocation);
				} else {
					File ifile = ilocation.listFiles()[0];
					CSVReader ireader = NDLDataUtils.readCSV(ifile);
					for(String itokens[] : ireader.readAll()) {
						String itext = itokens[1].substring(6);
						if(StringUtils.startsWithIgnoreCase(itokens[1], "issue")) {
							// normal
							itext =itokens[1].substring(6);
						} else {
							// suppl
							itext = itokens[1];
						}
						String issueKey = volKey + "_" + itext;
						NDLStitchingNode inode = vnode.getChildByKey(issueKey);
						if(inode == null) {
							// not exist
							// create
							inode = new NDLStitchingNode("tandfonline/I_" + volKey + "_" + itext, vnode);
							vnode.addChild(issueKey, inode);
							
							// add value
							inode.setFolder(jcollections.get(jcode) + "/I_" + volKey + "_" + itext);
							inode.addValue("dc.title",
									"Volume: " + vtext + " Issue: " + itext);
							inode.addValue("dc.source", "Taylor & Francis Online");
							inode.addValue("dc.source.uri", "https://www.tandfonline.com");
							// TODO URI needs to be added
							inode.addValue("dc.description.searchVisibility", "false");
						}
						
						String k = jcode + "_" + v[1] + "_" + v[2] + "_" + itext;
						List<Item> leaves = items.get(jcode + "_" + v[1] + "_" + v[2] + "_" + itext);
						if(leaves == null) {
							// System.err.println("Invalid Issue-Key: " + k);
							log("stitching.error.log", "Invalid Issue-Key: " + k);
							// prune empty intermediate nodes
							if(inode.isEmpty()) {
								// bottom-up approach
								vnode.removeChild(issueKey);
								if(vnode.isEmpty()) {
									jnode.removeChild(volKey);
									if(jnode.isEmpty()) {
										root.removeChild(jcode);
									}
								}
							}
						} else {
							// found case
							for(Item leaf : leaves) {
								// dummy leaf item
								SIPDataItem dummy = NDLDataUtils.createBlankSIP(leaf.handle);
								dummy.add("dc.title", leaf.title);
								
								// has part
								inode.addChild(leaf.handle, new NDLStitchingNode(dummy));
								counter++; // counting leaf items
							}
						}
					}
					
					ireader.close();
				}
			}
			
			vreader.close();
		}
		
		if(counter > 1) {
			// new journal encounters, flush the items
			System.out.println("Flushing ... " + counter + " items");
			writeStitchingData(root); // persist

			// reset
			root.resetChildren();
			counter = 0;
		}
	}
	
	// persist stitching data
	void writeStitchingData(NDLStitchingNode root) throws Exception {
		// BFS scan of stitching tree
		Queue<NDLStitchingNode> nodes = new LinkedList<NDLStitchingNode>();
		nodes.addAll(root.children.values());
		while(!nodes.isEmpty()) {
			NDLStitchingNode node = nodes.poll();
			// haspart update
			List<HasPart> parts = new LinkedList<HasPart>();
			for(NDLStitchingNode child : node.children.values()) {
				HasPart p = new HasPart(child.getSingleValue("dc.title"), child.item.getId(), !child.children.isEmpty(),
						true);
				parts.add(p);
			}
			if(!parts.isEmpty()) {
				// valid case
				log("stitching.log", node.item.getFolder());
				String hasPartJson = NDLDataUtils.getJson(parts);
				/*if(node.children.size() > 2) {
					System.err.println(node.item.getId() + " : " + hasPartJson);
				}*/
				log("stitching.log", node.item.getId() + " : " + hasPartJson); // logging
				node.addValue("dc.relation.haspart", hasPartJson);
				NDLStitchingNode parent = node.parent;
				if(parent != null && parent.item != null) {
					// parent available
					String isPartOfJson = NDLDataUtils
							.getJson(new IsPartOf(parent.item.getId(), parent.item.getSingleValue("dc.title")));
					log("stitching.log", node.item.getId() + " : " + isPartOfJson); // logging
					node.addValue("dc.relation.ispartof", isPartOfJson);
				}
				
				// add extra item
				writeItem(node.getItem());
			}
			
			// next level scanning
			nodes.addAll(node.children.values());
		}
	}
	
	// correct
	public static void main(String[] args) throws Exception {
		
		String input = "/home/dspace/debasis/NDL/generated_xml_data/Taylor and Francis/in/t&f.17012019.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/generated_xml_data/Taylor and Francis/logs";
		String outputLocation = "/home/dspace/debasis/NDL/generated_xml_data/Taylor and Francis/out";
		String name = "tandf.stitching.V1";
		String volLocation = "/home/dspace/debasis/NDL/generated_xml_data/Taylor and Francis/conf/TotalVolumeList";
		String issueLocation = "/home/dspace/debasis/NDL/generated_xml_data/Taylor and Francis/conf/TotalIssueList";
		
		//String jfile = "/home/dspace/debasis/NDL/generated_xml_data/Taylor and Francis/conf/2019.Jan.29.16.19.19.tandfTnF.journals_1.csv";
		String jfile = "/home/dspace/debasis/NDL/generated_xml_data/Taylor and Francis/conf/2019.Feb.04.15.11.02.tandfTnF.jcollections_1.csv";
		String conf = "/home/dspace/debasis/NDL/generated_xml_data/Taylor and Francis/conf/stitching";
	
		TAndFStitching2 p = new TAndFStitching2(input, logLocation, outputLocation, volLocation, issueLocation, name);
		p.turnOffLoadHierarchyFlag();
		//p.journals = NDLDataUtils.loadKeyValue(jfile);
		p.addTextLogger("stitching.log");
		p.addTextLogger("stitching.error.log");
		p.jcollections = NDLDataUtils.loadKeyValue(jfile);
		p.load(conf);
		p.processData();
		
		System.out.println("Done.");
	}
}