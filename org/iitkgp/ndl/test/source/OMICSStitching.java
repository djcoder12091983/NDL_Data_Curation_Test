package org.iitkgp.ndl.test.source;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.container.ConfigurationData;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;
import org.iitkgp.ndl.relation.HasPart;
import org.iitkgp.ndl.relation.IsPartOf;
import org.iitkgp.ndl.util.NDLDataUtils;
import org.xml.sax.SAXException;

public class OMICSStitching extends NDLSIPCorrectionContainer {
	
long parentFolderCounter = 0;
	
	class StitchingDetail {
		String handle;
		String title;
		String journal;
		String volume;
		String issue;
		String journalID;
		String volID;
		String issueID;
		String volKey;
		String issueKey;
		String collection;
		
		public StitchingDetail(String handle, String title, String journal, String volume, String issue) {
			this.handle = handle;
			this.title = title;
			this.journal = journal;
			this.volume = volume;
			this.issue = issue;
			
			if(StringUtils.isBlank(volume) && StringUtils.isBlank(issue)) {
				// special issue
				issueKey = journal + ":SI";
			} else {
				if(StringUtils.isNotBlank(volume)) {
					volKey = journal + ":" + volume;
				} else {
					volKey = journal + ":SV";
				}
				
				if(StringUtils.isNotBlank(issue)) {
					issueKey = volKey + ":" + issue;
				} else {
					issueKey = volKey + ":SI";
				}
			}
		}
	}
	
	List<StitchingDetail> items = new ArrayList<StitchingDetail>();
	Map<String, String> journalHandles = new HashMap<String, String>();
	Map<String, String> volumeHandles = new HashMap<String, String>();
	Map<String, String> issueHandles = new HashMap<String, String>();
	Map<String, NDLStitchingNode> jnodes = new HashMap<String, NDLStitchingNode>();
	
	// construction
	public OMICSStitching(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	// correction
	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
	
		String title = target.getSingleValue("dc.title");
		if(target.contains("lrmi.learningResourceType", "journal")) {
			// remove the item later on it will be saved
			jnodes.put(title, new NDLStitchingNode(target));
			return false;
		}
		
		String journal = NDLDataUtils.removeMultipleSpaces(target.getSingleValue("dc.identifier.other:journal"));
		if(StringUtils.isBlank(journal)) {
			// cross-check
			System.err.println("Blank Journal: " + target.getId());
		}
		
		String handle = target.getId();
		int p = handle.indexOf('/');
		String prefix = handle.substring(0, p);
		String id = handle.substring(p + 1);
		
		String volume = target.getSingleValue("dc.identifier.other:volume");
		String issue = target.getSingleValue("dc.identifier.other:issue");
		
		String journalID = journalHandles.get(journal);
		if(StringUtils.isBlank(journalID)) {
			String key = "journals." + ConfigurationData.escapeDot(journal);
			if(containsMappingKey(key)) {
				journalID = getMappingKey(key + ".Handle");
				journalHandles.put(journal, journalID);
			} else {
				// cross check
				throw new IllegalStateException("ERROR[" + handle + "]: " + journal + " is not found.");
			}
		}
		
		String lastNodeTitle;
		String volID = null, issueID;
		if(StringUtils.isBlank(volume) && StringUtils.isBlank(issue)) {
			String issueKey = journal + ":SI";
			issueID = issueHandles.get(issueKey);
			if(StringUtils.isBlank(issueID)) {
				issueID = prefix + "/I-" + id;
				issueHandles.put(issueKey, issueID);
			}
			lastNodeTitle = "Issue: Special";
		} else {
			String volKey, issueKey;
			if(StringUtils.isNotBlank(volume)) {
				volKey = journal + ":" + volume;
			} else {
				volKey = journal + ":SV";
			}
			if(StringUtils.isNotBlank(issue)) {
				issueKey = volKey + ":" + issue;
			} else {
				issueKey = volKey + ":SI";
			}
			volID = volumeHandles.get(volKey);
			if(StringUtils.isBlank(volID)) {
				volID = prefix + "/V-" + id;
				volumeHandles.put(volKey, volID);
			}
			issueID = issueHandles.get(issueKey);
			if(StringUtils.isBlank(issueID)) {
				issueID = prefix + "/I-" + id;
				issueHandles.put(issueKey, issueID);
			}
			lastNodeTitle = "Issue: " + NDLDataUtils.NVL(issue, "Special");
		}
		target.add("dc.relation.ispartof", NDLDataUtils.serializeIsPartOf(new IsPartOf(issueID, lastNodeTitle)));
		
		// add items to sort for stitching
		StitchingDetail detail = new StitchingDetail(handle, title, journal, volume, issue);
		detail.journalID = journalID;
		detail.volID = volID;
		detail.issueID = issueID;
		String f = target.getFolder();
		detail.collection = f.substring(0, f.lastIndexOf('/'));
		
		items.add(detail);
		
		return true;
	}
	
	// stitching preparation
	@Override
	public void postProcessData() throws Exception {
		super.postProcessData(); // super call
		System.out.println("Sorting and writing data for stitching ...");
		// sort and stitch
		Collections.sort(items, new Comparator<StitchingDetail>() {
			// comparison logic
			@Override
			public int compare(StitchingDetail first, StitchingDetail second) {
				String j1 = first.journal;
				String j2 = second.journal;
				int c = j1.compareTo(j2);
				if(c == 0) {
					// default volume/issue is 0
					// descending order
					Integer vol1 = Integer.valueOf(NDLDataUtils.NVL(first.volume, "0"));
					Integer vol2 = Integer.valueOf(NDLDataUtils.NVL(second.volume, "0"));
					c = vol2.compareTo(vol1);
					if(c == 0) {
						Integer issue1 = Integer.valueOf(NDLDataUtils.NVL(first.issue, "0"));
						Integer issue2 = Integer.valueOf(NDLDataUtils.NVL(second.issue, "0"));
						return issue2.compareTo(issue1);
					} else {
						return c;
					}
				} else {
					return c;
				}
			}
		});
		
		// stitch
		stitch();
	}
	
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
	}
	
	// stitching
	void stitch() throws Exception {
		System.out.println("Stitching ....");
		
		/*System.out.println(journalHandles.size());
		System.out.println(volumeHandles.size());
		System.out.println(issueHandles.size());
		System.out.println(jnodes.size());*/
		
		NDLStitchingNode root = new NDLStitchingNode(); // stitching root node
		
		int limit = 10000; // stitching limit
		int counter = 0;
		String prevJournal = null;
		for(StitchingDetail item : items) {
			String journal = item.journal;
			if(!StringUtils.equals(prevJournal, journal) && counter >= limit) {
				// new journal encounters, flush the items
				System.out.println("Flushing ... " + counter + " items");
				writeStitchingData(root); // persist

				// reset
				root.resetChildren();
				counter = 0;
			}
			
			// dummy item
			// no need of prefix notation 
			SIPDataItem dummy = NDLDataUtils.createBlankSIP(item.handle);
			dummy.add("dc.title", item.title);
			
			// first node journal
			NDLStitchingNode jnode = root.getChildByKey(journal);
			if(jnode == null) {
				// not exist
				// create
				jnode = jnodes.get(journal);
				jnodes.remove(journal); // remove the key TODO need to see
				root.addChild(journal, jnode);
			}
			
			String issn = getMappingKey("journals." + ConfigurationData.escapeDot(journal) + ".ISSN");
			String volKey =  item.volKey;
			NDLStitchingNode lastnode;
			if(StringUtils.isNotBlank(volKey)) {
				NDLStitchingNode vnode = jnode.getChildByKey(volKey);
				if(vnode == null) {
					// not exist
					// create
					vnode = new NDLStitchingNode(item.volID, jnode);
					jnode.addChild(volKey, vnode);
					// add value
					vnode.setFolder(item.collection + "/" + ++parentFolderCounter);
					vnode.addValue("dc.title", "Volume: " + NDLDataUtils.NVL(item.volume, "Special"));
					vnode.addValue("dc.source", "OMICS International");
					vnode.addValue("dc.source.uri", "https://www.omicsonline.org/");
					vnode.addValue("dc.identifier.issn", issn);
					vnode.addValue("dc.description.searchVisibility", "false");
				}
				lastnode = vnode;
			} else {
				// special issue
				lastnode = jnode;
			}
			
			String issueKey = item.issueKey;
			NDLStitchingNode inode = lastnode.getChildByKey(issueKey);
			if(inode == null) {
				// not exist
				// create
				inode = new NDLStitchingNode(item.issueID, lastnode);
				lastnode.addChild(issueKey, inode);
				// add value
				inode.setFolder(item.collection + "/" + ++parentFolderCounter);
				inode.addValue("dc.title", "Issue: " + NDLDataUtils.NVL(item.issue, "Special"));
				inode.addValue("dc.source", "OMICS International");
				inode.addValue("dc.source.uri", "https://www.omicsonline.org/");
				inode.addValue("dc.identifier.issn", issn);
				inode.addValue("dc.description.searchVisibility", "false");
			}
			
			// has part
			inode.addChild(item.handle, new NDLStitchingNode(dummy));
			
			counter++; // item counter
			prevJournal = journal; // previous journal
		}
		
		if(counter > 1) {
			// flush the items
			System.out.println("Flushing ... " + counter + " items");
			writeStitchingData(root); // persist
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
						false);
				parts.add(p);
			}
			if(!parts.isEmpty()) {
				// valid case
				String hasPartJson = NDLDataUtils.getJson(parts);
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
		
		String input = "/home/dspace/debasis/NDL/generated_xml_data/OMICS/2019.Jan.16.11.47.03.omicsV2.Corrected.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/generated_xml_data/OMICS/logs";
		String outputLocation = "/home/dspace/debasis/NDL/generated_xml_data/OMICS/out";
		String name = "OMICS.stitch.V3";
		
		String journalFile = "/home/dspace/debasis/NDL/generated_xml_data/OMICS/conf/2019.Jan.16.12.00.12.omics.journals.journals_1.csv";
		
		OMICSStitching p = new OMICSStitching(input, logLocation, outputLocation, name);
		p.turnOffLoadHierarchyFlag();
		p.addTextLogger("stitching.log");
		p.addMappingResource(journalFile, "Journal", "journals");
		p.correctData();
		
		System.out.println("Done.");
	}
}