package org.iitkgp.ndl.test.source;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;
import org.iitkgp.ndl.relation.HasPart;
import org.iitkgp.ndl.relation.IsPartOf;
import org.iitkgp.ndl.util.NDLDataUtils;
import org.xml.sax.SAXException;

public class SCIRPCorrection extends NDLSIPCorrectionContainer {
	
	long parentFolderCounter = 0;
	
	class StitchingDetail {
		String handle;
		String title;
		String journal;
		String year;
		String volume;
		String issue;
		String date;
		String issn;
		String eissn;
		String spatial;
		String collection;
		
		public StitchingDetail(String handle, String title, String journal, String year, String volume, String issue,
				String date) {
			this.handle = handle;
			this.title = title;
			this.journal = journal;
			this.year = year;
			this.volume = volume;
			this.issue = issue;
			this.date = date;
			
			volKey = journal + ":" + year + ":" + volume;
			issueKey = journal + ":" + year + ":" + volume + ":" + issue;
		}
		
		String journalID;
		String volID;
		String issueID;
		String volKey;
		String issueKey;
	}
	
	List<StitchingDetail> items = new ArrayList<StitchingDetail>();
	Map<String, String> journalHandles = new HashMap<String, String>();
	Map<String, String> volumeHandles = new HashMap<String, String>();
	Map<String, String> issueHandles = new HashMap<String, String>();
	
	// construction
	public SCIRPCorrection(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	// correction
	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		
		target.add("lrmi.educationalAlignment.educationalLevel", "ug_pg");
		target.add("lrmi.educationalUse", "research");
		target.add("lrmi.typicalAgeRange", "18-22", "22+");
		
		String handle = target.getId();
		int p = handle.indexOf('/');
		String prefix = handle.substring(0, p);
		String id = handle.substring(p + 1);
		String title = target.getSingleValue("dc.title");
		String journal = target.getSingleValue("dc.identifier.other:journal");
		String year = target.getSingleValue("dc.date.copyright");
		String volume = target.getSingleValue("dc.identifier.other:volume");
		String issue = target.getSingleValue("dc.identifier.other:issue");
		String date = target.getSingleValue("dc.publisher.date");
		String issn = target.getSingleValue("dc.identifier.issn");
		String eissn = target.getSingleValue("dc.identifier.other:eissn");
		String spatial = target.getSingleValue("dc.coverage.spatial");
		
		String journalID = journalHandles.get(journal);
		if(StringUtils.isBlank(journalID)) {
			journalID = prefix + "/J-" + id;
			journalHandles.put(journal, journalID);
		}
		String volKey = journal + ":" + year + ":" + volume;
		String volID = volumeHandles.get(volKey);
		if(StringUtils.isBlank(volID)) {
			volID = prefix + "/V-" + id;
			volumeHandles.put(volKey, volID);
		}
		String issueKey = journal + ":" + year + ":" + volume + ":" + issue;
		String issueID = issueHandles.get(issueKey);
		if(StringUtils.isBlank(issueID)) {
			issueID = prefix + "/I-" + id;
			issueHandles.put(issueKey, issueID);
		}
		
		String issueTitle = "Year: " + year + " Volume: " + volume + " Issue: " + issue;
		target.add("dc.relation.ispartof", NDLDataUtils.serializeIsPartOf(new IsPartOf(issueID, issueTitle)));
		
		// add items to sort for stitching
		StitchingDetail detail = new StitchingDetail(handle, title, journal, year, volume, issue, date);
		detail.journalID = journalID;
		detail.volID = volID;
		detail.issueID = issueID;
		detail.issn = issn;
		detail.eissn = eissn;
		String f = target.getFolder();
		detail.collection = f.substring(0, f.lastIndexOf('/'));
		detail.spatial = spatial;
		
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
					// same journal
					Integer year1 = Integer.valueOf(first.year);
					Integer year2 = Integer.valueOf(second.year);
					c = year2.compareTo(year1);
					if(c == 0) {
						Integer vol1 = Integer.valueOf(first.volume);
						Integer vol2 = Integer.valueOf(second.volume);
						c = vol2.compareTo(vol1);
						if(c == 0) {
							Integer issue1 = Integer.valueOf(first.issue);
							Integer issue2 = Integer.valueOf(second.issue);
							c = issue2.compareTo(issue1);
							if(c == 0) {
								try {
									Date date1 = DateUtils.parseDate(first.date, "yyyy-MM-dd");
									Date date2 = DateUtils.parseDate(second.date, "yyyy-MM-dd");
									return date2.compareTo(date1);
								} catch(Exception ex) {
									// error
									return 0;
								}
							} else {
								return c;
							}
						} else {
							return c;
						}
					} else {
						return c;
					}
				} else {
					return c;
				}
			}
		});
		
		// stitch
		stitch(items);
	}
	
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
	void stitch(List<StitchingDetail> items) throws Exception {
		System.out.println("Stitching ....");
		
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
				root.children.clear();
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
				jnode = new NDLStitchingNode(item.journalID, root);
				root.addChild(journal, jnode);
				// add value
				jnode.setFolder(item.collection + "/" + ++parentFolderCounter);
				jnode.addValue("dc.title", journal);
				jnode.addValue("dc.source", "Scientific Research Publishing (SCIRP)");
				jnode.addValue("dc.source.uri", "https://www.scirp.org/");
				jnode.addValue("dc.language.iso", "eng");
				jnode.addValue("dc.identifier.issn", item.issn);
				jnode.addValue("dc.identifier.other:eissn", item.eissn);
				jnode.addValue("dc.description.searchVisibility", "true");
				jnode.addValue("lrmi.educationalAlignment.educationalLevel", "ug_pg");
				jnode.addValue("lrmi.educationalUse", "research");
				jnode.addValue("lrmi.learningResourceType", "journal");
				jnode.addValue("lrmi.typicalAgeRange", "18-22", "22+");
				jnode.addValue("dc.coverage.spatial", item.spatial);
			}
			
			String volKey =  item.volKey;
			NDLStitchingNode vnode = jnode.getChildByKey(volKey);
			if(vnode == null) {
				// not exist
				// create
				vnode = new NDLStitchingNode(item.volID, jnode);
				jnode.addChild(volKey, vnode);
				// add value
				vnode.setFolder(item.collection + "/" + ++parentFolderCounter);
				vnode.addValue("dc.title", "Year: " + item.year + " Volume: " + item.volume);
				vnode.addValue("dc.source", "Scientific Research Publishing (SCIRP)");
				vnode.addValue("dc.source.uri", "https://www.scirp.org/");
				vnode.addValue("dc.language.iso", "eng");
				vnode.addValue("dc.identifier.issn", item.issn);
				vnode.addValue("dc.identifier.other:eissn", item.eissn);
				vnode.addValue("dc.description.searchVisibility", "false");
				vnode.addValue("dc.identifier.other:journal", journal);
			}
			
			String issueKey = item.issueKey;
			NDLStitchingNode inode = vnode.getChildByKey(issueKey);
			if(inode == null) {
				// not exist
				// create
				inode = new NDLStitchingNode(item.issueID, vnode);
				vnode.addChild(issueKey, inode);
				// add value
				inode.setFolder(item.collection + "/" + ++parentFolderCounter);
				inode.addValue("dc.title", "Year: " + item.year + " Volume: " + item.volume + " Issue: " + item.issue);
				inode.addValue("dc.source", "Scientific Research Publishing (SCIRP)");
				inode.addValue("dc.source.uri", "https://www.scirp.org/");
				inode.addValue("dc.language.iso", "eng");
				inode.addValue("dc.identifier.issn", item.issn);
				inode.addValue("dc.identifier.other:eissn", item.eissn);
				inode.addValue("dc.description.searchVisibility", "false");
				inode.addValue("dc.identifier.other:journal", journal);
				inode.addValue("dc.identifier.other:volume", item.volume);
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
						true);
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
	
	@Override
	public String getFolderName(SIPDataItem item) {
		// let the folder structure intact
		return null;
	}
	
	// correct
	public static void main(String[] args) throws Exception {
		
		String input = "/home/dspace/debasis/NDL/generated_xml_data/SCRIP/in/2018.Oct.27.17.52.07.SCRIP.data.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/generated_xml_data/SCRIP/logs";
		String outputLocation = "/home/dspace/debasis/NDL/generated_xml_data/SCRIP/out";
		String name = "SCIRP.V1";
		
		SCIRPCorrection p = new SCIRPCorrection(input, logLocation, outputLocation, name);
		p.addTextLogger("stitching.log");
		p.correctData();
		
		System.out.println("Done.");
	}
}