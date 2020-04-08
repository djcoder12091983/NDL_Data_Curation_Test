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
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;
import org.iitkgp.ndl.relation.HasPart;
import org.iitkgp.ndl.relation.IsPartOf;
import org.iitkgp.ndl.util.NDLDataUtils;
import org.xml.sax.SAXException;

public class JBCStitching extends NDLSIPCorrectionContainer {

	public JBCStitching(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	long parentFolderCounter = 0;
	
	class StitchingDetail {
		String handle;
		String title;
		String year;
		String volume;
		String issue;
		String place;
		String order;
		String collection;
		
		public StitchingDetail(String handle, String title, String year, String volume, String issue, String place,
				String order) {
			this.handle = handle;
			this.title = title;
			this.year = year;
			this.volume = volume;
			this.issue = issue;
			this.place = place;
			this.order = order;
			
			volKey = year + ":" + volume;
			issueKey = year + ":" + volume + ":" + issue;
		}
		
		String volID;
		String issueID;
		String volKey;
		String issueKey;
	}
	
	List<StitchingDetail> items = new ArrayList<StitchingDetail>();
	Map<String, String> volumeHandles = new TreeMap<String, String>(new Comparator<String>() {
		@Override
		public int compare(String first, String second) {
			String tokens1[] = first.split(":");
			String tokens2[] = second.split(":");
			Integer year1 = Integer.valueOf(tokens1[0]);
			Integer year2 = Integer.valueOf(tokens2[0]);
			int c = year2.compareTo(year1);
			if(c == 0) {
				Integer vol1 = Integer.valueOf(tokens1[1]);
				Integer vol2 = Integer.valueOf(tokens2[1]);
				return vol2.compareTo(vol1);
			} else {
				return c;
			}
		}
	});
	Map<String, String> issueHandles = new HashMap<String, String>();
	Map<String, String> ordering = null;
	
	// corect
	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		
		String detail = target.getSingleValue("dc.rights.uri");
		
		if(StringUtils.isNotBlank(detail)) {
			String handle = target.getId();
			int p = handle.indexOf('/');
			String prefix = handle.substring(0, p);
			String id = handle.substring(p + 1);
			String title = target.getSingleValue("dc.title");
			String date = target.getSingleValue("dc.publisher.date");
			String place = target.getSingleValue("dc.publisher.place");
			
			String tokens[] = detail.split("/");
			//System.out.println(target.getId() + " => " + detail);
			if(tokens.length == 3) {
				// valid data
				String year = date.substring(0, 4);
				String volume = tokens[0];
				String issue = tokens[1];
				String order = tokens[2];
				
				String volKey = year + ":" + volume;
				String volID = volumeHandles.get(volKey);
				if(StringUtils.isBlank(volID)) {
					volID = prefix + "/V-" + id;
					volumeHandles.put(volKey, volID);
				}
				String issueKey = year + ":" + volume + ":" + issue;
				String issueID = issueHandles.get(issueKey);
				if(StringUtils.isBlank(issueID)) {
					issueID = prefix + "/I-" + id;
					issueHandles.put(issueKey, issueID);
				}
				
				String issueTitle = "Year: " + year + " Volume: " + volume + " Issue: " + issue;
				String isPartOfJson = NDLDataUtils.serializeIsPartOf(new IsPartOf(issueID, issueTitle));
				target.add("dc.relation.ispartof", isPartOfJson);
				log("stitching.log", handle + " : " + isPartOfJson); // logging
				
				StitchingDetail data = new StitchingDetail(handle, title, year, volume, issue, place, order);
				data.volID = volID;
				data.issueID = issueID;
				String f = target.getFolder();
				data.collection = f.substring(0, f.lastIndexOf('/'));
				items.add(data);
			}
		}
		
		return true;
	}
	
	// gets order by place
	Integer order(String place) {
		place = NDLDataUtils.NVL(place, "__UNKNOWN__").toLowerCase();
		if(ordering.containsKey(place)) {
			return Integer.valueOf(ordering.get(place));
		} else {
			return Integer.valueOf(ordering.get("__UNKNOWN__".toLowerCase()));
		}
	}
	
	// stitching preparation
	@Override
	public void postProcessData() throws Exception {
		super.postProcessData(); // super call
		
		System.out.println("Preparing journal root node...");
		
		String jhandle = "jbc/JBC_root_journal_node";
		String jtitle = "Journal of Biological Chemistry (JBC)";
		
		// create journal root node
		SIPDataItem jnode = NDLDataUtils.createBlankSIP(jhandle);
		jnode.setFolder("/data/COLL@jbc_batch2/jbc_journal_root_node");
		jnode.add("dc.title", "Journal of Biological Chemistry (JBC)");
		jnode.add("dc.source", "Journal of Biological Chemistry (JBC)");
		jnode.add("dc.source.uri", "http://www.jbc.org");
		jnode.add("dc.language.iso", "eng");
		jnode.add("dc.description.searchVisibility", "true");
		jnode.add("lrmi.educationalUse", "research");
		jnode.add("lrmi.typicalAgeRange", "18-22", "22+");
		jnode.add("lrmi.learningResourceType", "journal");
		jnode.add("dc.identifier.issn", "00219258");
		jnode.add("dc.identifier.other:eissn", "1083351X");
		
		// journal has-part handled manually
		List<HasPart> jparts = new LinkedList<HasPart>();
		for(String y : volumeHandles.keySet()) {
			String tokens[] = y.split(":");
			String ytitle = "Year: " + tokens[0] + " Volume: " + tokens[1];
			jparts.add(new HasPart(ytitle, volumeHandles.get(y), true, false));
		}
		/*Collections.sort(jparts, new Comparator<HasPart>() {
			@Override
			public int compare(HasPart first, HasPart second) {
				return Integer.valueOf(second.getTitle()).compareTo(Integer.valueOf(first.getTitle()));
			}
		});*/
		String haspart = NDLDataUtils.getJson(jparts);
		jnode.add("dc.relation.haspart", haspart);
		writeItem(jnode); // write journal node
		
		log("stitching.log", "ROOT_NODE_HASPART: " + haspart); // logging
		
		System.out.println("Sorting and writing data for stitching ...");
		// sort and stitch
		Collections.sort(items, new Comparator<StitchingDetail>() {
			// ordering logic
			@Override
			public int compare(StitchingDetail first, StitchingDetail second) {
				Integer year1 = Integer.valueOf(first.year);
				Integer year2 = Integer.valueOf(second.year);
				int c = year2.compareTo(year1);
				if(c == 0) {
					Integer vol1 = Integer.valueOf(first.volume);
					Integer vol2 = Integer.valueOf(second.volume);
					c = vol2.compareTo(vol1);
					if(c == 0) {
						Integer issue1 = Integer.valueOf(first.issue);
						Integer issue2 = Integer.valueOf(second.issue);
						c = issue2.compareTo(issue1);
						if(c == 0) {
							c = order(first.place).compareTo(order(second.place));
							if(c == 0) {
								String o1 = first.order;
								String o2 = second.order;
								if(NumberUtils.isDigits(o1) && NumberUtils.isDigits(o2)) {
									// both are numeric
									// numeric comparison
									return Integer.valueOf(o1).compareTo(Integer.valueOf(o2));
								} else {
									if(NDLDataUtils.isRoman(o1) && NDLDataUtils.isRoman(o2)) {
										return Integer.valueOf(NDLDataUtils.romanToDecimal(o1))
												.compareTo(Integer.valueOf(NDLDataUtils.romanToDecimal(o2)));
									} else if(NDLDataUtils.isRoman(o1)) {
										return -1;
									} else if(NDLDataUtils.isRoman(o2)) {
										return 1;
									} else {
										// text comparison
										return o1.compareTo(o2);
									}
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
		
		// TODO test purpose
		// write stitching hierarchy
		for(StitchingDetail item : items) {
			log("hierarchy", new String[]{item.handle, item.year, item.volume, item.issue, item.place, item.order});
		}
		
		// stitch
		stitch(jhandle, jtitle);
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
	void stitch(String jhandle, String jtitle) throws Exception {
		System.out.println("Stitching ....");
		
		String vispart = NDLDataUtils.serializeIsPartOf(new IsPartOf(jhandle, jtitle));
		NDLStitchingNode root = new NDLStitchingNode(); // stitching root node
		
		int limit = 10000; // stitching limit
		int counter = 0;
		String prevVolume = null;
		for(StitchingDetail item : items) {
			String volume = item.volume;
			if(!StringUtils.equals(prevVolume, volume) && counter >= limit) {
				// new volume encounters, flush the items
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
			
			String volKey =  item.volKey;
			NDLStitchingNode vnode = root.getChildByKey(volKey);
			if(vnode == null) {
				// not exist
				// create
				vnode = new NDLStitchingNode(item.volID, root);
				root.addChild(volKey, vnode);
				// add value
				vnode.setFolder(item.collection + "/" + ++parentFolderCounter);
				vnode.addValue("dc.title", "Year: " + item.year + " Volume: " + item.volume);
				vnode.addValue("dc.source", "Journal of Biological Chemistry (JBC)");
				vnode.addValue("dc.source.uri", "http://www.jbc.org");
				vnode.addValue("dc.language.iso", "eng");
				vnode.addValue("dc.description.searchVisibility", "false");
				vnode.addValue("dc.identifier.other:journal", jtitle);
				vnode.addValue("dc.relation.ispartof", vispart); // volume is-part handled manually
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
				inode.addValue("dc.source", "Journal of Biological Chemistry (JBC)");
				inode.addValue("dc.source.uri", "http://www.jbc.org");
				inode.addValue("dc.language.iso", "eng");
				inode.addValue("dc.description.searchVisibility", "false");
				inode.addValue("dc.identifier.other:journal", jtitle);
				inode.addValue("dc.identifier.other:volume", item.volume);
			}
			
			// has part
			inode.addChild(item.handle, new NDLStitchingNode(dummy));
			
			counter++; // item counter
			prevVolume = volume; // previous volume
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
	
	@Override
	public String getFolderName(SIPDataItem item) {
		// let the folder structure intact
		return null;
	}
	
	// testing
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/NDL_sources/JBC/2019.Apr.10.10.58.30.JBC.V2.Corrected.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/NDL_sources/JBC/logs";
		String outputLocation = "/home/dspace/debasis/NDL/NDL_sources/JBC/out";
		String name = "jbc.stitching.v2";
		
		String fordering = "/home/dspace/debasis/NDL/NDL_sources/JBC/conf/jbc.order.csv";
		
		JBCStitching p = new JBCStitching(input, logLocation, outputLocation, name);
		// ordering
		p.ordering = NDLDataUtils.loadKeyValue(fordering, true);
		p.turnOffLoadHierarchyFlag();
		p.dontShowWarnings();
		p.addTextLogger("stitching.log");
		p.addCSVLogger("hierarchy", new String[]{"ID", "Year", "Volume", "Issue", "Place", "Order"});
		p.processData();
		
		System.out.println("Done.");
	}
}