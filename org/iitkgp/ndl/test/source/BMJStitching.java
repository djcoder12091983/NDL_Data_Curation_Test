package org.iitkgp.ndl.test.source;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Queue;

import org.apache.commons.lang3.CharUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;
import org.iitkgp.ndl.relation.HasPart;
import org.iitkgp.ndl.relation.IsPartOf;
import org.iitkgp.ndl.util.NDLDataUtils;
import org.xml.sax.SAXException;

import com.opencsv.CSVWriter;

// BMJ stitching
public class BMJStitching extends NDLSIPCorrectionContainer {
	
	long parentFolderCounter = 0;
	
	class StitchingDetail {
		String handle;
		String title;
		String year;
		String month;
		String collection;
		
		public StitchingDetail(String handle, String title, String year, String month) {
			this.handle = handle;
			this.title = title;
			this.year = year;
			
			monthKey = year + ":" + month;
		}
		
		String yearID;
		String monthID;
		String monthKey;
		String sortk;
	}
	
	List<StitchingDetail> items = new ArrayList<StitchingDetail>();
	Map<String, String> yearHandles = new HashMap<String, String>();
	Map<String, String> monthHandles = new HashMap<String, String>();

	public BMJStitching(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		
		// path update
		target.setFolder(target.getFolder().replace("subhayan/24E232A5E2327ADE/", ""));
		
		String handle = target.getId();
		int p = handle.indexOf('/');
		String prefix = handle.substring(0, p);
		String id = handle.substring(p + 1);
		
		Calendar date = Calendar.getInstance();
		date.setTime(DateUtils.parseDate(target.getSingleValue("dc.publisher.date"), "yyyy-MM-dd"));
		String year = String.valueOf(date.get(Calendar.YEAR));
		String month = date.getDisplayName(Calendar.MONTH, Calendar.LONG, Locale.getDefault());
		
		String title = target.getSingleValue("dc.title");
		
		String yearID = yearHandles.get(year);
		if(StringUtils.isBlank(yearID)) {
			yearID = prefix + "/Y-" + id;
			yearHandles.put(year, yearID);
		}
		String monthKey = year + ":" + month;
		String monthID = monthHandles.get(monthKey);
		if(StringUtils.isBlank(monthID)) {
			monthID = prefix + "/M-" + id;
			monthHandles.put(monthKey, monthID);
		}
		
		String montht = month;
		target.add("dc.relation.ispartof", NDLDataUtils.serializeIsPartOf(new IsPartOf(monthID, montht)));
		
		// add items to sort for stitching
		StitchingDetail detail = new StitchingDetail(handle, title, year, month);
		detail.year = year;
		detail.yearID = yearID;
		detail.month = month;
		detail.monthID = monthID;
		detail.sortk = target.getSingleValue("dc.relation.ispartofseries").split("/")[2];
		String f = target.getFolder();
		detail.collection = f.substring(0, f.lastIndexOf('/'));
		
		items.add(detail);
		
		return true;
	}
	
	String[] tokens(String txt) {
		int l = txt.length();
		for(int i = 0; i < l; i++) {
			char ch = txt.charAt(i);
			if(!CharUtils.isAsciiNumeric(ch)) {
				return new String[]{txt.substring(0, i), txt.substring(i)};
			}
		}
		return new String[]{txt, ""};
	}
	
	// stitching preparation
	@Override
	public void postProcessData() throws Exception {
		
		super.postProcessData(); // super call
		
		System.out.println("Preparing journal root node...");
		
		String jhandle = "bmj/BMJ_root_journal_node";
		String jtitle = "British Medical Journal (The BMJ)";
		
		// create journal root node
		SIPDataItem jnode = NDLDataUtils.createBlankSIP(jhandle);
		jnode.setFolder("/export-bmj/data/bmj_journal_root_node");
		jnode.add("dc.title", "British Medical Journal (The BMJ)");
		jnode.add("dc.source", "British Medical Journal (BMJ)");
		jnode.add("dc.source.uri", "https://www.bmj.com");
		jnode.add("dc.language.iso", "eng");
		jnode.add("dc.description.searchVisibility", "true");
		jnode.add("lrmi.educationalUse", "research");
		jnode.add("lrmi.typicalAgeRange", "18-22", "22+");
		jnode.add("lrmi.learningResourceType", "journal");
		jnode.add("dc.description",
				"British Medical Journal (BMJ)original part of the British Medical Journal Publishing Group.Originally "
				+ "called the British Medical Journal, the title was officially shortened to BMJ in 1988, and then changed "
				+ "to The BMJ in 2014.Known as BMJ [The BMJ] (1857-2019); British Medical Journal[Br Med J] (1861-1980); "
				+ "British Medical Journal(Clinical Research Ed.)[Br Med J (Clin Res Ed.)](1981-1988); Provincial Medical & "
				+ "Surgical Journal [Prov Med Surg J](1840-1852); Association Medical Journal [Assoc Med J](1853-1856);London "
				+ "Journal of Medicine [Lond J Med](1849-1852).");
		
		// journal has-part handled manually
		List<HasPart> jparts = new LinkedList<HasPart>();
		for(String y : yearHandles.keySet()) {
			jparts.add(new HasPart(y, yearHandles.get(y), true, false));
		}
		Collections.sort(jparts, new Comparator<HasPart>() {
			@Override
			public int compare(HasPart first, HasPart second) {
				return Integer.valueOf(second.getTitle()).compareTo(Integer.valueOf(first.getTitle()));
			}
		});
		jnode.add("dc.relation.haspart", NDLDataUtils.getJson(jparts));
		
		writeItem(jnode); // write journal node
		
		System.out.println("Sorting and writing data for stitching ...");
		// sort and stitch
		Collections.sort(items, new Comparator<StitchingDetail>() {
			// comparison logic
			@Override
			public int compare(StitchingDetail first, StitchingDetail second) {
				int y1 = Integer.parseInt(first.year);
				int y2 = Integer.parseInt(second.year);
				if(y1 != y2) {
					return y2 - y1;
				}
				// descending
				int c = NDLDataUtils.compareMonth(second.month, first.month);
				if(c != 0) {
					return c;
				}
				String []t1 = tokens(first.sortk);
				String []t2 = tokens(second.sortk);
				// ascending
				if(StringUtils.isNotBlank(t1[0]) && StringUtils.isNotBlank(t2[0])) {
					c = Long.valueOf(t1[0]).compareTo(Long.valueOf(t2[0]));
					if(c != 0) {
						return c;
					}
				}
				return t1[1].compareTo(t2[1]);
			}
		});
		
		CSVWriter temp = NDLDataUtils.openCSV(new File(logLocation, "sorted-data.csv"));
		for(StitchingDetail item : items) {
			temp.writeNext(new String[]{item.handle, item.title, item.month, item.year});
		}
		temp.close();
		
		// stitch
		stitch(jhandle, jtitle);
	}
	
	// stitching
	void stitch(String jhandle, String jtitle) throws Exception {
		System.out.println("Stitching ....");
		
		String yispart = NDLDataUtils.serializeIsPartOf(new IsPartOf(jhandle, jtitle));
		NDLStitchingNode root = new NDLStitchingNode(); // stitching root node
		
		int limit = 10000; // stitching limit
		int counter = 0;
		String prevYear = null;
		for(StitchingDetail item : items) {
			String year = item.year;
			if(!StringUtils.equals(prevYear, year) && counter >= limit) {
				// new year encounters, flush the items
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
			NDLStitchingNode ynode = root.getChildByKey(year);
			if(ynode == null) {
				// not exist
				// create
				ynode = new NDLStitchingNode(item.yearID, root);
				root.addChild(year, ynode);
				// add value
				ynode.setFolder(item.collection + "/" + ++parentFolderCounter);
				ynode.addValue("dc.title", year);
				ynode.addValue("dc.source", "British Medical Journal (BMJ)");
				ynode.addValue("dc.source.uri", "https://www.bmj.com");
				ynode.addValue("dc.language.iso", "eng");
				ynode.addValue("dc.description.searchVisibility", "false");
				ynode.addValue("lrmi.educationalUse", "research");
				ynode.addValue("lrmi.typicalAgeRange", "18-22", "22+");
				ynode.addValue("dc.relation.ispartof", yispart); // year is-part handled manually
			}
			
			String monthKey =  item.monthKey;
			NDLStitchingNode mnode = ynode.getChildByKey(monthKey);
			if(mnode == null) {
				// not exist
				// create
				mnode = new NDLStitchingNode(item.monthID, ynode);
				ynode.addChild(monthKey, mnode);
				// add value
				mnode.setFolder(item.collection + "/" + ++parentFolderCounter);
				mnode.addValue("dc.title", item.month);
				mnode.addValue("dc.source", "British Medical Journal (BMJ)");
				mnode.addValue("dc.source.uri", "https://www.bmj.com");
				mnode.addValue("dc.language.iso", "eng");
				mnode.addValue("dc.description.searchVisibility", "false");
			}
			
			// has part
			mnode.addChild(item.handle, new NDLStitchingNode(dummy));
			
			counter++; // item counter
			prevYear = year; // previous year
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

		void addValue(String field, String... values) {
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
	
	// testing
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/generated_xml_data/BMJ/in/2019.Feb.19.15.25.59.BMJ.V4.Corrected.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/generated_xml_data/BMJ/logs";
		String outputLocation = "/home/dspace/debasis/NDL/generated_xml_data/BMJ/out";
		String name = "bmj.stitching";
		
		BMJStitching p = new BMJStitching(input, logLocation, outputLocation, name);
		p.turnOffLoadHierarchyFlag();
		p.addTextLogger("stitching.log");
		p.processData();
		
		System.out.println("Done.");
	}
}