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
import org.apache.commons.lang3.math.NumberUtils;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;
import org.iitkgp.ndl.relation.HasPart;
import org.iitkgp.ndl.relation.IsPartOf;
import org.iitkgp.ndl.util.NDLDataUtils;
import org.xml.sax.SAXException;

public class ABPStitching extends NDLSIPCorrectionContainer {

	class StitchingDetail {
		String handle;
		String title;
		String year;
		String month;
		String monthCode;
		String day;
		String dayCode;
		String page;

		public StitchingDetail(String handle, String title, String year, String month, String monthCode, String day,
				String dayCode, String page) {
			this.handle = handle;
			this.title = title;
			this.year = year;
			this.month = month;
			this.monthCode = monthCode;
			this.day = day;
			this.dayCode = dayCode;
			this.page = page;

			monthKey = year + ":" + month;
			dayKey = year + ":" + month + ":" + dayCode;
		}

		String yearID;
		String monthID;
		String dayID;
		String monthKey;
		String dayKey;
	}

	List<StitchingDetail> items = new ArrayList<StitchingDetail>();
	Map<String, String> yearHandles = new HashMap<String, String>();
	Map<String, String> monthHandles = new HashMap<String, String>();
	Map<String, String> dayHandles = new HashMap<String, String>();

	// construction
	public ABPStitching(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}

	// correction
	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {

		String handle = target.getId();
		int p = handle.indexOf('/');
		String prefix = handle.substring(0, p);
		String id = handle.substring(p + 1);

		String title = target.getSingleValue("dc.title");
		String year = target.getSingleValue("ndl.sourceMeta.additionalInfo:level1");
		String month = target.getSingleValue("ndl.sourceMeta.additionalInfo:month");
		String monthCode = target.getSingleValue("ndl.sourceMeta.additionalInfo:level2");
		String day = target.getSingleValue("ndl.sourceMeta.additionalInfo:extra_str");
		String dayCode = target.getSingleValue("ndl.sourceMeta.additionalInfo:level3");
		String page = target.getSingleValue("ndl.sourceMeta.additionalInfo:page");

		String yearID = yearHandles.get(year);// ask if duplicate journals//make
												// journals unique
		if (StringUtils.isBlank(yearID)) {
			yearID = prefix + "/Y-" + id;
			yearHandles.put(year, yearID);
		}
		String monthKey = year + ":" + month;
		String monthID = monthHandles.get(monthKey);
		if (StringUtils.isBlank(monthID)) {
			monthID = prefix + "/M-" + id;
			monthHandles.put(monthKey, monthID);
		}
		String dayKey = year + ":" + month + ":" + dayCode;
		String dayID = dayHandles.get(dayKey);
		if (StringUtils.isBlank(dayID)) {
			dayID = prefix + "/D-" + id;
			dayHandles.put(dayKey, dayID);
		}

		target.add("dc.relation.ispartof", NDLDataUtils.serializeIsPartOf(new IsPartOf(dayID, day)));// ask

		// add items to sort for stitching
		StitchingDetail detail = new StitchingDetail(handle, title, year, month, monthCode, day, dayCode, page);
		detail.yearID = yearID;
		detail.monthID = monthID;
		detail.dayID = dayID;
		detail.page = page;

		items.add(detail);

		// TODO curation fields if any
		target.add("dc.description.searchVisibility", "true");
		target.updateSingleValue("dc.source", "CSSSC EAP-Amrita Bazar Patrika");

		return true;
	}

	// stitching preparation
	@Override
	public void postProcessData() throws Exception {// this function executes
													// only after whole/all
													// journals are scanned
		super.postProcessData(); // super call
		System.out.println("Sorting and writing data for stitching ...");
		// sort and stitch
		Collections.sort(items, new Comparator<StitchingDetail>() {
			// comparison logic
			@Override
			public int compare(StitchingDetail first, StitchingDetail second) {
				Integer y1 = Integer.valueOf(first.year);
				Integer y2 = Integer.valueOf(second.year);
				int c = y1.compareTo(y2);
				if (c == 0) {
					// same year
					Integer m1 = Integer.valueOf(first.monthCode);
					Integer m2 = Integer.valueOf(second.monthCode);
					c = m1.compareTo(m2);
					if (c == 0) {
						if (NumberUtils.isDigits(first.dayCode) && NumberUtils.isDigits(second.dayCode)) {
							Integer d1 = Integer.valueOf(first.dayCode);
							Integer d2 = Integer.valueOf(second.dayCode);
							c = d1.compareTo(d2);
						} else {
							c = first.dayCode.compareTo(second.dayCode);
						}
						if (c == 0) {
							try {
								Integer p1 = Integer.valueOf(first.page);
								Integer p2 = Integer.valueOf(second.page);
								return p1.compareTo(p2);
							} catch (Exception ex) {
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
			}
		});

		// stitch
		stitch(items);// ask how do we know that all items are included required
						// for stitching
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

	// stitching
	void stitch(List<StitchingDetail> items) throws Exception {
		System.out.println("Stitching ....");

		SIPDataItem top = NDLDataUtils.createBlankSIP("abp/ABP_TOP_NODE_123456789", true);
		top.add("dc.title", "CSSSC EAP :: Amrita Bazar Patrika");
		top.add("dc.source", "CSSSC EAP-Amrita Bazar Patrika");
		top.add("dc.source.uri", "https://doi.org/10.15130/EAP262");
		List<HasPart> parts = new LinkedList<HasPart>();
		for (String year : yearHandles.keySet()) {
			String handle = yearHandles.get(year);
			parts.add(new HasPart(year, handle, true, true));
		}
		Collections.sort(parts, new Comparator<HasPart>() {
			@Override
			public int compare(HasPart first, HasPart second) {
				return Integer.valueOf(first.getTitle()).compareTo(Integer.valueOf(second.getTitle()));
			}
		});
		top.add("dc.relation.haspart", NDLDataUtils.serializeHasPart(parts));
		writeItem(top); // write

		NDLStitchingNode root = new NDLStitchingNode(); // stitching root node

		int limit = 10000; // stitching limit
		int counter = 0;
		String prevYear = null;
		for (StitchingDetail item : items) {
			String year = item.year;
			if (!StringUtils.equals(prevYear, year) && counter >= limit) {
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
			NDLStitchingNode ynode = root.getChildByKey(year);
			if (ynode == null) {
				// not exist
				// create
				ynode = new NDLStitchingNode(item.yearID, root);
				root.addChild(year, ynode);
				// add value
				ynode.addValue("dc.title", year);
				ynode.addValue("dc.source", "CSSSC EAP-Amrita Bazar Patrika");
				ynode.addValue("dc.source.uri", "https://doi.org/10.15130/EAP262");
				ynode.addValue("dc.relation.ispartof", NDLDataUtils.serializeIsPartOf(
						new IsPartOf("abp/ABP_TOP_NODE_123456789", "CSSSC EAP :: Amrita Bazar Patrika")));
			}

			String monthKey = item.monthKey;
			NDLStitchingNode mnode = ynode.getChildByKey(monthKey);
			if (mnode == null) {
				// not exist
				// create
				mnode = new NDLStitchingNode(item.monthID, ynode);
				ynode.addChild(monthKey, mnode);
				// add value
				mnode.addValue("dc.title", item.month);
				mnode.addValue("dc.source", "CSSSC EAP-Amrita Bazar Patrika");
				mnode.addValue("dc.source.uri", "https://doi.org/10.15130/EAP262");
			}

			String dayKey = item.dayKey;
			NDLStitchingNode dnode = mnode.getChildByKey(dayKey);
			if (dnode == null) {
				// not exist
				// create
				dnode = new NDLStitchingNode(item.dayID, mnode);
				mnode.addChild(dayKey, dnode);
				// add value
				dnode.addValue("dc.title", item.day);
				dnode.addValue("dc.source", "CSSSC EAP-Amrita Bazar Patrika");
				dnode.addValue("dc.source.uri", "https://doi.org/10.15130/EAP262");
			}

			// has part
			dnode.addChild(item.handle, new NDLStitchingNode(dummy));

			counter++; // item counter
			prevYear = year; // previous journal
		}

		if (counter > 1) {
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
		while (!nodes.isEmpty()) {
			NDLStitchingNode node = nodes.poll();
			// haspart update
			List<HasPart> parts = new LinkedList<HasPart>();
			for (NDLStitchingNode child : node.children.values()) {
				HasPart p = new HasPart(child.getSingleValue("dc.title"), child.item.getId(), !child.children.isEmpty(),
						true);
				parts.add(p);
			}
			if (!parts.isEmpty()) {
				// valid case
				String hasPartJson = NDLDataUtils.getJson(parts);
				log("stitching.log", node.item.getId() + " : " + hasPartJson); // logging
				node.addValue("dc.relation.haspart", hasPartJson);
				NDLStitchingNode parent = node.parent;
				if (parent != null && parent.item != null) {
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

		String input = "/home/dspace/debasis/NDL/generated_xml_data/ABP/2019.Feb.28.12.43.40.ABP_Level1.Corrected.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/generated_xml_data/ABP/logs";
		String outputLocation = "/home/dspace/debasis/NDL/generated_xml_data/ABP/output";
		String name = "ABP_stitch";

		ABPStitching p = new ABPStitching(input, logLocation, outputLocation, name);
		p.dontPreserveFolderStructure();
		p.turnOffLoadHierarchyFlag();
		p.addTextLogger("stitching.log");
		p.correctData();
		
		System.out.println("Done.");
	}
}