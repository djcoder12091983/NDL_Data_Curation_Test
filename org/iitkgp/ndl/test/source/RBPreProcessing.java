package org.iitkgp.ndl.test.source;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.CharUtils;
import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.context.NDLConfigurationContext;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.container.NDLSIPDataContainer;
import org.iitkgp.ndl.util.NDLDataUtils;

public class RBPreProcessing extends NDLSIPDataContainer {
	
	static final Pattern SWF_ORDER_REGX = Pattern.compile("([0-9]+)eng\\.swf");
	static final Pattern TRACK_ORDER_REGX = Pattern.compile("([0-9]+)\\.mp3");
	
	Set<String> educations = null;
	Set<String> types = null;
	Map<String, String> phandles = new HashMap<String, String>();
	List<HierarchyData> hdata = new ArrayList<RBPreProcessing.HierarchyData>();
	static Set<String> SPECIAL_HANDLES = new HashSet<String>(4);
	
	static {
		SPECIAL_HANDLES.add("21963");
		SPECIAL_HANDLES.add("21964");
		SPECIAL_HANDLES.add("21967");
		SPECIAL_HANDLES.add("21968");
	}
	
	Map<String, Map<Group, List<DataItem>>> hmap = new HashMap<String, Map<Group, List<DataItem>>>();
	
	class DataItem {
		String handle;
		int order;
		boolean subjectf;
		
		public DataItem(String handle, int order, boolean subjectf) {
			this.handle = handle;
			this.order = order;
			this.subjectf = subjectf;
		}
	}
	
	class Group {
		String group;
		String location;
		String type;
		
		public Group(String group, String type, String location) {
			this.group = group;
			this.location = location;
			this.type = type;
		}
		
		@Override
		public int hashCode() {
			int h = 13;
			h = h * 31 + group.hashCode();
			return h;
		}
		
		@Override
		public boolean equals(Object obj) {
			Group g = (Group)obj;
			return g.group.equals(this.group);
		}
	}
	
	class HierarchyData {
		String handle;
		String education;
		String type;
		String group;
		int order;
		String title;
		boolean subject;
		
		public HierarchyData(String handle, String title, String education, String type, String group, int order, boolean subject) {
			this.handle = handle;
			this.title = title;
			this.education = education;
			this.type = type;
			this.group = group;
			this.order = order;
			this.subject = subject;
		}
	}

	public RBPreProcessing(String input, String logLocation, String name) {
		super(input, logLocation, name);
	}

	@Override
	protected boolean readItem(SIPDataItem item) throws Exception {
		
		String id = NDLDataUtils.getHandleSuffixID(item.getId());
		/*if(id.equals("22087")) {
			// cross check
			String education = item.getSingleValue("lrmi.educationalAlignment.educationalLevel");
			System.out.println("Education(" + education + "): " + educations.contains(education));
			System.out.println("Map: " + educations);
		}*/
		
		// existing hierarchy
		/*if(item.exists("dc.relation.ispartof")) {
			String parent = NDLDataUtils.getHandleSuffixID(item.getSingleValue("dc.relation.ispartof"));
			String order = item.getSingleValue("dc.relation");
			log("rb.heirarchy", new String[]{parent, id, order});
		}
		
		String title = item.getSingleValue("dc.title");
		log("rb.titles", new String[]{id, title});*/
		
		String title = item.getSingleValue("dc.title");
		
		// another hierarchy
		if(item.contains("lrmi.educationalAlignment.educationalLevel", educations)
				&& item.contains("dc.type", types)) {
			String group = item.getSingleValue("dc.description");
			boolean subjectf = false;
			if(SPECIAL_HANDLES.contains(id)) {
				// TODO special case for description (single case)
				subjectf = true;
			}
			if(StringUtils.isBlank(group)) {
				// alternate
				group = item.getSingleValue("dc.subject");
				subjectf = true;
			}
			if(StringUtils.isBlank(group)) {
				// cross check
				group = NDLDataUtils.NVL(group);
				//throw new IllegalStateException("Invalid group: " + id);
				System.err.println("Blank group: " + id);
				// dummy value
				group = "__NO_GROUP__";
			}
			String type = item.getSingleValue("dc.type");
			int order = -1;
			if(type.equals("video")) {
				// video item order
				order = Integer.parseInt(item.getSingleValue("dc.identifier.other:standardNo"));
			} else if(type.equals("audio")) {
				// audio item order
				String uri = item.getSingleValue("ndl.sourceMeta.additionalInfo:relatedContentUrl");
				Matcher m = TRACK_ORDER_REGX.matcher(uri);
				if(m.find()) {
					order = Integer.parseInt(m.group(1));
				} else {
					// cross-check
					throw new IllegalStateException("Invalid TRACK URL: " + uri);
				}
			} else if(type.equals("simulation")) {
				// simulation item order
				String uri = item.getSingleValue("dc.identifier.uri");
				Matcher m = SWF_ORDER_REGX.matcher(uri);
				if(m.find()) {
					order = Integer.parseInt(m.group(1));
				} else {
					// cross-check
					throw new IllegalStateException("Invalid SWF URL: " + uri);
				}
			}
			hdata.add(new HierarchyData(id, title, item.getSingleValue("lrmi.educationalAlignment.educationalLevel"),
					item.getSingleValue("dc.type"), group, order, subjectf));
			
			// add to hmap
			String education = item.getSingleValue("lrmi.educationalAlignment.educationalLevel");
			Map<Group, List<DataItem>> map = hmap.get(education);
			if(map == null) {
				map = new HashMap<Group, List<DataItem>>();
				hmap.put(education, map);
			}
			List<DataItem> items = map.get(new Group(group, null, null));
			if(items == null) {
				items = new LinkedList<DataItem>();
				map.put(new Group(group, type, NDLDataUtils.getParentFolder(item.getFolder())), items);
			}
			items.add(new DataItem(id, order, subjectf));
		}
		
		return true;
	}
	
	@Override
	public void postProcessData() throws Exception {
		super.postProcessData(); // super call
		
		System.out.println("Post processing data: " + hdata.size());
		
		// sort and write
		Collections.sort(hdata, new Comparator<HierarchyData>() {
			// comparison
			@Override
			public int compare(HierarchyData first, HierarchyData second) {
				int c = first.education.compareTo(second.education);
				if(c == 0) {
					c = first.type.compareTo(second.type);
					if(c == 0) {
						// TODO group order discussion
						c = first.group.compareTo(second.group);
						if(c == 0) {
							return Integer.valueOf(first.order).compareTo(Integer.valueOf(second.order));
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
		
		// write into csv
		for(HierarchyData hd : hdata) {
			log("rb.another.heirarchy", new String[] { hd.handle, hd.title, hd.education, hd.type, hd.group,
					String.valueOf(hd.order), String.valueOf(hd.subject) });
		}
		
		// write hierarchy logs
		List<String[]> data1 = new ArrayList<String[]>();
		List<String[]> data2 = new ArrayList<String[]>();
		for(String education : hmap.keySet()) {
			Map<Group, List<DataItem>> map = hmap.get(education);
			for(Group g : map.keySet()) {
				String group = g.group;
				String parenth;
				List<DataItem> items = map.get(g);
				if(!StringUtils.equals(group, "__NO_GROUP__")) {
					// create false node if needed
					boolean subjectf = items.get(0).subjectf;
					if(subjectf || items.size() > 1) {
						// subject flag
						//parenth = "P_" + education.replaceAll(" +", "_") + "_" + group.replaceAll(" +", "_");
						parenth = phandle(education, group);
						//log("rb.virtual.nodes", new String[]{phandles.get(education), parenth, group, education, g.type, g.location});
						data1.add(new String[]{phandles.get(education), parenth, group, education, g.type, g.location});
					} else {
						parenth = phandles.get(education);
					}
				} else {
					parenth = phandles.get(education);
				}
				for(DataItem item : items) {
					//log("rb.child.nodes", new String[]{item.handle, parenth, String.valueOf(item.order)});
					data2.add(new String[]{item.handle, parenth, String.valueOf(item.order)});
				}
			}
		}
		
		// sort and write
		Collections.sort(data1, new Comparator<String[]>() {
			@Override
			public int compare(String[] first, String[] second) {
				int c = first[0].compareTo(second[0]);
				if(c == 0) {
					return first[1].compareTo(second[1]);
				} else {
					return c;
				}
			}
		});
		Collections.sort(data2, new Comparator<String[]>() {
			@Override
			public int compare(String[] first, String[] second) {
				int c = first[1].compareTo(second[1]);
				if(c == 0) {
					return first[0].compareTo(second[0]);
				} else {
					return c;
				}
			}
		});
		
		for(String[] d : data1) {
			log("rb.virtual.nodes", d);
		}
		for(String[] d : data2) {
			log("rb.child.nodes", d);
		}
	}
	
	String phandle(String education, String group) {
		StringBuilder h = new StringBuilder("P_");
		h.append(removeSpecial(education)).append("_");
		h.append(removeSpecial(group));
		return h.toString();
	}
	
	static String removeSpecial(String txt) {
		int l = txt.length();
		StringBuilder ntxt = new StringBuilder();
		for(int i = 0; i < l; i++) {
			char ch = txt.charAt(i);
			if(CharUtils.isAsciiAlpha(ch) || CharUtils.isAsciiNumeric(ch)) {
				ntxt.append(ch);
			} else {
				ntxt.append("_");
			}
		}
		return ntxt.toString();
	}
	
	public static void main(String[] args) throws Exception {
		
		String input = "/home/dspace/debasis/NDL/NDL_sources/RAJ/in/2019.Apr.29.11.46.38.curation.rjbrd.Corrected.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/NDL_sources/RAJ/logs";
		String name = "rb_data";
		
		String educationFile = "/home/dspace/debasis/NDL/NDL_sources/RAJ/conf/education.level";
		String typeFile = "/home/dspace/debasis/NDL/NDL_sources/RAJ/conf/type";
		String phandleFile = "/home/dspace/debasis/NDL/NDL_sources/RAJ/conf/parent.mapping.csv";
		
		NDLConfigurationContext.addConfiguration("compressed.data.process.buffer.size", "10");
		
		RBPreProcessing p = new RBPreProcessing(input, logLocation, name);
		p.educations = NDLDataUtils.loadSet(educationFile);
		p.types = NDLDataUtils.loadSet(typeFile);
		p.phandles = NDLDataUtils.loadKeyValue(phandleFile);
		//p.addCSVLogger("rb.heirarchy", new String[]{"Parent_ID", "Child_ID", "Order"});
		//p.addCSVLogger("rb.titles", new String[]{"ID", "Title"});
		p.addCSVLogger("rb.another.heirarchy", new String[]{"Handle", "Title", "Educational_Level", "Type", "Description", "Order", "Subject_Flag"});
		p.addCSVLogger("rb.virtual.nodes", new String[]{"Parent", "ID", "Title", "Education", "Type", "Location"});
		p.addCSVLogger("rb.child.nodes", new String[]{"Child", "Parent", "Order"});
		p.processData();
		
		System.out.println("Done.");
	}
	
	public static void main1(String[] args) {
		/*String uri1 = "http://egyan.rajasthan.gov.in//goresa//wp-content//uploads//2015//10//PREP_Track12.mp3";
		String uri2 = "http://egyan.rajasthan.gov.in/content/rajasthan/2018/learn/class-ii/mathematics/chapter-06/swf/class2mathsch6aex2eng.swf";
		
		Matcher m1 = TRACK_ORDER_REGX.matcher(uri1);
		Matcher m2 = SWF_ORDER_REGX.matcher(uri2);
		
		if(m1.find()) {
			System.out.println("Track: " + m1.group(1));
		}
		if(m2.find()) {
			System.out.println("SWF: " + m2.group(1));
		}*/
		/*String text = "Class IV ";
		int l = text.length();
		for(int i = 0; i < l; i++) {
			System.out.print((int)text.charAt(i) + " ");
		}*/
		
		//System.out.println(removeSpecial("debasis(jaana)/nirupam##123suajit - jana debasis das"));
	}
}