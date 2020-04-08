package org.iitkgp.ndl.test.source;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.CharUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;
import org.iitkgp.ndl.relation.IsPartOf;
import org.iitkgp.ndl.util.NDLDataUtils;

import com.opencsv.CSVWriter;

// JAMA virtual node creations
public class JAMAStitching1 extends NDLSIPCorrectionContainer {
	
	static final String HANDLE_PREFIX = "jamanetwork/";
	static Pattern VOL_ISSUE_PATTERN = Pattern.compile("([0-9]+)([a-zA-Z])*$");

	public JAMAStitching1(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	// handle_id
	Set<String> vnodes = new HashSet<String>();
	List<SIPDataItem> vsipnodes = new ArrayList<SIPDataItem>();
	List<String[]> hierarchy = new ArrayList<String[]>();
	int pf = 0;
	
	String removeSpecial(String txt) {
		if(StringUtils.isBlank(txt)) {
			// blank case
			return "";
		}
		int l = txt.length();
		StringBuilder ntxt = new StringBuilder();
		for(int i = 0; i < l; i++) {
			char ch = txt.charAt(i);
			if(CharUtils.isAsciiAlpha(ch) || CharUtils.isAsciiNumeric(ch)) {
				ntxt.append(ch);
			} else {
				if(ch == 32) {
					// space replaced by _
					ntxt.append("_");
				}
			}
		}
		return ntxt.toString();
	}
	
	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		if(target.exists("dc.title.alternative")) {
			// delete
			return false;
		}
		
		String pfolder = NDLDataUtils.getParentFolder(target.getFolder());
		
		//removeMultipleSpaces("dc.relation.ispartofseries", "dc.date.issued"); // normalize space
		removeMultipleSpaces("dc.relation.ispartofseries"); // normalize space
		String jname = target.getSingleValue("dc.relation.ispartofseries");
		String jvnode = 'V' + removeSpecial(jname);
		String jhandle = HANDLE_PREFIX + jvnode;
		if(!vnodes.contains(jvnode)) {
			vnodes.add(jvnode);
			// create journal virtual node
			SIPDataItem sip = NDLDataUtils.createBlankSIP(jhandle);
			sip.setFolder(pfolder + "/V" + ++pf);
			sip.add("dc.title", jname);
			sip.add("dc.language.iso", "eng");
			sip.add("dc.source", "JAMA Network");
			sip.add("dc.source.uri", "https://jamanetwork.com");
			sip.add("dc.type", "text");
			sip.add("lrmi.educationalUse", "research");
			sip.add("lrmi.educationalAlignment.educationalLevel", "ug_pg");
			sip.add("dc.description.searchVisibility", "true");
			sip.add("lrmi.learningResourceType", "journal");
			vsipnodes.add(sip); // add to items
		}
		String year = target.getSingleValue("dc.date.copyright");
		String yvnode = jvnode + "_" + removeSpecial(year);
		String yhandle = HANDLE_PREFIX + yvnode;
		if(!vnodes.contains(yvnode)) {
			vnodes.add(yvnode);
			// create year node
			SIPDataItem sip = NDLDataUtils.createBlankSIP(yhandle);
			sip.setFolder(pfolder + "/V" + ++pf);
			sip.add("dc.title", year);
			sip.add("dc.source", "JAMA Network");
			sip.add("dc.source.uri", "https://jamanetwork.com");
			sip.add("dc.description.searchVisibility", "false");
			sip.add("dc.identifier.other:journal", jname);
			String ipjson = NDLDataUtils.serializeIsPartOf(new IsPartOf(jhandle, jname));
			log("hierarchy.ispart", yvnode + " => " + ipjson);
			sip.add("dc.relation.ispartof", ipjson);
			vsipnodes.add(sip); // add to items
			
			// hierarchy
			hierarchy.add(new String[]{jvnode, yvnode, year, year});
		}
		
		/*String issued = target.getSingleValue("dc.date.issued");
		if(issued.contains(",")) {
			issued = issued.substring(0, issued.indexOf(','));
		} else {
			issued = issued.split(" +")[0]; // first token
			//System.out.println("Issued: " + issued);
		}*/
		String vol = target.getSingleValue("dc.identifier.other:volume");
		if(StringUtils.isNotBlank(vol) && NDLDataUtils.isRoman(vol)) {
			vol = String.valueOf(NDLDataUtils.romanToDecimal(vol));
		}
		String issue = target.getSingleValue("dc.identifier.other:issue");
		String tissue = null;
		if(StringUtils.isNotBlank(issue)) { 
			tissue = issue.replaceFirst(" *\\(.+\\)$", "").trim();
			if(NDLDataUtils.isRoman(tissue)) {
				tissue = String.valueOf(NDLDataUtils.romanToDecimal(tissue));
			}
		}
		//String lvnode = yvnode + "_" + removeSpecial(issued) + "_V" + removeSpecial(vol) + "_I" + removeSpecial(tissue);
		String last, lhandle, lvnode;
		if(StringUtils.isNotBlank(vol) && StringUtils.isNotBlank(issue)) {
			// volume issue exists
			lvnode = yvnode + "_V" + removeSpecial(vol) + "_I" + removeSpecial(tissue);
			lhandle = HANDLE_PREFIX + lvnode;
			/*String last = issued + (StringUtils.isNotBlank(vol) ? (" Vol. " + vol) : "")
					+ (StringUtils.isNotBlank(issue) ? (" No. " + issue) : "");*/
			last = " Vol. " + vol + " No. " + issue;
			if(!vnodes.contains(lvnode)) {
				vnodes.add(lvnode);
				// create last node
				SIPDataItem sip = NDLDataUtils.createBlankSIP(lhandle);
				sip.setFolder(pfolder + "/V" + ++pf);
				sip.add("dc.title", last);
				sip.add("dc.source", "JAMA Network");
				sip.add("dc.source.uri", "https://jamanetwork.com");
				sip.add("dc.description.searchVisibility", "false");
				sip.add("dc.identifier.other:journal", jname);
				sip.add("dc.identifier.other:volume", vol);
				sip.add("dc.identifier.other:issue", issue);
				String ipjson = NDLDataUtils.serializeIsPartOf(new IsPartOf(yhandle, year));
				log("hierarchy.ispart", lvnode + " => " + ipjson);
				sip.add("dc.relation.ispartof", ipjson);
				vsipnodes.add(sip); // add to items
				
				/*if(StringUtils.contains(vol, "13a") || StringUtils.contains(tissue, "13a")) {
					System.err.println("ID: " + target.getId());
				}*/
				
				// hierarchy
				hierarchy.add(new String[]{yvnode, lvnode, last, normalize2number(vol) + "#" + normalize2number(tissue)});
			}
		} else {
			// directly added to year
			last = year;
			lhandle = yhandle;
			lvnode = yvnode;
		}
		
		// item parent
		String lid = NDLDataUtils.getHandleSuffixID(target.getId());
		String ipjson = NDLDataUtils.serializeIsPartOf(new IsPartOf(lhandle, last));
		log("hierarchy.ispart", lid + " => " + ipjson);
		target.add("dc.relation.ispartof", ipjson);
		// hierarchy
		String sp = "P" + NDLDataUtils.NVL(target.getSingleValue("dc.format.extent:startingPage"));
		hierarchy.add(new String[] { lvnode, lid, target.getSingleValue("dc.title"), "P" + normalize2number(sp)});
		
		return true;
	}
	
	int normalize2number(String text) {
		if(StringUtils.isBlank(text)) {
			return Integer.MIN_VALUE;
		}
		if(text.startsWith("P")) {
			text = text.substring(1);
			if(StringUtils.isBlank(text)) {
				return Integer.MAX_VALUE;
			}
		}
		if(NumberUtils.isDigits(text)) {
			// plain numeric
			return Integer.valueOf(text);
		} else {
			// no numeric
			int l = text.length();
			int c = 0;
			for(int i = 0; i < l; i++) {
				/// add up ascii values
				c += (int)text.charAt(i);
			}
			return c;
		}
	}
	
	@Override
	public void postProcessData() throws Exception {
		super.postProcessData(); // super call
		
		// add extra items
		System.out.println("Adding virutual nodes(" + vsipnodes.size() + ")...");
		for(SIPDataItem sip : vsipnodes) {
			writeItem(sip);
		}
		
		// sorting hierarchy data
		System.out.println("Sorting and wirting...");
		Collections.sort(hierarchy, new Comparator<String[]>() {
			// comparison logic
			@Override
			public int compare(String[] first, String[] second) {
				int c = first[0].compareTo(second[0]);
				if(c == 0) {
					 if(NumberUtils.isDigits(first[3]) && NumberUtils.isDigits(second[3])) {
						 // year comparison descending
						 return Integer.valueOf(second[3]).compareTo(Integer.valueOf(first[3]));
					 } else if(StringUtils.contains(first[3], "#") && StringUtils.contains(second[3], "#")) {
						 // volume issue
						 String t1[] = first[3].split("#");
						 String t2[] = second[3].split("#");
						 t1[0] = extractn(t1[0]);
						 t1[1] = extractn(t1[1]);
						 t2[0] = extractn(t2[0]);
						 t2[1] = extractn(t2[1]);
						 // descending order
						 c = Integer.valueOf(t2[0]).compareTo(Integer.valueOf(t1[0]));
						 if(c == 0) {
							 return Integer.valueOf(t2[1]).compareTo(Integer.valueOf(t1[1])); 
						 } else {
							 return c;
						 }
					} else if(StringUtils.startsWith(first[3], "P") && StringUtils.startsWith(second[3], "P")) {
						 String p1 = first[3].substring(1);
						 String p2 = second[3].substring(1);
						 return Integer.valueOf(p1).compareTo(Integer.valueOf(p2));
					} else {
						// same order
						return 0;
					}
				} else {
					return c;
				}
			}
		});
		CSVWriter hierarchyw = NDLDataUtils.openCSV(new File(logLocation, "hierarchy.data.csv"));
		hierarchyw.writeNext(new String[]{"PHandle", "CHandle", "CTitle", "Order"});
		for(String[] h : hierarchy) {
			// write data
			//log("hiearchy.data", h);
			hierarchyw.writeNext(h);
		}
		hierarchyw.close();
	}
	
	static String extractn(String txt) {
		Matcher m = VOL_ISSUE_PATTERN.matcher(txt);
		if(m.matches()) {
			return m.group(1);
		} else {
			 return txt;
		}
	}
	
	public static void main(String[] args) throws Exception {
		
		String input = "/home/dspace/debasis/NDL/NDL_sources/JAMA/May.16.16.42.13.Level8.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/NDL_sources/JAMA/logs";
		String outputLocation = "/home/dspace/debasis/NDL/NDL_sources/JAMA/out";
		String name = "JAMA.stitching.v1";
		
		JAMAStitching1 p = new JAMAStitching1(input, logLocation, outputLocation, name);
		p.turnOffLoadHierarchyFlag();
		p.turnOffControlFieldsValidationFlag();
		//p.addCSVLogger("hiearchy.data", new String[]{"PHandle", "CHandle", "CTitle", "Order"});
		p.addTextLogger("hierarchy.ispart");
		p.processData();
		
		System.out.println("Done.");
	}
	
	public static void main1(String[] args) {
		System.out.println(extractn("x13a"));
	}
}