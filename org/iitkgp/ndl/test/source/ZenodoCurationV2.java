package org.iitkgp.ndl.test.source;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.iitkgp.ndl.core.NDLDataNode;
import org.iitkgp.ndl.data.NDLAssetType;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;
import org.iitkgp.ndl.data.normalizer.NDLDataNormalizer;
import org.iitkgp.ndl.util.NDLDataUtils;

public class ZenodoCurationV2 extends NDLSIPCorrectionContainer {
	
	Pattern PAGES = Pattern.compile("[a-zA-Z0-9]+(-|–|−)[a-zA-Z0-9]+");
	
	Map<String, String> itypemap;
	Map<String, String> langmap;
	Map<String, String> eumap;
	Map<String, String> lrtmap;
	Set<String> eudelete;
	
	int descc = 0;
	int absremove = 0;
	int ftc = 0;
	
	public ZenodoCurationV2(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		
		// item delete
		if(target.contains("lrmi.educationalUse", eudelete) || target.getId().equals("zenodo/59170")) {		
			return false;
		}
		
		target.add("dc.description.searchVisibility", "true");
		/*transformFieldByExactMatch("lrmi.learningResourceType", "lrmi.interactivityType", itypemap);
		transformFieldByExactMatch("lrmi.learningResourceType", "lrmi.educationalUse", eumap);*/
		// LRT
		List<String> itypes = target.getValue("lrmi.interactivityType");
		//boolean delete = false;
		Set<String> lrts = new HashSet<String>(4);
		for(String itype : itypes) {
			if(itypemap.containsKey(itype)) {
				//delete = true;
				String v = itypemap.get(itype);
				if(!StringUtils.equalsIgnoreCase(v, "delete")) {
					lrts.add(v);
				}
			}
		}
		List<String> eusages = target.getValue("lrmi.educationalUse");
		for(String eusage : eusages) {
			if(eumap.containsKey(eusage)) {
				//delete = true;
				String v = eumap.get(eusage);
				if(!StringUtils.equalsIgnoreCase(v, "delete")) {
					lrts.add(v);
				}
			}
		}
		if(!lrts.isEmpty()) {
			delete("lrmi.learningResourceType");
			add("lrmi.learningResourceType", lrts);
		}
		// further LRT mapping
		transformFieldByExactMatch("lrmi.learningResourceType", lrtmap);
		target.removeDuplicate("lrmi.learningResourceType");
		
		transformFieldByExactMatch("dc.language.iso", langmap);
	
		updatepages(target);
		
		if(target.contains("dc.type", "pdf")) {
			target.add("dc.format.mimetype", "application/pdf");
			target.replace("dc.type", "pdf", "text");
		}
		
		if(isMultiValued("dc.type") && target.contains("dc.format.mimetype", "application/pdf")) {
			deleteIfNotContains("dc.type", "text");
		}
		
		transformFieldsById("mime_lrt", "<MIME,dc.format.mimetype>", "<Type,dc.type>");
		
		target.deleteDuplicateFieldValues("dc.identifier.uri", "dc.identifier.other:alternateWebpageUri",
				"dc.identifier.other:arXiv");
		
		deleteIfContainsByRegex("ndl.sourceMeta.additionalInfo:relatedContentUrl", ".*api.*");
		if(isSingleValued("dc.relation.ispartofseries")) {
			if(target.moveByRegex("dc.relation.ispartofseries", "dc.format.extent:size_in_Bytes", "[0-9]+") == 0) {
				// not moved successfully
				delete("dc.relation.ispartofseries");
			}
		}
		
		Map<String, String> tcontents = NDLDataUtils.mapFromJson(target.getSingleValue("dc.description.tableofcontents"));
		if(tcontents != null) {
			String acr = tcontents.get("acronym");
			String date = tcontents.get("dates");
			String place = tcontents.get("place");
			String title = tcontents.get("title");
			
			StringBuilder pub = new StringBuilder();
			boolean f = false;
			if(StringUtils.isNotBlank(title)) {
				pub.append(title);
				f = true;
			}
			if(StringUtils.isNotBlank(acr)) {
				pub.append(f ? "(" : "");
				pub.append(acr);
				pub.append(f ? ")" : "");
			}
			target.add("dc.publisher", pub.toString());
			
			if(!target.exists("dc.publisher.date")) {
				add("dc.publisher.date", date);
			}
			target.add("dc.publisher.place", place);
		}
		
		// author move
		// author info correction
		List<NDLDataNode> authors = target.getNodes("ndl.sourceMeta.additionalInfo:authorInfo");
		for(NDLDataNode anode : authors) {
			String atxt = anode.getTextContent();
			Map<String, Map<String, String>> ainfo = new HashMap<String, Map<String,String>>(2);
			Map<String, String> amap = NDLDataUtils.mapFromJson(NDLDataUtils.getValueByJsonKey(atxt, "authorInfo"));
			if(amap.containsKey("name")) {
				//String name = amap.get("name");
				//System.out.println("Name: "  + name);
				//System.out.println("[" + target.getId() + "] => " + amap.get("name"));
				add("dc.contributor.author", amap.get("name"));
			}
			ainfo.put("authorInfo", amap);
			// JSON
			String json = NDLDataUtils.removeMultipleSpaces(NDLDataUtils.getJson(ainfo));
			//System.out.println("JSON: " + json);
			anode.setTextContent(json);
		}
		target.removeDuplicate("dc.contributor.author");
		
		// study group
		/*List<String> sgroups = target.getValue("dc.contributor.other:studyGroup");
		for(String sgroup : sgroups) {
			Map<String, String> smap = NDLDataUtils.mapFromJson(sgroup);
			if(smap.containsKey("id")) {
				// add to subject
				target.add("dc.subject", smap.get("id"));
			}
		}
		delete("dc.contributor.other:studyGroup");*/
		deleteIfContainsByRegex("dc.subject", ".*(zenodo|pa17|(open access)).*");
		
		target.move("dc.identifier.other:uniqueId", "dc.rights.license");
		// thesis research
		if(target.contains("lrmi.learningResourceType", "thesis")) {
			target.move("dc.contributor.author", "dc.creator.researcher");
		}
		
		// description
		if(target.exists("dc.description.abstract")) {
			String abs = NDLDataUtils.normalizeText(target.getSingleValue("dc.description.abstract"));
			int l = abs.length();
			if(l >= 100 && l <= 2000) {
				// move to description
				target.add("dc.description", abs);
				descc++;
			} else if (l > 2000){
				// full text
				target.addAsset(NDLAssetType.FULLTEXT, abs.getBytes());
				ftc++;
			} else {
				// remove case
				absremove++;
			}
		}
		
		// delete fields
		delete("dc.description.uri", "dc.identifier.other:openlibraryWorkId", "ndl.sourceMeta.uniqueInfo",
				"dc.description", "dc.coverage.spatial", "dc.coverage.temporal", "dc.description.abstract",
				"dc.contributor.other:studyGroup", "lrmi.interactivityType", "lrmi.educationalUse",
				"dc.description.tableofcontents");
		
		return true;
	}
	
	void updatepages(SIPDataItem target) {
		// TODO need to discuss
		if(target.exists("dc.format.extent:pages")) {
			String pages = target.getSingleValue("dc.format.extent:pages").replaceAll(" +", "");
			Matcher mp = PAGES.matcher(pages);
			target.delete("dc.format.extent:pages");
			if(mp.matches()) {
				String tokens[] = pages.split("-|–|−");
				if(NDLDataUtils.isRoman(tokens[0])) {
					tokens[0] = String.valueOf(NDLDataUtils.romanToDecimal(tokens[0]));
				}
				if(NDLDataUtils.isRoman(tokens[1])) {
					tokens[1] = String.valueOf(NDLDataUtils.romanToDecimal(tokens[1]));
				}
				if(NumberUtils.isDigits(tokens[0]) && NumberUtils.isDigits(tokens[1])) {
					int sp = Integer.parseInt(tokens[0]);
					int ep = Integer.parseInt(tokens[1]);
					if(sp <= ep) {
						target.add("dc.format.extent:startingPage", tokens[0]);
						target.add("dc.format.extent:endingPage", tokens[0]);
						target.add("dc.format.extent:pageCount", String.valueOf(ep - sp + 1));
					}
				} else {
					System.err.println("Pages: " + pages);
				}
			} else {
				// track wrong values
				System.err.println("Pages: " + pages);
			}
		}
	}
	
	@Override
	protected void intermediateProcessHandler() {
		super.intermediateProcessHandler(); // super call
		System.out.println("Description: " + descc);
		System.out.println("Ful text: " + ftc);
		System.out.println("Abstract removed: " + absremove);
	}
	
	public static void main(String[] args) throws Exception {
		
		// flat SIP location or compressed SIP location
		String input = "/home/dspace/debasis/NDL/NDL_sources/zenodo/in/2019.May.16.12.24.39.zenodo.v1.Corrected.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/NDL_sources/zenodo/logs"; // log location if any
		String outputLocation = "/home/dspace/debasis/NDL/NDL_sources/zenodo/out";
		String name = "zenodo.v2";
		
		String itypefile = "/home/dspace/debasis/NDL/NDL_sources/zenodo/conf/it.lrt.mapping.csv";
		String eufile = "/home/dspace/debasis/NDL/NDL_sources/zenodo/conf/eu.lrt.mapping.csv";
		String eudeletefile = "/home/dspace/debasis/NDL/NDL_sources/zenodo/conf/eudelete";
		String langfile = "/home/dspace/debasis/NDL/NDL_sources/zenodo/conf/lang.mapping.csv";
		String lrtfile = "/home/dspace/debasis/NDL/NDL_sources/zenodo/conf/lrt.mapping.csv";
		String mimelrtfile = "/home/dspace/debasis/NDL/NDL_sources/zenodo/conf/mime.type.mapping.csv";
		
		ZenodoCurationV2 p = new ZenodoCurationV2(input, logLocation, outputLocation, name);
		p.turnOffLoadHierarchyFlag();
		p.turnOffControlFieldsValidationFlag();
		p.itypemap = NDLDataUtils.loadKeyValue(new File(itypefile));
		p.lrtmap = NDLDataUtils.loadKeyValue(new File(lrtfile));
		p.eumap = NDLDataUtils.loadKeyValue(new File(eufile));
		p.langmap = NDLDataUtils.loadKeyValue(new File(langfile));
		p.eudelete = NDLDataUtils.loadSet(new File(eudeletefile));
		p.addCSVLogger("author.names", new String[]{"Author", "Normalized-name"});
		p.addMappingResource(mimelrtfile, "ID", "mime_lrt");
		
		Set<String> namewrongtokens = new HashSet<>();
		namewrongtokens.add("dr");
		namewrongtokens.add("sceo");
		
		p.addNormalizer("dc.contributor.author", new NDLDataNormalizer() {
			
			@Override
			public Collection<String> transform(String input) {
				input = input.replaceAll("(STUDENTS)|([0-9]+)|\\*", "");
				String splits[] = new String[]{input};
				if(StringUtils.contains(input, "and")) {
					splits = input.split(" +and +");
				}
				Set<String> names = new HashSet<String>(4);
				for(String split : splits) {
					if(split.trim().equals(",")) {
						continue; // wrong name
					}
					String fsplits[] = split.split(" *, *");
					if(fsplits[0].split(" +").length > 1 && fsplits.length > 2) {
						// multiple name exists
						for(String fsplit : fsplits) {
							names.add(NDLDataUtils.normalizeSimpleNameByWrongNames(fsplit, namewrongtokens));
						}
					} else {
						// single name
						names.add(NDLDataUtils.normalizeSimpleNameByWrongNames(split, namewrongtokens));
					}
				}
				
				/*System.out.println("INPUT=>" + input);
				System.out.println("OUTPUT=>" + names);*/
				try {
					p.log("author.names", new String[]{input, NDLDataUtils.join(names, '|')});
				} catch(IOException ex) {
					// suppress the error
					System.err.println("CSV logging error: " + ex.getMessage());
				}
				
				return names;
			}
		});
		p.processData();
		
		System.out.println("Done.");
	}

}