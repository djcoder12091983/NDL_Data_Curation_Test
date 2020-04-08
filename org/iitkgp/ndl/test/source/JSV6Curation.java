package org.iitkgp.ndl.test.source;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.core.NDLDataNode;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;
import org.iitkgp.ndl.util.NDLDataUtils;

public class JSV6Curation extends NDLSIPCorrectionContainer {
	
	Set<String> titlesnolrt;
	Set<String> conferences;
	Set<String> journals;
	
	Map<String, String> tlrt1;
	Map<String, String> tlrt2;
	Map<String, String> tlrt3;

	public JSV6Curation(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		
		String handle = target.getId();
		
		List<NDLDataNode> subjects = target.getNodes("dc.subject");
		for(NDLDataNode subject : subjects) {
			if(subject.getTextContent().matches("^.*<font size=\".*\">.*$")) {
				// delete
				subject.remove();
			}
		}
		
		delete("dc.contributor.other:actor", "dc.publisher.institution", "lrmi.educationalAlignment.pedagogicObjective",
				"dc.coverage.spatial", "dc.relation.requires");
		transformFieldByExactMatch("dc.contributor.other:organization", "org", ';');
		
		if(target.exists("dc.contributor.other:organization")) {
			log("org.new", new String[] { handle,
					NDLDataUtils.join(target.getValue("dc.contributor.other:organization"), '#') });
		}
		
		if(isUnassigned("dc.language.iso")) {
			target.add("dc.language.iso", "eng");
		}
		if(isUnassigned("dc.rights.accessRights")) {
			target.add("dc.rights.accessRights", "open");
		}
		
		move("dc.description", "dc.description.abstract");
		move("ndl.sourceMeta.additionalInfo:DegreeType", "dc.description");
		target.add("dc.type", "text");
		target.add("dc.description.searchVisibility", "true");
		
		String title = NDLDataUtils.NVL(target.getSingleValue("dc.title"), StringUtils.EMPTY).trim();
		if(titlesnolrt.contains(title)) {
			target.addIfNotContains("lrmi.learningResourceType", "nolrt");
		}
		
		String agerange = target.getSingleValue("lrmi.typicalAgeRange");
		if(conferences.contains(agerange)) {
			if(StringUtils.equals(agerange, title.replaceFirst("\\([0-9]+\\)$", "").trim())) {
				target.add("lrmi.learningResourceType", "conferenceProceedings");
			} else {
				String replaceBy = NDLDataUtils.getHandleSuffixID(handle);
				if(replaceBy.contains("_")) {
					replaceBy = replaceBy.substring(0, replaceBy.indexOf('_'));
					String v = agerange + "(" + replaceBy + ")";
					target.add("dc.identifier.other:conferenceProceedings", v);
					log("journal.cp", new String[]{handle, v});
					
				} else {
					System.err.println("ERROR: " + handle);
				}
			}
		} else if(journals.contains(agerange)) {
			if(StringUtils.equals(agerange, title.replaceFirst("\\([0-9]+\\)$", "").trim())) {
				target.add("lrmi.learningResourceType", "journal");
			} else {
				String replaceBy = NDLDataUtils.getHandleSuffixID(handle);
				if(replaceBy.contains("_")) {
					replaceBy = replaceBy.substring(0, replaceBy.indexOf('_'));
					String v = agerange + "(" + replaceBy + ")";
					target.add("dc.identifier.other:journal", v);
					log("journal.cp", new String[]{handle, v});
				} else {
					System.err.println("ERROR: " + handle);
				}
			}
		}
		target.delete("lrmi.typicalAgeRange");
		
		/*if(handle.equals("jstage/isijinternational1989_42_4_42_4_456")) {
			System.out.println("title: " + title);
		}*/
		// title to LRT map
		if(isUnassigned("lrmi.learningResourceType")) {
			// exactly match
			for(String k : tlrt2.keySet()) {
				if(StringUtils.equalsIgnoreCase(title, k)) {
					target.add("lrmi.learningResourceType", tlrt2.get(k));
					log("title.2.lrt.apping", new String[]{handle, title, tlrt2.get(k)});
					break;
				}
			}
		}
		if(isUnassigned("lrmi.learningResourceType")) {
			// starts with
			for(String k : tlrt1.keySet()) {
				if(StringUtils.startsWithIgnoreCase(title, k)) {
					target.add("lrmi.learningResourceType", tlrt1.get(k));
					log("title.2.lrt.apping", new String[]{handle, title, tlrt1.get(k)});
					break;
				}
			}
		}
		if(isUnassigned("lrmi.learningResourceType")) {
			// contains
			for(String k : tlrt3.keySet()) {
				if(StringUtils.containsIgnoreCase(title, k)) {
					target.add("lrmi.learningResourceType", tlrt3.get(k));
					log("title.2.lrt.apping", new String[]{handle, title, tlrt3.get(k)});
					break;
				}
			}
		}
		
		
		return true;
	}
	
	public static void main(String[] args) throws Exception {
		
		String input = "/home/dspace/debasis/NDL/NDL_sources/JSTAGE/in/2019.Aug.09.11.42.42.js.v5.2.Corrected.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/NDL_sources/JSTAGE/logs"; // log location if any
		String outputLocation = "/home/dspace/debasis/NDL/NDL_sources/JSTAGE/out";
		String name = "js.v6";
		
		String orgfile = "/home/dspace/debasis/NDL/NDL_sources/JSTAGE/conf/org.csv";
		String tnolrtfile = "/home/dspace/debasis/NDL/NDL_sources/JSTAGE/conf/titles.nolrt";
		String conferencefile = "/home/dspace/debasis/NDL/NDL_sources/JSTAGE/conf/conference.proceedings";
		String jfile = "/home/dspace/debasis/NDL/NDL_sources/JSTAGE/conf/journals";
		
		String tlrt1file = "/home/dspace/debasis/NDL/NDL_sources/JSTAGE/conf/tlrt1.csv";
		String tlrt2file = "/home/dspace/debasis/NDL/NDL_sources/JSTAGE/conf/tlrt2.csv";
		String tlrt3file = "/home/dspace/debasis/NDL/NDL_sources/JSTAGE/conf/tlrt3.csv";
		
		JSV6Curation p = new JSV6Curation(input, logLocation, outputLocation, name);
		p.turnOffLoadHierarchyFlag();
		p.addMappingResource(orgfile, "Old", "org");
		p.titlesnolrt = NDLDataUtils.loadSet(tnolrtfile);
		p.journals = NDLDataUtils.loadSet(jfile);
		p.conferences = NDLDataUtils.loadSet(conferencefile);
		
		p.tlrt1 = NDLDataUtils.loadKeyValue(tlrt1file, true);
		p.tlrt2 = NDLDataUtils.loadKeyValue(tlrt2file, true);
		p.tlrt3 = NDLDataUtils.loadKeyValue(tlrt3file, true);
		p.addCSVLogger("title.2.lrt.apping", new String[]{"Handle", "Title", "LRT"});
		p.addCSVLogger("journal.cp", new String[]{"Handle", "journal/CP"});
		p.addCSVLogger("org.new", new String[]{"Handle", "org"});
		
		p.correctData();
		System.out.println("Done.");
	}

}