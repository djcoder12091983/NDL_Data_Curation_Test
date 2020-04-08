package org.iitkgp.ndl.test.source;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.core.NDLDataNode;
import org.iitkgp.ndl.data.NDLLanguageDetail;
import org.iitkgp.ndl.data.NDLLanguageTranslate;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;
import org.iitkgp.ndl.util.NDLDataUtils;

public class SIPCorrectionTest extends NDLSIPCorrectionContainer {
	Set<String> wrongAuthors = null;
	
	// constructor
	public SIPCorrectionTest(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	// correction logic
	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		
		// some sample corrections
		/*removeMultipleLines("dc.description.abstract", "dc.title");
		removeMultipleSpaces("dc.description.abstract", "dc.title");*/
		//split("dc.contributor.author", ';');
		//delete("dc.identifier.citation");
		//retainFirst("dc.publisher.date");
		/*if(getValue("dc.publisher.date").size() > 1) {
			System.err.println("Error!!");
		}*/
		
		// delete and update
		/*delete("dc.contributor.author");
		transformFieldsById("author", '|', "<Author,dc.contributor.author>");
		deleteIfContains("dc.contributor.author", wrongAuthors);
		target.replaceByRegex("dc.contributor.author", "™|\"", "");
		
		transformFieldByExactMatch("dc.contributor.author", "author11");
		target.replaceByRegex("dc.contributor.author", "\\(.*$", "");
		target.replaceByRegex("dc.contributor.editor", "\\[et al\\.\\]", "");
		
		if(target.getId().equals("cern/111075")) {
			System.out.println("1.");
			System.out.println(getValue("dc.contributor.author"));
			System.out.println(getValue("dc.contributor.editor"));
			System.out.println();
		}
		
		target.deleteDuplicateFieldValues("dc.contributor.author", "dc.contributor.editor", "dc.subject");
		
		if(target.getId().equals("cern/111075")) {
			System.out.println("2.");
			System.out.println(getValue("dc.contributor.author"));
			System.out.println(getValue("dc.contributor.editor"));
			System.out.println();
		}*/
		String title = target.getSingleValue("dc.title");
		if(StringUtils.containsIgnoreCase(title, "french")) {
			target.addIfNotContains("dc.language.iso", "fra");
		}
		if(target.getId().equals("cern/39045")) {
			System.out.println(getValue("dc.description"));
			System.out.println();
			System.out.println();
		}
		target.removeDuplicate("dc.description");
		target.removeBlank("dc.description");
		removeMultipleSpaces("dc.description");
		target.deleteDuplicateFieldValues("dc.description.abstract","dc.description", "dc.title.alternative");
		
		List<NDLDataNode> descNodes = target.getNodes("dc.description");
		List<NDLLanguageDetail> descLangs = new LinkedList<NDLLanguageDetail>();
		List<String> langtext = target.getValue("dc.language.iso");
		int s = langtext.size();
		List<String> texts = new LinkedList<String>();
		for(int i = 0; i < descNodes.size(); i++) {
			NDLDataNode descNode = descNodes.get(i);
			String desc = descNode.getTextContent();
			if (StringUtils.startsWithIgnoreCase(desc, "Collaboration with:")
					|| StringUtils.startsWithIgnoreCase(desc, "Presented at :")
					|| StringUtils.startsWithIgnoreCase(desc, "Published in :")) {
				// valid description
				continue;
			}
			texts.add(desc);
			descNode.remove();
		}
		if(target.getId().equals("cern/39045")) {
			System.out.println(texts);
			System.out.println(langtext);
		}
		int s1 = texts.size();
		if(s > 1 && s == s1) {
			Iterator<String> languages = langtext.iterator();
			for(String txt : texts) {
				if(languages.hasNext()) {
					String lang = languages.next();
					if(lang.equals("eng")) {
						target.add("dc.description", txt);
					} else {
						descLangs.add(new NDLLanguageDetail(lang, txt));
					}
				}
			}
		} else if(s1 == 1) {
			target.add("dc.description", texts.get(0));
		}
		List<NDLLanguageDetail> titleLangs = new LinkedList<NDLLanguageDetail>();
		if(s > 1) {
			Iterator<String> languages = langtext.iterator();
			List<NDLDataNode> titleNodes = target.getNodes("dc.title.alternative");
			for(int i = 0; i < titleNodes.size(); i++) {
				NDLDataNode titleNode = titleNodes.get(i);
				title = titleNode.getTextContent();
				if(languages.hasNext()) {
					String lang = languages.next();
					if(!lang.equals("eng")) {
						titleLangs.add(new NDLLanguageDetail(lang, title));
						titleNode.remove();
					}
				}
			}
		}
		
		if(!descLangs.isEmpty()) {
			String t = NDLDataUtils.serializeLanguageTranslation(new NDLLanguageTranslate("description", descLangs));
			target.add("ndl.sourceMeta.translation", t);
			log("sourceMeta.translation.log", target.getId());
			log("sourceMeta.translation.log", t);
		}
		if(!titleLangs.isEmpty()) {
			String t = NDLDataUtils
					.serializeLanguageTranslation(new NDLLanguageTranslate("title.alternative", titleLangs));
			target.add("ndl.sourceMeta.translation", t);
			log("sourceMeta.translation.log", target.getId());
			log("sourceMeta.translation.log", t);
		}
		
		/*// name normalization
		normalizeByOwn("dc.contributor.author", new NDLDataNormalizer() {
			
			@Override
			public Collection<String> transform(String input) {
				if(!NDLDataUtils.containsAny(input, true, "college", "institute")) {
					// normalized version
					return NDLDataUtils.createNewList(NDLDataUtils.normalizeSimpleName(input));
				} else {
					// empty list
					return NDLDataUtils.createNewList(input);
				}
			}
		});*/
		
		//normalizeByOwn("xx", new NDLSerialNumberNormalizerWithLeftZeroPadding(8));
		
		return true;
	}
	
	// testing
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/generated_xml_data/CERN/in/2018.Dec.14.12.07.43.CERN.P1.V7.Corrected.tar.gz"; // flat SIP location or compressed SIP location
		String logLocation = "/home/dspace/debasis/NDL/generated_xml_data/CERN/logs"; // log location if any
		String outputLocation = "/home/dspace/debasis/NDL/generated_xml_data/CERN/out";
		String name = "cern.phase1";

		/*String authorConf = "/home/dspace/debasis/NDL/generated_xml_data/CERN/conf/13.12.2018.author";
		String wrongAuthorFile = "/home/dspace/debasis/NDL/generated_xml_data/CERN/conf/wrong.authors";
		String authorFile = "/home/dspace/debasis/NDL/generated_xml_data/CERN/conf/author.conf.csv";*/
		
		SIPCorrectionTest p = new SIPCorrectionTest(input, logLocation, outputLocation, name);
		/*p.addMappingResource(authorConf, "Handle_ID", "author");
		p.wrongAuthors = NDLDataUtils.loadSet(wrongAuthorFile);
		p.addMappingResource(authorFile, "Old", "author11");*/
		//p.setMultipleValueSeparator('|');
		//p.addNormalizer("dc.identifier.issn", new NDLSerialNumberNormalizerWithLeftZeroPadding(8));
		p.addTextLogger("sourceMeta.translation.log");
		p.correctData();
		
		System.out.println("Done.");
	}

}