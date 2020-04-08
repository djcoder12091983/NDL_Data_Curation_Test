package org.iitkgp.ndl.test.source;

import java.io.File;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.iitkgp.ndl.core.NDLDataNode;
import org.iitkgp.ndl.data.NDLLanguageDetail;
import org.iitkgp.ndl.data.NDLLanguageTranslate;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;
import org.iitkgp.ndl.data.normalizer.NDLDataNormalizer;
import org.iitkgp.ndl.util.NDLDataUtils;
import org.iitkgp.ndl.util.NDLServiceUtils;

import com.opencsv.CSVReader;

public class CERNPhase1Correction extends NDLSIPCorrectionContainer {
	
	Set<String> wrongAuthors = null;
	Set<String> wrongEISBN = null;
	Set<String> falseTitles = null;
	Set<String> translationCorrection = null;
	Map<String, Map<String, StringBuilder>> identifiers = new HashMap<String, Map<String, StringBuilder>>();
	
	NDLDataNormalizer nameNormalizer = new NDLDataNormalizer() {
		
		@Override
		public Collection<String> transform(String input) {
			input = input.replaceFirst("^ *\\,", "");
			StringBuilder modified = new StringBuilder();
			String tokens[] = input.split(" +");
			for(String t : tokens) {
				if(StringUtils.isAllUpperCase(t)) {
					int l = t.length();
					for(int i = 0 ;i < l; i++) {
						modified.append(t.charAt(i)).append(" ");
					}
				} else {
					modified.append(t).append(" ");
				}
			}
			String name = modified.toString();
			//System.out.println("N: " + name);
			String newname = NDLDataUtils.normalizeSimpleName(name);
			//System.out.println("NN: " + newname);
			return NDLDataUtils.createNewList(newname);
		}
	};
	
	NDLDataNormalizer dateNormalizer = new NDLDataNormalizer() {
		
		@Override
		public Collection<String> transform(String input) {
			try {
				String date = NDLServiceUtils.normalilzeDate(input);
				if(date == null) {
					return null;
				} else {
					Date d = DateUtils.parseDate(date, "yyyy-MM-dd");
					if(d.after(new Date())) {
						// date is greater than current date
						return null;
					} else {
						// valid date
						return NDLDataUtils.createNewList(date);
					}
				}
			} catch(Exception ex) {
				// error delete
				return null;
			}
		}
	};

	public CERNPhase1Correction(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	void loadIdentifiers(String file) throws Exception {
		CSVReader reader = NDLDataUtils.readCSV(new File(file), 1);
		String line[] = null;
		while((line = reader.readNext()) != null) {
			String id = line[0];
			String value = line[1];
			Map<String, StringBuilder> values = identifiers.get(id);
			if(values == null) {
				values = new HashMap<String, StringBuilder>();
				identifiers.put(id, values);
			}
			String idval = value.substring(0, value.indexOf('(')).trim();
			if(StringUtils.containsIgnoreCase(value, "(print version)")) {
				StringBuilder isbn = values.get("dc.identifier.isbn");
				if(isbn == null) {
					isbn = new StringBuilder();
					values.put("dc.identifier.isbn", isbn);
				}
				isbn.append(idval).append('|');
			} else {
				StringBuilder eisbn = values.get("dc.identifier.other:eisbn");
				if(eisbn == null) {
					eisbn = new StringBuilder();
					values.put("dc.identifier.other:eisbn", eisbn);
				}
				eisbn.append(idval).append('|');
			}
		}
		reader.close();
	}
	
	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		
		String id = NDLDataUtils.getHandleSuffixID(target.getId());
		if(id.equals("1972502")) {
			// delete
			return false;
		}
		
		if(id.equals("2047404")) {
			target.updateSingleValue("dc.title", "Serkant|Bosphorus|Erkcan");
		}
		
		delete("dc.description.sponsorship", "dc.relation.haspart", "dc.identifier.other:itemId",
				"ndl.sourceMeta.additionalInfo:relatedContentUrl");
		
		// delete and update
		delete("dc.contributor.author");
		transformFieldsById("author", '|', "<Author,dc.contributor.author>");
		deleteIfContains("dc.contributor.author", wrongAuthors);
		target.replaceByRegex("dc.contributor.author", "&#x2122|\"", "");
		
		// id correction
		delete("dc.identifier.isbn", "dc.identifier.other:eisbn", "dc.identifier.issn");
		Map<String, StringBuilder> values = identifiers.get(id);
		if(values != null) {
			for(String key : values.keySet()) {
				String tokens[] = values.get(key).toString().split("\\|");
				for(String token : tokens) {
					int l = token.length();
					if(l == 10 || l == 13) {
						add(key, token);
					} else if(l == 8) {
						if(!target.exists("dc.identifier.issn")) {
							add("dc.identifier.issn", token);
						}
					}
				}
			}
		}
		retainFirst("dc.identifier.isbn"); // remove multi-valued
		deleteIfContains("dc.identifier.other:eisbn", wrongEISBN);
		deleteIfContains("dc.identifier.isbn", "3780193006955");
		
		retainFirst("dc.publisher.date");
		String sd = target.getSingleValue("dc.date.other:sponsordate");
		if(StringUtils.isNotBlank(sd)) {
			target.updateSingleValue("dc.publisher.date", sd.replace("\\", ""));
		}
		delete("dc.date.other:sponsordate");
		normalizeByOwn("dc.publisher.date", dateNormalizer, true); // delete invalid date
		
		transformFieldByExactMatch("dc.contributor.author", "author11");
		target.replaceByRegex("dc.contributor.author", "\\(.*$", "");
		target.replaceByRegex("dc.contributor.editor", "\\[et al\\.\\]", "");
		deleteIfContains("dc.contributor.editor",
				"Organizers: Prof.Alexander Studenikin, Chairman, studenik@srd.sinp.msu.ru");
		deleteIfContains("dc.identifier.other:doi", "10.1007\\/JHEP11(2016)0472", "10.5170\\/CERN–2013–003",
				"10.1140\\/epjc\\/s10052-014-3014-0", "10.1007\\/JHEP11(2016)047, 10.1007\\/JHEP04(2017)142");
		
		normalizeByOwn("dc.contributor.author", nameNormalizer);
		normalizeByOwn("dc.contributor.editor", nameNormalizer);
		
		// language
		String title = target.getSingleValue("dc.title");
		if(StringUtils.containsIgnoreCase(title, "french")) {
			target.addIfNotContains("dc.language.iso", "fra");
		}
		// visibility
		if (StringUtils.equalsIgnoreCase(title, "No caption") || StringUtils.equalsIgnoreCase(title, "Correction")
				|| falseTitles.contains(title)) {
			// false case
			target.updateSingleValue("dc.description.searchVisibility", "false");
		} else if(StringUtils.equalsIgnoreCase(title, "Discussion")) {
			target.updateSingleValue("dc.description.searchVisibility", "true");
		}
		
		if(isUnassigned("dc.rights.accessRights")) {
			target.add("dc.rights.accessRights", "open");
		}
		if(isUnassigned("dc.description.searchVisibility")) {
			target.add("dc.description.searchVisibility", "true");
		}
		
		transformFieldByExactMatch("dc.identifier.other:volume", "vol");
		target.replaceByRegex("dc.identifier.other:volume", "^(0|\\.)", "");
		
		// remove duplicates and HTML tag
		/*if(target.getId().equals("cern/45957")) {
			System.out.println(getValue("dc.description"));
			System.out.println();
			System.out.println();
		}*/
		target.removeDuplicate("dc.description");
		/*if(target.getId().equals("cern/45957")) {
			if(target.getId().equals("cern/45957")) {
				System.out.println(getValue("dc.description"));
				System.out.println();
				System.out.println();
			}
		}*/
		target.deleteDuplicateFieldValues("dc.title", "dc.title.alternative", "dc.publisher.place");
		target.deleteDuplicateFieldValues("dc.description.abstract","dc.description", "dc.title.alternative");
		target.deleteDuplicateFieldValues("dc.title.alternative", "dc.publisher");
		target.deleteDuplicateFieldValues("dc.publisher.place", "dc.subject");
		target.deleteDuplicateFieldValues("dc.contributor.author", "dc.contributor.editor", "dc.subject");
		target.deleteDuplicateFieldValues("dc.publisher.date", "dc.date.awarded", "dc.date.created");
		target.removeBlank("dc.description");
		removeMultipleSpaces("dc.rights.license", "dc.contributor.author", "dc.contributor.editor",
				"dc.title.alternative", "dc.relation.ispartofseries", "dc.contributor.other", "dc.description",
				"dc.description.abstract", "dc.publisher.place", "dc.publisher");
		
		// description and language translation logic
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
		
		// switch
		// TODO re-work
		if(translationCorrection.contains(id)) {
			String desc = getSingleValue("dc.description");
			List<NDLDataNode> translates = target.getNodes("ndl.sourceMeta.translation");
			for(NDLDataNode translate : translates) {
				NDLLanguageTranslate t = NDLDataUtils.desrializeLanguageTranslation(translate.getTextContent());
				if(t.getField().equals("description")) {
					NDLLanguageDetail d = t.getValues().get(0).get(0);
					target.updateSingleValue("dc.description", d.getTranslation());
					translate.remove();
					break;
				}
			}
			target.add("ndl.sourceMeta.translation",
					NDLDataUtils.serializeLanguageTranslation(new NDLLanguageTranslate("description",
							NDLDataUtils.createList(new NDLLanguageDetail("fra", desc)))));

		}
		
		return true;
	}
	
	/*public static void main(String[] args) {
		System.out.println(StringEscapeUtils.unescapeHtml4("Speti&#x2122;"));
	}*/
	
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/generated_xml_data/CERN/in/2018.Dec.14.12.07.43.CERN.P1.V7.Corrected.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/generated_xml_data/CERN/logs";
		String outputLocation = "/home/dspace/debasis/NDL/generated_xml_data/CERN/out";
		String name = "cern.phase1.v2";
		
		String authorConf = "/home/dspace/debasis/NDL/generated_xml_data/CERN/conf/13.12.2018.author";
		String wrongAuthorFile = "/home/dspace/debasis/NDL/generated_xml_data/CERN/conf/wrong.authors";
		String wrongEISBNFile = "/home/dspace/debasis/NDL/generated_xml_data/CERN/conf/wrong.eisbn";
		String idFile = "/home/dspace/debasis/NDL/generated_xml_data/CERN/conf/cern-id.csv";
		String volFile = "/home/dspace/debasis/NDL/generated_xml_data/CERN/conf/volume.conf.csv";
		String authorFile = "/home/dspace/debasis/NDL/generated_xml_data/CERN/conf/author.conf.csv";
		String falseTitleFile = "/home/dspace/debasis/NDL/generated_xml_data/CERN/conf/false.visibility.titles";
		String translationCorrectionFile = "/home/dspace/debasis/NDL/generated_xml_data/CERN/conf/translation.correct";
		
		CERNPhase1Correction p = new CERNPhase1Correction(input, logLocation, outputLocation, name);
		p.turnOffLoadHierarchyFlag();
		p.addTextLogger("sourceMeta.translation.log");
		p.wrongAuthors = NDLDataUtils.loadSet(wrongAuthorFile);
		p.wrongEISBN = NDLDataUtils.loadSet(wrongEISBNFile);
		p.falseTitles = NDLDataUtils.loadSet(falseTitleFile);
		p.translationCorrection = NDLDataUtils.loadSet(translationCorrectionFile);
		p.loadIdentifiers(idFile);
		p.addMappingResource(authorConf, "Handle_ID", "author");
		p.addMappingResource(volFile, "Old", "vol");
		p.addMappingResource(authorFile, "Old", "author11");
		p.correctData();
		
		System.out.println("Done.");
	}
}