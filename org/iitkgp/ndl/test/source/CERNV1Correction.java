package org.iitkgp.ndl.test.source;

import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.iitkgp.ndl.data.NDLLanguageDetail;
import org.iitkgp.ndl.data.NDLLanguageTranslate;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.container.ConfigurationData;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;
import org.iitkgp.ndl.data.normalizer.NDLDataNormalizer;
import org.iitkgp.ndl.data.transformer.PrefixAdderTransformer;
import org.iitkgp.ndl.util.NDLDataUtils;

public class CERNV1Correction extends NDLSIPCorrectionContainer {
	
	Set<String> wrongItemsByLrt = null;
	Set<String> wrongLrts = null;
	//Set<String> uniqueDesc = new HashSet<String>();
	Map<String, String> advRights = null;
	Map<String, String> ddc = null;
	Map<String, String> rights = null;
	
	PrefixAdderTransformer t1 = new PrefixAdderTransformer("Published in: ");
	PrefixAdderTransformer t2 = new PrefixAdderTransformer("Volume: ");
	
	NDLDataNormalizer nameNormalizer = new NDLDataNormalizer() {
		
		// custom code
		@Override
		public Collection<String> transform(String input) {
			int l = input.length();
			Stack<Character> track = new Stack<Character>();
			StringBuilder modified = new StringBuilder();
			boolean e = false;
			for(int i = 0; i < l; i++) {
				char ch = input.charAt(i);
				if(ch == '(') {
					track.push(ch);
					continue;
				}
				if(ch == ')') {
					if(!track.isEmpty()) {
						track.pop();
					} else {
						// error
						e = true;
					}
					continue;
				}
				if(track.isEmpty()) {
					modified.append(ch);
				}
			}
			List<String> modifiedNames = new LinkedList<String>();
			String names[] = modified.toString().split(" *; *");
			for(String name : names) {
				try {
					modifiedNames.add(NDLDataUtils.normalizeSimpleName(name));
				} catch(Exception ex) {
					// track error
					System.err.println("Error: " + input + " => " + name);
				}
			}
			
			// log
			try {
				String t = NDLDataUtils.join(modifiedNames, '|');
				log("names", new String[] { getCurrentTargetItem().getId(), input, t });
				if(e) {
					// error track
					log("names.wrong", new String[] { getCurrentTargetItem().getId(), input, t });
				}
			} catch(Exception ex) {
				// error suppress
			}
						
			return modifiedNames;
		}
	};
	
	NDLDataNormalizer dateNormalizer = new NDLDataNormalizer() {
		
		// custom code
		@Override
		public Collection<String> transform(String input) {
			input = input.trim();
			String tokens[] = input.split("( *\\. *)|( *- *)|(\\?)|( +)|( *; *)");
			int l = tokens.length;
			String newdate = null;
			try {
				if(l >= 3) {
					// take last 3 tokens
					newdate = NDLDataUtils.normalizeSimpleDate(Arrays.asList(Arrays.copyOfRange(tokens, l - 3, l)));
				} else {
					// less than 3
					if(tokens[l - 1].length() == 8) {
						// date itself
						String t1 = tokens[0];
						String t2 = tokens[1];
						Date d1 = DateUtils.parseDate(t1, "yyyyMMdd");
						Date d2 = DateUtils.parseDate(t2, "yyyyMMdd");
						String t;
						if(d1.compareTo(d2) > 0) {
							t = t1;
						} else {
							t = t2;
						}
						newdate = NDLDataUtils.normalizeSimpleDate(
								Arrays.asList(t.substring(0, 4), t.substring(4, 6), t.substring(6)));
					} else {
						newdate = NDLDataUtils.normalizeSimpleDate(Arrays.asList(tokens));
					}
				}
			} catch(Exception ex) {
				// track error
				System.err.println("Error: " + input);
			}
			
			String dt = NDLDataUtils.NVL(newdate, input);
			// log
			try {
				log("dates", new String[]{getCurrentTargetItem().getId(), input, newdate});
			} catch(Exception ex) {
				// error suppress
			}
			return NDLDataUtils.createNewList(dt);
		}
	};
	
	// construction
	public CERNV1Correction(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	// correct
	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		
		/*if (target.containsByRegex("lrmi.learningResourceType", "^Administrative Circular.*$",
				"^Operational Circular.*$", "^Staff Rules and Regulations.*$")) {
			return false;
		}*/
		
		if(!target.exists("lrmi.educationalUse") && !target.exists("dc.description.uri")) {
			// delete condition
			//log("delete", getCurrentTargetItem().getId());
			return false;
		}
		
		if (target.exists("dc.date.copyright") || target.exists("dc.date.issued")
				|| target.exists("dc.relation.requires")) {
			// invalid item
			return false;
		}
		
		if(target.contains("lrmi.learningResourceType", wrongItemsByLrt)) {
			// wrong items by LRT
			return false;
		}
		
		delete("ndl.sourceMeta.uniqueInfo", "lrmi.educationalAlignment.difficultyLevel", "dc.publisher.institution",
				"dc.date.other", "dc.subject.prerequisitetopic");
		
		//String id = NDLDataUtils.getHandleSuffixID(target.getId());
		//String uri = target.getSingleValue("dc.identifier.uri");
		
		if(isUnassigned("dc.title")) {
			// title does not exist
			if(!moveFirst("dc.relation.references", "dc.title")) {
				// still not available
				moveFirst("dc.title.alternative", "dc.title");
			}
		}
		if(isUnassigned("dc.title")) {
			// title still unassigned
			return false;
		}
		move("lrmi.educationalRole", "dc.title.alternative");
		move("dc.relation.replaces", "dc.title.alternative");
		move("dc.relation.references", "dc.title.alternative");
		if(target.exists("lrmi.typicalAgeRange")) {
			target.addIfNotContains("dc.language.iso", "fra");
			move("lrmi.typicalAgeRange", "dc.title.alternative");
		}
		if(target.exists("dc.relation")) {
			target.addIfNotContains("dc.language.iso", "deu");
			move("dc.relation", "dc.title.alternative");
		}
		
		/*if(isUnassigned("dc.description.abstract")) {
			moveFirst("lrmi.educationalAlignment.educationalFramework", "dc.description.abstract");
		}*/
		merge("dc.description.abstract", "lrmi.educationalAlignment.educationalFramework");
		target.update("dc.publisher.place", t1);
		move("dc.publisher.place", "dc.description");
		target.update("dc.relation.isreferencedby", t2);
		move("dc.relation.isreferencedby", "dc.description");
		
		target.add("dc.description.searchVisibility", "true");
		if(!target.exists("dc.subject.ddc")) {
			add("dc.subject.ddc", "539|621", '|');
		}
		
		if(transformAndNormalizeFieldByExactMatch("dc.subject.ddc", ddc, ';') == 0) {
			// default value set
			delete("dc.subject.ddc");
			add("dc.subject.ddc", "539|621", '|');
		}
		target.removeDuplicate("dc.subject.ddc");
		
		split("dc.subject", ';');
		
		transformFieldByExactMatch("lrmi.learningResourceType", "lrt");
		transformFieldByExactMatch("dc.language.iso", "lang");
		
		deleteIfContains("lrmi.learningResourceType", wrongLrts);
		delete("dc.contributor.other:director");
		move("dc.contributor.other:metaCoordinator", "dc.contributor.author");
		move("dc.contributor.other:actor", "dc.contributor.advisor");
		
		// name normalization
		normalizeByOwn("dc.contributor.editor", nameNormalizer);
		normalizeByOwn("dc.contributor.advisor", nameNormalizer);
		normalizeByOwn("dc.contributor.author", nameNormalizer);
		normalizeByOwn("dc.contributor.other:speaker", nameNormalizer);
		
		moveIfContains("dc.contributor.author", "dc.contributor.other:organization",
				"The European Organization For Nuclear Research, Cern", "Universidad De Oviedo, Spain",
				"The European Organisation For Nuclear Research, Cern");
		deleteIfContains("dc.contributor.author",
				"The Henryk Niewodniczanski Institute Of Nuclear Physics Cracow, Hninp",
				"The Henryk Niewodniczanski Institute Of Nuclear Physics Of Polish Academy Of Sciences, Hninp");
		
		// description and modified editors
		for(String desc : target.getValue("dc.description.uri")) {
			String value = NDLDataUtils.getValueByJsonKey(desc, "description");
			String key = "desc_lrt." + ConfigurationData.escapeDot(value);
			if(containsMappingKey(key)) {
				target.addIfNotContains("dc.language.iso", getMappingKey(key).split("\\|"));
			}
			
			/*if(uniqueDesc.add(value)) {
				// unique
				log("description", value);
			}*/
		}
		if(isUnassigned("dc.language.iso")) {
			target.add("dc.language.iso", "eng");
		}
		if(target.exists("ndl.sourceMeta.translation")) {
			target.addIfNotContains("dc.language.iso", "fra");
			// translation json
			NDLLanguageTranslate t = new NDLLanguageTranslate("title",
					new NDLLanguageDetail("eng", target.getSingleValue("ndl.sourceMeta.translation")));
			target.updateSingleValue("ndl.sourceMeta.translation", NDLDataUtils.serializeLanguageTranslation(t));
		}
		if(target.exists("dc.description.tableofcontents")) {
			target.addIfNotContains("dc.language.iso", "fra");
			NDLLanguageTranslate t = new NDLLanguageTranslate("description",
					new NDLLanguageDetail("fra", target.getSingleValue("dc.description.tableofcontents")));
			target.add("ndl.sourceMeta.translation", NDLDataUtils.serializeLanguageTranslation(t));
			delete("dc.description.tableofcontents");
		}
		if(target.containsByRegex("lrmi.educationalRole", "Règlement financier et règles financières intérieures")) {
			target.addIfNotContains("dc.language.iso", "fra");
		}
		
		/*if(target.exists("dc.contributor.editor")) {
			log("editors", new String[]{id, NDLDataUtils.join(getValue("dc.contributor.editor"), '|')});
		}*/
		
		target.deleteDuplicateFieldValues("dc.title", "dc.title.alternative");
		
		// access rights
		target.replaceByRegex("dc.rights.accessRights", "^Expired.*$", "open");

		if (isUnassigned("dc.rights.accessRights")
				&& target.containsByRegex("lrmi.educationalUse", "^.*((CERN library copies)|(Purchase it for me!)).*$")) {
			target.addIfNotContains("dc.rights.accessRights", "authorized");
		}
		
		if(isUnassigned("dc.rights.accessRights")) {
			// still not available
			transformFieldByWordMatch("dc.rights.accessRights", "dc.contributor.advisor", advRights);
		}
		
		delete("dc.contributor.advisor", "lrmi.educationalUse");
		
		// date
		move("dc.identifier.issn", "dc.publisher.date");
		deleteIfContainsByRegex("dc.publisher.date", "^[A-Z][a-z]+$");
		
		normalizeByOwn("dc.publisher.date", dateNormalizer);
		removeMultipleSpaces("dc.date.created");
		normalizeByOwn("dc.date.awarded", dateNormalizer);
		
		// rights
		removeMultipleSpaces("ndl.sourceMeta.additionalInfo:RightsStatement");
		List<String> rsValues = target.getValue("ndl.sourceMeta.additionalInfo:RightsStatement");
		for(String rs : rsValues) {
			if(rs.contains("http://arxiv.org/licenses/nonexclusive-distrib/1.0/")) {
				add("dc.rights.uri", "http://arxiv.org/licenses/nonexclusive-distrib/1.0/");
				break; // once done then leave
			} else if(rights.containsKey(rs)) {
				add("dc.rights.license", rs.trim().replaceAll("( | )+", " "));
				String ru = rights.get(rs);
				if(StringUtils.isNotBlank(ru)) {
					add("dc.rights.uri", ru.split("\\|")[0]);
					break; // once done then leave
				}
			}
		}
		delete("ndl.sourceMeta.additionalInfo:RightsStatement");
		
		return true;
	}
	
	// correct
	public static void main(String[] args) throws Exception {
		
		String input = "/home/dspace/debasis/NDL/generated_xml_data/CERN/in/CERN-Batch2-02.11.2018.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/generated_xml_data/CERN/logs";
		String outputLocation = "/home/dspace/debasis/NDL/generated_xml_data/CERN/out";
		String name = "CERN.V1";
		
		String langMappingFile = "/home/dspace/debasis/NDL/generated_xml_data/CERN/conf/lang.mapping.csv";
		String lrtMappingFile = "/home/dspace/debasis/NDL/generated_xml_data/CERN/conf/lrt.mapping.csv";
		
		String wrongItemsByLrtFile = "/home/dspace/debasis/NDL/generated_xml_data/CERN/conf/delete.items";
		String wrongLrtsFile = "/home/dspace/debasis/NDL/generated_xml_data/CERN/conf/delete.lrts";
		
		String descLRTMappingFile = "/home/dspace/debasis/NDL/generated_xml_data/CERN/conf/desc.lang.mapping.csv";
		String advRightsMappingFile = "/home/dspace/debasis/NDL/generated_xml_data/CERN/conf/advisor.rights.mapping.csv";
		
		String ddcFile = "/home/dspace/debasis/NDL/generated_xml_data/CERN/conf/ddc.11.map.csv";
		String rightsFile = "/home/dspace/debasis/NDL/generated_xml_data/CERN/conf/rights.uri.map.csv";
		
		CERNV1Correction p = new CERNV1Correction(input, logLocation, outputLocation, name);
		/*p.addCSVLogger("relations", new String[]{"ID", "URL", "Relations"});
		p.addCSVLogger("educations", new String[]{"ID", "URL", "Educations"});
		p.addCSVLogger("editors", new String[]{"ID", "Editors"});
		p.addTextLogger("description");*/
		p.addCSVLogger("dates", new String[]{"ID", "Old", "New"});
		p.addCSVLogger("names", new String[]{"ID", "Old", "New"});
		p.addCSVLogger("names.wrong", new String[]{"ID", "Old", "New"});
		//p.addTextLogger("delete");
		p.turnOffLoadHierarchyFlag(); // no need to load hierarchy
		
		p.wrongItemsByLrt = NDLDataUtils.loadSet(wrongItemsByLrtFile);
		p.wrongLrts = NDLDataUtils.loadSet(wrongLrtsFile);
		p.advRights = NDLDataUtils.loadKeyValue(advRightsMappingFile);
		
		// mapping
		p.ddc = NDLDataUtils.loadKeyValue(ddcFile);
		p.rights = NDLDataUtils.loadKeyValue(rightsFile);
		p.addMappingResource(langMappingFile, "Old", "lang");
		p.addMappingResource(lrtMappingFile, "Old", "lrt");
		p.addMappingResource(descLRTMappingFile, "Desc", "desc_lrt");
		
		p.correctData();
		
		System.out.println("Done.");
	}
}