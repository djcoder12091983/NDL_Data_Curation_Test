package org.iitkgp.ndl.test.source;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.iitkgp.ndl.core.NDLDataNode;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;
import org.iitkgp.ndl.data.normalizer.NDLDataNormalizer;
import org.iitkgp.ndl.util.NDLDataUtils;
import org.iitkgp.ndl.util.NDLServiceUtils;

public class DLICurationV2 extends NDLSIPCorrectionContainer {
	
	static Pattern PAGINATION_REGX = Pattern.compile("^([0-9]+) (p|P)(ages)?$");
	static Pattern PUBLISHER_REGX = Pattern.compile("(?i)^(.+)?\\(Digital Publisher: (.+)\\)$");
	static Pattern EDITION_REGX = Pattern.compile("^([0-9]+).*$");
	static Pattern IIT_REGX = Pattern.compile("(?i)^.*(i((\\.?)|( +))?)?i((\\.?)|( +))?i((\\.?)|( +))?t.*$");
	//static Pattern PUB_DATE_PATTERN1 = Pattern.compile("^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$");
	static Pattern PUB_DATE_PATTERN1 = Pattern.compile("^[0-9]{4}-[0-9]{4}$");
	static Pattern PUB_DATE_PATTERN2 = Pattern.compile("(?i)^First, *[0-9]{4}$");
	
	DLINameNormalizer namenormalizer;
	
	Set<String> descdelete1;
	Set<String> codelelet1;
	Set<String> coitemdelelet2;
	Set<String> descdelete2;
	Set<String> descsubjects;
	Set<String> descnotes;
	Set<String> pubdelete1;
	Set<String> pubdatedelete1;
	Set<String> insdelete1;
	Set<String> lcsdelete1;
	Set<String> seriesdelete1;
	Set<String> crdelete1;
	Set<String> pdtemp;
	Map<String, String> pubdatemapping;
	Map<String, String> comapping1;
	Map<String, String> corgmapping2;
	Map<String, String> descmapping2;
	Map<String, String> descpubmapping3;
	
	Set<String> licensedelete;
	Map<String, String> licensemapping;
	Set<String> holderdelete;
	Map<String, String> holdermpaping;
	Map<String, String> taauthormapping;
	
	Map<String, String[]> relationmapping1;
	Set<String> relationdelete1;
	Map<String, String> pubplacemapping1;
	
	Map<String, String[]> authorActions;
	Map<String, String[]> desceditios;
	Map<String, String[]> editormapping;

	public DLICurationV2(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	void loadconf(String deletef, String degreef, String removetokensf, String mappingf) throws Exception {
		namenormalizer = new DLINameNormalizer(deletef, degreef, removetokensf, mappingf);
	}
	
	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		
		delete("dc.subject.classification", "dc.contributor", "dc.description.sponsorship");
		
		removeMultipleLinesAndSpace("dc.title", "dc.contributor.author");
		removeHTMLTags("dc.title", "dc.contributor.author");
		
		// curation
		target.move("dc.contributor.other:authorInfo", "ndl.sourceMeta.additionalInfo:authorInfo");
		target.move("dc.identifier.other:VOLUME", "dc.identifier.other:volume");
		target.move("ndl.sourceMeta.additionalInfo:note", "dc.identifier.other:barcode");
		target.move("ndl.sourceMeta.additionalInfo:RightsStatement", "dc.rights.license");
		
		// description delete1
		List<NDLDataNode> descnodes = target.getNodes("dc.description");
		for(NDLDataNode dnode : descnodes) {
			String desc = dnode.getTextContent();
			// case wise update
			Matcher m = PAGINATION_REGX.matcher(desc);
			Matcher m2 = IIT_REGX.matcher(desc);
			if(m.find()) {
				String pc = target.getSingleValue("dc.format.extent:pageCount");
				if(StringUtils.isBlank(pc)) {
					// if not then move
					target.add("dc.format.extent:pageCount", m.group(1));
				}
				dnode.remove();
			} else if(m2.find()) {
				// thesis LRT
				target.updateSingleValue("lrmi.learningResourceType", "thesis");
			} else if(NumberUtils.isDigits(desc)) {
				// numeric value delete
				dnode.remove();
			} else if(descdelete1.contains(desc)) {
				// 1 to 1 mapping
				dnode.remove();
			} else if(descsubjects.contains(desc)) {
				// move to subject
				target.addIfNotContains("dc.subject", desc.replaceAll("(^\")|(\"$)", ""));
				dnode.remove();
				
				//System.out.println("KW: " + target.getValue("dc.subject"));
			} else if(descnotes.contains(desc) || StringUtils.startsWithIgnoreCase(desc, "First Published In")) {
				// note
				dnode.setTextContent("Note: " + desc);
			} else if(StringUtils.containsIgnoreCase(desc, "Ed.") || StringUtils.containsIgnoreCase(desc, "Edn")
					|| StringUtils.containsIgnoreCase(desc, "edition")) {
				// EDITION
				String tokens[] = desc.split(" +");
				int l = tokens.length;
				for(int i = 0 ; i< l; i++) {
					String t = tokens[i];
					if(StringUtils.containsIgnoreCase(t, "Ed.") || StringUtils.containsIgnoreCase(t, "Edn")
							|| StringUtils.containsIgnoreCase(t, "edition")) {
						boolean f = false;
						if(i - 1 >= 0) {
							Matcher m1 = EDITION_REGX.matcher(tokens[i - 1]);
							if(m1.find()) {
								target.add("dc.identifier.other:edition", m1.group(1));
								f = true;
							}
						} else if(i + 1 < l) {
							Matcher m1 = EDITION_REGX.matcher(tokens[i + 1]);
							if(m1.find()) {
								target.add("dc.identifier.other:edition", m1.group(1));
								f = true;
							}
						}
						if (!f && (StringUtils.containsIgnoreCase(t, " Ed.")
								|| StringUtils.containsIgnoreCase(t, " Ed ")
								|| StringUtils.containsIgnoreCase(t, " Ed("))) {
							// System.out.println(target.getId() + " => Editor: " + desc);
							// try to extract editor
							Stack<String> ed = new Stack<>(); // handle reverse case
							for(int j = i - 1; j >= 0; j--) {
								String t1 = tokens[j];
								if(t1.contains(":")) {
									break;
								}
								ed.add(t1.replaceFirst("\\.,$", "."));
							}
							if(!ed.isEmpty()) {
								StringBuilder editor = new StringBuilder();
								while(!ed.isEmpty()) {
									editor.append(ed.pop()).append(" ");
								}
								//System.err.println(target.getId() + " => Editor: " + editor); // test
								target.addIfNotContains("dc.contributor.editor", editor.toString());
							}
						}
						break;
					}
				}
				dnode.remove(); // delete after done
			} else if(StringUtils.containsIgnoreCase(desc, " Edited By")) {
				// editor
				String editor = desc.substring(desc.toLowerCase().lastIndexOf("edited by") + 9).trim();
				//System.err.println(target.getId() + " => Editor: " + editor); // test
				target.addIfNotContains("dc.contributor.editor", editor);
				
				dnode.remove(); // delete after done
			}
		}
		
		// publisher institute
		deleteIfContains("dc.publisher.institution", insdelete1);
		
		// title modification (AC - numeric remove)
		target.replaceByRegex("dc.title", "(?i) +?AC [0-9]+$", "");
		
		// volume issue from institution
		transformFieldsById("pub.ins.mapping", "<Volume,dc.identifier.other:volume>",
				"<Issue,dc.identifier.other:issue>", "<Part,dc.identifier.other:part>", "<Desc,dc.description>");
		transformFieldsById("desc.mapping", "<Volume,dc.identifier.other:volume>", "<Issue,dc.identifier.other:issue>",
				"<Edition,dc.identifier.other:edition>", "<Part,dc.identifier.other:part>");
		
		// publisher date
		List<NDLDataNode> pds = target.getNodes("dc.publisher.date");
		for(NDLDataNode pd : pds) {
			String pdate = pd.getTextContent();
			if(pubdatedelete1.contains(pdate)) {
				// pub date remove
				pd.remove();
				continue;
			}
			if(pubdatemapping.containsKey(pdate)) {
				// 1 1 mapping
				pd.setTextContent(pubdatemapping.get(pdate));
				continue;
			}
			Matcher m1 = PUB_DATE_PATTERN1.matcher(pdate);
			Matcher m2 = PUB_DATE_PATTERN2.matcher(pdate);
			if(m1.matches() || pdtemp.contains(pdate)) {
				// case 1
				target.add("dc.coverage.temporal", pdate);
				pd.remove();
			} else if(m2.matches()) {
				// first, year
				pd.setTextContent(pdate.split(" *, *")[0] + "-01-01");
			} else { 
				// normalize
				String nd = NDLServiceUtils.normalilzeDate(pdate);
				if(StringUtils.isNotBlank(nd)) {
					// update with normalized value
					pd.setTextContent(nd);
				}
			}
		}
		
		// dc.date.other@lawCaseYear
		List<NDLDataNode> lcyears = target.getNodes("dc.date.other:lawCaseYear");
		for(NDLDataNode lcyear : lcyears) {
			String lcy = NDLDataUtils.getValueByJsonKey(lcyear.getTextContent(), "lawCaseYear");
			if(lcsdelete1.contains(lcy) || lcy.startsWith("0000")) {
				lcyear.remove();
			} else {
				lcy = lcy.replaceAll("00", "01");
				String pd = target.getSingleValue("dc.publisher.date");
				if(!StringUtils.equals(pd, lcy)) {
					// if not match with publisher date
					target.addIfNotContains("dc.date.other:copyrightExpiryDate", lcy);
				}
				lcyear.remove();
			}
		}
		
		// series
		List<NDLDataNode> snodes = target.getNodes("dc.relation.ispartofseries");
		for(NDLDataNode snode : snodes) {
			String series = snode.getTextContent();
			if(seriesdelete1.contains(series)) {
				snode.remove();
			} else {
				series = series.replaceAll("00", "01");
				String pd = target.getSingleValue("dc.publisher.date");
				if(!StringUtils.equals(pd, series)) {
					// if not match with publisher date
					target.addIfNotContains("dc.date.other:digitalPublicationDate",
							NDLServiceUtils.normalilzeDate(series));
				}
				snode.remove();
			}
		}
		
		// publisher
		List<NDLDataNode> publishers = target.getNodes("dc.publisher");
		for(NDLDataNode publisher : publishers) {
			String pub = publisher.getTextContent();
			Matcher m = PUBLISHER_REGX.matcher(pub);
			if(StringUtils.containsIgnoreCase(pub, "(Digital Publisher: Digital Library of India)")) {
				publisher.setTextContent(pub.replaceFirst("(?i)\\(Digital Publisher: Digital Library of India\\)", "").trim());
			} else if (m.find()){
				// move to desc
				String t1 = m.group(2);
				if(StringUtils.isNotBlank(t1)) {
					target.add("dc.description", "The Custodian Institution for this doument is : " + t1);
				}
				
				// publisher update
				String t2 = m.group(1);
				if(StringUtils.isNotBlank(t2)) {
					publisher.setTextContent(t2.trim());
				} else {
					publisher.remove();
				}
			}
			
			pub = publisher.getTextContent();
			m = IIT_REGX.matcher(pub);
			if(m.find()) {
				target.updateSingleValue("lrmi.learningResourceType", "thesis");
			} else if(pubdelete1.contains(pub)) {
				// exact match delete case
				publisher.remove();
			}
		}
		
		// copy-right
		List<NDLDataNode> crights = target.getNodes("dc.date.copyright");
		for(NDLDataNode cright : crights) {
			String cr = cright.getTextContent();
			if(StringUtils.equalsIgnoreCase(cr, "Copyright Permitted")) {
				target.addIfNotContains("dc.rights.license", cr);
				cright.remove();
			}
		}
		
		// copyright date delete
		deleteIfContains("dc.date.copyright", crdelete1);
		
		// LRT
		String t = target.getSingleValue("dc.title");
		if(StringUtils.containsIgnoreCase(t, "journal")
				|| StringUtils.containsIgnoreCase(t, "National Academy Science Letters")) {
			target.updateSingleValue("lrmi.learningResourceType", "journal");
		}
		
		// title split
		if(t.contains("|") && target.contains("dc.language.iso", "hin")) {
			String split[] = t.split("\\|");
			target.updateSingleValue("dc.title", split[0]);
			target.updateSingleValue("dc.title.alternative", split[1]);
		}
		
		moveIfContains("dc.title.alternative", "dc.contributor.author", "V V Bran");
		if(target.getId().equals("dc.contributor.author")) {
			target.delete("dc.title.alternative");
			target.addIfNotContains("dc.identifier.other:volume", "1");
		}
		
		// ta
		List<NDLDataNode> talist = target.getNodes("dc.title.alternative");
		for(NDLDataNode tanode : talist) {
			String ta = tanode.getTextContent();
			if(taauthormapping.containsKey(ta)) {
				target.addIfNotContains("dc.contributor.author", taauthormapping.get(ta));
				tanode.remove();
			}
		}
		
		// name normalization
		normalize("dc.contributor.author");
		
		// dc.relation
		List<NDLDataNode> relations = target.getNodes("dc.relation");
		for(NDLDataNode relation : relations) {
			String rel = relation.getTextContent();
			if(relationdelete1.contains(rel)) {
				relation.remove();
			} else if(relationmapping1.containsKey(rel)) {
				// shift
				String mapping[] = relationmapping1.get(rel);
				String vol = mapping[0];
				String part = mapping[1];
				String srs = mapping[2];
				String inst = mapping[3];
				String place = mapping[4];
				
				if(!target.exists("dc.publisher.institution")) {
					target.add("dc.publisher.institution", inst);
				}
				if(!target.exists("dc.publisher.place")) {
					target.add("dc.publisher.place", place);
				}
				if(!target.exists("dc.identifier.other:volume")) {
					target.add("dc.identifier.other:volume", vol);
				}
				if(!target.exists("dc.identifier.other:part")) {
					target.add("dc.identifier.other:part", part);
				}
				if(!target.exists("dc.relation.ispartofseries")) {
					target.add("dc.relation.ispartofseries", srs);
				}
				
				relation.remove();
			}
		}
		
		// publisher.place
		transformFieldByExactMatch("dc.publisher.place", pubplacemapping1);
		
		// description 2
		descnodes = target.getNodes("dc.description");
		for(NDLDataNode descnode : descnodes) {
			String desc = descnode.getTextContent();
			//System.out.println("Desc: " + desc);
			String tokens[] = desc.split(":");
			String last;
			if(tokens.length > 1) {
				last = tokens[tokens.length - 1];
			} else {
				last = tokens[0];
			}
			
			if(last.matches("(?i)^.*(((in)?((trans)|(tran))\\.?)|(translator))$")) {
				
				target.addIfNotContains("dc.contributor.other:translator",
						last.replaceAll("(?i)(, *)?(((in)?((trans)|(tran))\\.?)|(translator))$", "").trim());
				
				//System.out.println("1. " + target.getValue("dc.contributor.other:translator"));
				
				descnode.remove();
			} else if(last.matches("(?i)^.*(Comp\\.?)$")) {
				target.addIfNotContains("dc.contributor.other:compiler",
						last.replaceAll("(?i)(, *)?(Comp\\.?)$", "").trim());
				
				//System.out.println("2. " + target.getValue("dc.contributor.other:compiler"));
				
				descnode.remove();
			} else if(last.matches("(?i)^.*(((ill)|(ills)|(illus))\\.?)$")) {
				target.addIfNotContains("dc.contributor.illustrator",
						last.replaceAll("(?i)(, *)?(((ill)|(ills)|(illus))\\.?)$", "").trim());
				
				//System.out.println("3. " + target.getValue("dc.contributor.illustrator"));
				
				descnode.remove();
			} else if(target.contains("dc.publisher", last.trim())) {
				descnode.remove();
			} else if(descdelete2.contains(desc)) {
				descnode.remove(); // straight way delete
			} else {
				// TODO match partially with author, desc delete
			}
		}
		
		List<NDLDataNode> cothers = target.getNodes("dc.contributor.other");
		for(NDLDataNode cother : cothers) {
			String value = cother.getTextContent();
			String kv[] = NDLDataUtils.getJSONKeyedValue(value);
			if(NDLDataUtils.invalidJSON(kv)) {
				// remove malformed JSON
				System.err.println("Removing JSON ============== " + value);
				cother.remove();
				continue;
			}
			// special case
			if(kv[0].equals("edition") && NumberUtils.isDigits(kv[1])) {
				cother.setTextContent(NDLDataUtils.getJson("edition", Integer.valueOf(kv[1])));
				continue;
			}
			if(kv[0].equals("volume")) {
				cother.remove();
				target.addIfNotContains("dc.identifier.other:volume", kv[1]);
				continue;
			}
			
			if(codelelet1.contains(value)) {
				cother.remove();
			} else if(comapping1.containsKey(value)) {
				kv = NDLDataUtils.getJSONKeyedValue(value);
				cother.setTextContent(NDLDataUtils.getJson(kv[0], comapping1.get(value)));
			}else {
				String org = NDLDataUtils.getValueByJsonKey(value, "organization");
				boolean d = false;
				if(StringUtils.startsWithIgnoreCase(org, "U.s. Dept.of")
						|| StringUtils.startsWithIgnoreCase(org, "U.s. Department")) {
					target.addIfNotContains("dc.publisher.department", value);
					d = true;
				} else if(corgmapping2.containsKey(org)) {
					target.addIfNotContains("dc.publisher.department", corgmapping2.get(org));
					d = true;
				}
				String itemid = NDLDataUtils.getValueByJsonKey(value, "itemId");
				if(coitemdelelet2.contains(itemid)) {
					d = true;
				}
				
				if(d) {
					cother.remove();
				}
			}
		}
		
		// more on description
		descnodes = target.getNodes("dc.description");
		for(NDLDataNode dnode : descnodes) {
			String value = dnode.getTextContent();
			if(descmapping2.containsKey(value)) {
				dnode.setTextContent(descmapping2.get(value));
			} else if(descpubmapping3.containsKey(value)) {
				dnode.remove();
				target.updateSingleValue("dc.publisher",
						NDLDataUtils.NVL(target.getSingleValue("dc.publisher"), "") + " " + value);
			} else {
				List<String> authors = target.getValue("dc.contributor.author");
				List<String> pubvalues = target.getValue("dc.publisher");
				boolean f = false;
				for(String author : authors) {
					if(StringUtils.containsIgnoreCase(value, author)) {
						f=  true;
						break;
					}
				}
				if(!f) {
					for(String pub : pubvalues) {
						if(StringUtils.containsIgnoreCase(value, pub)) {
							f=  true;
							break;
						}
					}
				}
				
				if(f) {
					System.err.println("1. Description matches with (publisher/author): " + target.getId());
					dnode.remove();
				}
			}
		}
		
		List<NDLDataNode> orgnodes = target.getNodes("dc.contributor.other:organization");
		for(NDLDataNode orgnode : orgnodes) {
			List<String> pubvalues = target.getValue("dc.publisher");
			boolean f = false;
			for(String pub : pubvalues) {
				if(StringUtils.containsIgnoreCase(orgnode.getTextContent(), pub)) {
					f=  true;
					break;
				}
			}
			
			if(f) {
				System.err.println("2. Organization matches with publisher: " + target.getId());
				orgnode.remove();
			}
		}
		
		// license and holder
		deleteIfContains("dc.rights.license", licensedelete);
		deleteIfContains("dc.rights.holder", holderdelete);
		transformFieldByExactMatch("dc.rights.license", licensemapping);
		transformFieldByExactMatch("dc.rights.holder", holdermpaping);
		
		//target.deleteDuplicateFieldValues("dc.publisher", "dc.contributor.other:organization");
		deleteIfContains("dc.title.alternative", true, "xxx", "़", "150");
		
		// author
		List<NDLDataNode> authorNodes = target.getNodes("dc.contributor.author");
		for(NDLDataNode authorNode : authorNodes) {
			String value = authorNode.getTextContent();
			if(authorActions.containsKey(value)) {
				String[] values = authorActions.get(value);
				boolean d = values[0].equalsIgnoreCase("DELETE");
				if(!d && StringUtils.isNotBlank(values[0])) {
					authorNode.setTextContent(values[0]);
				}
				if(StringUtils.isNotBlank(values[1])) {
					target.addIfNotContains("dc.contributor.editor", values[1]);
					d = true;
				}
				if(StringUtils.isNotBlank(values[2])) {
					target.addIfNotContains("dc.subject", values[2]);
					d = true;
				}
				if(StringUtils.isNotBlank(values[3])) {
					target.addIfNotContains("dc.publisher", values[3]);
					d = true;
				}
				if(StringUtils.isNotBlank(values[4])) {
					target.addIfNotContains("dc.description", values[4]);
					d = true;
				}
				if(StringUtils.isNotBlank(values[5])) {
					target.addIfNotContains("dc.contributor.other:organization", values[5]);
					d = true;
				}
				if(StringUtils.isNotBlank(values[6])) {
					target.addIfNotContains("dc.contributor.other:compiler", values[6]);
					d = true;
				}
				
				if(d) {
					authorNode.remove();
				}
			}
		}
		
		// desc editor
		List<NDLDataNode> descNodes = target.getNodes("dc.description");
		for(NDLDataNode descnode : descNodes) {
			String value = descnode.getTextContent();
			if(desceditios.containsKey(value)) {
				String[] values = desceditios.get(value);
				target.addIfNotContains("dc.contributor.editor", values[0]);
				
				descnode.remove();
			}
		}
		
		// editor
		List<NDLDataNode> editorNodes = target.getNodes("dc.contributor.editor");
		for(NDLDataNode editornode : editorNodes) {
			String value = editornode.getTextContent();
			if(editormapping.containsKey(value)) {
				String[] values = editormapping.get(value);
				boolean d = values[0].equalsIgnoreCase("DELETE");
				if(!d && StringUtils.isNotBlank(values[0])) {
					editornode.setTextContent(values[0]);
				}
				
				if(d) {
					editornode.remove();
				}
			}
		}
		
		//System.err.println("1. " + target.getValue("dc.description"));
		target.replaceByRegex("dc.description", "^The Custodian Institution for this doument is *:", "Custodian Institution:");
		//System.err.println("2. " + target.getValue("dc.description"));
		
		// publisher/author/description match
		descNodes = target.getNodes("dc.description");
		List<String> authors = target.getValue("dc.contributor.author");
		List<String> pubvalues = target.getValue("dc.publisher");
		boolean f1 = false, f2 = false;
		for(NDLDataNode descnode : descNodes) {
			String value = descnode.getTextContent();		
			if(potentiallymatch(authors, value)) {
				// author matches with desc
				descnode.remove();
			} else if(potentiallymatch(pubvalues, value)) {
				// publisher matches with desc
				descnode.remove();
			} else if(StringUtils.equalsIgnoreCase(value, "Custodian Institution: Rashtrapati Bhavan Library")) {
				f1 = true;
			} else if(StringUtils.equalsIgnoreCase(value, "Custodian Institution: Rashtrapati Bhavan.")) {
				f2 = true;
			} else if(value.contains("Translator ")) {
				target.addIfNotContains("dc.contributor.other:translator", value.replaceFirst("^.*Translator ", "").trim());
				descnode.remove();
			} else if(value.contains("Translated By ")) {
				target.addIfNotContains("dc.contributor.other:translator", value.replaceFirst("^.*Translated By ", "").trim());
				descnode.remove();
			} else if(value.contains("Illustrated By ")) {
				target.addIfNotContains("dc.contributor.illustrator", value.replaceFirst("^.*Illustrated By ", "").trim());
				descnode.remove();
			} else if(value.matches("^Custodian Institution:.* By .*$")) {
				if(value.matches("^Custodian Institution: Printed .*$")) {
					// move to publisher
					int s = target.getValue("dc.publisher").size();
					if(s == 0) {
						target.add("dc.publisher", value.replace("Custodian Institution:", "").trim());
					} else if(s == 1) {
						target.updateSingleValue("dc.publisher", target.getSingleValue("dc.publisher") + " "
								+ value.replace("Custodian Institution:", "").trim());
					}
					// System.err.println(target.getValue("dc.publisher"));
					descnode.remove();
				} else {
					descnode.setTextContent(value.replace("Custodian Institution:", "Note:"));
				}
			} else if(value.matches("^Custodian Institution: Printed .*$")) {
				// move to publisher
				int s = target.getValue("dc.publisher").size();
				if(s == 0) { 
					target.add("dc.publisher", value.replace("Custodian Institution:", "").trim());
				} else if(s == 1) {
					target.updateSingleValue("dc.publisher", target.getSingleValue("dc.publisher") + " "
							+ value.replace("Custodian Institution:", "").trim());
				}
				// System.err.println(target.getValue("dc.publisher"));
				descnode.remove();
			}
		}
		
		if(f1 && f2) {
			deleteIfContains("dc.description", "Custodian Institution: Rashtrapati Bhavan.");
		}
		
		move("dc.publisher.department:organization", "dc.contributor.other:organization");
		
		List<NDLDataNode> pinodes = target.getNodes("dc.publisher.institution");
		for(NDLDataNode pinode : pinodes) {
			String value = pinode.getTextContent();
			if(StringUtils.startsWithIgnoreCase(value, "Note")) {
				// move to description
				target.addIfNotContains("dc.description", value);
				pinode.remove();
			}
		}
		
		publishers = target.getNodes("dc.publisher");
		for(NDLDataNode pubnode : publishers) {
			String value = pubnode.getTextContent();
			if(StringUtils.containsIgnoreCase(value, "Indian Institute Of Technology")) {
				// remove and update
				target.delete("lrmi.learningResourceType");
				target.add("lrmi.learningResourceType", "thesis");
				break;
			}
		}
		
		// description merge TODO
		
		return true;
	}
	
	// potential match
	static boolean potentiallymatch(List<String> values, String value) {
		String tokens1[] = value.split("( +)|,|\\.");
		for(String v : values) {
			String tokens2[] = v.split("( +)|,|\\.");
			boolean f = false;
			for(String t2 : tokens2) {
				if(StringUtils.isBlank(t2)) {
					continue;
				}
				boolean f1 = false;
				for(String t1 : tokens1) {
					//System.out.println(t1 + " => " + t2);
					if(StringUtils.equalsIgnoreCase(t1, t2)) {
						f1 = true;
						break;
					}
				}
				f = f1;
				if(!f) {
					break;
				}
			}
			if(f) {
				// @ least one match
				return true;
			}
		}
		return false;
	}
	
	public static void main2(String[] args) {
		/*String txt = "123 Xages";
		Matcher m = PAGINATION_REGX.matcher(txt);
		if(m.find()) {
			System.out.println(m.group(1));
		}*/
		
		//System.out.println("\"Day's \"Course\"".replaceAll("(^\")|(\"$)", ""));
		//System.out.println("Avadh Jain Directory (1981) aC 5681".replaceFirst("(?i) ?AC [0-9]+$", ""));
		/*String text = "(Digital Publisher: JAI SINGH)";
		Matcher m = PUBLISHER_REGX.matcher(text);
		if(m.find()) {
			System.out.println(m.group(1));
			System.out.println(m.group(2));
		}*/
		
		//System.out.println("Avadh Jain Directory (1981) Ac 5681".replaceAll("(?i) +?AC [0-9]+$", ""));
		
		/*String text = "debasis I.X.Tjana";
		Matcher m = IIT_REGX.matcher(text);
		if(m.find()) {
			System.out.println("found");
		}*/
		
		System.out.println(potentiallymatch(Arrays.asList(new String[] { "debasis, jana paul", "p, k gupta" }),
				"my name is debasis jana gupta p ka"));
	}
	
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/NDL_sources/DLI/in/2019.Nov.19.12.58.30.DLI.filter.v1.Corrected.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/NDL_sources/DLI/out"; // log location if any
		String outputLocation = "/home/dspace/debasis/NDL/NDL_sources/DLI/out";
		String name = "DLI.filter.v2";
		
		String descdeletef1 = "/home/dspace/debasis/NDL/NDL_sources/DLI/conf/description.delete1";
		String codeletef1 = "/home/dspace/debasis/NDL/NDL_sources/DLI/conf/co.delete1";
		String coitemdeletef2 = "/home/dspace/debasis/NDL/NDL_sources/DLI/conf/item.id.delete1";
		String comappingf1 = "/home/dspace/debasis/NDL/NDL_sources/DLI/conf/co.mapping1.csv";
		String corgmappingf2 = "/home/dspace/debasis/NDL/NDL_sources/DLI/conf/co.org.mapping2.csv";
		String descdeletef2 = "/home/dspace/debasis/NDL/NDL_sources/DLI/conf/description.delete2";
		String descsubjectmovef = "/home/dspace/debasis/NDL/NDL_sources/DLI/conf/description.subject.move";
		String descnotesf = "/home/dspace/debasis/NDL/NDL_sources/DLI/conf/description.notes";
		String pubdeletef1 = "/home/dspace/debasis/NDL/NDL_sources/DLI/conf/publisher.delete1";
		String pubdatedeletef1 = "/home/dspace/debasis/NDL/NDL_sources/DLI/conf/pub.date.delete";
		String pubdatemappingf = "/home/dspace/debasis/NDL/NDL_sources/DLI/conf/pub.date.mapping.csv";
		String insdeletef1 = "/home/dspace/debasis/NDL/NDL_sources/DLI/conf/publisher.ins..delete1";
		String lcdeletef1 = "/home/dspace/debasis/NDL/NDL_sources/DLI/conf/lawcase.delete1";
		String seriesdeletef1 = "/home/dspace/debasis/NDL/NDL_sources/DLI/conf/seriesdelete1";
		String crdeletef1 = "/home/dspace/debasis/NDL/NDL_sources/DLI/conf/copyright.delete1";
		String insMappingf = "/home/dspace/debasis/NDL/NDL_sources/DLI/conf/pub.ins.vi.mapping.csv";
		String descMappingf = "/home/dspace/debasis/NDL/NDL_sources/DLI/conf/desc.mapping.csv";
		String pdtempf = "/home/dspace/debasis/NDL/NDL_sources/DLI/conf/pub.temporal";
		String relationmappingf = "/home/dspace/debasis/NDL/NDL_sources/DLI/conf/relation.mapping1.csv";
		String relationdeletef1 = "/home/dspace/debasis/NDL/NDL_sources/DLI/conf/relation.delete1";
		String pubplacemappingf1 = "/home/dspace/debasis/NDL/NDL_sources/DLI/conf/pubplace.mapping1.csv";
		String descmappingf2 = "/home/dspace/debasis/NDL/NDL_sources/DLI/conf/desc.mapping2.csv";
		String descpubmappingf3 = "/home/dspace/debasis/NDL/NDL_sources/DLI/conf/desc.mapping2.csv";
		
		String licensedeletef = "/home/dspace/debasis/NDL/NDL_sources/DLI/conf/license.delete1";
		String licensemappingf = "/home/dspace/debasis/NDL/NDL_sources/DLI/conf/license.mapping1.csv";
		String holderdeletef = "/home/dspace/debasis/NDL/NDL_sources/DLI/conf/holder.delete1";
		String holdermappingf = "/home/dspace/debasis/NDL/NDL_sources/DLI/conf/holder.mapping1.csv";
		String taauthormappingf = "/home/dspace/debasis/NDL/NDL_sources/DLI/conf/ta.author.mapping.csv";
		
		DLICurationV2 p = new DLICurationV2(input, logLocation, outputLocation, name);
		p.turnOnManualMultipleSpacesAndLinesRemoval();
		p.turnOnDetailValidationLogging();
		p.descdelete1 = NDLDataUtils.loadSet(descdeletef1);
		p.codelelet1 = NDLDataUtils.loadSet(codeletef1);
		p.coitemdelelet2 = NDLDataUtils.loadSet(coitemdeletef2);
		p.descdelete2 = NDLDataUtils.loadSet(descdeletef2);
		p.descsubjects = NDLDataUtils.loadSet(descsubjectmovef);
		p.descnotes = NDLDataUtils.loadSet(descnotesf);
		p.pubdelete1 = NDLDataUtils.loadSet(pubdeletef1);
		p.pubdatedelete1 = NDLDataUtils.loadSet(pubdatedeletef1);
		p.insdelete1 = NDLDataUtils.loadSet(insdeletef1);
		p.lcsdelete1 = NDLDataUtils.loadSet(lcdeletef1);
		p.seriesdelete1 = NDLDataUtils.loadSet(seriesdeletef1);
		p.crdelete1 = NDLDataUtils.loadSet(crdeletef1);
		p.pdtemp = NDLDataUtils.loadSet(pdtempf);
		p.pubdatemapping = NDLDataUtils.loadKeyValue(pubdatemappingf);
		p.relationmapping1 = NDLDataUtils.loadMap(relationmappingf);
		p.relationdelete1 = NDLDataUtils.loadSet(relationdeletef1);
		p.pubplacemapping1 = NDLDataUtils.loadKeyValue(pubplacemappingf1);
		p.comapping1 = NDLDataUtils.loadKeyValue(comappingf1);
		p.corgmapping2 = NDLDataUtils.loadKeyValue(corgmappingf2);
		p.descmapping2 = NDLDataUtils.loadKeyValue(descmappingf2);
		p.descpubmapping3 = NDLDataUtils.loadKeyValue(descpubmappingf3);
		p.taauthormapping = NDLDataUtils.loadKeyValue(taauthormappingf);
		
		p.licensedelete = NDLDataUtils.loadSet(licensedeletef);
		p.holderdelete = NDLDataUtils.loadSet(holderdeletef);
		p.licensemapping = NDLDataUtils.loadKeyValue(licensemappingf);
		p.holdermpaping = NDLDataUtils.loadKeyValue(holdermappingf);
		
		p.addMappingResource(insMappingf, "ID", "pub.ins.mapping");
		p.addMappingResource(descMappingf, "ID", "desc.mapping");
				
		String deletef = "/home/dspace/debasis/NDL/NDL_sources/DLI/conf/name.delete";
		String mappingf = "/home/dspace/debasis/NDL/NDL_sources/DLI/conf/name.mapping.csv";
		String degreef = "/home/dspace/debasis/NDL/NDL_sources/DLI/conf/name.degree";
		String removetokensf = "/home/dspace/debasis/NDL/NDL_sources/DLI/conf/name.remove.tokens";
		
		String authoractionf = "/home/dspace/debasis/NDL/NDL_sources/DLI/conf/author.mapping1.csv";
		String desceditorf = "/home/dspace/debasis/NDL/NDL_sources/DLI/conf/desc.editors.csv";
		String editormappingf = "/home/dspace/debasis/NDL/NDL_sources/DLI/conf/editor.mapping1.csv";
		
		p.authorActions = NDLDataUtils.loadMap(authoractionf);
		p.desceditios = NDLDataUtils.loadMap(desceditorf);
		p.editormapping = NDLDataUtils.loadMap(editormappingf);
		
		p.loadconf(deletef, degreef, removetokensf, mappingf);
		
		p.addNormalizer("dc.contributor.author", new NDLDataNormalizer() {
			
			@Override
			public Collection<String> transform(String input) {
				try {
					return p.namenormalizer.correctName(input);
				} catch(Exception ex) {
					// error
					throw new IllegalStateException(ex.getMessage(), ex);
				}
			}
		});
		
		p.correctData();
		
		System.out.println("Done.");
	}

}