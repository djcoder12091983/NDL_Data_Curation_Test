package org.iitkgp.ndl.test.source;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.CharUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.iitkgp.ndl.core.NDLDataNode;
import org.iitkgp.ndl.data.AIPDataItem;
import org.iitkgp.ndl.data.correction.NDLAIPCorrectionContainer;
import org.iitkgp.ndl.util.NDLDataUtils;

public class IITBombayAIPCorrection extends NDLAIPCorrectionContainer {
	
	static Pattern PATTERN1 = Pattern.compile("^(.*)\\((.*)\\)$");
	static Pattern PATTERN2 = Pattern.compile("^(.*)\\((.*)\\)([A-Z]?[0-9]+-[A-Z]?[0-9]+)\\.?$");
	static Pattern PATTERN3 = Pattern.compile("^[A-Z]?[0-9]+-[A-Z]?[0-9]+\\.?$");
	static Pattern PATTERN4 = Pattern.compile("^(.* )(.+)\\((.*)\\)$");
	
	// constructor
	public IITBombayAIPCorrection(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	@Override
	protected boolean correctTargetItem(AIPDataItem target) throws Exception {
		
		List<String> relations = getValue("dc.relation");
		for(String r : relations) {
			String tokens[] = r.split(" +");
			for(String t : tokens) {
				if(t.matches("[0-9]{4}")) {
					add("dc.publisher.date", t);
				}
			}
		}
		
		moveIfContains("dc.rights", "dc.rights.holder", "Copyright Wiley-VCH Verlag GmbH & Co. KGaA");
		
		// identifier extraction
		List<NDLDataNode> identifiers = target.getNodes("dc.identifier.other");
		String journal = null, vol = null, issue = null, pagination = "", desc = null;
		String spage = null, epage = null;
		long pcount = 0;
		for(NDLDataNode inode : identifiers) {
			String identifier = inode.getTextContent();
			if(identifier.startsWith("http://hdl.handle.net/")) {
				// delete it only
				inode.remove();
				continue;
			}
			log("extraction.log", target.getId());
			log("extraction.log", identifier);
			if (StringUtils.startsWithIgnoreCase(identifier, "proceedings") || StringUtils.containsIgnoreCase(identifier, "conference")
					|| StringUtils.containsIgnoreCase(identifier, "WORKSHOP")) {
				int p = identifier.lastIndexOf(',');
				String t = identifier.substring(0, p).trim();
				String t1 = identifier.substring(p + 1).trim();
				Matcher m = PATTERN3.matcher(t1);
				if(m.find()) {
					pagination = t1;
				}
				p = t.lastIndexOf(',');
				if(p != -1) {
					// second try
					t1 = t.substring(p + 1).trim();
					if(StringUtils.startsWithIgnoreCase(t1, "pts")) {
						// third try
						t1 = t1.substring(t1.lastIndexOf(',') + 1).trim();
					}
					if(StringUtils.startsWithIgnoreCase(t1, "VOL")) {
						vol = t1.substring(t1.indexOf(' ') + 1);
						desc = t.substring(0, p);
					} else {
						m = PATTERN1.matcher(t1);
						if(m.find()) {
							vol = m.group(1);
							issue = m.group(2);
							desc = t.substring(0, p);
						} else {
							m = PATTERN2.matcher(t1);
							if(m.find()) {
								vol = m.group(1);
								issue = m.group(2);
								pagination = m.group(3);
							} else if(NumberUtils.isDigits(t1)) {
								vol = t1;
							} else {
								desc = t;
							}
						}
					}
				}
			} else if(identifier.matches("[0-9A-Za-z]{4}-[0-9A-Za-z]{4}")) {
				// ISSN
				if(!target.exists("dc.identifier.issn")) {
					log("extraction.log", "ISSN: " + identifier);
					add("dc.identifier.issn", identifier);
				} else {
					log("extraction.log", "EISSN: " + identifier);
					add("dc.identifier.other:eissn", identifier);
				}
			} else if (StringUtils.startsWithIgnoreCase(identifier, "http://dx.doi.org/")
					|| identifier.matches("^[0-9]+\\.[0-9]+/.*$")) {
				// DOI
				String doi = identifier.replace("http://dx.doi.org/", "");
				log("extraction.log", "DOI: " + doi);
				target.add("dc.identifier.other:doi", doi);
			} else if(identifier.matches("[0-9]+(-[0-9A-Za-z]+){2,}") || identifier.matches("[0-9A-Za-z]{10,13}")) {
				// ISBN
				log("extraction.log", "ISBN: " + identifier);
				add("dc.identifier.isbn", identifier);
			} else {
				// ordinary journal volume
				String tokens[] = identifier.split(" *, *");
				for(String token : tokens) {
					Matcher m = PATTERN2.matcher(token);
					if(m.find()) {
						vol = m.group(1);
						issue = m.group(2);
						pagination = m.group(3);
					} else {
						m = PATTERN1.matcher(token);
						if(m.find()) {
							vol = m.group(1);
							issue = m.group(2);
						} else {
							m = PATTERN3.matcher(token);
							if(m.find()) {
								pagination = token;
							} else {
								m = PATTERN4.matcher(token);
								if(m.find()) {
									journal = m.group(1).trim();
									vol = m.group(2);
									issue = m.group(3);
								} else {
									if(NumberUtils.isNumber(token)) {
										// volume
										vol = token;
									} else {
										journal = token;
									}
								}
							}
						}
					}
				}
			}
			inode.remove();
		}
		if(StringUtils.isNotBlank(desc)) {
			target.add("dc.description", "Paper published in: " + desc);
			log("extraction.log", "Description: " + desc);
		}
		/*if(journal != null && journal.matches("[0-9]+-[0-9]+")) {
			System.err.println(target.getId());
		}*/
		if(StringUtils.isNotBlank(vol)) {
			vol = vol.trim();
			int p = vol.lastIndexOf(' ');
			if(p != -1) {
				// volume + journal information 
				String t = vol.substring(p + 1);
				if(CharUtils.isAsciiNumeric(t.charAt(0))) {
					journal = vol.substring(0, p);
					vol = t;
				}
			}
		}
		String pages[] = pagination.replaceAll("\\.|[A-Z]", "").split("-");
		if(pages.length == 2) {
			if(NumberUtils.isDigits(pages[0]) && NumberUtils.isDigits(pages[1])) {
				long ep = Long.parseLong(pages[1]);
				long sp = Long.parseLong(pages[0]);
				if(ep >= sp) {
					// normal case
					spage = pages[0];
					epage = pages[1];
					pcount = ep - sp + 1;
				} else if(StringUtils.isBlank(vol)) {
					//System.err.println(pagination);
					// track error
					// volume + pagination
					int l = pages[0].length();
					int i = l - 1;
					while(i >= 0) {
						String t = pages[0].substring(i);
						sp = Long.parseLong(t);
						if(sp > ep) {
							// try success
							vol = pages[0].substring(0, i + 1);
							spage = pages[0].substring(i + 1);
							sp = Long.parseLong(spage);
							epage = pages[1];
							pagination = spage + '-' + epage;
							pcount = ep - sp + 1;
							break;
						}
						i--; // move backward
					}
					//log("extraction.log", "Wrong with Pagination(" + pagination + "): " + target.getId());
				}
			} else {
				System.err.println(pagination);
			}
		}
		target.add("dc.identifier.other:journal", journal);	
		target.add("dc.identifier.other:volume", vol);
		target.add("dc.identifier.other:issue", issue);
		log("extraction.log", "Journal: " + NDLDataUtils.NVL(journal, "Not available"));
		log("extraction.log", "Volume: " + NDLDataUtils.NVL(vol, "Not available"));
		log("extraction.log", "Issue: " + NDLDataUtils.NVL(issue, "Not available"));
		if(pcount != 0) {
			log("extraction.log", "Pagination: " + pagination);
			target.add("dc.format.extent:startingPage", spage);
			target.add("dc.format.extent:endingPage", epage);
			target.add("dc.format.extent:pageCount", String.valueOf(pcount));
		}
		log("extraction.log", NDLDataUtils.NEW_LINE);
		
		// 1-1 mapping
		transformFieldsById("conf", ';', "<rights,dc.rights.accessRights>", "<visibility,dc.description.searchVisibility>",
				"<ddc,dc.subject.ddc>");
		
		return true;
	}
	
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/generated_xml_data/Nirupam/IIT-Bombay.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/generated_xml_data/Nirupam/logs";
		String outputLocation = "/home/dspace/debasis/NDL/generated_xml_data/Nirupam/out";
		String name = "iitb";
		
		String confFile = "/home/dspace/debasis/NDL/generated_xml_data/Nirupam/conf/iitb/";
		
		IITBombayAIPCorrection p = new IITBombayAIPCorrection(input, logLocation, outputLocation, name);
		p.addTextLogger("extraction.log");
		p.addMappingResource(confFile, "ID", "conf");
		p.correctData();
		
		/*String text = "0-7695-1868-0";
		System.out.println(text.matches("[0-9]+(-[0-9A-Za-z]+){2,}"));*/
		/*String text = "144(4)234-234";
		Matcher m = PATTERN2.matcher(text);
		if(m.find()) {
			System.out.println(m.group(1));
			System.out.println(m.group(2));
			System.out.println(m.group(3));
		}*/
		
		System.out.println("Done.");
	}

}