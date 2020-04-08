package org.iitkgp.ndl.test.source;

import java.util.List;

import org.apache.commons.lang3.CharUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.iitkgp.ndl.data.AIPDataItem;
import org.iitkgp.ndl.data.container.NDLAIPDataContainer;
import org.iitkgp.ndl.util.NDLDataUtils;

public class IITDelhiAIPReader extends NDLAIPDataContainer {
	
	public IITDelhiAIPReader(String input, String logLocation, String name) {
		super(input, logLocation, name);
	}
	
	@Override
	protected boolean readItem(AIPDataItem item) throws Exception {
		List<String> values = item.getValue("dc.identifier.other");
		values.addAll(item.getValue("dc.relation"));
		if(!values.isEmpty()) {
			log("log", item.getId());
			extract(NDLDataUtils.getHandleSuffixID(item.getId()), values);
		}
		return true;
	}
	
	void extract(String id, List<String> values) throws Exception {
		String journal = null, vol = null, issue = null, page = null, accession = null, unique = null, itemid = null;
		String sp = null, ep = null, pc = null, desc = null;
		StringBuilder jtext = new StringBuilder();
		for(String value : values) {
			if(CharUtils.isAsciiNumeric(value.charAt(0))) {
				// if starts with numeric
				itemid = value;
				continue;
			}
			log("log", value);
			if(value.matches("TH-[0-9]{1,4};")) {
				accession = value.replace(";", "");
			} else if(value.startsWith("TH")) {
				unique = value;
			} else {
				// special case 'WA1/5'
				String tokens[] = NDLDataUtils.split(value.replaceAll("[A-Z]{2}[0-9]/[0-9]", ""), "( +|,|\\.|\\(|\\))");
				int l = tokens.length;
				for(int i = 0; i < l;) {
					String t = tokens[i].trim();
					if(NumberUtils.isDigits(t)) {
						// start of volume
						if(i + 1 < l) {
							String next = tokens[i + 1];
							if(NumberUtils.isDigits(next) || next.matches("[0-9]+(-|–)[0-9]+")) {
								vol = t;
								if(i + 2 < l) {
									next = tokens[i + 2];
									if(next.equals("-") || next.equals("–")) {
										page = t + "-" + tokens[i + 3].replaceFirst("p|P", "");
										i += 4;
									} else {
										issue = tokens[i + 1];
										i += 2;
									}
								} else {
									issue = next;
									i += 2;
								}
							} else if(next.equals("-") || next.equals("–")) {
								// pagination
								page = t + "-" + tokens[i + 2].replaceFirst("p|P", "");
								i += 3;
							} else {
								//jtext.append(t).append(' ');
								vol = t;
								i++;
							}
						} else {
							i++;
						}
					} else if(t.matches("[0-9]+(-|–)[0-9]+(p|P)?")) {
						page = t.replaceFirst("p|P", "");
						i++;
					} else {
						jtext.append(t).append(' ');
						i++;
					}
				}
			}
		}
		journal = jtext.toString().trim();
		if(StringUtils.isNotBlank(journal)) {
			if(StringUtils.containsIgnoreCase(journal, "conference")
					|| StringUtils.containsIgnoreCase(journal, "workshop")
					|| StringUtils.containsIgnoreCase(journal, "proceedings")) {
				if(journal.contains("IEEE") && journal.endsWith("on")) {
					int p = journal.indexOf("IEEE");
					journal = journal.substring(p).trim() + " " + journal.substring(0, p).trim();
				}
				desc = "Paper published in " + journal;
				journal = null;
			} else if (journal.contains("IEEE") && journal.endsWith("on")) {
				int p = journal.indexOf("IEEE");
				journal = journal.substring(p).trim() + " " + journal.substring(0, p).trim();
			}
			log("log", "Journal: " + NDLDataUtils.NVL(journal, "NA"));
			log("log", "Desc: " + NDLDataUtils.NVL(desc, "NA"));
		}
		if(StringUtils.isNotBlank(vol)) {
			log("log", "Volume: " + vol);
		}
		if(StringUtils.isNotBlank(issue)) {
			log("log", "Issue: " + issue);
		}
		if(StringUtils.isNotBlank(accession)) {
			log("log", "Accession: " + accession);
		}
		if(StringUtils.isNotBlank(unique)) {
			log("log", "Unique ID: " + unique);
		}
		if(StringUtils.isNotBlank(page)) {
			log("log", "Page: " + page);
			String tokens[] = page.split("-|–");
			sp = tokens[0];
			if(tokens.length == 2) {
				ep = tokens[1];
				pc = String.valueOf(Long.parseLong(ep) - Long.parseLong(sp) + 1);
			}
		}
		log("log", NDLDataUtils.NEW_LINE);
		log("csv_log", new String[] { id, journal, desc, vol, issue, sp, ep, pc, accession,
				unique, itemid });
	}
	
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/generated_xml_data/Nirupam/2018.Oct.30.19.35.24.iit_Delhi.Corrected.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/generated_xml_data/Nirupam/logs";
		String name = "iitd";
	
		IITDelhiAIPReader p = new IITDelhiAIPReader(input, logLocation, name);
		p.addTextLogger("log");
		p.addCSVLogger("csv_log", new String[] { "ID", "Journal", "Desc", "Volume", "Issue", "Start-Page", "End-Page",
				"Page-Count", "AccessionNo", "UniqueID", "Item-ID" });
		p.processData();
		
		System.out.println("Done.");
	}

}