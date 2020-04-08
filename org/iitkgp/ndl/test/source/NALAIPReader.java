package org.iitkgp.ndl.test.source;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.iitkgp.ndl.data.AIPDataItem;
import org.iitkgp.ndl.data.container.NDLAIPDataContainer;
import org.iitkgp.ndl.util.NDLDataUtils;

public class NALAIPReader extends NDLAIPDataContainer {
	
	Map<String, String> lrt = new HashMap<String, String>();

	public NALAIPReader(String input, String logLocation, String name) {
		super(input, logLocation, name);
		lrt.put("Technical Report", "technicalReport");
		lrt.put("Conference", "conferenceProceedings");
		lrt.put("Project Report", "projectReport");
		lrt.put("PhD thesis", "thesis");
		lrt.put("[Lectures/Presentation]", "presentation");
		lrt.put("Proceeding", "proceeding");
	}
	
	@Override
	protected boolean readItem(AIPDataItem item) throws Exception {
		String other = item.getSingleValue("dc.identifier.other");
		if(StringUtils.isNotBlank(other)) {
			//System.out.println(other);
			String journal = null, vol = null, issue = null, page = null, issn = null, place = null;
			String sp = null, ep = null, pc = null;
			String id = item.getId();
			log("log", id);
			log("log", other);
			List<String> flrt = new LinkedList<String>();
			for(String k : lrt.keySet()) {
				if(StringUtils.containsIgnoreCase(other, k)) {
					flrt.add(lrt.get(k));
				}
			}
			if(flrt.isEmpty()) {
				String title = NDLDataUtils
						.removeMultipleSpaces(NDLDataUtils.removeNewLines(item.getSingleValue("dc.title")));
				other = other.substring(StringUtils.indexOfIgnoreCase(other, title) + title.length());
				StringBuilder jtext = new StringBuilder();
				String tokens[] = NDLDataUtils.split(other, "( +|,|\\.|\\(|\\)|:)");
				int l = tokens.length;
				for(int i = 0; i < l;) {
					String t = tokens[i].trim();
					if(StringUtils.equalsIgnoreCase(t, "issn")) {
						if(tokens[i + 1].equalsIgnoreCase("issn")) {
							issn = tokens[i + 2];
							i += 3;
						} else {
							issn = tokens[i + 1];
							i += 2;
						}
					} else if(t.matches("(p|P)+")) {
						page = tokens[i + 1];
						i += 2;
					} else if(NumberUtils.isDigits(t)) {
						if(t.length() < 4) {
							vol = t;
						}
						if(i + 1 < l) {
							String next = tokens[i + 1];
							if(NumberUtils.isDigits(next)) {
								vol = t;
								i += 2;
							} else {
								i++;
							}
						} else {
							i++;
						}
					} else {
						if(!StringUtils.containsIgnoreCase(t, "amp;")) {
							jtext.append(t).append(' ');
						}
						i++;
					}
				}
				journal = jtext.toString().trim();
				if(StringUtils.isNotBlank(journal)) {
					if(StringUtils.startsWithIgnoreCase(journal, "in")) {
						// invalid
						journal = null;
					}
					log("log", "Journal: " + NDLDataUtils.NVL(journal, "NA"));
				}
				if(StringUtils.isNotBlank(vol)) {
					log("log", "Volume: " + vol);
				}
				if(StringUtils.isNotBlank(issue)) {
					log("log", "Issue: " + issue);
				}
				if(StringUtils.isNotBlank(issn)) {
					log("log", "ISSN: " + issn);
				}
				if(StringUtils.isNotBlank(page)) {
					log("log", "Page: " + page);
					tokens = page.split("-|–");
					sp = tokens[0];
					if(tokens.length > 1) {
						ep = tokens[1];
						long p1 = Long.parseLong(sp);
						long p2 = Long.parseLong(ep);
						/*if(p2 < p1) {
						 * TODO
							long t1 = p1;
							p1 = p2;
							p2 = t1;
						}*/
						pc = String.valueOf(p2 - p1 + 1);
					}
				}
			} else {
				// lrt test
				log("log", "LRT: " + flrt.toString());
				// try to find out place
				String tokens[] = NDLDataUtils.split(other, "( +|,|\\.|\\(|\\)|:)");
				int l = tokens.length;
				String last = tokens[l - 1];
				if(StringUtils.equalsIgnoreCase(last, "india") || StringUtils.equalsIgnoreCase(last, "china")) {
					if(StringUtils.isAlpha(tokens[l - 2])) {
						place = tokens[l - 2];
					}
				}
			}
			log("log", NDLDataUtils.NEW_LINE);
			log("log_csv", new String[] { NDLDataUtils.getHandleSuffixID(id), journal, vol, issue, sp, ep, pc, issn,
					NDLDataUtils.join(flrt, '|'), place });
		}
		return true;
	}
	
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/generated_xml_data/Nirupam/2018.Dec.05.19.01.49.CSIR-NAL.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/generated_xml_data/Nirupam/logs";
		String name = "nal";
		
		NALAIPReader p = new NALAIPReader(input, logLocation, name);
		p.addTextLogger("log");
		p.addCSVLogger("log_csv", new String[] { "ID", "Journal", "Volume", "Issue", "Start-Page", "End-Page",
				"Page-Count", "ISSN", "LRT" , "Place"});
		p.processData();
		
		System.out.println("Done.");
	}

}