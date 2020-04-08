package org.iitkgp.ndl.test.source;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.data.AIPDataItem;
import org.iitkgp.ndl.data.container.NDLAIPDataContainer;
import org.iitkgp.ndl.util.NDLDataUtils;

public class ISIKolkataAIPReader extends NDLAIPDataContainer {
	
	public ISIKolkataAIPReader(String input, String logLocation, String name) {
		super(input, logLocation, name);
	}
	
	@Override
	protected boolean readItem(AIPDataItem item) throws Exception {
		String other = item.getSingleValue("dc.identifier.other");
		if(StringUtils.isNotBlank(other)) {
			other = other.replaceAll("(-|–)+", "-"); // normalize
			//System.out.println(other);
			log("log", item.getId());
			log("log", other);
			
			String journal = null, vol = null, issue = null, page = null;
			String sp = null, ep = null, pc = null;
			StringBuilder jtext = new StringBuilder();
			String tokens[] = NDLDataUtils.split(other, "( +|,|\\.|\\(|\\)|:)");
			int l = tokens.length;
			for(int i = 0; i < l;) {
				String t = tokens[i].trim();
				String nt = t.toLowerCase(); // normalized
				if(StringUtils.equalsIgnoreCase(t, "v")) {
					// volume
					if(i + 1 < l) {
						vol = tokens[i + 1];
						if(i + 2 < l) {
							String next = tokens[i + 2].toLowerCase();
							if(next.matches("p[a-z]?[0-9]+((-|–)[a-z]?[0-9]+)?")) {
								page = nt.replaceAll("[a-z]", "");
								i += 3;
							} else {
								if(next.matches("[0-9]+(-|–|/)[0-9]+") || next.matches("[0-9]+")) {
									// issue
									issue = next;
									i += 3;
								} else {
									i += 2;
								}
							}
						} else {
							i += 2;
						}
					} else {
						i++;
					}
				} else if(nt.matches("v[0-9]+")) {
					// volume
					vol = t.replaceFirst("^(v|V)", "");
					i++;
				} else if(nt.matches("pt[0-9]+")) {
					// issue
					issue = nt.replaceFirst("^(pt)", "");
					i++;
				} else if(StringUtils.equalsIgnoreCase(t, "is") || StringUtils.equalsIgnoreCase(t, "no")) {
					// issue
					issue = tokens[i + 1];
					i += 2;
				} else if(StringUtils.equalsIgnoreCase(t, "p")) {
					// page
					if(i + 1 < l) {
						page = tokens[i + 1].replaceAll("[A-Za-z]", "");;
						i += 2;
					} else {
						i++;
					}
				} else if(nt.matches("p[a-z]?[0-9]+((-|–)[a-z]?[0-9]+)?")) {
					page = nt.replaceAll("[a-z]", "");
					i++;
				} else {
					jtext.append(t).append(' ');
					i++;
				}
			}
			journal = jtext.toString().trim();
			if(StringUtils.isNotBlank(journal)) {
				log("log", journal);
			}
			if(StringUtils.isNotBlank(vol)) {
				log("log", "Volume: " + vol);
			}
			if(StringUtils.isNotBlank(issue)) {
				log("log", "Issue: " + issue);
			}
			if(StringUtils.isNotBlank(page)) {
				log("log", "Page: " + page);
				page = page.replaceFirst("\\+.*$", ""); // special case
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
			log("log", NDLDataUtils.NEW_LINE);
			log("log_csv",
					new String[] { NDLDataUtils.getHandleSuffixID(item.getId()), journal, vol, issue, sp, ep, pc });
		}
		return true;
	}
	
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/generated_xml_data/Nirupam/2018.Nov.29.17.21.11.ISI_KOL_mod_AIP.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/generated_xml_data/Nirupam/logs";
		String name = "isi.kolkata";
		
		ISIKolkataAIPReader p = new ISIKolkataAIPReader(input, logLocation, name);
		p.addTextLogger("log");
		p.addCSVLogger("log_csv",
				new String[] { "ID", "Journal", "Volume", "Issue", "Start-Page", "End-Page", "Page-Count" });
		p.processData();
		
		System.out.println("Done.");
	}
}