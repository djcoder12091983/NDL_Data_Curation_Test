package org.iitkgp.ndl.test.source;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.iitkgp.ndl.data.AIPDataItem;
import org.iitkgp.ndl.data.container.NDLAIPDataContainer;
import org.iitkgp.ndl.util.NDLDataUtils;

public class CSIRIICTAIPReader extends NDLAIPDataContainer {

	public CSIRIICTAIPReader(String input, String logLocation, String name) {
		super(input, logLocation, name);
	}
	
	@Override
	protected boolean readItem(AIPDataItem item) throws Exception {
		List<String> other = item.getValue("dc.identifier.other");
		List<String> publisher = item.getValue("dc.publisher");
		for(String p : publisher) {
			if(StringUtils.containsIgnoreCase(p, "vol.")) {
				other.add(p);
			} else {
				log("log", item.getId());
				log("log", p);
				String pub, place = null;
				String tokens[] = NDLDataUtils.split(p, " *(,|\\.) *");
				if(tokens.length == 2 && tokens[1].matches("[A-Za-z ]+")) {
					pub = tokens[0];
					place = tokens[1];
				} else {
					pub = p;
				}
				if(StringUtils.isNotBlank(pub)) {
					log("log", "Publisher: " + pub);
				}
				if(StringUtils.isNotBlank(place)) {
					log("log", "Place: " + place);
				}
				log("log", NDLDataUtils.NEW_LINE);
				log("log_csv_publisher", new String[] { NDLDataUtils.getHandleSuffixID(item.getId()), pub, place });
			}
		}
		if(!other.isEmpty()) {
			extract(item.getId(), other);
		}
		
		return true;
	}
	
	void extract(String id, List<String> texts) throws Exception {
		String journal = null, vol = null, issue = null, page = null, issn = null, eissn = null, date = null;
		String sp = null, ep = null, pc = null, degree = null, place = null, inst = null;
		StringBuilder jtext = new StringBuilder();
		log("log", id);
		for(String text : texts) {
			//System.out.println(text);
			log("log", text);
			if(StringUtils.startsWithIgnoreCase(text, "Ph.D Thesis")) {
				// phd thesis
				String tokens[] = text.split(",");
				degree = "phd";
				int l = tokens.length;
				if(l > 1) {
					inst = tokens[1];
				}
				if(l > 2) {
					place = tokens[2].replace(".", "");
				}
			} else if(text.matches("[0-9]{4}(-|–)?[0-9]{4}")) {
				// issn
				issn = text;
			} else if(text.matches("[0-9A-Za-z]{4}(-|–)?[0-9A-Za-z]{4}")) {
				// eissn
				eissn = text;
			} else {
				//System.out.println(text);
				String tokens[] = NDLDataUtils.split(text, "( +|,|\\.|\\(|\\)|:)");
				int l = tokens.length;
				for(int i = 0; i < l;) {
					String t = tokens[i].trim();
					if(t.matches(".{4}(-|–)?.{4}")) {
						if(i + 1 < l) {
							String next = tokens[i + 1];
							if(StringUtils.equalsIgnoreCase(next, "online")) {
								// eissn
								eissn = t;
								i += 2;
							} else if(StringUtils.equalsIgnoreCase(next, "print")) {
								// issn
								issn = t;
								i += 2;
							} else {
								if(t.matches("[0-9]{4}(-|–)?[0-9]{4}")){
									issn = t;
								} else {
									jtext.append(t).append(' ');
								}
								i++;
							}
						} else {
							// issn
							if(t.matches("[0-9]+(-|–)[0-9]+(p|P)?")) {
								page = t.replaceFirst("p|P", "");
							} else {
								jtext.append(t).append(' ');
							}
							i++;
						}
					} else if(StringUtils.equalsIgnoreCase(t, "print")) {
						// issn
						if(i + 1 < l) {
							issn = tokens[i + 1];
							i += 2;
						} else {
							i++;
						}
					} else if(StringUtils.equalsIgnoreCase(t, "online")) {
						// eissn
						if(i + 1 < l) {
							eissn = tokens[i + 1];
							i += 2;
						} else {
							i++;
						}
					} else if(StringUtils.equalsIgnoreCase(t, "vol")) {
						if(i + 1 < l) {
							vol = tokens[i + 1];
							if(i + 2 < l) {
								String next = tokens[i + 2];
								if(next.matches("[0-9]+(-|–)[0-9]+(p|P)")) {
									page = next.replaceFirst("p|P", "");
								} else {
									issue = next;
								}
								i += 3;
							} else {
								i += 2;
							}
						} else {
							i++;
						}
					} else if(StringUtils.equalsIgnoreCase(t, "no")) {
						if(i + 1 < l) {
							issue = tokens[i + 1];
							i += 2;
						} else {
							i++;
						}
					} else if(t.matches("[0-9]+(-|–)[0-9]+(p|P)?")) {
						page = t.replaceFirst("p|P", "");
						i++;
					} else if(NumberUtils.isDigits(t)) {
						if(t.length() == 4) {
							int t1 = Integer.parseInt(t);
							if(t1 >= 1900 && t1 <= 2018) {
								date = t;
							}
							i++;
						} else if(i + 1 < l) {
							// next available
							String next = tokens[i + 1];
							if(NumberUtils.isDigits(next)) {
								vol = t;
								issue = t;
								i += 2;
							} else if(next.matches("-|–")) {
								// page
								page = t + '-' + tokens[i + 2];
								i += 3;
							} else {
								vol = t;
								i++;
							}
						} else {
							vol = t;
							i++;
						}
					} else {
						jtext.append(t).append(' ');
						i++;
					}
				}
			}
		}
		if(StringUtils.equals(vol, "0")) {
			// invalid vol
			vol = null;
		}
		journal = jtext.toString().trim();
		if(StringUtils.isNotBlank(journal)) {
			log("log", "Journal: " + journal);
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
		if(StringUtils.isNotBlank(eissn)) {
			log("log", "EISSN: " + eissn);
		}
		if(StringUtils.isNotBlank(degree)) {
			log("log", "Degree: " + degree);
		}
		if(StringUtils.isNotBlank(inst)) {
			log("log", "Institution: " + inst);
		}
		if(StringUtils.isNotBlank(place)) {
			log("log", "Place: " + place);
		}
		if(StringUtils.isNotBlank(date)) {
			log("log", "Date: " + date);
		}
		if(StringUtils.isNotBlank(page)) {
			log("log", "Page: " + page);
			String tokens[] = page.replaceAll("p|P", "").split("-|–");
			sp = tokens[0];
			ep = tokens[1];
			long p1 = Long.parseLong(sp);
			long p2 = Long.parseLong(ep);
			if(p2 < p1) {
				long t1 = p1;
				p1 = p2;
				p2 = t1;
			}
			pc = String.valueOf(p2 - p1 + 1);
		}
		log("log", NDLDataUtils.NEW_LINE);
		log("log_csv", new String[] { NDLDataUtils.getHandleSuffixID(id), journal, vol, issue, sp, ep, pc, issn, eissn,
				degree, inst, place, date });
	}
	
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/generated_xml_data/Nirupam/2018.Nov.27.12.09.04.CSRI-IICT.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/generated_xml_data/Nirupam/logs";
		String name = "csir.iict";
		
		CSIRIICTAIPReader p = new CSIRIICTAIPReader(input, logLocation, name);
		p.addTextLogger("log");
		p.addCSVLogger("log_csv", new String[] { "ID", "Journal", "Volume", "Issue", "Start-Page", "End-Page",
				"Page-Count", "ISSN", "EISSN", "Degree", "Institute", "Place", "Date" });
		p.addCSVLogger("log_csv_publisher", new String[] { "ID", "Publisher", "Place" });
		p.processData();
		
		System.out.println("Done.");
	}
}