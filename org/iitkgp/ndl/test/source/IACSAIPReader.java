package org.iitkgp.ndl.test.source;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.iitkgp.ndl.data.AIPDataItem;
import org.iitkgp.ndl.data.container.NDLAIPDataContainer;
import org.iitkgp.ndl.util.NDLDataUtils;

public class IACSAIPReader extends NDLAIPDataContainer {
	
	public IACSAIPReader(String input, String logLocation, String name) {
		super(input, logLocation, name);
	}

	@Override
	protected boolean readItem(AIPDataItem item) throws Exception {
		
		List<String> values = item.getValue("dc.identifier.other");
		values.addAll(item.getValue("dc.relation"));
		if(!values.isEmpty()) {
			extract(NDLDataUtils.getHandleSuffixID(item.getId()), values);
		}
		return true;
	}
	
	void extract(String id, List<String> values) throws Exception {
		String journal = null, vol = null, issue = null, page = null, doi = null, date = null;
		String sp = null, ep = null, pc = null;
		StringBuilder jtext = new StringBuilder();
		if(!values.isEmpty()) {
			for(String value : values) {
				if (StringUtils.containsIgnoreCase(value, "PACS No")
						|| StringUtils.containsIgnoreCase(value, "Indian Patent")
						|| StringUtils.containsIgnoreCase(value, "International Patent Classification")
						|| StringUtils.containsIgnoreCase(value, "10th")) {
					// wrong token
					continue;
				}
				log("iacs.log", value);
				if(value.contains("dx.doi")) {
					doi = value.substring(value.indexOf("dx.doi.org/") + "dx.doi.org/".length());
					doi = doi.replaceFirst(";$", "");
					continue;
				}
				String tokens[] = NDLDataUtils.split(value, "( +|,|\\.|\\(|\\)|;|:)");
				int l = tokens.length;
				for(int i = 0; i < l;) {
					String t = tokens[i].trim();
					if(StringUtils.equalsIgnoreCase(t, "CLMS")) {
						i++;
						continue;
					}
					if(StringUtils.startsWithIgnoreCase(t, "vol")) {
						vol = tokens[i + 1];
						if(StringUtils.endsWithIgnoreCase(vol, "p")) {
							// special case
							vol = vol.replaceFirst("p|P", "");
							tokens[i + 2] =  'p' + tokens[i + 2];
						}
						i += 2;
					} else if(StringUtils.startsWithIgnoreCase(t, "issue")
							|| StringUtils.startsWithIgnoreCase(t, "number")
							|| StringUtils.startsWithIgnoreCase(t, "no")) {
						issue = tokens[i + 1];
						i += 2;
					} else if(StringUtils.equalsIgnoreCase(t, "year")) {
						date = tokens[i + 1];
						i += 2;
					} else if(StringUtils.equalsIgnoreCase(t, "page") || StringUtils.equalsIgnoreCase(t, "p")) {
						String next = tokens[i + 1];
						if(StringUtils.startsWithIgnoreCase(next, "no")
								|| StringUtils.startsWithIgnoreCase(next, "number")) {
							page = tokens[i + 2];
							i += 3;
						} else {
							page = next;
							i += 2;
						}
					} else if(t.matches("[0-9]{4}") || t.matches("[0-9]{4}-[0-9]{2}")) {
						// year
						date = t;
						i++;
					} else {
						if(NumberUtils.isDigits(t)) {
							String next = tokens[i + 1];
							if(next.equals("-") || next.equals("–")) {
								// pagination
								page = t + "-" + tokens[i + 2];
								i += 3;
							} else if(NumberUtils.isDigits(next)) {
								vol = t;
								issue = next;
								i += 2;
							} else {
								if(StringUtils.isBlank(vol)) {
									// not assigned yet
									vol = t;
								}
								i++;
							}
						} else if(t.matches("[0-9]+[A-Z]")) {
							vol = t;
							String next = tokens[i + 1];
							if(NumberUtils.isDigits(next)) {
								issue = next;
							}
							i += 2;
						} else if(t.matches("[0-9]+(-|–)[0-9]+") || t.matches("(p|P)[0-9]+")) {
							// pagination
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
				log("iacs.log", "Journal: " + journal);
			}
			if(StringUtils.isNotBlank(vol)) {
				log("iacs.log", "Volume: " + vol);
			}
			if(StringUtils.isNotBlank(issue)) {
				log("iacs.log", "Issue: " + issue);
			}
			if(StringUtils.isNotBlank(doi)) {
				log("iacs.log", "DOI: " + doi);
			}
			if(StringUtils.isNotBlank(page)) {
				log("iacs.log", "Page: " + page);
				String tokens[] = page.split("-|–");
				sp = tokens[0];
				if(tokens.length == 2) {
					ep = tokens[1];
					pc = String.valueOf(Long.parseLong(ep) - Long.parseLong(sp) + 1);
				}
			}
			if(StringUtils.isNotBlank(date)) {
				log("iacs.log", "Date: " + date);
				date = date.split("-")[0];
			}
			log("iacs.log", NDLDataUtils.NEW_LINE);
		}
		log("iacs_csv", new String[]{id, journal, vol, issue, sp, ep, pc, date, doi});
	}
	
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/generated_xml_data/Nirupam/2018.Nov.13.15.00.05.IACS KOLKATA.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/generated_xml_data/Nirupam/logs";
		String name = "iacs.kolkata";
		
		IACSAIPReader r = new IACSAIPReader(input, logLocation, name);
		r.addTextLogger("iacs.log");
		r.addCSVLogger("iacs_csv",
				new String[] { "ID", "Journal", "Volume", "Issue", "Start-Page", "End-Page", "Page-Count", "Date", "DOI" });
		r.processData();
		
		/*String text = "78A";
		System.out.println(text.matches("[0-9]+[A-Z]"));*/
		
		System.out.println("Done.");
	}

}