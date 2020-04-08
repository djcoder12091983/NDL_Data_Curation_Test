package org.iitkgp.ndl.test.source;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.iitkgp.ndl.data.AIPDataItem;
import org.iitkgp.ndl.data.container.NDLAIPDataContainer;
import org.iitkgp.ndl.util.NDLDataUtils;

public class CUSATAIPReader extends NDLAIPDataContainer {
	
	static Pattern P1 = Pattern.compile("\\(DOI:(.+)\\)");
	static Pattern P2 = Pattern.compile("((http://)?www\\..+)");

	public CUSATAIPReader(String input, String logLocation, String name) {
		super(input, logLocation, name);
	}
	
	@Override
	protected boolean readItem(AIPDataItem item) throws Exception {
		String id = item.getId();
		List<String> values = item.getValue("dc.identifier.other");
		values.addAll(item.getValue("dc.relation"));
		String journal = null, vol = null, issue = null, page = null, org = null, uri = null, date = null;
		String sp = null, ep = null, pc = null, doi = null, issn = null, eissn = null;
		StringBuilder jtext = new StringBuilder();
		if(!values.isEmpty()) {
			log("log", id);
			for(String v : values) {
				//System.out.println(v);
				if(StringUtils.containsIgnoreCase(v, "OCIS codes") || StringUtils.startsWithIgnoreCase(v, "PII: ")) {
					// invalid text
					continue;
				}
				log("log", v);
				if(StringUtils.startsWithIgnoreCase(v, "DOI 10.1002 /")) {
					// need to correct
					log("log", "Probably DOI or URL to be corected.");
				} else if(NDLDataUtils.containsAny(v, true, "University", "Faculty", "School", "Department", "Division",
						"FACULTY", "Plasma", "Institute")) {
					org = v;
				} else if(v.matches(".{4}(-|–)?.{4}")) {
					// issn
					issn = v;
				} else if(v.matches(".{4} *(-|–)? *.{4}")) { 
					// issn
					issn = v.replaceAll("-| |–", "");
				} else if(v.matches("^http://dx\\.doi\\.org/.*$")) {
					// doi
					doi = v.substring(v.indexOf("dx.doi.org/") + "dx.doi.org/".length());
				} else if(StringUtils.startsWithIgnoreCase(v, "doi:")) {
					// doi
					doi = v.substring("doi:".length()).trim();
				} else if(v.matches("^(http://)?www\\..*$")) {
					// uri
					uri = v;
				} else {
					// special cases
					String tokens[] = v.split(" *, *");
					if (tokens.length == 2 && StringUtils.startsWithIgnoreCase(tokens[0], "doi")
							&& StringUtils.startsWithIgnoreCase(tokens[1], "www.")) {
						log("log", "Probably DOI or URL to be corected.");
						continue;
					}
					if(v.contains("D OI 10.1002 /app .25420")) {
						doi = "10.1002/app.25420";
						v = v.replace("10.1002 /app .25420", "");
					} else {
						Matcher m = P1.matcher(v);
						if(m.find()) {
							doi = m.group(1).trim();
						} else {
							m = P2.matcher(v);
							if(m.find()) {
								uri = m.group();
							}
						}
						v = v.replaceFirst("\\(DOI:(.+)\\)", "").replaceFirst("(http://)?www\\..+", "");
					}
					tokens = NDLDataUtils.split(v, "( +|,|\\.|\\(|\\)|:|/|=)");
					int l = tokens.length;
					for(int i = 0; i < l;) {
						String t = tokens[i].trim();
						if(StringUtils.equalsIgnoreCase(t, "e-issn")) {
							// eissn
							eissn = tokens[i + 1];
							i += 2;
						} else if(StringUtils.equalsIgnoreCase(t, "p-issn")) {
							// issn
							issn = tokens[i + 1];
							i += 2;
						} else if(t.matches(".{4}(-|–)?.{4}")) {
							if(i + 1 < l) {
								String next = tokens[i + 1];
								if(StringUtils.equalsIgnoreCase(next, "print")) {
									// issn
									issn = t;
								} else if(StringUtils.equalsIgnoreCase(next, "online")) {
									// eissn
									eissn = t;
								}
								i += 2;
							} else {
								page = t;
								i++;
							}
						} else if(StringUtils.equalsIgnoreCase(t, "issn")) {
							if(i + 1 < l) {
								String next = tokens[i + 1];
								if(StringUtils.equalsIgnoreCase(next, "print")) {
									// issn
									issn = tokens[i + 2];
								} else if(StringUtils.equalsIgnoreCase(next, "online")) {
									// eissn
									eissn = tokens[i + 2];
								}
								i += 3;
							} else {
								i++;
							}
						} else if (StringUtils.equalsIgnoreCase(t, "vol")) {
							vol = tokens[i + 1];
							i += 2;
						} else if (StringUtils.equalsIgnoreCase(t, "no")
								|| StringUtils.startsWithIgnoreCase(t, "issue")) {
							if(i + 1 < l) {
								issue = tokens[i + 1];
								i += 2;
							} else {
								i++;
							}
						} else if (StringUtils.equalsIgnoreCase(t, "pp")
								|| StringUtils.startsWithIgnoreCase(t, "page")) {
							page = tokens[i + 1];
							i += 2;
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
									issue = next;
									i += 2;
								} else if(next.matches("-|–")) {
									// page
									page = t + '-' + tokens[i + 2];
									i += 3;
								} else if(NDLDataUtils.isMonth(next)) {
									date = t + ' ' + next + tokens[i + 2];
									i += 3;
								} else if(next.matches("[A-Za-z]?[0-9]+(-|–)[A-Za-z]?[0-9]+")) {
									// page
									page = next;
									i += 2;
								} else {
									vol = t;
									i++;
								}
							} else {
								vol = t;
								i++;
							}
						} else if(NDLDataUtils.isMonth(t)) {
							date = t + ' ' + tokens[i + 1];
							i += 2;
						} else if(t.matches("[A-Za-z]?[0-9]+(-|–)[A-Za-z]?[0-9]+")) {
							page = t;
							i++;
						} else {
							// journal
							jtext.append(t).append(' ');
							i++;
						}
					}
				}
			}
			// done
			if(StringUtils.isNotBlank(uri) && !StringUtils.startsWithIgnoreCase(uri, "http://")) {
				uri = "http://" + uri;
			}
			journal = jtext.toString().trim();
			if(StringUtils.isNotBlank(journal)) {
				if(StringUtils.equalsIgnoreCase(journal, "hindi")) {
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
			if(StringUtils.isNotBlank(eissn)) {
				log("log", "EISSN: " + eissn);
			}
			if(StringUtils.isNotBlank(date)) {
				log("log", "Date: " + date);
			}
			if(StringUtils.isNotBlank(uri)) {
				log("log", "URI: " + uri);
			}
			if(StringUtils.isNotBlank(org)) {
				log("log", "ORG: " + org);
			}
			if(StringUtils.isNotBlank(doi)) {
				log("log", "DOI: " + doi);
			}
			if(StringUtils.isNotBlank(page)) {
				log("log", "Page: " + page);
				String tokens[] = page.replaceAll("[A-Za-z]", "").split("-|–");
				sp = tokens[0];
				if(tokens.length > 1) {
					ep = tokens[1];
					long p1 = Long.parseLong(sp);
					long p2 = Long.parseLong(ep);
					/*if(p2 < p1) {
						// TODO discuss
						long t1 = p1;
						p1 = p2;
						p2 = t1;
					}*/
					pc = String.valueOf(p2 - p1 + 1);
				}
			}
			log("log", NDLDataUtils.NEW_LINE);
			log("log_csv", new String[] { NDLDataUtils.getHandleSuffixID(id), journal, date, vol, issue, sp, ep, pc,
					org, uri, doi, issn, eissn });
		}
		return true;
	}

	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/generated_xml_data/Nirupam/2018.Nov.27.15.30.49.CUSAT_Thesis_AIP_Output_27_11_18.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/generated_xml_data/Nirupam/logs";
		String name = "cusat";
		
		CUSATAIPReader p = new CUSATAIPReader(input, logLocation, name);
		p.addTextLogger("log");
		p.addCSVLogger("log_csv", new String[] { "ID", "Journal", "Date", "Volume", "Issue", "Start-Page", "End-Page",
				"Page-Count", "Organization", "URI", "DOI", "ISSN", "EISSN" });
		p.processData();
		/*String text = "2347–856X";
		System.out.println(text.matches(".{4} *(-|–)? *.{4}"));*/
		
		/*String text = "1)01 10.1002 / a pp.24563, www.interscience.wiley.com";
		Matcher m = P2.matcher(text);
		if(m.find()) {
			System.out.println(m.group(1));
		}*/
		
		System.out.println("Done.");
	}
}