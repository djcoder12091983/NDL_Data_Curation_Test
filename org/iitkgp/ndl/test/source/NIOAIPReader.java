package org.iitkgp.ndl.test.source;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.iitkgp.ndl.data.AIPDataItem;
import org.iitkgp.ndl.data.container.NDLAIPDataContainer;
import org.iitkgp.ndl.util.NDLDataUtils;

public class NIOAIPReader extends NDLAIPDataContainer {
	
	static Pattern P1 = Pattern.compile("(doi|DOI): *([0-9/A-Z.a-z]+)(,|;| )");
	static Pattern P2 = Pattern.compile("(doi|DOI): *([0-9/A-Z.a-z]+).?$");

	public NIOAIPReader(String input, String logLocation, String name) {
		super(input, logLocation, name);
	}
	
	@Override
	protected boolean readItem(AIPDataItem item) throws Exception {
		String id = item.getId();
		List<String> others = item.getValue("dc.identifier.other");
		String journal = null, vol = null, issue = null, page = null, desc = null, date = null;
		String sp = null, ep = null, pc = null, doi = null, issn = null, isbn = null;
		if(!others.isEmpty()) {
			for(String o : others) {
				if(!StringUtils.equalsIgnoreCase(o, "no")) {
					StringBuilder jtext = new StringBuilder();
					log("log", id);
					log("log", o);
					if(o.matches("[0-9A-Za-z]{4}(-|–)[0-9A-Za-z]{4}")) {
						issn = o;
						log("log", "ISSN: " + o);
						continue;
					}
					if(o.matches("[0-9A-Za-z]+((-|–)[0-9A-Za-z]+)+")) {
						isbn = o;
						log("log", "ISBN: " + o);
						continue;
					}
					// extract
					Matcher m = P1.matcher(o);
					if(m.find()) {
						doi = m.group(2);
						o = o.replaceFirst("(doi|DOI): *([0-9/A-Z.a-z]+)(,|;| )", "");
					} else {
						m = P2.matcher(o);
						if(m.find()) {
							doi = m.group(2);
							o = o.replaceFirst("(doi|DOI): *([0-9/A-Z.a-z]+).?$", "");
						}
					}
					String tokens[] = NDLDataUtils.split(o, "( +|,|\\.|\\(|\\)|:|;)");
					int l = tokens.length;
					for(int i = 0; i < l;) {
						String t = tokens[i].trim();
						if(StringUtils.equalsIgnoreCase(t, "vol")) {
							if(i + 1 < l) {
								vol = tokens[i + 1];
								if(i + 2 < l) {
									String next = tokens[i + 2];
									if(next.matches("([A-Z]?[0-9]+(-|–)[A-Z]?[0-9]+(p|P)*)|([A-Z]?[0-9]+(p|P)+)")) {
										if(i + 3 < l) {
											if(!NDLDataUtils.isMonth(tokens[i + 3])) {
												page = next.replaceAll("p|P", "");
											}
										} else {
											page = next.replaceAll("p|P", "");
										}
										i += 3;
									} else if(StringUtils.equalsIgnoreCase(next, "p")) {
										page = tokens[i + 3].replaceAll("p|P", "");
										i += 4;
									} else {
										if(NumberUtils.isDigits(next)) {
											int t1 = Integer.parseInt(next);
											if(t1 >= 1900 && t1 <= 2018) {
												date = next;
											} else {
												issue = next;
											}
										}
										i += 3;
									}
								} else {
									i += 2;
								}
							} else {
								i++;
							}
						} /*else if(StringUtils.containsIgnoreCase(t, "doi")) {
							doi = tokens[i + 1];
							i += 2;
						}*/ else if(t.matches("([A-Z]?[0-9]+(-|–)[A-Z]?[0-9]+(p|P)*)|([A-Z]?[0-9]+(p|P)+)")) {
							page = t.replaceAll("p|P", "");
							i++;
						} else if(NumberUtils.isDigits(t)) {
							if(i + 1 < l) {
								String next = tokens[i + 1];
								if(next.toLowerCase().matches("p+")) {
									page = t;
									i += 2;
								} else {
									int t1 = Integer.parseInt(t);
									if(t1 >= 1900 && t1 <= 2018) {
										date = t;
									}
									i++;
								}
							} else {
								int t1 = Integer.parseInt(t);
								if(t1 >= 1900 && t1 <= 2018) {
									date = t;
								}
								i++;
							}
						} else {
							if(NumberUtils.isDigits(t) && t.length() == 4) {
								// 4 digit is year
								int t1 = Integer.parseInt(t) ;
								if(t1 >= 1900 && t1 <= 2018) {
									date = t;
								}
							} else {
								jtext.append(t).append(' ');
							}
							i++;
						}
					}
					journal = jtext.toString().trim();
					String jtokens[] = journal.split(" +");
					if (StringUtils.isNotBlank(journal)) {
						if (jtokens.length > 10 || StringUtils.startsWithIgnoreCase(journal, "in ")
								|| StringUtils.startsWithIgnoreCase(journal, "NIO/TR")
								|| StringUtils.startsWithIgnoreCase(journal, "encyclopedia")
								|| StringUtils.startsWithIgnoreCase(journal, "a ")
								|| StringUtils.startsWithIgnoreCase(journal, "an ")
								|| StringUtils.startsWithIgnoreCase(journal, "Refresher course")
								|| StringUtils.containsIgnoreCase(journal, "proceeding")
								|| StringUtils.containsIgnoreCase(journal, "PhD Thesis")
								|| StringUtils.containsIgnoreCase(journal, "seminar")
								|| StringUtils.containsIgnoreCase(journal, "training")
								|| StringUtils.containsIgnoreCase(journal, "bulletin")
								|| StringUtils.containsIgnoreCase(journal, "report")
								|| StringUtils.containsIgnoreCase(journal, "symposium")
								|| StringUtils.containsIgnoreCase(journal, "conference")
								|| (StringUtils.containsIgnoreCase(journal, "association")
										&& !StringUtils.startsWithIgnoreCase(journal, "journal"))) {
							desc = journal;
							journal  = null;
						} else {
							int p = StringUtils.indexOfIgnoreCase(journal, " ed");
							if(p != -1) {
								journal = journal.substring(0, p).trim();
							}
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
					if(StringUtils.isNotBlank(date)) {
						log("log", "Date: " + date);
					}
					if(StringUtils.isNotBlank(doi)) {
						log("log", "DOI: " + doi);
					}
					if(StringUtils.isNotBlank(page)) {
						log("log", "Page: " + page);
						tokens = page.replaceAll("[A-Z]", "").split("-|–");
						sp = tokens[0];
						ep = tokens.length == 2 ? tokens[1] : null;
						if(StringUtils.isNotBlank(ep)) {
							long p1 = Long.parseLong(sp);
							long p2 = Long.parseLong(ep);
							if(p2 < p1) {
								//System.err.println(page);
								long t1 = p1;
								p1 = p2;
								p2 = t1;
							}
							pc = String.valueOf(p2 - p1 + 1);
						}
					}
					log("log", NDLDataUtils.NEW_LINE);
				}
			}
			log("log_csv", new String[] { NDLDataUtils.getHandleSuffixID(id), journal, date, vol, issue, sp, ep, pc,
					desc, doi, issn, isbn });
		}
		return true;
	}
	
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/generated_xml_data/Nirupam/2018.Dec.05.14.03.37.CSIR-NatInstOceanography_updated_AIP_4_12_18.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/generated_xml_data/Nirupam/logs";
		String name = "nio";
		
		NIOAIPReader p = new NIOAIPReader(input, logLocation, name);
		p.addTextLogger("log");
		p.addCSVLogger("log_csv", new String[] { "ID", "Journal", "Date", "Volume", "Issue", "Start-Page", "End-Page",
				"Page-Count", "Description", "DOI", "ISSN", "ISBN" });
		p.processData();
		/*String text = "Journal of Geophysical Research (D: Atmos.), vol.115(16); doi:10.1029/2009JD013268, 12 pp.";
		Matcher m = P1.matcher(text);
		if(m.find()) {
			System.out.println(m.group(2));
		}*/
		
		System.out.println("Done.");
	}
}