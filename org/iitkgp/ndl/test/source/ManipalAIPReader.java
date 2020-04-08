package org.iitkgp.ndl.test.source;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.iitkgp.ndl.data.AIPDataItem;
import org.iitkgp.ndl.data.container.NDLAIPDataContainer;
import org.iitkgp.ndl.util.NDLDataUtils;

public class ManipalAIPReader extends NDLAIPDataContainer {
	
	static Pattern P1 = Pattern.compile("^.*([0-9]{2}\\.[0-9A-Za-z]+/.*)$");
	//static Pattern P2 = Pattern.compile("^((doi|DOI)(\\.|:))?org/");
	
	static String S1 = "Masters thesis,";
	static String S2 = "Phd. Thesis thesis,";
	
	public ManipalAIPReader(String input, String logLocation, String name) {
		super(input, logLocation, name);
	}
	
	@Override
	protected boolean readItem(AIPDataItem item) throws Exception {
		String lrt = item.getSingleValue("lrmi.learningResourceType");
		
		String journal = null, vol = null, issue = null, page = null, issn = null, isbn = null;
		String sp = null, ep = null, pc = null, inst = null, place = null, doi = null;

		String title = NDLDataUtils
				.removeMultipleSpaces(NDLDataUtils.removeNewLines(item.getSingleValue("dc.title")));
		List<String> desclist = item.getValue("dc.description");
		for(String desc : desclist) {
			// after title
			int pos = StringUtils.indexOfIgnoreCase(desc, title);
			if(pos == -1) {
				// invalid description
				continue;
			}
			if(StringUtils.containsIgnoreCase(desc, "in: ")) {
				// invalid description
				continue;
			}
			//System.out.println(desc);
			log("log", item.getId());
			log("log", desc);
			desc = desc.substring(pos + title.length());
			
			if(StringUtils.equals(lrt, "book")) {
				String tokens[] = NDLDataUtils.split(desc, "( +|,|\\.|\\(|\\)|:)");
				int l = tokens.length;
				for(int i = 0; i < l;) {
					String t = tokens[i].trim();
					if(StringUtils.equalsIgnoreCase(t, "ISBN")) {
						isbn = tokens[i + 1];
						i += 2;
					} else if(StringUtils.equalsIgnoreCase(t, "ISSN")) {
						issn = tokens[i + 1];
						i += 2;
					} else if(StringUtils.equalsIgnoreCase(t, "p")) {
						page = tokens[i + 1];
						i += 2;
					} else {
						i++;
					}
				}
			} else if(StringUtils.equals(lrt, "thesis")) {
				String t = null;
				int p = desc.indexOf(S1);
				if(p == -1) {
					p = desc.indexOf(S2);
					if(p != -1) {
						t = desc.substring(p + S2.length());
					}
				} else {
					t = desc.substring(p + S1.length());
				}
				if(StringUtils.isNotBlank(t)) {
					String tokens[] = t.split(" *, *");
					inst = tokens[0];
					if(tokens.length > 1) {
						place = tokens[1];
					}
				}
			} else if(StringUtils.equals(lrt, "article")) {
				StringBuilder jtext = new StringBuilder();
				String tokens[] = NDLDataUtils.split(desc, "( +|,|\\.|\\(|\\))");
				int l = tokens.length;
				for(int i = 0; i < l;) {
					String t = tokens[i].trim();
					if(StringUtils.equalsIgnoreCase(t, "ISBN")) {
						isbn = NDLDataUtils.removeHTMLTags(tokens[i + 1]).replaceAll("-|–", "");
						if(isbn.length() < 10) {
							// take up to end
							StringBuilder t1 = new StringBuilder();
							for(int j = i + 1; j< l; j++) {
								t1.append(NDLDataUtils.removeHTMLTags(tokens[j]));
							}
							isbn = isbn + t1.toString().trim();
							isbn = isbn.replaceAll("-|–|:", "");
							i = l; // end
						} else {
							i += 2;
						}
					} else if(StringUtils.equalsIgnoreCase(t, "ISSN")) {
						if(i + 1 < l) {
							if(tokens[i + 1].equalsIgnoreCase("issn")) {
								// ISSN ISSN
								i++;
							}
							issn = NDLDataUtils.removeHTMLTags(tokens[i + 1]).replaceAll("-|–", "");
							if(issn.length() < 8) {
								StringBuilder t1 = new StringBuilder();
								for(int j = i + 1; j< l; j++) {
									t1.append(NDLDataUtils.removeHTMLTags(tokens[j]));
								}
								issn = issn + t1.toString().trim();
								i = l; // end
							} else {
								i += 2;
							}
							issn = issn.replaceAll("-|–|:", "");
							int l1 = issn.length();
							if(l1 >= 10 && l1 <= 13) {
								isbn = issn;
								issn = null;
							}
						} else {
							i++;
						}
					} else if(t.matches("(p|P)+")) {
						page = tokens[i + 1];
						i += 2;
					} else if(NumberUtils.isDigits(t)) {
						int t1 = Integer.parseInt(t);
						if(t1 < 1900) {
							vol = t;
						}
						if(i + 1 < l) {
							String next = tokens[i + 1];
							if(NumberUtils.isDigits(next)) {
								issue = next;
								i += 2;
							} else {
								i++;
							}
						} else {
							i++;
						}
					} else {
						jtext.append(t).append(' ');
						i++;
					}
				}
				journal = jtext.toString().trim();
			}
		}
		
		List<String> otherlist = item.getValue("dc.identifier.other");
		for(String other : otherlist) {
			//System.out.println(other);
			log("log", other);
			//System.out.println(other);
			Matcher m = P1.matcher(other);
			if(m.find()) {
				doi = m.group(1).trim();
			} else if(other.matches("[0-9A-Za-z]{10,13}")) {
				// isbn
				isbn = other;
			}
		}
		
		boolean f = false;
		if(StringUtils.isNotBlank(journal)) {
			if(StringUtils.containsIgnoreCase(journal, "conference")
					|| StringUtils.containsIgnoreCase(journal, "proceeding")) {
				journal = null;
			} else {
				f = true;
			}
			log("log", "Journal: " + NDLDataUtils.NVL(journal, "NA"));
		}
		if(StringUtils.isNotBlank(vol)) {
			f = true;
			log("log", "Volume: " + vol);
		}
		if(StringUtils.isNotBlank(issue)) {
			f = true;
			log("log", "Issue: " + issue);
		}
		if(StringUtils.isNotBlank(issn)) {
			f = true;
			log("log", "ISSN: " + issn);
		}
		if(StringUtils.isNotBlank(isbn)) {
			f = true;
			log("log", "ISBN: " + isbn);
		}
		if(StringUtils.isNotBlank(doi)) {
			f = true;
			log("log", "DOI: " + doi);
		}
		if(StringUtils.isNotBlank(inst)) {
			f = true;
			log("log", "Institute: " + inst);
		}
		if(StringUtils.isNotBlank(place)) {
			f = true;
			log("log", "Place: " + place);
		}
		if(StringUtils.isNotBlank(page)) {
			f = true;
			log("log", "Page: " + page);
			String tokens[] = page.split("-|–");
			sp = tokens[0];
			if(tokens.length > 1) {
				ep = tokens[1];
				long p1 = Long.parseLong(sp);
				long p2 = Long.parseLong(ep);
				if(p2 < p1) {
					// delete wrong pagination
					sp = null;
					ep = null;
				} else {
					pc = String.valueOf(p2 - p1 + 1);
				}
			}
		}
		
		log("log", NDLDataUtils.NEW_LINE);
		if(f) {
			// any value found
			log("log_csv", new String[] { NDLDataUtils.getHandleSuffixID(item.getId()), journal, vol, issue, sp, ep, pc,
					issn, isbn, inst, place, doi });
		}
		
		return true;
	}
	
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/generated_xml_data/Nirupam/ManipalUniv.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/generated_xml_data/Nirupam/logs";
		String name = "manipal";
		
		ManipalAIPReader p = new ManipalAIPReader(input, logLocation, name);
		p.addTextLogger("log");
		p.addCSVLogger("log_csv", new String[] { "ID", "Journal", "Volume", "Issue", "Start-Page", "End-Page",
				"Page-Count", "ISSN", "ISBN", "Institute", "Place", "DOI" });
		p.processData();
		
		/*String text = "doi: 10.5455/2320-6012.ijrms20140222";
		Matcher m = P1.matcher(text);
		if(m.find()) {
			System.out.println(m.group(1));
		}*/
		/*Matcher m1 = P1.matcher(text);
		System.out.println(m1.find());*/
		
		/*String text = "00200020&#x2013;3408";
		System.out.println(Jsoup.parse(text).text());*/
		
		System.out.println("Done");
	}
}