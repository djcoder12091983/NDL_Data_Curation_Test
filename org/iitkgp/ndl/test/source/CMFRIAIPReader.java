package org.iitkgp.ndl.test.source;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.iitkgp.ndl.data.AIPDataItem;
import org.iitkgp.ndl.data.container.NDLAIPDataContainer;
import org.iitkgp.ndl.util.NDLDataUtils;

public class CMFRIAIPReader extends NDLAIPDataContainer {

	public CMFRIAIPReader(String input, String logLocation, String name) {
		super(input, logLocation, name);
	}
	
	@Override
	protected boolean readItem(AIPDataItem item) throws Exception {
		String other = item.getSingleValue("dc.identifier.other");
		if(StringUtils.isNotBlank(other)) {
			log("log", item.getId());
			log("log", other);
			String title = NDLDataUtils
					.removeMultipleSpaces(NDLDataUtils.removeNewLines(item.getSingleValue("dc.title")));
			/*if(item.getId().equals("123456005_cmfri/282850")) {
				System.out.println(other);
				System.out.println(title);
			}*/
			other = other.substring(StringUtils.indexOfIgnoreCase(other, title) + title.length());
			String journal = null, series = null, vol = null, issue = null, page = null, isbn = null;
			String sp = null, ep = null, pc = null;
			StringBuilder jtext = new StringBuilder();
			String tokens[] = NDLDataUtils.split(other, "( +|,|\\.|\\(|\\))");
			int l = tokens.length;
			for(int i = 0; i < l;) {
				String t = tokens[i].trim();
				if(StringUtils.equalsIgnoreCase(t, "isbn")) {
					isbn = tokens[i + 1];
					i += 2;
				} else if(NumberUtils.isDigits(t)) {
					if(t.length() < 4) {
						String next = null;
						if(i + 1 < l) {
							next = tokens[i + 1];
						}
						if(!NDLDataUtils.isMonth(next)) {
							vol = t;
						}
					}
					if(i + 1 < l) {
						String next = tokens[i + 1];
						if(NumberUtils.isDigits(next) || next.matches("[0-9]+&[0-9]+")) {
							issue = next;
							if(i + 2 < l) {
								next = tokens[i + 2];
								if(next.equals("&")) {
									issue = issue + "&" + tokens[i + 3];
									i += 4;
								} else {
									i += 2;
								}
							} else {
								i += 2;
							}
						} else {
							// normal move
							i++;
						}
					} else {
						i++;
					}
				} else if(StringUtils.equalsIgnoreCase(t, "no")) {
					if(i + 1 < l) {
						String next = tokens[i + 1];
						if(NumberUtils.isDigits(next)) {
							issue = next;
							i += 2;
						} else {
							jtext.append(t);
							i++;
						}
					}
				} else if(t.matches("(p|P)+")) {
					String next = tokens[i + 1];
					if(next.matches("[0-9]+-[0-9]+")) {
						page = next;
					}
					i += 2;
				} else {
					jtext.append(t).append(' ');
					i++;
				}
			}
			journal = jtext.toString().trim();
			if(StringUtils.isNotBlank(journal)) {
				if(StringUtils.containsIgnoreCase(journal, "series")) {
					series = journal + " " + (StringUtils.isNotBlank(vol) ? vol : "");
					journal = null;
					vol = null;
				} else if(StringUtils.startsWithIgnoreCase(journal, "in:")
						|| (StringUtils.isBlank(vol) || StringUtils.isBlank(issue))) {
					journal = null;
				} else if(StringUtils.contains(journal, "मत्स्यगंधा Matsyagandha")) {
					journal = "मत्स्यगंधा Matsyagandha";
				}
				log("log", "Journal: " + NDLDataUtils.NVL(journal, "NA"));
				log("log", "Series: " + NDLDataUtils.NVL(series, "NA"));
			}
			if(StringUtils.isNotBlank(vol)) {
				log("log", "Volume: " + vol);
			}
			if(StringUtils.isNotBlank(issue)) {
				log("log", "Issue: " + issue);
			}
			if(StringUtils.isNotBlank(isbn)) {
				log("log", "ISBN: " + isbn);
			}
			if(StringUtils.isNotBlank(page)) {
				log("log", "Page: " + page);
				tokens = page.split("-|–");
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
			log("log_csv", new String[] { NDLDataUtils.getHandleSuffixID(item.getId()), other, journal, series, vol,
					issue, sp, ep, pc, isbn });
		}
			
		return true;
	}
	
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/generated_xml_data/Nirupam/2018.Nov.27.12.37.15.Central_MFRI.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/generated_xml_data/Nirupam/logs";
		String name = "cmfri";
		
		CMFRIAIPReader p = new CMFRIAIPReader(input, logLocation, name);
		p.addTextLogger("log");
		p.addCSVLogger("log_csv", new String[] { "ID", "Text", "Journal", "Series", "Volume", "Issue", "Start-Page",
				"End-Page", "Page-Count", "ISBN" });
		p.processData();
		
		/*String text = "Shubha, V and Ramesh, TG (1987) Controller 2001 for stabilization of high voltage. Technical Report. National Aeronautical Laboratory, Bangalore, India.";
		String tokens[] = NDLDataUtils.split(text, " +");
		for(String t : tokens) {
			Pattern P = Pattern.compile(".*([0-9]{4}).*");
			Matcher m = P.matcher(t);
			if(m.find()) {
				System.out.println(m.group(1));
				break;
			}
		}*/
		System.out.println("Done.");
	}

}