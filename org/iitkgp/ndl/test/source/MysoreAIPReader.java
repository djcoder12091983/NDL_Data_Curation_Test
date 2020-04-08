package org.iitkgp.ndl.test.source;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.iitkgp.ndl.data.AIPDataItem;
import org.iitkgp.ndl.data.container.NDLAIPDataContainer;
import org.iitkgp.ndl.util.NDLDataUtils;

public class MysoreAIPReader extends NDLAIPDataContainer {
	
	public MysoreAIPReader(String input, String logLocation, String name) {
		super(input, logLocation, name);
	}
	
	@Override
	protected boolean readItem(AIPDataItem item) throws Exception {
		String desc = item.getSingleValue("dc.description");
		List<String> relation = item.getValue("dc.relation");
		String other = item.getSingleValue("dc.identifier.other");
		
		String journal = null, vol = null, issue = null, page = null, doi = null, uri = null, issn = null, isbn = null;
		String sp = null, ep = null, pc = null;
		StringBuilder jtext = new StringBuilder();
		log("log", item.getId());
		if(StringUtils.isNoneBlank(desc)) {
			log("log", desc);
			String title = item.getSingleValue("dc.title");
			desc = desc.substring(StringUtils.indexOfIgnoreCase(desc, title) + title.length());
			String tokens[] = NDLDataUtils.split(desc, "( +|,|\\.|\\(|\\))");
			int l = tokens.length;
			for(int i = 0; i < l;) {
				String t = tokens[i].trim();
				if(StringUtils.equalsIgnoreCase(t, "issn")) {
					if(tokens[i + 1].matches(".{4}(-|–).{4}")) {
						issn = tokens[i + 1];
						i += 2;
					} else {
						StringBuilder t1 = new StringBuilder();
						for(int j = i + 1; j < l; j++) {
							t1.append(tokens[j]);
						}
						issn = t1.toString();
						// end
						i = l;
					}
				} else if(StringUtils.equalsIgnoreCase(t, "isbn")) {
					isbn = tokens[i + 1];
					i += 2;
				} else if(NumberUtils.isDigits(t)) {
					if(i + 1 < l) {
						String next = tokens[i + 1];
						if(NumberUtils.isDigits(next) || next.matches("[0-9]+(-|–)[0-9]+")) {
							vol = t;
							issue = next;
							i += 2;
						} else {
							jtext.append(t).append(' ');
							i++;
						}
					} else {
						// single value is volume (last value)
						vol = t;
						i++;
					}
				} else if (t.matches("(p|P)+")) {
					page = tokens[i + 1];
					i += 2;
				} else {
					jtext.append(t).append(' ');
					i++;
				}
			}
		}
		
		for(String r : relation) {
			if(r.startsWith("http://")) {
				log("log", r);
				uri = r;
			}
		}
		
		if(StringUtils.isNotBlank(other)) {
			log("log", other);
			if(StringUtils.containsIgnoreCase(other, "dx.doi.org/")) {
				doi = other.substring(other.indexOf("dx.doi.org/") + "dx.doi.org/".length()).trim();
			} else if(StringUtils.containsIgnoreCase(other, "doi:")) {
				doi = other.substring(other.indexOf("doi:") + "doi:".length()).trim();
			}
		}
		
		journal = jtext.toString().trim();
		if(StringUtils.isNotBlank(journal)) {
			journal = journal.replaceFirst("Cop$", "").trim();
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
		if(StringUtils.isNotBlank(isbn)) {
			log("log", "ISBN: " + isbn);
		}
		if(StringUtils.isNotBlank(uri)) {
			log("log", "URI: " + uri);
		}
		if(StringUtils.isNotBlank(doi)) {
			log("log", "DOI: " + doi);
		}
		if(StringUtils.isNotBlank(page)) {
			log("log", "Page: " + page);
			String tokens[] = page.split("-|–");
			sp = tokens[0];
			if(tokens.length == 2){
				ep = tokens[1];
				long t1 = Long.parseLong(sp);
				long t2 = Long.parseLong(ep);
				if(t1 <= t2) {
					pc = String.valueOf(t2 - t1 + 1);
				} else {
					sp = null;
					ep = null;
				}
			}
		}
		log("log", NDLDataUtils.NEW_LINE);
		log("log_csv", new String[] { NDLDataUtils.getHandleSuffixID(item.getId()), journal, vol, issue, sp, ep, pc,
				issn, isbn, uri, doi });
		
		return true;
	}
	
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/generated_xml_data/Nirupam/2018.Nov.26.15.23.56.University_Of_Mysore.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/generated_xml_data/Nirupam/logs";
		String name = "mysore";
		
		MysoreAIPReader p = new MysoreAIPReader(input, logLocation, name);
		p.addTextLogger("log");
		p.addCSVLogger("log_csv",
				new String[] { "ID", "Journal", "Volume", "Issue", "Start-Page", "End-Page", "Page-Count", "ISSN",
						"ISBN", "AlternateUri", "DOI" });
		p.processData();
		
		System.out.println("Done.");
	}

}