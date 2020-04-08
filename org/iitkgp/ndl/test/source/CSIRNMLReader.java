package org.iitkgp.ndl.test.source;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.iitkgp.ndl.data.AIPDataItem;
import org.iitkgp.ndl.data.container.NDLAIPDataContainer;
import org.iitkgp.ndl.util.NDLDataUtils;

public class CSIRNMLReader extends NDLAIPDataContainer {
	
	public CSIRNMLReader(String input, String logLocation, String name) {
		super(input, logLocation, name);
	}

	@Override
	protected boolean readItem(AIPDataItem item) throws Exception {
		List<String> values = item.getValue("dc.identifier.other");
		String vol = null, issue = null, page = null;
		String sp = null, ep = null, pc = null, issn = null, isbn = null;
		for(String value : values) {
			String tokens[] = NDLDataUtils.split(value, "( +|,|\\.|\\(|\\)|;|:)");
			int l = tokens.length;
			for(int i = 0; i < l;) {
				String t = tokens[i].trim();
				if(StringUtils.equalsIgnoreCase(t, "isbn")) {
					String next = tokens[i + 1].replace("-", "");
					int nl = next.length();
					if(nl == 8) {
						// issn
						issn = next;
					} else {
						// isbn
						isbn = next;
					}
					i += 2;
				} else if(NumberUtils.isDigits(t) && t.length() < 4) {
					// 4 digit number is year
					if(i + 1 < l) {
						String next = tokens[i + 1];
						if(NumberUtils.isDigits(next) || next.matches("[0-9]+-[0-9]+")) {
							// volume issue
							if(next.length() < 4) {
								vol = t;
								issue = next;
							}
							i += 2;
						} else {
							vol = t;
							i ++;
						}
					} else {
						i++;
					}
				} else if(StringUtils.equalsIgnoreCase(t, "pp")) {
					String next = tokens[i + 1];
					if(next.matches("[0-9]+(-|–)[0-9]+")) {
						// pagination
						page = next;
					}
					i += 2;
				} else if(t.matches("[0-9]+-[0-9]+")) {
					String next = null;
					if(i + 1 < l) {
						next = tokens[i + 1];
					}
					String prev = tokens[i - 1];
					if(!NDLDataUtils.isMonth(next) && !NDLDataUtils.isMonth(prev)) {
						// next or previous token is not month name
						page = t;
					}
					i++;
				} else {
					i++;
				}
			}
		}
		if(StringUtils.isNotBlank(page)) {
			String tokens[] = page.split("-|–");
			sp = tokens[0];
			if(tokens.length == 2) {
				ep = tokens[1];
				pc = String.valueOf(Long.parseLong(ep) - Long.parseLong(sp) + 1);
			}
		}
		log("log", new String[] { NDLDataUtils.getHandleSuffixID(item.getId()), NDLDataUtils.join(values, '|'), vol,
				issue, sp, ep, pc, issn, isbn });
		return true;
	}
	
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/generated_xml_data/Nirupam/2018.Dec.13.15.39.48.CSIR-NML.Corrected.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/generated_xml_data/Nirupam/logs";
		String name = "csir.nml";
		
		CSIRNMLReader p = new CSIRNMLReader(input, logLocation, name);
		p.addCSVLogger("log", new String[]{"ID", "Text", "Volume", "Issue", "Start-Page", "End-Page", "Page-Count", "ISSN", "ISBN"});
		p.processData();
				
		System.out.println("Done.");
	}
}