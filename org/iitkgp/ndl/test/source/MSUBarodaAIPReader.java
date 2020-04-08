package org.iitkgp.ndl.test.source;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.iitkgp.ndl.data.AIPDataItem;
import org.iitkgp.ndl.data.container.NDLAIPDataContainer;
import org.iitkgp.ndl.util.NDLDataUtils;

public class MSUBarodaAIPReader extends NDLAIPDataContainer {
	
	public MSUBarodaAIPReader(String input, String logLocation, String name) {
		super(input, logLocation, name);
	}

	@Override
	protected boolean readItem(AIPDataItem item) throws Exception {
		String title = item.getSingleValue("dc.title");
		String tokens[] = NDLDataUtils.split(title, "( +|\\.|,)");
		int l = tokens.length;
		String vol = null, issue = null;
		for(int i = 0; i < l;) {
			String t = tokens[i].trim();
			if(StringUtils.startsWithIgnoreCase(t, "vol")) {
				String next1 = tokens[i + 1];
				if(i + 2 < l) {
					String next2 = tokens[i + 2];
					if(next2.equals("&")) {
						vol = next1 + " & " + tokens[i + 3];
						i += 4;
					} else {
						vol = next1;
						i += 2;
					}
				} else {
					vol = next1;
					i += 2;
				}
			} else if(StringUtils.startsWithIgnoreCase(t, "no") || StringUtils.startsWithIgnoreCase(t, "issue")) {
				if(i + 1 < l) {
					String next = tokens[i + 1];
					if(NumberUtils.isDigits(next)) {
						issue = next;
					}
					i += 2;
				} else {
					i++;
				}
			} else {
				i++;
			}
		}
		//if(StringUtils.isNotBlank(vol) || StringUtils.isNotBlank(issue)) {
			String id = NDLDataUtils.getHandleSuffixID(item.getId());
			log("log", new String[]{id, title, vol, issue});
		//}
		return true;
	}
	
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/generated_xml_data/Nirupam/MSU-Baroda.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/generated_xml_data/Nirupam/logs";
		String name = "msu.baroda";
	
		MSUBarodaAIPReader p = new MSUBarodaAIPReader(input, logLocation, name);
		p.addCSVLogger("log", new String[]{"ID", "Title", "Volume", "Issue"});
		p.processData();
		
		System.out.println("Done.");
	}

}