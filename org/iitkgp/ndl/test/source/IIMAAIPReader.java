package org.iitkgp.ndl.test.source;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.iitkgp.ndl.data.AIPDataItem;
import org.iitkgp.ndl.data.container.NDLAIPDataContainer;
import org.iitkgp.ndl.util.NDLDataUtils;

public class IIMAAIPReader extends NDLAIPDataContainer {
	
	public IIMAAIPReader(String input, String logLocation, String name) {
		super(input, logLocation, name);
	}
	
	@Override
	protected boolean readItem(AIPDataItem item) throws Exception {
		String relation = item.getSingleValue("dc.relation");
		String itemid = null, date = null;
		boolean wp = false;
		if(StringUtils.isNotBlank(relation)) {
			String tokens[] = NDLDataUtils.split(relation, "(;| +|/)");
			int l = tokens.length;
			for(int i = 0; i < l; i++) {
				String t = tokens[i].trim();
				if(StringUtils.containsIgnoreCase(t, "wp") || StringUtils.containsIgnoreCase(t, "w.p")) {
					wp = true;
				} else if(NumberUtils.isDigits(t)) {
					if(t.length() == 4 && Integer.parseInt(t) >= 1900) {
						date = t;
					} else {
						itemid = t;
					}
				} else if(t.matches("[0-9]{4}-[0-9]{2}-[0-9]{2}")) {
					date = t;
				}
			}
			log("log_csv", new String[] { NDLDataUtils.getHandleSuffixID(item.getId()), relation,
					wp ? ("Working Paper" + (StringUtils.isNotBlank(itemid) ? (" No " + itemid) : "")) : "", date });
		}
		return true;
	}
	
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/generated_xml_data/Nirupam/2018.Dec.05.10.19.37.IIM_Ahm_mod_AIP.Corrected.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/generated_xml_data/Nirupam/logs";
		String name = "iima";
		
		IIMAAIPReader p = new IIMAAIPReader(input, logLocation, name);
		//p.addTextLogger("extraction.log");
		p.addCSVLogger("log_csv", new String[]{"ID", "Relation", "ItemID", "Date"});
		p.processData();
		
		System.out.println("Done.");
		
	}
}