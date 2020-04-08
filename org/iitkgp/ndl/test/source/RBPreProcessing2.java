package org.iitkgp.ndl.test.source;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.context.NDLConfigurationContext;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.container.NDLSIPDataContainer;
import org.iitkgp.ndl.util.NDLDataUtils;

public class RBPreProcessing2 extends NDLSIPDataContainer {
	
	List<String[]> data = new ArrayList<String[]>();
	Map<String, String> titles = new HashMap<String, String>();

	public RBPreProcessing2(String input, String logLocation, String name) {
		super(input, logLocation, name);
	}
	
	@Override
	protected boolean readItem(SIPDataItem item) throws Exception {
		// valid item
		String ispart = NDLDataUtils.getHandleSuffixID(item.getSingleValue("dc.relation.ispartof"));
		String order = item.getSingleValue("dc.relation"); // order field
		String title = item.getSingleValue("dc.title"); // title
		String id = NDLDataUtils.getHandleSuffixID(item.getId());
		
		if(StringUtils.isNotBlank(ispart) && StringUtils.isNotBlank(order) && !StringUtils.equals(ispart, "0")) {
			// exclude root node
			String row[] = new String[]{ispart, id, title, order};
			data.add(row);
		}
		titles.put(id, title);
		
		return true;
	}
	
	@Override
	public void postProcessData() throws Exception {
		super.postProcessData(); // super call
		// csv logger
		addCSVLogger("rb.hierarchy", new String[] {"Parent_ID", "Parent_Title", "Item_ID", "Title", "Order"});
		// write hierarchy data
		for(String[] row : data) {
			log("rb.hierarchy", new String[]{row[0], titles.get(row[0]), row[1], row[2], row[3]});
		}
	}

	public static void main(String[] args) throws Exception {

		String input = "/home/dspace/debasis/NDL/NDL_sources/RAJ/in/2019.May.16.20.05.58.rb.v2.Corrected.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/NDL_sources/RAJ/logs";
		String name = "rb.hierarchy";
		
		NDLConfigurationContext.addConfiguration("compressed.data.process.buffer.size", "10");
		NDLConfigurationContext.addConfiguration("process.display.threshold.limit", "50");
		
		RBPreProcessing2 p = new RBPreProcessing2(input, logLocation, name);
		p.processData();
		
		System.out.println("Done.");
	}
}