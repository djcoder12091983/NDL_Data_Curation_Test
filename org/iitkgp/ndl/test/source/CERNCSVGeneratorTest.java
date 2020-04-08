package org.iitkgp.ndl.test.source;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.container.DefaultNDLSIPDataContainer;
import org.iitkgp.ndl.util.NDLDataUtils;

public class CERNCSVGeneratorTest extends DefaultNDLSIPDataContainer {
	
	Set<String> rsvalues = new HashSet<String>();
	
	public CERNCSVGeneratorTest(String input, String logLocation, String name) {
		super(input, logLocation, name);
	}
	
	@Override
	protected boolean readItem(SIPDataItem item) throws Exception {
		Collection<String> values = NDLDataUtils.getValuesByJsonKey(item.getValue("ndl.sourceMeta.additionalInfo"),
				"RightsStatement");
		for(String v : values) {
			if(rsvalues.add(v)) {
				log("rs.log", v);
			}
		}
		// does not matter
		return true;
	}
	
	// test
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/generated_xml_data/CERN/in/CERN-Batch2-02.11.2018.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/generated_xml_data/CERN/logs";
		String name = "CERN.v1.rs";
		
		CERNCSVGeneratorTest g = new CERNCSVGeneratorTest(input, logLocation, name);
		g.addTextLogger("rs.log");
		g.processData();
		
		System.out.println("Done.");
	}
}