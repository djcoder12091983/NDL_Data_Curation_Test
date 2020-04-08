package org.iitkgp.ndl.test.source;

import org.iitkgp.ndl.converter.NDLSIP2CSVConverter;
import org.iitkgp.ndl.data.Filter;
import org.iitkgp.ndl.data.SIPDataItem;

/**
 * Generates log files given conditions
 * @author Debasis
 *
 */
public class IARNasaTechDocsLogs {
	
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/IAR/raw_data/nasa-techdocs/in/nasa_techdocs-v3.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/IAR/raw_data/nasa-techdocs/logs";
		
		System.out.println("Start.");
		
		NDLSIP2CSVConverter converter = new NDLSIP2CSVConverter(input, logLocation, "Nasa-techdocs-logs");
		// filter logic
		converter.addDataFilter(new Filter<SIPDataItem>() {
			public boolean filter(SIPDataItem data) {
				//System.out.println(data.getValue("dc.date.other"));
				return data.getValue("dc.date.other:copyright").size() > 1;
			};
		});
		// column selection
		converter.addColumnSelector("dc.date.other:copyright", "Copyright Date");
		
		converter.convert(); // convert
		
		System.out.println("End.");
	}

}