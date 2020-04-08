package org.iitkgp.ndl.test.source;

import java.io.File;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.data.RowData;
import org.iitkgp.ndl.data.RowDataList;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.iterator.SIPDataIterator;
import org.iitkgp.ndl.util.CommonUtilities;

// CSV data generation for multiple sources
public class CSVGenerationForMultipleSources {
	
	static final long DISPLAY_THRESHOLD = 50000;

	public static void main(String[] args) throws Exception {
		
		String csvlocation = "/home/dspace/debasis/NDL/NDL_sources/DUPLICATE_COMPARISON/csv_data";
		String location = "/home/dspace/debasis/NDL/NDL_sources/DUPLICATE_COMPARISON/live_data/Springer-LIVE";
		File source = new File(location);
		
		RowDataList data = new RowDataList(2);
		int csvidx = 1;
		
		System.out.println("Start.");
		long start = System.currentTimeMillis();
		
		long c = 0;
		long tc = 0;
		for(File file : source.listFiles()) {
			System.out.println("Processing: " + file.getName());
			
			SIPDataIterator reader = new SIPDataIterator(file);
			reader.init();
			
			while(reader.hasNext()) {
				tc++;
				SIPDataItem sip = reader.next();
				
				//System.out.println(sip.getSingleValue("dc.identifier.other:doi"));
				
				RowData row = new RowData();
				String url = sip.getSingleValue("dc.identifier.uri");
				if(StringUtils.isNotBlank(url)) {
					//String doi = url.replaceFirst("^http://dx\\.doi\\.org/", "");
					int p = url.indexOf("10.");
					if(p != -1) {
						String doi = url.substring(p).replaceAll("%2F", "/");
						row.addData("ID", sip.getId());
						row.addData("DOI", doi);
						data.addRowData(row);

						if(++c % DISPLAY_THRESHOLD == 0) {
							System.out.println("Writing " + c + " data into csv.");
							System.out.println(CommonUtilities.durationMessage(System.currentTimeMillis() - start));

							// write CSV data
							File csvfile = new File(csvlocation, "springer.doi." + csvidx++ + ".csv");
							data.flush2CSV(csvfile, '|');
							// c = 0; // reset
						}
					}
				}
			}
			
			reader.close();
		}
		
		System.out.println("Done. Found: " + c + "/" + tc);
		System.out.println(CommonUtilities.durationMessage(System.currentTimeMillis() - start));
	}
}