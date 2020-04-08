package org.iitkgp.ndl.test.source;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.iitkgp.ndl.data.AIPDataItem;
import org.iitkgp.ndl.data.compress.CompressedDataItem;
import org.iitkgp.ndl.data.compress.TarGZCompressedFileReader;
import org.iitkgp.ndl.util.CommonUtilities;
import org.iitkgp.ndl.util.NDLDataUtils;

import com.opencsv.CSVWriter;

public class JEEAdvancedDataReader {
	
	String source;
	String logLocation;
	
	List<String[]> data = new ArrayList<String[]>();
	Map<String, String> titles = new HashMap<String, String>();
	
	public JEEAdvancedDataReader(String source,String logLocation) {
		this.source=source;
		this.logLocation=logLocation;
	}
	
	// read AIP
	void read() throws Exception {
		TarGZCompressedFileReader reader = new TarGZCompressedFileReader(new File(source));
		reader.init();
		CompressedDataItem item;
		// aip reader
		int c = 0;
		long start = System.currentTimeMillis();
		while((item = reader.next()) != null) {
			Map<String, byte[]> contents = new HashMap<String, byte[]>(2);
			contents.put(item.getEntryName(), item.getContents());
			AIPDataItem aip = new AIPDataItem();
			aip.load(contents);
			
			if(aip.isItem()) {
				// valid item
				String ispart = NDLDataUtils.getHandleSuffixID(aip.getSingleValue("dc.relation.ispartof"));
				String order = aip.getSingleValue("ndl.sequenceNo");
				String title = aip.getSingleValue("dc.title");
				String id = NDLDataUtils.getHandleSuffixID(aip.getId());
				
				String row[] = new String[]{ispart, id, title, order};
				data.add(row);
				//System.out.println(ispart + ", " + aip.getId() + ", " + title + ", " + order);
				titles.put(id, title);
				
				if(++c % 1000 == 0) {
					long end = System.currentTimeMillis();
					
					System.out.println("Processed: " + c);
					System.out.println(CommonUtilities.durationMessage(end - start));
				}
			}
		}
		
		System.out.println("Total processed: " + c);

		reader.close();
		
		System.out.println("Writing csv...");
		
		// write CSV
		CSVWriter hierarachy = NDLDataUtils.openCSV(new File(logLocation, "hierarachy.tree.csv"));
		hierarachy.writeNext(new String[]{"Parent_ID", "Parent_Title", "Item_ID", "Title", "Order"});
		for(String[] row : data) {
			hierarachy.writeNext(new String[]{row[0], titles.get(row[0]), row[1], row[2], row[3]});
		}
		hierarachy.close();
		
		long end = System.currentTimeMillis();
		System.out.println(CommonUtilities.durationMessage(end - start));
	}
	
	public static void main(String[] args) throws Exception {
		
		String source = "/home/dspace/debasis/NDL/generated_xml_data/JEE_advanced/out/2019.Mar.19.12.19.57.jee_advanced_v1/2019.Mar.19.12.19.57.jee_advanced_v1.Corrected.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/generated_xml_data/JEE_advanced/logs";
		
		System.out.println("Start.");
		
		JEEAdvancedDataReader p = new JEEAdvancedDataReader(source, logLocation);
		p.read();
		
		System.out.println("Done.");
	}

}