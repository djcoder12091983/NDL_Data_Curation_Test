package org.iitkgp.ndl.test.source;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.iitkgp.ndl.util.CommonUtilities;
import org.iitkgp.ndl.util.NDLDataUtils;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

public class SpringerDOIDuplicatesFinder {
	
	public static void main(String[] args) throws Exception {
		
		System.out.println("Start.");
		
		String sourceLocation = "";
		String logLocation = "";
		
		Map<String, String> doimap = new HashMap<String, String>();
		long start = System.currentTimeMillis();
		
		// sources
		File csvfile = new File(sourceLocation);
		for(File file : csvfile.listFiles()) {
			System.out.println("Source Processing: " + file.getName());
			CSVReader reader = NDLDataUtils.readCSV(file);
			String tokens[] = null;
			while((tokens = reader.readNext()) != null) {
				String id = tokens[0]; // normalize
				String doi = tokens[1]; // normalize
				
				doimap.put(doi, id);
			}
			
			reader.close();
			
			System.out.println(CommonUtilities.durationMessage(System.currentTimeMillis() - start));
		}
		
		CSVWriter writer = NDLDataUtils.openCSV(new File(logLocation, "springer.doi.duplicates.csv"));
		// CSV header (source_handle, destination_handle, doi)

		// destination
		long count = 0;
		String destinationLocation = "";
		csvfile = new File(sourceLocation);
		for(File file : csvfile.listFiles()) {
			System.out.println("Destination Processing: " + file.getName());
			CSVReader reader = NDLDataUtils.readCSV(file);
			String tokens[] = null;
			while((tokens = reader.readNext()) != null) {
				String doi = null;
				// TODO get id and doi and normalize if needed
				// look-up
				if(doimap.containsKey(doi)) {
					// write into CSV
					
					// found counter
					count++;
				}
			}
			
			reader.close();

			System.out.println("Found: " + count);
			System.out.println(CommonUtilities.durationMessage(System.currentTimeMillis() - start));
		}
		
		writer.close();
		
		System.out.println("Done.");
		System.out.println("Found: " + count);
		System.out.println(CommonUtilities.durationMessage(System.currentTimeMillis() - start));
	}
}