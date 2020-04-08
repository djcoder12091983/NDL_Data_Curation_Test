package org.iitkgp.ndl.test.source;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.iterator.SIPDataIterator;
import org.iitkgp.ndl.util.NDLDataUtils;

import com.opencsv.CSVWriter;

public class ASMEComparisonReport {
	
	static final long DISPLAY_THRESHOLD = 10000;
	
	class Data {
		String handle;
		String doi;
		String title;
		String journal;
		String volume;
		String issue;
		String url;
	}
	
	String olddata;
	String newdata;
	String location;
	
	// old data
	Map<String, Data> omap = new HashMap<>();
	
	public ASMEComparisonReport(String olddata, String newdata, String location) {
		this.olddata = olddata;
		this.newdata = newdata;
		this.location = location;
	}
	
	
	// TODO exception handling is not OK
	void process() throws Exception {
		
		long c = 0;
		System.out.println("Old data indexing....");
		for(File file : new File(olddata).listFiles()) {
			
			System.out.println("Processing ... " + file.getName());
			
			SIPDataIterator i = new SIPDataIterator(file);
			i.init();
			while(i.hasNext()) {
				SIPDataItem sip = i.next();
				String doi = sip.getSingleValue("dc.identifier.other:doi");
				// indexing
				if(StringUtils.isNotBlank(doi)) {
					Data d = new Data();
					d.handle = sip.getId();
					d.doi = doi;
					d.title = sip.getSingleValue("dc.title");
					d.journal = sip.getSingleValue("dc.identifier.other:journal");
					d.volume = sip.getSingleValue("dc.identifier.other:volume");
					d.issue = sip.getSingleValue("dc.identifier.other:issue");
					d.url = sip.getSingleValue("dc.identifier.uri");
					
					omap.put(doi, d);
				}
				
				if(++c % DISPLAY_THRESHOLD == 0) {
					System.out.println("Processed: " + c);
				}
			}
			i.close();
		}
		
		System.out.println("Processed: " + c);

		System.out.println("Comparing with new data.....");
		
		BufferedReader newd = new BufferedReader(new FileReader(newdata));
		String l;
		c = 0;
		long ndc = 0, mc = 0, ndvimc = 0;
		CSVWriter ndw = NDLDataUtils.openCSV(new File(location, "asme.new.data.csv"));
		ndw.writeNext(new String[]{"DOI", "Journal", "Volume", "Issue", "File name"});
		CSVWriter mcw = NDLDataUtils.openCSV(new File(location, "asme.missing.vi.data.csv"));
		mcw.writeNext(new String[] { "Handle", "DOI", "Journal", "Old Volume", "New volume", "Old Issue", "New issue",
				"Title", "URL" });
		while((l = newd.readLine()) != null) {
			String tokens[] = l.split("\\|");
			String doi = tokens[0];
			if(omap.containsKey(doi)) {
				// compare if volume issue missing
				Data oldd = omap.get(doi);
				String oldv = oldd.volume;
				String oldi = oldd.issue;
				String newv = tokens[2];
				String newi = tokens[3];
				
				if (StringUtils.isBlank(newv) || StringUtils.isBlank(newi) || !StringUtils.equalsIgnoreCase(oldv, newv)
						|| !StringUtils.equalsIgnoreCase(oldi, newi)) {
					// missing
					mc++;
					mcw.writeNext(new String[] { oldd.handle, doi, tokens[1], oldv, tokens[2], oldi, tokens[3],
							oldd.title, oldd.url });
				}
			} else {
				// new data
				ndc++;
				if(StringUtils.isBlank(tokens[2]) || StringUtils.isBlank(tokens[3])) {
					ndvimc++;
				}
				//int p = tokens[4].lastIndexOf('/');
				ndw.writeNext(new String[]{doi, tokens[1], tokens[2], tokens[3], tokens[4]});
			}
			
			if(++c % DISPLAY_THRESHOLD == 0) {
				System.out.println("Processed: " + c);
			}
		}
		
		System.out.println("Processed: " + c);
		System.out.println(
				"New data: " + ndc + " New data volume/issue missing count: " + ndvimc + " Missing data: " + mc);
		
		newd.close();
		ndw.close();
		mcw.close();
		
		System.out.println("Done.");
	}
	
	public static void main(String[] args) throws Exception {
		
		String olddata = "/home/dspace/debasis/NDL/NDL_sources/ASME/ASME-LiveData";
		String newdata = "/home/dspace/debasis/NDL/NDL_sources/ASME/asme.new.data";
		String location = "/home/dspace/debasis/NDL/NDL_sources/ASME/logs";
		
		ASMEComparisonReport p = new ASMEComparisonReport(olddata, newdata, location);
		p.process();
	}

}