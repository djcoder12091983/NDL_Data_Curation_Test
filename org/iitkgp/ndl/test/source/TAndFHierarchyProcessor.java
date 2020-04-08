package org.iitkgp.ndl.test.source;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.CharUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.iterator.SIPDataIterator;
import org.iitkgp.ndl.util.NDLDataUtils;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

// Taylor and francis hierarchy processing for given file structure
public class TAndFHierarchyProcessor {
	
	String location;
	String outLocation;
	Set<String> handles = new HashSet<String>();
	
	BufferedWriter logger = null;
	
	public TAndFHierarchyProcessor(String location, String outLocation){
		this.location = location;
		this.outLocation = outLocation;
	}
	
	void collectValidItems(String source) throws Exception {
		System.out.println("Collecting valid items...");
		
		SIPDataIterator reader = null;
		long count = 0;
		try {
			reader = new SIPDataIterator(source);
			reader.init(); // initialization
			while(reader.hasNext()) {
				SIPDataItem item = reader.next();
				//String url = item.getSingleValue("dc.identifier.uri");
				if(!item.exists("dc.relation.haspart")) {
					// leaf item
					String id = NDLDataUtils.getHandleSuffixID(item.getId());
					handles.add(id);
				}
				
				if(++count % 10000 == 0) {
					System.out.println("Processed: " + count);
				}
			}
		} finally {
			IOUtils.closeQuietly(reader);
		}
	}
	
	String normalize(String id) {
		id = StringEscapeUtils.escapeHtml4(id); // escape
		StringBuilder modified = new StringBuilder();
		int l = id.length();
		for(int i = 0; i < l; i++) {
			char ch = id.charAt(i);
			if(CharUtils.isAsciiAlphanumeric(ch)) {
				modified.append(ch);
			} else {
				modified.append('_');
			}
		}
		
		return modified.toString().toLowerCase(); // further normalization
	}
	
	void handleHierarchy(CSVWriter writer, File file, String hierarchy) throws Exception {
		if(file.isFile()) {
			// csv file
			String t[] = hierarchy.split("/"); // journal and volume
			String journal = t[0];
			String vol = t[1];
			String issue = file.getName().replaceFirst("\\..*$", "");
			CSVReader reader = null;
			try {
				String log = file.getAbsolutePath() + " is processing ....";
				System.out.println(log);
				
				logger.write(log);;
				logger.newLine();
				
				reader = NDLDataUtils.readCSV(file);
				String tokens[];
				while((tokens = reader.readNext()) != null) {
					String url = tokens[0].replace("full", "abs");
					String id = normalize(tokens[1]); // normalize
					if(handles.contains(id)) {
						writer.writeNext(new String[]{journal, vol, issue, url, id});
						// remove from set to track remaining after process is done
						handles.remove(id);
					}
				}
			} finally {
				IOUtils.closeQuietly(reader);
			}
		} else if(file.isDirectory()) {
			File children[] = file.listFiles();
			for(File child : children) {
				// recursive processing
				handleHierarchy(writer, child,
						hierarchy + (StringUtils.isNotBlank(hierarchy) ? "/" : "") + child.getName());
			}
		}
	}
	
	public void process(String source) throws Exception {
		
		logger = new BufferedWriter(new FileWriter(new File(outLocation, "logger.log")));
		
		collectValidItems(source); // gets valid items from source
		
		CSVWriter writer = null;
		//CSVWriter error = null;
		BufferedWriter error = null;
		try {
			System.out.println("Hierarchy file(s) processing ...");
			
			// global writer
			writer = NDLDataUtils.openCSV(new File(outLocation, "TnF_Dir_Structure.v3.csv"));
			writer.writeNext(new String[]{"Journal", "Volume", "Issue", "URL", "Handle-ID"});
			handleHierarchy(writer, new File(location), StringUtils.EMPTY); // process whole hierarchy
			
			System.err.println("Missing items found: " + handles.size());
			
			// track errors if any
			if(!handles.isEmpty()) {
				// error exists
				//error = NDLDataUtils.openCSV(new File(location, "missing.url.csv"));
				//error.writeNext(new String[]{"Handle-ID"});
				error = new BufferedWriter(new FileWriter(new File(outLocation, "missing.items.csv")));
				for(String handle : handles) {
					error.write(handle);
					error.newLine();
				}
			}
		} finally {
			IOUtils.closeQuietly(writer);
			IOUtils.closeQuietly(error);
			IOUtils.closeQuietly(logger);
		}
	}
	
	public static void main(String[] args) throws Exception {
		String location = "/home/dspace/debasis/NDL/generated_xml_data/Taylor and Francis/conf/TotalItemOrder";
		String outLocation = "/home/dspace/debasis/NDL/generated_xml_data/Taylor and Francis/logs";
		String source = "/home/dspace/debasis/NDL/generated_xml_data/Taylor and Francis/in/t&f.17012019.tar.gz";
		
		TAndFHierarchyProcessor p = new TAndFHierarchyProcessor(location, outLocation);
		// System.out.println(p.normalize("10_1016_S0968-8080%2814%2943763-7"));
		p.process(source);
		
		System.out.println("Done.");
	}
}