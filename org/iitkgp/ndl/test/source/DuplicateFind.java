package org.iitkgp.ndl.test.source;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.util.NDLDataUtils;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

// duplicate find
public class DuplicateFind {
	
	public static void main1(String[] args) {
		//System.out.println("debasis\\ jana\\\\ debasis \\ jana \\".replaceAll("\\\\", ""));
	}
	
	public static void main(String[] args) throws Exception {
		
		long limit = 10000;
		
		String infile = "/home/dspace/debasis/NDL/NDL_sources/WEL/in";
		String out = "/home/dspace/debasis/NDL/NDL_sources/WEL/out";
		CSVWriter writer1 = NDLDataUtils.openCSV(new File(out, "wel.unique.csv"));
		writer1.writeNext(new String[]{"ID", "Title"});
		CSVWriter writer2 = NDLDataUtils.openCSV(new File(out, "wel.duplicate.csv"));
		writer2.writeNext(new String[]{"ID", "Title"});
		try {
			long c = 0;
			long uc = 0;
			long dc = 0;
			Set<String> unique = new HashSet<>();
			String tokens[];
			
			File[] files = new File(infile).listFiles();
			for(File file : files) {
				// reader files
				System.out.println("Processing: " + file.getName());
				CSVReader reader = NDLDataUtils.readCSV(file, 1);
				try {
					while((tokens = reader.readNext()) != null) {
						/*if(tokens[0].equals("wplbn0002575555")) {
							System.out.println("T: " + tokens[1]);
						}*/
						tokens[1] = tokens[1].replace("\\", "");
						if(StringUtils.isBlank(tokens[1])) {
							tokens[1] = "Untitled";
						}
						if(unique.add(tokens[0])) {
							// unique
							writer1.writeNext(new String[]{tokens[0], tokens[1]});
							uc++;
						} else {
							// duplicate
							writer2.writeNext(new String[]{tokens[0], tokens[1]});
							dc++;
						}
						
						/*if(tokens[0].equals("wplbn0002575555")) {
							System.out.println("T: " + tokens[1]);
						}*/
						
						if(++c % limit == 0) {
							System.out.println("Processed: " + c + ". Unique: " + uc + " Duplicate: " + dc + ".");
						}
					}
				} finally {
					reader.close();
				}
			}
			
			System.out.println("Total: " + c + ". Unique: " + uc + " Duplicate: " + dc + ".");
			
		} finally {
			writer1.close();
			writer2.close();
		}
	}

}